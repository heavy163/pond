#!/usr/bin/env python
"""数据完整性检查 + 自动修复 + 覆盖度台账。

    # 只看最近 7 天有没有洞（不写库）
    python scripts/check_gaps.py --days 7

    # 检测 + 修复 + 写 coverage 台账
    python scripts/check_gaps.py --days 7 --execute

    # 只查某家
    python scripts/check_gaps.py --venue okx --days 14 --execute

退出码：0=无缺口（或全部修复成功），1=仍有不可修复缺口，2=修复失败。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from pond.cryptos.config import CryptosConfig
from pond.cryptos.gaps import GapDetector, GapRepairer
from pond.cryptos.store import CryptoStore


def parse_args():
    p = argparse.ArgumentParser(description="cryptos 数据完整性检查")
    p.add_argument("--venue", help="逗号分隔；默认全部 enabled")
    p.add_argument("--data", default="kline,funding,oi")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--no-repair", action="store_true", help="只检测不修复")
    p.add_argument("--execute", action="store_true", help="写库（修复数据 + coverage 台账）")
    p.add_argument("--config", default=None)
    p.add_argument("--no-coverage", action="store_true", help="只修复，不写 coverage 台账（给每小时调用）")
    p.add_argument("--strict", action="store_true", help="有永久丢失缺口时返回 1（默认只告警）")
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    store = CryptoStore.from_env()
    cfg = CryptosConfig.load(args.config)
    venues = (
        [v.strip() for v in args.venue.split(",") if v.strip()]
        if args.venue
        else [s.name for s in cfg.enabled_venues()]
    )
    data = [d.strip() for d in args.data.split(",") if d.strip()]

    det = GapDetector(store, cfg)
    gaps = det.detect(venues, data, args.days)

    repairable = [g for g in gaps if g.repairable and not g.rampup]
    lost = [g for g in gaps if not g.repairable and not g.rampup]
    rampup = [g for g in gaps if g.rampup]

    print("=" * 84)
    print(f"检查窗口 {args.days} 天 / {len(venues)} 家 / {len(data)} 类数据")
    print(
        f"缺口 {len(gaps)} 个：可修复 {len(repairable)}，永久丢失 {len(lost)}，"
        f"采集首日噪声 {len(rampup)}"
    )
    print("=" * 84)

    if gaps:
        by_v = {}
        for g in gaps:
            by_v.setdefault((g.venue, g.data_type), [0, 0, 0])
            by_v[(g.venue, g.data_type)][0] += 1
            by_v[(g.venue, g.data_type)][1] += g.missing
            if not g.repairable:
                by_v[(g.venue, g.data_type)][2] += 1
        print(f"{'venue':<13}{'data':<9}{'缺口天数':>9}{'缺失根数':>9}{'永久丢失':>9}")
        for (v, dt), (n, miss, l) in sorted(by_v.items()):
            print(f"{v:<13}{dt:<9}{n:>9}{miss:>9}{l:>9}")
        print("-" * 84)
        for g in gaps[:12]:
            flag = "✗ 永久" if not g.repairable else "✓ 可补"
            print(f"  {flag}  {g.venue}/{g.data_type}/{g.code} {g.day} 缺 {g.missing}")
        if len(gaps) > 12:
            print(f"  ... 另有 {len(gaps) - 12} 个")

    rc = 0
    if args.execute:
        if repairable and not args.no_repair:
            print("\n开始修复 ...")
            rep = asyncio.run(GapRepairer(store, cfg, execute=True).repair(repairable))
            print(f"修复完成：补回 {rep.repaired} 行，失败 {rep.failed} 个窗口")
            for e in rep.errors[:8]:
                print(f"  ! {e}")
            if rep.failed:
                rc = 2
        if args.no_coverage:
            print("（--no-coverage：跳过台账，台账由每日 00:20 写）")
        else:
            n = det.write_coverage(gaps, venues, args.days)
            print(f"coverage 台账写入 {n} 行")
    else:
        print("\n（dry-run：加 --execute 才会修复并写 coverage 台账）")

    if lost:
        if args.strict:
            rc = max(rc, 1)
        print("\n⚠️ 永久丢失的数据（任何手段都补不回来，下游必须知道）：")
        for g in lost[:8]:
            print(f"  {g.venue}/{g.data_type}/{g.code} {g.day}: {g.reason}")

    if args.json:
        print(json.dumps([g.__dict__ | {"day": str(g.day)} for g in gaps], ensure_ascii=False))

    store.finalize()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
