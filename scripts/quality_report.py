#!/usr/bin/env python
"""下游取数门禁 CLI。

    # 建 SQL 视图（一次性）
    python scripts/quality_report.py --install-views

    # 检查全部 8 家最近 7 天 kline 是否可用（门禁口径）
    python scripts/quality_report.py --days 7

    # 检查 OI（这会暴露 6 家快照所的永久缺失）
    python scripts/quality_report.py --data oi --days 7

    # 单合约行级标记（给前值填充决策）
    python scripts/quality_report.py --venue okx --code BTC-USDT --rows

退出码：0=全部可用，1=有窗口不可用（CI / 因子管道可以直接用）。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from pond.cryptos.config import CryptosConfig
from pond.cryptos.quality import QualityGate
from pond.cryptos.store import CryptoStore


def parse_args():
    p = argparse.ArgumentParser(description="cryptos 数据完整性门禁")
    p.add_argument("--venue", help="逗号分隔；默认全部 enabled")
    p.add_argument("--data", default="kline")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--code", help="单合约行级标记")
    p.add_argument("--rows", action="store_true", help="打印行级 present/lost 标记")
    p.add_argument("--min-ratio", type=float, default=0.98)
    p.add_argument("--max-stale", type=float, default=2.0)
    p.add_argument("--allow-lost", action="store_true")
    p.add_argument("--install-views", action="store_true")
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    store = CryptoStore.from_env()
    cfg = CryptosConfig.load(None)
    gate = QualityGate(store, cfg)

    if args.install_views:
        gate.install_views()
        print("视图已安装：cryptos.v_coverage_day / cryptos.v_coverage_venue_day")

    venues = (
        [v.strip() for v in args.venue.split(",") if v.strip()]
        if args.venue
        else [s.name for s in cfg.enabled_venues()]
    )

    if args.rows:
        if not args.code:
            print("--rows 需要 --code", file=sys.stderr)
            return 2
        rows = gate.row_flags(args.venue or venues[0], args.code, args.data, args.days)
        missing = [r for r in rows if not r["present"]]
        print(f"{args.venue or venues[0]}/{args.code}/{args.data}: {len(rows)} 个应有小时，"
              f"缺 {len(missing)}（其中永久丢失 {sum(1 for r in missing if r['lost'])}）")
        for r in rows[-48:]:
            flag = "OK " if r["present"] else ("LOST" if r["lost"] else "MISS")
            print(f"  {r['datetime']:%Y-%m-%d %H:%M}  {flag}")
        if args.json:
            print(json.dumps([{**r, "datetime": r["datetime"].isoformat()} for r in rows],
                             ensure_ascii=False))
        store.finalize()
        return 0

    results = []
    rc = 0
    print("=" * 96)
    print(f"{'venue':<13}{'data':<9}{'codes':>6}{'应有':>7}{'实有':>7}{'覆盖率':>9}"
          f"{'缺失':>7}{'丢失日':>7}{'停更h':>7}  可用")
    for v in venues:
        q = gate.check(
            v,
            data_type=args.data,
            days=args.days,
            min_ratio=args.min_ratio,
            max_stale_hours=args.max_stale,
            allow_lost=args.allow_lost,
        )
        results.append(q.summary())
        miss = sum(int((b - a).total_seconds() // 3600)
                   for ranges in q.missing.values() for a, b in ranges)
        lostn = sum(len(x) for x in q.lost.values())
        print(f"{v:<13}{args.data:<9}{q.codes_present:>6}{q.expected_hours:>7}"
              f"{q.present_hours:>7}{q.ratio:>8.2%}{miss:>7}{lostn:>7}"
              f"{q.stale_hours:>7.1f}  {'✓' if q.usable else '✗ ' + q.reasons[0][:28]}")
        if not q.usable:
            rc = 1
    print("=" * 96)
    if args.json:
        print(json.dumps(results, ensure_ascii=False))
    store.finalize()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
