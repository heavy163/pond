#!/usr/bin/env python
"""历史回灌 CLI。

    # 看计划（不写库）
    python scripts/backfill_cryptos.py --venue gate --data oi --days 180

    # 真写：Gate 180 天 OI
    python scripts/backfill_cryptos.py --venue gate --data oi --days 180 --execute

    # 两家一起（各自独立限频，并行跑）
    python scripts/backfill_cryptos.py --venue gate,bybit --data oi --execute

优先级：OI（不可再生）> funding > kline。
回灌行写 ingested_at = bar时间+1s，实时数据永远覆盖它，不会反向污染。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from pond.cryptos.backfill import BackfillRunner
from pond.cryptos.config import CryptosConfig
from pond.cryptos.store import CryptoStore


def parse_args():
    p = argparse.ArgumentParser(description="cryptos 历史回灌")
    p.add_argument("--venue", required=True, help="逗号分隔，如 gate,bybit")
    p.add_argument("--data", default="oi", help="逗号分隔：oi,funding,kline（默认 oi）")
    p.add_argument("--symbols", default=None, help="逗号分隔的 base，如 BTC,ETH（默认用配置宇宙）")
    p.add_argument("--days", type=int, default=None, help="回灌天数；OI 会被各所上限截断")
    p.add_argument("--chunk-days", type=int, default=30, help="每批天数（断点粒度）")
    p.add_argument("--rate-factor", type=float, default=0.5, help="限频折扣，默认 0.5")
    p.add_argument("--config", default=None)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    venues = [v.strip() for v in args.venue.split(",") if v.strip()]
    data = [d.strip() for d in args.data.split(",") if d.strip()]

    store = CryptoStore.from_env()
    cfg = CryptosConfig.load(args.config)
    runner = BackfillRunner(
        store=store,
        config=cfg,
        chunk_days=args.chunk_days,
        rate_factor=args.rate_factor,
        execute=args.execute,
    )

    if not args.execute:
        logger.warning("dry-run：只拉取不写库，加 --execute 才写")

    symbols = (
        [x.strip() for x in args.symbols.split(",") if x.strip()]
        if args.symbols
        else None
    )
    results = asyncio.run(runner.run(venues, data, args.days, symbols))

    print("\n" + "=" * 78)
    print(f"{'venue':<13}{'data':<10}{'codes':>7}{'rows':>10}{'跳过':>7}{'耗时':>10}{'errors':>8}")
    for r in results:
        print(
            f"{r.venue:<13}{r.data_type:<10}{r.codes:>7}{r.rows:>10}{r.skipped:>7}"
            f"{r.duration_ms/1000:>9.1f}s{len(r.errors):>8}"
        )
        for e in r.errors[:5]:
            print(f"    ! {e}")
    total = sum(r.rows for r in results)
    print(f"合计 {total:,} 行")
    print("=" * 78)

    if args.json:
        import json

        print(json.dumps([r.__dict__ for r in results], ensure_ascii=False))

    store.finalize()
    return 0 if all(not r.errors for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
