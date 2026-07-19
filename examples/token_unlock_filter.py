#!/usr/bin/env python3
"""
代币解锁排除 — 使用示例

从 CoinMarketCap 免费 API 获取即将解锁的代币数据，
自动映射到币安 USDT 永续合约，返回需排除的合约列表。

用法:
    python examples/token_unlock_filter.py [--pct 1.0] [--days 14]

依赖:
    pip install requests loguru
"""

import argparse
import sys
from pathlib import Path

# 确保能找到 pond 模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pond.token_unlock import UnlockFilter


def main():
    parser = argparse.ArgumentParser(
        description="代币解锁排除 — 过滤即将解锁的币安合约"
    )
    parser.add_argument(
        "--pct", type=float, default=1.0, help="最小解锁占比阈值 % (默认: 1.0)"
    )
    parser.add_argument("--days", type=int, default=14, help="未来窗口天数 (默认: 14)")
    args = parser.parse_args()

    uf = UnlockFilter()

    # 获取排除列表
    excluded = uf.get_excluded_contracts(
        min_unlock_pct=args.pct,
        window_days=args.days,
    )

    print(f"\n{'='*60}")
    print("  Token Unlock Exclusion Report")
    print(f"  Threshold: >= {args.pct}% unlock within {args.days} days")
    print(f"  Binance USDT Perpetual Contracts to EXCLUDE: {len(excluded)}")
    print(f"{'='*60}")

    if excluded:
        for sym in excluded:
            print(f"    ❌ {sym}")
    else:
        print("    ✅ No contracts to exclude")

    # 获取详细摘要
    summary = uf.get_unlock_summary(
        min_unlock_pct=args.pct,
        window_days=args.days,
    )

    if summary:
        print(f"\n{'='*60}")
        print("  Detailed Unlock Summary (on Binance)")
        print(f"{'='*60}")
        for s in summary:
            if s["on_binance_futures"]:
                print(
                    f"  {s['contract']:>12s} | "
                    f"{s['unlock_date'][:10]} | "
                    f"{s['unlock_pct']:>4.1f}% | "
                    f"${s['unlock_usd']:>10,.0f} | "
                    f"mcap=${s['market_cap']:>10,.0f}"
                )


if __name__ == "__main__":
    main()
