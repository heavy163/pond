"""核对「按规则该采到的标的」与「实际写进来的标的」的差集。

为什么不用 sync_stat.status：它只说明请求没报错，不说明每个标的都拿到了数据。

口径：
  - 合约（perp）：instrument 表 contract_type='perp' 的全部 code
  - 现货（spot）：本所若有同名永续，只算这些对（filter_spot_by_perp 生效）；
    纯现货所（upbit 无永续）算全部
  - 实采：最近 N 小时内 ingested_at 有写入的 code

用法:
  /usr/bin/python3.12 scripts/audit_coverage.py              # 默认 3 小时
  /usr/bin/python3.12 scripts/audit_coverage.py --hours 1 --venue gate
  /usr/bin/python3.12 scripts/audit_coverage.py --no-filter  # 按未过滤全集核对
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

import clickhouse_connect

CH = dict(host="192.168.1.200", port=8123, username="default", password="")
DB = "cryptos"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=3, help="最近 N 小时内有写入即算实采")
    ap.add_argument("--venue", default=None)
    ap.add_argument("--no-filter", action="store_true", help="不做「现货按永续」过滤")
    ap.add_argument("--show-missing", type=int, default=3, help="每项最多列几个缺失 code")
    args = ap.parse_args()

    c = clickhouse_connect.get_client(**CH)
    where = f"AND venue = '{args.venue}'" if args.venue else ""

    # 1) 应采口径
    inst = c.query(f"""
        SELECT venue, code, contract_type FROM {DB}.instrument FINAL
        WHERE contract_type IN ('perp', 'spot') {where}
    """).result_rows
    perp: dict[str, set[str]] = defaultdict(set)
    spot: dict[str, set[str]] = defaultdict(set)
    for v, code, t in inst:
        (perp if t == "perp" else spot)[v].add(code)

    declared: dict[tuple[str, str], set[str]] = {}
    for v in set(perp) | set(spot):
        declared[(v, "perp")] = perp[v]
        sp = spot[v]
        if not args.no_filter and perp[v]:
            sp = sp & perp[v]
        declared[(v, "spot")] = sp

    # 2) 实采口径
    got_fut = c.query(f"""
        SELECT venue, code FROM {DB}.kline_futures_1h
        WHERE ingested_at > now() - INTERVAL {args.hours} HOUR {where}
        GROUP BY venue, code
    """).result_rows
    got_spot = c.query(f"""
        SELECT venue, code FROM {DB}.kline_spot_1h
        WHERE ingested_at > now() - INTERVAL {args.hours} HOUR {where}
        GROUP BY venue, code
    """).result_rows
    got: dict[tuple[str, str], set[str]] = defaultdict(set)
    for v, code in got_fut:
        got[(v, "perp")].add(code)
    for v, code in got_spot:
        got[(v, "spot")].add(code)

    print(
        f"窗口：最近 {args.hours} 小时（按 ingested_at）"
        f"{'，现货已按永续过滤' if not args.no_filter else '，未过滤'}"
    )
    print(f"{'venue':<13}{'类型':<7}{'应采':>7}{'实采':>7}{'缺失':>7}{'覆盖率':>9}")
    print("-" * 52)
    total_d = total_g = 0
    bad = 0
    for (v, t), want in sorted(declared.items()):
        if not want:
            continue
        have = got.get((v, t), set()) & want
        miss = len(want) - len(have)
        pct = len(have) / len(want) * 100
        total_d += len(want)
        total_g += len(have)
        flag = "" if miss == 0 else ("  ←" if pct < 95 else "")
        bad += 1 if pct < 95 else 0
        print(f"{v:<13}{t:<7}{len(want):>7}{len(have):>7}{miss:>7}{pct:>8.1f}%{flag}")

    print("-" * 52)
    pct_all = total_g / total_d * 100 if total_d else 0.0
    print(f"{'合计':<13}{'':<7}{total_d:>7}{total_g:>7}{total_d - total_g:>7}{pct_all:>8.1f}%")

    if args.show_missing:
        print("\n缺失样例：")
        for (v, t), want in sorted(declared.items()):
            miss_list = sorted(want - got.get((v, t), set()))
            if miss_list:
                print(f"  {v}/{t}: {len(miss_list)} 个，例如 {miss_list[: args.show_missing]}")

    c.close()
    print(f"\n低于 95% 的组合：{bad} 个")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
