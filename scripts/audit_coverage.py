"""核对「交易所声明可采」与「我们实际采到」的差集。

用途：数据完整性的唯一可验证判据——不是看 sync_stat 里 status=synced，
而是逐所比对 instrument 表里的合约/现货对，与 K 线表里最近 N 小时真正写进来的 code。

用法:
  /usr/bin/python3.12 scripts/audit_coverage.py                 # 默认查 3 小时窗口
  /usr/bin/python3.12 scripts/audit_coverage.py --hours 1
  /usr/bin/python3.12 scripts/audit_coverage.py --venue gate
"""

from __future__ import annotations

import argparse
import sys

import clickhouse_connect

CH = dict(host="192.168.1.200", port=8123, username="default", password="")
DB = "cryptos"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=3, help="以最近 N 小时内有写入为准（用 ingested_at，不用 bar 时间）")
    ap.add_argument("--venue", default=None)
    ap.add_argument("--show-missing", type=int, default=5, help="每所最多列出几个缺失 code")
    args = ap.parse_args()

    c = clickhouse_connect.get_client(**CH)
    where = f"AND venue = '{args.venue}'" if args.venue else ""

    rows = c.query(f"""
        SELECT i.venue,
               i.contract_type,
               uniqExact(i.code) AS declared,
               countIf(k.code != '') AS collected
        FROM (
            SELECT venue, code, contract_type
            FROM {DB}.instrument FINAL
            WHERE contract_type IN ('perp', 'spot') {where}
        ) i
        LEFT JOIN (
            SELECT venue, code, 'x' AS code FROM (
                SELECT venue, code FROM {DB}.kline_futures_1h
                 WHERE ingested_at > now() - INTERVAL {args.hours} HOUR
                UNION ALL
                SELECT venue, code FROM {DB}.kline_spot_1h
                 WHERE ingested_at > now() - INTERVAL {args.hours} HOUR
            ) GROUP BY venue, code
        ) k ON i.venue = k.venue AND i.code = k.code
        GROUP BY i.venue, i.contract_type
        ORDER BY i.venue, i.contract_type
    """).result_rows

    print(f"窗口：最近 {args.hours} 小时")
    print(f"{'venue':<13}{'类型':<7}{'声明':>8}{'实采':>8}{'缺失':>8}{'覆盖率':>9}")
    print("-" * 56)
    bad = 0
    total_declared = total_collected = 0
    for venue, ctype, declared, collected in rows:
        missing = declared - collected
        pct = collected / declared * 100 if declared else 0.0
        total_declared += declared
        total_collected += min(collected, declared)
        flag = "" if missing <= 0 else ("  ←" if pct < 95 else "")
        bad += 1 if pct < 95 else 0
        print(f"{venue:<13}{ctype:<7}{declared:>8}{collected:>8}{missing:>8}{pct:>8.1f}%{flag}")

    print("-" * 56)
    pct_all = total_collected / total_declared * 100 if total_declared else 0.0
    print(f"{'合计':<13}{'':<7}{total_declared:>8}{total_collected:>8}"
          f"{total_declared - total_collected:>8}{pct_all:>8.1f}%")

    if args.show_missing:
        miss = c.query(f"""
            SELECT i.venue, i.contract_type, arraySlice(groupArray(i.code), 1, {args.show_missing}) AS sample, count() AS n
            FROM (
                SELECT venue, code, contract_type FROM {DB}.instrument FINAL
                WHERE contract_type IN ('perp', 'spot') {where}
            ) i
            LEFT JOIN (
                SELECT venue, code, max(1) AS hit FROM (
                    SELECT venue, code FROM {DB}.kline_futures_1h
                     WHERE ingested_at > now() - INTERVAL {args.hours} HOUR
                    UNION ALL
                    SELECT venue, code FROM {DB}.kline_spot_1h
                     WHERE ingested_at > now() - INTERVAL {args.hours} HOUR
                ) GROUP BY venue, code
            ) k ON i.venue = k.venue AND i.code = k.code
            WHERE k.hit = 0
            GROUP BY i.venue, i.contract_type
            ORDER BY n DESC
        """).result_rows
        if miss:
            print("\n缺失样例：")
            for venue, ctype, sample, n in miss:
                print(f"  {venue}/{ctype}: {n} 个，例如 {sample}")

    c.close()
    print(f"\n低于 95% 的组合：{bad} 个")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
