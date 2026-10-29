"""把云端录制的 SQLite 恢复到本地 ClickHouse（只补缺失行，不覆盖已有数据）。

场景
----
本机宕机几天 → 云端 recorder 一直在录 → 本机恢复后，把这份 SQLite 交给运维，
用本工具把**本地缺的那些小时**补回去。已有的行不重灌（省时间，也不制造 part）。

做法
----
按 (表, 交易所, 自然月) 分片：
    1. 从本地 CH 拉出该月已有的 (code, ts) 键
    2. 扫 SQLite 该月的行，键不在本地集合里的才写入
内存只跟「一个交易所一个月的键数」有关（~600 码 × 744 小时 ≈ 45 万键），不会爆。

用法
----
    # 只体检，不写库（默认）
    /usr/bin/python3.12 ops/recorder/restore.py --db ~/recorder/cryptos.sqlite

    # 真写
    /usr/bin/python3.12 ops/recorder/restore.py --db ~/recorder/cryptos.sqlite --execute

    # 只补某个时间段 / 某几张表
    /usr/bin/python3.12 ops/recorder/restore.py --db ... --execute \
        --from 2026-10-01 --to 2026-10-05 --tables kline_futures_1h open_interest
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
from loguru import logger

CH = dict(host="192.168.1.200", port=8123, username="default", password="")

# 表 → (时间列, 键列, 需要转 datetime 的列)
SPEC = {
    "kline_futures_1h": ("open_time", ("venue", "code", "open_time"),
                         ("open_time", "datetime", "ingested_at")),
    "kline_spot_1h": ("open_time", ("venue", "code", "open_time"),
                      ("open_time", "datetime", "ingested_at")),
    "open_interest": ("datetime", ("venue", "code", "datetime"),
                      ("datetime", "ingested_at")),
    "funding_rate": ("datetime", ("venue", "code", "datetime"),
                     ("datetime", "ingested_at")),
    "mark_price": ("datetime", ("venue", "code", "datetime"),
                   ("datetime", "ingested_at")),
    "instrument": ("ingested_at", ("venue", "code"),
                   ("listed_at", "delisted_at", "ingested_at")),
}

BATCH = 50_000


def month_buckets(lo: datetime, hi: datetime):
    cur = lo.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur <= hi:
        nxt = (cur + timedelta(days=32)).replace(day=1)
        yield cur, nxt
        cur = nxt


def to_dt(v):
    return None if v is None else datetime.fromtimestamp(int(v), tz=timezone.utc)


def restore_table(con, ch, table: str, target_db: str, lo, hi, execute: bool) -> tuple[int, int]:
    tcol, keycols, dtcols = SPEC[table]
    if table == "instrument":
        lo = datetime(1970, 1, 1, tzinfo=timezone.utc)
        hi = datetime.now(timezone.utc) + timedelta(days=1)
    cols = [r[0] for r in ch.query(f"DESCRIBE TABLE {target_db}.{table}").result_rows]
    idx = {c: i for i, c in enumerate(cols)}
    total_missing = total_checked = 0

    # 键的取法：时间序列表用 (code, ts)；instrument 没有时间键，只用 code
    is_time = len(keycols) == 3
    sel = "code, toUnixTimestamp(" + tcol + ")" if is_time else "code"

    venues = [r[0] for r in con.execute(f'SELECT DISTINCT venue FROM "{table}"')]
    buckets = [(lo, hi)] if table == "instrument" else list(month_buckets(lo, hi))
    for venue in venues:
        for m_lo, m_hi in buckets:
            # 本地已有键
            where = "venue = %(v)s"
            params = {"v": venue}
            if is_time:
                where += f" AND {tcol} >= toDateTime(%(a)s) AND {tcol} < toDateTime(%(b)s)"
                params |= {"a": int(m_lo.timestamp()), "b": int(m_hi.timestamp())}
            rows = ch.query(f"SELECT {sel} FROM {target_db}.{table} WHERE {where}",
                            parameters=params).result_rows
            have = {(c, int(t)) for c, t in rows} if is_time else {r[0] for r in rows}

            if is_time:
                cur = con.execute(
                    f'SELECT * FROM "{table}" WHERE venue = ? AND {tcol} >= ? AND {tcol} < ?',
                    (venue, int(m_lo.timestamp()), int(m_hi.timestamp())))
            else:
                cur = con.execute(f'SELECT * FROM "{table}" WHERE venue = ?', (venue,))
            names = [d[0] for d in cur.description]
            batch, missing = [], 0
            for row in cur:
                rec = dict(zip(names, row))
                key = (rec["code"], int(rec[tcol])) if is_time else rec["code"]
                total_checked += 1
                if key in have:
                    continue
                missing += 1
                vals = []
                for c in cols:
                    v = rec.get(c)
                    vals.append(to_dt(v) if c in dtcols else v)
                batch.append(vals)
                if len(batch) >= BATCH:
                    if execute:
                        ch.insert(f"{target_db}.{table}", batch, column_names=cols)
                    batch = []
            if batch and execute:
                ch.insert(f"{target_db}.{table}", batch, column_names=cols)
            if missing:
                logger.info(f"  {table}/{venue} {m_lo:%Y-%m}: 缺 {missing:,} 行"
                            f"{'（已写入）' if execute else '（dry-run）'}")
            total_missing += missing
    return total_checked, total_missing


def main() -> int:
    ap = argparse.ArgumentParser(description="SQLite 录制 → 本地 ClickHouse（只补缺失）")
    ap.add_argument("--db", required=True, help="云端 recorder 产出的 SQLite")
    ap.add_argument("--target-db", default="cryptos")
    ap.add_argument("--tables", nargs="*", default=list(SPEC))
    ap.add_argument("--from", dest="lo", help="YYYY-MM-DD（默认=SQLite 最早时间）")
    ap.add_argument("--to", dest="hi", help="YYYY-MM-DD（默认=SQLite 最晚时间）")
    ap.add_argument("--execute", action="store_true", help="真写库；缺省只体检")
    args = ap.parse_args()

    path = Path(args.db).expanduser()
    if not path.exists():
        print(f"找不到 {path}")
        return 2
    con = sqlite3.connect(path)
    ch = clickhouse_connect.get_client(**CH)

    print(f"{'表':<22}{'扫描行':>12}{'缺失行':>12}")
    grand = 0
    for table in args.tables:
        tcol = SPEC[table][0]
        lo_r, hi_r = con.execute(f'SELECT min({tcol}), max({tcol}) FROM "{table}"').fetchone()
        if lo_r is None:
            print(f"{table:<22}{0:>12,}{0:>12,}   (空)")
            continue
        lo = datetime.fromisoformat(args.lo).replace(tzinfo=timezone.utc) if args.lo else to_dt(lo_r)
        if args.hi:
            hi = datetime.fromisoformat(args.hi).replace(tzinfo=timezone.utc) + timedelta(days=1)
        else:
            hi = (to_dt(hi_r) or datetime.now(timezone.utc)) + timedelta(hours=1)
        checked, missing = restore_table(con, ch, table, args.target_db, lo, hi, args.execute)
        grand += missing
        print(f"{table:<22}{checked:>12,}{missing:>12,}")

    print(f"\n合计缺失 {grand:,} 行 {'（已写入 ' + args.target_db + '）' if args.execute else '（dry-run，加 --execute 才写）'}")
    con.close()
    ch.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
