"""ClickHouse → SQLite 电子存档（离线可携、可回灌）。

用途
----
本机（192.168.1.200）同时跑 ClickHouse、交易库、MQTT Broker。整机宕机 = 数据不可逆中断。
本工具把 ClickHouse 里**不可再生 / 关键**的表按天导出成 SQLite 单文件，
一个月一个文件（`quant_YYYYMM.sqlite`），可以整包丢到云服务器上存 6 个月；
需要时再从 SQLite 原样灌回 ClickHouse。

设计要点
--------
1. **按「天」为最小归档单元**：每天独立导出，可重跑（先 DELETE 当天再插入），不会重复。
2. **时间列一律存 epoch**：DateTime → epoch 秒，DateTime64 → epoch 毫秒，Date → 'YYYY-MM-DD'。
   避开 ClickHouse 服务端时区（Asia/Shanghai）与 Python 本地时区的一切歧义。
3. **原样保留 DDL**：导出时把 `SHOW CREATE TABLE` 存进 `_manifest`，恢复时按原建表语句重建。
4. **两种模式**：
   - `full`    —— 小表、不可再生（持仓/钱包/信息），全历史归档
   - `rolling` —— 大表、可回补或只需近期（K线/OI/预测），只留最近 N 个月
5. 导出完成后写 `_days` 台账（表, 日期, 行数），`verify` 用它跟 ClickHouse 对数。

用法
----
    # 看清单
    /usr/bin/python3.12 ops/archive/ch_sqlite.py inventory

    # 导出（默认最近 6 个月 + full 表全历史），落到 archives/
    systemd-run --user --scope -p MemoryMax=8G --collect --quiet \
        /usr/bin/python3.12 ops/archive/ch_sqlite.py export --months 6

    # 校验（SQLite 行数 vs ClickHouse 行数）
    /usr/bin/python3.12 ops/archive/ch_sqlite.py verify

    # 回灌到测试库（默认 quant_test，绝不写 quant）
    /usr/bin/python3.12 ops/archive/ch_sqlite.py restore --target quant_test
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

import clickhouse_connect

CH = dict(host="192.168.1.200", port=8123, username="default", password="")
SRC_DB = "quant"
ARCHIVE_DIR = Path.home() / "quant_archive"

# ── 归档范围 ───────────────────────────────────────────────────────────
# mode: full = 全历史；rolling = 只留最近 N 个月
TABLES: dict[str, dict] = {
    # 行情：交易所不一定给这么久的历史，且 OI 只给快照 —— 丢了就永久没有
    "kline_futures_1h":                 {"mode": "rolling"},
    "kline_spot_1h":                    {"mode": "rolling"},
    "future_open_interest":             {"mode": "rolling"},
    "future_funding_rate":              {"mode": "rolling"},
    "future_long_short_ratio":          {"mode": "rolling"},
    "future_long_short_position_ratio": {"mode": "full"},
    # 自己的预测与交易：完全不可再生
    "futures_prediction":               {"mode": "rolling"},
    "futures_prediction_v2":            {"mode": "rolling"},
    "future_position":                  {"mode": "full"},
    "future_wallet":                    {"mode": "full"},
    "future_info":                      {"mode": "full"},
    "market_regime":                    {"mode": "full"},
    "stock_strategy_signal":            {"mode": "full"},
}

DT64 = "DateTime64"
SCALAR_OK = ("Int", "UInt", "Float", "Decimal", "String", "Enum", "Bool", "Date", "IPv4", "UUID")


def _strip_nullable(ch_type: str) -> tuple[str, bool]:
    t = ch_type.strip()
    if t.startswith("Nullable("):
        return t[len("Nullable("):-1], True
    if t.startswith("LowCardinality("):
        return t[len("LowCardinality("):-1], False
    return t, False


def sql_type(ch_type: str) -> str:
    base, _ = _strip_nullable(ch_type)
    if base.startswith(DT64) or base.startswith("DateTime"):
        return "INTEGER"
    if base.startswith("Date"):
        return "TEXT"
    if base.startswith(("Int", "UInt", "Bool")):
        return "INTEGER"
    if base.startswith(("Float", "Decimal")):
        return "REAL"
    if base.startswith(("String", "Enum", "FixedString", "IPv4", "UUID")):
        return "TEXT"
    raise ValueError(f"未支持的 ClickHouse 类型: {ch_type}（复杂类型需先扩展本工具）")


def to_sql(val, ch_type: str):
    """ClickHouse 值 → SQLite 可存值。"""
    if val is None:
        return None
    base, _ = _strip_nullable(ch_type)
    if base.startswith(DT64):
        return int(round(val.timestamp() * 1000))
    if base.startswith("DateTime"):
        return int(val.timestamp())
    if base.startswith("Date"):
        return val.isoformat()[:10]
    if base.startswith(("Int", "UInt", "Bool", "Float", "Decimal")):
        return val
    return str(val)


def from_sql(val, ch_type: str):
    """SQLite 值 → 供 clickhouse_connect 插入的 Python 值。"""
    if val is None:
        return None
    base, _ = _strip_nullable(ch_type)
    if base.startswith(DT64):
        return dt.datetime.fromtimestamp(val / 1000, tz=dt.timezone.utc).astimezone()
    if base.startswith("DateTime"):
        return dt.datetime.fromtimestamp(val, tz=dt.timezone.utc).astimezone()
    if base.startswith("Date"):
        return dt.date.fromisoformat(val) if isinstance(val, str) else val
    if base.startswith(("Decimal",)):
        from decimal import Decimal
        return Decimal(str(val))
    return val


# ── SQLite 辅助 ────────────────────────────────────────────────────────
def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute(
        """CREATE TABLE IF NOT EXISTS _manifest (
               tbl        TEXT PRIMARY KEY,
               time_col   TEXT NOT NULL,
               mode       TEXT NOT NULL,
               ch_ddl     TEXT NOT NULL,
               columns    TEXT NOT NULL,
               ch_types   TEXT NOT NULL,
               updated_at TEXT NOT NULL)"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS _days (
               tbl TEXT NOT NULL, day TEXT NOT NULL, rows INTEGER NOT NULL,
               exported_at TEXT NOT NULL, PRIMARY KEY (tbl, day))"""
    )
    return con


def _meta(raw: str) -> list[str]:
    """兼容两种存法：JSON（新）/ 逗号分隔（旧）。"""
    raw = raw.strip()
    if raw.startswith("["):
        return json.loads(raw)
    return raw.split(",")


def sql_tbl(name: str) -> str:
    return f"t_{name}"


def detect_time_col(cols: list[tuple[str, str]]) -> str:
    for name, ctype in cols:
        if ctype.startswith(("DateTime", "Date")):
            return name
    raise ValueError("找不到时间列，无法做按天归档")


def ensure_table(con: sqlite3.Connection, tbl: str, cols: list[tuple[str, str]]) -> None:
    defs = ", ".join(f'"{n}" {sql_type(t)}' for n, t in cols)
    con.execute(f'CREATE TABLE IF NOT EXISTS "{sql_tbl(tbl)}" ({defs})')
    con.execute(f'CREATE INDEX IF NOT EXISTS "ix_{sql_tbl(tbl)}_t" ON "{sql_tbl(tbl)}" ('
                + ", ".join(f'"{n}"' for n, t in cols if t.startswith(("DateTime", "Date"))) + ")")


def archive_file(tbl: str, day: dt.date) -> Path:
    """full 表 → 单个 quant_full.sqlite（小、要全历史）；
    rolling 表 → 每月一个 quant_rolling_YYYYMM.sqlite（便于按 6 个月保留/删除）。"""
    if TABLES[tbl]["mode"] == "full":
        return ARCHIVE_DIR / "quant_full.sqlite"
    return ARCHIVE_DIR / f"quant_rolling_{day:%Y%m}.sqlite"


# ── 命令：inventory ────────────────────────────────────────────────────
def cmd_inventory(_: argparse.Namespace) -> None:
    c = clickhouse_connect.get_client(**CH)
    print(f"{'表名':<36}{'模式':<10}{'行数':>12}{'最小时间':<21}{'最大时间':<21}")
    total = 0
    for tbl, cfg in TABLES.items():
        cols = [(r[0], r[1]) for r in c.query(f"DESCRIBE TABLE {SRC_DB}.{tbl}").result_rows]
        tcol = detect_time_col(cols)
        n, tmin, tmax = c.query(f"SELECT count(), min({tcol}), max({tcol}) FROM {SRC_DB}.{tbl}").result_rows[0]
        total += n
        print(f"{tbl:<36}{cfg['mode']:<10}{n:>12,}{str(tmin)[:19]:<21}{str(tmax)[:19]:<21}")
    print(f"\n合计 {total:,} 行，{len(TABLES)} 张表")
    c.close()


# ── 命令：export ───────────────────────────────────────────────────────
def _server_tz(c) -> dt.tzinfo:
    name = c.query("SELECT timezone()").result_rows[0][0]
    from zoneinfo import ZoneInfo
    return ZoneInfo(name)


def _bounds(day: dt.date, tz: dt.tzinfo) -> tuple[int, int]:
    """某天的 [lo, hi) epoch 秒，按 ClickHouse 服务端时区的自然日切。"""
    lo = dt.datetime.combine(day, dt.time(), tzinfo=tz)
    hi = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), tzinfo=tz)
    return int(lo.timestamp()), int(hi.timestamp())


def cmd_export(args: argparse.Namespace) -> None:
    c = clickhouse_connect.get_client(**CH)
    tz = _server_tz(c)
    cutoff = (dt.date.today() - dt.timedelta(days=args.months * 31))
    tables = args.tables or list(TABLES)
    grand = 0
    for tbl in tables:
        mode = TABLES[tbl]["mode"]
        cols = [(r[0], r[1]) for r in c.query(f"DESCRIBE TABLE {SRC_DB}.{tbl}").result_rows]
        types = [t for _, t in cols]
        tcol = detect_time_col(cols)
        ddl = c.query(f"SHOW CREATE TABLE {SRC_DB}.{tbl}").result_rows[0][0]
        colnames = ", ".join(f'"{n}"' for n, _ in cols)

        tmin, tmax = c.query(
            f"SELECT min(toDate({tcol})), max(toDate({tcol})) FROM {SRC_DB}.{tbl}"
        ).result_rows[0]
        if tmin is None:
            print(f"[skip] {tbl}: 空表")
            continue
        start = tmin if mode == "full" else max(tmin, cutoff)
        if start > tmax:
            print(f"[skip] {tbl}: {cutoff} 之后无数据（最新 {tmax}）")
            continue

        rows_table, day = 0, start
        while day <= tmax:
            lo, hi = _bounds(day, tz)
            con = open_db(archive_file(tbl, day))
            ensure_table(con, tbl, cols)
            con.execute("INSERT OR REPLACE INTO _manifest VALUES (?,?,?,?,?,?,?)",
                        (tbl, tcol, mode, ddl, json.dumps([n for n, _ in cols], ensure_ascii=False),
                         json.dumps(types), dt.datetime.now().isoformat(timespec="seconds")))
            # 幂等：先删当天再重灌
            if tcol and dict(cols)[tcol].startswith("Date") and not dict(cols)[tcol].startswith("DateTime"):
                con.execute(f'DELETE FROM "{sql_tbl(tbl)}" WHERE "{tcol}" >= ? AND "{tcol}" < ?',
                            (day.isoformat(), (day + dt.timedelta(days=1)).isoformat()))
            else:
                con.execute(f'DELETE FROM "{sql_tbl(tbl)}" WHERE "{tcol}" >= ? AND "{tcol}" < ?', (lo, hi))

            # 用 epoch 做窗口，避开字符串时间字面量的时区歧义
            # ReplacingMergeTree 必须带 FINAL，否则把未合并的重复行也归档进去，
            # 回灌后行数会多于源库 FINAL 口径（future_wallet 实测多 553 行）
            final = " FINAL" if "ReplacingMergeTree" in ddl else ""
            sql = (f"SELECT {colnames} FROM {SRC_DB}.{tbl}{final} "
                   f"WHERE toUnixTimestamp({tcol}) >= {lo} AND toUnixTimestamp({tcol}) < {hi}")
            ph = ",".join("?" * len(cols))
            n = 0
            with c.query_row_block_stream(sql) as stream:
                for block in stream:
                    payload = [tuple(to_sql(v, types[i]) for i, v in enumerate(row)) for row in block]
                    con.executemany(f'INSERT INTO "{sql_tbl(tbl)}" VALUES ({ph})', payload)
                    n += len(payload)
            con.execute("INSERT OR REPLACE INTO _days VALUES (?,?,?,?)",
                        (tbl, day.isoformat(), n, dt.datetime.now().isoformat(timespec="seconds")))
            con.commit()
            con.close()
            rows_table += n
            day += dt.timedelta(days=1)
        grand += rows_table
        print(f"[done] {tbl:<34} {start} → {tmax}  {rows_table:>10,} 行")
    # 清掉没有内容的文件（例如某月该表本来就没数据）
    for f in sorted(ARCHIVE_DIR.glob("quant_*.sqlite")):
        con = sqlite3.connect(f)
        n = con.execute("SELECT COALESCE(SUM(rows), 0) FROM _days").fetchone()[0]
        con.close()
        if n == 0:
            f.unlink()
    print(f"\n[export] 合计写入 {grand:,} 行 → {ARCHIVE_DIR}")
    c.close()


# ── 命令：verify ───────────────────────────────────────────────────────
def cmd_verify(_: argparse.Namespace) -> None:
    c = clickhouse_connect.get_client(**CH)
    files = sorted(ARCHIVE_DIR.glob("quant_*.sqlite"))
    if not files:
        print(f"[verify] {ARCHIVE_DIR} 下没有归档文件")
        sys.exit(1)
    bad = 0
    for f in files:
        con = sqlite3.connect(f)
        print(f"\n=== {f.name} ===")
        for tbl, tcol, days, rows_sql in con.execute(
                """SELECT tbl, (SELECT time_col FROM _manifest m WHERE m.tbl=d.tbl),
                          count(*), sum(rows) FROM _days d GROUP BY tbl ORDER BY tbl"""):
            lo, hi = con.execute(
                "SELECT min(day), max(day) FROM _days WHERE tbl=?", (tbl,)).fetchone()
            # 只有 ReplacingMergeTree 能加 FINAL（MergeTree 会报 ILLEGAL_FINAL）；
            # 且必须加 —— 普通 count() 含未合并重复行（future_wallet 差 553 行）
            ddl = c.query(f"SHOW CREATE TABLE {SRC_DB}.{tbl}").result_rows[0][0]
            final = " FINAL" if "ReplacingMergeTree" in ddl else ""
            n_ch = c.query(
                f"SELECT count() FROM {SRC_DB}.{tbl}{final} "
                f"WHERE {tcol} >= toDateTime('{lo} 00:00:00') "
                f"AND {tcol} < toDateTime('{hi} 00:00:00') + INTERVAL 1 DAY").result_rows[0][0]
            ok = "OK " if n_ch == rows_sql else "差异"
            if n_ch != rows_sql:
                bad += 1
            print(f"  [{ok}] {tbl:<34} sqlite={rows_sql:>10,}  ch={n_ch:>10,}  {lo} → {hi}")
        con.close()
    print(f"\n[verify] 差异表 {bad} 个")
    c.close()
    if bad:
        sys.exit(1)


# ── 命令：restore ──────────────────────────────────────────────────────
def cmd_restore(args: argparse.Namespace) -> None:
    if args.target == SRC_DB and not args.force:
        print("[restore] 拒绝直接写 quant（生产库）。确需回灌请加 --force，"
              "或先恢复到 quant_test 校验")
        sys.exit(2)
    c = clickhouse_connect.get_client(**CH)
    c.command(f"CREATE DATABASE IF NOT EXISTS {args.target}")
    files = [Path(args.file)] if args.file else sorted(ARCHIVE_DIR.glob("quant_*.sqlite"))
    total = 0
    for f in files:
        con = sqlite3.connect(f)
        tables = [r[0] for r in con.execute("SELECT DISTINCT tbl FROM _manifest ORDER BY tbl")]
        for tbl in tables:
            if args.tables and tbl not in args.tables:
                continue
            ddl, colnames_raw, types_raw = con.execute(
                "SELECT ch_ddl, columns, ch_types FROM _manifest WHERE tbl=?", (tbl,)).fetchone()
            colnames, types = _meta(colnames_raw), _meta(types_raw)
            cols = list(zip(colnames, types))
            ddl_new = ddl.replace(f"CREATE TABLE {SRC_DB}.{tbl}",
                                  f"CREATE TABLE IF NOT EXISTS {args.target}.{tbl}", 1)
            c.command(ddl_new)
            # 整表恢复默认先清空 —— 否则重跑一次就翻倍（我在测试时踩过：142,179 变 283,252）
            if not args.append:
                c.command(f"TRUNCATE TABLE IF EXISTS {args.target}.{tbl}")
            sel = ", ".join(f'"{n}"' for n, _ in cols)
            cur = con.execute(f'SELECT {sel} FROM "{sql_tbl(tbl)}"')
            buf, n = [], 0
            while True:
                batch = cur.fetchmany(50_000)
                if not batch:
                    break
                buf = [tuple(from_sql(v, types[i]) for i, v in enumerate(row)) for row in batch]
                c.insert(f"{args.target}.{tbl}", buf, column_names=[n for n, _ in cols])
                n += len(buf)
            total += n
            print(f"  [restore] {args.target}.{tbl:<34} {n:>10,} 行")
        con.close()
    print(f"\n[restore] 合计 {total:,} 行 → {args.target}")
    c.close()


def cmd_prune(args: argparse.Namespace) -> None:
    """删除超过保留期的 rolling 月文件（full 文件永久保留）。"""
    keep_from = dt.date.today() - dt.timedelta(days=args.months * 31)
    keep_tag = f"{keep_from:%Y%m}"
    removed = 0
    for f in sorted(ARCHIVE_DIR.glob("quant_rolling_*.sqlite")):
        tag = f.stem.rsplit("_", 1)[-1]
        if tag < keep_tag:
            f.unlink()
            removed += 1
            print(f"  [prune] 删除 {f.name}")
    print(f"[prune] 保留 {args.months} 个月（{keep_tag} 起），删除 {removed} 个文件")


def main() -> None:
    ap = argparse.ArgumentParser(description="ClickHouse → SQLite 电子存档")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("inventory").set_defaults(func=cmd_inventory)

    p = sub.add_parser("export")
    p.add_argument("--months", type=int, default=6, help="rolling 表保留月数（默认 6）")
    p.add_argument("--tables", nargs="*", help="只导出指定表")
    p.set_defaults(func=cmd_export)

    sub.add_parser("verify").set_defaults(func=cmd_verify)

    pp = sub.add_parser("prune")
    pp.add_argument("--months", type=int, default=6)
    pp.set_defaults(func=cmd_prune)

    p = sub.add_parser("restore")
    p.add_argument("--file", help="单个归档文件；缺省=全部")
    p.add_argument("--target", default="quant_test", help="目标库（默认 quant_test）")
    p.add_argument("--tables", nargs="*")
    p.add_argument("--force", action="store_true", help="允许写生产库 quant")
    p.add_argument("--append", action="store_true",
                   help="追加而不是整表覆盖（默认先 TRUNCATE，保证可重复执行）")
    p.set_defaults(func=cmd_restore)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
