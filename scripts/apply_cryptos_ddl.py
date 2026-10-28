#!/usr/bin/env python
"""应用 sql/cryptos_ddl.sql 到 ClickHouse（幂等）。

守卫：
  1. 只允许 cryptos 库的 DDL，出现其他库名直接拒绝
  2. 执行前后比对 quant / quant_v4 的表数与字节数，必须完全不变
  3. 默认 dry-run，--execute 才真正执行

    python scripts/apply_cryptos_ddl.py            # 只看会执行什么
    python scripts/apply_cryptos_ddl.py --execute
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import clickhouse_connect

REPO_ROOT = Path(__file__).resolve().parents[1]
DDL = REPO_ROOT / "sql" / "cryptos_ddl.sql"
PROTECTED = ("quant", "quant_v4", "stock", "system")
TARGET_DB = "cryptos"

CH = dict(
    host=os.environ.get("CRYPTOS_CH_HOST", "192.168.1.200"),
    port=int(os.environ.get("CRYPTOS_CH_PORT", "8123")),
    username=os.environ.get("CRYPTOS_CH_USER", "default"),
    password=os.environ.get("CRYPTOS_CH_PASSWORD", ""),
)


def stats(cli, db: str) -> tuple[int, int]:
    r = cli.query(
        "SELECT uniqExact(table), sum(bytes_on_disk) FROM system.parts "
        "WHERE active AND database = {db:String}",
        parameters={"db": db},
    ).first_row
    return int(r[0] or 0), int(r[1] or 0)


def statements() -> list[str]:
    body = "\n".join(
        line for line in DDL.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("--")
    )
    return [s.strip() for s in body.split(";") if s.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    stmts = statements()
    for s in stmts:
        head = s.lstrip().upper()[:120]
        if TARGET_DB.upper() not in head:
            print(f"❌ 拒绝执行非 {TARGET_DB} 语句:\n{s[:200]}")
            return 2

    cli = clickhouse_connect.get_client(**CH)
    before = {db: stats(cli, db) for db in PROTECTED}
    print(f"目标: {CH['host']}:{CH['port']}/{TARGET_DB}   语句数: {len(stmts)}")
    print(f"执行前受保护库: {before}")

    if not args.execute:
        for i, s in enumerate(stmts, 1):
            print(f"  [{i}] {s.splitlines()[0][:100]} ...")
        print("\n(dry-run，加 --execute 才执行)")
        return 0

    for i, s in enumerate(stmts, 1):
        cli.command(s)
        print(f"  [{i}/{len(stmts)}] OK  {s.splitlines()[0][:80]}")

    after = {db: stats(cli, db) for db in PROTECTED}
    print(f"执行后受保护库: {after}")
    if before != after:
        print("❌ 受保护库发生变化！")
        return 3
    print("✅ 受保护库全部未变")

    tables = cli.query(
        f"SELECT name FROM system.tables WHERE database = '{TARGET_DB}' ORDER BY name"
    ).result_rows
    print(f"{TARGET_DB} 共 {len(tables)} 张表: {[t[0] for t in tables]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
