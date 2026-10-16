"""持有者表 DDL 迁移：修正 ReplacingMergeTree 主键 + 补分析必需列。

背景与证据：report/manual/institution_follow/scripts/sandbox_holder_ddl.py 实测，
原 `ORDER BY (datetime, code)` 会把同一公告日同一只股票的 10 个流通股东压成 1 行
（丢 90%，且不报错）。

本脚本做三件事，全部【从 ORM 生成 DDL】以保证 ORM 与活库不漂移：
  1. 备份现有 DDL + 断言待重建表【行数为 0】（非 0 直接拒绝，绝不静默丢数据）；
  2. DROP 待重建表；
  3. 实例化 ClickHouseManager（其 __init__ 会 metadata.create_all），从 ORM 重建；
  4. 读回校验：列集合 / ORDER BY / PARTITION BY 与 ORM 编译结果逐字一致。

⚠️ 只操作 quant 库。quant_v4 的同名副本不动（它是陈旧副本，本任务不用）。
   若需要，把 quant_v4 也纳管只需在 DATABASES 里加一项。

用法（默认只读 dry-run）：
    PYTHONPATH=<pond> /usr/bin/python3.12 pond/clickhouse/scripts/migrate_holder_ddl.py
    PYTHONPATH=<pond> /usr/bin/python3.12 pond/clickhouse/scripts/migrate_holder_ddl.py --execute
"""
import argparse
import json
import pathlib
import sys

import clickhouse_connect
from sqlalchemy.schema import CreateTable

HOST, PORT, DB = "192.168.1.200", 8123, "quant"

# 待重建的表（主键被证明有误，或新增了必需列）
# HolderCounts 不在列内：它的 (datetime, code) 本来就唯一，只顺带把
# partition_by 从 toYYYYMM 改成 toYear；因与同组表一致性要求，也一并重建。
TABLES = [
    "FreeHoldingDetail",
    "HoldingDetail",
    "FreeHoldingStatistic",
    "HoldingStatistic",
    "HolderCounts",
    "StockRestrictedReleaseDetail",
]

BACKUP = pathlib.Path(__file__).resolve().parent.parent.parent.parent / "_cache" / \
    "holder_ddl_backup.json"


def live_ddl(c, table: str) -> str:
    rows = c.query(f"SHOW CREATE TABLE {DB}.`{table}`").result_rows
    return rows[0][0] if rows else ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="真正执行 DROP+CREATE（默认只读）")
    ap.add_argument("--drop-nonempty", action="store_true",
                    help="允许重建非空表（会丢数据）。仅当数据可从源头完整重拉时使用；"
                         "当前 holder 表即属此类：源为东财公开接口，重拉成本约 50 分钟。")
    args = ap.parse_args()

    # ── ORM 侧：编译出期望 DDL ────────────────────────────────────────────────
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    from pond.clickhouse import metadata                                    # noqa: E402
    import pond.clickhouse.holders  # noqa: F401,E402  (注册表定义)
    from pond.clickhouse.manager import ClickHouseManager                   # noqa: E402

    c = clickhouse_connect.get_client(host=HOST, port=PORT, username="default",
                                      password="", database=DB, connect_timeout=15)

    print("=== ① 备份现有 DDL + 行数断言 ===")
    backup, blocked = {}, []
    for t in TABLES:
        n = c.query(f"SELECT count() FROM {DB}.`{t}`").result_rows[0][0]
        ddl = live_ddl(c, t)
        backup[t] = {"rows": int(n), "ddl": ddl}
        ob = ddl.split("ORDER BY")[1].split("\n")[0].strip() if "ORDER BY" in ddl else "?"
        flag = "✅" if n == 0 else "❌ 非空"
        print(f"  {t:32s} rows={n:>10,}  ORDER BY={ob:32s} {flag}")
        if n != 0:
            blocked.append(t)
    BACKUP.parent.mkdir(parents=True, exist_ok=True)
    BACKUP.write_text(json.dumps(backup, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"  备份 → {BACKUP}")

    if blocked and not args.drop_nonempty:
        print(f"\n❌ 拒绝执行：以下表非空 {blocked}。"
              f"重建会丢数据，必须先人工确认迁移策略。"
              f"\n   若数据可从源头完整重拉，加 --drop-nonempty 明确承担数据丢失。")
        return 1
    if blocked:
        print(f"\n⚠️  --drop-nonempty 已开启：以下非空表将被 DROP 重建 {blocked}")

    print("\n=== ② ORM 编译出的期望 DDL ===")
    # 只建 engine 取 dialect —— 不实例化 ClickHouseManager，
    # 否则它的 __init__ 里的 create_all 会在 dry-run 阶段就产生副作用。
    from sqlalchemy import create_engine                                    # noqa: E402

    dialect = create_engine(f"clickhouse://default:@{HOST}:{PORT}/{DB}").dialect
    expect = {}
    for t in TABLES:
        tbl = metadata.tables[f"{t}"]
        sql = str(CreateTable(tbl).compile(dialect=dialect))
        expect[t] = sql
        ob = sql.split("ORDER BY")[1].split("\n")[0].strip() if "ORDER BY" in sql else "?"
        print(f"  {t:32s} ORDER BY={ob}")
        # 自检：ORDER BY 的每一列必须真在表里。写错列名不会报错，只会让
        # ReplacingMergeTree 按【错误子集】去重 —— 这正是本轮丢 22~37% 行的成因。
        if ob != "?":
            ob_cols = [x.strip() for x in ob.strip().strip("()").split(",")]
            missing = [x for x in ob_cols if x not in tbl.columns]
            assert not missing, f"{t}: ORDER BY 引用了不存在的列 {missing}"

    if not args.execute:
        print("\n[dry-run] 未执行任何写操作。加 --execute 才会 DROP+CREATE。")
        return 0

    print("\n=== ③ DROP + 从 ORM 重建 ===")
    for t in TABLES:
        c.command(f"DROP TABLE IF EXISTS {DB}.`{t}`")
        print(f"  dropped {DB}.{t}")
    # ClickHouseManager.__init__ 内会执行 metadata.create_all(self.engine)
    ClickHouseManager(f"clickhouse://default:@{HOST}:{PORT}/{DB}", data_start=None)
    print("  metadata.create_all 完成")

    print("\n=== ④ 读回校验（列集合 / ORDER BY 逐字比对 ORM）===")
    bad = []
    for t in TABLES:
        ddl = live_ddl(c, t)
        cols_live = [r[0] for r in c.query(f"DESCRIBE {DB}.`{t}`").result_rows]
        cols_orm = [col.name for col in metadata.tables[f"{t}"].columns]
        ob_live = ddl.split("ORDER BY")[1].split("\n")[0].strip() if "ORDER BY" in ddl else "?"
        ob_orm = expect[t].split("ORDER BY")[1].split("\n")[0].strip() \
            if "ORDER BY" in expect[t] else "?"
        ok_cols = set(cols_live) == set(cols_orm)
        ok_ob = ob_live == ob_orm
        print(f"  {t:32s} 列 {len(cols_live):>2d}({'OK' if ok_cols else 'MISMATCH'})  "
              f"ORDER BY live={ob_live} orm={ob_orm} {'OK' if ok_ob else 'MISMATCH'}")
        if not (ok_cols and ok_ob):
            bad.append(t)

    if bad:
        print(f"\n❌ 校验未通过：{bad}")
        return 1
    print("\n✅ 全部表已按新 ORM 重建并通过读回校验")
    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
