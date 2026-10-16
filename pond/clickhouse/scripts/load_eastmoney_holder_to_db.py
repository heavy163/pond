"""把东财「十大流通股东 / 十大股东」明细灌进 ClickHouse。

对应报表：
    REPORT=free  → RPT_F10_EH_FREEHOLDERS → quant.FreeHoldingDetail（默认）
    REPORT=top   → RPT_F10_EH_HOLDERS     → quant.HoldingDetail

用法（配合 run_chunked_backfill.py；SYMBOL_OFFSET/SYMBOL_LIMIT 在**报告期**维度切片）：

    REPORT=free EXECUTE=1 PYTHONPATH=<pond> /usr/bin/python3.12 \
        pond/clickhouse/scripts/run_chunked_backfill.py \
        pond/clickhouse/scripts/load_eastmoney_holder_to_db.py \
        --total 87 --chunk-size 12 --timeout 900

⚠️ 不要一次 INSERT 跨 20 年的数据：本表 partition_by=toYear，单块跨年数超过 100
   会撞 `max_partitions_per_insert_block`。本脚本按报告期逐个写，天然规避。

设计说明：
- 分块以**报告期**为单位（本数据是截面不是时序），每块一个全新进程，
  复用 run_chunked_backfill 的硬超时 + 进程组 SIGKILL + 自动重试。
- 空报告期（该季还没披露 / 尚未存在）跳过，不算失败。
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

from pond.clickhouse.data_proxy.eastmoney import EastMoneyHolderProxy
from pond.clickhouse.manager import ClickHouseManager

PERIOD_START = os.environ.get("PERIOD_START", "2005-01-01")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="真正写库（默认只读）")
    args = ap.parse_args()
    if os.environ.get("EXECUTE") == "1":
        args.execute = True

    report = os.environ.get("REPORT", "free")
    proxy = EastMoneyHolderProxy(report=report)
    table = proxy.cfg["table"]

    periods = proxy.get_periods(start=PERIOD_START)
    off = int(os.environ.get("SYMBOL_OFFSET", 0))
    lim = int(os.environ.get("SYMBOL_LIMIT", len(periods)))
    chunk = periods[off:off + lim]
    if not chunk:
        print(f"offset={off} 已越过报告期数量 {len(periods)}，无事可做")
        return 0

    host = os.environ.get("CLICKHOUSE_HOST", "192.168.1.200")
    db = os.environ.get("CLICKHOUSE_DB", "quant")
    pwd = os.environ.get("CLICKHOUSE_PWD", "")
    manager = ClickHouseManager(
        f"clickhouse://default:{pwd}@{host}:8123/{db}",
        data_start=None,
        native_uri=f"clickhouse+native://default:{pwd}@{host}:9000/{db}?tcp_keepalive=true",
    )

    print(f"report={report} → {db}.{table}；报告期 {len(periods)} 个，"
          f"本块 offset={off} limit={lim} ⇒ {len(chunk)} 期 "
          f"[{chunk[0]} ~ {chunk[-1]}]；execute={args.execute}", flush=True)

    ok, empty, failed, rows_total = [], [], [], 0
    t0 = time.time()
    for i, rd in enumerate(chunk, 1):
        try:
            df = proxy.fetch(rd)
        except Exception as e:                                            # noqa: BLE001
            failed.append(f"{rd}({type(e).__name__})")
            print(f"  ❌ {rd}: {type(e).__name__}: {str(e)[:200]}", flush=True)
            continue
        if df is None or len(df) == 0:
            empty.append(rd)
            print(f"  ·  {rd}: 无数据（跳过）", flush=True)
            continue

        # 不变量：公告日既是 datetime 也是 publish_date
        bad = (df["datetime"] != df["publish_date"]).sum()
        assert bad == 0, f"{rd}: datetime 与 publish_date 不等的有 {bad} 行"
        assert df["datetime"].notna().all(), f"{rd}: 有缺失公告日"
        assert df["report_date"].notna().all(), f"{rd}: 有缺失报告期"
        assert df["code"].notna().all() and df["name"].notna().all(), f"{rd}: code/name 有缺失"

        dup = df.duplicated(subset=["datetime", "code", "name"]).sum()
        if dup:
            print(f"  ⚠️ {rd}: 主键 (datetime,code,name) 重复 {dup} 行，已去重", flush=True)
            df = df.drop_duplicates(subset=["datetime", "code", "name"])

        if args.execute:
            manager.save_dataframe(table, df)
        rows_total += len(df)
        ok.append((rd, len(df), int(df["code"].nunique()),
                   int(df["name"].nunique())))
        print(f"  ✅ {rd}: {len(df):>7,} 行 / {df['code'].nunique():>5,} 只 / "
              f"{df['name'].nunique():>6,} 个股东  [{i}/{len(chunk)}] {time.time()-t0:.0f}s",
              flush=True)

    print(f"\n=== 本块小结（{'已写库' if args.execute else 'dry-run，未写库'}）===")
    print(f"  成功 {len(ok)} 期 / 空 {len(empty)} / 失败 {len(failed)} ｜ 行数 {rows_total:,}"
          f" ｜ 耗时 {time.time()-t0:.0f}s")
    if failed:
        print(f"  失败清单: {failed}")

    res_path = os.environ.get("CHUNK_RESULT")
    if res_path:
        from pathlib import Path
        Path(res_path).write_text(json.dumps(
            {"report": report, "offset": off, "limit": lim, "ok": ok, "empty": empty,
             "failed": failed, "rows": rows_total, "executed": bool(args.execute),
             "finished_at": datetime.now().isoformat(timespec="seconds")},
            ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"  块结果 → {res_path}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
