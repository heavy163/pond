"""回补【已退市/长期停牌】A 股日线 → quant.baostock_kline_1d_hfq。

为什么不能复用 `load_baostock_stock_daily_to_db.py`：

1. `BaostockDataProxy.get_symobls()` 只返回【当前在市】标的（来源
   `query_all_stock(day=...)`）⇒ 结构上拿不到退市股。实测该表 2015-01-06 起 6,949
   只，2015 年出现过的 2,596 只里到 2024 年只消失 1 只（sh.600005），
   而机构持仓源同口径量出 **250 只**消失 —— 严重幸存者偏差。
2. `StockHelper.sync_kline` 收尾有个尾部完整性阈值
   `lastest_count / request_count >= 0.98`（用 signal 前 2 天窗口计数）。
   退市股当天必然 0 行 ⇒ 该检查**恒 False**，会把「回补成功」报成失败。

本脚本复用同一套 proxy 取数逻辑（含 baostock error_code 检查），写入由自己控制。

⚠️ 写入路径必须与 `StockHelper.__sync_kline` 完全一致（同一张表、同一列集合、
   同一 datetime 语义），否则回补的数据与既有数据不是同一个口径。

用法（默认只读 dry-run；配合 run_chunked_backfill.py 分块执行）：
    SYMBOL_FILE=<gap.json> SYMBOL_OFFSET=0 SYMBOL_LIMIT=200 \
    PYTHONPATH=<pond> /usr/bin/python3.12 \
        pond/clickhouse/scripts/load_baostock_delisted_daily_to_db.py [--execute]
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from loguru import logger

from pond.clickhouse.data_proxy.baostock import BaostockDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.enums import Adjust, Interval

# 行情表起点前一档：确保退市股能拿到它全部历史（它可能 2005 年就上市）
SYNC_START = datetime(2015, 1, 5)
TABLE = "baostock_kline_1d_hfq"
RELOGIN_EVERY = 300          # baostock 模块级连接长跑会退化，周期性重登


class BaostockExplicitSymbolsProxy(BaostockDataProxy):
    """用显式 symbol 清单替换 get_symobls()（绕开「只返回在市标的」）。"""

    def __init__(self, symbols: list[str], sync_stock_list_date: datetime) -> None:
        self._explicit = list(symbols)
        super().__init__(sync_stock_list_date=sync_stock_list_date)

    def get_symobls(self) -> list[str]:
        return list(self._explicit)


def load_symbols(path: str, key: str) -> list[str]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(obj, dict):
        if key not in obj:
            raise SystemExit(f"SYMBOL_FILE 里没有键 {key!r}，可选: {list(obj)[:20]}")
        obj = obj[key]
    if not isinstance(obj, list):
        raise SystemExit("SYMBOL_FILE 必须解析成 list，或 dict[key]=list")
    return [str(s) for s in obj]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="真正写库（默认只读）")
    args = ap.parse_args()
    # run_chunked_backfill.py 不支持给子脚本透传额外 CLI 参数，
    # 所以额外支持 EXECUTE=1 环境变量（env 会随分块驱动下发）。
    if os.environ.get("EXECUTE") == "1":
        args.execute = True

    sym_file = os.environ.get("SYMBOL_FILE")
    if not sym_file:
        raise SystemExit("必须设置 SYMBOL_FILE 环境变量")
    symbols = load_symbols(sym_file, os.environ.get("SYMBOL_KEY", "missing_all"))

    off = int(os.environ.get("SYMBOL_OFFSET", 0))
    lim = int(os.environ.get("SYMBOL_LIMIT", len(symbols) or 1))
    chunk = symbols[off:off + lim]
    if not chunk:
        print(f"SYMBOL_OFFSET={off} 已越过清单长度 {len(symbols)}，无事可做")
        return 0

    host = os.environ.get("CLICKHOUSE_HOST", "192.168.1.200")
    db = os.environ.get("CLICKHOUSE_DB", "quant")
    pwd = os.environ.get("CLICKHOUSE_PWD", "")
    conn = f"clickhouse://default:{pwd}@{host}:8123/{db}"
    native = f"clickhouse+native://default:{pwd}@{host}:9000/{db}?tcp_keepalive=true"

    end = datetime.now()
    print(f"清单 {sym_file} 共 {len(symbols)} 只；本块 offset={off} limit={lim} "
          f"⇒ {len(chunk)} 只；区间 {SYNC_START.date()}→{end.date()}；"
          f"execute={args.execute}")

    manager = ClickHouseManager(conn, data_start=SYNC_START, native_uri=native)
    proxy = BaostockExplicitSymbolsProxy(chunk, sync_stock_list_date=end)
    proxy.min_sync_interval_days = 0
    proxy.min_start_date = SYNC_START

    ok, empty, failed, rows_total = [], [], [], 0
    t0 = time.time()
    for i, sym in enumerate(chunk, 1):
        if i > 1 and (i - 1) % RELOGIN_EVERY == 0:
            logger.info(f"周期性重登 baostock（已处理 {i-1} 只）")
            proxy.reset_connection()
        try:
            df = proxy.get_klines(symbol=sym, period=Interval.DAY_1, adjust=Adjust.HFQ,
                                  start=SYNC_START, end=end)
        except Exception as e:                                          # noqa: BLE001
            failed.append(f"{sym}({type(e).__name__})")
            logger.warning(f"取数失败 {sym}: {type(e).__name__}: {str(e)[:160]}")
            continue
        if df is None or len(df) == 0:
            empty.append(sym)
            continue
        df = df.drop_duplicates(subset=["datetime"])
        df["code"] = sym
        if "turn" not in df.columns:
            df["turn"] = None
        if args.execute:
            manager.save_dataframe(TABLE, df)
        rows_total += len(df)
        ok.append((sym, len(df), str(df["datetime"].min().date()),
                   str(df["datetime"].max().date())))
        if i % 50 == 0 or i == len(chunk):
            print(f"  [{i}/{len(chunk)}] ok={len(ok)} empty={len(empty)} "
                  f"failed={len(failed)} rows={rows_total:,} "
                  f"{time.time()-t0:.0f}s")

    print(f"\n=== 本块小结（{'已写库' if args.execute else 'dry-run，未写库'}）===")
    print(f"  成功 {len(ok)} / 空 {len(empty)} / 失败 {len(failed)} ｜ 行数 {rows_total:,}"
          f" ｜ 耗时 {time.time()-t0:.0f}s")
    if ok[:5]:
        print("  样例（symbol, 行数, 起, 止）:")
        for r in ok[:5]:
            print(f"    {r}")
    if empty:
        print(f"  空结果 {len(empty)} 只（退市久远/源无数据）: {empty[:10]}")
    if failed:
        print(f"  失败 {len(failed)} 只: {failed[:10]}")

    res_path = os.environ.get("CHUNK_RESULT")
    if res_path:
        Path(res_path).write_text(json.dumps(
            {"offset": off, "limit": lim, "ok": len(ok), "empty": len(empty),
             "failed": failed, "rows": rows_total, "detail": ok,
             "executed": bool(args.execute)}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        print(f"  块结果 → {res_path}")

    # 失败块要能让外部看见，否则 run_chunked_backfill 的调用方只看到 rc=0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
