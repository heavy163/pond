"""
真实 ClickHouse 数据同步测试。
使用 quant_test 数据库，仅同步今天 0:00 开始的数据。
参考 pond/clickhouse/helper.py 的 __main__ 入口写法。

注意事项:
  - __sync_futures_kline / __sync_futures_funding_rate 内部使用 asyncio.run()
    在工作线程创建新事件循环, 而 AsyncDirectDataProxy 的 aiohttp session
    在主线程创建, 导致 "Future attached to a different loop" 错误。
    这是代码现有问题, 跨线程 asyncio 不兼容。
  - 因此 kline/funding_rate 的异步 worker 会失败, 但 funding_rate 仍可通过
    数据写入 (save_to_db 在 res_dict[tid]=True 之前执行) 向 ClickHouse 写数据。
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

from loguru import logger

os.environ.setdefault("BINANCE_API_KEY", "")
os.environ.setdefault("BINANCE_API_SECRET", "")
os.environ.setdefault("CHAIN_BASE_API_KEY", "")

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.helper import FuturesHelper, DataProxy, DirectDataProxy
from pond.duckdb.crypto import CryptoDB
from pond.utils.times import timeframe2minutes
from binance.um_futures import UMFutures as UMFuturesClient


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CLICKHOUSE_HOST = "192.168.1.200"
CLICKHOUSE_DB = "quant_test"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""  # 无密码

def _make_conn_str(http_port=8123, native_port=9000):
    if CLICKHOUSE_PASSWORD:
        pw_part = f":{CLICKHOUSE_PASSWORD}"
    else:
        pw_part = ":"
    http = (
        f"clickhouse://{CLICKHOUSE_USER}{pw_part}"
        f"@{CLICKHOUSE_HOST}:{http_port}/{CLICKHOUSE_DB}"
    )
    native = (
        f"clickhouse+native://{CLICKHOUSE_USER}{pw_part}"
        f"@{CLICKHOUSE_HOST}:{native_port}/{CLICKHOUSE_DB}?tcp_keepalive=true"
    )
    return http, native

SYNC_TYPES = ["kline", "funding_rate"]
INTERVAL = "1h"
# data_start = 今天 00:00 UTC（注意：必须用 UTC 时间，不是本地时间），避免拉取过多历史数据
TODAY_UTC_0 = datetime.now(tz=timezone.utc).replace(
    hour=0, minute=0, second=0, microsecond=0
).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# 准备 ClickHouse 连接
# ---------------------------------------------------------------------------
logger.info(f"Connecting to ClickHouse {CLICKHOUSE_HOST}, database={CLICKHOUSE_DB}")

conn_str, native_conn_str = _make_conn_str()

try:
    manager = ClickHouseManager(
        conn_str,
        data_start=TODAY_UTC_0,  # data_start 控制拉取起点 = 今天 00:00
        native_uri=native_conn_str,
    )
    result = manager.native_sql_read_table("SELECT 1 AS t")
    logger.success(f"ClickHouse connected, test result: {result['t'].iloc[0]}")
except Exception as e:
    logger.error(f"ClickHouse connection failed: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 准备 CryptoDB / FuturesHelper
# ---------------------------------------------------------------------------
DUCKDB_PATH = Path("/tmp/duckdb_test_pond")
DUCKDB_PATH.mkdir(parents=True, exist_ok=True)
crypto_db = CryptoDB(DUCKDB_PATH)

helper = FuturesHelper(crypto_db, manager, fix_kline_with_cryptodb=False)

class NoProxyDirectDataProxy(DirectDataProxy):
    def __init__(self) -> None:
        self.exchange = UMFuturesClient(timeout=30)

helper.data_proxy = NoProxyDirectDataProxy()

symbols = helper.get_perpetual_symbols(datetime.now())
logger.info(f"Perpetual symbols: {len(symbols) if symbols else 0}")

# ---------------------------------------------------------------------------
# 同步数据
# ---------------------------------------------------------------------------
# 不传 end_time, signal 默认为 datetime.now(), 验证窗口能正确命中写入数据
# data_start = 今天 00:00 控制 Binance API 拉取起点, 避免历史数据
results = {}

for what in SYNC_TYPES:
    workers = 2
    allow_missing = 50 if what not in ("kline", "funding_rate") else 0

    logger.info(f"--- Syncing {what} interval={INTERVAL} ---")

    try:
        ret = helper.sync(INTERVAL, workers=workers,
                          what=what, allow_missing_count=allow_missing)
        results[what] = ret
        prefix = "SUCCEEDED" if ret else "FAILED"
        logger.warning(f"sync {what} {prefix}")
    except Exception as e:
        results[what] = False
        logger.error(f"sync {what} threw: {e}")
        import traceback
        traceback.print_exc()

# ---------------------------------------------------------------------------
# 验证写入
# ---------------------------------------------------------------------------
logger.info("=== Verification ===")

tables = manager.native_sql_read_table(
    f"SELECT name FROM system.tables WHERE database = '{CLICKHOUSE_DB}'"
)
if tables is not None and not tables.empty:
    for tbl in tables["name"].tolist():
        try:
            cnt = manager.native_sql_read_table(
                f"SELECT count(*) AS c FROM {CLICKHOUSE_DB}.{tbl}"
            )
            if cnt is not None and not cnt.empty and cnt["c"].iloc[0] > 0:
                logger.success(f"  {tbl}: {cnt['c'].iloc[0]} rows  ← DATA WRITTEN")
        except Exception:
            pass

# ---------------------------------------------------------------------------
# 清理: 删除测试数据, 关闭 async session
# ---------------------------------------------------------------------------
logger.info("Cleaning up test data...")
manager.native_sql_read_table(
    f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.future_funding_rate"
)
try:
    import asyncio
    asyncio.run(helper.async_data_proxy.close())
except Exception:
    pass
logger.success("Test data cleaned up")

# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------
print()
print("=" * 60)
print("真实数据同步测试完成")
print(f"  ClickHouse:   {CLICKHOUSE_HOST} / {CLICKHOUSE_DB}")
print(f"  data_start:   {TODAY_UTC_0}  (控制拉取起点)")
print(f"  signal:       datetime.now() (默认, 验证窗口正确)")
print(f"  Kline:        {'OK' if results.get('kline') else 'FAIL'}")
print(f"  Funding Rate: {'OK' if results.get('funding_rate') else 'FAIL'}")
print("=" * 60)
