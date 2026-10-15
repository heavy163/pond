"""商品期货【主力连续】日线同步（新浪源 → quant.cn_futures_kline_1d）。

表 cn_futures_kline_1d 由 ORM 定义（本仓库新增），首次运行自动建表。
符号清单：akshare.futures_display_main_sina()，约 82 个主力连续（RB0/I0/MA0…）。

⚠️ 主力连续在换月处有跳空，回测需另行按换月规则拼接，不能直接当连续价格用。
⚠️ 新浪接口不支持「按 symbol 增量」，每次调用都受 start_date/end_date 限制，
   故本脚本按 sync_data_start → 今日 拉取；日常增量重跑即可（ReplacingMergeTree 去重）。
⚠️ baostock/新浪均为外部 HTTP 源 ⇒ workers=1 以保证稳定与限速合规。

用法：
    PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \
        pond/clickhouse/scripts/load_akshare_futures_daily_to_db.py
"""

import os
from datetime import datetime

from loguru import logger

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.stock_helper import StockHelper
from pond.clickhouse.data_proxy.akshare import AKShareFuturesMainDataProxy
from pond.enums import Interval, Adjust, Product

sync_start = datetime(2015, 1, 1)
sync_end = datetime.now()

host = os.environ.get("CLICKHOUSE_HOST", "192.168.1.200")
database = os.environ.get("CLICKHOUSE_DB", "quant")
password: str = os.environ.get("CLICKHOUSE_PWD", "")

conn_str = f"clickhouse://default:{password}@{host}:8123/{database}"
native_conn_str = (
    f"clickhouse+native://default:{password}@{host}:9000/{database}?tcp_keepalive=true"
)

manager = ClickHouseManager(conn_str, data_start=sync_start, native_uri=native_conn_str)

helper = StockHelper(manager)
helper.sync_data_start = sync_start
helper.fix_data = False

data_proxy = AKShareFuturesMainDataProxy()
data_proxy.min_sync_interval_days = 0
data_proxy.min_start_date = sync_start
logger.info(f"cn futures main symbols: {len(data_proxy.get_symobls())}")
helper.set_data_proxy(data_proxy)

logger.info(f"cn futures daily sync {sync_start.date()} → {sync_end.date()}")
ok = helper.sync_kline(
    interval=Interval.DAY_1,
    adjust=Adjust.NFQ,
    product=Product.FUTURES,
    workers=1,
    end_time=sync_end,
)
logger.info(f"cn futures daily sync finished, ok={ok}")
