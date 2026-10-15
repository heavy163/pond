"""A股日线增量同步（baostock 源 → quant.baostock_kline_1d_hfq）。

背景：该表已有 2015-01-06 起、6796 只、约 1399 万行，但停在 2026-06-29。
本脚本只做【增量接续】。

⚠️ 关键：StockHelper.__sync_kline 只在「signal 前 30 天」窗口内找每只票的最后一条记录。
现有数据停在 3.5 个月前，落在窗口外 ⇒ 会回落到 sync_data_start。
所以 sync_data_start 必须显式设为断点日期 2026-06-29（BaostockDataProxy.get_klines
内部会 +1 天，故实际从 06-30 开始取），否则会从 2015 年重拉。
日常（每天都跑）增量则默认行为即正确，不需要改这里。

用法：
    PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \
        pond/clickhouse/scripts/load_baostock_stock_daily_to_db.py
"""

import os
import sys
from datetime import datetime

from loguru import logger

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.stock_helper import StockHelper
from pond.clickhouse.data_proxy.baostock import BaostockDataProxy
from pond.enums import Interval, Adjust, Product

# 现有数据的断点
sync_start = datetime(2026, 6, 29)
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

# baostock 是模块级全局连接，非线程安全 ⇒ workers=1（与既有脚本一致）
data_proxy = BaostockDataProxy(sync_stock_list_date=sync_end)
data_proxy.min_sync_interval_days = 0
data_proxy.min_start_date = sync_start
helper.set_data_proxy(data_proxy)

logger.info(f"stock daily incremental sync {sync_start.date()} → {sync_end.date()}")
ok = helper.sync_kline(
    interval=Interval.DAY_1,
    adjust=Adjust.HFQ,
    product=Product.STOCK,
    workers=1,
    end_time=sync_end,
)
logger.info(f"stock daily incremental sync finished, ok={ok}")
# ok=False 说明增量未达完整性阈值（缺口/数据源异常）。必须让外部能看见，
# 否则 systemd-run / 调用方只会看到 rc=0，把失败当成功。
sys.exit(0 if ok else 1)
