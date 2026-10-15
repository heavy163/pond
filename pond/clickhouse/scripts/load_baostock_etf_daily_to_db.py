"""ETF / 场内基金 日线全量同步（baostock 源 → quant.fund_kline_daily_hfq）。

表 fund_kline_daily_hfq 由 ORM 定义、此前为空。
符号清单：baostock query_all_stock 中 code_name 含 "ETF" 且代码前缀属
sh.51/56/58、sz.15/16（与 AKShareFundDataProxy 的 ETF 口径一致）。

⚠️ baostock 模块级全局连接非线程安全 ⇒ workers=1。
⚠️ StockHelper.sync_kline 末尾的完整性校验要求 rename=False（该 df 只用于计数），
   否则中文注释的表会在 group_by("datetime") 处抛 ColumnNotFoundError。

用法：
    PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \
        pond/clickhouse/scripts/load_baostock_etf_daily_to_db.py
"""

import os
import sys
from datetime import datetime

from loguru import logger

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.stock_helper import StockHelper
from pond.clickhouse.data_proxy.baostock import BaoStockETFDataProxy
from pond.enums import Interval, Adjust, Product

sync_start = datetime(2015, 1, 5)
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


class _SlicedETFProxy(BaoStockETFDataProxy):
    """
    支持按 offset/limit 切片，供【分块回填】使用。

    为什么需要：baostock 的模块级 socket 在连续大量请求后会损坏
    （实测 0xf9 非法 UTF-8 → error_code 10002007 → 库内部状态坏死后全线程死锁），
    长驻进程跑几千次请求必然积累坏状态。唯一稳妥的做法是
    **每块启用独立进程**（socket 全新），配合外部 timeout + 失败重试，
    见 scripts/run_chunked_backfill.py。
    """

    def __init__(self, *args, offset: int = 0, limit: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._offset = offset
        self._limit = limit

    def get_symobls(self) -> list[str]:
        symbols = super().get_symobls()
        end = self._offset + self._limit if self._limit is not None else None
        sliced = symbols[self._offset : end]
        logger.info(
            f"ETF 切片 offset={self._offset} limit={self._limit} "
            f"→ {len(sliced)}/{len(symbols)} 只"
        )
        return sliced


_symbol_offset = int(os.environ.get("SYMBOL_OFFSET", "0"))
_symbol_limit = os.environ.get("SYMBOL_LIMIT", "")
data_proxy = _SlicedETFProxy(
    sync_stock_list_date=sync_end,
    offset=_symbol_offset,
    limit=int(_symbol_limit) if _symbol_limit else None,
)
logger.info(f"etf symbols: {len(data_proxy.get_symobls())}")
helper.set_data_proxy(data_proxy)

logger.info(f"etf daily sync {sync_start.date()} → {sync_end.date()}")
ok = helper.sync_kline(
    interval=Interval.DAY_1,
    adjust=Adjust.HFQ,
    product=Product.ETF,
    workers=1,
    end_time=sync_end,
)
logger.info(f"etf daily sync finished, ok={ok}")
sys.exit(0 if ok else 1)
