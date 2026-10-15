import os
import math
import time
from datetime import datetime
from datetime import timedelta
import threading
from concurrent.futures import ThreadPoolExecutor

from loguru import logger
import polars as pl
from pond.clickhouse import TsTable
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.data_proxy.tdx import TdxDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.enums import Interval, Adjust, Product
from pond.utils.times import (
    datetime2utctimestamp_milli,
    timeframe2minutes,
)

# 数据查询超时时间（秒）
DEFAULT_QUERY_TIMEOUT = 60

# 全局共享的超时查询线程池（单例）
_timeout_executor = None
_timeout_executor_lock = threading.Lock()


def get_timeout_executor(max_workers: int = 4) -> ThreadPoolExecutor:
    """获取共享的超时查询线程池（单例模式）"""
    global _timeout_executor
    with _timeout_executor_lock:
        if _timeout_executor is None or _timeout_executor._shutdown:
            _timeout_executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="timeout_query_worker"
            )
    return _timeout_executor


class StockHelper:
    clickhouse: ClickHouseManager = None
    data_proxy: DataProxy = None
    fix_data: bool = False
    sync_data_start: datetime = None

    def __init__(
        self, clickhouse: ClickHouseManager, tdx_path: str = None, **kwargs
    ) -> None:
        self.clickhouse = clickhouse
        self.configs = kwargs
        if tdx_path is not None:
            self.data_proxy = TdxDataProxy(tdx_path)

    def set_data_proxy(self, data_proxy: DataProxy):
        self.data_proxy = data_proxy

    def gen_stub_kline_as_list(self, start: datetime, end: datetime):
        open_time_mill = datetime2utctimestamp_milli(start)
        close_time_mill = datetime2utctimestamp_milli(end)
        stub_list = [
            open_time_mill,
            0,
            0,
            0,
            0,
            0,
            close_time_mill,
            0,
            0,
            0,
            0,
        ]
        return stub_list

    def get_synced_codes(
        self,
        table: TsTable,
        code_col: str = "code",
        time_col: str = "datetime",
        sync_time: datetime | None = None,
    ):
        if sync_time is None:
            sync_time = self.clickhouse.get_latest_record_time(table)
        latest_records_df = self.clickhouse.native_read_table(
            table,
            start_date=sync_time - timedelta(seconds=1),
            end_date=sync_time,
        )
        latest_synced_codes = (
            [] if latest_records_df is None else latest_records_df[code_col].unique()
        )
        return latest_synced_codes

    def sync_kline(
        self,
        interval: Interval,
        adjust: Adjust,
        product=Product.STOCK,
        workers: int = 0,
        end_time: datetime | None = None,
        time_col: str = "datetime",
        query_timeout: int = DEFAULT_QUERY_TIMEOUT,
    ) -> bool:
        table = self.data_proxy.get_table(interval, adjust, product)
        if table is None:
            return False

        signal = end_time if end_time is not None else datetime.now()
        signal = signal.replace(hour=15, minute=0, second=0, microsecond=0)
        workers = math.ceil(os.cpu_count() / 2) if workers <= 0 else workers

        symbols = self.data_proxy.get_symobls()

        if symbols is None or len(symbols) == 0:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} at {end_time} failed, symbols is empty."
            )
            return False
        if self.fix_data:
            latest_synced_codes = self.get_synced_codes(table, sync_time=signal)
            symbols = [s for s in symbols if s not in latest_synced_codes]
        task_counts = math.ceil(len(symbols) / workers)
        res_dict = {}
        threads: list[threading.Thread] = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = threading.Thread(
                target=self.__sync_kline,
                args=(
                    signal,
                    table,
                    worker_symbols,
                    interval,
                    adjust,
                    res_dict,
                    query_timeout,
                ),
            )
            worker.start()
            threads.append(worker)

        [t.join() for t in threads]

        for tid in res_dict.keys():
            if not res_dict[tid]:
                # return false if any thread failed.
                return False

        request_count = len(symbols)
        latest_kline_df = self.clickhouse.native_read_table(
            table=table,
            start_date=signal - timedelta(minutes=timeframe2minutes(interval) * 2),
            end_date=signal,
            filters=None,
            # 注意：这里必须 rename=False。rename=True 会把列名换成 ORM 的 comment，
            # 对中文注释的表（FundKlineDailyHFQ 等）会得到「日期」而非 datetime，
            # 下面的 group_by(time_col) 就会抛 ColumnNotFoundError。
            # 该 df 仅用于计数，用原始列名即可。
            rename=False,
        )
        if latest_kline_df is None or len(latest_kline_df) == 0:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} failed, latest kline df is empty."
            )
            return False
        latest_kline_df = pl.from_pandas(latest_kline_df)
        lastest_count = (
            latest_kline_df.group_by(time_col).count().sort(time_col)[-1, "count"]
        )
        if lastest_count / request_count < 0.98:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} failed, lastest count {lastest_count} request count {request_count}"
            )
            return False
        return True

    def _get_klines_with_timeout(
        self,
        symbol: str,
        interval: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
        timeout: int = DEFAULT_QUERY_TIMEOUT,
    ):
        """带超时的k线数据查询（使用共享线程池）"""

        def query_func():
            return self.data_proxy.get_klines(
                symbol=symbol,
                period=interval,
                adjust=adjust,
                start=start,
                end=end,
                limit=limit,
            )

        # 使用共享的线程池，避免每次创建新线程
        executor = get_timeout_executor()
        future = executor.submit(query_func)
        return future.result(timeout=timeout)

    def _reset_proxy_connection(self) -> None:
        """
        让数据源重建底层会话（若支持）。baostock 全局连接长跑会退化，
        表现为后半程每次请求必超时；其余源无此方法则静默跳过。
        """
        reset = getattr(self.data_proxy, "reset_connection", None)
        if callable(reset):
            reset()

    def __sync_kline(
        self,
        signal: datetime,
        table: TsTable,
        symbols: list[str],
        interval: Interval,
        adjust: Adjust,
        res_dict: dict[int, bool],
        timeout: int = DEFAULT_QUERY_TIMEOUT,
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        failed_symbols: list[str] = []
        # baostock 全局连接长跑会退化，且退化点随「响应体积」前移：
        #  · A股 72 天窗口（小响应）：约 3800~4200 次请求后整体超时
        #  · ETF 全历史（2800+ 行大响应）：约 527 次请求 / 14 分钟后就整体失败
        # 退化表现为 error_code=10002007「网络接收错误」（已被 get_klines 显式抛出）。
        # 三种防线同时上：按条数、按时间、按连续错误。重建会话仅耗时 ~1s，宁多勿少。
        relogin_every = 300
        relogin_interval_seconds = 480
        last_relogin = time.time()
        timeout_streak = 0
        timeout_streak_limit = 3

        if signal is None:
            signal = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)

        latest_klines_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        latest_klines_df = pl.from_pandas(latest_klines_df)

        for i in range(0, len(symbols)):
            if i > 0 and (
                i % relogin_every == 0
                or time.time() - last_relogin > relogin_interval_seconds
            ):
                # 周期性重建会话，避免 baostock 跑到后半程整体退化（见 reset_connection）
                logger.info(
                    f"已处理 {i} 只（距上次重建会话 {time.time() - last_relogin:.0f}s），"
                    f"周期性重建源会话"
                )
                self._reset_proxy_connection()
                last_relogin = time.time()
            logger.debug(
                f"stock helper sync kline counting {i}/{len(symbols)} for {signal}"
            )
            symbol = symbols[i]
            time.sleep(0.1)
            latest_record = latest_klines_df.filter(pl.col("code") == symbol)
            lastest_record = (
                latest_record[0, "datetime"]
                if len(latest_record) > 0
                else self.sync_data_start
            )
            if lastest_record.date() == signal.date():
                logger.debug(
                    f"stock helper sync kline ignore too short duration {lastest_record}-{signal}"
                )
                continue

            def _fetch(sym: str, start_: datetime, end_: datetime):
                return self._get_klines_with_timeout(
                    symbol=sym,
                    interval=interval,
                    adjust=adjust,
                    start=start_,
                    end=end_,
                    limit=1000,
                    timeout=timeout,
                )

            # 带超时的查询
            # ⚠️ 单只标的超时/连接抖动不得中断整轮同步：baostock 偶发 hang 至 60s+
            # （实测 sz.002074 卡 62.6s），异常若逃逸会毁掉整轮任务。
            try:
                klines_df = _fetch(symbol, lastest_record, signal)
                timeout_streak = 0
            except Exception as e:
                timeout_streak += 1
                failed_symbols.append(f"{symbol}({type(e).__name__})")
                logger.warning(
                    f"stock helper sync kline {symbol} skipped by {type(e).__name__}: {e}"
                )
                if timeout_streak < timeout_streak_limit:
                    continue
                # 连续超时 = 会话已退化。重建连接后【重试本只】，
                # 否则后半程会以 60s/只 的速度空转到收尾。
                logger.warning(
                    f"连续 {timeout_streak} 次超时，重建源会话并重试 {symbol}"
                )
                self._reset_proxy_connection()
                timeout_streak = 0
                try:
                    klines_df = _fetch(symbol, lastest_record, signal)
                except Exception as e2:
                    logger.warning(
                        f"stock helper sync kline {symbol} 重登后仍失败: "
                        f"{type(e2).__name__}: {e2}"
                    )
                    continue

            if klines_df is not None and len(klines_df) > 0:
                klines_df = klines_df[klines_df["datetime"] > lastest_record]
                klines_df = klines_df[klines_df["datetime"] <= signal]
                klines_df["code"] = symbol
                if "turn" not in klines_df.columns:
                    klines_df["turn"] = None
                klines_df = klines_df.rename(
                    mapper={"date": "datetime"}, axis="columns"
                )
                klines_df = klines_df.drop_duplicates(subset=["datetime"])
                self.clickhouse.save_dataframe(table.__tablename__, klines_df)

        if failed_symbols:
            logger.warning(
                f"stock helper sync kline {table.__tablename__} finished with "
                f"{len(failed_symbols)}/{len(symbols)} symbols skipped: {failed_symbols[:30]}"
            )
        res_dict[tid] = True


if __name__ == "__main__":
    import os
    from pond.clickhouse.data_proxy.baostock import BaostockDataProxy

    tdx_path = None  # r"D:\windows\programs\TongDaXin"
    host = os.environ.get("CLICKHOUSE_HOST").strip()
    password = os.environ.get("CLICKHOUSE_PWD").strip()
    conn_str = f"clickhouse://default:{password}@{host}:8123/quant"
    native_conn_str = (
        f"clickhouse+native://default:{password}@{host}:9000/quant?tcp_keepalive=true"
    )
    # 此处有天坑，原因是同步机制中增加了start-end限制，避免接口返回异常，如果部分标的再start附近没有数据，start就不会更新，再次查询还是同样的start-end,就下载不了数据。
    # 没想到好的办法解决，在以这个时间为起始时间全部同步完之后，按每次增加1年再同步一遍。
    sync_start = datetime(2020, 1, 5)
    manager = ClickHouseManager(
        conn_str, data_start=sync_start, native_uri=native_conn_str
    )
    helper = StockHelper(manager, tdx_path=tdx_path)
    helper.fix_data = True
    helper.sync_data_start = sync_start
    ret = False
    sync_end = datetime.now()
    sync_end = datetime(2026, 2, 13)
    logger.info(f"sync at {sync_start} start")
    data_proxy = BaostockDataProxy(sync_stock_list_date=sync_end)
    data_proxy.min_sync_interval_days = 0
    data_proxy.min_start_date = helper.sync_data_start
    helper.set_data_proxy(data_proxy)
    while sync_end > datetime(2020, 1, 1):
        data_proxy.sync_stock_list_date = sync_end
        ret = helper.sync_kline(
            interval=Interval.DAY_1,
            adjust=Adjust.HFQ,
            workers=1,
            end_time=sync_end,
        )
        sync_end -= timedelta(days=1)
    logger.info(f"sync at {sync_start} end")
