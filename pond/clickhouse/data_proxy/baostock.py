from datetime import datetime, timedelta

from loguru import logger
import pandas as pd
import baostock as bs
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import (
    BaoStockKline5m,
    BaoStockKline1dHfq,
    FundKlineDailyHFQ,
)
from pond.enums import Adjust, Interval, Product
from pond.utils.times import timeit_cls_method_wrapper


def query_all_stock_with_lookback(
    day: datetime, max_lookback_days: int = 15
) -> pd.DataFrame:
    """
    query_all_stock 在「该日数据尚未生成」时返回空表（典型：当天盘中查当天，
    或查询未开盘日），此时 get_symobls 返回空 → sync_kline 直接 `symbols is empty` 失败。
    这里向前回溯到最近一个有数据的日期，返回带 code/tradeStatus/code_name 的 DataFrame。
    """
    for i in range(max_lookback_days):
        d = (day - timedelta(days=i)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        rs = bs.query_all_stock(day=d.strftime("%Y-%m-%d"))
        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())
        if data_list:
            if i > 0:
                logger.info(
                    f"query_all_stock: {day.date()} 返回空 → 回溯到 {d.date()}（{len(data_list)} 条）"
                )
            return pd.DataFrame(data_list, columns=rs.fields)
    logger.warning(
        f"query_all_stock: 回溯 {max_lookback_days} 天仍无数据（起始 {day.date()}）"
    )
    return pd.DataFrame(columns=["code", "tradeStatus", "code_name"])


class BaostockDataProxy(DataProxy):
    sync_stock_list_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None

    def __init__(self, sync_stock_list_date: datetime) -> None:
        super().__init__()
        self.sync_stock_list_date = sync_stock_list_date
        lg = bs.login()
        logger.info("login respond error_code:" + lg.error_code)
        logger.info("login respond  error_msg:" + lg.error_msg)

    def reset_connection(self) -> None:
        """
        重建 baostock 会话。

        ⚠️ 必须有：baostock 用的是**模块级全局连接**，长跑后会退化 ——
        实测约 4000 次 query_history_k_data_plus 之后每次请求都卡满超时（60s），
        表现为大批 TimeoutError（2026-10-14 两次实跑分别在 3864 / ~4200 只处复现）。
        重新 login 即可恢复，因此长跑必须周期性调用本方法
        （见 StockHelper.__sync_kline 的 RELOGIN_EVERY）。
        """
        try:
            bs.logout()
        except Exception as e:
            # 已断开/未登录时 logout 会报错，不影响后续 login
            logger.warning(f"baostock logout before relogin failed: {e}")
        lg = bs.login()
        logger.info(
            f"baostock re-login error_code:{lg.error_code} msg:{lg.error_msg}"
        )

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> BaoStockKline5m:
        return {
            (Interval.MINUTE_5, Adjust.NFQ, Product.STOCK): BaoStockKline5m,
            (Interval.DAY_1, Adjust.HFQ, Product.STOCK): BaoStockKline1dHfq,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        stocks_df = query_all_stock_with_lookback(self.sync_stock_list_date)
        stocks_df = stocks_df[~stocks_df["code_name"].str.contains("指数", na=False)]
        stocks_df = stocks_df[~stocks_df["code"].str.startswith("sh.00")]
        stocks_df = stocks_df[~stocks_df["code"].str.startswith("sz.39")]
        return stocks_df["code"].to_list()

    def get_frequency(self, interval: Interval) -> str:
        if interval == Interval.MINUTE_5:
            return "5"
        elif interval == Interval.MINUTE_15:
            return "15"
        elif interval == Interval.MINUTE_30:
            return "30"
        elif interval == Interval.HOUR_1:
            return "60"
        elif interval == Interval.DAY_1:
            return "d"
        elif interval == Interval.WEEK_1:
            return "w"
        elif interval == Interval.MONTH_1:
            return "m"
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    def get_fields(self, interval: Interval) -> str:
        common_fields = (
            "date, time, code, open, high, low, close, volume, amount, adjustflag"
        )
        if interval in [
            Interval.MINUTE_5,
            Interval.MINUTE_15,
            Interval.MINUTE_30,
            Interval.HOUR_1,
        ]:
            return common_fields
        elif interval in [Interval.WEEK_1, Interval.MONTH_1]:
            return common_fields + ", turn, pctChg"
        elif interval == Interval.DAY_1:
            return (
                common_fields[0:5]
                + common_fields[11:]
                + ", turn, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST"
            )

    @timeit_cls_method_wrapper
    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        # baostock always return data include start day but already existed.
        start = start.replace(hour=0) + timedelta(days=1)
        if end is None:
            if period == Interval.MINUTE_5:
                end = start + timedelta(days=365)
            elif period == Interval.DAY_1:
                end = start + timedelta(days=365 * 50)
        if end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            logger.debug(
                f"get_klines {symbol} from {start} to {end} ignored by min sync interval days {self.min_sync_interval_days}"
            )
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            logger.debug(
                f"get_klines {symbol} from {start} to {end} ignored by min_start_date {self.min_start_date}"
            )
            return None
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")
        fields = self.get_fields(period)
        rs = bs.query_history_k_data_plus(
            symbol,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            frequency=self.get_frequency(period),
            adjustflag=adjust,
        )
        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        logger.debug(
            f"query_history_k_data_plus {symbol} from {start_date} to {end_date} data len {len(result)} respond error_code: {rs.error_code}, msg {rs.error_msg}"
        )
        # ⚠️ baostock 用 error_code 表达失败而不抛异常：实测 10002007「网络接收错误」
        # 会返回空结果集。不检查就会把「请求失败」当成「该标的无数据」→ 静默丢数
        # （2026-10-14 实测 1053/1594 只 ETF 被这样丢掉），
        # 且调用方的重试 / 重建会话逻辑永远不会被触发。必须显式抛出。
        if rs.error_code != "0":
            raise RuntimeError(
                f"baostock query_history_k_data_plus {symbol} "
                f"[{start_date}~{end_date}] failed: "
                f"error_code={rs.error_code} msg={rs.error_msg}"
            )
        if len(result) == 0:
            if end.date() < datetime.now().date():
                return self.get_klines(
                    symbol, period, adjust, end, end + timedelta(days=365)
                )
            else:
                return result
        if "time" in result.columns:
            result["time"] = result.apply(
                lambda row: datetime.strptime(row["time"][:-4], "%Y%m%d%H%M%S"), axis=1
            )
            result["datetime"] = pd.to_datetime(result["time"])
        elif "date" in result.columns:
            result["datetime"] = pd.to_datetime(result["date"])
        result = result.drop(
            columns=[
                col for col in ["date", "time", "adjustflag"] if col in result.columns
            ],
        )
        result = result.replace("", None).astype(
            {
                col: "float64"
                for col in result.columns
                if col not in ["code", "date", "time", "datetime"]
            }
        )
        return result


# ETF / 场内基金 的 baostock 代码前缀
ETF_CODE_PREFIXES = ("sh.51", "sh.56", "sh.58", "sz.15", "sz.16")


class BaoStockETFDataProxy(BaostockDataProxy):
    """
    ETF / 场内基金日线（baostock 源），落 fund_kline_daily_hfq。

    与 BaostockDataProxy 的差别只有两点：
      1. get_symobls 只取 ETF（按 code_name 含 "ETF" + 代码前缀，与 AKShareFundDataProxy 口径一致）
      2. get_fields 只取行情字段（ETF 无估值指标，避免整列 NaN 的无效拉取）
    其余（增量、复权、字段清洗）完全沿用父类。
    """

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.ETF
    ) -> FundKlineDailyHFQ:
        return {
            (Interval.DAY_1, Adjust.HFQ, Product.ETF): FundKlineDailyHFQ,
            (Interval.DAY_1, Adjust.HFQ, Product.FUND): FundKlineDailyHFQ,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        df = query_all_stock_with_lookback(self.sync_stock_list_date)
        df = df[df["code_name"].astype(str).str.contains("ETF")]
        df = df[df["code"].str.startswith(ETF_CODE_PREFIXES)]
        return df["code"].tolist()

    def get_fields(self, interval: Interval) -> str:
        if interval == Interval.DAY_1:
            return "date,code,open,high,low,close,volume,amount,turn,adjustflag"
        return super().get_fields(interval)
