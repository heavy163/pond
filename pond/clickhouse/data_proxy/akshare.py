from datetime import datetime, timedelta

from loguru import logger
import pandas as pd
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import (
    FundKlineDailyHFQ,
    FundNetValue,
    CnFuturesKline1d,
    BondKlineDaily,
)
from pond.enums import Adjust, Interval, Product
from pond.utils.times import timeit_cls_method_wrapper
import akshare as ak


class AKShareFundDataProxy(DataProxy):
    sync_codes_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None

    def __init__(self) -> None:
        super().__init__()

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> FundKlineDailyHFQ:
        return {
            (Interval.DAY_1, Adjust.HFQ, Product.FUND): FundKlineDailyHFQ,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        df = ak.fund_name_em()
        df = df[df["基金简称"].str.contains("ETF")]
        return df["基金代码"].tolist()

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
        if end is None:
            end = start + timedelta(days=365 * 3)
        if end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        df = ak.fund_etf_hist_em(
            symbol=symbol,
            period=period.toAKshare(),
            start_date=start_date,
            end_date=end_date,
            adjust=adjust.toAKshare(),
        )
        if len(df) == 0:
            return df
        df["日期"] = pd.to_datetime(df["日期"])
        df["date"] = df["日期"]
        df["turn"] = df["换手率"]
        return df


class AKShareFundNetValueProxy(DataProxy):
    sync_codes_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None
    downloading_batch_days = 365 * 3

    def __init__(self) -> None:
        super().__init__()

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> FundNetValue:
        return {
            (Interval.DAY_1, Adjust.HFQ, Product.FUND): FundNetValue,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        df = ak.fund_name_em()
        df = df[df["基金简称"].str.contains("ETF")]
        return df["基金代码"].tolist()

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

    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        while start < datetime.now() - timedelta(days=self.min_sync_interval_days):
            logger.debug(f"get_klines start downloading for {symbol} at {start}")
            try:
                df = self.__get_klines(symbol, period, adjust, start, end, limit)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                start += timedelta(days=self.downloading_batch_days)
                if end is not None:
                    end += timedelta(days=self.downloading_batch_days)
                continue
            return df
        return None

    @timeit_cls_method_wrapper
    def __get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        if end is None:
            end = start + timedelta(days=self.downloading_batch_days)
        if end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        df = ak.fund_etf_fund_info_em(
            fund=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if len(df) == 0:
            return df
        df["date"] = pd.to_datetime(df["净值日期"])
        df["tradable"] = (df["申购状态"] == "开放申购") & (df["赎回状态"] == "开放赎回")
        return df


# 新浪 futures_main_sina 中文列 → 表英文列
_FUTURES_COL_MAP = {
    "开盘价": "open",
    "最高价": "high",
    "最低价": "low",
    "收盘价": "close",
    "成交量": "volume",
    "持仓量": "open_interest",
    "动态结算价": "settle",
}


class AKShareFuturesMainDataProxy(DataProxy):
    """
    国内商品期货【主力连续】日线（新浪源），落 cn_futures_kline_1d。

    ⚠️ 主力连续（RB0/I0/MA0…）在换月处有跳空，回测需另行按换月规则拼接。
    源不提供成交额，表也不设该列（不造空列）。
    """

    min_sync_interval_days = 1
    min_start_date = None

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.FUTURES
    ) -> CnFuturesKline1d:
        return {
            (Interval.DAY_1, Adjust.NFQ, Product.FUTURES): CnFuturesKline1d,
        }[(interval, adjust, product)]

    # 停摆判定窗口：近 N 天没有任何 bar 视为主力连续已停摆。
    # 例：JR0 粳稻 / LR0 晚籼稻 / RI0 早籼稻 / WH0 强麦，最后成交分别在
    # 2022-12-30 / 2022-06-29 / 2022-06-30 / 2023-02-28，新浪清单里仍挂着。
    # 这类品种会让 StockHelper 的完整性校验恒报 ok=False ——
    # 一个永远亮着的告警等于没有告警，会掩盖真实缺口，所以在此剔除。
    ACTIVE_LOOKBACK_DAYS = 90

    def get_symobls(self) -> list[str]:
        all_symbols = ak.futures_display_main_sina()["symbol"].tolist()
        end = datetime.now()
        start = end - timedelta(days=self.ACTIVE_LOOKBACK_DAYS)
        active: list[str] = []
        dormant: list[str] = []
        unknown: list[str] = []
        for s in all_symbols:
            try:
                k = ak.futures_main_sina(
                    symbol=s,
                    start_date=start.strftime("%Y%m%d"),
                    end_date=end.strftime("%Y%m%d"),
                )
                has_bar = k is not None and len(k) > 0
            except Exception as e:
                # 探测失败一律【保留】：源抖动不得把仍在交易的品种静默剔除
                logger.warning(
                    f"futures {s} 活跃性探测失败({type(e).__name__})，保守保留"
                )
                unknown.append(s)
                continue
            (active if has_bar else dormant).append(s)
        if dormant:
            logger.warning(
                f"主力连续剔除 {len(dormant)} 个停摆品种"
                f"（近 {self.ACTIVE_LOOKBACK_DAYS} 天无 bar）: {dormant}"
            )
        if unknown:
            logger.warning(f"主力连续活跃性未知、已保留: {unknown}")
        logger.info(f"主力连续活跃清单: {len(active)}/{len(all_symbols)}")
        return active

    def get_frequency(self, interval: Interval) -> str:
        if interval == Interval.DAY_1:
            return "d"
        raise ValueError(f"Unsupported interval: {interval}")

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
        if end is None or end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            return None
        df = ak.futures_main_sina(
            symbol=symbol,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if df is None or len(df) == 0:
            return df
        df["datetime"] = pd.to_datetime(df["日期"])
        df["code"] = symbol
        df = df.rename(columns=_FUTURES_COL_MAP)
        return df


class AKShareBondDailyDataProxy(DataProxy):
    """
    可转债日线（新浪源），落 bond_kline_1d。

    🚫 【暂缓 · 用户 2026-10-14 决定】不要使用。本机此源对 2021+ 活跃券覆盖极差，
    详见 BondKlineDaily docstring。保留代码仅为将来换源（通达信/付费源）时作骨架参考。

    该接口不支持 start/end 参数，返回全历史 → 在本地按 start/end 过滤。
    """

    min_sync_interval_days = 1
    min_start_date = None

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.BOND
    ) -> BondKlineDaily:
        return {
            (Interval.DAY_1, Adjust.NFQ, Product.BOND): BondKlineDaily,
        }[(interval, adjust, product)]

    @staticmethod
    def to_sina_symbol(code: str) -> str:
        """6 位转债代码 → 新浪 symbol。11xxxx=沪, 12xxxx=深。"""
        return ("sh" if code.startswith("11") else "sz") + code

    def get_symobls(self) -> list[str]:
        df = ak.bond_zh_cov()
        return [self.to_sina_symbol(c) for c in df["债券代码"].astype(str)]

    def get_frequency(self, interval: Interval) -> str:
        if interval == Interval.DAY_1:
            return "d"
        raise ValueError(f"Unsupported interval: {interval}")

    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        if end is None or end > datetime.now():
            end = datetime.now()
        if self.min_start_date is not None and start < self.min_start_date:
            return None
        df = ak.bond_zh_hs_cov_daily(symbol=symbol)
        if df is None or len(df) == 0 or "date" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] > start) & (df["date"] <= end)]
        if len(df) == 0:
            return None
        df["datetime"] = df["date"]
        df["code"] = symbol
        return df
