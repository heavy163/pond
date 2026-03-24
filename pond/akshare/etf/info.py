import akshare as ak
import pandas as pd
from datetime import datetime
import time
from pathlib import Path


def get_all_funds() -> pd.DataFrame:
    fund_df = ak.fund_purchase_em()
    col_mapping = {
        "基金代码": "symbol",
        "基金简称": "name",
        "基金类型": "type",
        "最新净值/万份收益": "latest_net_value",
        "最新净值/万份收益-报告时间": "latest_net_value_report_date",
        "申购状态": "purchase_status",
        "赎回状态": "redemption_status",
        "下一开放日": "next_open_date",
        "购买起点": "min_purchase_amount",
        "日累计限定金额": "max_daily_purchase_amount",
        "手续费": "fee",
    }
    return fund_df.rename(mapper=col_mapping, axis=1)


def get_exchange_trading_etfs() -> pd.DataFrame:
    fund_df = get_all_funds()
    etfs = fund_df[fund_df["name"].str.upper().str.contains("ETF")]
    etfs = etfs[etfs["purchase_status"].str.contains("场内交易")]
    return etfs


def get_fund_basic_info(symbol: list[str]) -> pd.DataFrame:
    col_mapping = {
        "基金代码": "symbol",
        "基金名称": "name",
        "基金全称": "name_full",
        "成立时间": "established_date",
        "最新规模": "latest_scale",
        "基金公司": "company",
        "基金经理": "manager",
        "托管银行": "bank",
        "基金类型": "type",
        "评级机构": "rating_institution",
        "基金评级": "rating",
        "投资策略": "investment_strategy",
        "投资目标": "investment_goal",
        "业绩比较基准": "benchmark",
    }
    fund_individual_basic_info_xq_df = ak.fund_individual_basic_info_xq(symbol=symbol)
    return fund_individual_basic_info_xq_df.T.rename(mapper=col_mapping, axis=1)


def batch_get_etf_basic_info(
    symbols: list[str],
    cache_dir: str | Path,
    crawl_interval_seconds: int = 0.1,
    retry_times: int = 3,
) -> pd.DataFrame:
    if isinstance(cache_dir, str):
        cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    dfs = []
    for symbol in symbols:
        try:
            cache_file = cache_dir / f"{symbol}.csv"
            if cache_file.exists():
                df = pd.read_csv(cache_file)
            else:
                df = get_fund_basic_info(symbol)
                print(f"Get fund basic info for {symbol}")
                df.to_csv(cache_file, index=False)
            dfs.append(df)
            time.sleep(crawl_interval_seconds)
        except Exception as e:
            print(f"Error when get fund basic info for {symbol}: {e}")
            time.sleep(crawl_interval_seconds)
            continue
    if len(dfs) < len(symbols) and retry_times > 0:
        return batch_get_etf_basic_info(
            symbols, cache_dir, crawl_interval_seconds, retry_times - 1
        )
    return pd.concat(dfs, axis=0)


def get_exchange_etf_detail(
    cache_dir: str | Path,
    crawl_interval_seconds: int = 0.1,
    retry_times: int = 3,
) -> pd.DataFrame:
    etfs = get_exchange_trading_etfs()
    symbols = etfs["symbol"].tolist()
    df = batch_get_etf_basic_info(
        symbols,
        cache_dir=cache_dir,
        crawl_interval_seconds=crawl_interval_seconds,
        retry_times=retry_times,
    )
    df = etfs.join(df, on="symbol", how="left")
    return df


if __name__ == "__main__":
    cache_dir = Path("cache") / datetime.now().date().strftime("%Y%m%d")
    df = get_exchange_etf_detail(cache_dir)
    print(df)
