# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm
# https://github.com/binance/binance-futures-connector-python

import time
import threading

import pandas as pd
import polars as pl
import datetime as dt
from tqdm import tqdm
from loguru import logger

from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures
from pond.duckdb.crypto.const import kline_schema
from pond.binance_history.type import TIMEFRAMES


def get_future_info_df(client: CMFutures | UMFutures) -> pd.DataFrame:
    info = client.exchange_info()
    df = pd.DataFrame.from_records(info["symbols"])
    df["update_datetime"] = dt.datetime.utcfromtimestamp(
        info["serverTime"] / 1000
    ).replace(tzinfo=dt.timezone.utc)
    df["deliveryDate"] = df["deliveryDate"].apply(
        lambda x: dt.datetime.utcfromtimestamp(x / 1000).replace(tzinfo=dt.timezone.utc)
    )
    df["onboardDate"] = df["onboardDate"].apply(
        lambda x: dt.datetime.utcfromtimestamp(x / 1000).replace(tzinfo=dt.timezone.utc)
    )
    return df


def get_future_symbol_list(client: CMFutures | UMFutures) -> list[str]:
    df = get_future_info_df(client)
    return [
        ss["symbol"] for _, ss in df.iterrows() if ss["contractType"] == "PERPETUAL"
    ]


def get_klines(
    client: CMFutures | UMFutures,
    symbol: str,
    interval: TIMEFRAMES,
    start: int,
    end: int,
    res_list: list[pl.DataFrame],
):
    dd = client.klines(symbol=symbol, interval=interval, startTime=start, endTime=end, limit=1000)
    dd = pl.from_records(dd, schema=kline_schema)
    res_list.append(dd)


def get_supply_df(
    client: CMFutures | UMFutures,
    lack_df: pl.DataFrame,
    symbol: str,
    interval: TIMEFRAMES = "1d",
) -> pl.DataFrame:
    """
    base = 1577836800000
    base_dt = dt.datetime.utcfromtimestamp(base/1e3)
    base += dt.timedelta(days=1).total_seconds()*1e3
    base_dt2 = dt.datetime.utcfromtimestamp(base/1e3)
    """


    res_list = []
    t_list = []
    for i in range(0, len(lack_df), 2):
        start = lack_df["open_time"][i]
        end = lack_df["open_time"][i + 1]
        logger.info(
            f"[{symbol}] Supplement missing {interval} data: "
            f"{dt.datetime.utcfromtimestamp(start/1e3)} -> "
            f"{dt.datetime.utcfromtimestamp(end/1e3)}"
        )

        t = threading.Thread(
            target=get_klines,
            args=(client, symbol, interval, start, end, res_list),
        )
        t.start()
        t_list.append(t)

    if t_list:
        [t.join() for t in t_list]

    return pl.concat(res_list)


if __name__ == "__main__":
    import polars as pl
    from pond.duckdb.crypto.const import kline_schema

    symbol = "BTSUSDT"
    interval = "1d"
    start = "2022-1-1"
    end = "2023-11-1"
    dt_format = "%Y-%m-%d"
    start_dt = dt.datetime.strptime(start, dt_format).timestamp() * 1e3
    end_dt = dt.datetime.strptime(end, dt_format).timestamp() * 1e3
    # proxies = {"https": "127.0.0.1:7890"}
    proxies = {}

    um_client = UMFutures(proxies=proxies)
    cm_client = CMFutures(proxies=proxies)

    res_list = []
    symbols = ["BTCUSDT", "BTSUSDT", "ETHUSDT"]
    # r = get_future_info_df(um_client)
    dd = um_client.klines(
        symbol=symbol, interval=interval, startTime=int(start_dt), endTime=int(end_dt)
    )
    df = (
        pl.from_records(dd, schema=kline_schema)
        .with_columns(
            (pl.col("open_time") * 1e3).cast(pl.Datetime),
            (pl.col("close_time") * 1e3).cast(pl.Datetime),
            jj_code=pl.lit(symbol),
        )
        .to_pandas()
    )

    cm_symbol_list = get_future_symbol_list(cm_client)
    um_symbol_list = get_future_symbol_list(um_client)

    tz = "Asia/Shanghai"
    # r = client.continuous_klines("BTCUSD", "PERPETUAL", "1m")
    print(1)
