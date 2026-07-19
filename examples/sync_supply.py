#!/usr/bin/env python3
"""
示例：使用 CMCMarketDataClient 同步 Binance 永续合约的代币供应量。

流程：
  1. 从 Binance exchangeInfo 获取当前所有交易对
  2. 筛选 PERPETUAL USDT 合约
  3. 用 CMCMarketDataClient 解析 symbol → CMC id 映射（首次运行建缓存）
  4. 批量查询 total_supply / market_cap_fdv_ratio
  5. 写入 ClickHouse FutureInfo 表（一天一更新，跳过当日已同步的标的）

首次运行：   需要 CMC API Key，建映射缓存 → ~4-6 次 HTTP 请求
日常运行：   读缓存查供应量 → 2 次 HTTP 请求
30 天重验证：比对 discriminator，检测底层项目是否更换

环境变量：
  CMC_PRO_API_KEY      必需。CoinMarketCap API Key
  CLICKHOUSE_PWD       ClickHouse 密码
  CMC_CACHE_PATH       可选。映射缓存文件路径（默认 cmc_mapping_cache.json）

用法：
  # 首次运行（需要 CMC API Key）
  CMC_PRO_API_KEY=xxx python examples/sync_supply.py

  # 指定缓存路径
  CMC_PRO_API_KEY=xxx CMC_CACHE_PATH=/tmp/cmc_cache.json python examples/sync_supply.py
"""

import configparser
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from pond.clickhouse.helper import FuturesHelper, DirectDataProxy
from pond.clickhouse.kline import FutureInfo
from pond.duckdb.crypto import CryptoDB
import pandas as pd
from loguru import logger

# 将项目根目录加入 sys.path，允许直接 python examples/sync_supply.py 运行
_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_INI_PATH = _PROJECT_ROOT / ".env.ini"


def _load_env_ini():
    """如果环境变量未设置，从 .env.ini 读取作为 fallback。"""
    ini = _INI_PATH
    if not ini.exists():
        return
    cfg = configparser.ConfigParser()
    cfg.read(ini)
    for section in cfg.sections():
        for key, value in cfg[section].items():
            key = key.upper()
            if os.environ.get(key) is not None:
                continue
            value = value.strip()
            # 去掉可选的包裹引号（单引号或双引号）
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1].strip()
            os.environ[key] = value


def main():
    _load_env_ini()
    # ── 1. 初始化 ──
    db_path = Path(os.environ.get("DUCKDB_PATH", "/share/DuckDB"))
    crypto_db = CryptoDB(db_path)

    host = os.environ.get("CLICKHOUSE_HOST").strip()
    password = os.environ.get("CLICKHOUSE_PWD").strip()
    conn_str = f"clickhouse://default:{password}@{host}:8123/quant"
    native_conn_str = (
        f"clickhouse+native://default:{password}@{host}:9000/quant?tcp_keepalive=true"
    )

    # 如果 clickhouse 不可用，这里会报错。实际使用时可改为 try/except
    from pond.clickhouse.manager import ClickHouseManager

    clickhouse = ClickHouseManager(
        conn_str,
        data_start=datetime(2020, 1, 1),
        native_uri=native_conn_str,
    )

    helper = FuturesHelper(
        crypto_db,
        clickhouse,
        fix_kline_with_cryptodb=False,
    )

    # 代理配置：.env.ini 中有值则用代理，否则直连
    proxy_host = os.environ.get("PROXY_HOST")
    proxy_port = os.environ.get("PROXY_PORT")
    if proxy_host and proxy_port:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        helper.data_proxy = DirectDataProxy(
            proxies={"https": proxy_url, "http": proxy_url}
        )
    else:
        helper.data_proxy = DirectDataProxy(proxies={})

    logger.info("CMCMarketDataClient 初始化完成")
    logger.info(f"缓存文件: {helper.cmc_client.cache_path}")
    logger.info(f"缓存是否需要重验证: {helper.cmc_client.needs_re_validate()}")

    # ── 2. 获取当前交易对 ──
    signal = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    symbols = helper.get_perpetual_symbols(signal)

    if not symbols:
        logger.error("无法获取 Binance 合约列表，请检查网络连接")
        sys.exit(1)

    logger.info(f"获取到 {len(symbols)} 个 PERPETUAL USDT 合约标的")

    # ── 3. 执行同步 ──
    res_dict = {}
    helper._FuturesHelper__sync_futures_info(signal, FutureInfo, symbols, res_dict)
    success = res_dict.get(threading.current_thread().ident, False)

    # ── 4. 结果汇总 ──
    if success:
        logger.success("供应量同步完成 ✅")
    else:
        logger.warning("供应量部分完成，部分标的可能未同步 ⚠️")

    # 打印缓存中已解析的标的数量
    cached_count = len(helper.cmc_client.cache.get("symbols", {}))
    unresolved_count = len(helper.cmc_client.cache.get("unresolved", []))
    logger.info(f"已解析映射: {cached_count} 个标的")
    logger.info(f"无法匹配:   {unresolved_count} 个标的")

    # ── 5. 验证最新数据 ──
    try:
        latest = clickhouse.read_latest_n_record(
            FutureInfo.__tablename__,
            signal - pd.Timedelta(days=7),
            signal,
            10,
        )
        if latest is not None and not latest.empty:
            logger.info("最近写入的供应量数据（前 5 条）:")
            print(latest.head(5).to_string())
    except Exception as e:
        logger.warning(f"查询最新数据失败: {e}")


if __name__ == "__main__":
    main()
