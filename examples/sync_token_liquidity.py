#!/usr/bin/env python3
"""
示例：同步代币 DEX 流动性数据到 ClickHouse。

流程：
  1. 从 Binance exchangeInfo 获取所有 USDT 永续合约
  2. 对每个 baseAsset，通过 DEX Screener search API 查询交易对
  3. 按 liquidity.usd 降序取前 5 个池
  4. 写入 ClickHouse token_liquidity 表（ReplacingMergeTree 自动去重）

DEX Screener 限速 60 req/min，约 665 个标的需 ~11 分钟。

环境变量：
  CLICKHOUSE_PWD       ClickHouse 密码
  CLICKHOUSE_HOST      ClickHouse 主机

用法：
  python examples/sync_token_liquidity.py
"""

import configparser
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pond.clickhouse.helper import FuturesHelper, DirectDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.duckdb.crypto import CryptoDB

_INI_PATH = _PROJECT_ROOT / ".env.ini"


def _load_env_ini():
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
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1].strip()
            os.environ[key] = value


def main():
    _load_env_ini()

    db_path = Path(os.environ.get("DUCKDB_PATH", "/share/DuckDB"))
    crypto_db = CryptoDB(db_path)

    host = os.environ.get("CLICKHOUSE_HOST", "").strip()
    password = os.environ.get("CLICKHOUSE_PWD", "").strip()
    if not host:
        logger.error("请设置 CLICKHOUSE_HOST 环境变量")
        sys.exit(1)

    conn_str = f"clickhouse://default:{password}@{host}:8123/quant"
    native_conn_str = (
        f"clickhouse+native://default:{password}@{host}:9000/quant?tcp_keepalive=true"
    )

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

    proxy_host = os.environ.get("PROXY_HOST")
    proxy_port = os.environ.get("PROXY_PORT")
    if proxy_host and proxy_port:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        helper.data_proxy = DirectDataProxy(
            proxies={"https": proxy_url, "http": proxy_url}
        )
    else:
        helper.data_proxy = DirectDataProxy(proxies={})

    logger.info("DEX 流动性同步开始...")

    success = helper.sync_token_liquidity()

    if success:
        logger.success("DEX 流动性同步完成 ✅")
    else:
        logger.warning("DEX 流动性同步失败或部分完成 ⚠️")

    try:
        signal = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        df = clickhouse.native_sql_read_table(
            """
            SELECT symbol, chain, dex_id, liquidity_usd, volume_h24
            FROM token_liquidity FINAL
            ORDER BY liquidity_usd DESC
            LIMIT 10
            """,
        )
        if df is not None and not df.empty:
            logger.info("最新写入的流动性数据（前 10 条）:")
            print(df.to_string(index=False))
    except Exception as e:
        logger.warning(f"查询最新数据失败: {e}")


if __name__ == "__main__":
    main()
