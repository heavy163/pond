#!/usr/bin/env python3
"""
示例：同步代币解锁数据到 ClickHouse。

流程：
  1. 从 CMC 免费公开 API 获取未来 N 天内的代币解锁数据（2 次 API 调用）
  2. 获取 Binance USDT 永续合约列表
  3. 建立 CMC symbol → Binance 合约映射（复用 UnlockFilter）
  4. 从 cmc_mapping_cache 解析 platform / contract_address（需 CMC Pro API Key）
  5. 写入 ClickHouse token_unlock 表（ReplacingMergeTree 自动去重）

CMC 免费 API 无需 API key，但 platform/contract_address 解析依赖 Pro API。
如果无 Pro API key，platform 将标记为 "unknown"，不影响核心功能。

环境变量：
  CMC_PRO_API_KEY       可选。用于解析 platform/contract_address（消歧）
  CLICKHOUSE_PWD        ClickHouse 密码
  CLICKHOUSE_HOST       ClickHouse 主机
  CMC_CACHE_PATH        可选。映射缓存文件路径（默认 cmc_mapping_cache.json）

用法：
  # 基本用法（需要 .env.ini 或环境变量配置）
  python examples/sync_token_unlock.py

  # 指定窗口天数
  python examples/sync_token_unlock.py --window 180

  # 完整环境变量
  CLICKHOUSE_HOST=xxx CLICKHOUSE_PWD=xxx python examples/sync_token_unlock.py
"""

import argparse
import configparser
import os
import sys
from datetime import datetime
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
    parser = argparse.ArgumentParser(description="同步代币解锁数据到 ClickHouse")
    parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="未来窗口天数，只同步此窗口内的解锁 (默认: 90)",
    )
    args = parser.parse_args()

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

    # FuturesHelper 初始化需要 CMC_PRO_API_KEY（用于 platform 解析缓存）
    # 即使没有，也会以 "unknown"/"0x0" 退化运行
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

    logger.info(f"同步配置: window_days={args.window}")
    logger.info(f"CMC 缓存文件: {helper.cmc_client.cache_path}")

    success = helper.sync_token_unlock(window_days=args.window)

    if success:
        logger.success(f"代币解锁同步完成 ✅ (window={args.window}天)")
    else:
        logger.warning("代币解锁同步失败或部分完成 ⚠️")

    try:
        df = clickhouse.native_sql_read_table(
            """
            SELECT symbol, binance_code, next_unlock_time,
                   next_unlock_pct, next_unlock_amount_usd, market_cap
            FROM token_unlock FINAL
            WHERE next_unlock_time >= now()
              AND next_unlock_time <= now() + INTERVAL 90 DAY
            ORDER BY next_unlock_amount_usd DESC
            LIMIT 10
            """,
        )
        if df is not None and not df.empty:
            logger.info("最近写入的解锁数据（前 10 条）:")
            print(df.to_string(index=False))
        else:
            logger.info("token_unlock 表中暂无数据")
    except Exception as e:
        logger.warning(f"查询最新数据失败: {e}")


if __name__ == "__main__":
    main()
