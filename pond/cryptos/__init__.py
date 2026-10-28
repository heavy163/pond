"""cryptos：多交易所 1h 行情/资金费率/持仓量采集。

与 pond 既有 quant 管道完全隔离：
  - 独立库 cryptos（CryptoStore 构造期断言）
  - 独立适配器层（venues/）
  - 不改 pond/clickhouse/*（FuturesHelper 继续服务 quant）

用法：
    from pond.cryptos import CryptoHelper, CryptoStore
    store = CryptoStore.from_env()
    helper = CryptoHelper(store)
    results = asyncio.run(helper.run_round())
"""

from pond.cryptos.config import BucketSpec, CryptosConfig, VenueSpec
from pond.cryptos.notify import LogNotifier, VenueDataEvent, build_notifier
from pond.cryptos.store import CryptoStore
from pond.cryptos.sync import CryptoHelper, VenueResult

__all__ = [
    "BucketSpec",
    "CryptosConfig",
    "VenueSpec",
    "CryptoStore",
    "CryptoHelper",
    "VenueResult",
    "VenueDataEvent",
    "LogNotifier",
    "build_notifier",
]
