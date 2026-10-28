"""适配器注册表：加一个交易所 = 加一个文件 + 在这里登记一行。"""

from __future__ import annotations

from pond.cryptos.config import VenueSpec
from pond.cryptos.venues.base import VenueAdapter
from pond.cryptos.venues.binance import BinanceAdapter
from pond.cryptos.venues.bitget import BitgetAdapter
from pond.cryptos.venues.bybit import BybitAdapter
from pond.cryptos.venues.gate import GateAdapter
from pond.cryptos.venues.htx import HtxAdapter
from pond.cryptos.venues.hyperliquid import HyperliquidAdapter
from pond.cryptos.venues.kucoin import KucoinAdapter
from pond.cryptos.venues.mexc import MexcAdapter
from pond.cryptos.venues.okx import OkxAdapter

ADAPTERS: dict[str, type[VenueAdapter]] = {
    # Binance 默认关闭：生产库 quant.kline_futures_1h 已有它的历史数据
    "binance": BinanceAdapter,
    "okx": OkxAdapter,
    "bybit": BybitAdapter,
    "gate": GateAdapter,
    "bitget": BitgetAdapter,
    "kucoin": KucoinAdapter,
    "mexc": MexcAdapter,
    "htx": HtxAdapter,
    "hyperliquid": HyperliquidAdapter,
}


def available_venues() -> set[str]:
    return set(ADAPTERS)


def make_adapter(spec: VenueSpec) -> VenueAdapter:
    cls = ADAPTERS.get(spec.name)
    if cls is None:
        raise KeyError(f"未实现交易所适配器: {spec.name}，已实现 {sorted(ADAPTERS)}")
    return cls(spec)
