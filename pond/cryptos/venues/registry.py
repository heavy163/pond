"""适配器注册表：加一个交易所 = 加一个文件 + 在这里登记一行。"""

from __future__ import annotations

from pond.cryptos.config import VenueSpec
from pond.cryptos.venues.base import VenueAdapter
from pond.cryptos.venues.binance import BinanceAdapter

ADAPTERS: dict[str, type[VenueAdapter]] = {
    "binance": BinanceAdapter,
    # P1 待接入（每个文件 ~200 行，接口差异写在各自模块 docstring 里）:
    # "okx": OkxAdapter,
    # "bybit": BybitAdapter,
    # "gate": GateAdapter,
    # "bitget": BitgetAdapter,
    # "htx": HtxAdapter,
    # "hyperliquid": HyperliquidAdapter,
    # "kucoin": KucoinAdapter,
    # "mexc": MexcAdapter,
}


def available_venues() -> set[str]:
    return set(ADAPTERS)


def make_adapter(spec: VenueSpec) -> VenueAdapter:
    cls = ADAPTERS.get(spec.name)
    if cls is None:
        raise KeyError(f"未实现交易所适配器: {spec.name}，已实现 {sorted(ADAPTERS)}")
    return cls(spec)
