"""Token Unlock 数据获取与合约过滤

从 CoinMarketCap 免费公开 API 获取代币解锁数据，
并与币安 USDT 永续合约列表做交叉过滤。

数据源: https://api.coinmarketcap.com/data-api/v3/token-unlock/listing
        完全免费，无需 API key，覆盖 ~388/406 个代币。

用法:
    from pond.token_unlock import UnlockFilter

    uf = UnlockFilter()
    excluded = uf.get_excluded_contracts(
        min_unlock_pct=1.0,     # 解锁量 ≥ 流通量的 1%
        window_days=14,         # 未来 14 天内解锁
    )
    # excluded => ["ZROUSDT", "KAITOUSDT", ...]
"""

from pond.token_unlock.client import CMCUnlockClient
from pond.token_unlock.binance_filter import UnlockFilter

__all__ = ["CMCUnlockClient", "UnlockFilter"]
