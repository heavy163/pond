"""
币安 USDT 永续合约过滤 — 根据代币解锁数据排除波动风险合约

将 CMC 代币解锁数据与币安 USDT 永续合约做交叉映射，
返回需要排除的合约列表。

映射逻辑:
  1. CMC symbol (大写) → 尝试直接匹配币安合约 symbol（不含 USDT 后缀）
  2. 特殊映射表处理 CMC slug → 币安 symbol 的差异
  3. 支持自定义 symbol 映射

用法:
    from pond.token_unlock import UnlockFilter

    uf = UnlockFilter()
    excluded = uf.get_excluded_contracts(
        min_unlock_pct=1.0,     # 解锁量 ≥ 流通量 1%
        window_days=14,         # 未来 14 天内
    )
    # => ["ZROUSDT", "KAITOUSDT", ...]

    # 用现有交易列表过滤
    my_symbols = ["BTCUSDT", "ETHUSDT", "ZROUSDT", "KAITOUSDT"]
    safe = uf.filter_symbols(my_symbols, window_days=7)
    # => ["BTCUSDT", "ETHUSDT"]
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from loguru import logger

from pond.token_unlock.client import CMCUnlockClient


# ═══════════════════════════════════════════════════════════════════
#  Symbol 映射表：CMC symbol/slug → 币安合约 baseAsset
#
#  CMC symbol 有时与币安合约 baseAsset 不同：
#    - Binance 用 "1000PEPE"、"1000SHIB"、"1000BONK" 等
#    - CMC slug 可能含连字符
#    - 偶尔名称完全不同
#
#  这组映射覆盖了常见的差异。
# ═══════════════════════════════════════════════════════════════════

# CMC symbol → Binance baseAsset
SYMBOL_OVERRIDES: dict[str, str] = {
    "PEPE": "1000PEPE",
    "SHIB": "1000SHIB",
    "BONK": "1000BONK",
    "FLOKI": "1000FLOKI",
    "LUNC": "LUNA",  # Terra Classic vs Terra
    "LUNA": "LUNA2",  # Terra (v2) on Binance
}

# CMC slug → Binance baseAsset (当 symbol 过于通用时使用)
SLUG_OVERRIDES: dict[str, str] = {}


class UnlockFilter:
    """代币解锁信息 → 币安合约排除列表

    典型用法:
        uf = UnlockFilter()
        # 获取所有需要排除的合约
        excluded = uf.get_excluded_contracts()

        # 在交易前过滤
        all_contracts = ["BTCUSDT", "ETHUSDT", "ZROUSDT", ...]
        tradeable = uf.filter_symbols(all_contracts)
    """

    def __init__(
        self,
        unlock_client: CMCUnlockClient | None = None,
        symbol_overrides: dict[str, str] | None = None,
        slug_overrides: dict[str, str] | None = None,
        binance_timeout: int = 15,
    ):
        self._client = unlock_client or CMCUnlockClient()
        self.symbol_overrides = {**(symbol_overrides or {}), **SYMBOL_OVERRIDES}
        self.slug_overrides = {**(slug_overrides or {}), **SLUG_OVERRIDES}
        self.binance_timeout = binance_timeout

        # 缓存：避免重复请求
        self._cached_unlocks: list[Any] | None = None
        self._cached_binance_symbols: list[str] | None = None

    # ═════════════════════════════════════════════════════════════
    #  公开 API
    # ═════════════════════════════════════════════════════════════

    def get_excluded_contracts(
        self,
        min_unlock_pct: float = 1.0,
        window_days: int = 14,
        max_unlock_pct: float = 100.0,
    ) -> list[str]:
        """获取需要排除的币安 USDT 永续合约列表

        Args:
            min_unlock_pct: 最小解锁占比阈值（占总锁仓 %）
                            e.g. 1.0 = 解锁量 ≥ 总锁仓的 1%
            window_days:   未来窗口天数，只排除此窗口内的解锁
            max_unlock_pct: 最大解锁占比上限（排除已完成大部分解锁的代币）

        Returns:
            需要排除的合约 symbol 列表，如 ["ZROUSDT", "KAITOUSDT"]
        """
        unlock_entries = self._get_unlocks(window_days=window_days)
        binance_symbols = self._get_binance_futures_symbols()
        binance_base_set = set(binance_symbols)
        cmc_to_binance = self._build_symbol_map(unlock_entries, binance_base_set)

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=window_days)
        excluded: list[str] = []

        for entry in unlock_entries:
            if entry.next_unlock is None:
                continue

            binance_symbol = cmc_to_binance.get(entry.symbol)
            if not binance_symbol:
                continue

            unlock_pct = entry.next_unlock.token_amount_pct
            if unlock_pct < min_unlock_pct:
                continue
            if unlock_pct > max_unlock_pct:
                continue

            unlock_date = entry.next_unlock.date
            if unlock_date < now:
                continue
            if unlock_date > cutoff:
                continue

            excluded.append(binance_symbol)

        return sorted(set(excluded))

    def filter_symbols(
        self,
        symbols: list[str],
        min_unlock_pct: float = 1.0,
        window_days: int = 14,
    ) -> list[str]:
        """从交易列表中过滤掉有解锁风险的合约

        Args:
            symbols: 原始交易合约列表 (如 ["BTCUSDT", "ZROUSDT"])
            min_unlock_pct: 最小解锁占比
            window_days: 窗口天数

        Returns:
            过滤后的安全合约列表
        """
        excluded = set(
            self.get_excluded_contracts(
                min_unlock_pct=min_unlock_pct,
                window_days=window_days,
            )
        )
        return [s for s in symbols if s.upper() not in excluded]

    def get_unlock_summary(
        self,
        min_unlock_pct: float = 0.5,
        window_days: int = 30,
    ) -> list[dict]:
        """获取即将解锁代币的摘要信息

        Returns:
            [{
                "symbol": "ZRO",
                "contract": "ZROUSDT",
                "unlock_date": "2026-07-20",
                "unlock_amount": 32600000,
                "unlock_usd": 26460000,
                "unlock_pct": 3.26,
                "market_cap": 287250000,
                "unlock_pct_of_mcap": 9.21,
                "on_binance_futures": True,
            }, ...]
        """
        unlock_entries = self._get_unlocks(window_days=window_days)
        binance_symbols = self._get_binance_futures_symbols()
        binance_base_set = set(binance_symbols)
        cmc_to_binance = self._build_symbol_map(unlock_entries, binance_base_set)

        now = datetime.now(timezone.utc)
        results: list[dict] = []

        for entry in unlock_entries:
            if entry.next_unlock is None:
                continue
            if entry.next_unlock.token_amount_pct < min_unlock_pct:
                continue

            unlock_date = entry.next_unlock.date
            if unlock_date < now:
                continue
            cutoff = now + timedelta(days=window_days)
            if unlock_date > cutoff:
                continue

            contract = cmc_to_binance.get(entry.symbol, "")
            mcap = entry.market_cap or 0

            results.append(
                {
                    "symbol": entry.symbol,
                    "slug": entry.slug,
                    "name": entry.name,
                    "contract": contract,
                    "unlock_date": unlock_date.strftime("%Y-%m-%d %H:%M UTC"),
                    "unlock_timestamp_ms": int(unlock_date.timestamp() * 1000),
                    "unlock_amount": entry.next_unlock.token_amount,
                    "unlock_usd": round(entry.next_unlock.token_amount_usd, 2),
                    "unlock_pct": entry.next_unlock.token_amount_pct,
                    "total_unlocked_pct": entry.total_unlocked_pct,
                    "price": entry.price,
                    "market_cap": mcap,
                    "on_binance_futures": bool(contract),
                }
            )

        return sorted(results, key=lambda r: r["unlock_date"])

    # ═════════════════════════════════════════════════════════════
    #  内部方法
    # ═════════════════════════════════════════════════════════════

    def _get_unlocks(self, window_days: int | None = None) -> list[Any]:
        """获取或缓存解锁数据

        Args:
            window_days: 未来窗口天数。None 表示使用 fetch_all 默认值 (90)。
        """
        cache_key = f"window_{window_days or 90}"
        cached = getattr(self, "_cached_unlocks", None)
        cached_key = getattr(self, "_cached_unlocks_key", None)
        if cached is not None and cached_key == cache_key:
            return cached
        entries = self._client.fetch_upcoming_by_window(window_days=window_days or 90)
        self._cached_unlocks = entries
        self._cached_unlocks_key = cache_key
        return entries

    def _get_binance_futures_symbols(self) -> list[str]:
        """获取币安所有 USDT 永续合约 symbol

        使用公开 REST API: GET /fapi/v1/exchangeInfo
        无需 API key，完全免费。
        """
        if self._cached_binance_symbols is not None:
            return self._cached_binance_symbols

        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        try:
            resp = requests.get(url, timeout=self.binance_timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Binance exchange info: {e}")
            return []

        usdt_symbols: list[str] = []
        for s in data.get("symbols", []):
            if s["status"] != "TRADING":
                continue
            if s["quoteAsset"] == "USDT" and "PERPETUAL" in s.get("contractType", ""):
                usdt_symbols.append(s["symbol"])

        logger.info(f"Fetched {len(usdt_symbols)} USDT perpetual symbols from Binance")
        self._cached_binance_symbols = usdt_symbols
        return usdt_symbols

    def _build_symbol_map(
        self,
        unlock_entries: list[Any],
        binance_symbols: set[str],
    ) -> dict[str, str]:
        """建立 CMC symbol → 币安合约 symbol 的映射

        返回: {"ZRO": "ZROUSDT", "KAITO": "KAITOUSDT", ...}
        """
        cmap: dict[str, str] = {}

        # 币安的 base asset 集合（去掉 USDT 后缀）
        binance_bases: set[str] = set()
        for sym in binance_symbols:
            if sym.endswith("USDT"):
                base = sym[:-4]
                binance_bases.add(base)

        for entry in unlock_entries:
            cmc_symbol = entry.symbol.upper()
            cmc_slug = entry.slug or ""

            # 1) 检查 symbol override
            if cmc_symbol in self.symbol_overrides:
                mapped = self.symbol_overrides[cmc_symbol]
                if mapped in binance_bases:
                    cmap[cmc_symbol] = f"{mapped}USDT"
                    continue

            # 2) 尝试直接匹配
            if cmc_symbol in binance_bases:
                cmap[cmc_symbol] = f"{cmc_symbol}USDT"
                continue

            # 3) 检查 slug override
            if cmc_slug in self.slug_overrides:
                mapped = self.slug_overrides[cmc_slug]
                if mapped in binance_bases:
                    cmap[cmc_symbol] = f"{mapped}USDT"
                    continue

            # 4) 尝试 slug 作为币安 base
            slug_base = cmc_slug.upper().replace("-", "")
            if slug_base in binance_bases and slug_base != cmc_symbol:
                cmap[cmc_symbol] = f"{slug_base}USDT"
                continue

            # 5) 尝试短 symbol（去掉数字前缀）
            for prefix in ["1000", "10000"]:
                if cmc_symbol.startswith(prefix):
                    short = cmc_symbol[len(prefix) :]
                    if short in binance_bases:
                        cmap[cmc_symbol] = f"{short}USDT"
                        break

            # 6) slug 大写去连字符（覆盖 name != symbol 的场景）
            if cmc_symbol not in cmap:
                slug_clean = cmc_slug.upper().replace("-", "").replace(" ", "")
                if slug_clean in binance_bases and slug_clean != cmc_symbol:
                    cmap[cmc_symbol] = f"{slug_clean}USDT"
                    continue

            # 7) name 首字母缩写（最后手段）
            if cmc_symbol not in cmap:
                acronym = "".join(w[0] for w in entry.name.upper().split() if w)
                if acronym in binance_bases and acronym != cmc_symbol:
                    cmap[cmc_symbol] = f"{acronym}USDT"
                    continue

        return cmap

    def clear_cache(self):
        """清理缓存，下次调用重新获取"""
        self._cached_unlocks = None
        self._cached_unlocks_key = None
        self._cached_binance_symbols = None
