"""Tests for UnlockFilter (Binance symbol mapping + exclusion)."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pond.token_unlock.binance_filter import UnlockFilter, SYMBOL_OVERRIDES
from pond.token_unlock.client import CMCUnlockClient, NextUnlock, TokenUnlockEntry


# ══════════════════════════════════════════════════════════════════
#  Fixtures / Helpers
# ══════════════════════════════════════════════════════════════════


def make_entry(
    symbol: str,
    slug: str = "",
    unlock_pct: float = 1.0,
    unlock_date: datetime | None = None,
) -> TokenUnlockEntry:
    if unlock_date is None:
        unlock_date = datetime.now(timezone.utc) + timedelta(days=1)
    return TokenUnlockEntry(
        crypto_id=hash(symbol) % 100000,
        symbol=symbol,
        slug=slug or symbol.lower(),
        name=symbol,
        circulating_supply=1_000_000.0,
        total_supply=10_000_000.0,
        max_supply=10_000_000.0,
        total_unlocked_pct=50.0,
        next_unlock=NextUnlock(
            token_amount=10000.0,
            token_amount_usd=50000.0,
            token_amount_pct=unlock_pct,
            date=unlock_date,
        ),
        price=5.0,
        market_cap=5_000_000.0,
    )


def make_entry_no_unlock(symbol: str) -> TokenUnlockEntry:
    return TokenUnlockEntry(
        crypto_id=hash(symbol) % 100000,
        symbol=symbol,
        slug=symbol.lower(),
        name=symbol,
        circulating_supply=1_000_000.0,
        total_supply=10_000_000.0,
        max_supply=10_000_000.0,
        total_unlocked_pct=100.0,
        next_unlock=None,
        price=5.0,
        market_cap=5_000_000.0,
    )


@pytest.fixture
def mock_filter() -> UnlockFilter:
    """Create UnlockFilter with mocked external APIs.

    Mocks _cached_unlocks and _cached_binance_symbols so no HTTP calls are made.
    """
    uf = UnlockFilter()
    # Block real HTTP
    uf._client = MagicMock(spec=CMCUnlockClient)
    return uf


# ══════════════════════════════════════════════════════════════════
#  Symbol mapping tests
# ══════════════════════════════════════════════════════════════════


class TestBuildSymbolMap:
    def test_direct_match(self, mock_filter):
        """CMC symbol 'ZRO' should match Binance base 'ZRO'."""
        entries = [make_entry("ZRO")]
        binance_set = {"BTCUSDT", "ETHUSDT", "ZROUSDT", "KAITOUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert cmap["ZRO"] == "ZROUSDT"

    def test_override_1000pepe(self, mock_filter):
        """CMC symbol 'PEPE' should map to '1000PEPEUSDT' via override."""
        entries = [make_entry("PEPE")]
        binance_set = {"BTCUSDT", "1000PEPEUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert cmap["PEPE"] == "1000PEPEUSDT"

    def test_slug_fallback(self, mock_filter):
        """When symbol doesn't match but slug does, fall back to slug."""
        entries = [make_entry("0G", slug="zero-gravity")]
        # No '0G' base, but slug 'zero-gravity' won't match either
        # In this case, mapping should fail
        binance_set = {"BTCUSDT", "ETHUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert "0G" not in cmap

    def test_no_match_returns_empty(self, mock_filter):
        """Token not on Binance should not appear in map."""
        entries = [make_entry("NONEONBINANCE")]
        binance_set = {"BTCUSDT", "ETHUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert "NONEONBINANCE" not in cmap

    def test_multiple_entries(self, mock_filter):
        """Multiple tokens should all be mapped correctly."""
        entries = [
            make_entry("ZRO"),
            make_entry("KAITO"),
            make_entry("PEPE"),
        ]
        binance_set = {"ZROUSDT", "KAITOUSDT", "1000PEPEUSDT", "BTCUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert cmap["ZRO"] == "ZROUSDT"
        assert cmap["KAITO"] == "KAITOUSDT"
        assert cmap["PEPE"] == "1000PEPEUSDT"

    def test_none_on_binance_remain_unmapped(self, mock_filter):
        """Tokens not on Binance get no entry in the map."""
        entries = [
            make_entry("ONBINANCE"),
            make_entry("NOTONBINANCE"),
        ]
        binance_set = {"ONBINANCEUSDT", "BTCUSDT"}
        cmap = mock_filter._build_symbol_map(entries, binance_set)
        assert "ONBINANCE" in cmap
        assert "NOTONBINANCE" not in cmap


# ══════════════════════════════════════════════════════════════════
#  get_excluded_contracts
# ══════════════════════════════════════════════════════════════════


class TestGetExcludedContracts:
    def test_excludes_by_unlock_pct(self, mock_filter):
        """Should exclude tokens with unlock_pct >= min_unlock_pct."""
        mock_filter._cached_unlocks = [
            make_entry("LARGE", unlock_pct=3.0),
            make_entry("SMALL", unlock_pct=0.5),
            make_entry("NONE", unlock_pct=0.0),
        ]
        mock_filter._cached_binance_symbols = ["LARGEUSDT", "SMALLUSDT", "NONEUSDT"]
        excluded = mock_filter.get_excluded_contracts(
            min_unlock_pct=1.0, window_days=30
        )
        assert "LARGEUSDT" in excluded
        assert "SMALLUSDT" not in excluded
        assert "NONEUSDT" not in excluded

    def test_excludes_by_window(self, mock_filter):
        """Should exclude only tokens unlocking within window_days."""
        today = datetime.now(timezone.utc)
        mock_filter._cached_unlocks = [
            make_entry("SOON", unlock_pct=2.0, unlock_date=today + timedelta(hours=1)),
            make_entry("LATER", unlock_pct=2.0, unlock_date=today + timedelta(days=20)),
            make_entry("FAR", unlock_pct=2.0, unlock_date=today + timedelta(days=60)),
        ]
        mock_filter._cached_binance_symbols = ["SOONUSDT", "LATERUSDT", "FARUSDT"]
        excluded = mock_filter.get_excluded_contracts(
            min_unlock_pct=1.0, window_days=14
        )
        assert "SOONUSDT" in excluded
        assert "LATERUSDT" not in excluded  # 20 days > 14
        assert "FARUSDT" not in excluded

    def test_skips_unmapped_symbols(self, mock_filter):
        """Tokens not on Binance should not appear in excluded list."""
        mock_filter._cached_unlocks = [
            make_entry("NOTONBINANCE", unlock_pct=5.0),
        ]
        mock_filter._cached_binance_symbols = ["BTCUSDT", "ETHUSDT"]
        excluded = mock_filter.get_excluded_contracts(min_unlock_pct=1.0, window_days=7)
        assert excluded == []

    def test_skips_expired_unlocks(self, mock_filter):
        """Past unlocks should be excluded."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        mock_filter._cached_unlocks = [
            make_entry("OLD", unlock_pct=5.0, unlock_date=yesterday),
        ]
        mock_filter._cached_binance_symbols = ["OLDUSDT"]
        excluded = mock_filter.get_excluded_contracts(min_unlock_pct=1.0, window_days=7)
        assert excluded == []

    def test_empty_unlocks_returns_empty(self, mock_filter):
        """No unlock data should yield empty exclusion list."""
        mock_filter._cached_unlocks = []
        mock_filter._cached_binance_symbols = ["SOMEUSDT"]
        excluded = mock_filter.get_excluded_contracts()
        assert excluded == []


# ══════════════════════════════════════════════════════════════════
#  filter_symbols
# ══════════════════════════════════════════════════════════════════


class TestFilterSymbols:
    def test_filters_excluded_contracts(self, mock_filter):
        """filter_symbols should remove excluded contracts from input list."""
        mock_filter._cached_unlocks = [
            make_entry("ZRO", unlock_pct=3.0),
            make_entry("KAITO", unlock_pct=2.0),
        ]
        mock_filter._cached_binance_symbols = ["ZROUSDT", "KAITOUSDT", "BTCUSDT"]

        symbols = ["BTCUSDT", "ETHUSDT", "ZROUSDT", "KAITOUSDT"]
        safe = mock_filter.filter_symbols(symbols, min_unlock_pct=1.0, window_days=30)
        assert "ZROUSDT" not in safe
        assert "KAITOUSDT" not in safe
        assert "BTCUSDT" in safe
        assert "ETHUSDT" in safe

    def test_preserves_unknown_symbols(self, mock_filter):
        """Symbols not in UnlockFilter's data should pass through."""
        mock_filter._cached_unlocks = []
        mock_filter._cached_binance_symbols = []

        symbols = ["BTCUSDT", "ETHUSDT", "RANDOMUSDT"]
        safe = mock_filter.filter_symbols(symbols)
        assert safe == symbols

    def test_empty_input(self, mock_filter):
        """Empty symbol list should return empty."""
        assert mock_filter.filter_symbols([]) == []


# ══════════════════════════════════════════════════════════════════
#  Symbol override constants
# ══════════════════════════════════════════════════════════════════


class TestSymbolOverrides:
    def test_known_overrides_exist(self):
        """We should have overrides for known multi-scale tokens."""
        assert SYMBOL_OVERRIDES["PEPE"] == "1000PEPE"
        assert SYMBOL_OVERRIDES["SHIB"] == "1000SHIB"
        assert SYMBOL_OVERRIDES["BONK"] == "1000BONK"
        assert SYMBOL_OVERRIDES["FLOKI"] == "1000FLOKI"
        assert SYMBOL_OVERRIDES["LUNC"] == "LUNA"
        assert SYMBOL_OVERRIDES["LUNA"] == "LUNA2"
