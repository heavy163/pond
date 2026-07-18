"""
Tests for CMCMarketDataClient.

Mocks all external HTTP calls; tests focus on:
  - Cache load/save
  - discriminator building
  - resolve_mapping with multiple variants
  - validate_mappings change detection
  - batch_quotes_by_id batching logic
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import requests

from pond.cmc import CMCMarketDataClient


# ══════════════════════════════════════════════════════════════════
#  Fixtures
# ══════════════════════════════════════════════════════════════════

@pytest.fixture
def cmc_client(tmp_path: Path) -> CMCMarketDataClient:
    """Return a CMCMarketDataClient with a temp cache file and mock session."""
    client = CMCMarketDataClient(
        api_key="test_key_12345",
        cache_path=tmp_path / "cmc_mapping_cache.json",
    )
    # Replace the real session with a mock
    client.session = MagicMock()
    return client


SAMPLE_BTC_INFO = {
    "BTC": {
        "id": 1,
        "name": "Bitcoin",
        "symbol": "BTC",
        "platform": None,  # native coin
    }
}

SAMPLE_PEPE_INFO = {
    "PEPE": [
        {
            "id": 24482,
            "name": "Pepe",
            "symbol": "PEPE",
            "platform": {
                "id": 1027,
                "name": "Ethereum",
                "slug": "ethereum",
                "token_address": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
            },
        },
        {
            "id": 30071,
            "name": "PEPE Chain",
            "symbol": "PEPE",
            "platform": {
                "id": 30070,
                "name": "PEPE Chain",
                "slug": "pepe-chain",
                "token_address": "0x1111111111111111111111111111111111111111",
            },
        },
        {
            "id": 32000,
            "name": "Wall Street Pepe",
            "symbol": "PEPE",
            "platform": {
                "id": 1027,
                "name": "Ethereum",
                "slug": "ethereum",
                "token_address": "0x2222222222222222222222222222222222222222",
            },
        },
    ]
}

SAMPLE_QUOTES = {
    "1": {"quote": {"USD": {"market_cap": 1_000_000_000_000}}},
    "24482": {"quote": {"USD": {"market_cap": 3_000_000_000}}},   # Pepe (ETH) highest
    "30071": {"quote": {"USD": {"market_cap": 500_000_000}}},     # PEPE Chain
    "32000": {"quote": {"USD": {"market_cap": 100_000_000}}},     # Wall St Pepe
}


# ══════════════════════════════════════════════════════════════════
#  discriminator building
# ══════════════════════════════════════════════════════════════════

class TestBuildDiscriminator:
    def test_native_coin(self):
        disc = CMCMarketDataClient._build_discriminator("BTC", None)
        assert disc == "BTC::native::0x0"

    def test_erc20_token(self):
        platform = {
            "slug": "ethereum",
            "token_address": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
        }
        disc = CMCMarketDataClient._build_discriminator("PEPE", platform)
        assert disc == (
            "PEPE::ethereum::"
            "0x6982508145454Ce325dDbE47a25d4ec3d2311933"
        )

    def test_generates_upper_base_asset(self):
        platform = {"slug": "binance-smart-chain", "token_address": "0xabc"}
        disc = CMCMarketDataClient._build_discriminator("cake", platform)
        assert disc.startswith("CAKE::")


# ══════════════════════════════════════════════════════════════════
#  resolve_mapping
# ══════════════════════════════════════════════════════════════════

class TestResolveMapping:
    def test_single_variant_direct_match(self, cmc_client):
        """BTC has only one variant → should return it directly."""
        cmc_client.session.request.return_value.json.side_effect = [
            {"data": SAMPLE_BTC_INFO},   # _fetch_info
            {"data": SAMPLE_QUOTES},      # batch_quotes_by_id
        ]
        result = cmc_client.resolve_mapping(["BTC"])
        assert "BTC" in result
        assert result["BTC"]["cmc_id"] == 1
        assert result["BTC"]["discriminator"] == "BTC::native::0x0"

    def test_multi_variant_picks_highest_market_cap(self, cmc_client):
        """PEPE has 3 variants → should pick Pepe (ETH) with highest mcap."""
        cmc_client.session.request.return_value.json.side_effect = [
            {"data": SAMPLE_PEPE_INFO},   # _fetch_info
            {"data": SAMPLE_QUOTES},       # batch_quotes_by_id
        ]
        result = cmc_client.resolve_mapping(["PEPE"])
        assert result["PEPE"]["cmc_id"] == 24482  # Pepe (ETH)
        assert "ethereum" in result["PEPE"]["discriminator"]

    def test_empty_input(self, cmc_client):
        assert cmc_client.resolve_mapping([]) == {}


# ══════════════════════════════════════════════════════════════════
#  cache load / save
# ══════════════════════════════════════════════════════════════════

class TestCache:
    def test_fresh_cache_on_empty_file(self, cmc_client):
        """Loading from non-existent file should produce default structure."""
        cache = cmc_client.cache
        assert cache["version"] == 1
        assert cache["symbols"] == {}
        assert cache["by_discriminator"] == {}

    def test_save_and_reload(self, cmc_client):
        cmc_client.cache["symbols"]["TEST"] = {
            "cmc_id": 999,
            "discriminator": "TEST::native::0x0",
            "resolved_at": "2026-01-01T00:00:00Z",
            "re_validated_at": "2026-01-01T00:00:00Z",
        }
        cmc_client._save_cache()

        # Reload in a new client
        client2 = CMCMarketDataClient(
            api_key="test_key",
            cache_path=cmc_client.cache_path,
        )
        client2.session = MagicMock()
        cached = client2.get_cached_mapping("TEST")
        assert cached is not None
        assert cached["cmc_id"] == 999

    def test_update_cache(self, cmc_client):
        resolved = {
            "BTC": {
                "cmc_id": 1,
                "name": "Bitcoin",
                "discriminator": "BTC::native::0x0",
                "chain": None,
                "contract_address": None,
            }
        }
        cmc_client._update_cache(resolved, ["BTC", "UNKNOWN"])
        assert cmc_client.get_cached_mapping("BTC")["cmc_id"] == 1
        assert cmc_client.get_cached_mapping("UNKNOWN") is None
        assert "UNKNOWN" in cmc_client.cache["unresolved"]

    def test_unknown_asset(self, cmc_client):
        """An asset in unresolved list should return None without HTTP call."""
        cmc_client.cache["unresolved"] = ["NONEXIST"]
        assert cmc_client.get_cached_mapping("NONEXIST") is None


# ══════════════════════════════════════════════════════════════════
#  validate_mappings (discriminator comparison)
# ══════════════════════════════════════════════════════════════════

class TestValidateMappings:
    def test_no_changes_when_discriminator_matches(self, cmc_client):
        """If CMC still returns the same discriminator → no changes."""
        # Pre-populate cache
        cmc_client.cache["symbols"]["PEPE"] = {
            "cmc_id": 24482,
            "name": "Pepe",
            "discriminator": (
                "PEPE::ethereum::"
                "0x6982508145454Ce325dDbE47a25d4ec3d2311933"
            ),
            "chain": "ethereum",
            "contract_address": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
            "resolved_at": "2026-01-01T00:00:00Z",
            "re_validated_at": "2026-01-01T00:00:00Z",
        }
        cmc_client.cache["by_discriminator"][
            "PEPE::ethereum::"
            "0x6982508145454Ce325dDbE47a25d4ec3d2311933"
        ] = "PEPE"

        # Mock CMC to return same data
        cmc_client.session.request.return_value.json.side_effect = [
            {"data": SAMPLE_PEPE_INFO},   # _fetch_info
            {"data": SAMPLE_QUOTES},       # batch_quotes_by_id
        ]

        changes = cmc_client.validate_mappings()
        assert len(changes) == 0

    def test_detects_project_swap(self, cmc_client):
        """If CMC now returns a different variant → detect change."""
        cmc_client.cache["symbols"]["PEPE"] = {
            "cmc_id": 24482,
            "name": "Pepe",
            "discriminator": (
                "PEPE::ethereum::"
                "0x6982508145454Ce325dDbE47a25d4ec3d2311933"
            ),
            "chain": "ethereum",
            "contract_address": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
            "resolved_at": "2026-01-01T00:00:00Z",
            "re_validated_at": "2026-01-01T00:00:00Z",
        }
        cmc_client.cache["by_discriminator"][
            "PEPE::ethereum::"
            "0x6982508145454Ce325dDbE47a25d4ec3d2311933"
        ] = "PEPE"

        # Mock CMC to return PEPE Chain as highest market cap (different from cached)
        changed_quotes = {
            "24482": {"quote": {"USD": {"market_cap": 100_000_000}}},
            "30071": {"quote": {"USD": {"market_cap": 3_000_000_000}}},  # PEPE Chain now highest
        }
        cmc_client.session.request.return_value.json.side_effect = [
            {"data": SAMPLE_PEPE_INFO},
            {"data": changed_quotes},
        ]

        changes = cmc_client.validate_mappings()
        assert len(changes) == 1
        assert changes[0]["base_asset"] == "PEPE"
        assert "pepe-chain" in changes[0]["new_discriminator"]


# ══════════════════════════════════════════════════════════════════
#  batch_quotes_by_id
# ══════════════════════════════════════════════════════════════════

class TestBatchQuotesById:
    def test_batching(self, cmc_client):
        """With MAX_SYMBOLS_PER_REQUEST=100, 250 ids should make 3 requests."""
        ids = list(range(250))
        cmc_client.session.request.return_value.json.return_value = {"data": {}}
        result = cmc_client.batch_quotes_by_id(ids)
        assert cmc_client.session.request.call_count == 3


# ══════════════════════════════════════════════════════════════════
#  needs_re_validate
# ══════════════════════════════════════════════════════════════════

class TestNeedsReValidate:
    def test_no_cache_means_needs_validation(self, cmc_client):
        assert cmc_client.needs_re_validate() is True

    def test_fresh_cache_does_not_need_validation(self, cmc_client):
        cmc_client.cache["built_at"] = datetime.now(timezone.utc).isoformat()
        assert cmc_client.needs_re_validate() is False

    def test_expired_cache_needs_validation(self, cmc_client):
        old = datetime.now(timezone.utc) - timedelta(days=31)
        cmc_client.cache["built_at"] = old.isoformat()
        assert cmc_client.needs_re_validate() is True
