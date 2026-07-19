"""Tests for CMCUnlockClient."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

from pond.token_unlock.client import CMCUnlockClient, NextUnlock, TokenUnlockEntry


# ══════════════════════════════════════════════════════════════════
#  Fixtures
# ══════════════════════════════════════════════════════════════════


@pytest.fixture
def client() -> CMCUnlockClient:
    """Return a CMCUnlockClient with a mocked session."""
    c = CMCUnlockClient()
    c.session = MagicMock()
    return c


SAMPLE_CMC_RESPONSE = {
    "data": {
        "tokenUnlockList": [
            {
                "cryptoId": 24482,
                "symbol": "ZRO",
                "slug": "layerzero",
                "name": "LayerZero",
                "circulatingSupply": 354010000.0,
                "totalSupply": 1000000000.0,
                "maxSupply": 1000000000.0,
                "totalUnlockedPercentage": 64.13,
                "nextUnlocked": {
                    "tokenAmount": 32600000.0,
                    "tokenAmountUsd": 26460000.0,
                    "tokenAmountPercentage": 3.26,
                    "date": 1784505600000,  # 2026-07-20 00:00 UTC
                },
                "quotes": [{"price": 0.8114, "marketCap": 287000000.0}],
            },
            {
                "cryptoId": 39127,
                "symbol": "LISA",
                "slug": "agentlisa",
                "name": "AgentLISA",
                "circulatingSupply": 216225000.0,
                "totalSupply": 1000000000.0,
                "maxSupply": None,
                "totalUnlockedPercentage": 37.79,
                "nextUnlocked": {
                    "tokenAmount": 29108333.0,
                    "tokenAmountUsd": 42397.0,
                    "tokenAmountPercentage": 2.91,
                    "date": 1784484000000,  # 2026-07-19 18:00 UTC
                },
                "quotes": [{"price": 0.001456, "marketCap": 314938.0}],
            },
            {
                "cryptoId": 88888,
                "symbol": "NOLOCK",
                "slug": "no-lock",
                "name": "No Lock",
                "circulatingSupply": 1000000.0,
                "totalSupply": 1000000.0,
                "maxSupply": None,
                "totalUnlockedPercentage": 100.0,
                "nextUnlocked": None,
                "quotes": [{"price": 1.0, "marketCap": 1000000.0}],
            },
        ],
        "totalCount": "406",
    },
    "status": {"error_code": "0", "error_message": "SUCCESS"},
}


# ══════════════════════════════════════════════════════════════════
#  NextUnlock
# ══════════════════════════════════════════════════════════════════


class TestNextUnlock:
    def test_from_dict(self):
        d = {
            "tokenAmount": 32600000.0,
            "tokenAmountUsd": 26460000.0,
            "tokenAmountPercentage": 3.26,
            "date": 1784505600000,
        }
        nu = NextUnlock.from_dict(d)
        assert nu.token_amount == 32600000.0
        assert nu.token_amount_usd == 26460000.0
        assert nu.token_amount_pct == 3.26
        assert nu.date == datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)


# ══════════════════════════════════════════════════════════════════
#  TokenUnlockEntry
# ══════════════════════════════════════════════════════════════════


class TestTokenUnlockEntry:
    def test_from_cmc_item_with_unlock(self):
        item = SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][0]
        entry = TokenUnlockEntry.from_cmc_item(item)
        assert entry.crypto_id == 24482
        assert entry.symbol == "ZRO"
        assert entry.slug == "layerzero"
        assert entry.name == "LayerZero"
        assert entry.circulating_supply == 354010000.0
        assert entry.total_supply == 1000000000.0
        assert entry.max_supply == 1000000000.0
        assert entry.total_unlocked_pct == 64.13
        assert entry.next_unlock is not None
        assert entry.next_unlock.token_amount_pct == 3.26
        assert entry.price == 0.8114
        assert entry.market_cap == 287000000.0

    def test_from_cmc_item_no_unlock(self):
        item = SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][2]
        entry = TokenUnlockEntry.from_cmc_item(item)
        assert entry.symbol == "NOLOCK"
        assert entry.next_unlock is None

    def test_from_cmc_item_null_supplies(self):
        """maxSupply=None should map to None, not 0."""
        item = SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][1]
        entry = TokenUnlockEntry.from_cmc_item(item)
        assert entry.max_supply is None
        assert entry.total_supply is not None


# ══════════════════════════════════════════════════════════════════
#  CMCUnlockClient
# ══════════════════════════════════════════════════════════════════


class TestCMCUnlockClient:
    def test_fetch_upcoming_success(self, client):
        """Should parse CMC response into TokenUnlockEntry list."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_CMC_RESPONSE
        client.session.request.return_value = mock_resp

        entries = client.fetch_upcoming(limit=100)
        assert len(entries) == 3
        assert entries[0].symbol == "ZRO"
        assert entries[1].symbol == "LISA"
        assert entries[2].symbol == "NOLOCK"

    def test_fetch_upcoming_http_error_retry(self, client):
        """Should retry on 5xx, then raise."""
        client.session.request.side_effect = requests.HTTPError(
            "Server Error", response=MagicMock(status_code=500)
        )
        with pytest.raises(RuntimeError):
            client.fetch_upcoming()

    def test_fetch_upcoming_rate_limit_retry_then_ok(self, client):
        """Should retry on 429, then succeed."""
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        fail_resp.raise_for_status.side_effect = requests.HTTPError(
            "Rate Limited", response=fail_resp
        )
        ok_resp = MagicMock()
        ok_resp.json.return_value = SAMPLE_CMC_RESPONSE

        client.session.request.side_effect = [fail_resp, ok_resp]
        entries = client.fetch_upcoming()
        assert len(entries) == 3

    def test_timeout_defaults(self):
        """Default timeout should be 30, max_retries 3."""
        c = CMCUnlockClient()
        assert c.timeout == 30
        assert c.max_retries == 3

    def test_custom_timeout(self):
        c = CMCUnlockClient(timeout=15, max_retries=5)
        assert c.timeout == 15
        assert c.max_retries == 5

    def test_fetch_all_deduplicates(self, client):
        """fetch_all should combine 4 calls and deduplicate by symbol."""
        resp_zro = MagicMock()
        resp_zro.json.return_value = {
            "data": {
                "tokenUnlockList": [SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][0]]
            },
            "status": {"error_code": "0"},
        }
        resp_lisa = MagicMock()
        resp_lisa.json.return_value = {
            "data": {
                "tokenUnlockList": [SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][1]]
            },
            "status": {"error_code": "0"},
        }

        # 4 calls: first two return ZRO, last two return LISA (test dedup)
        client.session.request.side_effect = [resp_zro, resp_zro, resp_lisa, resp_lisa]
        entries = client.fetch_all()
        assert len(entries) == 2  # ZRO + LISA, no duplicates
        assert {e.symbol for e in entries} == {"ZRO", "LISA"}

    def test_fetch_all_partial_failure(self, client):
        """fetch_all should gracefully handle sub-call failures."""
        ok_resp = MagicMock()
        ok_resp.json.return_value = {
            "data": {
                "tokenUnlockList": [SAMPLE_CMC_RESPONSE["data"]["tokenUnlockList"][0]]
            },
            "status": {"error_code": "0"},
        }
        fail = requests.HTTPError("Fail", response=MagicMock(status_code=500))
        # First param combo: 3 retries all fail → RuntimeError
        # Remaining 3 param combos: each succeeds on first try
        client.session.request.side_effect = [
            fail,
            fail,
            fail,
            ok_resp,
            ok_resp,
            ok_resp,
        ]
        entries = client.fetch_all()
        assert len(entries) == 1  # Only got ZRO from successful calls
        assert entries[0].symbol == "ZRO"


# ══════════════════════════════════════════════════════════════════
#  get_upcoming_unlocks 快捷函数
# ══════════════════════════════════════════════════════════════════


class TestGetUpcomingUnlocks:
    @patch("pond.token_unlock.client.CMCUnlockClient")
    def test_filters_by_window(self, mock_client_class):
        """Should filter entries outside the time window."""
        mock_instance = mock_client_class.return_value
        mock_instance.fetch_upcoming.return_value = [
            TokenUnlockEntry(
                crypto_id=1,
                symbol="NOW",
                slug="now",
                name="Now",
                circulating_supply=1e6,
                total_supply=1e6,
                max_supply=1e6,
                total_unlocked_pct=50.0,
                next_unlock=NextUnlock(
                    token_amount=1000,
                    token_amount_usd=5000,
                    token_amount_pct=2.0,
                    date=datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                price=5.0,
                market_cap=5e6,
            ),
            TokenUnlockEntry(
                crypto_id=2,
                symbol="LATER",
                slug="later",
                name="Later",
                circulating_supply=1e6,
                total_supply=1e6,
                max_supply=1e6,
                total_unlocked_pct=50.0,
                next_unlock=NextUnlock(
                    token_amount=1000,
                    token_amount_usd=5000,
                    token_amount_pct=2.0,
                    date=datetime.now(timezone.utc) + timedelta(days=30),
                ),
                price=5.0,
                market_cap=5e6,
            ),
        ]

        # Import here to use patched client
        from pond.token_unlock.client import get_upcoming_unlocks

        entries = get_upcoming_unlocks(min_unlock_pct=0.0, window_days=7)
        assert len(entries) == 1
        assert entries[0].symbol == "NOW"
