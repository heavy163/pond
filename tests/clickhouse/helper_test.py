"""
Tests for FuturesHelper data sync interfaces using mocks.
All external dependencies (ClickHouse, Binance API, CoinGecko, etc.) are replaced with mocks.
"""
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import polars as pl
from datetime import datetime, timedelta

from pond.clickhouse.helper import FuturesHelper
from pond.clickhouse.kline import (
    FuturesKline1H,
    FuturesKline4H,
    FuturesKline5m,
    FuturesKline15m,
    FuturesKline1d,
    FutureInfo,
    FutureFundingRate,
    TokenHolders,
    FutureLongShortRatio,
    FutureOpenInterest,
    FutureLongShortPositionRatio,
)

# ------------------------------------------------------------------------
#  Mock data
# ------------------------------------------------------------------------

MOCK_EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "onboardDate": int(datetime(2020, 1, 1).timestamp() * 1000),
        },
        {
            "symbol": "ETHUSDT",
            "pair": "ETHUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "ETH",
            "onboardDate": int(datetime(2020, 1, 1).timestamp() * 1000),
        },
        {
            "symbol": "BNBUSDT",
            "pair": "BNBUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BNB",
            "onboardDate": int(datetime(2020, 1, 1).timestamp() * 1000),
        },
    ]
}

MOCK_KLINE_RAW = [
    1700000000000,
    50000.0,
    51000.0,
    49000.0,
    50500.0,
    1000.0,
    1700003600000,
    50000000.0,
    10000,
    500.0,
    25000000.0,
    "0",
]

MOCK_FUNDING_RATE_RAW = {
    "symbol": "BTCUSDT",
    "fundingRate": "0.0001",
    "fundingTime": 1700000000000,
    "markPrice": "50000.0",
}

MOCK_COIN_MARKET_DATA = {
    "total_supply": 21000000.0,
    "market_cap_fdv_ratio": 0.85,
}

MOCK_EXTRA_INFO_DF = pd.DataFrame(
    {
        "close_time": [datetime(2024, 1, 1)],
        "longAccount": [0.5],
        "shortAccount": [0.3],
        "longShortRatio": [1.5],
    }
)

# pl.DataFrame for save_klines_from_ws tests
MOCK_WS_KLINES_PL = pl.DataFrame(
    {
        "pair": ["BTCUSDT"],
        "open_time": [datetime(2024, 1, 1, 0, 0)],
        "close_time": [datetime(2024, 1, 1, 1, 0)],
    }
).with_columns(
    pl.col("open_time").dt.replace_time_zone("UTC"),
    pl.col("close_time").dt.replace_time_zone("UTC"),
)


# ------------------------------------------------------------------------
#  Fixtures
# ------------------------------------------------------------------------

@pytest.fixture
def mock_clickhouse():
    ch = MagicMock()
    ch.data_start = datetime(2020, 1, 1)
    ch.read_latest_n_record.return_value = pd.DataFrame(
        columns=["code", "datetime"]
    )
    return ch


@pytest.fixture
def mock_crypto_db():
    return MagicMock()


@pytest.fixture
def helper(mock_clickhouse, mock_crypto_db):
    """Build a FuturesHelper where every external dependency is mocked."""
    with patch.multiple(
        "pond.clickhouse.helper",
        DirectDataProxy=MagicMock(),
        AsyncDirectDataProxy=MagicMock(),
        CoinGeckoIDMapper=MagicMock(),
        BinanceContractTool=MagicMock(),
        ChainbaseClient=MagicMock(),
        Client=MagicMock(),
        BinanceWSClientWrapper=MagicMock(),
    ):
        h = FuturesHelper(
            mock_crypto_db,
            mock_clickhouse,
            fix_kline_with_cryptodb=False,
        )

        h.data_proxy.um_future_exchange_info.return_value = MOCK_EXCHANGE_INFO

        async def mock_klines(*a, **kw):
            return [MOCK_KLINE_RAW]

        async def mock_funding(*a, **kw):
            return [MOCK_FUNDING_RATE_RAW]

        h.async_data_proxy.um_future_klines = mock_klines
        h.async_data_proxy.um_future_funding_rate = mock_funding

        # gecko_id_mapper and contact_tool are class-level attributes set
        # at import time, so patch.multiple cannot replace them.
        # Override after construction.
        h.gecko_id_mapper = MagicMock()
        h.contact_tool = MagicMock()

        # binance_wss and dict_exchange_info are class-level mutable dicts
        # shared across instances; reset so no state leaks between tests.
        h.binance_wss = {}
        h.dict_exchange_info = {}

        yield h


# ------------------------------------------------------------------------
#  get_futures_table  –  table routing
# ------------------------------------------------------------------------

class TestGetFuturesTable:
    def test_kline_intervals(self, helper):
        assert helper.get_futures_table("4h", "kline") is FuturesKline4H
        assert helper.get_futures_table("1h", "kline") is FuturesKline1H
        assert helper.get_futures_table("5m", "kline") is FuturesKline5m
        assert helper.get_futures_table("15m", "kline") is FuturesKline15m
        assert helper.get_futures_table("1d", "kline") is FuturesKline1d

    def test_info(self, helper):
        assert helper.get_futures_table("1d", "info") is FutureInfo

    def test_funding_rate(self, helper):
        assert helper.get_futures_table("1h", "funding_rate") is FutureFundingRate

    def test_holders(self, helper):
        assert helper.get_futures_table("1d", "holders") is TokenHolders

    def test_unknown(self, helper):
        assert helper.get_futures_table("x", "kline") is None


# ------------------------------------------------------------------------
#  get_exchange_info  –  caching
# ------------------------------------------------------------------------

class TestGetExchangeInfo:
    def test_caches_by_date(self, helper):
        s = datetime(2024, 1, 1)
        r1 = helper.get_exchange_info(s)
        r2 = helper.get_exchange_info(s)
        assert helper.data_proxy.um_future_exchange_info.call_count == 1
        assert r1 is r2

    def test_different_date_fetches_again(self, helper):
        helper.get_exchange_info(datetime(2024, 1, 1))
        helper.get_exchange_info(datetime(2024, 1, 2))
        assert helper.data_proxy.um_future_exchange_info.call_count == 2


# ------------------------------------------------------------------------
#  get_perpetual_symbols  –  symbol filtering
# ------------------------------------------------------------------------

class TestGetPerpetualSymbols:
    def test_filters_correctly(self, helper):
        syms = helper.get_perpetual_symbols(datetime(2024, 6, 1))
        assert len(syms) == 3
        for s in syms:
            assert s["contractType"] == "PERPETUAL"
            assert s["pair"].endswith("USDT")
            assert s["status"] == "TRADING"

    def test_excludes_recently_onboarded(self, helper):
        recent = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
        helper.data_proxy.um_future_exchange_info.return_value = {
            "symbols": [
                {
                    "symbol": "NEWUSDT",
                    "pair": "NEWUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "NEW",
                    "onboardDate": recent,
                }
            ]
        }
        assert helper.get_perpetual_symbols(datetime(2024, 6, 1)) == []

    def test_api_error_returns_none(self, helper):
        helper.data_proxy.um_future_exchange_info.side_effect = Exception("boom")
        assert helper.get_perpetual_symbols(datetime(2024, 6, 1)) is None


# ------------------------------------------------------------------------
#  sync  –  kline
# ------------------------------------------------------------------------

class TestSyncKline:
    def test_success(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = pd.DataFrame(
            {"cnt": [3]}
        )
        assert helper.sync_futures_kline("1h", workers=3) is True
        assert mock_clickhouse.save_to_db.called

    def test_no_symbols_returns_false(self, helper, mock_clickhouse):
        helper.data_proxy.um_future_exchange_info.return_value = {"symbols": []}
        assert helper.sync_futures_kline("1h", workers=3) is False
        assert not mock_clickhouse.save_to_db.called

    def test_verification_fails_when_count_is_low(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = pd.DataFrame(
            {"cnt": [0]}
        )
        assert helper.sync_futures_kline("1h", workers=3) is False

    def test_allow_missing_count_tolerates_gaps(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = pd.DataFrame(
            {"cnt": [2]}
        )
        assert helper.sync("1h", workers=3, what="kline", allow_missing_count=1) is True

    def test_verification_returns_empty_df(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = pd.DataFrame()
        assert helper.sync_futures_kline("1h", workers=3) is False

    def test_verification_returns_none(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = None
        assert helper.sync_futures_kline("1h", workers=3) is False


# ------------------------------------------------------------------------
#  sync  –  funding_rate
# ------------------------------------------------------------------------

class TestSyncFundingRate:
    def test_success(self, helper, mock_clickhouse):
        mock_clickhouse.native_sql_read_table.return_value = pd.DataFrame(
            {"cnt": [3]}
        )
        assert helper.sync("1h", workers=3, what="funding_rate") is True
        assert mock_clickhouse.save_to_db.called


# ------------------------------------------------------------------------
#  sync  –  info
# ------------------------------------------------------------------------

class TestSyncInfo:
    def test_success(self, helper, mock_clickhouse):
        with patch("pond.clickhouse.helper.get_coin_market_data") as m:
            m.return_value = MOCK_COIN_MARKET_DATA
            helper.gecko_id_mapper.get_coingecko_id.return_value = "bitcoin"
            assert helper.sync_futures_info("1d", workers=3) is True
            assert mock_clickhouse.save_to_db.called

    def test_empty_coingecko_id_means_noop(self, helper, mock_clickhouse):
        helper.gecko_id_mapper.get_coingecko_id.return_value = ""
        assert helper.sync_futures_info("1d", workers=3) is True
        assert mock_clickhouse.save_to_db.called


# ------------------------------------------------------------------------
#  sync  –  holders
# ------------------------------------------------------------------------

class TestSyncHolders:
    def test_success(self, helper, mock_clickhouse):
        helper.gecko_id_mapper.get_coingecko_id.return_value = "bitcoin"
        helper.contact_tool.get_token_chain_info.return_value = {
            "ethereum": "0xabc"
        }
        helper.chainbase_client.get_topn_holders.return_value = [
            {"wallet_address": "0xabc", "amount": 1000.0, "usd_value": 5e7}
        ]
        assert helper.sync("1d", workers=3, what="holders") is True
        assert mock_clickhouse.save_to_db.called

    def test_skip_when_data_up_to_date(self, helper, mock_clickhouse):
        helper.gecko_id_mapper.get_coingecko_id.return_value = "bitcoin"
        helper.contact_tool.get_token_chain_info.return_value = {
            "ethereum": "0xabc"
        }
        helper.chainbase_client.get_topn_holders.return_value = [
            {"wallet_address": "0xabc", "amount": 1000.0, "usd_value": 5e7}
        ]
        now = datetime.now()
        mock_clickhouse.read_latest_n_record.return_value = pd.DataFrame(
            {"code": ["BTCUSDT"], "datetime": [now]}
        )
        assert helper.sync("1d", workers=3, what="holders") is True


# ------------------------------------------------------------------------
#  sync  –  extra info (long_short_ratio, open_interest, position_ratio)
# ------------------------------------------------------------------------

class TestSyncExtraInfo:
    @pytest.fixture
    def mock_funcs(self):
        with patch("pond.clickhouse.helper.get_long_short_account_ratio_history") as m1:
            with patch("pond.clickhouse.helper.get_long_short_position_ratio_history") as m2:
                with patch("pond.clickhouse.helper.get_open_interest_history") as m3:
                    m1.return_value = MOCK_EXTRA_INFO_DF
                    m2.return_value = MOCK_EXTRA_INFO_DF
                    m3.return_value = MOCK_EXTRA_INFO_DF
                    yield {"lsr": m1, "lsp": m2, "oi": m3}

    def test_long_short_ratio(self, helper, mock_clickhouse, mock_funcs):
        assert helper.sync("1h", workers=3, what="long_short_ratio") is True
        assert mock_clickhouse.save_to_db.called

    def test_long_short_position_ratio(self, helper, mock_clickhouse, mock_funcs):
        assert (
            helper.sync("1h", workers=3, what="long_short_position_ratio")
            is True
        )
        assert mock_clickhouse.save_to_db.called

    def test_open_interest(self, helper, mock_clickhouse, mock_funcs):
        assert helper.sync("1h", workers=3, what="open_interest") is True
        assert mock_clickhouse.save_to_db.called

    def test_allow_missing_count_tolerates_failures(
        self, helper, mock_clickhouse, mock_funcs
    ):
        mock_funcs["lsr"].return_value = None
        assert (
            helper.sync(
                "1h", workers=3, what="long_short_ratio", allow_missing_count=3
            )
            is True
        )


# ------------------------------------------------------------------------
#  set_data_prox
# ------------------------------------------------------------------------

class TestSetDataProxy:
    def test_swap(self, helper):
        new = MagicMock()
        helper.set_data_prox(new)
        assert helper.data_proxy is new


# ------------------------------------------------------------------------
#  WebSocket subscribe / unsubscribe
# ------------------------------------------------------------------------

class TestSubscribeUnsubscribe:
    def test_subscribe_creates_client(self, helper):
        helper.subscribe_futures("1h")
        from pond.clickhouse.helper import BinanceWSClientWrapper

        BinanceWSClientWrapper.assert_called_once()
        assert "1h" in helper.binance_wss
        assert helper.binance_wss["1h"] is not None

    def test_subscribe_twice_no_duplicate(self, helper):
        helper.subscribe_futures("1h")
        helper.subscribe_futures("1h")
        from pond.clickhouse.helper import BinanceWSClientWrapper

        BinanceWSClientWrapper.assert_called_once()

    def test_unsubscribe_stops_and_clears(self, helper):
        helper.subscribe_futures("1h")
        ws = helper.binance_wss["1h"]
        helper.unsubscribe_futures("1h")
        ws.stop_all.assert_called_once()
        assert helper.binance_wss["1h"] is None

    def test_unsubscribe_without_subscribe_does_nothing(self, helper):
        helper.unsubscribe_futures("1h")


# ------------------------------------------------------------------------
#  save_klines_from_ws
# ------------------------------------------------------------------------

class TestSaveKlinesFromWS:
    def test_no_client(self, helper):
        helper.save_klines_from_ws("1h")

    def test_client_returns_none(self, helper):
        helper.subscribe_futures("1h")
        ws = helper.binance_wss["1h"]
        ws.get_aggregated_kline_dataframe.return_value = None
        helper.save_klines_from_ws("1h")

    def test_client_returns_empty(self, helper):
        helper.subscribe_futures("1h")
        ws = helper.binance_wss["1h"]
        ws.get_aggregated_kline_dataframe.return_value = pd.DataFrame()
        helper.save_klines_from_ws("1h")

    def test_client_returns_data(self, helper, mock_clickhouse):
        helper.subscribe_futures("1h")
        ws = helper.binance_wss["1h"]
        ws.get_aggregated_kline_dataframe.return_value = MOCK_WS_KLINES_PL.clone()
        mock_clickhouse.read_latest_n_record.return_value = pd.DataFrame(
            {"code": ["BTCUSDT"], "datetime": [datetime(2023, 12, 31)]}
        )
        helper.save_klines_from_ws("1h")
        assert mock_clickhouse.save_dataframe.called


# ------------------------------------------------------------------------
#  Edge cases: sync with various what= values
# ------------------------------------------------------------------------

class TestSyncEdgeCases:
    def test_sync_unknown_what_no_crash(self, helper, mock_clickhouse):
        """what='unknown' should attempt __sync_futures_extra_info but fails gracefully."""
        with patch("pond.clickhouse.helper.get_long_short_account_ratio_history") as m:
            m.return_value = None
            result = helper.sync("1h", workers=3, what="unknown")
            assert result is not None
