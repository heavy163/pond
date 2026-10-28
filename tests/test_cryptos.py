"""cryptos 采集层单测（不联网、不写库）。

覆盖三个最容易出错、且改一次会静默错很久的点：
  1. include 精确匹配（BTCUSDT 不能带上 WBTCUSDT）
  2. GCRA 限频（速率准确 + 无惊群）
  3. 配置三级优先级的覆盖方向（DB > YAML > 默认）
"""

from __future__ import annotations

import asyncio
import time

import pytest

from pond.cryptos.config import BucketSpec, CryptosConfig, VenueSpec
from pond.cryptos.limiter import GcraLimiter
from pond.cryptos.schema import snap_to_hour, to_utc


def test_include_exact_does_not_catch_prefix():
    spec = VenueSpec(name="binance", symbols_include=("BTCUSDT",))
    codes = ["BTCUSDT", "WBTCUSDT", "BTCUSDT_261225", "ETHUSDT"]
    assert spec.select_symbols(codes) == ["BTCUSDT"]


def test_include_fragment_still_works():
    spec = VenueSpec(name="binance", symbols_include=("ETH",))
    codes = ["BTCUSDT", "ETHUSDT", "ETHBTC", "SOLUSDT"]
    assert spec.select_symbols(codes) == ["ETHUSDT", "ETHBTC"]


def test_exclude_and_top_n():
    spec = VenueSpec(name="binance", symbols_include=(), symbols_exclude=("USDC",), top_n=2)
    codes = ["AUSDT", "BUSDC", "CUSDT", "DUSDT"]
    assert spec.select_symbols(codes) == ["AUSDT", "CUSDT"]


def test_bucket_for_falls_back_to_default():
    spec = VenueSpec(
        name="okx",
        buckets={"default": BucketSpec(rate=10), "candles": BucketSpec(rate=40)},
    )
    assert spec.bucket_for("candles").rate == 40
    assert spec.bucket_for("unknown").rate == 10


def test_bucket_rejects_bad_mode():
    with pytest.raises(ValueError):
        BucketSpec(mode="rps", rate=1)


def test_db_overrides_yaml():
    """DB 的 enabled=0 必须压过 YAML 的 enabled=true。"""
    cfg = CryptosConfig(venues={"binance": VenueSpec(name="binance", enabled=True)})
    overlaid = cfg._overlay_db([{"venue": "binance", "enabled": 0}])
    assert overlaid.venues["binance"].enabled is False


def test_db_empty_fields_do_not_override():
    cfg = CryptosConfig(venues={"binance": VenueSpec(name="binance", concurrency=8)})
    overlaid = cfg._overlay_db([{"venue": "binance", "concurrency": 0, "data": ""}])
    assert overlaid.venues["binance"].concurrency == 8


def test_gcra_rate_is_accurate():
    async def run():
        lim = GcraLimiter(50)               # 50/s
        t0 = time.monotonic()
        for _ in range(25):
            await lim.acquire()
        return time.monotonic() - t0

    elapsed = asyncio.run(run())
    # 25 个请求、50/s、burst=1 → 第 1 个立即放行，其余 24 个间隔 20ms → 0.48s
    assert abs(elapsed - 24 / 50) < 0.08, elapsed


def test_gcra_no_thundering_herd():
    async def run():
        lim = GcraLimiter(100)
        stamps = []

        async def one():
            await lim.acquire()
            stamps.append(time.monotonic())

        await asyncio.gather(*[one() for _ in range(10)])
        stamps.sort()
        return [b - a for a, b in zip(stamps, stamps[1:])]

    gaps = asyncio.run(run())
    # 100/s → 每个间隔 10ms；若无惊群，所有间隔都应接近 10ms
    assert max(gaps) < 0.03, gaps
    assert min(gaps) > 0.004, gaps


def test_snap_to_hour_removes_ms_jitter():
    dt = to_utc(1793116800002)               # Binance 实测值，带 2ms 抖动
    assert snap_to_hour(dt).microsecond == 0
    assert snap_to_hour(dt).minute == 0
    assert int(snap_to_hour(dt).timestamp()) == 1793116800


def test_store_refuses_non_cryptos_database():
    from pond.cryptos.store import CryptoStore

    with pytest.raises(ValueError, match="只允许写 cryptos"):
        CryptoStore(host="127.0.0.1", database="quant")


# ---------------- select_instruments：按 base 匹配（实测踩过的坑）----------------


def _rows():
    return [
        {"raw_code": "BTCUSDT", "base": "BTC"},
        {"raw_code": "WBTCUSDT", "base": "WBTC"},
        {"raw_code": "AIXBT_USDT", "base": "AIXBT"},
        {"raw_code": "ETHFI_USDT", "base": "ETHFI"},
        {"raw_code": "ETHUSDT", "base": "ETH"},
        {"raw_code": "XBTUSDTM", "base": "BTC"},     # KuCoin 用 XBT 表示 BTC
    ]


def test_select_instruments_by_base_not_substring():
    """写 BTC 只应拿到 base=BTC 的两个，不能带上 WBTC / AIXBT。"""
    spec = VenueSpec(name="gate", symbols_include=("BTC",))
    assert sorted(spec.select_instruments(_rows())) == ["BTCUSDT", "XBTUSDTM"]


def test_select_instruments_exact_raw_code():
    spec = VenueSpec(name="gate", symbols_include=("ETHUSDT",))
    assert spec.select_instruments(_rows()) == ["ETHUSDT"]


def test_select_instruments_fragment_fallback():
    """include 里没有任何 base/raw 命中时，退化为子串匹配（如写 'ETH' 想找 ETHFI）。"""
    spec = VenueSpec(name="gate", symbols_include=("ETHFI",))
    assert spec.select_instruments(_rows()) == ["ETHFI_USDT"]


def test_select_instruments_exclude_base():
    spec = VenueSpec(name="gate", symbols_exclude=("WBTC", "AIXBT"))
    got = spec.select_instruments(_rows())
    assert "WBTCUSDT" not in got and "AIXBT_USDT" not in got
    assert "BTCUSDT" in got


# ---------------- 回灌计划：各所 OI 深度上限 ----------------


def _runner():
    from pond.cryptos.backfill import BackfillRunner
    from pond.cryptos.config import CryptosConfig

    cfg = CryptosConfig.load("configs/cryptos.yaml")
    return BackfillRunner(store=None, config=cfg, execute=False)


def test_backfill_caps_gate_oi_at_179_days():
    """Gate 的 180 天是硬边界，必须留余量，否则 400。"""
    runner = _runner()
    _spec, targets = runner.plan("gate", ["oi"], 365)
    assert "oi" in targets
    days = (runner_now := __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc)) - targets["oi"][0]
    assert 178 <= days.days <= 180, days


def test_backfill_skips_venues_without_oi_history():
    """OKX 只有当前值，请求回灌应被跳过而不是静默返回 0 行。"""
    runner = _runner()
    _spec, targets = runner.plan("okx", ["oi"], 30)
    assert targets == {}


def test_backfill_rate_is_halved():
    """回灌必须降速，不能挤占实时配额。"""
    runner = _runner()
    spec, _ = runner.plan("gate", ["oi"], 30)
    live = runner.config.get("gate")
    for k, b in spec.buckets.items():
        assert b.rate <= live.buckets[k].rate * 0.5 + 1e-9


# ---------------- 缺口可修复性分类 ----------------


def _detector():
    from pond.cryptos.config import CryptosConfig
    from pond.cryptos.gaps import GapDetector

    return GapDetector(store=None, config=CryptosConfig.load("configs/cryptos.yaml"))


def test_oi_repairability_matches_exchange_history_depth():
    """只有 Gate(179d)/Bybit(7d) 能补 OI，其余 6 家永久丢失。"""
    import datetime as dt

    d = _detector()
    today = dt.datetime.now(dt.timezone.utc).date()
    assert d._repairable("gate", "oi", today) is True
    assert d._repairable("bybit", "oi", today) is True
    for v in ("okx", "bitget", "kucoin", "mexc", "htx", "hyperliquid"):
        assert d._repairable(v, "oi", today) is False, v


def test_oi_beyond_exchange_window_is_lost():
    import datetime as dt

    d = _detector()
    today = dt.datetime.now(dt.timezone.utc).date()
    assert d._repairable("gate", "oi", today - dt.timedelta(days=200)) is False
    assert d._repairable("bybit", "oi", today - dt.timedelta(days=30)) is False
    # K线永远可补（交易所都给全历史）
    assert d._repairable("okx", "kline", today - dt.timedelta(days=2000)) is True


def test_lost_reason_is_explicit():
    import datetime as dt

    d = _detector()
    today = dt.datetime.now(dt.timezone.utc).date()
    reason = d._why_lost("okx", "oi", today)
    assert "无历史接口" in reason and "永久" in reason


# ---------------- 下游取数契约 ----------------


def test_missing_hours_compress_to_ranges():
    import datetime as dt

    from pond.cryptos.quality import QualityGate

    base = dt.datetime(2026, 10, 27, tzinfo=dt.timezone.utc)
    hours = [base + dt.timedelta(hours=h) for h in (0, 1, 2, 5, 6, 10)]
    got = QualityGate._compress(hours)
    assert len(got) == 3
    assert got[0] == (base, base + dt.timedelta(hours=3))
    assert got[2] == (base + dt.timedelta(hours=10), base + dt.timedelta(hours=11))


def test_kline_timestamp_column_is_open_time():
    """K线的 datetime 是 close_time(xx:59:59.999)，跨表对齐必须用 open_time。"""
    from pond.cryptos.quality import TS_COL

    assert TS_COL["kline"] == "open_time"
    assert TS_COL["oi"] == "datetime"
    assert TS_COL["funding"] == "datetime"


def test_oi_repairable_helper_matches_backfill_caps():
    import datetime as dt

    from pond.cryptos.quality import oi_repairable

    today = dt.datetime.now(dt.timezone.utc).date()
    assert oi_repairable("gate", today) is True
    assert oi_repairable("gate", today - dt.timedelta(days=200)) is False
    assert oi_repairable("bybit", today) is True
    assert oi_repairable("okx", today) is False
