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
