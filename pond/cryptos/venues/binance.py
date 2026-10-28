"""Binance USDT-M 永续适配器。

实测确认（2026-10）：
  - fapi.binance.com             : exchangeInfo 936 个合约，523 个 USDT 永续 TRADING
  - klines / fundingRate / markPriceKlines 均可用
  - 持仓量历史在 fapi.binance.com/futures/data/openInterestHist（只留 30 天）
  - 现货行情必须走 data-api.binance.vision（api.binance.com 在本网络 DNS 被污染）

K线 weight 随 limit 变化：≤100→1, ≤500→2, ≤1000→5, ≤1500→10。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000


class BinanceAdapter(VenueAdapter):
    name = "binance"
    FAPI = "https://fapi.binance.com"
    SPOT = "https://data-api.binance.vision"

    def __init__(self, spec):
        super().__init__(spec)
        self._code_map: dict[str, str] = {}

    # ---------------- 元数据 ----------------

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(f"{self.FAPI}/fapi/v1/exchangeInfo", endpoint="meta", cost=1)
        rows: list[dict[str, Any]] = []
        for s in d.get("symbols", []):
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("status") != "TRADING":
                continue
            if s.get("quoteAsset") != "USDT":
                continue
            base, quote = s["baseAsset"], s["quoteAsset"]
            code = f"{base}-{quote}"
            self._code_map[s["symbol"]] = code
            rows.append(
                {
                    "venue": self.name,
                    "code": code,
                    "raw_code": s["symbol"],
                    "base": base,
                    "quote": quote,
                    "contract_type": "perp",
                    "multiplier": 1.0,
                    "listed_at": to_utc(s.get("onboardDate") or 0),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[binance] instruments: {len(rows)} 个 USDT 永续")
        return rows

    def resolve_code(self, raw_code: str) -> str:
        if raw_code in self._code_map:
            return self._code_map[raw_code]
        # 兜底：BTCUSDT → BTC-USDT（99% 的 USDT 对适用）
        if raw_code.endswith("USDT") and len(raw_code) > 4:
            return f"{raw_code[:-4]}-USDT"
        return raw_code

    # ---------------- K线 ----------------

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        url = (
            f"{self.FAPI}/fapi/v1/klines"
            if market == "perp"
            else f"{self.SPOT}/api/v3/klines"
        )
        code = self.resolve_code(raw_code)
        out: list[dict[str, Any]] = []
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        table = "kline_futures_1h" if market == "perp" else "kline_spot_1h"

        while cursor < end_ms:
            limit = 1500
            rows = await self.request(
                url,
                {
                    "symbol": raw_code,
                    "interval": "1h",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": limit,
                },
                endpoint="kline",
                cost=self._weight(limit),
            )
            if not rows:
                break
            now = datetime.now(timezone.utc)
            for r in rows:
                open_time = to_utc(r[0])
                if open_time >= end:      # 只收闭合 bar
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "open_time": open_time,
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "datetime": to_utc(r[6]),
                        "quote_volume": float(r[7]),
                        "count": float(r[8]),
                        "taker_buy_volume": float(r[9]),
                        "taker_buy_quote_volume": float(r[10]),
                        "ingested_at": now,
                    }
                )
            nxt = int(rows[-1][0]) + HOUR_MS
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < limit:
                break
        return out

    # ---------------- funding ----------------

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        raw: list[dict[str, Any]] = []

        while cursor < end_ms:
            rows = await self.request(
                f"{self.FAPI}/fapi/v1/fundingRate",
                {
                    "symbol": raw_code,
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": 1000,
                },
                endpoint="funding",
                cost=1,
            )
            if not rows:
                break
            raw.extend(rows)
            nxt = int(rows[-1]["fundingTime"]) + 1
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 1000:
                break

        if not raw:
            return []
        times = sorted(int(r["fundingTime"]) for r in raw)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval_sec = int(median(diffs) / 1000) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(r["fundingTime"])),
                "fundingRate": float(r["fundingRate"]),
                "markPrice": float(r.get("markPrice") or 0.0),
                "interval_sec": interval_sec,
                "ingested_at": now,
            }
            for r in raw
        ]

    # ---------------- 持仓量 ----------------

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        while cursor < end_ms:
            rows = await self.request(
                f"{self.FAPI}/futures/data/openInterestHist",
                {
                    "symbol": raw_code,
                    "period": "1h",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": 500,
                },
                endpoint="oi",
                cost=1,
            )
            if not rows:
                break
            for r in rows:
                ts = to_utc(r["timestamp"])
                if ts >= end:
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ts,
                        "sumOpenInterest": float(r["sumOpenInterest"]),
                        "sumOpenInterestValue": float(r["sumOpenInterestValue"]),
                        "ingested_at": now,
                    }
                )
            nxt = int(rows[-1]["timestamp"]) + HOUR_MS
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 500:
                break
        return out

    # ---------------- 标记价 ----------------

    async def mark_price(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor < end_ms:
            rows = await self.request(
                f"{self.FAPI}/fapi/v1/markPriceKlines",
                {
                    "symbol": raw_code,
                    "interval": "1h",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": 1500,
                },
                endpoint="kline",
                cost=2,
            )
            if not rows:
                break
            for r in rows:
                ot = to_utc(r[0])
                if ot >= end:
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ot,
                        "markPrice": float(r[4]),
                        "indexPrice": None,   # Binance 该接口不含指数价
                        "ingested_at": now,
                    }
                )
            nxt = int(rows[-1][0]) + HOUR_MS
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 1500:
                break
        return out

    @staticmethod
    def _weight(limit: int) -> float:
        if limit <= 100:
            return 1.0
        if limit <= 500:
            return 2.0
        if limit <= 1000:
            return 5.0
        return 10.0
