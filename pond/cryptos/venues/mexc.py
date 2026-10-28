"""MEXC 合约。

实测（2026-10）：
  - detail: 1184 个，含 contractSize（BTC_USDT 是 0.0001）
  - kline 是**列式**结构：{time:[秒], open:[], close:[], high:[], low:[], vol:[张], amount:[U]}
    与别家"每行一个数组"完全不同，这是最容易写错的地方
  - funding 分页用 page_num，含 collectCycle（小时）可直接得到 interval_sec
  - OI 无历史，ticker 的 holdVol 是张数，需 × contractSize
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000
HOUR_S = 3600


class MexcAdapter(VenueAdapter):
    name = "mexc"
    BASE = "https://contract.mexc.com/api/v1"

    def __init__(self, spec):
        super().__init__(spec)
        self._size: dict[str, float] = {}     # raw symbol → contractSize

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(f"{self.BASE}/contract/detail", endpoint="meta", cost=2)
        out: list[dict[str, Any]] = []
        for it in d.get("data") or []:
            if it.get("quoteCoin") != "USDT":
                continue
            sym = it["symbol"]                       # BTC_USDT
            size = float(it.get("contractSize") or 1)
            self._size[sym] = size
            out.append(
                {
                    "venue": self.name,
                    "code": sym.replace("_", "-"),
                    "raw_code": sym,
                    "base": it.get("baseCoin") or sym.split("_")[0],
                    "quote": "USDT",
                    "contract_type": "perp",
                    "multiplier": size,
                    "listed_at": to_utc(int(it.get("listingTime") or 0)),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[mexc] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        return raw_code.replace("_", "-")

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        size = self._size.get(raw_code, 1.0)
        d = await self.request(
            f"{self.BASE}/contract/kline/{raw_code}",
            {
                "interval": "Min60",
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
            },
            endpoint="kline",
            cost=2,
        )
        data = d.get("data") or {}
        times = data.get("time") or []
        if not times:
            return []
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for i, t in enumerate(times):
            ts = to_utc(int(t) * 1000)
            if ts < start or ts >= end:
                continue
            vol_contracts = float(data["vol"][i] or 0)
            out.append(
                {
                    "venue": self.name,
                    "code": code,
                    "open_time": ts,
                    "open": float(data["open"][i]),
                    "high": float(data["high"][i]),
                    "low": float(data["low"][i]),
                    "close": float(data["close"][i]),
                    "volume": vol_contracts * size,       # 张 → 币
                    "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                    "quote_volume": float(data.get("amount", [0] * len(times))[i] or 0),
                    "count": 0.0,
                    "taker_buy_volume": 0.0,
                    "taker_buy_quote_volume": 0.0,
                    "ingested_at": now,
                }
            )
        return out

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        raw_rows: list[dict[str, Any]] = []
        page = 1
        while page <= 20:
            d = await self.request(
                f"{self.BASE}/contract/funding_rate/history",
                {"symbol": raw_code, "page_num": page, "page_size": 100},
                endpoint="funding",
                cost=1,
            )
            data = d.get("data") or {}
            rows = data.get("resultList") or []
            if not rows:
                break
            hit_old = False
            for r in rows:
                t = int(r["settleTime"])
                if start_ms <= t < end_ms:
                    raw_rows.append(r)
                if t < start_ms:
                    hit_old = True
            if hit_old or len(rows) < 100:
                break
            page += 1
        if not raw_rows:
            return []
        now = datetime.now(timezone.utc)
        out = []
        for r in raw_rows:
            cycle_h = int(r.get("collectCycle") or 0)
            interval = cycle_h * 3600 if cycle_h else 28800
            out.append(
                {
                    "venue": self.name,
                    "code": code,
                    "datetime": snap_to_hour(to_utc(r["settleTime"])),
                    "fundingRate": float(r["fundingRate"]),
                    "markPrice": 0.0,
                    "interval_sec": interval,
                    "ingested_at": now,
                }
            )
        return out

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/contract/ticker", {"symbol": raw_code}, endpoint="oi", cost=1
        )
        data = d.get("data") or {}
        hold = data.get("holdVol")
        if hold is None:
            return []
        size = self._size.get(raw_code, 1.0)
        oi_base = float(hold) * size
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw_code),
                "datetime": snap_to_hour(end - timedelta(hours=1)),
                "sumOpenInterest": oi_base,
                "sumOpenInterestValue": oi_base * float(data.get("lastPrice") or 0),
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
