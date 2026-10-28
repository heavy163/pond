"""Bybit v5 linear（USDT 永续）。

实测（2026-10）：
  - instruments 有 cursor 分页（limit 1000）
  - kline 每行 7 列：[ts, o, h, l, c, vol(币), turnover(U)]，**最新在前**
  - funding 每页 200，最新在前，用 endTime 向更早翻
  - OI 有历史但只留约 8 天（每页 200），仍是四家里较好的
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000


class BybitAdapter(VenueAdapter):
    name = "bybit"
    BASE = "https://api.bybit.com"

    async def instruments(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            d = await self.request(
                f"{self.BASE}/v5/market/instruments-info", params, endpoint="meta", cost=1
            )
            res = d.get("result") or {}
            for it in res.get("list", []):
                if it.get("contractType") != "LinearPerpetual":
                    continue
                if it.get("status") != "Trading":
                    continue
                if it.get("quoteCoin") != "USDT":
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": f"{it['baseCoin']}-{it['quoteCoin']}",
                        "raw_code": it["symbol"],
                        "base": it["baseCoin"],
                        "quote": it["quoteCoin"],
                        "contract_type": "perp",
                        "multiplier": 1.0,
                        "listed_at": to_utc(int(it.get("launchTime") or 0)),
                        "delisted_at": None,
                        "ingested_at": datetime.now(timezone.utc),
                    }
                )
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break
        logger.info(f"[bybit] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        return f"{raw_code[:-4]}-USDT" if raw_code.endswith("USDT") else raw_code

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor = start_ms
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor < end_ms:
            d = await self.request(
                f"{self.BASE}/v5/market/kline",
                {
                    "category": "linear",
                    "symbol": raw_code,
                    "interval": "60",
                    "start": cursor,
                    "end": end_ms - 1,
                    "limit": 1000,
                },
                endpoint="kline",
                cost=2,
            )
            rows = (d.get("result") or {}).get("list") or []
            if not rows:
                break
            for r in reversed(rows):        # 接口最新在前，反过来变升序
                ts = to_utc(r[0])
                if ts < start or ts >= end:
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "open_time": ts,
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                        "quote_volume": float(r[6]),
                        "count": 0.0,
                        "taker_buy_volume": 0.0,
                        "taker_buy_quote_volume": 0.0,
                        "ingested_at": now,
                    }
                )
            newest = int(rows[0][0])
            if newest <= cursor:
                break
            cursor = newest + HOUR_MS
            if len(rows) < 1000:
                break
        return out

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor_end = end_ms
        raw_rows: list[dict[str, Any]] = []
        while cursor_end > start_ms:
            d = await self.request(
                f"{self.BASE}/v5/market/funding/history",
                {
                    "category": "linear",
                    "symbol": raw_code,
                    "startTime": start_ms,
                    "endTime": cursor_end - 1,
                    "limit": 200,
                },
                endpoint="funding",
                cost=1,
            )
            rows = (d.get("result") or {}).get("list") or []
            if not rows:
                break
            raw_rows.extend(rows)
            oldest = min(int(r["fundingRateTimestamp"]) for r in rows)
            if oldest >= cursor_end or oldest <= start_ms:
                break
            cursor_end = oldest
        if not raw_rows:
            return []
        times = sorted(int(r["fundingRateTimestamp"]) for r in raw_rows)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval = int(median(diffs) / 1000) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(r["fundingRateTimestamp"])),
                "fundingRate": float(r["fundingRate"]),
                "markPrice": 0.0,
                "interval_sec": interval,
                "ingested_at": now,
            }
            for r in raw_rows
            if start_ms <= int(r["fundingRateTimestamp"]) < end_ms
        ]

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor_end = end_ms
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor_end > start_ms:
            d = await self.request(
                f"{self.BASE}/v5/market/open-interest",
                {
                    "category": "linear",
                    "symbol": raw_code,
                    "intervalTime": "1h",
                    "startTime": start_ms,
                    "endTime": cursor_end - 1,
                    "limit": 200,
                },
                endpoint="oi",
                cost=1,
            )
            rows = (d.get("result") or {}).get("list") or []
            if not rows:
                break
            for r in rows:
                ts = snap_to_hour(to_utc(r["timestamp"]))
                if ts < start or ts >= end:
                    continue
                oi = float(r["openInterest"])
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ts,
                        "sumOpenInterest": oi,
                        "sumOpenInterestValue": 0.0,   # 接口不含 USD 口径，留 0 不臆造
                        "ingested_at": now,
                    }
                )
            oldest = min(int(r["timestamp"]) for r in rows)
            if oldest >= cursor_end or oldest <= start_ms:
                break
            cursor_end = oldest
        return out
