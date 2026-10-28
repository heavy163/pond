"""Bitget v2 USDT-FUTURES。

实测（2026-10）：
  - contracts: 862 个，symbolType='perpetual'
  - candles 每行 7 列：[ts, o, h, l, c, vol(币), quoteVol]，**升序**，单页最多 200
  - funding 分页用 pageNo，最新在前
  - OI **只有当前值**（startTime/endTime 被忽略）→ 每小时快照
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000
PRODUCT = "USDT-FUTURES"


class BitgetAdapter(VenueAdapter):
    name = "bitget"
    BASE = "https://api.bitget.com/api/v2"

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/mix/market/contracts",
            {"productType": PRODUCT},
            endpoint="meta",
            cost=1,
        )
        out: list[dict[str, Any]] = []
        for it in d.get("data") or []:
            if it.get("symbolType") not in (None, "perpetual"):
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
        logger.info(f"[bitget] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        return f"{raw_code[:-4]}-USDT" if raw_code.endswith("USDT") else raw_code

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对（USDT 计价）。"""
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        d = await self.request(
            f"{self.BASE}/spot/public/symbols", None, endpoint="meta", cost=1
        )
        for it in d.get("data") or []:
            if it.get("status") != "online":
                continue
            if it.get("quoteCoin") != "USDT":
                continue
            out.append({
                "venue": self.name,
                "code": f"{it['baseCoin']}-USDT",
                "raw_code": it["symbol"],
                "base": it["baseCoin"], "quote": "USDT",
                "contract_type": "spot", "multiplier": 1.0,
                "listed_at": None, "delisted_at": None, "ingested_at": now,
            })

        logger.info(f"[bitget] spot_instruments: {len(out)} 个 USDT 现货对")
        return out

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market == "spot":
            return await self._kline_spot(raw_code, start, end)
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        SPAN = 200 * HOUR_MS          # ★ 单页 200 根；按固定窗推进
        while cursor < end_ms:
            page_end = min(cursor + SPAN, end_ms)
            d = await self.request(
                f"{self.BASE}/mix/market/candles",
                {
                    "symbol": raw_code,
                    "productType": PRODUCT,
                    "granularity": "1H",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": 200,
                },
                endpoint="kline",
                cost=1,
            )
            rows = d.get("data") or []
            if not rows:
                break
            for r in rows:
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
            cursor = page_end
        return out

    async def _kline_spot(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Bitget 现货 K线：/api/v2/spot/market/candles，symbol 与合约同名 BTCUSDT。

        返回数组: [ts, o, h, l, c, baseVol, quoteVol]
        """
        code = self.resolve_code(raw_code)
        cursor, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        SPAN = 500 * HOUR_MS
        while cursor < end_ms:
            page_end = min(cursor + SPAN, end_ms)
            d = await self.request(
                f"{self.BASE}/spot/market/candles",
                {"symbol": raw_code, "granularity": "1h",
                 "startTime": cursor, "endTime": page_end - 1, "limit": 500},
                endpoint="kline", cost=1,
            )
            rows = d.get("data") or []
            if not rows:
                break
            for r in rows:
                ts = to_utc(r[0])
                if ts < start or ts >= end:
                    continue
                out.append({
                    "venue": self.name, "code": code, "open_time": ts,
                    "open": float(r[1]), "high": float(r[2]),
                    "low": float(r[3]), "close": float(r[4]),
                    "volume": float(r[5]), "quote_volume": float(r[6]),
                    "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                    "count": 0.0, "taker_buy_volume": 0.0,
                    "taker_buy_quote_volume": 0.0, "ingested_at": now,
                })
            cursor = page_end
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
                f"{self.BASE}/mix/market/history-fund-rate",
                {
                    "symbol": raw_code,
                    "productType": PRODUCT,
                    "pageSize": 100,
                    "pageNo": page,
                },
                endpoint="funding",
                cost=1,
            )
            rows = d.get("data") or []
            if not rows:
                break
            hit_old = False
            for r in rows:
                t = int(r["fundingTime"])
                if start_ms <= t < end_ms:
                    raw_rows.append(r)
                if t < start_ms:
                    hit_old = True
            if hit_old or len(rows) < 100:
                break
            page += 1
        if not raw_rows:
            return []
        times = sorted(int(r["fundingTime"]) for r in raw_rows)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval = int(median(diffs) / 1000) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(r["fundingTime"])),
                "fundingRate": float(r["fundingRate"]),
                "markPrice": 0.0,
                "interval_sec": interval,
                "ingested_at": now,
            }
            for r in raw_rows
        ]

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/mix/market/open-interest",
            {"symbol": raw_code, "productType": PRODUCT},
            endpoint="oi",
            cost=1,
        )
        items = (d.get("data") or {}).get("openInterestList") or []
        if not items:
            return []
        size = float(items[0].get("size") or 0)
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw_code),
                "datetime": snap_to_hour(end - timedelta(hours=1)),
                "sumOpenInterest": size,
                "sumOpenInterestValue": 0.0,
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
