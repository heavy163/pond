"""Gate.io USDT 永续（api/v4）。

实测（2026-10）：
  - contracts: 1066 个，响应较慢（~1.7s），字段含 quanto_multiplier
  - candlesticks 返回 dict：{t: 秒, o,h,l,c, v: 张数, sum: 计价量}
    **时间戳是秒，不是毫秒** —— 这是 Gate 最容易写错的地方
  - funding: {r, t(秒)}
  - contract_stats 提供**180 天** OI 历史（四家里最深），open_interest 是张数，
    需乘 quanto_multiplier 转成币本位
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_S = 3600
HOUR_MS = HOUR_S * 1000


class GateAdapter(VenueAdapter):
    name = "gate"
    BASE = "https://api.gateio.ws/api/v4"

    def __init__(self, spec):
        super().__init__(spec)
        self._mult: dict[str, float] = {}     # raw(contract) → quanto_multiplier

    async def instruments(self) -> list[dict[str, Any]]:
        rows = await self.request(
            f"{self.BASE}/futures/usdt/contracts", endpoint="meta", cost=2
        )
        out: list[dict[str, Any]] = []
        for it in rows or []:
            if it.get("in_delisting"):
                continue
            name = it["name"]                      # BTC_USDT
            base, quote = name.split("_")[0], name.split("_")[1]
            mult = float(it.get("quanto_multiplier") or 1)
            self._mult[name] = mult
            out.append(
                {
                    "venue": self.name,
                    "code": f"{base}-{quote}",
                    "raw_code": name,
                    "base": base,
                    "quote": quote,
                    "contract_type": "perp",
                    "multiplier": mult,
                    "listed_at": to_utc(int(it.get("create_time") or 0) * 1000),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[gate] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        return raw_code.replace("_", "-")

    def _multiplier(self, raw_code: str) -> float:
        return self._mult.get(raw_code, 1.0)

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp())          # ★ Gate 用秒
        end_s = int(end.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        mult = self._multiplier(raw_code)
        while cursor < end_s:
            rows = await self.request(
                f"{self.BASE}/futures/usdt/candlesticks",
                {
                    "contract": raw_code,
                    "interval": "1h",
                    "from": cursor,
                    "to": end_s - 1,
                },
                endpoint="kline",
                cost=2,
            )
            if not rows:
                break
            for r in rows:
                ts = to_utc(int(r["t"]) * 1000)
                if ts < start or ts >= end:
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "open_time": ts,
                        "open": float(r["o"]),
                        "high": float(r["h"]),
                        "low": float(r["l"]),
                        "close": float(r["c"]),
                        "volume": float(r["v"]) * mult,     # 张 → 币
                        "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                        "quote_volume": float(r.get("sum") or 0),
                        "count": 0.0,
                        "taker_buy_volume": 0.0,
                        "taker_buy_quote_volume": 0.0,
                        "ingested_at": now,
                    }
                )
            nxt = max(int(r["t"]) for r in rows) + HOUR_S
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 1000:
                break
        return out

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        start_s = int(start.timestamp())
        cursor = start_s
        end_s = int(end.timestamp())
        raw_rows: list[dict[str, Any]] = []
        while cursor < end_s:
            rows = await self.request(
                f"{self.BASE}/futures/usdt/funding_rate",
                {
                    "contract": raw_code,
                    "from": cursor,
                    "to": end_s - 1,
                    "limit": 1000,
                },
                endpoint="funding",
                cost=1,
            )
            if not rows:
                break
            raw_rows.extend(rows)
            nxt = max(int(r["t"]) for r in rows) + 1
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 1000:
                break
        if not raw_rows:
            return []
        times = sorted(int(r["t"]) for r in raw_rows)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval = int(median(diffs)) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(int(r["t"]) * 1000)),
                "fundingRate": float(r["r"]),
                "markPrice": 0.0,
                "interval_sec": interval,
                "ingested_at": now,
            }
            for r in raw_rows
            if start_s <= int(r["t"]) < end_s
        ]

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        mult = self._multiplier(raw_code)
        cursor = int(start.timestamp())
        end_s = int(end.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor < end_s:
            rows = await self.request(
                f"{self.BASE}/futures/usdt/contract_stats",
                {
                    "contract": raw_code,
                    "interval": "1h",
                    "from": cursor,
                    "to": end_s - 1,
                    "limit": 1000,
                },
                endpoint="oi",
                cost=2,
            )
            if not rows:
                break
            for r in rows:
                ts = snap_to_hour(to_utc(int(r["time"]) * 1000))
                if ts < start or ts >= end:
                    continue
                oi_base = float(r.get("open_interest") or 0) * mult
                mark = float(r.get("mark_price") or 0)
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ts,
                        "sumOpenInterest": oi_base,
                        "sumOpenInterestValue": oi_base * mark,
                        "ingested_at": now,
                    }
                )
            nxt = max(int(r["time"]) for r in rows) + HOUR_S
            if nxt <= cursor:
                break
            cursor = nxt
            if len(rows) < 1000:
                break
        return out
