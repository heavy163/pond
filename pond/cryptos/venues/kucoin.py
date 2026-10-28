"""KuCoin Futures。

实测（2026-10）：
  - contracts/active: 712 个，字段 baseCurrency='XBT'（注意不是 BTC）
  - kline: from/to 必须是**毫秒**，单页最多 **200** 根，返回升序
  - funding: from/to 也是毫秒（传秒会返回空），返回 {fundingRate, timepoint}
  - OI 无历史接口，只能从 contracts/active 的 openInterest 取当前值做快照
    （712 行一次拉回，按 symbol 缓存，不逐 symbol 请求）
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000
# KuCoin 用 XBT 表示 BTC，这里做一次规范化
CODE_ALIAS = {"XBT": "BTC"}


class KucoinAdapter(VenueAdapter):
    name = "kucoin"
    BASE = "https://api-futures.kucoin.com"

    def __init__(self, spec):
        super().__init__(spec)
        self._oi_cache: dict[str, float] = {}
        self._oi_ts: float = 0.0

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/api/v1/contracts/active", endpoint="meta", cost=2
        )
        out: list[dict[str, Any]] = []
        for it in d.get("data") or []:
            if it.get("quoteCurrency") != "USDT":
                continue
            if it.get("status") not in (None, "Open"):
                continue
            base = CODE_ALIAS.get(it["baseCurrency"], it["baseCurrency"])
            out.append(
                {
                    "venue": self.name,
                    "code": f"{base}-USDT",
                    "raw_code": it["symbol"],
                    "base": base,
                    "quote": "USDT",
                    "contract_type": "perp",
                    "multiplier": float(it.get("multiplier") or 1),
                    "listed_at": to_utc(int(it.get("firstOpenDate") or 0)),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
            oi = it.get("openInterest")
            if oi is not None:
                self._oi_cache[it["symbol"]] = float(oi)
        self._oi_ts = time.time()
        logger.info(f"[kucoin] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        base = raw_code.split("USDT")[0]
        return f"{CODE_ALIAS.get(base, base)}-USDT"

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
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
                f"{self.BASE}/api/v1/kline/query",
                {
                    "symbol": raw_code,
                    "granularity": 60,
                    "from": cursor,
                    "to": end_ms - 1,
                },
                endpoint="kline",
                cost=2,
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

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        d = await self.request(
            f"{self.BASE}/api/v1/contract/funding-rates",
            {
                "symbol": raw_code,
                "from": int(start.timestamp() * 1000),
                "to": int(end.timestamp() * 1000),
            },
            endpoint="funding",
            cost=1,
        )
        rows = d.get("data") or []
        if not rows:
            return []
        times = sorted(int(r["timepoint"]) for r in rows)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval = int(median(diffs) / 1000) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(r["timepoint"])),
                "fundingRate": float(r["fundingRate"]),
                "markPrice": 0.0,
                "interval_sec": interval,
                "ingested_at": now,
            }
            for r in rows
            if start <= to_utc(r["timepoint"]) < end
        ]

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        if not self._oi_cache or time.time() - self._oi_ts > 600:
            await self.instruments()          # 顺带刷新 OI 快照表
        oi = self._oi_cache.get(raw_code)
        if oi is None:
            return []
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw_code),
                "datetime": snap_to_hour(end - timedelta(hours=1)),
                "sumOpenInterest": oi,
                "sumOpenInterestValue": 0.0,
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
