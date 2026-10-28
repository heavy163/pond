"""Hyperliquid（去中心化永续，USDC 结算，统一按 USDT 口径记 code）。

实测（2026-10）：
  - 全部接口都是 POST /info + JSON body，没有 REST 路径
  - metaAndAssetCtxs 返回 [ {universe:[{name,...}]}, [ctx{openInterest, markPx, oraclePx,...}] ]
    两个数组按下标对应
  - candleSnapshot 返回 {t, T, s, i, o, c, h, l, v, n}，v 是币本位，n 是笔数
  - fundingHistory 只传 startTime 即可（传 endTime 会返回空），
    时间戳带毫秒抖动（...017）→ 必须 snap_to_hour
  - **funding 每小时结算**（interval_sec=3600），与其他所不同
  - OI 无历史，只有 ctx 当前值 → 每小时快照
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000


class HyperliquidAdapter(VenueAdapter):
    name = "hyperliquid"
    BASE = "https://api.hyperliquid.xyz"

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(f"{self.BASE}/info", method="POST",
                           json_body={"type": "metaAndAssetCtxs"}, endpoint="meta", cost=2)
        universe = (d[0] or {}).get("universe") if isinstance(d, list) else []
        out: list[dict[str, Any]] = []
        for it in universe or []:
            if it.get("isDelisted"):
                continue
            name = it["name"]                       # BTC
            out.append(
                {
                    "venue": self.name,
                    "code": f"{name}-USDT",
                    "raw_code": name,
                    "base": name,
                    "quote": "USDT",
                    "contract_type": "perp",
                    "multiplier": 1.0,
                    "listed_at": to_utc(0),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[hyperliquid] instruments: {len(out)} 个永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        return f"{raw_code}-USDT"

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        d = await self.request(
            f"{self.BASE}/info",
            method="POST",
            json_body={
                "type": "candleSnapshot",
                "req": {
                    "coin": raw_code,
                    "interval": "1h",
                    "startTime": int(start.timestamp() * 1000),
                    "endTime": int(end.timestamp() * 1000),
                },
            },
            endpoint="kline",
            cost=2,
        )
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for r in d or []:
            ts = to_utc(r["t"])
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
                    "volume": float(r["v"]),
                    "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                    "quote_volume": float(r["v"]) * float(r["c"]),
                    "count": float(r.get("n") or 0),
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
        d = await self.request(
            f"{self.BASE}/info",
            method="POST",
            json_body={
                "type": "fundingHistory",
                "coin": raw_code,
                "startTime": int(start.timestamp() * 1000),
            },
            endpoint="funding",
            cost=1,
        )
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for r in d or []:
            ts = snap_to_hour(to_utc(r["time"]))
            if ts < start or ts >= end:
                continue
            out.append(
                {
                    "venue": self.name,
                    "code": code,
                    "datetime": ts,
                    "fundingRate": float(r["fundingRate"]),
                    "markPrice": 0.0,
                    "interval_sec": 3600,          # HL 每小时结算
                    "ingested_at": now,
                }
            )
        return out

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        d = await self.request(f"{self.BASE}/info", method="POST",
                           json_body={"type": "metaAndAssetCtxs"}, endpoint="oi", cost=2)
        if not isinstance(d, list) or len(d) < 2:
            return []
        universe = (d[0] or {}).get("universe") or []
        ctxs = d[1] or []
        idx = next((i for i, u in enumerate(universe) if u.get("name") == raw_code), None)
        if idx is None or idx >= len(ctxs):
            return []
        ctx = ctxs[idx]
        oi = float(ctx.get("openInterest") or 0)
        px = float(ctx.get("markPx") or 0)
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw_code),
                "datetime": snap_to_hour(end - timedelta(hours=1)),
                "sumOpenInterest": oi,
                "sumOpenInterestValue": oi * px,
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
