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

    async def _multiplier(self, raw_code: str) -> float:
        """quanto_multiplier。

        ★ 必须懒加载：回灌时 universe 来自 DB，不会调 instruments()，
        缓存为空。此时退回默认 1.0 会让币数放大 100 倍（实测 AAVE 0.01→1.0，
        428k 币被写成 42.8M 币），所以宁可抛错让该 symbol 失败。
        """
        if raw_code not in self._mult:
            c = await self.request(
                f"{self.BASE}/futures/usdt/contracts/{raw_code}",
                endpoint="meta",
                cost=1,
            )
            qm = (c or {}).get("quanto_multiplier")
            if qm is None:
                raise ValueError(f"gate {raw_code} 没有 quanto_multiplier，无法换算币本位")
            self._mult[raw_code] = float(qm)
        return self._mult[raw_code]

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对（USDT 计价）。"""
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        rows = await self.request(
            f"{self.BASE}/spot/currency_pairs", None, endpoint="meta", cost=1
        )
        for it in rows or []:
            if it.get("trade_status") != "tradable":
                continue
            if it.get("quote") != "USDT":
                continue
            out.append({
                "venue": self.name,
                "code": it["id"].replace("_", "-"),
                "raw_code": it["id"],
                "base": it["base"], "quote": it["quote"],
                "contract_type": "spot", "multiplier": 1.0,
                "listed_at": None, "delisted_at": None, "ingested_at": now,
            })

        logger.info(f"[gate] spot_instruments: {len(out)} 个 USDT 现货对")
        return out

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market == "spot":
            return await self._kline_spot(raw_code, start, end)
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        cursor = int(start.timestamp())          # ★ Gate 用秒
        end_s = int(end.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        mult = await self._multiplier(raw_code)
        SPAN = 1000 * 3600          # ★ 单页上限 1000 根；必须按固定窗推进
        while cursor < end_s:
            page_end = min(cursor + SPAN, end_s)
            rows = await self.request(
                f"{self.BASE}/futures/usdt/candlesticks",
                {
                    "contract": raw_code,
                    "interval": "1h",
                    "from": cursor,
                    "to": page_end - 1,
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
            cursor = page_end
        return out

    async def _kline_spot(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Gate 现货 K线。currency_pair 与合约 contract 同为 BTC_USDT；返回数组不是 dict。

        数组顺序: [t, quote_vol, close, high, low, open, base_vol, closed]
        """
        code = self.resolve_code(raw_code)
        cursor, end_s = int(start.timestamp()), int(end.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        SPAN = 1000 * 3600
        while cursor < end_s:
            page_end = min(cursor + SPAN, end_s)
            rows = await self.request(
                f"{self.BASE}/spot/candlesticks",
                {"currency_pair": raw_code, "interval": "1h",
                 "from": cursor, "to": page_end - 1},
                endpoint="kline", cost=2,
            )
            if not rows:
                break
            for r in rows:
                ts = to_utc(int(r[0]) * 1000)
                if ts < start or ts >= end:
                    continue
                out.append({
                    "venue": self.name, "code": code, "open_time": ts,
                    "open": float(r[5]), "high": float(r[3]),
                    "low": float(r[4]), "close": float(r[2]),
                    "volume": float(r[6]), "quote_volume": float(r[1]),
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
        start_s = int(start.timestamp())
        cursor = start_s
        end_s = int(end.timestamp())
        raw_rows: list[dict[str, Any]] = []
        SPAN = 1000 * 8 * 3600      # 1000 条 × 8h
        while cursor < end_s:
            page_end = min(cursor + SPAN, end_s)
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
            cursor = page_end
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
        mult = await self._multiplier(raw_code)
        cursor = int(start.timestamp())
        end_s = int(end.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        SPAN = 1000 * 3600
        while cursor < end_s:
            page_end = min(cursor + SPAN, end_s)
            rows = await self.request(
                f"{self.BASE}/futures/usdt/contract_stats",
                {
                    "contract": raw_code,
                    "interval": "1h",
                    "from": cursor,
                    "to": page_end - 1,
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
                # 实测：open_interest 单位是【张】，open_interest_usd 是交易所自己算好的 U 口径。
                # 优先用官方的 USD 字段，避免用 mark 自乘引入误差。
                oi_base = float(r.get("open_interest") or 0) * mult
                usd = r.get("open_interest_usd")
                if usd is not None:
                    oi_usd = float(usd)
                else:
                    oi_usd = oi_base * float(r.get("mark_price") or 0)
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ts,
                        "sumOpenInterest": oi_base,
                        "sumOpenInterestValue": oi_usd,
                        "ingested_at": now,
                    }
                )
            cursor = page_end
        return out
