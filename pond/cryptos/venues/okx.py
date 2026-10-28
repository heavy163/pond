"""OKX USDT 本位永续。

实测（2026-10）：
  - instruments: 526 个 SWAP，需按 settleCcy=USDT + state=live 过滤
  - candles 每行 9 列：[ts, o, h, l, c, vol(张), volCcy(币), volCcyQuote(U), confirm]
  - 翻页用 `after=<ts>` 向更早翻；超过 300 根要换 /market/history-candles
  - funding 每页 100 条，用 `after` 向更早翻
  - OI **只有当前值**（无历史）→ 每小时快照，无法回补
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

HOUR_MS = 3600 * 1000


class OkxAdapter(VenueAdapter):
    name = "okx"
    BASE = "https://www.okx.com"

    async def instruments(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for inst_type in ("SWAP",):
            d = await self.request(
                f"{self.BASE}/api/v5/public/instruments",
                {"instType": inst_type},
                endpoint="meta",
                cost=1,
            )
            for it in d.get("data", []):
                if it.get("settleCcy") != "USDT" or it.get("state") != "live":
                    continue
                inst_id = it["instId"]
                if not inst_id.endswith("-SWAP"):
                    continue
                base, quote = inst_id.split("-")[0], inst_id.split("-")[1]
                ct_val = float(it.get("ctVal") or 1)          # 每张合约的币数
                mult = float(it.get("ctMult") or 1)
                out.append(
                    {
                        "venue": self.name,
                        "code": f"{base}-{quote}",
                        "raw_code": inst_id,
                        "base": base,
                        "quote": quote,
                        "contract_type": "perp",
                        "multiplier": ct_val * mult,
                        "listed_at": to_utc(int(it.get("listTime") or 0)),
                        "delisted_at": None,
                        "ingested_at": datetime.now(timezone.utc),
                    }
                )
        logger.info(f"[okx] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw: str) -> str:
        return raw.replace("-SWAP", "") if raw.endswith("-SWAP") else raw

    # ---------------- K线 ----------------

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对（USDT 计价）。"""
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        d = await self.request(
            f"{self.BASE}/api/v5/public/instruments",
            {"instType": "SPOT"}, endpoint="meta", cost=1,
        )
        for it in d.get("data") or []:
            if it.get("state") != "live":
                continue
            if it.get("quoteCcy") != "USDT":
                continue
            out.append({
                "venue": self.name,
                "code": f"{it['baseCcy']}-USDT",
                "raw_code": it["instId"],
                "base": it["baseCcy"], "quote": "USDT",
                "contract_type": "spot", "multiplier": 1.0,
                "listed_at": to_utc(int(it["listTime"])) if it.get("listTime") else None,
                "delisted_at": None, "ingested_at": now,
            })

        logger.info(f"[okx] spot_instruments: {len(out)} 个 USDT 现货对")
        return out

    async def kline(
        self, raw: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market not in ("perp", "spot"):
            return []
        # 永续 raw 形如 BTC-USDT-SWAP；OKX 现货 instId 就是 BTC-USDT
        inst = raw if market == "perp" else self.resolve_code(raw)
        code = self.resolve_code(raw)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor_end = end_ms
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor_end > start_ms:
            path = "/api/v5/market/candles" if not out else "/api/v5/market/history-candles"
            d = await self.request(
                f"{self.BASE}{path}",
                {"instId": inst, "bar": "1H", "limit": 300, "after": cursor_end - 1},
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
                if r[8] == "0":      # confirm=0 表示未闭合
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
                        "volume": float(r[6]),          # volCcy = 币本位成交量
                        "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                        "quote_volume": float(r[7]),
                        "count": 0.0,                    # OKX 不提供笔数
                        "taker_buy_volume": 0.0,
                        "taker_buy_quote_volume": 0.0,
                        "ingested_at": now,
                    }
                )
            oldest = int(rows[-1][0])
            if oldest <= start_ms or oldest >= cursor_end:
                break
            cursor_end = oldest
            if len(rows) < 100:      # 已到上市起点
                break
        return out

    # ---------------- funding ----------------

    async def funding(
        self, raw: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor = end_ms
        raw_rows: list[dict[str, Any]] = []
        while cursor > start_ms:
            d = await self.request(
                f"{self.BASE}/api/v5/public/funding-rate-history",
                {"instId": raw, "limit": 100, "after": cursor - 1},
                endpoint="funding",
                cost=1,
            )
            rows = d.get("data") or []
            if not rows:
                break
            raw_rows.extend(rows)
            oldest = int(rows[-1]["fundingTime"])
            if oldest >= cursor or oldest <= start_ms:
                break
            cursor = oldest
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
                "fundingRate": float(r.get("realizedRate") or r["fundingRate"] or 0),
                "markPrice": 0.0,
                "interval_sec": interval,
                "ingested_at": now,
            }
            for r in raw_rows
            if start_ms <= int(r["fundingTime"]) < end_ms
        ]

    # ---------------- OI（只有快照）----------------

    async def open_interest(
        self, raw: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": raw},
            endpoint="oi",
            cost=1,
        )
        rows = d.get("data") or []
        if not rows:
            return []
        r = rows[0]
        # 快照只能代表"最近一个已闭合小时"
        ts = snap_to_hour(end - timedelta(hours=1))
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw),
                "datetime": ts,
                "sumOpenInterest": float(r.get("oiCcy") or 0),      # 币本位
                "sumOpenInterestValue": float(r.get("oiUsd") or 0),
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
