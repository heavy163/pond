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
    _px: dict[str, float] | None = None      # ticker 价格缓存（本轮内有效）

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

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对（USDT 计价）。"""
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        cursor = ""
        while True:
            params = {"category": "spot", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            d = await self.request(
                f"{self.BASE}/v5/market/instruments-info", params, endpoint="meta", cost=1
            )
            res = d.get("result") or {}
            for it in res.get("list", []):
                if it.get("status") != "Trading":
                    continue
                if it.get("quoteCoin") != "USDT":
                    continue
                out.append({
                    "venue": self.name,
                    "code": f"{it['baseCoin']}-{it['quoteCoin']}",
                    "raw_code": it["symbol"],
                    "base": it["baseCoin"], "quote": it["quoteCoin"],
                    "contract_type": "spot", "multiplier": 1.0,
                    "listed_at": None, "delisted_at": None, "ingested_at": now,
                })
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break

        logger.info(f"[bybit] spot_instruments: {len(out)} 个 USDT 现货对")
        return out

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        # Bybit 的现货与永续用同一个 symbol 字符串（BTCUSDT），只差 category
        if market not in ("perp", "spot"):
            return []
        category = "linear" if market == "perp" else "spot"
        code = self.resolve_code(raw_code)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        cursor = start_ms
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        SPAN = 1000 * HOUR_MS       # ★ 单页 1000 根；必须按固定窗推进，不能按返回最大值推进
        while cursor < end_ms:
            page_end = min(cursor + SPAN, end_ms)
            d = await self.request(
                f"{self.BASE}/v5/market/kline",
                {
                    "category": category,
                    "symbol": raw_code,
                    "interval": "60",
                    "start": cursor,
                    "end": page_end - 1,
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
            cursor = page_end
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

    async def _ensure_tickers(self) -> None:
        """一次拉全部 linear ticker，缓存到本轮结束（1 请求换 500+ 合约的价格）。"""
        if self._px is not None:
            return
        self._px = {}
        d = await self.request(
            f"{self.BASE}/v5/market/tickers",
            {"category": "linear"},
            endpoint="tickers",
            cost=1,
        )
        for t in (d.get("result") or {}).get("list") or []:
            try:
                self._px[t["symbol"]] = float(t["lastPrice"])
            except (KeyError, TypeError, ValueError):
                continue

    def _last_price(self, raw_code: str) -> float:
        return float((self._px or {}).get(raw_code, 0.0))

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        code = self.resolve_code(raw_code)
        await self._ensure_tickers()
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
                # Bybit 的 open-interest 接口不带 U 口径。只有【最近一小时】才用当前
                # lastPrice 换算——历史行用今天的价格去乘会得到错的价格口径，
                # 所以在回灌场景一律留 0，由因子层 join kline close 重算。
                recent = abs((now - ts).total_seconds()) <= 3600
                px = self._last_price(raw_code) if recent else 0.0
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "datetime": ts,
                        "sumOpenInterest": oi,
                        "sumOpenInterestValue": oi * px,
                        "ingested_at": now,
                    }
                )
            oldest = min(int(r["timestamp"]) for r in rows)
            if oldest >= cursor_end or oldest <= start_ms:
                break
            cursor_end = oldest
        return out
