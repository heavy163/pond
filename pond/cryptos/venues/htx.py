"""HTX（火币）USDT 本位永续，域名 api.hbdm.com。

实测（2026-10）：
  - swap_contract_info: 398 个，contract_size 是每张的币数（BTC 是 0.001）
  - kline: {id: 秒, open, close, high, low, amount(币), vol(张), trade_turnover(U), count}
    amount 已经是币本位，直接用；trade_turnover 是计价量
  - funding 分页 6593 条，用 page_index
  - OI 只有当前值 → 每小时快照
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


class HtxAdapter(VenueAdapter):
    name = "htx"
    BASE = "https://api.hbdm.com"

    def _biz_status(self, payload):
        # htx：{"status":"error","err-code":"invalid-parameter"}，HTTP 200
        if not isinstance(payload, dict):
            return None
        st = payload.get("status")
        if st is None or st == "ok":
            return None
        ec = str(payload.get("err-code") or "")
        msg = f'htx {ec} {payload.get("err-msg") or ""}'.strip()
        if "too-many" in ec or "frequency" in ec or "rate" in ec or "too many" in msg.lower():
            return ("rate", msg)
        return ("other", msg)

    async def instruments(self) -> list[dict[str, Any]]:
        d = await self.request(
            f"{self.BASE}/linear-swap-api/v1/swap_contract_info",
            endpoint="meta",
            cost=2,
        )
        out: list[dict[str, Any]] = []
        for it in (d.get("data") or []):
            if it.get("contract_status") != 1:      # 1 = 可交易
                continue
            cc = it["contract_code"]                # BTC-USDT
            base, quote = cc.split("-")[0], cc.split("-")[1]
            out.append(
                {
                    "venue": self.name,
                    "code": f"{base}-{quote}",
                    "raw_code": cc,
                    "base": base,
                    "quote": quote,
                    "contract_type": "perp",
                    "multiplier": float(it.get("contract_size") or 1),
                    "listed_at": to_utc(0),
                    "delisted_at": None,
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[htx] instruments: {len(out)} 个 USDT 永续")
        return out

    def resolve_code(self, raw_code: str) -> str:
        if "-" in raw_code:                  # 合约 contract_code：BTC-USDT
            return raw_code
        u = raw_code.upper()                 # 现货 symbol：btcusdt
        return f"{u[:-4]}-USDT" if u.endswith("USDT") else raw_code

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对（USDT 计价）。"""
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        d = await self.request(
            "https://api.huobi.pro/v1/common/symbols", None, endpoint="meta", cost=1
        )
        for it in d.get("data") or []:
            if it.get("state") != "online":
                continue
            if (it.get("quote-currency") or "").lower() != "usdt":
                continue
            out.append({
                "venue": self.name,
                "code": f"{it['base-currency'].upper()}-USDT",
                "raw_code": it["symbol"],
                "base": it["base-currency"].upper(), "quote": "USDT",
                "contract_type": "spot", "multiplier": 1.0,
                "listed_at": None, "delisted_at": None, "ingested_at": now,
            })

        logger.info(f"[htx] spot_instruments: {len(out)} 个 USDT 现货对")
        return out

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        if market == "spot":
            return await self._kline_spot(raw_code, start, end)
        if market != "perp":
            return []
        code = self.resolve_code(raw_code)
        end_s = int(end.timestamp())
        cursor_end = end_s
        start_s = int(start.timestamp())
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor_end > start_s:
            d = await self.request(
                f"{self.BASE}/linear-swap-ex/market/history/kline",
                {
                    "contract_code": raw_code,
                    "period": "60min",
                    "size": 2000,
                    "from": start_s,
                    "to": cursor_end - 1,
                },
                endpoint="kline",
                cost=1,
            )
            rows = d.get("data") or []
            if not rows:
                break
            for r in rows:
                ts = to_utc(int(r["id"]) * 1000)
                if ts < start or ts >= end:
                    continue
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "open_time": ts,
                        "open": float(r["open"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "close": float(r["close"]),
                        "volume": float(r["amount"]),
                        "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                        "quote_volume": float(r.get("trade_turnover") or 0),
                        "count": float(r.get("count") or 0),
                        "taker_buy_volume": 0.0,
                        "taker_buy_quote_volume": 0.0,
                        "ingested_at": now,
                    }
                )
            oldest = min(int(r["id"]) for r in rows)
            if oldest >= cursor_end or oldest <= start_s:
                break
            cursor_end = oldest
        return out

    async def _kline_spot(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """HTX 现货 K线：api.huobi.pro（合约是 api.hbdm.com）。

        现货接口只有 size，没有 from/to：一次取最近 2000 根再按窗口裁剪。
        """
        code = self.resolve_code(raw_code)
        symbol = raw_code.replace("-", "").lower()
        d = await self.request(
            "https://api.huobi.pro/market/history/kline",
            {"symbol": symbol, "period": "60min", "size": 2000},
            endpoint="kline", cost=1,
        )
        rows = d.get("data") or []
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for r in rows:
            ts = to_utc(int(r["id"]) * 1000)
            if ts < start or ts >= end:
                continue
            out.append({
                "venue": self.name, "code": code, "open_time": ts,
                "open": float(r["open"]), "high": float(r["high"]),
                "low": float(r["low"]), "close": float(r["close"]),
                "volume": float(r.get("amount") or 0),
                "quote_volume": float(r.get("vol") or 0),
                "datetime": ts + timedelta(milliseconds=HOUR_MS - 1),
                "count": 0.0, "taker_buy_volume": 0.0,
                "taker_buy_quote_volume": 0.0, "ingested_at": now,
            })
        out.sort(key=lambda x: x["open_time"])
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
                f"{self.BASE}/linear-swap-api/v1/swap_historical_funding_rate",
                {"contract_code": raw_code, "page_index": page, "page_size": 100},
                endpoint="funding",
                cost=1,
            )
            data = d.get("data") or {}
            rows = data.get("data") or []
            if not rows:
                break
            hit_old = False
            for r in rows:
                t = int(r["funding_time"])
                if start_ms <= t < end_ms:
                    raw_rows.append(r)
                if t < start_ms:
                    hit_old = True
            if hit_old or page >= int(data.get("total_page") or 1):
                break
            page += 1
        if not raw_rows:
            return []
        times = sorted(int(r["funding_time"]) for r in raw_rows)
        diffs = [b - a for a, b in zip(times, times[1:])]
        interval = int(median(diffs) / 1000) if diffs else 28800
        now = datetime.now(timezone.utc)
        return [
            {
                "venue": self.name,
                "code": code,
                "datetime": snap_to_hour(to_utc(r["funding_time"])),
                "fundingRate": float(r.get("realized_rate") or r["funding_rate"]),
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
            f"{self.BASE}/linear-swap-api/v1/swap_open_interest",
            {"contract_code": raw_code},
            endpoint="oi",
            cost=1,
        )
        rows = d.get("data") or []
        if not rows:
            return []
        r = rows[0]
        return [
            {
                "venue": self.name,
                "code": self.resolve_code(raw_code),
                "datetime": snap_to_hour(end - timedelta(hours=1)),
                "sumOpenInterest": float(r.get("amount") or 0),      # 币本位
                "sumOpenInterestValue": float(r.get("value") or 0),
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
