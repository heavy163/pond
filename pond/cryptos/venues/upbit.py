"""Upbit（韩国第一大所，KRW 现货市场）。

口径要点（实测 2026-10-28）：
  - 875 个市场：KRW 299 / BTC 331 / USDT 245。本适配器**只取 KRW 市场**，
    因为「上币爆拉」只发生在 KRW 直接入金的场景，BTC/USDT 市场没有这个效应。
  - Upbit 是**现货所**：没有 funding、没有 OI。请求这两类返回 []（不是报错）。
  - code 采用统一约定 BASE-QUOTE（BTC-KRW），raw_code 是 Upbit 原生 KRW-BTC。
  - 1h K线用 /v1/candles/minutes/60，单页最多 200 根，`to` 向更早翻页。
  - 分钟线的时间对齐：KST = UTC+9，但两者都在整点 :00 对齐，所以直接按 UTC 落库。
  - 限频 10 req/s（官方），配置里给 8/s。

额外：`fx_rates()` 记录 KRW/USD（取 KRW-USDT 的 trade_price），
用于计算 Korea premium —— 只在 sync 里按 venue 调一次，不按 symbol。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.schema import snap_to_hour, to_utc
from pond.cryptos.venues.base import VenueAdapter

KST = timezone(timedelta(hours=9))


class UpbitAdapter(VenueAdapter):
    name = "upbit"
    BASE = "https://api.upbit.com"
    PAGE = 200          # 单页上限

    # ---------------- 合约/市场清单 ----------------

    async def instruments(self) -> list[dict[str, Any]]:
        rows = await self.request(
            f"{self.BASE}/v1/market/all", endpoint="meta", cost=1
        )
        out: list[dict[str, Any]] = []
        for it in rows or []:
            raw = it.get("market") or ""
            if not raw.startswith("KRW-"):
                continue
            base = raw.split("-", 1)[1]
            out.append(
                {
                    "venue": self.name,
                    "code": f"{base}-KRW",
                    "raw_code": raw,
                    "base": base,
                    "quote": "KRW",
                    "market": "spot",
                    "contract_type": "spot",
                    "state": "live",
                    "listed_at": None,
                    "delisted_at": None,
                    "tick_size": None,
                    "contract_size": None,
                    "multiplier": 1.0,      # 现货：1 张 = 1 币（与合约所的 multiplier 语义对齐）
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        logger.info(f"[upbit] KRW 市场 {len(out)} 个")
        return out

    # ---------------- K线 ----------------

    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "spot"
    ) -> list[dict[str, Any]]:
        if market != "spot":
            return []
        code = f"{raw_code.split('-', 1)[1]}-KRW"
        cursor = end                      # 向后翻页的上界（不含）
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        while cursor > start:
            d = await self.request(
                f"{self.BASE}/v1/candles/minutes/60",
                {
                    "market": raw_code,
                    "to": cursor.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "count": self.PAGE,
                },
                endpoint="kline",
                cost=1,
            )
            if not d:
                break
            oldest = None
            for r in d:
                ts = snap_to_hour(
                    datetime.fromisoformat(r["candle_date_time_utc"]).replace(
                        tzinfo=timezone.utc
                    )
                )
                if ts < start or ts >= end:
                    continue
                if oldest is None or ts < oldest:
                    oldest = ts
                out.append(
                    {
                        "venue": self.name,
                        "code": code,
                        "open_time": ts,
                        "open": float(r["opening_price"]),
                        "high": float(r["high_price"]),
                        "low": float(r["low_price"]),
                        "close": float(r["trade_price"]),
                        # Upbit 的 candle_acc_trade_volume 是**币**，price 是**计价货币(KRW)**
                        "volume": float(r["candle_acc_trade_volume"]),
                        "datetime": ts + timedelta(minutes=59, seconds=59, milliseconds=999),
                        "quote_volume": float(r["candle_acc_trade_price"]),
                        "count": 0.0,
                        "taker_buy_volume": 0.0,
                        "taker_buy_quote_volume": 0.0,
                        "ingested_at": now,
                    }
                )
            if oldest is None or len(d) < self.PAGE:
                break
            cursor = oldest
        return out

    # ---------------- Upbit 没有的东西 ----------------

    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        return []          # 现货所无资金费率

    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        return []          # 现货所无持仓量

    # ---------------- 汇率（按 venue 调一次） ----------------

    async def fx_rates(
        self, start: datetime, end: datetime, base: str = "USD", quote: str = "KRW"
    ) -> list[dict[str, Any]]:
        """KRW/USD 快照。Upbit 只给当前值，没有历史 → 只能逐小时积累。

        datetime 记「上一个已收盘整点」，与 K线口径一致。
        """
        d = await self.request(
            f"{self.BASE}/v1/ticker",
            # Upbit 没有 KRW-USD 市场，用 KRW-USDT 代理（USDT≈USD，偏差 <0.1%）
            {"markets": f"{quote}-USDT" if base == "USD" else f"{quote}-{base}"},
            endpoint="fx",
            cost=1,
        )
        if not d:
            return []
        rate = float(d[0]["trade_price"])       # 1 base = rate quote
        ts = snap_to_hour(end)
        return [
            {
                "datetime": ts,
                "base": base,
                "quote": quote,
                "rate": rate,
                "source": self.name,
                "ingested_at": datetime.now(timezone.utc),
            }
        ]
