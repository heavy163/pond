"""交易所适配器基类：限频、重试、配额自校正、连接池。

适配器只回答一件事：给 raw symbol + 时间窗，返回规范化后的行（list[dict]）。
不识库、不写库、不管并发编排 —— 那是 sync.py 的事。
"""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from collections import Counter
from datetime import datetime
from typing import Any

import httpx
from loguru import logger

from pond.cryptos.config import VenueSpec
from pond.cryptos.limiter import (
    BadRequest,
    CircuitBreaker,
    GcraLimiter,
    IPBanned,
    RetriesExhausted,
)


class VenueAdapter(ABC):
    name: str = ""

    def __init__(self, spec: VenueSpec):
        self.spec = spec
        self.stats: Counter = Counter()
        self._limiters: dict[str, GcraLimiter] = {}
        self._client: httpx.AsyncClient | None = None
        self.breaker = CircuitBreaker(name=self.name)

    # ---------------- 生命周期 ----------------

    async def __aenter__(self) -> "VenueAdapter":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.spec.timeout_sec),
            limits=httpx.Limits(
                max_connections=max(4, self.spec.concurrency * 2),
                max_keepalive_connections=max(2, self.spec.concurrency),
            ),
            headers={"User-Agent": "pond-cryptos/0.1 (+https://github.com/)"},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------------- 限频 ----------------

    def limiter(self, endpoint: str = "default") -> GcraLimiter:
        if endpoint not in self._limiters:
            b = self.spec.bucket_for(endpoint)
            self._limiters[endpoint] = GcraLimiter(
                b.per_sec, b.burst, name=f"{self.name}.{endpoint}"
            )
            logger.debug(f"[{self.name}] 建桶 {endpoint}: {b.rate}/{b.window}s")
        return self._limiters[endpoint]

    # ---------------- HTTP ----------------

    async def request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        endpoint: str = "default",
        cost: float = 1.0,
        method: str = "GET",
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if self._client is None:
            raise RuntimeError(f"{self.name}: 必须在 async with 里使用")
        if not self.breaker.allow():
            raise RetriesExhausted(f"{self.name} 处于熔断状态")

        lim = self.limiter(endpoint)
        last_exc: Exception | None = None
        for attempt in range(self.spec.max_retries + 1):
            await lim.acquire(cost)
            self.stats["requests"] += 1
            try:
                # httpx 的 timeout 只覆盖单段读写；对端拖着不回包时会挂死，
                # 加一层硬超时（upbit 实测有过连接 ESTAB 但永不返回）。
                resp = await asyncio.wait_for(
                    self._client.request(method, url, params=params, json=json_body),
                    timeout=self.spec.timeout_sec * 2,
                )
            except asyncio.TimeoutError as e:
                # 服务端拖死（upbit 的 USDG/PYUSD/EURC 等市场会挂住不回包），
                # 重试只会再花同样的时间，直接放弃这个 symbol。
                self.stats["network_err"] += 1
                raise RetriesExhausted(f"{self.name} 请求超时: {url}") from e
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                self.stats["network_err"] += 1
                await self._backoff(attempt)
                continue

            code = resp.status_code
            self._observe_quota(endpoint, resp)

            if code == 200:
                # 有的交易所把「限流」塞在 200 里：mexc 实测返回
                #   {"success": false, "code": 510, "message": "Requests are too frequent"}
                # 只看 HTTP 状态会把这种响应当成成功 → data=None → 适配器返回 [] →
                # 该合约这一小时静默没有 OI，而 status 仍是 synced、errors=0。
                # 实测 mexc 每小时约 20% 的合约以这种方式被丢掉（且是轮换的，很难发现）。
                payload = resp.json()
                if isinstance(payload, dict) and payload.get("success") is False:
                    msg = str(payload.get("message") or "")
                    low = msg.lower()
                    if payload.get("code") == 510 or "frequent" in low or "too many" in low \
                            or "rate" in low:
                        self.stats["biz_rate_limit"] += 1
                        wait = min(2.0 ** attempt, 30.0)
                        logger.warning(f"[{self.name}] 业务级限流(200) 暂停 {wait:.1f}s（{endpoint}）")
                        lim.pause(wait)
                        last_exc = RetriesExhausted(f"{self.name} 业务限流: {msg[:80]}")
                        await self._backoff(attempt)
                        continue
                    self.breaker.record(True)
                    raise BadRequest(f"{self.name} success=false: {msg[:160]}")
                self.breaker.record(True)
                return payload

            if code == 429:
                self.stats["http_429"] += 1
                # 交易所的 Retry-After 可能给到几百秒（upbit 实测），直接照做会让
                # 整个桶静默挂死几十分钟。上限 30s，靠重试次数兜底。
                wait = min(self._retry_after(resp, default=2.0 ** attempt), 30.0)
                logger.warning(f"[{self.name}] 429，暂停 {wait:.1f}s（{endpoint}）")
                lim.pause(wait)
                last_exc = RetriesExhausted(f"{self.name} 429")
                continue

            if code == 418:
                wait = self._retry_after(resp, default=600.0)
                self.breaker.record(False)
                logger.error(f"[{self.name}] 418 IP 被封，硬停 {wait:.0f}s")
                raise IPBanned(f"{self.name} 418, retry_after={wait}")

            if 400 <= code < 500:
                # 参数错重试无意义，直接把错误抛出去（由上层记到该 symbol 的 error）
                self.breaker.record(True)   # 不算服务端故障
                raise BadRequest(f"{self.name} {code}: {resp.text[:200]}")

            # 5xx
            self.stats["http_5xx"] += 1
            self.breaker.record(False)
            last_exc = RetriesExhausted(f"{self.name} {code}")
            await self._backoff(attempt)

        raise RetriesExhausted(f"{self.name} 重试耗尽: {last_exc}")

    # ---------------- 内部 ----------------

    @staticmethod
    def _retry_after(resp: httpx.Response, default: float) -> float:
        raw = resp.headers.get("Retry-After")
        try:
            return max(0.0, float(raw)) if raw else default
        except ValueError:
            return default

    async def _backoff(self, attempt: int) -> None:
        self.stats["retries"] += 1
        delay = (2 ** attempt) + random.uniform(0, 0.3 * (2 ** attempt))
        await asyncio.sleep(min(delay, 60.0))

    def _observe_quota(self, endpoint: str, resp: httpx.Response) -> None:
        """读响应头里的剩余额度，快用完时临时降速（不同所有不同 header）。"""
        used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if used:
            try:
                if int(used) > 0.9 * 2400:      # Binance 上限 2400/min
                    self.limiter(endpoint).slow_down(0.5)
            except ValueError:
                pass
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining:
            try:
                if float(remaining) < 0.2 * float(self.spec.bucket_for(endpoint).rate):
                    self.limiter(endpoint).slow_down(0.5)
            except ValueError:
                pass

    # ---------------- 子类必须实现 ----------------

    @abstractmethod
    async def instruments(self) -> list[dict[str, Any]]:
        """返回 instrument 表的行（venue 由调用方补）。"""

    @abstractmethod
    async def kline(
        self, raw_code: str, start: datetime, end: datetime, market: str = "perp"
    ) -> list[dict[str, Any]]:
        """返回该 symbol 在 [start, end) 的 1h K线（可能多页）。"""

    async def spot_instruments(self) -> list[dict[str, Any]]:
        """现货可交易对。没有现货业务的所返回空。"""
        return []

    @abstractmethod
    async def funding(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """资金费率事件流（不插值）。"""

    @abstractmethod
    async def open_interest(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """持仓量历史（窗口极短，错过不可再生）。"""

    async def fx_rates(
        self, start, end, base: str = "USD", quote: str = "KRW"
    ) -> list[dict]:
        """汇率（默认无）。只有需要法币换算的所实现。"""
        return []

    async def mark_price(
        self, raw_code: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        return []

    # ---------------- 工具 ----------------

    def resolve_code(self, raw_code: str) -> str:
        """raw symbol → 规范 code。子类可覆盖。"""
        return raw_code
