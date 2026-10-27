"""限频与熔断。

GcraLimiter 用 GCRA（理论到达时间）而非朴素令牌桶：
  - O(1)、无惊群（每个协程算出自己的放行时刻再睡）
  - 天然 FIFO，不存在后来者插队
  - 支持 cost，可直接吃交易所的 weight 语义

已用 report 侧仿真验证：三所不同限额并行、20 协程同抢一个桶时
请求间隔 std < 1ms（详见 doc/crypto_multivenue_sync.md §13.4）。
"""

from __future__ import annotations

import asyncio
import time

from loguru import logger


class GcraLimiter:
    def __init__(self, rate_per_sec: float, burst: float = 1.0, name: str = ""):
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec 必须 > 0，收到 {rate_per_sec}")
        self.rate = rate_per_sec
        self.interval = 1.0 / rate_per_sec
        self.burst = max(1.0, burst)
        self.name = name
        self._tat = 0.0
        self._base_rate = rate_per_sec
        self._lock = asyncio.Lock()
        self._paused_until = 0.0

    async def acquire(self, cost: float = 1.0) -> float:
        """阻塞到允许发起请求，返回实际等待秒数。"""
        waited = 0.0
        pause = self._paused_until - time.monotonic()
        if pause > 0:
            await asyncio.sleep(pause)
            waited += pause

        async with self._lock:
            now = time.monotonic()
            tat = max(self._tat, now)
            new_tat = tat + cost * self.interval
            allow_at = new_tat - self.burst * self.interval
            self._tat = new_tat
        delay = allow_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
            waited += delay
        return waited

    def pause(self, seconds: float) -> None:
        """429 / 418 时硬停该桶。"""
        until = time.monotonic() + max(0.0, seconds)
        if until > self._paused_until:
            self._paused_until = until
            logger.warning(f"[{self.name}] 限频桶暂停 {seconds:.1f}s")
            # 关键：恢复后从"现在"重新起算，不保留历史欠账
            self._tat = until

    def slow_down(self, factor: float = 0.5, seconds: float = 300.0) -> None:
        """响应头显示剩余额度不足时的临时降速。"""
        self.rate = max(0.1, self.rate * factor)
        self.interval = 1.0 / self.rate
        logger.warning(f"[{self.name}] 限频临时降至 {self.rate:.2f}/s，{seconds:.0f}s 后恢复")
        # 恢复任务不阻塞调用方
        asyncio.get_event_loop().call_later(seconds, self._restore)

    def _restore(self) -> None:
        self.rate = self._base_rate
        self.interval = 1.0 / self.rate
        logger.info(f"[{self.name}] 限频恢复至 {self.rate:.2f}/s")


class CircuitBreaker:
    """连续失败 N 次后熔断 cooldown 秒（按交易所实例化）。"""

    def __init__(self, fail_threshold: int = 5, cooldown: float = 300.0, name: str = ""):
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown
        self.name = name
        self.fails = 0
        self.open_until = 0.0

    def allow(self) -> bool:
        return time.monotonic() >= self.open_until

    def record(self, ok: bool) -> None:
        if ok:
            self.fails = 0
            return
        self.fails += 1
        if self.fails >= self.fail_threshold and self.allow():
            self.open_until = time.monotonic() + self.cooldown
            logger.error(f"[{self.name}] 连续 {self.fails} 次失败，熔断 {self.cooldown:.0f}s")


class VenueError(Exception):
    """适配器层统一异常基类。"""


class BadRequest(VenueError):
    """4xx（非 429/418）：参数错，重试无意义。"""


class RateLimited(VenueError):
    """429 重试耗尽。"""


class IPBanned(VenueError):
    """418：IP 被交易所封禁，必须整所停止。"""


class RetriesExhausted(VenueError):
    pass
