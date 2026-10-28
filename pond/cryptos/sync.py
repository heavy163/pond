"""同步编排：每所一个独立 task，各自限频/并发/熔断/超时。

一轮的语义（见 doc §13）：
  window = [now 整点 - lookback_hours, now 整点)   ← 只含已闭合 bar
  每个所在自己的 task 里跑完 → 立刻发单所通知 → 写 sync_stat
  全部结束 → 发汇总通知（保持 data_service 的下游兼容）
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.config import CryptosConfig, VenueSpec
from pond.cryptos.notify import (
    FAILED,
    PARTIAL,
    SYNCED,
    TIMEOUT,
    LogNotifier,
    Notifier,
    VenueDataEvent,
    build_notifier,
)
from pond.cryptos.schema import (
    TABLE_FUNDING,
    TABLE_INSTRUMENT,
    TABLE_KLINE_FUT,
    TABLE_KLINE_SPOT,
    TABLE_MARK,
    TABLE_OI,
    now_utc,
)
from pond.cryptos.store import CryptoStore
from pond.cryptos.venues.registry import make_adapter

DATA_TABLE = {
    "kline": TABLE_KLINE_FUT,
    "spot": TABLE_KLINE_SPOT,
    "funding": TABLE_FUNDING,
    "oi": TABLE_OI,
    "mark": TABLE_MARK,
}


@dataclass
class VenueResult:
    venue: str
    window_start: datetime
    window_end: datetime
    status: str = SYNCED
    rows: dict[str, int] = field(default_factory=dict)
    codes: int = 0
    errors: list[str] = field(default_factory=list)
    duration_ms: int = 0
    stats: dict[str, int] = field(default_factory=dict)

    def event(self, ingested_at: datetime | None = None) -> VenueDataEvent:
        return VenueDataEvent(
            venue=self.venue,
            window_start=self.window_start.isoformat(),
            window_end=self.window_end.isoformat(),
            status=self.status,
            rows=self.rows,
            codes=self.codes,
            errors=self.errors[:20],
            duration_ms=self.duration_ms,
            ingested_at=(ingested_at or now_utc()).isoformat(),
        )

    def stat_row(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "status": self.status,
            "requests": int(self.stats.get("requests", 0)),
            "retries": int(self.stats.get("retries", 0)),
            "http_429": int(self.stats.get("http_429", 0)),
            "rows": sum(self.rows.values()),
            "codes": self.codes,
            "avg_latency_ms": 0,
            "duration_ms": self.duration_ms,
            "checked_at": now_utc(),
        }


class CryptoHelper:
    def __init__(
        self,
        store: CryptoStore,
        config: CryptosConfig | None = None,
        notifier: Notifier | None = None,
        plan: str = "crypto",
        config_path: str | None = None,
        dry_run: bool = False,
        overrides: dict[str, Any] | None = None,
    ):
        self.store = store
        self.config_path = config_path
        self.dry_run = dry_run
        self.plan = plan
        self.overrides = overrides or {}
        self.config = config or CryptosConfig.load(config_path)
        self._apply_overrides(self.config)
        self.notifier = notifier or build_notifier("auto", plan)

    # ---------------- 配置 ----------------

    def _apply_overrides(self, cfg: CryptosConfig) -> None:
        """CLI 覆盖必须作用在每次 reload 之后，否则会被文件值冲掉。"""
        if "lookback_hours" in self.overrides:
            cfg.lookback_hours = int(self.overrides["lookback_hours"])
        symbols = self.overrides.get("symbols")
        data = self.overrides.get("data")
        for spec in cfg.venues.values():
            if symbols:
                spec.symbols_include = tuple(symbols)
                spec.top_n = 0
            if data:
                spec.data = tuple(data)

    def reload_config(self) -> CryptosConfig:
        """每轮重新加载：venue_config 表可动态启停交易所，无需重启进程。"""
        rows = self.store.venue_config_rows()
        self.config = CryptosConfig.load(self.config_path, rows)
        self._apply_overrides(self.config)
        return self.config

    def window(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        top = (now or now_utc()).astimezone(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        return top - timedelta(hours=self.config.lookback_hours), top

    # ---------------- 主流程 ----------------

    async def run_round(
        self,
        now: datetime | None = None,
        venues: list[str] | None = None,
        refresh_instruments: bool = False,
    ) -> list[VenueResult]:
        cfg = self.reload_config()
        start, end = self.window(now)
        specs = [s for s in cfg.enabled_venues() if not venues or s.name in venues]
        logger.info(
            f"本轮窗口 [{start.isoformat()}, {end.isoformat()}) "
            f"交易所 {[s.name for s in specs]}"
        )

        tasks = {
            s.name: asyncio.create_task(
                self._run_venue(s, start, end, refresh_instruments), name=f"venue:{s.name}"
            )
            for s in specs
        }
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)

        results: list[VenueResult] = []
        for (name, _), g in zip(tasks.items(), gathered):
            if isinstance(g, BaseException):
                res = VenueResult(name, start, end, status=FAILED, errors=[repr(g)])
                res.duration_ms = 0
                self.notifier.publish_venue(res.event())
            else:
                res = g
            results.append(res)

        self.notifier.publish_aggregate([r.event() for r in results], self.plan)
        return results

    async def _run_venue(
        self, spec: VenueSpec, start: datetime, end: datetime, refresh: bool
    ) -> VenueResult:
        t0 = time.monotonic()
        res = VenueResult(spec.name, start, end)
        try:
            async with asyncio.timeout(spec.deadline_sec):
                async with make_adapter(spec) as ad:
                    raw_codes = await self._universe(ad, spec, refresh)
                    res.codes = len(raw_codes)
                    if not raw_codes:
                        res.status = PARTIAL
                        res.errors.append(
                            f"{spec.name}: universe 为空（include={spec.symbols_include}）"
                        )
                    for dtype in spec.data:
                        res.rows[dtype] = await self._sync_type(
                            ad, spec, dtype, raw_codes, start, end, res
                        )
                    res.stats = dict(ad.stats)
        except (TimeoutError, asyncio.TimeoutError):
            res.status = TIMEOUT
            res.errors.append(f"{spec.name}: 超过 {spec.deadline_sec}s 时间预算")
            logger.error(res.errors[-1])
        except Exception as e:  # 单所失败不影响其他所
            res.status = FAILED
            res.errors.append(f"{spec.name}: {type(e).__name__}: {e}")
            logger.exception(f"[{spec.name}] 本轮失败")

        res.duration_ms = int((time.monotonic() - t0) * 1000)
        if res.status == SYNCED and res.errors:
            res.status = PARTIAL

        # 通知先发（下游可立刻开始处理该所），统计后写
        self.notifier.publish_venue(res.event())
        if not self.dry_run:
            await asyncio.to_thread(self.store.write_sync_stat, res.stat_row())
        logger.info(
            f"[{spec.name}] {res.status} rows={res.rows} codes={res.codes} "
            f"{res.duration_ms}ms errors={len(res.errors)}"
        )
        return res

    async def _universe(self, ad, spec: VenueSpec, refresh: bool) -> list[str]:
        inst = await ad.instruments()
        if inst and (refresh or not self.dry_run and not self._has_instruments(spec.name)):
            await asyncio.to_thread(self.store.insert, TABLE_INSTRUMENT, inst)
        return spec.select_instruments(inst)

    def _has_instruments(self, venue: str) -> bool:
        rows = self.store.query(
            f"SELECT count() FROM {self.store.database}.instrument WHERE venue = %(v)s",
            {"v": venue},
        )
        return bool(rows and rows[0][0])

    async def _sync_type(
        self,
        ad,
        spec: VenueSpec,
        dtype: str,
        raw_codes: list[str],
        start: datetime,
        end: datetime,
        res: VenueResult,
    ) -> int:
        table = DATA_TABLE[dtype]
        fn = self._fetcher(ad, dtype)
        sem = asyncio.Semaphore(spec.concurrency)
        total = 0

        async def one(raw: str) -> int:
            async with sem:
                rows = await fn(raw, start, end)
                if rows and not self.dry_run:
                    await asyncio.to_thread(self.store.insert, table, rows)
                return len(rows)

        outcomes = await asyncio.gather(
            *[one(c) for c in raw_codes], return_exceptions=True
        )
        for raw, o in zip(raw_codes, outcomes):
            if isinstance(o, BaseException):
                if len(res.errors) < 20:
                    res.errors.append(f"{dtype} {raw}: {type(o).__name__}: {o}")
            else:
                total += o
        return total

    @staticmethod
    def _fetcher(ad, dtype: str):
        if dtype in ("kline", "spot"):
            market = "perp" if dtype == "kline" else "spot"
            return lambda raw, s, e: ad.kline(raw, s, e, market=market)
        if dtype == "funding":
            return ad.funding
        if dtype == "oi":
            return ad.open_interest
        if dtype == "mark":
            return ad.mark_price
        raise ValueError(f"未知数据类型 {dtype}")

    def close(self) -> None:
        self.notifier.close()
        if isinstance(self.notifier, LogNotifier):
            pass
