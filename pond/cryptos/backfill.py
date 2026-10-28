"""历史回灌。

原则（doc §11）：
  1. 先抢不可再生的：OI（Gate 180 天 / Bybit 8 天），其余 6 家没有历史接口
  2. 回灌单位是 (venue, data_type, code)，不是时间窗 —— 交易所 API 按 symbol 组织
  3. 断点续传：每推进一个 chunk 就写 backfill_job，崩溃后从 cursor 继续
  4. **不覆盖实时数据**：回灌行的 ingested_at = bar 时间 + 1s，
     实时的 now64(3) 永远更大，ReplacingMergeTree 会保留实时值
  5. 不挤占实时：回灌用独立限频（实时配额的 50%）

用法：
    python scripts/backfill_cryptos.py --venue gate --data oi --days 180
    python scripts/backfill_cryptos.py --venue gate,bybit --data oi --execute
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.config import CryptosConfig, VenueSpec
from pond.cryptos.schema import (
    TABLE_BACKFILL,
    TABLE_FUNDING,
    TABLE_KLINE_FUT,
    TABLE_KLINE_SPOT,
    TABLE_OI,
    now_utc,
)
from pond.cryptos.store import CryptoStore
from pond.cryptos.venues.registry import make_adapter

# 各所 OI 历史真实深度（实测，doc §15.2）。不在表里的 = 只有当前值，无法回灌。
# 必须留安全余量：Gate 的 180 天是硬边界，按 180 天请求会因几秒误差被 400 拒绝
# （实测报 "from time exceeds 180-day limit"），所以取 179 / 7。
OI_MAX_DAYS: dict[str, int] = {
    "gate": 179,
    "bybit": 7,
}

DATA_TABLE = {
    "kline": TABLE_KLINE_FUT,
    "spot": TABLE_KLINE_SPOT,
    "funding": TABLE_FUNDING,
    "oi": TABLE_OI,
}

# 回灌时的数据类型 → adapter 方法
FETCH = {
    "kline": lambda ad, raw, s, e: ad.kline(raw, s, e, market="perp"),
    "spot": lambda ad, raw, s, e: ad.kline(raw, s, e, market="spot"),
    "funding": lambda ad, raw, s, e: ad.funding(raw, s, e),
    "oi": lambda ad, raw, s, e: ad.open_interest(raw, s, e),
}


@dataclass
class BackfillResult:
    venue: str
    data_type: str
    codes: int
    rows: int
    skipped: int
    errors: list[str]
    duration_ms: int

    def as_job_rows(self, target_start: datetime) -> list[dict[str, Any]]:
        return []


class BackfillRunner:
    def __init__(
        self,
        store: CryptoStore,
        config: CryptosConfig,
        chunk_days: int = 30,
        rate_factor: float = 0.5,
        execute: bool = False,
        resume: bool = True,
    ):
        self.store = store
        self.config = config
        self.chunk_days = chunk_days
        self.rate_factor = rate_factor      # 回灌只用实时配额的 50%
        self.execute = execute
        self.resume = resume

    # ---------------- 计划 ----------------

    def plan(
        self, venue: str, data_types: list[str], days: int | None
    ) -> tuple[VenueSpec, dict[str, list[datetime]]]:
        """返回 (降速后的 spec, {data_type: [目标起点]})"""
        spec = self.config.get(venue)
        # 回灌降速：所有桶 × rate_factor
        buckets = {
            k: replace(b, rate=max(0.1, b.rate * self.rate_factor))
            for k, b in spec.buckets.items()
        }
        slow = replace(spec, buckets=buckets)

        now = now_utc().replace(minute=0, second=0, microsecond=0)
        targets: dict[str, list[datetime]] = {}
        for dt in data_types:
            if dt == "oi":
                cap = OI_MAX_DAYS.get(venue, 0)
                if cap <= 0:
                    logger.warning(f"[{venue}] 没有 OI 历史接口，跳过（只能靠增量积累）")
                    continue
                d = min(days or cap, cap)
                targets[dt] = [now - timedelta(days=d)]
            else:
                targets[dt] = [now - timedelta(days=days or 30)]
        return slow, targets

    def universe(
        self, venue: str, symbols: list[str] | None = None
    ) -> list[tuple[str, str]]:
        """从 instrument 表取 (raw_code, code)，与实时共用同一宇宙。"""
        rows = self.store.query(
            f"SELECT raw_code, code FROM {self.store.database}.instrument FINAL "
            f"WHERE venue = %(v)s",
            {"v": venue},
        )
        items = [(r[0], r[1]) for r in rows]
        spec = self.config.get(venue)
        if symbols:
            spec = replace(spec, symbols_include=tuple(symbols), top_n=0)
        if spec.symbols_include or spec.top_n:
            keep = set(
                spec.select_instruments(
                    [{"raw_code": r, "base": c.split("-")[0]} for r, c in items]
                )
            )
            items = [(r, c) for r, c in items if r in keep]
        return items

    # ---------------- 执行 ----------------

    async def run(
        self,
        venues: list[str],
        data_types: list[str],
        days: int | None = None,
        symbols: list[str] | None = None,
    ) -> list[BackfillResult]:
        tasks = []
        for v in venues:
            slow, targets = self.plan(v, data_types, days)
            if not targets:
                continue
            uni = self.universe(v, symbols)
            if not uni:
                logger.warning(f"[{v}] instrument 表为空，先跑一次实时同步")
                continue
            tasks.append((v, slow, targets, uni))

        results = await asyncio.gather(
            *[self._run_venue(*t) for t in tasks], return_exceptions=True
        )
        out: list[BackfillResult] = []
        for t, r in zip(tasks, results):
            if isinstance(r, BaseException):
                logger.exception(f"[{t[0]}] 回灌失败: {r}")
                out.append(
                    BackfillResult(t[0], ",".join(t[2]), 0, 0, 0, [repr(r)], 0)
                )
            else:
                out.extend(r)
        return out

    async def _run_venue(self, venue, spec, targets, uni) -> list[BackfillResult]:
        out: list[BackfillResult] = []
        async with make_adapter(spec) as ad:
            for data_type, starts in targets.items():
                out.append(
                    await self._backfill_type(ad, venue, spec, data_type, starts[0], uni)
                )
        return out

    async def _backfill_type(
        self,
        ad,
        venue: str,
        spec: VenueSpec,
        data_type: str,
        target_start: datetime,
        uni: list[tuple[str, str]],
    ) -> BackfillResult:
        t0 = time.monotonic()
        table = DATA_TABLE[data_type]
        fetch = FETCH[data_type]
        sem = asyncio.Semaphore(spec.concurrency)
        total_rows = 0
        skipped = 0
        errors: list[str] = []
        now = now_utc().replace(minute=0, second=0, microsecond=0)

        async def one(raw: str, code: str) -> int:
            nonlocal skipped
            cursor = now
            rows_for_code = 0
            while cursor > target_start:
                chunk_start = max(target_start, cursor - timedelta(days=self.chunk_days))
                async with sem:
                    rows = await fetch(ad, raw, chunk_start, cursor)
                if rows:
                    # ★ 回灌版本号 = bar 时间 + 1s，保证实时数据永远覆盖它
                    for r in rows:
                        r["ingested_at"] = r["datetime"] + timedelta(seconds=1)
                    if self.execute:
                        await asyncio.to_thread(self.store.insert, table, rows)
                    rows_for_code += len(rows)
                if self.execute:
                    await asyncio.to_thread(
                        self._save_job,
                        venue, data_type, code, chunk_start, target_start, rows_for_code,
                    )
                cursor = chunk_start
            if rows_for_code == 0:
                skipped += 1
            return rows_for_code

        outcomes = await asyncio.gather(
            *[one(r, c) for r, c in uni], return_exceptions=True
        )
        for (raw, _code), o in zip(uni, outcomes):
            if isinstance(o, BaseException):
                if len(errors) < 20:
                    errors.append(f"{raw}: {type(o).__name__}: {o}")
            else:
                total_rows += o

        if self.execute:
            await asyncio.to_thread(
                self._save_job, venue, data_type, "*", target_start, target_start, total_rows,
                status="done",
            )

        res = BackfillResult(
            venue=venue,
            data_type=data_type,
            codes=len(uni),
            rows=total_rows,
            skipped=skipped,
            errors=errors,
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
        logger.info(
            f"[{venue}/{data_type}] codes={res.codes} rows={total_rows} "
            f"skipped={skipped} errors={len(errors)} {res.duration_ms/1000:.1f}s"
        )
        return res

    def _save_job(
        self,
        venue: str,
        data_type: str,
        code: str,
        cursor_time: datetime,
        target_start: datetime,
        rows_total: int,
        status: str = "running",
    ) -> None:
        self.store.insert(
            TABLE_BACKFILL,
            [
                {
                    "venue": venue,
                    "data_type": data_type,
                    "code": code,
                    "cursor_time": cursor_time,
                    "target_start": target_start,
                    "status": status,
                    "rows_total": rows_total,
                    "err": "",
                    "updated_at": now_utc(),
                }
            ],
        )

    # ---------------- 续跑 ----------------

    def pending(self, venue: str, data_type: str) -> dict[str, datetime]:
        """返回未完成的 (code → cursor)，用于断点续跑。"""
        rows = self.store.query(
            f"SELECT code, max(cursor_time) FROM {self.store.database}.backfill_job "
            f"WHERE venue = %(v)s AND data_type = %(d)s AND status != 'done' "
            f"GROUP BY code",
            {"v": venue, "d": data_type},
        )
        return {r[0]: r[1] for r in rows}
