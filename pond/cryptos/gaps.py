"""数据完整性：缺口检测 + 自动修复 + 覆盖度台账。

为什么需要这个模块
------------------
实时同步的 `lookback_hours=6` 只是一个**盲自愈**：它能补最近 6 小时的
「交易所晚发布」和「进程崩了一次」，但它有两个致命问题：

1. **超过 6 小时的停机就永久留洞，且没人知道**。`sync_stat` 只记「这一轮成功」，
   不记「这段时间本该有数据但没有」。
2. **OI 的能力是非对称的**：Gate 有 179 天、Bybit 7 天历史，另外 6 家只有「当前值」。
   停机 2 天，K线和 funding 能全量补回，**但那 6 家的 48 小时 OI 永远消失**。

本模块把三件事分开：

- `detect()`：按天对齐「应有根数」和「实际根数」，列出缺口（不猜）
- `repair()`：能补的补（K线/funding 全量，OI 仅 Gate/Bybit 且在窗口内）
- `coverage` 表：每个 (venue, code, 日期) 一行，记 n_kline/n_oi/n_funding + status。
  `status='lost'` 是**显式的永久损失标记**，下游做因子时必须能读到它。

设计取舍
--------
按**天**检测而不是按小时：停机几乎总是跨小时的整段，按天检测把 SQL 从
「生成每小时再反连接」的百万行降成「group by 一天一次」，8 家 × 3 类型
一共 48 次轻查询，秒级完成。修复时按天重拉整日（24 根，幂等），代价可忽略。
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.backfill import DATA_TABLE, FETCH, OI_MAX_DAYS
from pond.cryptos.config import CryptosConfig
from pond.cryptos.schema import TABLE_COVERAGE, now_utc
from pond.cryptos.store import CryptoStore
from pond.cryptos.venues.registry import make_adapter

def _as_utc(dt: datetime) -> datetime:
    """clickhouse_connect 对 DateTime64 有时返回 naive，统一成 UTC aware。"""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# 每日应有根数
EXPECTED_PER_DAY = {
    "kline": 24,
    "oi": 24,          # 快照模式也是每小时一条
    "funding": 3,      # 8h 结算；Hyperliquid 单独覆盖
}
FUNDING_PER_DAY = {"hyperliquid": 24}

# 数据可修复性：oi 只有这两家有历史接口（doc §15.2）
OI_REPAIRABLE = set(OI_MAX_DAYS)


@dataclass
class Gap:
    venue: str
    data_type: str
    code: str
    day: date
    missing: int
    expected: int
    repairable: bool = True
    reason: str = ""
    rampup: bool = False       # 采集首日（该类型还没开始采）不应当作故障


@dataclass
class RepairResult:
    gaps: list[Gap] = field(default_factory=list)
    repaired: int = 0
    failed: int = 0
    lost: int = 0
    errors: list[str] = field(default_factory=list)


class GapDetector:
    def __init__(self, store: CryptoStore, config: CryptosConfig):
        self.store = store
        self.config = config

    # ---------------- 检测 ----------------

    def detect(
        self, venues: list[str] | None = None, data_types: list[str] | None = None, days: int = 7
    ) -> list[Gap]:
        venues = venues or [s.name for s in self.config.enabled_venues()]
        data_types = data_types or ["kline", "funding", "oi"]
        gaps: list[Gap] = []
        for v in venues:
            for dt in data_types:
                if dt == "kline":
                    gaps += self._detect_table(v, "kline", days)
                elif dt == "oi":
                    gaps += self._detect_table(v, "oi", days)
                elif dt == "funding":
                    gaps += self._detect_funding(v, days)
        gaps += self._detect_stale(venues, days)
        return gaps

    def _bounds(self, venue: str, table: str) -> dict[str, tuple[datetime, datetime]]:
        rows = self.store.query(
            f"SELECT code, min(datetime), max(datetime) FROM {self.store.database}.{table} "
            f"WHERE venue = %(v)s GROUP BY code",
            {"v": venue},
        )
        return {r[0]: (_as_utc(r[1]), _as_utc(r[2])) for r in rows}

    def _daily_counts(self, venue: str, table: str, since: datetime) -> dict[tuple[str, date], int]:
        rows = self.store.query(
            f"SELECT code, toDate(datetime) d, count() n FROM {self.store.database}.{table} "
            f"WHERE venue = %(v)s AND datetime >= %(s)s GROUP BY code, d",
            {"v": venue, "s": since},
        )
        return {(r[0], r[1]): int(r[2]) for r in rows}

    def _detect_table(self, venue: str, data_type: str, days: int) -> list[Gap]:
        """按天对齐「应有根数」与「实际根数」。

        期望值的来源是【合约自己的首末 bar】而不是全局日期：
        首日和当日天然不完整，用区间交集算，不搞「首尾日一律跳过」——
        那样当天内部挖的洞（最常见：某小时同步失败）会被漏掉。
        """
        table = DATA_TABLE[data_type]
        since = now_utc() - timedelta(days=days)
        seen = self._daily_counts(venue, table, since)

        # ★ 参照时钟：K线。OI 快照 / funding 的「应有根数」由当天 K线根数决定，
        #   不能从自身表取边界 —— okx OI 被删空后自身没有任何边界，整段洞会隐形。
        if data_type == "kline":
            ref_counts: dict[tuple[str, date], int] = seen
            bounds = self._bounds(venue, table)
        else:
            ref_counts = self._daily_counts(
                venue, DATA_TABLE["kline"], max(since, since if data_type == "kline" else since)
            )
            bounds = self._bounds(venue, DATA_TABLE["kline"])
        now = now_utc()
        this_hour = now.replace(minute=0, second=0, microsecond=0)
        # ★ 采集起点：早于「本类数据第一行」的空白不是缺口，是还没开始采
        floor_row = self.store.query(
            f"SELECT min(datetime) FROM {self.store.database}.{table} WHERE venue = %(v)s",
            {"v": venue},
        )
        floor = _as_utc(floor_row[0][0]) if floor_row and floor_row[0][0] else since

        # ★ 上界用「全所最晚 bar」：某合约单独停更（最常见）时，它自己的 hi
        #   同时也是边界，区间内部一个洞都看不到 —— 这就是单合约尾部缺失的盲区。
        ceiling = max((h for _, h in bounds.values()), default=since)
        out: list[Gap] = []
        for code, (lo, hi) in bounds.items():
            # 与全所最新差超过 24h：大概率已下架/停牌，不算同步故障
            delisted = (ceiling - hi) > timedelta(hours=24)
            d = max(lo.date(), since.date())
            while d <= min(now_utc().date(), ceiling.date()):
                day_start = max(
                    lo, floor, datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                )
                if day_start >= datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=1):
                    d += timedelta(days=1)
                    continue
                day_close = min(ceiling, datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                                + timedelta(days=1) - timedelta(hours=1))
                if data_type == "kline":
                    expected = int((day_close - day_start).total_seconds() // 3600) + 1
                    # 当日只算已收盘的 bar
                    if d == this_hour.date():
                        expected = min(
                            expected,
                            max(0, int((this_hour - day_start).total_seconds() // 3600)),
                        )
                        # 交易所发布最后一根 bar 有几分钟延迟，减 1 根容差，
                        # 否则每次检查都会把「刚收盘还没发出来」误报成缺口
                        expected = max(0, expected - 1)
                else:
                    # 有 K线的小时才应该有 OI 快照；funding 再按结算周期折算
                    n_ref = ref_counts.get((code, d), 0)
                    if data_type == "funding":
                        interval = 1 if venue == "hyperliquid" else 8
                        expected = max(1, n_ref // interval) if n_ref else 0
                    else:
                        expected = n_ref
                actual = seen.get((code, d), 0)
                if actual < expected:
                    out.append(
                        Gap(
                            venue=venue,
                            data_type=data_type,
                            code=code,
                            day=d,
                            missing=expected - actual,
                            expected=expected,
                            repairable=self._repairable(venue, data_type, d),
                            reason=(
                                f"疑似下架/停牌（最后一条 {hi}，早于全所 {ceiling}）"
                                if delisted
                                else ("" if self._repairable(venue, data_type, d)
                                      else self._why_lost(venue, data_type, d))
                            ),
                            rampup=(
                                delisted
                                or (data_type != "kline" and d == floor.date())
                            ),
                        )
                    )
                d += timedelta(days=1)
        return out

    def _detect_stale(self, venues: list[str], days: int) -> list[Gap]:
        """整家交易所停止更新（停机/断网）。

        上面的方法只看「合约自己首末 bar 之间」，尾部整体缺失它发现不了：
        数据停在 3 天前，边界也是 3 天前，区间内部一个洞都没有。
        所以必须单独查「新鲜度」——这正是停机一两天最先暴露的症状。
        """
        out: list[Gap] = []
        now = now_utc()
        for v in venues:
            for dt in ("kline", "oi", "funding"):
                table = DATA_TABLE[dt]
                r = self.store.query(
                    f"SELECT max(datetime), count() FROM {self.store.database}.{table} "
                    f"WHERE venue = %(v)s",
                    {"v": v},
                )
                if not r or not r[0][1]:
                    continue
                last = _as_utc(r[0][0])
                lag_h = (now - last).total_seconds() / 3600
                # funding 8h 一次，容忍 12h；其余容忍 2h
                limit = 12 if dt == "funding" else 2
                if lag_h > limit:
                    out.append(
                        Gap(
                            venue=v,
                            data_type=dt,
                            code="*",
                            day=(now - timedelta(hours=lag_h)).date(),
                            missing=int(lag_h),
                            expected=int(lag_h),
                            repairable=True,
                            reason=f"整家停止更新 {lag_h:.1f} 小时（最后一条 {last}）",
                        )
                    )
        return out

    def _detect_funding(self, venue: str, days: int) -> list[Gap]:
        expected = FUNDING_PER_DAY.get(venue, EXPECTED_PER_DAY["funding"])
        since = now_utc() - timedelta(days=days)
        rows = self.store.query(
            f"SELECT code, toDate(datetime) d, count() n FROM {self.store.database}.funding_rate "
            f"WHERE venue = %(v)s AND datetime >= %(s)s GROUP BY code, d",
            {"v": venue, "s": since},
        )
        seen = {(r[0], r[1]): int(r[2]) for r in rows}
        bounds_rows = self.store.query(
            f"SELECT code, min(datetime), max(datetime) FROM {self.store.database}.funding_rate "
            f"WHERE venue = %(v)s GROUP BY code",
            {"v": venue},
        )
        out: list[Gap] = []
        for code, lo, hi in bounds_rows:
            lo, hi = _as_utc(lo), _as_utc(hi)
            d = max(since.date(), lo.date() + timedelta(days=1))
            end = min(now_utc().date(), hi.date())
            while d < end:
                n = seen.get((code, d), 0)
                # funding 允许 1 根的抖动（跨日归属）
                if n < expected - 1:
                    out.append(
                        Gap(venue, "funding", code, d, expected - n, expected, True, "")
                    )
                d += timedelta(days=1)
        return out

    def _repairable(self, venue: str, data_type: str, day: date) -> bool:
        if data_type != "oi":
            return True
        if venue not in OI_REPAIRABLE:
            return False
        depth = OI_MAX_DAYS[venue]
        return day >= (now_utc().date() - timedelta(days=depth))

    def _why_lost(self, venue: str, data_type: str, day: date) -> str:
        if data_type == "oi" and venue not in OI_REPAIRABLE:
            return f"{venue} 只提供当前 OI，无历史接口 → 该日 OI 永久不可恢复"
        if data_type == "oi":
            return f"超出 {venue} 的 {OI_MAX_DAYS[venue]} 天窗口 → 永久不可恢复"
        return "不可恢复"

    # ---------------- 台账 ----------------

    def write_coverage(
        self, gaps: list[Gap], venues: list[str], days: int = 7
    ) -> int:
        """每 (venue, code, 日期) 一行，status=ok/gap/lost。"""
        lost_keys = {
            (g.venue, g.data_type, g.code, g.day) for g in gaps
            if not g.repairable and g.code != "*"
        }
        gap_keys = {(g.venue, g.data_type, g.code, g.day) for g in gaps if g.code != "*"}
        since = now_utc().date() - timedelta(days=days)
        out: list[dict[str, Any]] = []
        for v in venues:
            # 一次查出三类数据的每日计数
            agg: dict[tuple[str, date], dict[str, int]] = defaultdict(dict)
            for dt, table in (("kline", DATA_TABLE["kline"]), ("oi", DATA_TABLE["oi"]),
                              ("funding", DATA_TABLE["funding"])):
                rows = self.store.query(
                    f"SELECT code, toDate(datetime) d, count() n FROM {self.store.database}.{table} "
                    f"WHERE venue = %(v)s AND datetime >= %(s)s GROUP BY code, d",
                    {"v": v, "s": since},
                )
                for code, d, n in rows:
                    agg[(code, d)][dt] = int(n)
            for (code, d), counts in agg.items():
                status = "ok"
                notes = []
                for dt in ("kline", "oi", "funding"):
                    if (v, dt, code, d) in lost_keys:
                        status = "lost"
                        notes.append(f"{dt} 永久缺失")
                    elif (v, dt, code, d) in gap_keys and status == "ok":
                        status = "gap"
                        notes.append(f"{dt} 待修复")
                out.append(
                    {
                        "venue": v,
                        "code": code,
                        "trade_date": d,
                        "n_kline": min(counts.get("kline", 0), 24),
                        "n_oi": min(counts.get("oi", 0), 24),
                        "n_funding": min(counts.get("funding", 0), 24),
                        "checked_at": now_utc(),
                        "status": status,
                        "note": "; ".join(notes),
                    }
                )
        if out:
            self.store.insert(TABLE_COVERAGE, out)
        return len(out)


class GapRepairer:
    """把可修复的缺口按 (venue, code, 日期) 重拉一遍。

    写库沿用回灌规则：ingested_at = bar 时间 + 1s，实时值永远覆盖它。
    """

    def __init__(self, store: CryptoStore, config: CryptosConfig, execute: bool = True):
        self.store = store
        self.config = config
        self.execute = execute

    async def repair(self, gaps: list[Gap]) -> RepairResult:
        res = RepairResult(gaps=gaps)
        by_venue: dict[str, list[Gap]] = defaultdict(list)
        for g in gaps:
            if g.repairable:
                by_venue[g.venue].append(g)
            else:
                res.lost += 1

        for venue, items in by_venue.items():
            spec = self.config.get(venue)
            async with make_adapter(spec) as ad:
                by_type: dict[str, list[Gap]] = defaultdict(list)
                for g in items:
                    by_type[g.data_type].append(g)
                for dt, gs in by_type.items():
                    # 同一 code 的多天合并成一个窗口，减少请求
                    windows: dict[str, tuple[date, date]] = {}
                    for g in gs:
                        lo, hi = windows.get(g.code, (g.day, g.day))
                        windows[g.code] = (min(lo, g.day), max(hi, g.day))
                    sem = asyncio.Semaphore(spec.concurrency)
                    tasks = [
                        self._repair_code(ad, venue, dt, code, lo, hi, sem, res)
                        for code, (lo, hi) in windows.items()
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)
        return res

    async def _repair_code(self, ad, venue, dt, code, lo: date, hi: date, sem, res) -> None:
        start = datetime(lo.year, lo.month, lo.day, tzinfo=timezone.utc)
        end = datetime(hi.year, hi.month, hi.day, tzinfo=timezone.utc) + timedelta(days=1)
        # raw_code 与 code 的映射（instrument 表里存了 raw）
        raw = self.store.query(
            f"SELECT raw_code FROM {self.store.database}.instrument FINAL "
            f"WHERE venue = %(v)s AND code = %(c)s LIMIT 1",
            {"v": venue, "c": code},
        )
        if not raw:
            res.failed += 1
            res.errors.append(f"{venue}/{code}: instrument 表里找不到 raw_code")
            return
        raw_code = raw[0][0]
        try:
            async with sem:
                rows = await FETCH[dt](ad, raw_code, start, end)
        except Exception as e:  # noqa: BLE001
            res.failed += 1
            res.errors.append(f"{venue}/{dt}/{code} {lo}~{hi}: {type(e).__name__}: {e}")
            return
        if not rows:
            res.failed += 1
            res.errors.append(f"{venue}/{dt}/{code} {lo}~{hi}: 交易所返回空")
            return
        for r in rows:
            r["ingested_at"] = r["datetime"] + timedelta(seconds=1)
        if self.execute:
            try:
                await asyncio.to_thread(self.store.insert, DATA_TABLE[dt], rows)
            except Exception as e:  # noqa: BLE001
                res.failed += 1
                res.errors.append(f"{venue}/{dt}/{code} 写库失败: {type(e).__name__}: {e}")
                return
        res.repaired += len(rows)
        logger.info(f"[repair] {venue}/{dt}/{code} {lo}~{hi}: {len(rows)} 行")
