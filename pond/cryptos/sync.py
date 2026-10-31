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
    TABLE_FX,
    TABLE_OI,
    now_utc,
)
from pond.cryptos.store import CryptoStore, clip_window
from pond.cryptos.venues.registry import make_adapter

DATA_TABLE = {
    "kline": TABLE_KLINE_FUT,
    "spot": TABLE_KLINE_SPOT,
    "funding": TABLE_FUNDING,
    "oi": TABLE_OI,
    "mark": TABLE_MARK,
    "fx": TABLE_FX,
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
                self._run_venue(s, start, end), name=f"venue:{s.name}"
            )
            for s in specs
        }
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)

        t_round = time.monotonic()
        by_name: dict[str, VenueResult] = {}
        for (name, _), g in zip(tasks.items(), gathered):
            if isinstance(g, BaseException):
                res = VenueResult(name, start, end, status=FAILED, errors=[repr(g)])
                res.duration_ms = 0
                self.notifier.publish_venue(res.event())
            else:
                res = g
            by_name[name] = res

        # ---- 失败的所原地重试一次 ----
        # OI 是快照：这一小时没写进去，除 gate/bybit 外都永久补不回来。
        # 一次网络抖动就丢一整小时，代价太大，所以对「早失败」的所再试一次。
        # 三个限制防止拖垮下一轮：只重试一次、只重试 10 分钟内失败的、整轮已超 30 分钟就不重试。
        retry = [
            s for s in specs
            if by_name[s.name].status in (TIMEOUT, FAILED)
            and by_name[s.name].duration_ms < 600_000
            and (time.monotonic() - t_round) < 1800
        ]
        if retry and not self.dry_run:
            logger.warning(f"重试失败的所：{[s.name for s in retry]}")
            await asyncio.sleep(10)
            again = await asyncio.gather(
                *[self._run_venue(s, start, end) for s in retry], return_exceptions=True
            )
            for s, g in zip(retry, again):
                if isinstance(g, BaseException) or g.status == FAILED:
                    continue                      # 还是失败：保留原始结果
                logger.warning(f"[{s.name}] 重试成功：{by_name[s.name].status} → {g.status}")
                by_name[s.name] = g

        results = [by_name[s.name] for s in specs]
        self.notifier.publish_aggregate([r.event() for r in results], self.plan)
        return results

    async def _run_venue(
        self, spec: VenueSpec, start: datetime, end: datetime
    ) -> VenueResult:
        t0 = time.monotonic()
        res = VenueResult(spec.name, start, end)
        try:
            async with asyncio.timeout(spec.deadline_sec):
                async with make_adapter(spec) as ad:
                    uni = await self._universe(ad, spec)
                    res.codes = len(uni["perp"]) or len(uni["spot"])
                    # 纯现货所（upbit）没有永续是正常的，只有声明了合约类数据才报错
                    if not uni["perp"] and {"kline", "funding", "oi", "mark"} & set(spec.data):
                        res.status = PARTIAL
                        res.errors.append(f"{spec.name}: 合约 universe 为空")
                    for dtype in spec.data:
                        codes = uni["spot"] if dtype == "spot" else uni["perp"]
                        res.rows[dtype] = await self._sync_type(
                            ad, spec, dtype, codes, start, end, res
                        )
                    # OI 是快照：这一小时写入的合约数必须≈universe。
                    # 少一条就是永久少一条（除 gate/bybit 外都没有历史接口），
                    # 所以宁可把整所标 partial 让人看见，也不能让它悄悄过去。
                    if "oi" in spec.data and res.codes:
                        n_oi = res.rows.get("oi", 0)
                        if n_oi < res.codes * 0.98:
                            res.status = PARTIAL
                            res.errors.append(
                                f"oi: 本轮只写入 {n_oi}/{res.codes} 条（<98%），"
                                f"快照数据事后无法补齐"
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

    async def _universe(self, ad, spec: VenueSpec) -> dict[str, list[str]]:
        """按市场返回各自的 raw_code 列表。

        现货与合约是两套 symbol（bybit 同名、kucoin/htx 不同名），
        混用会漏采（现货对里有很多没有合约的币）。

        注意：instruments() 是【每轮实时调用】，没有缓存，也不看什么 refresh 开关。
        新上架的合约下一轮自动进 universe，下架的自动退出。别再引入 instrument 缓存——
        缓存过就会漏掉新币，且不会报错，只是静静地少采。
        """
        inst = await ad.instruments()
        # 有些所（upbit）instruments() 本身就返回现货市场（contract_type='spot'），
        # 没有独立的「合约列表」。所以先按 contract_type 把 inst 拆开，
        # 再对只有合约列表的所去补 spot_instruments()。
        perp_rows = [r for r in inst if (r.get("contract_type") or "perp") != "spot"]
        spot_rows = [r for r in inst if r.get("contract_type") == "spot"]
        if "spot" in spec.data and not spot_rows:
            spot_rows = await ad.spot_instruments()
        rows = perp_rows + spot_rows
        # 每轮都写（ReplacingMergeTree，几千行可忽略）：原先只在「该所一条都没有」时写，
        # 导致新增的 spot 元数据永远进不来。
        if rows and not self.dry_run:
            await asyncio.to_thread(self.store.insert, TABLE_INSTRUMENT, rows)
        perp = spec.select_instruments(perp_rows)
        spot = spec.select_instruments(spot_rows)
        # 现货只留「本所有同名永续」的对：没有合约就无法对冲/做空，
        # 基差、carry、跨期都做不了，采集这类对只是白占配额。
        # 纯现货所（upbit 没有任何永续）跳过过滤，否则整所被清空。
        if self.config.filter_spot_by_perp and perp and spot:
            perp_codes = {r.get("code") for r in perp_rows}
            keep_raw = {r.get("raw_code") for r in spot_rows if r.get("code") in perp_codes}
            kept = [c for c in spot if c in keep_raw]
            logger.info(
                f"[{spec.name}] 现货按永续过滤: {len(spot)} → {len(kept)} 对"
                f"（丢弃 {len(spot) - len(kept)} 个无对应合约）"
            )
            spot = kept
        return {"perp": perp, "spot": spot}

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

        # fx 是"每个交易所一条汇率"，不是每个 symbol 一条 → 只调一次
        if dtype == "fx":
            rows = await ad.fx_rates(start, end)
            if rows and not self.dry_run:
                await asyncio.to_thread(self.store.insert, table, rows)
            return len(rows)

        fn = self._fetcher(ad, dtype)
        sem = asyncio.Semaphore(spec.concurrency)
        total = 0

        # 每个币单独 INSERT 会在全量下变成几千次 HTTP 往返，ClickHouse 侧
        # 还会爆 part；改成攒够 500 行写一次（原来 3,519 次 → 约 40 次）。
        buf: list[dict] = []
        buf_lock = asyncio.Lock()

        async def flush() -> None:
            if not buf or self.dry_run:
                buf.clear()
                return
            rows = list(buf)
            buf.clear()
            await asyncio.to_thread(self.store.insert, table, rows)

        done = {"n": 0}
        t_start = time.monotonic()
        got: dict[str, int] = {}       # raw_code -> 这一轮实际拿到的行数

        async def one(raw: str) -> int:
            async with sem:
                rows = clip_window(table, await fn(raw, start, end), start, end)
                done["n"] += 1
                if done["n"] % 50 == 0:
                    el = time.monotonic() - t_start
                    logger.info(
                        f"[{spec.name}] {dtype} 进度 {done['n']}/{len(raw_codes)}"
                        f" {el:.0f}s ({done['n']/el:.2f}/s)"
                    )
                if rows and not self.dry_run:
                    async with buf_lock:
                        buf.extend(rows)
                        need = len(buf) >= 500
                    if need:
                        async with buf_lock:
                            await flush()
                got[raw] = len(rows)
                return len(rows)

        async def run_batch(codes: list[str]) -> None:
            outcomes = await asyncio.gather(
                *[one(c) for c in codes], return_exceptions=True
            )
            for raw, o in zip(codes, outcomes):
                if isinstance(o, BaseException):
                    got[raw] = 0
                    if len(res.errors) < 20:
                        res.errors.append(f"{dtype} {raw}: {type(o).__name__}: {o}")

        await run_batch(raw_codes)

        # ---- 补采：第一遍拿到 0 行的合约 ----
        # 「0 行」有两种含义，处理方式完全不同：
        #   ① 真的没数据（停牌 / 新上架 / 该窗口内没有结算）
        #   ② 被限流或瞬时错误吃掉了（交易所常把错误包在 HTTP 200 里 —— okx 50011、
        #      bybit 10006、htx too-many-requests、mexc 510）
        # 对 OI 这种快照数据，②远多于①，而漏掉就是永久缺失（多数所没有历史接口，
        # 实测 mexc 曾每小时静默丢 20% 合约）。所以必须补采：只对空的那些重试，
        # 最多两轮，代价是每个空合约多 1~2 次请求。
        if not self.dry_run:
            for pass_no in (1, 2):
                missing = [c for c in raw_codes if got.get(c, 0) == 0]
                if not missing:
                    break
                ratio = len(missing) / max(len(raw_codes), 1)
                if ratio > 0.30:
                    # 三成以上全空 = 系统性故障（限流风暴 / 熔断），补采只会雪上加霜
                    res.errors.append(
                        f"{dtype}: {len(missing)}/{len(raw_codes)} ({ratio:.0%}) 首次全空，"
                        f"疑似系统性限流或熔断，跳过补采"
                    )
                    logger.error(f"[{spec.name}] {dtype} 首次 {ratio:.0%} 全空，跳过补采")
                    break
                logger.warning(
                    f"[{spec.name}] {dtype} 补采第 {pass_no} 轮：{len(missing)} 个合约首轮为空"
                )
                await asyncio.sleep(2.0)
                await run_batch(missing)

            still = [c for c in raw_codes if got.get(c, 0) == 0]
            if still:
                ratio = len(still) / max(len(raw_codes), 1)
                msg = (
                    f"{dtype}: 补采后仍有 {len(still)}/{len(raw_codes)} ({ratio:.1%}) 个合约无数据"
                    f"（例：{', '.join(still[:5])}）"
                )
                if ratio > 0.02:
                    res.errors.append(msg)          # >2% → 整个所标 partial
                    logger.error(f"[{spec.name}] {msg}")
                else:
                    logger.warning(f"[{spec.name}] {msg}")

        total = sum(got.values())
        async with buf_lock:
            await flush()
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
