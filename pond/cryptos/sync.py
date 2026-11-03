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

# 每轮需要补的【最小步长】：kline/spot 是 1 小时一根；oi 是快照、每小时一条；
# funding 是事件流（结算间隔 1~8h 不定），用 1 秒表示「从最后一条之后继续找」。
STEP = {
    "kline": timedelta(hours=1),
    "spot": timedelta(hours=1),
    "oi": timedelta(hours=1),
    "mark": timedelta(hours=1),
    "funding": timedelta(seconds=1),
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
    # 补采用的现场：universe 是这一轮的合约清单，done 是「每个 dtype 每个合约拿到几行」。
    # 有这两样，失败后就能只补缺的，而不是整所重拉。
    universe: dict[str, list[str]] = field(default_factory=dict)
    done: dict[str, dict[str, int]] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)   # dtype -> 已补采轮数

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
    # 补采容错规则：前 FREE_PASS 轮容错 0（一个不许少），之后每多补 1 轮容错 +1。
    # 真正的成本闸门是「请求预算 = 一整轮的量」（在 _sync_type 里按标的数算），
    # MAX_PASS 只是防死循环的保险丝。
    FREE_PASS = 3
    MAX_PASS = 20
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
        # 补采名单：只要「还有合约没拿到」就补，不管整所是 failed 还是 partial。
        # 精确补采只请求缺的那几个，很便宜，所以时间闸门放宽到 40 分钟；
        # 唯一昂贵的是「连 universe 都没拉到」的整所重跑，它仍然卡在 10 分钟内。
        elapsed = time.monotonic() - t_round
        retry = []
        for s in specs:
            r = by_name[s.name]
            if not r.universe:
                if r.status in (TIMEOUT, FAILED) and r.duration_ms < 600_000 and elapsed < 1800:
                    retry.append(s)
            elif self._needs_refill(r, s) and elapsed < 2400:
                retry.append(s)
        if retry and not self.dry_run:
            logger.warning(f"补采失败的所：{[s.name for s in retry]}")
            await asyncio.sleep(10)
            for s in retry:
                res = by_name[s.name]
                before = res.status
                await self._retry_missing(s, start, end, res)
                self._finalize_status(res, s)
                logger.warning(
                    f"[{s.name}] 补采后 {before} → {res.status} rows={res.rows}"
                )
                self.notifier.publish_venue(res.event())
                await asyncio.to_thread(self.store.write_sync_stat, res.stat_row())

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
                    res.universe = uni      # 补采要用：有了它就不必再拉一次 instruments
                    res.codes = len(uni["perp"]) or len(uni["spot"])
                    # 纯现货所（upbit）没有永续是正常的，只有声明了合约类数据才报错
                    if not uni["perp"] and {"kline", "funding", "oi", "mark"} & set(spec.data):
                        res.status = PARTIAL
                        res.errors.append(f"{spec.name}: 合约 universe 为空")
                    for dtype in spec.data:
                        codes = uni["spot"] if dtype == "spot" else uni["perp"]
                        # 单个 dtype 出异常不拖垮整所：已成功的 dtype 保留在 res 里，
                        # 缺的交给补采按合约补，而不是整所重来。
                        try:
                            res.rows[dtype] = await self._sync_type(
                                ad, spec, dtype, codes, start, end, res
                            )
                        except (TimeoutError, asyncio.TimeoutError):
                            raise           # 时间预算用完，整所退出
                        except Exception as e:
                            res.errors.append(f"{dtype}: {type(e).__name__}: {e}")
                            logger.error(
                                f"[{spec.name}] {dtype} 失败：{type(e).__name__}: {e}"
                            )
                    for k, v in ad.stats.items():
                        res.stats[k] = int(v)
        except (TimeoutError, asyncio.TimeoutError):
            res.status = TIMEOUT
            res.errors.append(f"{spec.name}: 超过 {spec.deadline_sec}s 时间预算")
            logger.error(res.errors[-1])
        except Exception as e:  # 单所失败不影响其他所
            res.status = FAILED
            res.errors.append(f"{spec.name}: {type(e).__name__}: {e}")
            logger.exception(f"[{spec.name}] 本轮失败")

        res.duration_ms = int((time.monotonic() - t0) * 1000)
        self._finalize_status(res, spec)

        # 通知先发（下游可立刻开始处理该所），统计后写
        self.notifier.publish_venue(res.event())
        if not self.dry_run:
            await asyncio.to_thread(self.store.write_sync_stat, res.stat_row())
        logger.info(
            f"[{spec.name}] {res.status} rows={res.rows} codes={res.codes} "
            f"{res.duration_ms}ms errors={len(res.errors)}"
        )
        return res

    def _finalize_status(self, res: VenueResult, spec: VenueSpec) -> None:
        """统一判定 status，补采前后各调一次，必须可重复调用（幂等）。

        OI 是快照：这一小时写入的合约数必须≈universe。少一条就是永久少一条
        （除 gate/bybit 外都没有历史接口），所以宁可标 partial 让人看见，
        也不能让它悄悄过去。
        """
        # 去掉上一轮判定的 oi 结论，避免补采后重复报
        res.errors = [e for e in res.errors if not e.startswith("数据不新鲜")]
        for dtype, codes in self._missing_codes(res, spec).items():
            n_pass = res.attempts.get(dtype, 0)
            tol = max(0, n_pass - self.FREE_PASS)
            if len(codes) > tol:
                res.status = PARTIAL if res.status == SYNCED else res.status
                res.errors.append(
                    f"数据不新鲜: {dtype} 补采 {n_pass} 轮后仍缺 {len(codes)} 个标的的"
                    f"最新数据（容错 {tol}）"
                )
        if res.status == SYNCED and res.errors:
            res.status = PARTIAL

    def _missing_codes(self, res: VenueResult, spec: VenueSpec) -> dict[str, list[str]]:
        """这一轮还差哪些标的（每个 dtype 一份）。没有 universe 时返回空。

        判据是【时间戳】不是行数：拿回一堆旧 K 线也算不合格。
        """
        out: dict[str, list[str]] = {}
        for dtype in spec.data:
            codes = res.universe.get("spot" if dtype == "spot" else "perp") or []
            if not codes:
                continue
            need = self._expected_ts(dtype, res.window_end)
            if need <= 0:
                continue
            done = res.done.get(dtype, {})
            miss = [c for c in codes if done.get(c, 0) < need]
            if miss:
                out[dtype] = miss
        return out

    def _needs_refill(self, res: VenueResult, spec: VenueSpec) -> bool:
        """还值不值得再补一轮 —— 超出容错阶梯才补，容错之内的不再打扰交易所。"""
        for dtype, codes in self._missing_codes(res, spec).items():
            tol = max(0, res.attempts.get(dtype, 0) - self.FREE_PASS)
            if len(codes) > tol:
                return True
        return False

    async def _retry_missing(self, spec: VenueSpec, start, end, res: VenueResult) -> None:
        """按标的精确补采：只重拉这一轮没拿到的合约。

        为什么不用「整所重拉」：整所重拉会把已经成功的合约再请求一遍，白烧配额；
        而且每次都从第一个合约开始，照样撞上同一波限流 —— 重试次数增加，
        成功率却不会稳定上升。只补 done 里为 0 的合约，才是一次重试一次机会。
        """
        uni = res.universe
        if not uni:
            # 连 universe 都没拉到 → 一行都没写进库，整所重跑不会产生重复
            logger.warning(f"[{spec.name}] 没拿到 universe，整所重跑（无重复风险）")
            again = await self._run_venue(spec, start, end)
            if again.status != FAILED:
                res.status = again.status
                res.rows = again.rows
                res.codes = again.codes          # 不回填会让面板把这家的码数显示成 0
                res.errors = again.errors
                res.done = again.done
                res.universe = again.universe
                res.stats = again.stats
                res.duration_ms += again.duration_ms
            return

        try:
            async with asyncio.timeout(spec.deadline_sec):
                async with make_adapter(spec) as ad:
                    for dtype in spec.data:
                        codes = uni["spot"] if dtype == "spot" else uni["perp"]
                        if not codes:
                            continue
                        need = self._expected_ts(dtype, res.window_end)
                        if need <= 0:
                            continue
                        done = res.done.get(dtype, {})
                        missing = [c for c in codes if done.get(c, 0) < need]
                        if not missing:
                            continue
                        logger.warning(
                            f"[{spec.name}] {dtype} 精确补采 {len(missing)}/{len(codes)} 个合约"
                        )
                        res.rows[dtype] = res.rows.get(dtype, 0) + await self._sync_type(
                            ad, spec, dtype, missing, start, end, res
                        )
                    for k, v in ad.stats.items():
                        res.stats[k] = res.stats.get(k, 0) + int(v)
        except (TimeoutError, asyncio.TimeoutError):
            res.errors.append(f"{spec.name}: 补采超时（{spec.deadline_sec}s）")
            return
        except Exception as e:
            res.errors.append(f"补采失败: {type(e).__name__}: {e}")
            return

        # 补采跑完：首轮的失败原因已不成立，清掉后按实际结果重判
        res.errors = [e for e in res.errors if "补采" not in e]
        res.status = SYNCED

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

        # ── 按合约精确增量 ──────────────────────────────────────────────
        # 原来的做法：不管库里有没有，每个合约都按 [end-6h, end) 整窗拉 6 根，
        # 拉回来再靠新鲜度判据挑出缺的。流量因此和「新鲜度」无关。
        # 现在先取每个 code 的 max(时间)，已经新鲜到本轮末的一个请求都不发，
        # 落后的只补 [latest+step, end)。窗口下界仍是 start（= end - lookback），
        # 所以长缺口不会被这里变成无界回补 —— 那是 gapcheck 的职责。
        last: dict[str, int] = {}          # 规范 code -> 最新 epoch 秒
        fund: dict[str, tuple[int, int]] = {}   # funding 专用：code -> (最新结算, interval_sec)
        step = STEP.get(dtype)
        if step is not None and not self.dry_run:
            try:
                if dtype == "funding":
                    # 回看 72h 而不是 start：8h 结算的合约上一次结算常常在 6h 窗口之外，
                    # 只看 6h 会让它们全部落到「没有记录 → 按整窗拉」的兜底分支。
                    fund = await asyncio.to_thread(
                        self.store.funding_state,
                        spec.name,
                        start - timedelta(hours=72),
                    )
                else:
                    last = await asyncio.to_thread(
                        self.store.latest_by_code, table, spec.name, start
                    )
            except Exception as e:                                   # noqa: BLE001
                logger.warning(f"[{spec.name}] {dtype} 取最新时间戳失败，退回整窗: {e}")

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
        # raw_code -> 本轮拿到的【最新时间戳(epoch秒)】，0 表示一行都没拿到。
        # 用时间戳而不是行数：某个标的有 5 根旧 K 线、缺最新那根，行数>0
        # 但数据是旧的，只数行数会把它当成功。
        got: dict[str, int] = {}
        expect_ts = self._expected_ts(dtype, end)
        n_rows = {"n": 0}

        async def one(raw: str) -> int:
            async with sem:
                lo = start
                # funding：还没到下次结算就整轮跳过（8h 结算的合约每小时问一遍有 7/8 是空）
                if dtype == "funding":
                    st = fund.get(ad.resolve_code(raw))
                    if st:
                        lt_f, iv = st
                        if int(end.timestamp()) < lt_f + max(iv, 3600) - 60:
                            done["n"] += 1
                            got[raw] = lt_f
                            return 0
                        if lt_f + 1 > int(start.timestamp()):
                            lo = datetime.fromtimestamp(lt_f + 1, tz=timezone.utc)
                        if lo >= end:
                            done["n"] += 1
                            got[raw] = lt_f
                            return 0
                lt = last.get(ad.resolve_code(raw))
                if lt and step is not None:
                    # 用 epoch 比较：库里读回的时间戳不带时区，直接和 UTC 的 end 比会差 8 小时
                    lo_s = max(int(start.timestamp()), lt + int(step.total_seconds()))
                    if lo_s >= int(end.timestamp()):   # 已经新鲜到窗口末尾 → 零请求
                        done["n"] += 1
                        got[raw] = lt
                        return 0
                    lo = datetime.fromtimestamp(lo_s, tz=timezone.utc)
                rows = clip_window(table, await fn(raw, lo, end), lo, end)
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
                got[raw] = max((self._row_ts(dtype, r) for r in rows), default=0)
                n_rows["n"] += len(rows)
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

        # ---- 补采：只补「数据不新鲜」的标的 ----
        # 判据是时间戳，不是行数：有旧 K 线、缺最新那根也算不合格。
        # 容错按轮次递增，不用百分比 —— 百分比等于默认「丢 2% 不算丢」，
        # 1,070 个合约允许丢 21 个，正是要命的漏法。
        #   前 3 轮容错 0（必须一个不少）
        #   之后每多补 1 轮，容错 +1（给「真的没成交」的标的留出口）
        # 请求预算 = 一整轮的量，防止限流风暴时对着空响应反复打。
        if not self.dry_run:
            n_pass = 0
            spent = 0
            # 预算下限 200：小所（比如 20 个标的）也要能爬完容错阶梯，
            # 8 个不新鲜需要 11 轮 × 8 = 88 次请求。
            budget = max(len(raw_codes), 200)
            while True:
                missing = [c for c in raw_codes if got.get(c, 0) < expect_ts]
                tol = max(0, n_pass - self.FREE_PASS)
                if len(missing) <= tol:
                    break
                if n_pass >= self.MAX_PASS:
                    break
                if n_pass == 0 and all(got.get(c, 0) == 0 for c in raw_codes):
                    res.errors.append(
                        f"{dtype}: {len(raw_codes)} 个标的本轮全部无数据，疑似系统性故障，不补采"
                    )
                    logger.error(
                        f"[{spec.name}] {dtype} 全部 {len(raw_codes)} 个标的无数据，不补采"
                    )
                    break
                if spent + len(missing) > budget:
                    logger.warning(
                        f"[{spec.name}] {dtype} 补采预算用尽（已发 {spent} 次请求），停止补采"
                    )
                    break
                spent += len(missing)
                n_pass += 1
                logger.warning(
                    f"[{spec.name}] {dtype} 第 {n_pass} 轮补采：{len(missing)} 个标的不新鲜"
                    f"（本轮容错 {tol}）"
                )
                await asyncio.sleep(min(float(n_pass), 5.0))
                await run_batch(missing)

            still = [c for c in raw_codes if got.get(c, 0) < expect_ts]
            tol = max(0, n_pass - self.FREE_PASS)
            if len(still) > tol:
                msg = (
                    f"{dtype}: 补采 {n_pass} 轮后仍有 {len(still)} 个标的数据不新鲜"
                    f"（容错 {tol}，例：{', '.join(still[:5])}）"
                )
                res.errors.append(msg)
                logger.error(f"[{spec.name}] {msg}")
            elif still:
                logger.warning(
                    f"[{spec.name}] {dtype} 仍有 {len(still)} 个标的不新鲜，在容错 {tol} 之内"
                )
        res.attempts[dtype] = res.attempts.get(dtype, 0) + n_pass

        total = n_rows["n"]
        async with buf_lock:
            await flush()
        # 记账：哪个合约拿到几行。所级失败后靠它算「缺哪几个」，只补那几个。
        res.done.setdefault(dtype, {}).update(got)
        return total

    @staticmethod
    def _row_ts(dtype: str, row: dict) -> int:
        """一行数据的时间戳（epoch 秒）。K 线看 open_time，其余看 datetime。"""
        v = row.get("open_time") if dtype in ("kline", "spot") else row.get("datetime")
        if v is None:
            v = row.get("datetime") or row.get("open_time")
        if isinstance(v, datetime):
            return int(v.timestamp())
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _expected_ts(dtype: str, end: datetime) -> int:
        """本轮数据至少要「新到这个时间」。0 = 不按时间校验。

        kline/spot/oi/mark 逐小时对齐：窗口 end 时刻，最后一根收盘 K 线和最新
        OI 快照都落在 end-1h。funding 按结算周期（1h/4h/8h），没有「每小时
        必须有一条」的要求，所以不参与新鲜度判定。

        注意：返回 0 会让 funding 同时【不进入 _missing_codes】→ 不补采、不重试。
        这是刻意的：资金费率拉不到就等下一次结算窗口，丢了就丢了，不为它重试。
        """
        if dtype in ("kline", "spot", "oi", "mark"):
            return int((end - timedelta(hours=1)).timestamp())
        return 0

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
