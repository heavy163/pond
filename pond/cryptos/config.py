"""cryptos 采集配置。

优先级（高 → 低）：
    1. ClickHouse 表 cryptos.venue_config（运行时覆盖，动态启停，多容器共享）
    2. YAML 文件 configs/cryptos.yaml（默认值，进 git）
    3. 本文件的 dataclass 默认值（兜底，空配置也能跑）

设计约定见 doc/crypto_multivenue_sync.md §13。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable

import yaml
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "cryptos.yaml"

DATA_TYPES = ("kline", "spot", "funding", "oi", "mark", "fx")
BUCKET_MODES = ("requests", "weight")


@dataclass(frozen=True)
class BucketSpec:
    """一个限频桶。mode=weight 时 rate 是权重配额（如 Binance 1200 weight/min）。"""

    mode: str = "requests"
    rate: float = 10.0          # 每个 window 的配额
    window: float = 1.0         # 秒
    burst: float = 1.0          # 允许的突发倍数

    def __post_init__(self):
        if self.mode not in BUCKET_MODES:
            raise ValueError(f"bucket mode 必须是 {BUCKET_MODES}，收到 {self.mode!r}")
        if self.rate <= 0 or self.window <= 0:
            raise ValueError(f"bucket rate/window 必须 > 0，收到 rate={self.rate} window={self.window}")

    @property
    def per_sec(self) -> float:
        return self.rate / self.window


@dataclass
class VenueSpec:
    name: str
    enabled: bool = True
    concurrency: int = 8
    deadline_sec: int = 3000          # 单所时间预算（一轮 3600s，留 10 分钟收尾）
    timeout_sec: float = 15.0
    max_retries: int = 3
    data: tuple[str, ...] = ("kline", "funding", "oi")
    symbols_include: tuple[str, ...] = ()
    symbols_exclude: tuple[str, ...] = ()
    top_n: int = 0                    # 0 = 不限
    buckets: dict[str, BucketSpec] = field(default_factory=dict)

    def __post_init__(self):
        bad = [d for d in self.data if d not in DATA_TYPES]
        if bad:
            raise ValueError(f"{self.name}: 未知 data 类型 {bad}，可选 {DATA_TYPES}")
        if not self.buckets:
            self.buckets = {"default": BucketSpec()}
        elif "default" not in self.buckets:
            self.buckets["default"] = BucketSpec()

    def bucket_for(self, endpoint: str) -> BucketSpec:
        return self.buckets.get(endpoint) or self.buckets["default"]

    def select_symbols(self, raw_codes: Iterable[str]) -> list[str]:
        """按 include/exclude/top_n 过滤 raw code（不含 base 信息时的退化版本）。

        include 的每个模式独立判断：能精确命中就只取精确（BTCUSDT 不会带上 WBTCUSDT），
        否则退化为子串匹配（写 USDT 就取全部 USDT 对）。
        """
        sel = list(raw_codes)
        if self.symbols_include:
            pool: set[str] = set()
            for pat in self.symbols_include:
                exact = [c for c in sel if c == pat]
                pool.update(exact if exact else [c for c in sel if pat in c])
            sel = [c for c in sel if c in pool]
        if self.symbols_exclude:
            sel = [c for c in sel if not any(p in c for p in self.symbols_exclude)]
        if self.top_n and len(sel) > self.top_n:
            sel = sorted(sel)[: self.top_n]
        return sel

    def select_instruments(self, rows: list[dict]) -> list[str]:
        """按 instrument 行（含 raw_code/base）选合约，返回 raw_code 列表。

        匹配优先级：base 资产 > 精确 raw_code > 子串兜底。
        原因：用户写 "BTC" 的意图是"比特币"，而不是"所有含 BTC 字样的合约"——
        纯子串会把 AIXBT_USDT、WBTC_USDT 一起选进来（实测踩过）。
        """
        inc = set(self.symbols_include)
        if inc:
            hit = [
                r for r in rows
                if r.get("base") in inc or r["raw_code"] in inc
            ]
            if not hit:      # 兜底：写 FragMent 时仍按子串匹配
                hit = [r for r in rows if any(p in r["raw_code"] for p in inc)]
            rows = hit
        exc = set(self.symbols_exclude)
        if exc:
            rows = [
                r for r in rows
                if not (
                    r.get("base") in exc
                    or r["raw_code"] in exc
                    or any(p in r["raw_code"] for p in exc)
                )
            ]
        raws = [r["raw_code"] for r in rows]
        if self.top_n and len(raws) > self.top_n:
            raws = sorted(raws)[: self.top_n]
        return raws


@dataclass
class CryptosConfig:
    lookback_hours: int = 6
    max_total_connections: int = 64
    round_budget_sec: int = 3300
    venues: dict[str, VenueSpec] = field(default_factory=dict)

    # ---------------- 加载 ----------------

    @classmethod
    def load(
        cls,
        path: str | Path | None = None,
        db_rows: list[dict[str, Any]] | None = None,
    ) -> "CryptosConfig":
        cfg = cls()
        p = Path(path) if path else DEFAULT_CONFIG_PATH
        if p.exists():
            cfg = cfg._overlay_yaml(p)
        else:
            logger.warning(f"配置文件不存在，使用内置默认值: {p}")
        if db_rows:
            cfg = cfg._overlay_db(db_rows)
        cfg.validate()
        return cfg

    def _overlay_yaml(self, path: Path) -> "CryptosConfig":
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        g = raw.get("global", {}) or {}
        venues: dict[str, VenueSpec] = {}
        for name, spec in (raw.get("venues") or {}).items():
            venues[name] = self._make_spec(name, spec or {})
        return replace(
            self,
            lookback_hours=int(g.get("lookback_hours", self.lookback_hours)),
            max_total_connections=int(g.get("max_total_connections", self.max_total_connections)),
            round_budget_sec=int(g.get("round_budget_sec", self.round_budget_sec)),
            venues=venues,
        )

    def _overlay_db(self, rows: list[dict[str, Any]]) -> "CryptosConfig":
        """DB 覆盖只改显式给出的字段（NULL / 空串表示不覆盖）。"""
        venues = dict(self.venues)
        for r in rows:
            name = r["venue"]
            base = venues.get(name) or VenueSpec(name=name)
            kw: dict[str, Any] = {}
            if r.get("enabled") is not None:
                kw["enabled"] = bool(int(r["enabled"]))
            for f in ("concurrency", "deadline_sec", "top_n"):
                v = r.get(f)
                if v not in (None, 0):
                    kw[f] = int(v)
            data = (r.get("data") or "").strip()
            if data:
                kw["data"] = tuple(x.strip() for x in data.split(",") if x.strip())
            inc = (r.get("symbols_include") or "").strip()
            if inc:
                kw["symbols_include"] = tuple(x.strip() for x in inc.split(",") if x.strip())
            exc = (r.get("symbols_exclude") or "").strip()
            if exc:
                kw["symbols_exclude"] = tuple(x.strip() for x in exc.split(",") if x.strip())
            buckets_json = (r.get("buckets_json") or "").strip()
            if buckets_json:
                kw["buckets"] = {
                    k: BucketSpec(**v) for k, v in yaml.safe_load(buckets_json).items()
                }
            venues[name] = base if not kw else replace(base, **kw)
        return replace(self, venues=venues)

    @staticmethod
    def _make_spec(name: str, raw: dict[str, Any]) -> VenueSpec:
        buckets = {
            k: BucketSpec(**v) for k, v in (raw.get("buckets") or {}).items()
        }
        sym = raw.get("symbols") or {}
        return VenueSpec(
            name=name,
            enabled=bool(raw.get("enabled", True)),
            concurrency=int(raw.get("concurrency", 8)),
            deadline_sec=int(raw.get("deadline_sec", 3000)),
            timeout_sec=float(raw.get("timeout_sec", 15.0)),
            max_retries=int(raw.get("max_retries", 3)),
            data=tuple(raw.get("data") or ("kline", "funding", "oi")),
            symbols_include=tuple(sym.get("include") or ()),
            symbols_exclude=tuple(sym.get("exclude") or ()),
            top_n=int(sym.get("top_n") or 0),
            buckets=buckets,
        )

    # ---------------- 校验与查询 ----------------

    def validate(self) -> None:
        """未注册的交易所：enabled 时报错（会静默少数据），disabled 时只提示。"""
        from pond.cryptos.venues.registry import available_venues

        known = available_venues()
        not_impl = [v for v, s in self.venues.items() if s.enabled and v not in known]
        if not_impl:
            raise ValueError(
                f"这些交易所在配置里 enabled 但还没有适配器: {not_impl}，已实现: {sorted(known)}"
            )
        idle = [v for v, s in self.venues.items() if not s.enabled and v not in known]
        if idle:
            logger.debug(f"配置里有未实现的交易所（已 disabled，忽略）: {idle}")
        if not any(v.enabled for v in self.venues.values()):
            logger.warning("所有交易所都是 disabled，本轮不会拉取任何数据")

    def enabled_venues(self) -> list[VenueSpec]:
        return [v for v in self.venues.values() if v.enabled]

    def get(self, name: str) -> VenueSpec:
        if name not in self.venues:
            raise KeyError(f"配置里没有交易所 {name}，可选: {sorted(self.venues)}")
        return self.venues[name]
