"""下游取数契约：数据完整性必须被**查询**，不能被推断。

为什么不能靠「有没有这行」判断
------------------------------
下游常见写法是 `select count() from kline where ...` 再比 24×天数。这必然误判，因为
「行不存在」至少有四种含义，而它们的下游处理完全不同：

| 情况 | 含义 | 下游该怎么做 |
|---|---|---|
| 交易所那小时没有成交 | 正常（极少数），或合约刚上/已下 | 跳过该小时 |
| 采集漏了，但可补 | 同步故障 | **等修复**，不要当真实值 |
| 采集漏了，且不可补（6 家 OI） | 永久损失 | **该样本标记不可用**，不能前值填充 |
| 整家停了 | 管道宕机 | 整个窗口不可用 |

`count()` 把四者混成一个数字。所以完整性要从**台账 + 参照时钟**里读，而不是从数据本身猜。

三个入口（按使用场景）
----------------------
1. **SQL**：`cryptos.v_coverage_day` / `v_coverage_venue_day`（笔记本里直接 select）
2. **Python 门禁**：`QualityGate.check(...)` → `usable / ratio / missing_ranges / lost_days / stale`
3. **行级标记**：`QualityGate.row_flags(...)` → 每个应有小时是否缺失（供 join / 前值填充决策）

硬规则
------
- OI 因子必须 merge `lost` 标记；把「数据缺失」当「持仓没变」是最危险的错误
- 完整性结论有**时间性**：今天判定 gap 的窗口，明天可能已被修复。
  研究要可复现就必须记 `checked_at`（台账里本来就有）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pond.cryptos.backfill import DATA_TABLE, OI_MAX_DAYS
from pond.cryptos.config import CryptosConfig
from pond.cryptos.gaps import _as_utc
from pond.cryptos.schema import now_utc
from pond.cryptos.store import CryptoStore


class DataQualityError(RuntimeError):
    """数据不满足可用性门槛。"""


@dataclass
class WindowQuality:
    venue: str
    data_type: str
    start: datetime
    end: datetime
    codes_requested: int = 0
    codes_present: int = 0
    expected_hours: int = 0
    present_hours: int = 0
    missing: dict[str, list[tuple[datetime, datetime]]] = field(default_factory=dict)
    lost: dict[str, list[date]] = field(default_factory=dict)
    stale_hours: float = 0.0
    reasons: list[str] = field(default_factory=list)

    @property
    def ratio(self) -> float:
        return self.present_hours / self.expected_hours if self.expected_hours else 1.0

    @property
    def usable(self) -> bool:
        return not self.reasons

    def summary(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "data_type": self.data_type,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "codes": self.codes_present,
            "ratio": round(self.ratio, 4),
            "missing_hours": sum(
                int((b - a).total_seconds() // 3600) for v in self.missing.values() for a, b in v
            ),
            "lost_days": sum(len(v) for v in self.lost.values()),
            "stale_hours": round(self.stale_hours, 1),
            "usable": self.usable,
            "reasons": self.reasons,
        }


# ★ K线的 datetime 是 close_time（xx:59:59.999），整点在 open_time。
#   OI / funding 的 datetime 就是整点。跨表对齐必须统一到 open_time。
TS_COL = {"kline": "open_time", "spot": "open_time", "oi": "datetime", "funding": "datetime"}


class QualityGate:
    """下游取数前调一次；不通过就别算因子。"""

    def __init__(self, store: CryptoStore, config: CryptosConfig | None = None):
        self.store = store
        self.config = config or CryptosConfig.load(None)

    # ---------------- 窗口级门禁 ----------------

    def _ref_table(self, venue: str) -> str:
        """非 K线数据用哪个表当时钟：优先合约 K线，没有就用现货 K线（Upbit）。"""
        rows = self.store.query(
            f"SELECT count() FROM {self.store.database}.{DATA_TABLE['kline']} "
            f"WHERE venue = %(v)s",
            {"v": venue},
        )
        return DATA_TABLE["kline"] if rows and rows[0][0] else DATA_TABLE["spot"]

    def check(
        self,
        venue: str,
        data_type: str = "kline",
        days: int = 7,
        codes: list[str] | None = None,
        min_ratio: float = 0.98,
        max_stale_hours: float = 2.0,
        allow_lost: bool = False,
    ) -> WindowQuality:
        table = DATA_TABLE[data_type]
        end = now_utc().replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=days)
        q = WindowQuality(venue=venue, data_type=data_type, start=start, end=end)

        # 参照时钟：K线（OI/funding 的应有小时由 K线决定）
        ref_table = table if data_type in ("kline", "spot") else self._ref_table(venue)
        where_code = ""
        params: dict[str, Any] = {"v": venue, "s": int(start.timestamp()), "e": int(end.timestamp())}
        if codes:
            where_code = " AND code IN %(codes)s"
            params["codes"] = tuple(codes)

        ref_ts = TS_COL["kline"] if data_type not in ("kline", "spot") else TS_COL[data_type]
        act_ts = TS_COL[data_type]
        ref_rows = self.store.query(
            f"SELECT code, {ref_ts} FROM {self.store.database}.{ref_table} "
            f"WHERE venue = %(v)s AND toUnixTimestamp({ref_ts}) >= %(s)s AND toUnixTimestamp({ref_ts}) < %(e)s{where_code}",
            params,
        )
        actual_rows = self.store.query(
            f"SELECT code, {act_ts} FROM {self.store.database}.{table} "
            f"WHERE venue = %(v)s AND toUnixTimestamp({act_ts}) >= %(s)s AND toUnixTimestamp({act_ts}) < %(e)s{where_code}",
            params,
        )

        ref: dict[str, set[datetime]] = {}
        for code, ts in ref_rows:
            ref.setdefault(code, set()).add(_as_utc(ts))
        act: dict[str, set[datetime]] = {}
        for code, ts in actual_rows:
            act.setdefault(code, set()).add(_as_utc(ts))

        q.codes_requested = len(codes) if codes else len(ref)
        q.codes_present = len(act)

        for code, hours in ref.items():
            expected = sorted(hours)
            present = act.get(code, set())
            q.expected_hours += len(expected)
            q.present_hours += len(present & set(expected))
            gaps = self._compress([h for h in expected if h not in present])
            if gaps:
                q.missing[code] = gaps

        # 永久丢失：来自 coverage 台账，不能靠数据本身推断
        lost_rows = self.store.query(
            f"SELECT code, trade_date FROM {self.store.database}.coverage FINAL "
            f"WHERE venue = %(v)s AND data_type = %(d)s AND status = 'lost' "
            f"AND trade_date >= %(s)s AND trade_date <= %(e)s",
            {"v": venue, "d": data_type, "s": start.date(), "e": end.date()},
        )
        for code, d in lost_rows:
            if codes and code not in codes:
                continue
            q.lost.setdefault(code, []).append(d)

        # 新鲜度：整家停更时，上面的「区间内缺口」一个都查不出来
        last_row = self.store.query(
            f"SELECT max(datetime) FROM {self.store.database}.{table} WHERE venue = %(v)s",
            {"v": venue},
        )
        if last_row and last_row[0][0]:
            q.stale_hours = (now_utc() - _as_utc(last_row[0][0])).total_seconds() / 3600

        if q.codes_present == 0:
            q.reasons.append("该窗口内没有任何数据")
        if q.stale_hours > max_stale_hours:
            q.reasons.append(f"数据已停更 {q.stale_hours:.1f} 小时（阈值 {max_stale_hours}h）")
        if q.ratio < min_ratio:
            q.reasons.append(f"覆盖率 {q.ratio:.2%} 低于阈值 {min_ratio:.0%}")
        if q.lost and not allow_lost:
            q.reasons.append(
                f"{sum(len(v) for v in q.lost.values())} 个 (code, 日) 永久丢失，"
                f"allow_lost=False"
            )
        return q

    def assert_usable(self, **kwargs) -> WindowQuality:
        q = self.check(**kwargs)
        if not q.usable:
            raise DataQualityError(f"{q.venue}/{q.data_type}: " + "; ".join(q.reasons))
        return q

    # ---------------- 行级标记 ----------------

    def row_flags(
        self, venue: str, code: str, data_type: str = "kline", days: int = 7
    ) -> list[dict[str, Any]]:
        """返回每个「应有小时」一条记录，带 present / lost 标记。

        下游要前值填充时，**只能填 present=False 且 lost=False 的行**；
        lost=True 的行一旦填充就是把缺失当成了真实值。
        """
        table = DATA_TABLE[data_type]
        end = now_utc().replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=days)
        ref_table = table if data_type in ("kline", "spot") else self._ref_table(venue)
        ref_ts = TS_COL["kline"]
        act_ts = TS_COL[data_type]
        ref = self.store.query(
            f"SELECT {ref_ts} FROM {self.store.database}.{ref_table} "
            f"WHERE venue = %(v)s AND code = %(c)s AND toUnixTimestamp({ref_ts}) >= %(s)s AND toUnixTimestamp({ref_ts}) < %(e)s",
            {"v": venue, "c": code, "s": int(start.timestamp()), "e": int(end.timestamp())},
        )
        act = self.store.query(
            f"SELECT {act_ts} FROM {self.store.database}.{table} "
            f"WHERE venue = %(v)s AND code = %(c)s AND toUnixTimestamp({act_ts}) >= %(s)s AND toUnixTimestamp({act_ts}) < %(e)s",
            {"v": venue, "c": code, "s": int(start.timestamp()), "e": int(end.timestamp())},
        )
        have = {_as_utc(r[0]) for r in act}
        lost_days = {
            r[0]
            for r in self.store.query(
                f"SELECT trade_date FROM {self.store.database}.coverage FINAL "
                f"WHERE venue = %(v)s AND code = %(c)s AND data_type = %(d)s AND status = 'lost'",
                {"v": venue, "c": code, "d": data_type},
            )
        }
        out = []
        for (ts,) in ref:
            ts = _as_utc(ts)
            out.append(
                {
                    "datetime": ts,
                    "present": ts in have,
                    "lost": ts.date() in lost_days,
                }
            )
        return sorted(out, key=lambda r: r["datetime"])

    # ---------------- 内部 ----------------

    @staticmethod
    def _compress(hours: list[datetime]) -> list[tuple[datetime, datetime]]:
        """把离散缺失小时压成区间，便于人读。"""
        if not hours:
            return []
        hours = sorted(hours)
        out: list[tuple[datetime, datetime]] = []
        lo = prev = hours[0]
        for h in hours[1:]:
            if (h - prev) == timedelta(hours=1):
                prev = h
                continue
            out.append((lo, prev + timedelta(hours=1)))
            lo = prev = h
        out.append((lo, prev + timedelta(hours=1)))
        return out

    # ---------------- 台账视图 ----------------

    def install_views(self) -> None:
        """建两个 SQL 视图，让笔记本不用写 join 逻辑。"""
        self.store.command(
            f"""
            CREATE OR REPLACE VIEW {self.store.database}.v_coverage_day AS
            SELECT venue, code, data_type, trade_date, n_expected, n_actual,
                   status, note, checked_at,
                   multiIf(status = 'ok', toUInt8(1), toUInt8(0))            AS is_complete,
                   multiIf(status = 'lost', toUInt8(1), toUInt8(0))          AS is_lost,
                   multiIf(status IN ('gap','lost'), toUInt8(1), toUInt8(0)) AS needs_attention
            FROM {self.store.database}.coverage FINAL
            """
        )
        self.store.command(
            f"""
            CREATE OR REPLACE VIEW {self.store.database}.v_coverage_venue_day AS
            SELECT venue, data_type, trade_date,
                   count()                              AS codes,
                   countIf(status = 'ok')               AS ok_codes,
                   countIf(status = 'lost')             AS lost_codes,
                   round(countIf(status = 'ok') / count(), 3) AS ok_ratio,
                   sum(greatest(n_expected - n_actual, 0)) AS missing_bars
            FROM {self.store.database}.coverage FINAL
            GROUP BY venue, data_type, trade_date
            """
        )
        logger.info("已安装 v_coverage_day / v_coverage_venue_day")


def oi_repairable(venue: str, day: date) -> bool:
    """OI 该日是否还能补（决定下游该等修复还是永久排除）。"""
    if venue not in OI_MAX_DAYS:
        return False
    return day >= now_utc().date() - timedelta(days=OI_MAX_DAYS[venue])
