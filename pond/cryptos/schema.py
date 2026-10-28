"""cryptos 表结构定义（与 sql/cryptos_ddl.sql 严格一致）。

真实来源是 DDL 文件；这里只是代码侧的一份列清单，用于：
  - 批量 INSERT 时指定 column_names（避免依赖顺序）
  - 校验适配器返回的列是否齐全
改表结构时两处必须同步改，`check_schema()` 会在启动时比对。
"""

from __future__ import annotations

from datetime import datetime, timezone

TABLE_INSTRUMENT = "instrument"
TABLE_KLINE_FUT = "kline_futures_1h"
TABLE_KLINE_SPOT = "kline_spot_1h"
TABLE_FUNDING = "funding_rate"
TABLE_OI = "open_interest"
TABLE_MARK = "mark_price"
TABLE_COVERAGE = "coverage"
TABLE_BACKFILL = "backfill_job"
TABLE_VENUE_CONFIG = "venue_config"
TABLE_SYNC_STAT = "sync_stat"

KLINE_COLUMNS = [
    "venue",
    "code",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "datetime",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ingested_at",
]

COLUMNS: dict[str, list[str]] = {
    TABLE_INSTRUMENT: [
        "venue",
        "code",
        "raw_code",
        "base",
        "quote",
        "contract_type",
        "multiplier",
        "listed_at",
        "delisted_at",
        "ingested_at",
    ],
    TABLE_KLINE_FUT: KLINE_COLUMNS,
    TABLE_KLINE_SPOT: KLINE_COLUMNS,
    TABLE_FUNDING: [
        "venue",
        "code",
        "datetime",
        "fundingRate",
        "markPrice",
        "interval_sec",
        "ingested_at",
    ],
    TABLE_OI: [
        "venue",
        "code",
        "datetime",
        "sumOpenInterest",
        "sumOpenInterestValue",
        "ingested_at",
    ],
    TABLE_MARK: [
        "venue",
        "code",
        "datetime",
        "markPrice",
        "indexPrice",
        "ingested_at",
    ],
    TABLE_SYNC_STAT: [
        "venue",
        "window_start",
        "window_end",
        "status",
        "requests",
        "retries",
        "http_429",
        "rows",
        "codes",
        "avg_latency_ms",
        "duration_ms",
        "checked_at",
    ],
    TABLE_BACKFILL: [
        "venue",
        "data_type",
        "code",
        "cursor_time",
        "target_start",
        "status",
        "rows_total",
        "err",
        "updated_at",
    ],
}

# 表 → 时间列（用于覆盖度查询与 FINAL 去重窗口）
TIME_COLUMN = {
    TABLE_KLINE_FUT: "open_time",
    TABLE_KLINE_SPOT: "open_time",
    TABLE_FUNDING: "datetime",
    TABLE_OI: "datetime",
    TABLE_MARK: "datetime",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(ms_or_dt) -> datetime:
    """毫秒时间戳或已有 datetime → UTC aware datetime。"""
    if isinstance(ms_or_dt, datetime):
        return ms_or_dt if ms_or_dt.tzinfo else ms_or_dt.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(int(ms_or_dt) / 1000.0, tz=timezone.utc)


def snap_to_hour(dt: datetime) -> datetime:
    """把"几乎在整点"的时间戳对齐到整点。

    交易所的资金费率结算是整点触发的，但 API 返回的 fundingTime 带 1~2ms 抖动
    （实测 Binance 返回 1793116800002 而不是 ...000）。原样入库会让
    `JOIN ON (code, datetime)` 在整点上必然匹配失败，所以必须在适配器层对齐。
    资金费率周期只有 1/2/4/8h 四种，全部落在整点，对齐无损。
    """
    return dt.replace(minute=0, second=0, microsecond=0)


def validate_rows(table: str, rows: list[dict]) -> list[dict]:
    """检查每个 dict 的 key 覆盖该表全部列，缺列直接报错（不静默填 0）。"""
    cols = COLUMNS[table]
    for i, r in enumerate(rows[:1]):
        missing = [c for c in cols if c not in r]
        if missing:
            raise ValueError(f"{table} 第 {i} 行缺列 {missing}，实际有 {sorted(r)}")
    return rows
