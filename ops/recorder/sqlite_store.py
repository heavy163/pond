"""cryptos 采集的 SQLite 存储后端（单机版录制用）。

目的
----
把 `pond/cryptos` 那套已经跑通的 9 家交易所适配器，换成写 SQLite ——
于是同一份采集逻辑可以**原样搬到云服务器**上跑：不需要 ClickHouse、
不需要联网访问本机数据库，只要 python3 + requests + loguru + pyyaml。

接口
----
鸭子类型对齐 `pond.cryptos.store.CryptoStore` 中被 `sync.py` 实际调用的方法：
    .database / .insert() / .query() / .venue_config_rows()
    .write_sync_stat() / .count_window() / .distinct_codes() / .latest_time()
    .command() / .finalize()

时间一律存 **epoch 秒（INTEGER）**：SQLite 没有时区概念，
存整数可以避免「本地时区 vs UTC vs 交易所时区」三方扯皮。

去重靠主键 + `INSERT OR REPLACE`：ClickHouse 侧是 ReplacingMergeTree(ingested_at)，
语义正好对应「同一个 key、后写覆盖前写」。
"""
from __future__ import annotations

import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from pond.cryptos.schema import COLUMNS

# ── 建表语句（与 sql/cryptos_ddl.sql 的列一一对应，时间列改 INTEGER epoch）──
DDL: dict[str, str] = {
    "instrument": """
        CREATE TABLE IF NOT EXISTS instrument (
            venue TEXT NOT NULL, code TEXT NOT NULL, raw_code TEXT,
            base TEXT, quote TEXT, contract_type TEXT,
            multiplier REAL, listed_at INTEGER, delisted_at INTEGER,
            ingested_at INTEGER,
            PRIMARY KEY (venue, code))""",
    "kline_futures_1h": """
        CREATE TABLE IF NOT EXISTS kline_futures_1h (
            venue TEXT NOT NULL, code TEXT NOT NULL, open_time INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, datetime INTEGER, quote_volume REAL, count INTEGER,
            taker_buy_volume REAL, taker_buy_quote_volume REAL, ingested_at INTEGER,
            PRIMARY KEY (venue, code, open_time))""",
    "kline_spot_1h": """
        CREATE TABLE IF NOT EXISTS kline_spot_1h (
            venue TEXT NOT NULL, code TEXT NOT NULL, open_time INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, datetime INTEGER, quote_volume REAL, count INTEGER,
            taker_buy_volume REAL, taker_buy_quote_volume REAL, ingested_at INTEGER,
            PRIMARY KEY (venue, code, open_time))""",
    "funding_rate": """
        CREATE TABLE IF NOT EXISTS funding_rate (
            venue TEXT NOT NULL, code TEXT NOT NULL, datetime INTEGER NOT NULL,
            fundingRate REAL, markPrice REAL, interval_sec INTEGER, ingested_at INTEGER,
            PRIMARY KEY (venue, code, datetime))""",
    "open_interest": """
        CREATE TABLE IF NOT EXISTS open_interest (
            venue TEXT NOT NULL, code TEXT NOT NULL, datetime INTEGER NOT NULL,
            sumOpenInterest REAL, sumOpenInterestValue REAL, ingested_at INTEGER,
            PRIMARY KEY (venue, code, datetime))""",
    "mark_price": """
        CREATE TABLE IF NOT EXISTS mark_price (
            venue TEXT NOT NULL, code TEXT NOT NULL, datetime INTEGER NOT NULL,
            markPrice REAL, indexPrice REAL, ingested_at INTEGER,
            PRIMARY KEY (venue, code, datetime))""",
    "fx_rate": """
        CREATE TABLE IF NOT EXISTS fx_rate (
            datetime INTEGER NOT NULL, base TEXT NOT NULL, quote TEXT NOT NULL,
            rate REAL, source TEXT, ingested_at INTEGER,
            PRIMARY KEY (base, quote, datetime))""",
    "sync_stat": """
        CREATE TABLE IF NOT EXISTS sync_stat (
            venue TEXT NOT NULL, window_start INTEGER, window_end INTEGER,
            status TEXT, requests INTEGER, retries INTEGER, http_429 INTEGER,
            rows INTEGER, codes INTEGER, avg_latency_ms REAL, duration_ms INTEGER,
            checked_at INTEGER NOT NULL,
            PRIMARY KEY (venue, checked_at))""",
}

# 每张表的时间列（用于保留期清理）
TIME_COL: dict[str, str] = {
    "kline_futures_1h": "open_time",
    "kline_spot_1h": "open_time",
    "funding_rate": "datetime",
    "open_interest": "datetime",
    "mark_price": "datetime",
    "fx_rate": "datetime",
    "sync_stat": "checked_at",
}

# 这些列不是时间 → 直接透传
_TS_COLS = {
    "open_time", "datetime", "listed_at", "delisted_at", "ingested_at",
    "window_start", "window_end", "checked_at",
}


def to_ts(v: Any) -> int | None:
    """datetime / 字符串 / int → epoch 秒。"""
    if v is None:
        return None
    if isinstance(v, datetime):
        return int(v.timestamp()) if v.tzinfo else int(v.replace(tzinfo=timezone.utc).timestamp())
    if isinstance(v, (int, float)):
        return int(v)
    return int(datetime.fromisoformat(str(v)).timestamp())


def from_ts(v: Any) -> datetime | None:
    if v is None:
        return None
    return datetime.fromtimestamp(int(v), tz=timezone.utc)


class SqliteStore:
    """与 CryptoStore 同接口，写 SQLite。线程安全（采集在 asyncio.to_thread 里调）。"""

    database = "cryptos"  # sync.py 的 SQL 里会拼这个前缀

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._con = sqlite3.connect(self.path, check_same_thread=False)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.execute("PRAGMA temp_store=MEMORY")
        self._con.execute("PRAGMA cache_size=-64000")  # 64 MB
        for ddl in DDL.values():
            self._con.execute(ddl)
        self._con.commit()

    # ---------------- 写 ----------------

    def insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        cols = COLUMNS[table]
        idx_ts = [i for i, c in enumerate(cols) if c in _TS_COLS]
        data = []
        for r in rows:
            vals = []
            for i, c in enumerate(cols):
                v = r.get(c)
                vals.append(to_ts(v) if i in idx_ts else v)
            data.append(tuple(vals))
        ph = ",".join("?" * len(cols))
        sql = f'INSERT OR REPLACE INTO "{table}" VALUES ({ph})'
        with self._lock:
            self._con.executemany(sql, data)
            self._con.commit()
        return len(rows)

    def write_sync_stat(self, stat: dict[str, Any]) -> None:
        self.insert("sync_stat", [stat])

    # ---------------- 读 ----------------

    def query(self, sql: str, params: dict | None = None) -> list[tuple]:
        """只翻译 sync.py 实际用到的那一条（instrument 计数），其余直接报错——不猜。"""
        p = params or {}
        m = re.search(r"SELECT\s+count\(\)\s+FROM\s+\S*\.?(\w+)\s+WHERE\s+venue\s*=\s*%\(v\)s", sql)
        if m and "instrument" in sql:
            with self._lock:
                return self._con.execute(
                    f"SELECT count(*) FROM {m.group(1)} WHERE venue = ?", (p.get("v"),)
                ).fetchall()
        raise NotImplementedError(f"SqliteStore.query 不支持这条 SQL: {sql[:120]}")

    def count_window(self, table: str, venue: str, start: datetime, end: datetime) -> int:
        tcol = TIME_COL[table]
        with self._lock:
            return self._con.execute(
                f'SELECT count(*) FROM "{table}" WHERE venue=? AND {tcol}>=? AND {tcol}<?',
                (venue, to_ts(start), to_ts(end)),
            ).fetchone()[0]

    def distinct_codes(self, table: str, venue: str, start: datetime, end: datetime) -> int:
        tcol = TIME_COL[table]
        with self._lock:
            return self._con.execute(
                f'SELECT count(DISTINCT code) FROM "{table}" WHERE venue=? AND {tcol}>=? AND {tcol}<?',
                (venue, to_ts(start), to_ts(end)),
            ).fetchone()[0]

    def latest_time(self, table: str, venue: str) -> datetime | None:
        tcol = TIME_COL[table]
        with self._lock:
            v = self._con.execute(
                f'SELECT max({tcol}) FROM "{table}" WHERE venue=?', (venue,)
            ).fetchone()[0]
        return from_ts(v)

    def venue_config_rows(self) -> list[dict[str, Any]]:
        """云上单机录制不读运行期覆盖表，配置全部来自 configs/cryptos.yaml。"""
        return []

    def command(self, sql: str) -> None:
        with self._lock:
            self._con.execute(sql)
            self._con.commit()

    def finalize(self) -> None:
        with self._lock:
            self._con.close()

    # ---------------- 运维 ----------------

    def prune(self, retain_days: int = 365, max_size_gb: float | None = None) -> dict[str, int]:
        """时间保留期 + 可选的体积上限，两道闸防止空间无限增长。

        1) 先删掉早于 retain_days 的行（默认 365 天）
        2) 若给了 max_size_gb 且文件仍超限，按 30 天为步长继续删最老的数据，
           直到落在上限内（最多退 24 次 = 2 年）
        返回每张表累计删除的行数。
        """
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=retain_days)).timestamp())
        deleted: dict[str, int] = {}
        with self._lock:
            for table, tcol in TIME_COL.items():
                cur = self._con.execute(f'DELETE FROM "{table}" WHERE {tcol} < ?', (cutoff,))
                deleted[table] = cur.rowcount
            self._con.commit()

            if max_size_gb:
                for _ in range(24):
                    self._con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    if self.path.stat().st_size / 1e9 <= max_size_gb:
                        break
                    lo = min(
                        (v for t, c in TIME_COL.items()
                         if (v := self._con.execute(f'SELECT min({c}) FROM "{t}"').fetchone()[0]) is not None),
                        default=None,
                    )
                    if lo is None:
                        break
                    step = int(lo) + 30 * 86400
                    for table, tcol in TIME_COL.items():
                        cur = self._con.execute(f'DELETE FROM "{table}" WHERE {tcol} < ?', (step,))
                        deleted[table] += cur.rowcount
                    self._con.commit()
                    self._con.execute("VACUUM")

            if sum(deleted.values()) > 5_000:
                self._con.execute("VACUUM")   # 删得多才重写文件（VACUUM 很贵）
            self._con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return deleted

    def stats(self) -> dict[str, Any]:
        # WAL 模式下数据可能还在 -wal 里，先 checkpoint 再量体积，否则永远读到 0 MB
        self._con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        out: dict[str, Any] = {"path": str(self.path), "size_mb": round(self.path.stat().st_size / 1e6, 1), "tables": {}}
        with self._lock:
            for table, tcol in TIME_COL.items():
                n = self._con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
                lo, hi = self._con.execute(f'SELECT min({tcol}), max({tcol}) FROM "{table}"').fetchone()
                cols = {r[1] for r in self._con.execute(f'PRAGMA table_info("{table}")').fetchall()}
                if "venue" in cols:
                    per_venue = self._con.execute(
                        f'SELECT venue, count(*) FROM "{table}" GROUP BY venue ORDER BY 2 DESC'
                    ).fetchall()
                else:  # fx_rate：按 源货币对 分组
                    per_venue = self._con.execute(
                        f'SELECT quote, count(*) FROM "{table}" GROUP BY quote ORDER BY 2 DESC'
                    ).fetchall()
                out["tables"][table] = {
                    "rows": n,
                    "min": from_ts(lo).isoformat()[:19] if lo else None,
                    "max": from_ts(hi).isoformat()[:19] if hi else None,
                    "by_venue": {v: c for v, c in per_venue},
                }
        return out
