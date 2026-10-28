"""cryptos 存储层。

刻意不用 pond.clickhouse.ClickHouseManager：
  - 它 import ray / akshare / clickhouse_driver，采集容器里全是不需要的重依赖
  - 它在 __init__ 里 metadata.create_all()，会把全局 metadata 的表建到目标库
所以这里直接用 clickhouse_connect，表结构由 sql/cryptos_ddl.sql 唯一决定。

两个守卫：
  1. 构造期断言 database == 'cryptos'，杜绝误连 quant 生产库
  2. client 按线程隔离 —— clickhouse_connect 的 client 不是线程安全的，
     而写库走 asyncio.to_thread，多个线程并发用同一个 client 会报
     "Attempt to execute concurrent queries within the same session"
"""

from __future__ import annotations

import os
import threading
from datetime import datetime
from typing import Any

import clickhouse_connect
from loguru import logger

from pond.cryptos.schema import COLUMNS, TIME_COLUMN, validate_rows

CRYPTOS_DB = "cryptos"


class CryptoStore:
    def __init__(
        self,
        *,
        host: str = "192.168.1.200",
        port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = CRYPTOS_DB,
        connect_timeout: int = 15,
    ):
        if database != CRYPTOS_DB:
            raise ValueError(
                f"CryptoStore 只允许写 {CRYPTOS_DB} 库，收到 {database!r}（quant 是生产库，禁止触碰）"
            )
        self.database = database
        self._params = dict(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            connect_timeout=connect_timeout,
        )
        self._tls = threading.local()
        self._clients: list = []
        self._lock = threading.Lock()
        self._client()          # 启动即验证连通性
        logger.info(f"CryptoStore 已连接 {host}:{port}/{database}")

    @classmethod
    def from_env(cls) -> "CryptoStore":
        return cls(
            host=os.environ.get("CRYPTOS_CH_HOST", "192.168.1.200"),
            port=int(os.environ.get("CRYPTOS_CH_PORT", "8123")),
            user=os.environ.get("CRYPTOS_CH_USER", "default"),
            password=os.environ.get("CRYPTOS_CH_PASSWORD", ""),
            database=os.environ.get("CRYPTOS_CH_DB", CRYPTOS_DB),
        )

    # ---------------- 连接（按线程） ----------------

    def _client(self):
        c = getattr(self._tls, "client", None)
        if c is None:
            c = clickhouse_connect.get_client(**self._params)
            self._tls.client = c
            with self._lock:
                self._clients.append(c)
        return c

    # ---------------- 写 ----------------

    def insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        validate_rows(table, rows)
        cols = COLUMNS[table]
        data = [[r[c] for c in cols] for r in rows]
        self._client().insert(
            table=table, data=data, column_names=cols, database=self.database
        )
        return len(rows)

    # ---------------- 读 ----------------

    def query(self, sql: str, params: dict | None = None) -> list[tuple]:
        return self._client().query(sql, parameters=params or {}).result_rows

    def count_window(self, table: str, venue: str, start: datetime, end: datetime) -> int:
        """按 (venue, 时间窗) 统计不重复行数（FINAL 太慢，用 uniqExact）。"""
        tcol = TIME_COLUMN[table]
        rows = self.query(
            f"SELECT uniqExact(code, {tcol}) FROM {self.database}.{table} "
            f"WHERE venue = %(venue)s "
            f"AND toUnixTimestamp({tcol}) >= %(start)s "
            f"AND toUnixTimestamp({tcol}) < %(end)s",
            {"venue": venue, "start": int(start.timestamp()), "end": int(end.timestamp())},
        )
        return int(rows[0][0]) if rows else 0

    def distinct_codes(self, table: str, venue: str, start: datetime, end: datetime) -> int:
        tcol = TIME_COLUMN[table]
        rows = self.query(
            f"SELECT uniqExact(code) FROM {self.database}.{table} "
            f"WHERE venue = %(venue)s AND toUnixTimestamp({tcol}) >= %(start)s "
            f"AND toUnixTimestamp({tcol}) < %(end)s",
            {"venue": venue, "start": int(start.timestamp()), "end": int(end.timestamp())},
        )
        return int(rows[0][0]) if rows else 0

    def latest_time(self, table: str, venue: str) -> datetime | None:
        tcol = TIME_COLUMN[table]
        rows = self.query(
            f"SELECT max({tcol}) FROM {self.database}.{table} WHERE venue = %(venue)s",
            {"venue": venue},
        )
        v = rows[0][0] if rows else None
        if v is None or (isinstance(v, datetime) and v.year <= 1970):
            return None
        return v

    def venue_config_rows(self) -> list[dict[str, Any]]:
        """运行时配置覆盖（表不存在时返回空，不报错）。"""
        cols = [
            "venue",
            "enabled",
            "concurrency",
            "deadline_sec",
            "buckets_json",
            "data",
            "symbols_include",
            "symbols_exclude",
            "top_n",
        ]
        try:
            res = self._client().query(
                f"SELECT {', '.join(cols)} FROM {self.database}.venue_config FINAL"
            )
            return [dict(zip(cols, r)) for r in res.result_rows]
        except Exception as e:  # 表没建 / 权限不足都不该阻塞同步
            logger.warning(f"读取 venue_config 失败，使用配置文件值: {e}")
            return []

    def write_sync_stat(self, stat: dict[str, Any]) -> None:
        self.insert("sync_stat", [stat])

    def command(self, sql: str) -> None:
        """执行 DDL / ALTER / CREATE VIEW 这类不返回结果集的语句。"""
        self._client().command(sql)

    def finalize(self) -> None:
        for c in self._clients:
            try:
                c.close()
            except Exception:
                pass
        self._clients.clear()
