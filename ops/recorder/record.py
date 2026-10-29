"""单机版交易所数据录制（SQLite 落盘）。

跟本地那套写 ClickHouse 的采集是**同一份适配器代码**（pond/cryptos/venues/*），
只换存储后端 —— 所以搬到云服务器上跑时行为一致，不会出现「云上采的跟前端不一样」。

用法
----
    # 跑一轮（默认全部启用的交易所），写完自动按保留期清理
    /usr/bin/python3.12 ops/recorder/record.py once

    # 只跑指定交易所（调试用）
    /usr/bin/python3.12 ops/recorder/record.py once --venues bybit okx

    # 常驻：每小时 :05 跑一轮，每天清理一次
    /usr/bin/python3.12 ops/recorder/record.py loop

    # 看现状 / 手动清理
    /usr/bin/python3.12 ops/recorder/record.py status
    /usr/bin/python3.12 ops/recorder/record.py prune --retain-days 365

云服务器部署
------------
    python3 -m venv .venv && .venv/bin/pip install requests aiohttp loguru pyyaml
    git clone <repo> && cd pond
    .venv/bin/python ops/recorder/record.py loop

没有 ClickHouse、没有 MQTT、没有本机数据库依赖。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pond.cryptos.notify import LogNotifier  # noqa: E402
from pond.cryptos.sync import CryptoHelper  # noqa: E402

from sqlite_store import SqliteStore  # noqa: E402  (同目录)

DEFAULT_DB = Path.home() / "recorder" / "cryptos.sqlite"
DEFAULT_CONFIG = ROOT / "configs" / "cryptos.yaml"


def build_helper(args) -> tuple[CryptoHelper, SqliteStore]:
    store = SqliteStore(args.db)
    helper = CryptoHelper(
        store=store,  # type: ignore[arg-type]  # 鸭子类型，接口对齐 CryptoStore
        config_path=str(args.config),
        plan="recorder",
        # 云上没有 dove MQTT Broker：强制走日志，不发消息也不报错
        notifier=LogNotifier("recorder"),
    )
    return helper, store


async def run_once(args) -> int:
    helper, store = build_helper(args)
    t0 = time.monotonic()
    try:
        results = await helper.run_round(venues=args.venues, refresh_instruments=args.refresh_instruments)
    finally:
        helper.close()

    print(f"\n{'交易所':<14}{'状态':<10}{'行数':>10}{'标的':>7}{'耗时':>9}  错误")
    total = 0
    bad = 0
    for r in sorted(results, key=lambda x: x.venue):
        total += sum(r.rows.values()) if isinstance(r.rows, dict) else 0
        if r.status != "synced":
            bad += 1
        err = "; ".join(r.errors[:2])[:60] if r.errors else ""
        print(f"{r.venue:<14}{r.status:<10}{sum(r.rows.values()):>10,}{r.codes:>7}{r.duration_ms/1000:>8.1f}s  {err}")

    print(f"\n合计 {total:,} 行，{len(results)} 家，非 synced {bad} 家，"
          f"总耗时 {time.monotonic()-t0:.1f}s")
    print(f"SQLite: {store.path}  {store.stats()['size_mb']:.1f} MB")  # stats 内部会 checkpoint，读到真实大小
    store.finalize()
    return 0 if bad == 0 else 1


def cmd_once(args) -> int:
    return asyncio.run(run_once(args))


def cmd_loop(args) -> int:
    """常驻：整点后第 5 分钟跑一轮（交易所 K 线在整点收盘，留 5 分钟给数据落定）。"""
    logger.info(f"recorder loop 启动，db={args.db}，保留 {args.retain_days} 天")
    last_prune = 0.0
    while True:
        now = datetime.now(timezone.utc)
        nxt = now.replace(minute=5, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(hours=1)
        wait = (nxt - now).total_seconds()
        logger.info(f"下一轮 {nxt.isoformat()}（{wait/60:.1f} 分钟后）")
        time.sleep(max(1.0, wait))

        rc = asyncio.run(run_once(args))
        if rc != 0:
            logger.warning(f"本轮有交易所未 synced（rc={rc}），继续下一轮")

        # 每天清理一次
        if time.monotonic() - last_prune > 86400:
            helper, store = build_helper(args)
            deleted = store.prune(args.retain_days, args.max_size_gb)
            store.finalize()
            helper.close()
            logger.info(f"[prune] 保留 {args.retain_days} 天，删除 {deleted}")
            last_prune = time.monotonic()


def cmd_prune(args) -> int:
    store = SqliteStore(args.db)
    deleted = store.prune(args.retain_days, args.max_size_gb)
    st = store.stats()
    store.finalize()
    print(f"保留 {args.retain_days} 天，删除：")
    for t, n in deleted.items():
        print(f"  {t:<22}{n:>10,}")
    print(f"\n剩余 {st['size_mb']} MB @ {st['path']}")
    return 0


def cmd_status(args) -> int:
    store = SqliteStore(args.db)
    st = store.stats()
    store.finalize()
    print(f"文件: {st['path']}  {st['size_mb']} MB")
    for t, s in st["tables"].items():
        print(f"\n{t}  {s['rows']:,} 行  {s['min']} → {s['max']}")
        for v, c in list(s["by_venue"].items())[:12]:
            print(f"    {v:<14}{c:>10,}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="单机版交易所数据录制（SQLite）")
    ap.add_argument("--db", default=str(DEFAULT_DB), help=f"SQLite 路径（默认 {DEFAULT_DB}）")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG), help="cryptos.yaml 路径")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("once")
    p.add_argument("--venues", nargs="*")
    p.add_argument("--refresh-instruments", action="store_true")
    p.add_argument("--retain-days", type=int, default=365)
    p.set_defaults(func=cmd_once, retain_days=365)

    p = sub.add_parser("loop")
    p.add_argument("--retain-days", type=int, default=365)
    p.add_argument("--max-size-gb", type=float, default=None)
    p.set_defaults(func=cmd_loop)

    p = sub.add_parser("prune")
    p.add_argument("--retain-days", type=int, default=365)
    p.add_argument("--max-size-gb", type=float, default=None, help="体积上限，超了继续删最老的")
    p.set_defaults(func=cmd_prune)

    p = sub.add_parser("status")
    p.set_defaults(func=cmd_status)

    args = ap.parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> {level: <7} {message}")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
