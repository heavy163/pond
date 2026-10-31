#!/usr/bin/env python
"""cryptos 1h 同步 CLI。

    # 干跑（只拉不写）
    python scripts/sync_cryptos.py --venue binance --symbols BTCUSDT,ETHUSDT

    # 真写
    python scripts/sync_cryptos.py --execute --symbols BTCUSDT,ETHUSDT,SOLUSDT

    # 全宇宙（合约清单每轮自动刷新，无需额外开关）
    python scripts/sync_cryptos.py --execute

退出码：全部 success → 0；有 failed/timeout → 1（systemd 据此告警）。
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from pond.cryptos.config import CryptosConfig
from pond.cryptos.notify import build_notifier
from pond.cryptos.store import CryptoStore
from pond.cryptos.sync import CryptoHelper


def parse_args():
    p = argparse.ArgumentParser(description="cryptos 1h 同步")
    p.add_argument("--venue", action="append", help="只跑指定交易所（可多次），默认全部 enabled")
    p.add_argument("--lookback", type=int, help="回溯小时数，覆盖配置")
    p.add_argument("--symbols", help="逗号分隔 raw symbol，覆盖配置的 include")
    p.add_argument("--data", help="逗号分隔数据类型 kline,spot,funding,oi,mark")
    p.add_argument("--config", default=None, help="YAML 路径")
    p.add_argument("--lock-wait", type=int, default=0,
                   help="抢不到同步锁时最多等多少秒（默认 0=直接跳过本轮）")
    p.add_argument("--mqtt", default="auto", choices=["auto", "mqtt", "log", "off"])
    p.add_argument("--execute", action="store_true", help="真正写库（默认 dry-run 只拉不写）")
    p.add_argument("--json", action="store_true", help="末尾打印 JSON 便于脚本消费")
    return p.parse_args()


def _acquire_lock(wait_sec: float = 0.0):
    """同一个库只允许一轮同步在跑。

    2026-11-01 事故：00:02 整轮同步和 00:05 合约刷新同时跑，两轮抢同一批
    交易所限速 → 7/8 所 status=partial，且同一个 window_end 写了两份。
    这里用 flock 串行化：普通轮次抢不到就跳过（下一小时再来），
    合约刷新最多等 15 分钟。
    """
    d = Path.home() / ".local/state/cryptos"
    d.mkdir(parents=True, exist_ok=True)
    fh = open(d / "sync.lock", "a+")   # a+ 不截断别人持有的文件
    deadline = time.monotonic() + wait_sec
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fh.write(f"{os.getpid()}\n")
            fh.flush()
            return fh
        except BlockingIOError:
            if time.monotonic() >= deadline:
                fh.close()
                return None
            time.sleep(5)


def main() -> int:
    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    lock = _acquire_lock(wait_sec=args.lock_wait)
    if lock is None:
        logger.warning("另一轮同步正在运行（~/.local/state/cryptos/sync.lock 被占用），本轮跳过")
        return 0

    store = CryptoStore.from_env()
    overrides: dict = {}
    if args.lookback:
        overrides["lookback_hours"] = args.lookback
    if args.symbols:
        overrides["symbols"] = [x.strip() for x in args.symbols.split(",") if x.strip()]
    if args.data:
        overrides["data"] = [x.strip() for x in args.data.split(",") if x.strip()]
    cfg = CryptosConfig.load(args.config)

    notifier = build_notifier(args.mqtt, plan="crypto")
    helper = CryptoHelper(
        store=store,
        config=cfg,
        notifier=notifier,
        config_path=args.config,
        dry_run=not args.execute,
        overrides=overrides,
    )

    if not args.execute:
        logger.warning("dry-run：只拉取，不写库。加 --execute 才会写。")

    results = asyncio.run(
        helper.run_round(
            venues=args.venue
        )
    )

    print("\n" + "=" * 78)
    print(f"{'venue':<12}{'status':<10}{'codes':>7}{'rows':>10}{'耗时':>10}{'errors':>8}")
    for r in results:
        print(
            f"{r.venue:<12}{r.status:<10}{r.codes:>7}{sum(r.rows.values()):>10}"
            f"{r.duration_ms/1000:>9.1f}s{len(r.errors):>8}"
        )
        for e in r.errors[:5]:
            print(f"    ! {e}")
    print("=" * 78)

    if args.json:
        import json

        print(
            json.dumps(
                [
                    {
                        "venue": r.venue,
                        "status": r.status,
                        "rows": r.rows,
                        "codes": r.codes,
                        "duration_ms": r.duration_ms,
                        "errors": r.errors,
                    }
                    for r in results
                ],
                ensure_ascii=False,
            )
        )

    helper.close()
    store.finalize()
    return 0 if all(r.status in ("synced", "partial") for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
