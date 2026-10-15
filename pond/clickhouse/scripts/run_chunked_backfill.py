"""分块回填驱动：把一个长跑回填任务切成 N 块，每块用【独立子进程】执行。

为什么必须这样：baostock 的模块级 socket 在连续大量请求后会损坏
（实测 `'utf-8' codec can't decode byte 0xf9` → `error_code 10002007 网络接收错误`
→ 库内部状态坏死后**全线程 futex 死锁**，且库本身无任何超时/清理机制）。
一个长驻进程跑几千次请求必然积累坏状态，且挂死后没有任何自愈机会。

本驱动的保证：
  1. 每块一个全新进程 ⇒ socket 全新，坏状态不跨块累积；
  2. 每块有外部硬超时 ⇒ 挂死块会被强杀（含进程组），不会占住一夜；
  3. 失败块自动重试 ⇒ 瞬时故障能自愈；
  4. 单块失败**不中断整体**，最后汇总报告 ⇒ 长任务可无人值守；
  5. 幂等 ⇒ 重跑同一块安全（ReplacingMergeTree 按主键去重）。

用法：
    PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \\
        pond/clickhouse/scripts/run_chunked_backfill.py \\
        pond/clickhouse/scripts/load_baostock_etf_daily_to_db.py \\
        --total 1594 --chunk-size 150 --timeout 900 --retries 3
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def run_chunk(
    script: Path,
    offset: int,
    limit: int,
    timeout: int,
    log_path: Path,
    extra_env: dict[str, str],
) -> tuple[int, bool]:
    """跑一块。返回 (returncode, timed_out)。用进程组确保超时能连子线程一起杀干净。"""
    env = os.environ.copy()
    env.update(extra_env)
    env["SYMBOL_OFFSET"] = str(offset)
    env["SYMBOL_LIMIT"] = str(limit)
    with open(log_path, "wb") as log_fh:
        proc = subprocess.Popen(
            [sys.executable, "-u", str(script)],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(REPO_ROOT),
            start_new_session=True,  # 自成进程组，便于整组清理
        )
        try:
            return proc.wait(timeout=timeout), False
        except subprocess.TimeoutExpired:
            # baostock 挂死时子进程可能有几十个线程，必须整组 SIGKILL
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
            return -9, True


def main() -> int:
    ap = argparse.ArgumentParser(description="分块回填驱动")
    ap.add_argument("script", help="目标 load 脚本路径（须支持 SYMBOL_OFFSET/SYMBOL_LIMIT）")
    ap.add_argument("--total", type=int, required=True, help="符号总数")
    ap.add_argument("--chunk-size", type=int, default=150)
    ap.add_argument("--timeout", type=int, default=900, help="单块硬超时（秒）")
    ap.add_argument("--retries", type=int, default=3, help="单块最大尝试次数")
    ap.add_argument("--log-dir", default="/tmp/chunked_backfill")
    args = ap.parse_args()

    script = Path(args.script).resolve()
    if not script.exists():
        print(f"❌ 脚本不存在: {script}")
        return 2

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    offsets = list(range(0, args.total, args.chunk_size))
    print(f"=== 分块回填: {script.name} ===")
    print(f"    总数 {args.total}，块大小 {args.chunk_size}，共 {len(offsets)} 块")
    print(f"    单块超时 {args.timeout}s，重试上限 {args.retries}，日志 {log_dir}")
    print(flush=True)

    failed: list[tuple[int, int]] = []
    t0 = time.time()
    for idx, off in enumerate(offsets, 1):
        limit = min(args.chunk_size, args.total - off)
        for attempt in range(1, args.retries + 1):
            log_path = log_dir / f"{stamp}_off{off:05d}_try{attempt}.log"
            t = time.time()
            rc, timed_out = run_chunk(script, off, limit, args.timeout, log_path, {})
            dt = time.time() - t
            if rc == 0:
                print(
                    f"  [{idx}/{len(offsets)}] offset={off:<6} limit={limit:<5} "
                    f"OK  {dt:6.1f}s",
                    flush=True,
                )
                break
            tag = "TIMEOUT" if timed_out else f"rc={rc}"
            print(
                f"  [{idx}/{len(offsets)}] offset={off:<6} limit={limit:<5} "
                f"{tag} {dt:6.1f}s (尝试 {attempt}/{args.retries}) {log_path}",
                flush=True,
            )
            if attempt < args.retries:
                time.sleep(5)
        else:
            failed.append((off, limit))

    print()
    print(f"=== 完成，用时 {(time.time() - t0) / 60:.1f} 分钟 ===")
    print(f"    成功块 {len(offsets) - len(failed)}/{len(offsets)}")
    if failed:
        print(f"    ⚠️ 失败块 {len(failed)} 个（重跑同一命令即可续补，幂等）:")
        for off, limit in failed:
            print(f"       offset={off} limit={limit}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
