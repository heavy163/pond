#!/usr/bin/env bash
# cryptos 同步包装脚本（systemd timer 调用，也可手动跑）
#
# 为什么不让 systemd 直接调 python：
#   1. 需要把每轮输出落到滚动日志，journal 只保留有限长度且不好 grep 历史
#   2. 需要固定工作目录（仓库在 HGFS 挂载点上）
#   3. 需要把退出码原样传回 systemd（失败要能被 systemctl --failed 看到）
set -uo pipefail

REPO="/mnt/hgfs/e/workspace/FutureTradeDev/pond"
LOG_DIR="${HOME}/.local/state/cryptos"
LOG="${LOG_DIR}/sync.log"
KEEP=5000

mkdir -p "$LOG_DIR"
cd "$REPO" || { echo "仓库不存在: $REPO" >&2; exit 78; }

ts_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
{
    echo "==================== ${ts_start} ===================="
} >> "$LOG"

/usr/bin/python3.12 scripts/sync_cryptos.py --execute "$@" 2>&1 | tee -a "$LOG"
rc=${PIPESTATUS[0]}

echo "[${ts_start}] exit=${rc}" >> "$LOG"

# 滚动截断：只在超过 2*KEEP 行时裁一次，避免每轮都重写整个文件
lines=$(wc -l < "$LOG")
if [ "$lines" -gt $((KEEP * 2)) ]; then
    tail -n "$KEEP" "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
fi

exit "$rc"
