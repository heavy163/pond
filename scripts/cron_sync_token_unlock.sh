#!/usr/bin/env bash
# 每日代币解锁数据同步
# 已添加到 crontab：每日 02:00 执行
#
# 容错设计：
#   - 失败后等待 5 分钟自动重试 1 次
#   - sync 内部已分段保存（listing → detail batch），部分失败不丢数据
#   - 日志超过 5000 行自动截断，避免磁盘撑满

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/pond}"

cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/token_unlock.log"

# 如果存在 pyenv/conda 虚拟环境，在此激活
# source /opt/miniconda3/etc/profile.d/conda.sh && conda activate pond

do_sync() {
    echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_unlock sync start ---" >> "$LOG_FILE"
    if python3 "$PROJECT_DIR/examples/sync_token_unlock.py" --window 90 >> "$LOG_FILE" 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') sync SUCCESS" >> "$LOG_FILE"
        return 0
    else
        local rc=$?
        echo "$(date '+%Y-%m-%d %H:%M:%S') sync FAILED (exit code $rc)" >> "$LOG_FILE"
        return $rc
    fi
    echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_unlock sync end ---" >> "$LOG_FILE"
}

# 首次执行
if do_sync; then
    :
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') retrying in 5 minutes..." >> "$LOG_FILE"
    sleep 300
    do_sync || true
fi

# 日志截断：保留最后 5000 行
if [ "$(wc -l < "$LOG_FILE")" -gt 5000 ]; then
    tail -n 5000 "$LOG_FILE" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "$LOG_FILE"
fi
