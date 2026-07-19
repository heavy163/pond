#!/usr/bin/env bash
# 代币 DEX 流动性数据同步
# 已添加到 crontab：每 8 小时执行一次（00:00 / 08:00 / 16:00）
#
# 容错设计：
#   - 失败后等待 5 分钟自动重试 1 次
#   - 每 55 次请求暂停 60 秒，应对 DEX Screener 限速（60 req/min）
#   - 单次同步约 11 分钟，日志超过 5000 行自动截断

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/pond}"

cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/token_liquidity.log"

# 如果存在 pyenv/conda 虚拟环境，在此激活
# source /opt/miniconda3/etc/profile.d/conda.sh && conda activate pond

do_sync() {
    echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_liquidity sync start ---" >> "$LOG_FILE"
    if python3 "$PROJECT_DIR/examples/sync_token_liquidity.py" >> "$LOG_FILE" 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') sync SUCCESS" >> "$LOG_FILE"
        return 0
    else
        local rc=$?
        echo "$(date '+%Y-%m-%d %H:%M:%S') sync FAILED (exit code $rc)" >> "$LOG_FILE"
        return $rc
    fi
    echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_liquidity sync end ---" >> "$LOG_FILE"
}

do_sync || {
    echo "$(date '+%Y-%m-%d %H:%M:%S') retrying in 5 minutes..." >> "$LOG_FILE"
    sleep 300
    do_sync || true
}

if [ "$(wc -l < "$LOG_FILE")" -gt 5000 ]; then
    tail -n 5000 "$LOG_FILE" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "$LOG_FILE"
fi
