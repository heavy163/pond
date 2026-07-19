#!/usr/bin/env bash
set -euo pipefail

# 每日代币解锁数据同步
# 添加到 crontab：  crontab -e
#   0 2 * * * /path/to/pond/scripts/cron_sync_token_unlock.sh >> /var/log/pond/token_unlock.log 2>&1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/pond}"

cd "$PROJECT_DIR"

# 日志
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/token_unlock.log"

echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_unlock sync start ---" >> "$LOG_FILE"

# 如果存在 pyenv/conda 虚拟环境，在此激活
# 例如： source /opt/miniconda3/etc/profile.d/conda.sh && conda activate pond

if python3 "$PROJECT_DIR/examples/sync_token_unlock.py" --window 90 >> "$LOG_FILE" 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') sync SUCCESS" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') sync FAILED (exit code $?)" >> "$LOG_FILE"
fi

echo "--- $(date '+%Y-%m-%d %H:%M:%S') token_unlock sync end ---" >> "$LOG_FILE"
