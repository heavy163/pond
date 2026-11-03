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

# 同步后是否要立刻查缺口：只在出错时才查，见下面注释
GAP_SUMMARY="$(tail -n 60 "$LOG" | grep -cE '\b(partial|failed)\b' || true)"
if [ "$rc" -eq 0 ] && [ "${GAP_SUMMARY:-0}" -eq 0 ]; then
    # 常规轮次没事就不查缺口了。
    #   1) 同步已改成「按合约精确增量」：每轮拿各合约库里的 max(时间) 续拉，
    #      6 小时内的空档会在下一轮自动补上，不需要再扫一遍；
    #   2) 每小时跑一次 check_gaps --days 2 是重复劳动，纯浪费请求和流量；
    #   3) 长空档（>6h，如停机过夜）交给每天 00:20 的 cryptos-gapcheck.timer。
    # 只有本轮出现 partial/failed 才立刻补一次，保住「出问题当场修」这条底线。
    echo "[gapcheck] 本轮无 partial/failed，跳过小时级补采" >> "$LOG"
else
    /usr/bin/python3.12 scripts/check_gaps.py --days 2 --execute --no-coverage 2>&1 | tee -a "$LOG"
    gap_rc=${PIPESTATUS[0]}
    [ "$gap_rc" -ne 0 ] && echo "[gapcheck] exit=${gap_rc}" >> "$LOG"
fi

echo "[${ts_start}] exit=${rc}" >> "$LOG"

# 滚动截断：只在超过 2*KEEP 行时裁一次，避免每轮都重写整个文件
lines=$(wc -l < "$LOG")
if [ "$lines" -gt $((KEEP * 2)) ]; then
    tail -n "$KEEP" "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
fi

exit "$rc"
