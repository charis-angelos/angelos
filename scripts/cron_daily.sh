#!/bin/bash
set -euo pipefail

# Daily cron job: generate a summary and update tasks.
# Expected to be run from the angelos/ directory.

cd "$(dirname "$0")/.."

# Load environment
export $(grep -v '^#' .env | xargs) 2>/dev/null || true
export RUST_LOG="${RUST_LOG:-warn}"

GATEWAY="./gateway/target/release/gateway"
LOG_DIR="./memory/logs"
TIMESTAMP=$(date +%Y-%m-%d_%H%M)
LOGFILE="$LOG_DIR/cron_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

{
    echo "=== cron_daily.sh started at $(date) ==="

    # Today's date for the prompt
    TODAY=$(date +%Y-%m-%d)

    PROMPT="今天是 ${TODAY}。
请执行以下每日任务：
1. 检查 memory/tasks/pending.md 中未完成的任务，生成一份今日待办摘要。
2. 如果 memory/daily/${TODAY}.md 已存在，总结其中的关键内容；如果不存在，创建一个包含今日待办的空模板。
3. 将以上内容整合为精炼的「今日简报」，输出为 Markdown。"

    echo "Running daily agent prompt..."
    "$GATEWAY" --mode cron --prompt "$PROMPT" 2>&1

    echo "=== cron_daily.sh finished at $(date) ==="
} >> "$LOGFILE" 2>&1
