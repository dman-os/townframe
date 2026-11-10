#!/usr/bin/env bash
# W1/W2 matrix: one playwright run per (mode, round), under the X5 guards (load <= 8, battery
# >= 30% or charging). Run inside the dev shell, wrapped in `mbx reserve --cpus 4 --memory 8G --`.
#   run-w1w2.sh <mode: w1|w2|resize> <rounds> [start]   (extra env: W2_N, W_GREP)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
mode=$1; rounds=$2; start=${3:-1}
export MAX_LOAD=${MAX_LOAD:-8}
wait_load() {
  for _ in $(seq 1 90); do
    awk -v m="$MAX_LOAD" '{exit !($1 <= m)}' /proc/loadavg && return
    sleep 10
  done
}
wait_power() {
  for _ in $(seq 1 240); do
    cap=$(cat /sys/class/power_supply/BAT*/capacity 2>/dev/null | head -1); st=$(cat /sys/class/power_supply/BAT*/status 2>/dev/null | head -1)
    [ -z "$cap" ] && return
    { [ "$cap" -ge 30 ] || [ "$st" = Charging ] || [ "$st" = Full ]; } && return
    sleep 60
  done
}
for round in $(seq "$start" "$((start + rounds - 1))"); do
  wait_power; wait_load
  echo "=== $mode round $round $(uptime)"
  (cd "$here" && LADDER_SPEC=w1w2.spec.ts W_MODE=$mode W_ROUND=$round \
    npx playwright test -c playwright.config.ts --workers=1 ${W_GREP:+-g "$W_GREP"} 2>&1 | grep -E '^(W1|W2|RESIZE)|failed|Error|passed|✘' | head -40)
done
