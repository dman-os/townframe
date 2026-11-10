#!/usr/bin/env bash
# X5 matrix: interleaved rounds over the variants (one playwright run per variant per round, all
# fling cells inside), gated on host load like run-matrix.sh. Servers (serve.ts on 4711 / 4712)
# must already be up.   run-x5.sh <rounds> <variant>...
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
rounds=$1; shift
export X5_OUT=${X5_OUT:-/run/media/asdf/p3N/tmp/x5/results.jsonl}
wait_load() {
  [ -z "${MAX_LOAD:-}" ] && return
  for _ in $(seq 1 90); do
    awk -v m="$MAX_LOAD" '{exit !($1 <= m)}' /proc/loadavg && return
    sleep 10
  done
}
# Power guard (battery host): below 30% and not Full/Charging-above-30, wait.
wait_power() {
  for _ in $(seq 1 240); do
    cap=$(cat /sys/class/power_supply/BAT*/capacity 2>/dev/null | head -1); st=$(cat /sys/class/power_supply/BAT*/status 2>/dev/null | head -1)
    [ -z "$cap" ] && return
    { [ "$cap" -ge 30 ] && { [ "$st" = Charging ] || [ "$st" = Full ]; }; } || [ "$st" = Full ] || { sleep 60; continue; }
    return
  done
}
for round in $(seq "${START:-1}" "$((${START:-1} + rounds - 1))"); do
  for variant in "$@"; do
    wait_power
    wait_load
    echo "=== round $round $variant $(uptime)"
    (cd "$here" && LADDER_SPEC=x5.spec.ts X5_VARIANTS=$variant X5_ROUND=$round \
      npx playwright test -c playwright.config.ts --workers=1 2>&1 | grep -E '^X5|failed|Error|passed' | head -20)
  done
done
