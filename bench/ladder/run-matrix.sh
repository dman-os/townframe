#!/usr/bin/env bash
# Interleaved rounds over the rungs: for each round, each rung runs all its workloads once, so a
# load spike from another agent hits every rung of a round rather than one rung. Appends to
# $LADDER_OUT; the servers (serve.ts, ports 4701 / 4702) must already be up.
#   run-matrix.sh <rounds> <rung>...
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
rounds=$1; shift
export LADDER_OUT=${LADDER_OUT:-/run/media/asdf/p3N/tmp/x4/results.jsonl}
declare -A WL=(
  [r0]=corpus-rust
  [r1-dom]=syn7,syn18,corpus
  [r1-html]=syn7,syn18,corpus,corpus-rust
  [r1-static]=syn7,syn18,corpus
  [r2]=syn7,syn18,corpus
  [r3]=syn7,syn18,corpus
  [r3a]=syn7,syn18,corpus
  [r3b]=syn7,syn18,corpus
)
# MAX_LOAD=<n>: before each rung, wait (up to 15 minutes) for the 1-minute load average to fall to
# n, so rounds on a shared host mostly land under the "load <= 10" cut the tables apply.
wait_load() {
  [ -z "${MAX_LOAD:-}" ] && return
  for _ in $(seq 1 90); do
    awk -v m="$MAX_LOAD" '{exit !($1 <= m)}' /proc/loadavg && return
    sleep 10
  done
}
for round in $(seq "${START:-1}" "$((${START:-1} + rounds - 1))"); do
  for rung in "$@"; do
    wait_load
    echo "=== round $round $rung $(uptime)"
    (cd "$here" && LADDER_RUNG=$rung LADDER_WORKLOADS=${WL[$rung]} LADDER_ROUND=$round \
      npx playwright test -c playwright.config.ts --workers=1 2>&1 | grep -v '^LADDER' | tail -15)
  done
done
