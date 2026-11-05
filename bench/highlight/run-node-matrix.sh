#!/usr/bin/env bash
# Node (V8, the same engine family as Chromium) matrix: one process per cell so rssDelta is per cell.
# usage: run-node-matrix.sh <corpus-dir> <dump-dir> <out.jsonl>
set -euo pipefail
corpus=$1; dump=$2; out=$3; : > "$out"
for eng in highlights shiki-onig shiki-js; do
  for lang in rust ts json md; do
    for size in 1k 10k 100k; do
      reps=5; [ "$size" = 100k ] && reps=3
      d=""; [ "$size" != 100k ] && d=$dump
      node --max-old-space-size=6000 bench-js.mjs "$eng" "$corpus" "$lang" "$size" "$reps" $d >> "$out"
    done
  done
done
