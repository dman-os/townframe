#!/usr/bin/env bash
# Runs fidelity.mjs for every engine dump against the Shiki (Oniguruma) oracle and prints JSON lines.
# usage: fidelity-table.sh <dump-dir> -> one JSON object per (engine, lang, size)
set -euo pipefail
d=$1
for e in shiki-js highlights irosashi irosashi-min irosashi-patched arborium syntect; do
  for l in rust ts json md; do for s in 1k 10k; do
    [ -f "$d/$e.$l-$s.tokens.json" ] || continue
    r=$(node "$(dirname "$0")/fidelity.mjs" "$d/shiki-onig.$l-$s.tokens.json" "$d/$e.$l-$s.tokens.json" 0 2>/dev/null | head -1)
    echo "{\"engine\":\"$e\",\"lang\":\"$l\",\"size\":\"$s\",\"r\":$r}"
  done; done
done
