#!/usr/bin/env bash
# Runs every built artifact over the corpus. usage: run-matrix.sh <bin-dir> <corpus-dir> <out-dir> <engine>...
# native: the release binary; wasip3: wasmtime CLI (-S p3, same engine as the guest runtime); web: Node/V8.
set -euo pipefail
bin=$1; corpus=$2; out=$3; shift 3
mkdir -p "$out"
declare -A ext=([rust]=rs [ts]=ts [json]=json [md]=md)
for e in "$@"; do
  : > "$out/$e.native.jsonl"; : > "$out/$e.wasip3.jsonl"; : > "$out/$e.web.jsonl"
  for lang in rust ts json md; do
    for size in 1k 10k 100k; do
      f="$corpus/$lang-$size.${ext[$lang]}"
      nr=5; wr=5; if [ "$size" = 10k ]; then nr=5; wr=3; fi; if [ "$size" = 100k ]; then nr=3; wr=2; fi
      # fancy-regex syntect is orders slower: one warm rep at 100k.
      if [ "$e" = syntect ] && [ "$size" = 100k ]; then nr=2; wr=2; fi
      "$bin/native-$e" "$e" "$lang" "$f" "$size" "$nr" >> "$out/$e.native.jsonl"
      # tree-sitter (arborium) has no wasip3 artifact: see the findings, C objects do not link there.
      if [ -f "$bin/wasip3-$e.wasm" ]; then
        wasmtime run -W component-model-async=y -S p3 --dir "$corpus" "$bin/wasip3-$e.wasm" "$e" "$lang" "$f" "$size" "$wr" >> "$out/$e.wasip3.jsonl"
      fi
      node bench-web.mjs "$bin/web-$e" "$e" "$corpus" "$lang" "$size" "$wr" >> "$out/$e.web.jsonl"
    done
  done
done
