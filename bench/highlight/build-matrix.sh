#!/usr/bin/env bash
# Builds every engine for native, wasm32-wasip3 and wasm32-unknown-unknown (+ wasm-bindgen nodejs glue)
# into <bin-dir>, one engine per build so each artifact's size belongs to exactly one candidate.
# Run from the worktree root: bench/highlight/build-matrix.sh <bin-dir> [engines...]
set -euo pipefail
# The nix shell exports CC=gcc; the cc crate would then build C grammars as x86 objects. Force clang.
export CC_wasm32_unknown_unknown=clang
bin=$1; shift
engines=${*:-irosashi syntect arborium}
mkdir -p "$bin"
T=$(./x/agent-build.ts -- bash -c 'echo $CARGO_TARGET_DIR' | tail -1)
M=bench/highlight/Cargo.toml
for e in $engines; do
  # engine name -> cargo feature: irosashi (embedded assets) needs irosashi-full, the rest match.
  feat=$e; [ "$e" = irosashi ] && feat=irosashi-full; [ "$e" = irosashi-min ] && feat=irosashi
  ./x/agent-build.ts -- cargo build --release --manifest-path $M --features "$feat"
  cp "$T/release/hlbench" "$bin/native-$e"
  # arborium's tree-sitter C objects cannot link into wasm32-wasip3 here (see findings), so skip it.
  if [ "$e" != arborium ]; then
    ./x/agent-build.ts -- cargo build --release --manifest-path $M --features "$feat" --target wasm32-wasip3 --bin hlbench
    cp "$T/wasm32-wasip3/release/hlbench.wasm" "$bin/wasip3-$e.wasm"
  fi
  ./x/agent-build.ts -- cargo build --release --manifest-path $M --features "$feat" --target wasm32-unknown-unknown --lib
  rm -rf "$bin/web-$e"; mkdir -p "$bin/web-$e"
  wasm-bindgen --target nodejs --out-dir "$bin/web-$e" "$T/wasm32-unknown-unknown/release/highlight_bench.wasm"
done
