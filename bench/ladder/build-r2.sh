#!/usr/bin/env bash
# R2: release build of the plain dioxus-web app + wasm-bindgen into r2/site. Run from the worktree
# root through plain cargo (mbx).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/r2"
cargo build --release --target wasm32-unknown-unknown --config 'resolver.feature-unification="selected"'
target=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
rm -rf site && mkdir -p site
wasm-bindgen --target web --no-typescript --out-dir site --out-name ladder_r2 "$target/wasm32-unknown-unknown/release/ladder_r2.wasm"
cp index.html site/index.html
ls -la site
