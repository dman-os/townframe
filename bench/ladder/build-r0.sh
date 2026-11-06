#!/usr/bin/env bash
# Bundles r0/entry.js (+ the vendored @pierre/diffs 1.6.4 tarball install in $X1/r0-build, installed
# with --ignore-scripts) into r0/dist. esbuild only reads the package; none of its scripts run.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
X1=${X1:-/run/media/asdf/p3N/tmp/x1}
rm -rf "$here/r0/dist" && mkdir -p "$here/r0/dist"
cp "$here/r0/entry.js" "$X1/r0-build/entry.js"
(cd "$X1/r0-build" && ./node_modules/.bin/esbuild entry.js --bundle --splitting --format=esm \
  --target=chrome120 --minify --outdir="$here/r0/dist" --entry-names=app --chunk-names=chunk-[hash] \
  --loader:.wasm=file --log-level=warning)
cp "$here/r0/index.html" "$here/r0/dist/index.html"
ls -la "$here/r0/dist" | head -20
