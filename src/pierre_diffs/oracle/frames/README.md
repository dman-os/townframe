# Frame differential: our CodeView against upstream's

Measures, per animation frame while a scripted wheel scroll runs, whether the rows rendered in
the page (the document and every shadow root: `[data-line]`, separators) cover the viewport's
edges, how far past them the window reaches (the overscan actually rendered), when DOM edits land
relative to the frame, and main-thread stalls (long tasks, frame intervals).

```sh
# ours: the browser build of the showcase, in one terminal
deno run --allow-all x/dayframe.ts showcase-web --port 8765
# upstream: bundle upstream/entry.js (esbuild, with the oracle's pinned @pierre/diffs closure
# from ensure.mjs as node_modules) to upstream/bundle.js, put /tmp/pr84.diff beside it, and
node serve.mjs upstream 8790
# then, with PLAYWRIGHT_CORE / PLAYWRIGHT_BROWSERS_PATH as for the interpreter harness:
THROTTLE=4 VH=1100 node measure.mjs chromium "http://127.0.0.1:8790/index.html" upstream root
node measure.mjs firefox "http://127.0.0.1:8765/?tab=diffs&diffs_view=pr84" ours window
```

`THROTTLE` is Chromium's CPU throttle rate, `VH` the viewport height. The last argument is the
scroller: `root` (upstream's own scroller) or `window` (ours, the document).
