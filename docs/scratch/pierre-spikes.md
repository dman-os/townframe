
## P2 scroll

Branch `spike/pierre-scroll`. Micro `src/micro_pierre_spike_scroll` (1M synthetic lines, window model after Pierre), spec `src/dayframe_web/e2e/pierre_scroll.spec.ts` (prints `PIERRE_SPIKE <name> <json>`), budget test `micro_pierre_spike_scroll::budgets` (replays the guest's Dioxus diff through `SurfaceTree::apply`). Chromium headless, `--workers=1`, web build `dayframe_web`, host `wasm-release`. Release guests unless stated (`DAYFRAME_GUEST_PROFILE=release`); the shell packages **debug** guests by default. Raised-limits runs: `DAYFRAME_RAISED_LIMITS=1` + `PIERRE_RAISED=1`. Stats are median / p95 over n runs (n given).

### Pierre's numbers (source)
Row 20 px `constants.ts:62`; chunk (`hunkLineCount`) 50 `constants.ts:60` (CodeView 1, `:67-70`); overscroll 1000 `Virtualizer.ts:25` (CodeView 200, `CodeView.ts:763`); window rows `ceil(ceil((viewport+2*overscroll)/20)/chunk)*chunk+chunk` `VirtualizedFile.ts:1178-1183` (600 px viewport: 200 rows); start quantized to chunk, centred `:1209-1226`. Rebase `CodeView.ts:649-690`: container 12M, trigger 1M, targetTop 2M, targetBottom 10M, threshold 11M; resolve `:2983-3097`. Chromium caps element height at 33,554,428 px (measured).

### 1. Baseline on the unchanged wire
- Scroll offsets reach the guest end to end: yes (test "scroll offsets reach the guest..."): DOM `scrollTop = 10,000,000` -> `onscroll` payload -> window start 499,989 (viewport = clientHeight 598), row `v500000` rendered, window `offsetTop` = start*20. Earlier research marked this unverified; it works.
- **Pierre's own window does not fit.** Native budget test, rows of 5 nodes (1 span/line): 23 mutations/row, 200 rows = 4,628 mutations > 4,096 cap. First-window max rows commit before a limit: 98 (1 span), 72 (3), 48 (7), 21 (18), 10 (38). Binding limit at the max is `MAX_BATCH_WORK` (128K, quadratic: each mutation charges stack depth, which grows with sibling count), not mutations. Mutations/row = 8 per extra span + 7 (3 spans 39, 7 spans 71, 18 spans 159, 38 spans 319); 200 rows at 7 spans (P3's highlighted load) = 14,228 mutations by this Dioxus diff (P3's by-arithmetic 5,600 is 2.5x low). A 50-row chunk move at 7 spans (3,511 mutations) also fails, on `MAX_BATCH_BYTES` (256 KiB). Retained nodes/row: 5, 9, 17, 39, 79.
- Overrun is not a clean withdrawal in the browser: jumping the 100-row Pierre-profile window to a 200-row one aborts the guest in `dispatch` (its `expect("guest presentation commit failed")`) and the shell panics at `clay_rt_wcmp/lib.rs:208` ("suspended on its first poll") then `:655` (RefCell already borrowed). Pinned by the spec test "KNOWN BREAK...". Cause chain inferred from the panic text and `pageerror: unreachable`, not stepped through.
- Default profile used for measurements: code-view (overscroll 200, chunk 1): 41-51 rows, ~290 nodes.
- Browser clamp: 3M lines (60M px) -> `scrollHeight` 33,554,428, last reachable line 1,677,732 (1M lines = 20M px fits Chromium; Firefox's cap is lower, `CodeView.ts:686`).
- Per wheel step, code-view window, 1 span/line, n=200, 1 step/frame:
  - release guest, 100 px/step (132 mutations, 5 new rows): guest dispatch (guest diff + wcmp bridge + import apply) 6.1 / 7.9 ms; `SurfaceTree::render` 0.1 ms; tree apply 0.1 ms; scroll event -> committed DOM 7.1 / 8.9 ms; 0 frames >25 ms.
  - release guest, 1000 px/step (1,257 mutations, whole window replaced): dispatch 16 / 18.8 ms; scroll -> DOM 17.5 / 20.3 ms; 0 frames >25 ms.
  - **debug guest**, 100 px/step: dispatch 205 / 265 ms (n=200); scroll -> DOM 206 / 266 ms; 208 of 615 frames >25 ms, 200 long tasks. 1000 px/step: scroll -> DOM 719 / 1258 ms. Debug-guest figures are not representative; what shells package today is debug.
- Host time split: `SurfaceTree::render` and tree apply are measured directly; VDOM diff and DOM apply are not separable without instrumenting dioxus-web, so "host tail" (end of guest dispatch -> first DOM mutation batch) covers render + diff + apply.

### 2a. Element commands (commit 1db44442)
`presentation.wit` `surface.element-command(element, scroll | scroll-by | set-focus | read(id, client-rect|scroll-offset|scroll-size))`, fire-and-forget; reads are answered by an `element-reply` event. Host keeps each mounted element's `MountedData` (`ElementHandles`, filled by the `mounted` listener), a `Send` queue drains on the renderer thread; the guest adapter replaces the serialized event converter so `onmounted` yields a `MountedData` whose backing sends the commands (all `RenderedElementBacking` methods but `scroll_to`/scrollIntoView). Touches both engine adapters (wcmp, wasmtime), `clay_guest_session`, the `clay_mdk` test host.
- Jump to line 500,000 and back with `MountedData::scroll`: DOM `scrollTop` = 10,000,000 then 0; window follows; `get_client_rect` / `get_scroll_size` / `get_scroll_offset` equal the DOM's values (spec). Click -> committed window round trip, n=20: 31 / 35 ms (no coalescer, release guest); 16.5 / 28.6 ms with the coalescer.
- Findings: (i) a `mounted` event used to be dropped whenever any batch landed first (whole-tree revision check), so the guest never saw `onmounted`; fixed by 2c's generation check. (ii) Stale element handles: host entries are not removed when the guest removes an element; an id reused by a non-mounted element can address a detached node. (iii) A failed fire-and-forget command comes back as an `element-reply` with request 0 and makes the guest panic, which lands on the unclean abort path above. (iv) Reads take two crossings and a later dispatch; the Dioxus API is async so this fits, but code needing a value synchronously cannot have one.
- Decision for the user: this widens the guest-controlled surface ADR 015 narrowed (guest can scroll/focus any element it saw `mounted`). Options: ship as is; scope it per element by manifest/capability; or a host-owned scroller primitive (guest sends row ranges) with no element commands.

### 2b. Scroll coalescing (a3081758)
Host holds the latest `scroll` per element and delivers once per animation frame. Chromium already delivers at most one native scroll per frame, so under a real wheel there is nothing to merge: wheel 100 px/step, n=200, guest dispatch 6.1 vs 6.0 ms median, no difference. Synthetic burst (8 scrollTop writes + scroll events per frame, 100 frames, 900 events): off = 201 delivered, **699 dropped as stale** (host drops events carrying an older revision than the guest's newest; the host re-renders asynchronously after a commit), 2.01 dispatches/frame; on = 100 delivered, 0 dropped, 1/frame; per-dispatch cost unchanged (6.3 vs 6.6 ms). Value: determinism (last position wins, guest entered at a frame boundary) and non-browser hosts; not speed. Needs a frame source per host (browser only here; others deliver immediately).

### 2c. Incremental host projection (a75bae63)
Nodes carry the revision of the last batch that touched them or a descendant (`touch` walks parents, stops at an already-marked ancestor); `SurfaceRoot` projects a memoized component per element that has element grandchildren (leaf spans inline). Listeners now carry the node's generation and the host drops an event only if its node is gone (was: any newer revision; the `mounted` and 699-dropped findings above). Both paths coexist behind `window.__clayNoIncremental` for A/B (a real change would delete the whole-tree path).
Raised budgets (`spike-raised-limits`: mutations 128K, bytes 16 MiB, slots 32768, element id 32767, retained bytes 8 MiB, work 32M), Pierre window (200 rows), 50-row chunk per frame (1000 px), release guest, n=60, median / p95 ms:

| spans/line | retained nodes | mutations/commit | guest dispatch | `SurfaceTree::render` whole / incr | host tail whole | host tail incr | scroll -> DOM whole / incr |
|---|---|---|---|---|---|---|---|
| 3 | 1,848 | 2,057 | 37 / 43 | 0.7 / 0 | 3.8 / 5.7 | 2.3 / 3.6 | 41 / 41 |
| 7 | 3,448 | 3,657 | 59 / 62 | 1.3 / 0 | 6.9 / 8.5 | 3.3 / 5.1 | 66 / 62 |
| 18 | 7,848 | 8,057 | 97 / 99 | 2.7 / 0 | 12.3 / 15.5 | 5.5 / 6.9 | 109 / 99 |
| 38 | 15,848 | 16,057 | 170 / 180 | 6 / 0 | 27.6 / 31.9 | 10.7 / 13 | 195 / 178 |

Host work is not the bottleneck: the whole-tree projection costs 4 -> 28 ms per revision across 1.8k -> 15.8k nodes and incremental cuts the host tail ~2.5x (render itself to ~0), but the **guest dispatch (guest diff + wcmp bridge) is 10x larger and unchanged**, so frames stay over budget at any realistic row load (60-62 of ~250 frames >25 ms even at 3 spans/line, a window shift every frame). Retained-node cost scales linearly (about 0.4 ms / 1,000 nodes whole-tree, host tail); incremental still grows (about 0.7 ms / 1,000 nodes) because rebuilt rows are fully re-projected.
Cost of raising the limits: nothing visible in these runs beyond the above (mutation counts 2k-48k per commit were accepted; first mount of the 38-span window was 48,258 mutations). What it does not fix: the work budget is quadratic in sibling count, so raising it 256x is what admits 200 siblings; a hostile guest can then force 32M units of host work per batch. Options: raise with a linear charge (fix the quadratic stack-depth term); multi-batch commits with an atomicity rule; coarser token runs; or the host scroller primitive.

### P1/P3 data points checked
- Bridge cost: a 132-mutation release-guest commit is 6.1 ms (about 0.05 ms/mutation incl. diff); the 16,057-mutation commit is 166 ms (0.010 ms/mutation, release). The debug guest was 205 ms for 132 mutations (1.55 ms/mutation), matching P1's ~1.2 ms/mutation. So P1's cost is debug-build overhead, not the per-mutation host call design: release is ~100x cheaper. I cannot split guest compute from bridge crossing (no guest-side timer); the number is guest diff + bridge + `SurfaceTree::apply` together (apply alone: 0.7-4.9 ms at 2k-16k mutations). Batching the crossing is not shown to be needed; unmeasured.
- Caps at 217 rows reproduced: batch mutations, work (128K, quadratic in siblings), bytes (256 KiB).

### 2d. Paged-scroll rebase (f4a73682)
Model-driven as in Pierre: logical scrollTop = page offset + DOM scrollTop; content box capped at 12M px; rebase past 11M / under 1M moves the offset and the guest commands the DOM back (to 2M or 10M) with `MountedData::scroll`. Depends on 2a (without commands the DOM could not be repositioned). Tests: 3M-line file fully reachable (`jump-last` shows line 3,000,000, DOM scrollTop in 2M..12M); dragging to 11.5M rebases the DOM to 2,000,000 with logical position kept (`v575000` rendered), further +2,000 px moves 100 lines, scrolling up past the trigger rebases back (> 9M); unpaged control reproduces the browser clamp. Pure resolve function unit-tested. Caveat: between the scroll event and the DOM reposition one more event can arrive with the new offset and old DOM top, so the window can show a wrong region for about a frame; not measured.

### Verification
`cargo nextest run -p clay_surface -p clay_dioxus -p clay_mdk -p clay_rt_wcmp` (guest fixtures prepared): 139 passed. Clippy clean on clay_surface, clay_dioxus, clay_mdk, clay_rt_wcmp, clay_rt_wasmtime, clay_guest_session, micro_pierre_spike_scroll (all targets). Playwright `pierre_scroll`, normal build: 12 passed; scale tests with `DAYFRAME_RAISED_LIMITS=1 PIERRE_RAISED=1`: 4 passed. Not run: `clay_rt_wasmtime`, `clay_rt_wcmp_wasmi` test suites, guardrails, Firefox/Blitz.

## P4 dispatch

Question: where do the 37-170 ms of a release-guest dispatch (50-row chunk, 3-38 spans/line) go, and do the three cheapest levers help. Same setup as P2 (raised budgets, release guest, Pierre profile, spec `PIERRE_RAISED=1`, tests named `p4-*` in `pierre_scroll.spec.ts`). Medians / p95, n=60 committing dispatches unless stated.

### Method (cheapest honest)
- **Guest-side timer**: the guest links `wasi:clocks`, so `std::time::Instant` works in it (the host serves `monotonic-clock` from `performance.now`, 0.1 ms resolution; one call costs 4 us, measured). `clay_mdk` feature `spike-phases` (`guest/dom.rs`, `phases`) brackets each `dispatch` into: event parse + handler, `VirtualDom::render_immediate` (diff), batch take, `surface.apply` call. The micro hands the records to the page through a hidden span. The host side already records `SurfaceTree::apply` and the whole dispatch.
- **Controls**: the same diff run in the guest with a writer that drops every edit (no wire), natively in release, and raw wasm micro-benchmarks (button `bench`).
- **Chromium CPU profile** (100 us sampling) of the fast scroll, self time by function; guest wasm functions are named in it.
- Derived: bridge + host decode = guest `apply` call minus `SurfaceTree::apply`; crossing overhead = host dispatch minus the guest's total.

### Self time per phase (50-row chunk, Pierre window, release guest)
| spans/line | mutations | parse + handler | VDOM diff (render_immediate, recording writer) | batch take | `surface.apply` call | of which `SurfaceTree::apply` | bridge + decode (derived) | host dispatch total |
|---|---|---|---|---|---|---|---|---|
| 7 | 3,657 | 0.4 / 0.7 | **45.9 / 49.3** (94%) | 0 | 1.4 / 1.7 | 0.5 / 0.7 | ~0.9 | 48.9 / 52.9 |
| 18 | 8,057 | 0.3 / 0.5 | **85.4 / 99.1** (96%) | 0 | 2.8 / 3.3 | 1.1 / 1.3 | ~1.7 | 90.6 / 105.1 |
- Wire encoding (`take`), the wcmp crossing with host decode, and `SurfaceTree::apply` together are 3-4% of the dispatch. Entry/exit crossing of the dispatch itself is about 1 ms. The wire is not the bottleneck; the guest's VirtualDom diff is.
- Inside the diff phase, the app's `Row` renders account for 11.2 ms of 44 at 7 spans (measured with a per-row timer, which itself adds 100 host calls); the rest is Dioxus diff and mutation recording.

### The diff is slow only inside the wcmp web runtime
- The same diff of a 50-row shift, natively in release: 0.36 ms (7 spans) / 0.69 ms (18), with a null writer, 0.40 / 0.76 with the recording writer (recording is about 10%). In the guest with a null writer (no wire at all): 41 / 77 ms (the guest-internal bench, medians of 20). So the guest runs the identical work **about 110x slower than native**, and wire encoding is not why.
- Raw wasm in that guest is not slow: 20M integer-loop iterations 2.7-5 ms, 4M-word memory fill+sum 3-10 ms, 1M `#[inline(never)]` leaf calls 6.7 ms. What is slow: calls that set up a stack frame (1M framed calls 0.8-1.8 s, 0.8-1.8 us each), and the allocator (alloc+free of a `Box<u64>` 3.9 us, a 100k-`String` loop 1.2-2.4 s, about 12-24 us per string). A host import (`Instant::now`) costs 4 us, so these are not explained by one host call each.
- CPU profile of 60 fast frames (6.0 s sampled, 0.76 s idle): guest wasm functions have about 75-330 ms of self time in total; the large self times are in the wcmp runtime: `WebStore::call_host`, `Calls::direct`/`invoke_in`, `wrapper::results`/`carry`/`from_js`, host dlmalloc/free, `wasm-to-js` and wasm-bindgen closures (about 2.9 s together, roughly equal to 60 x the guest dispatch). Some profile entries are Playwright's injected script (`visitNode`, `visitChild`) and were excluded from that sum. Mechanism not established: JSPI-run guest stack, per-call shim for calls that cross the guest's core modules, or the stack-pointer global; I did not read `wcmp-wasm-core-web` far enough to say. A decisive control not done: instantiate the same core module directly in V8 (outside wcmp) and run the same bench.
- Consequence: guest code that allocates or makes frame-carrying calls costs about two orders of magnitude more in this runtime than natively, so the mutation count (allocations, calls) drives dispatch time: about 12.5 us per mutation at 7 spans, 10.6 us at 18, near-linear. This is why the P1 figure (1.2 ms per mutation on a debug guest) and the release figure (about 0.01 ms) differ by 100x and why the debug guest was 30x slower than release.

### The three levers (fast = 1000 px/frame, one 50-row chunk per frame; slow = 200 px/frame)
Dispatch is the committing guest dispatch as the host sees it; frames counted from rAF intervals (>17.5 ms = missed a 60 Hz frame; >25 ms). 60 commits per run unless noted.

| lever | setup | commits | dispatch median / p95 | frames >17.5 / >25 ms (of) | mutations per commit |
|---|---|---|---|---|---|
| baseline | 7 spans, chunk 50, keyed, fast | 60 | 49.7 / 55.4 | 61 / 61 (234) | 3,657 |
| baseline | 18 spans, chunk 50, keyed, fast | 60 | 89.9 / 98.2 | 62 / 62 (259) | 8,057 |
| 1 merge | P3 corpus raw (rust + ts alternating lines; 6.6 / 5.9 spans per line), fast | 60 | 50.1 / 58.5 | 64 / 64 (227) | 4,354 |
| 1 merge | same, whitespace + same-color merged | 60 | 45.5 / 54.2 | 64 / 64 (213) | 3,664 |
| 2 chunk | 7 spans, chunk 50, slow | 30 | 49.0 / 54.6 | 30 / 30 (358) | 3,657 |
| 2 chunk | 7 spans, chunk 25, slow | 60 | 29.5 / 33.0 | 57 / 57 (372) | 1,832 |
| 2 chunk | 7 spans, chunk 10, slow | 150 | 16.3 / 18.6 | 1 / 1 (454) | 737 |
| 2 chunk | 18 spans, chunk 50, slow | 30 | 87.5 / 95.7 | 30 / 30 (605) | 8,057 |
| 2 chunk | 18 spans, chunk 25, slow | 60 | 51.4 / 56.5 | 60 / 60 (539) | 4,032 |
| 2 chunk | 18 spans, chunk 10, slow | 150 | 25.0 / 27.4 | 24 / 24 (588) | 1,617 |
| 3 keys | 7 spans, keyed, fast | 60 | 49.7 / 55.4 | 61 / 61 (234) | 3,657 |
| 3 keys | 7 spans, unkeyed, fast | 60 | 95.7 / 106.5 | 63 / 63 (245) | 4,804 |
| 3 keys | 18 spans, keyed, fast | 60 | 89.9 / 98.2 | 62 / 62 (259) | 8,057 |
| 3 keys | 18 spans, unkeyed, fast | 60 | 181.5 / 197.5 | 61 / 61 (299) | 11,404 |

1. **Merging tokens.** On P3's corpus (first 1,000 lines each of `rust-1k.rs` and `ts-1k.ts`, tokenized by Pierre's own `@pierre/highlights`, pierre-dark; generator `src/micro_pierre_spike_scroll/corpus/gen.mjs`): raw 6.62 (rust) / 5.91 (ts) spans per line; merging adjacent same-color tokens alone removes only 6.6% / 9.9% (6.18 / 5.32); also merging whitespace-only tokens into the next token (Shiki's `mergeWhitespaces`) removes 15.6% / 21.3% (5.58 / 4.65). In the browser that is 16% fewer mutations and 9% lower dispatch (50.1 -> 45.5 ms), no change in missed frames. A small win, not a fix.
2. **Smaller chunks.** Costs scale with rows added per commit, so a chunk of 10 at 200 px/frame (10 rows per frame) cuts the dispatch to 16.3 ms at 7 spans (the frame budget; 1 frame over 25 ms of 454) and 25.0 ms at 18 spans (24 frames over 25 ms of 588). Chunks of 25 still miss every commit. Total work per scrolled row is unchanged, so a fast scroll (50 rows per frame) gets nothing from it; the smaller window (140 rows instead of 200) also holds 20-30% fewer nodes. Best of the three at normal reading speeds, but it only moves the cliff: at 18 spans, scrolling faster than about 10 rows per frame misses frames again.
3. **Keyed rows.** The spike micro already keys rows by line number (`Row { key: "{line}" }`), and the diff already touches only the edges: a 50-row shift is 3,657 mutations at 7 spans (73 per new row) versus 4,628 to mount 200 rows; the budget test shows the same (1,257 mutations for 50 new rows at 1 span). Unkeyed (positional) is 2x worse (95.7 vs 49.7 ms, 181 vs 90 ms), so keys are necessary and already paid for; no further gain.

### What this says for options (not decided)
- The bottleneck is guest compute in the wcmp web runtime (about 110x native), driven by allocations and frame-carrying calls per mutation. Levers on the app side only trade rows per commit (chunk 10 fits the frame at 7 spans) or shave spans (10-20%).
- Next experiments with the largest expected return: (a) find why calls with stack frames and the allocator are slow under wcmp (control outside wcmp; compare with the wasmi engine); (b) fewer mutations by construction, e.g. one text node per row instead of a span per token, or the host scroller primitive with host-side row rendering; (c) a guest build with a custom bump/arena allocator (the diff is allocation-heavy), which would test whether the allocator alone explains most of it.
- Not measured: the unreleased `render_immediate` split between Dioxus diff and mutation recording inside the guest (native says recording is 10%), and Firefox.
