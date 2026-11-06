
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

## P5 wasm speed

Question: why does the guest run about 110x slower than native under the wcmp web runtime (P4)? **Answer, measured: the guest's stack pointer is not a wasm global. Every frame-carrying function calls two imported functions, `env.__wasm_get_stack_pointer` / `__wasm_set_stack_pointer`, which the component backs with `canon context.get/set i32 0`, and wcmp implements each as a full host-function call.** Swapping just those two functions for a wasm global brings the in-wcmp guest from ~60-95 ms to ~2-3 ms per diff, with everything else (wcmp, JSPI, allocator, shims) unchanged.

### Mechanism (file:line)
- The guest core module imports four `env` functions (`wasm-tools print`, `m0.wat`:68-71 in the scratchpad dump): `__wasm_{set,get}_tls_base` and `__wasm_{get,set}_stack_pointer`. `framed` in the bench compiles to `call $__wasm_get_stack_pointer; i32.const 272; i32.sub; local.tee; call $__wasm_set_stack_pointer; ... ` and restores with another set. Static call sites in the release module: 1,413 get + 2,528 set (`grep -cF 'call $__wasm_get_stack_pointer'` over `wasm-tools print` of the unbundled module). Leaf functions and loops carry no frame, so they are fast (P4).
- wit-component wires those imports in the component as `(core func (canon context.get i32 0))` / `(canon context.set i32 0)` (`wasm-tools print` of the component, `$env` core instance, near the end of the text).
- wcmp lowers them to ordinary host functions: `rust/wcmp/src/executor/instantiate.rs:759-764` (`TrampolineSpec::ContextGet/ContextSet` -> `build_context_get/set`), `executor/intrinsics.rs:81-139`: each call goes through `runtime_layer::host_func` (`runtime_layer.rs:270-289`, a `Func::new` closure that takes the store `Caller`, bumps `host_frames`), locks the `HandleTables` mutex, finds the current thread and reads/writes `record.context[slot]`. On the web backend that wasm -> JS -> wasm-bindgen -> Rust -> JS round trip is the `wasm-to-js`, `WebStore::call_host`, `Calls::direct/invoke_in`, `wrapper::params/results/from_js`, dlmalloc self time of the P4 CPU profile (the `GenericShunt<Map<Iter<ValType>, wrapper::params>>` entry is the per-call param `Vec` build).
- Our side: why the guest is built this way (the `wasm32-wasip3` target links with `wasm-component-ld`; `rustc --print target-spec-json` shows `linker: wasm-component-ld`, `--stack-first`, stack 1 MiB). Which toolchain component decides "stack pointer lives in context slot 0" instead of a `__stack_pointer` global I did not trace: unverified. `src/clay_mdk/lib.rs:75-120` (S6 task-stack hook) reads `__init_stack_pointer`, not the context slot; it is not the cause (see ruled out).

### Measurements
All guest-side numbers are the guest's own timers (`bench`, P4's button `bench-7` / `bench-18` -> `diff-no-writer-median-ms`, medians of 20 diffs inside each run; framed = 1M `#[inline(never)]` calls with a 256 B frame). Host load average was 14-24 during these runs (other agents), so compare within an interleaved set, not across sets.

**M1. Control outside wcmp** (Node 24 / V8, same machine; core module of the same release component, built with `--features p5-direct`, which adds the exports `p5_init`, `p5_bench`, `p5_diff`, `p5_len`; imports stubbed by `src/micro_pierre_spike_scroll/p5/direct.mjs`):
```
cargo build (via ./x/agent-build.ts --) --locked --config 'resolver.feature-unification="selected"' -p micro_pierre_spike_scroll --release --target wasm32-wasip3 --features p5-direct
wasm-tools component unbundle <component> --module-dir <dir> -o <dir>/out.wasm
node src/micro_pierre_spike_scroll/p5/direct.mjs <dir>/unbundled-module0.wasm js 3 7     # and 18
node src/micro_pierre_spike_scroll/p5/patch-sp.mjs m1.wat m1-global.wat; wasm-tools parse m1-global.wat -o m1-global.wasm
node src/micro_pierre_spike_scroll/p5/direct.mjs m1-global.wasm global 5 7              # and 18
```
| config | diff 7 spans (ms) | diff 18 spans (ms) | 1M framed calls (ms) | 100k alloc+free (ms) | 100k strings (ms) |
|---|---|---|---|---|---|
| native release (P4, not rerun) | 0.36 | 0.69 | | | |
| V8 direct, SP = JS closure imports | 2.07, 2.40, 2.64 | 4.84, 5.27 | 52-71 | 13-17 | 45-75 |
| V8 direct, SP = wasm global | 0.58-1.23 (5 runs) | 2.31, 2.40 | 24-36 | 1.6-3.5 | 10-19 |
| wcmp web, Chromium (below, V0) | 62-95 | 105-166 | 1,340-2,060 | 420-870 | 2,700-3,600 |
So outside wcmp the same bytes run 2-4x native with a JS-closure SP and 1.7-3.5x with a global; wcmp is 20-40x slower than even the JS-closure case.
SP imports per `p5_diff` call (rebuild of 140 rows plus one shift diff, counted with wrapped imports): 7 spans 192,953 gets + 268,526 sets; 18 spans 382,189 + 531,466. (The count covers the untimed rebuild too, so it is an upper bound for the timed diff.)

**M2. Toggle inside wcmp** (the decisive one). Same host site, same Chromium, same Playwright bench test; only the guest bytes change (`src/micro_pierre_spike_scroll/p5/serve.ts` serves the built `dayframe_web` site with the pierre component swapped and its BLAKE3 digest recomputed):
```
cd src/dayframe_web && PIERRE_RAISED=1 ../../x/agent-build.ts -- pnpm exec playwright test pierre_scroll.spec.ts -g "guest-internal bench" --workers=1 --reporter=line
```
- V0 = the p5-direct component unmodified. V0r = V0 printed and reparsed with wasm-tools (rules out the print/parse round trip). V1 = V0r with the two `canon context.get/set 0` core funcs replaced by two exported funcs of a tiny extra core module over a mutable global initialised to 1048576 (`__init_stack_pointer`); nothing else touched. Cross-module wasm-to-wasm calls (guest -> that module) cost nothing visible, so wcmp's linking of core modules is not slow.
- Pin a5db353 (the P4 pin), 3 runs each, interleaved (diff-no-writer-median-ms, tokens 7 / 18; framed-calls-1M-ms, tokens 7):

| variant | diff 7 spans | diff 18 spans | framed 1M | alloc+free 100k | 100k strings |
|---|---|---|---|---|---|
| V0 | 92.5, 83.8, 61.8 (+95.4 first run) | 105.5, 162.3, 143.1 (+166.3) | 1620, 1612, 1368 | 752, 727, 500 | 3,629 (run 1) |
| V0r | 94.3, 72.5, 80.5 | 211.8, 172.0, 137.7 | 2062, 2208, 1585 | 866, 602, 763 | |
| V1 (SP global) | 2.70, 2.00, 3.80 (+2.10) | 3.80, 3.00, 4.40 (+3.30) | 58, 47, 61 (+55) | 11, 10, 12 | 39 (run 1) |
- Per context op: framed does 1 get + 2 sets; (V0 1,368-1,620 ms - V1 ~58 ms) / 1M / 3 = 0.44-0.52 us per `context.get/set` (derived, not directly timed). `Instant::now` (one host import) measured 6-10 us here under load.
- V1 vs V0: diff 7 spans 24-40x lower, 18 spans 35-50x lower, framed calls 25-30x lower, alloc+free 60-70x lower. V1 is 5.5-10x the native 0.36 ms for 7 spans and 4.3-6.4x native 0.69 for 18 (wasm + load).

**M3. JSPI bisection with the newer pin** (coordinator request). Re-pinned on this branch in its own commit (`bbaec72d`: `Cargo.toml`, `Cargo.lock`, `src/wasm_utils_rs/Cargo.{toml,lock}` a5db353 -> f2c7714, which runs sync-typed exports as a plain core call instead of through `WebAssembly.promising`); rebuilt the host (`x/dayframe-web.ts --build`, `DAYFRAME_GUEST_PROFILE=release DAYFRAME_RAISED_LIMITS=1`), same bench, 3 interleaved runs:
| variant (pin f2c7714) | diff 7 spans | diff 18 spans | framed 1M |
|---|---|---|---|
| V0 | 55.1, 76.2, 66.8 | 116.7, 134.3, 64.9 | 1209, 1380, 898 |
| V1 | 2.40, 3.30, 1.60 | 4.10, 4.20, 2.20 | 57, 69, 36 |
The plain-core-call path is possibly a little faster at the median (V0 66.8 vs 83.8 on 7 spans) but the 3-run spreads overlap under a load average of 14-24, so I do not claim an improvement. It does not remove the slowness: the context slot access remains the cost. JSPI-run stacks are not what makes the guest slow.

### Ruled out (each by measurement above)
- Direct-V8 run of the same module is fast (M1): not the guest build profile, not debug assertions (it is the release module), not the allocator itself (alloc+free 100k: 1.6-3.5 ms with a global SP, 7-12 ms in wcmp V1).
- Cross-core-module calls (V1 adds a module hop for every SP access and is fast).
- JSPI (M3), print/parse round trip (V0r), the S6 task-stack hook (V1 runs the same hooks and is fast; the hook uses `global.get __init_stack_pointer`, not the context slot).
- The allocator is slow only because every `malloc`/`free` has a frame.

### Fix options (not applied; wcmp is read-only here)
1. **wcmp: make `context.get/set` for core wasm free.** Cheapest for us. Options inside wcmp: (a) lower `canon context.get/set N` to an in-module wasm global pair (one global per slot, saved/restored at task switch by the runtime), so the hot path is a V8 inlined global access; this is what V1 emulates; (b) failing that, a fast path in `build_context_get/set` that avoids `host_func`/`Caller`/mutex/param `Vec` (but a JS boundary crossing per access, ~0.1-0.3 us in V8 even with the best host function, M1 JS-closure row: 20-40x slower than global but 25x faster than today). Cost: wcmp runtime change on the web backend; the slot must be saved and restored at every task switch and suspension (the V1 hack is only safe because only one stack is in flight during the bench; with interleaved tasks on distinct stacks it would corrupt), so this needs per-task save/restore at the points wcmp already switches thread. Expected gain: diff ~2-4 ms instead of ~60-170 ms (M2 V1; the dispatch's 37-170 ms from P4 should fall about 25-50x, unmeasured end to end).
2. **Guest build: stop using a context slot for the stack pointer.** If the toolchain can emit a plain `__stack_pointer` global for components that do not use shared-everything threads (a linker flag or target option on `wasm32-wasip3`, e.g. whatever makes wasm-ld emit the `__wasm_{get,set}_stack_pointer` imports in `wasm-component-ld`), the guest is fast under any runtime including wcmp's other backends, with no wcmp change. Cost: unverified: I did not find the flag; needs a toolchain/wasm-component-ld source dive, and a plain global breaks any design where each task has its own stack (S6 `task_stacks.rs` swaps a state pointer by stack address, so it needs a check). Also a tradeoff to check with the user for ADR 015 semantics: it is a guest-build change, not a guest-controlled surface widening.
3. **Post-link rewrite of the guest component** (what V1 did): replace the two `context.*` funcs with a stack-pointer global in an extra core module, done by a build step (`wasm-tools print | edit | parse`, ~30 s on this 1.8 MB component; or a wasm-tools/`wit-component` API). Cost: build-pipeline hack; correctness limited to the one-active-stack case unless tasks are serialized per instance; the print/parse also drops about 90 KB of custom sections (size 1,785,395 -> 1,695,656 B, effect unknown).
4. **App-side levers (P4)** stay available but only trade rows per commit; none removes a 25-50x factor.
5. Not measured: Firefox (JSPI) and the wasmi backend; both would still lower `context.get/set` through wcmp's host_func, so they should show the same ratio (unverified).

### Commands run (all from the worktree root unless noted)
`wasm-tools component unbundle` / `print` / `parse` / `validate`; builds through `./x/agent-build.ts` (`cargo build ... -p micro_pierre_spike_scroll --release --target wasm32-wasip3 --features p5-direct`; `cargo build --offline ... -p dayframe_web --bin dayframe_web --target wasm32-unknown-unknown --profile wasm-release`; `deno run --allow-all x/dayframe-web.ts --build`); the node and Playwright commands above; servers were started by me with `deno run --allow-all src/micro_pierre_spike_scroll/p5/serve.ts <site> <component>` and killed by PID after each run. Scratch artefacts (components, wat dumps, logs) are in the scratchpad `p5/` dir, not committed; `p5/runvar.sh` there ran each variant.

## W1 context slots

Fork branch `context-slots-as-globals` (dman-os/wasm-component-model-polyfill), rev `45e7dffb82a99e7b91bf6c27dcd7ad44547c2de7`, off f2c7714. This worktree re-pins to it in all four manifests/locks (`cargo metadata --locked` passes in the root and in `src/wasm_utils_rs`).

### Mechanism
- A store-wide core module (`executor/context_slots.rs`) imports two host-made mutable `i32` globals and exports `get0/set0/get1/set1` (`global.get`/`global.set`). `TrampolineSpec::ContextGet/Set` link those instead of host functions, so a guest's `context.get/set` stays in wasm.
- The globals hold the slots of one "live" thread (kept in `HandleTables.context_slots`, next to `TaskTables`); every other thread's slots stay in `Thread.context`. `context_slots::sync` saves the globals into the live thread's record and loads the current thread's record when `TaskTables::current_thread()` differs from the live one.
- Rather than hooking the ~55 scope mutation sites, sync runs where guest code gets control: after every `host_func` body returns, and before wcmp calls into the guest (plain entry call, start fn of a prepared call, thread start `run_thread`, destructor tasks (both paths), the `task.return` caller function, `BoundaryCall::enter` for realloc/post-return, core instance `start`, stack-switching `start`/`resume`, host-suspension `start`/`start_at_rest`/`fly`). Host readers go through `slots_of`.
- JSPI: a flight (`fly`) runs with the resumed thread's scopes on the stack, so syncing at its start is right; K5 plain call path (`start_thread_entry`, `reports_traps_late`) syncs before the plain `entry.call`.
- Behavior change: `context.get/set` with no task on the stack used to trap; it now reads whatever the globals hold (unverified that any guest relies on the trap).

### Fork tests (commands run)
`WCMP_TEST_BACKEND=wasmi cargo nextest run -p wcmp --lib`: 839 passed. Default backend `cargo nextest run -p wcmp -p wcmp-wasm-core --lib`: 847 passed. New `baseline::context_slots` (two threads interleave with suspend/resume-later and suspend-then-resume; each sees its own slots): passes on wasmi; with the host-return sync commented out the interleave test fails (mutation check). Clippy clean: `-p wcmp -p wcmp-wasm-core --lib --profile test` (native) and `-p wcmp -p wcmp-wasm-core-web --target wasm32-unknown-unknown`. Not run (need Nix): browser lanes, root integration lanes, `tests/zena.rs`.

### Measurement (Chromium headless, release guests, `DAYFRAME_RAISED_LIMITS=1 PIERRE_RAISED=1`, unmodified guest bytes: the two sites' `packages/index.json` are identical)
Two sites built from this worktree, differing only in the pin (before f2c7714, after 45e7dff), served statically, specs run interleaved before/after. Host load average ~38 throughout, so read ratios, not absolutes.

`guest-internal bench` (diff-no-writer-median-ms, 3 interleaved runs):
| | 7 spans | 18 spans | framed 1M calls |
|---|---|---|---|
| before | 137.9, 139.3, 135.1 | 280.6, 345.9, 325.3 | 3645, 2969, 3688 |
| after | 2.40, 6.80, 8.10 | 3.90, 9.10, 8.80 | 46, 87, 83 |

P4 dispatch specs (`phases tokens7/18 fast`, n=60 committing dispatches each, guestDispatchMs median / p95), 2 interleaved runs:
| | tokens7 | tokens18 |
|---|---|---|
| before r1 / r2 | 177.6 / 485.3 ; 192.2 / 333.0 | 307.2 / 483.3 ; 366.6 / 550.6 |
| after r1 / r2 | 10.6 / 28.9 ; 10.8 / 27.6 | 21.2 / 36.9 ; 8.9 / 12.1 |
Frames over 50 ms: 77/76 before vs 9/4 after (tokens7). Long tasks 60 vs 5/6.

### Review change: trap with no task
The context module now also imports a `no_task` global; its `get`/`set` do `global.get $no_task; if unreachable` before touching the slot (one global read and a branch, no host call). `sync` sets it when no task is on the stack and clears it when one is. Fork rev `228bc7b359fd6ec65bcdcf2a644f85684e2c5e0b` (this worktree is re-pinned to it; the measurements above were taken at 45e7dff, before this check, not re-measured). New fork test `executor::context_slots::tests::it_traps_a_slot_access_while_no_task_is_on_the_stack`; `cargo nextest run -p wcmp --lib` wasmi 840 passed, default backend with wcmp-wasm-core 848 passed; clippy clean native and wasm32.

### Townframe validation at 228bc7b
Fixtures prepared (`x/check-dayframe.ts --prepare-only`). `cargo nextest run -p clay_rt_wcmp -p clay_rt_wcmp_wasmi -p clay_guest_session -p clay_host`: 148 passed. dayframe_web Playwright suite (`CI=1 DAYFRAME_WEB_PORT=4562`): NOT run, killed at the start when free disk fell under the floor (3 GB, then 2.7 GB).
Not run: wasmi-backend measurement.

## X1 ladder

Question: for the Pierre window workload, how much of the 8.3 ms (120 Hz) frame does each layer cost, from the browser alone up to the Clay guest stack. One workload for every rung, one rung per page, all driven by Playwright (`--workers=1`) in headless Chromium 156.0.8078.4 (Playwright 1.64.1, software compositing, no GPU) on a Ryzen 9 7940HS (16 threads). Harness in `bench/ladder/` (this branch, committed, not pushed); raw results and logs in `/run/media/asdf/p3N/tmp/x1/` (`results.jsonl`: one JSON line per rung x workload x round, 166 lines; `tables-clean.md` / `tables-all.md`: every generated table; `profile-groups.txt`; `doc-tables.md`).

### Workload and rungs
- Rows 20 px, scroller 600 px (clientHeight 598), window `ceil(ceil((598 + 2*1000)/20)/50)*50 + 50` = 200 rows, start quantized to 50-line chunks and centred on the viewport (`window()` of the micro, ported to `bench/ladder/common.js` and `r2/src/main.rs`); rows keyed by line number, window positioned absolutely inside a content box of the whole file height; 8,000-line file. One **commit** = `scrollTop += 1000`, i.e. the window moves one chunk: 50 rows removed, 50 rows added (850 nodes at 7 spans, 1,950 at 18).
- Row DOM is the micro's: `div(height:20px...) > span(gutter) + N token spans`. Workloads: `syn7` / `syn18` (the micro's synthetic source 0: 7 / 18 spans per line, no colors), `corpus` (P4's raw corpus tokens, `corpus/tokens.json`: rust and ts lines alternating, source 1, colored spans), `corpus-rust` (rust tokens only, for R0).
- **R0** real `@pierre/diffs` 1.6.4 (`Virtualizer` + `VirtualizedFile`, pierre-dark, main-thread Shiki, no worker pool, `disableFileHeader`), rendering `rust-1k.rs` repeated to 8,000 lines (the file `corpus/tokens.json`'s rust half was tokenized from). `npm pack` into `/run/media/asdf/p3N/tmp/x1/pkg`, installed with `--ignore-scripts` into `r0-build/`, bundled with esbuild into `r0/dist` (`build-r0.sh`); no package script was run. Only `corpus-rust`: the micro has no rust-only mode, so R0 is compared with R1/R2 `corpus-rust`, not with R3.
- **R1** hand-written vanilla JS, two variants: `dom` (`createElement` / `createTextNode` into a `DocumentFragment`, `appendChild`) and `html` (one HTML string per added chunk, `insertAdjacentHTML`). Scroll listener computes the window and applies a keyed shift (remove leading rows, build and attach trailing rows). **R1s** (control): all 4,400 rows the run scrolls through are pre-rendered and nothing reacts to scroll.
- **R2** plain Dioxus web: `bench/ladder/r2` (own workspace, `Cargo.lock` seeded from the root one; dioxus fork rev `a3e1e68`, `dioxus::launch`, `onscroll` handler, release, `lto = "fat"`, wasm-bindgen 0.2.127, 728,559 byte wasm). `Row` / `Window` are the micro's components copied verbatim.
- **R3** the existing Pierre spike micro (release guest, 1,783,225 bytes, `spike-phases`) in `dayframe_web` (`wasm-release` host with `DAYFRAME_RAISED_LIMITS=1`, host wasm 13,089,200 bytes), profile `pierre`, scroll coalescing off (`__clayNoCoalesce`, as P4's baselines). Site assembled by `bench/ladder/assemble-r3.ts` (a copy of `x/dayframe-web.ts` without the guest/manifest step: the native manifest emitters had already run and the host disk was short).
- R2b (the Rust diff as a plain core wasm module driven from JS) is **not built**; P5's direct-V8 figures (0.6-1.2 / 2.3-2.4 ms for the diff alone, 7 / 18 spans) are the nearest data point and were not re-run.

### Method (what each number is)
- Each rung x workload x round runs five fresh pages: (A) wall clock of 70 commits after 10 warm-ups, plus a MutationObserver count and DOM census of one more commit; (B) the same 70 commits under a Chromium trace (`browser.startTracing`, categories `devtools.timeline`, `disabled-by-default-devtools.timeline`, `blink.user_timing`, `toplevel`, `v8`), sliced per commit by `performance.mark`; (C) continuous scroll under a trace, 100 frames at 1000 px/frame ("fast": one 50-row chunk per frame) and (D) 200 px/frame ("slow": a commit every fifth frame); (E) a V8 CPU profile (100 us sampling) over 70 commits. A commit window is: scrollTop write, wait until the rendered window moves, then two more frames.
- **Trace classes** are self times on the renderer main thread: *script* = FunctionCall/EvaluateScript/v8.*/timers/event dispatch (includes wasm and the DOM API calls made from it), *style* = UpdateLayoutTree, *layout* = Layout, *paint* = PrePaint + Paint + Layerize + Commit + UpdateLayer(Tree), *other* = the rest of the task (RunTask self time, HitTest, ParseHTML, GC events inside tasks). *busy* = sum of `RunTask` durations in the window (three frames, so one frame of real work plus idle ones). Raster happens on other threads and is not counted.
- **Frames over budget**: headless Chromium here cannot run uncapped (`--disable-frame-rate-limit --disable-gpu-vsync` left rAF at a median 17.4 ms against 16.7 ms without), so rAF intervals only show 60 Hz vsync misses. The 8.3 / 16.7 ms counts are therefore of frames whose *main-thread busy time* (every `RunTask` between one rAF callback and the next, from the trace) exceeds the budget, i.e. frames that would not fit a 120 / 60 Hz deadline if the compositor were free. `performance.now()` is clamped to 0.1 ms in this build, so the per-phase timers of R1 and the guest are 0.1 ms quantized.
- **Statistics**: per round the median / p95 over 70 commits; cells are the median over rounds of the per-round medians and `[lo-hi]` is the spread of the round medians. Rounds interleave the rungs (round 1-6: r0, r1-dom, r1-html, r2, r3; round 7-11: r1-static, r1-dom, r2, r3). Tables below use only runs whose 1-minute load average was <= 10 at both ends (159 of 166 runs; the 7 above 10 were all in rounds 1-2 and are listed as NOISY in `tables-all.md`); host load was 1-3 for most rounds (1-minute load at the start of a run: median 2.6, minimum 1.1, maximum 30.9). Even at low load single rounds have outliers (r1-dom syn7 round 7: busy 20.6 ms at load 3.2; r1-static syn7 round 8: 29.0), which is why medians over many rounds are reported with spreads.

### Ladder summary (per commit, ms; delta vs the rung named in the last column)
| workload | rung | script | delta script | style+layout+paint | busy total | delta busy | frames >8.3 ms (fast scroll, of 100) | frames >8.3 ms (slow scroll, of 100) | delta vs |
|---|---|---|---|---|---|---|---|---|---|
| syn7 | r1-static | 0.33 | - | 5.25 | 7.34 | - | 10 | 1 | - |
| syn7 | r1-dom | 1.48 | +1.14 | 6.48 | 8.95 | +1.61 | 8 | 1 | r1-static |
| syn7 | r2 | 2.39 | +0.91 | 6.41 | 9.89 | +0.94 | 15 | 5 | r1-dom |
| syn7 | r3 | 7.61 | +5.22 | 6.21 | 14.78 | +4.88 | 99 | 20 | r2 |
| syn7 | r1-html | 1.08 | -0.39 | 5.99 | 8.43 | -0.52 | 6 | 2 | r1-dom |
| syn18 | r1-static | 0.33 | - | 5.71 | 7.74 | - | 12 | 4 | - |
| syn18 | r1-dom | 1.88 | +1.54 | 9.02 | 12.23 | +4.49 | 82 | 20 | r1-static |
| syn18 | r2 | 3.19 | +1.32 | 8.82 | 13.46 | +1.23 | 99 | 20 | r1-dom |
| syn18 | r3 | 12.15 | +8.96 | 9.71 | 23.23 | +9.77 | 99 | 21 | r2 |
| syn18 | r1-html | 1.59 | -0.29 | 9.47 | 12.93 | +0.70 | 76 | 20 | r1-dom |
| corpus | r1-static | 0.32 | - | 7.84 | 10.87 | - | 75 | 20 | - |
| corpus | r1-dom | 1.72 | +1.40 | 4.76 | 7.44 | -3.43 | 9 | 1 | r1-static |
| corpus | r2 | 2.80 | +1.08 | 5.10 | 8.96 | +1.52 | 19 | 6 | r1-dom |
| corpus | r3 | 8.47 | +5.67 | 5.07 | 14.54 | +5.58 | 99 | 20 | r2 |
| corpus | r1-html | 1.04 | -0.68 | 4.77 | 7.09 | -0.35 | 4 | 1 | r1-dom |
| corpus-rust | r1-dom | 1.71 | - | 4.92 | 7.55 | - | 6 | 1 | r1-static |
| corpus-rust | r2 | 2.75 | +1.04 | 5.02 | 8.96 | +1.41 | 24 | 8 | r1-dom |
| corpus-rust | r1-html | 1.04 | -0.68 | 5.03 | 7.59 | +0.03 | 4 | 0 | r1-dom |
| corpus-rust | r0 | 2.54 | +0.82 | 6.80 | 11.54 | +3.98 | 53 | 16 | r1-dom |

### Per workload
**`syn7`: per commit, ms (trace; the window is the commit's three frames)**

| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |
|---|---|---|---|---|---|---|---|---|---|
| R1s static control | 5 | 0.3 | 0.0 | 0.0 | 5.2 | 1.7 | 7.3 [7.1-29.0] | 9.7 | 18.2 |
| R1 vanilla createElement | 9 | 1.5 | 0.5 | 1.4 | 4.6 | 0.8 | 8.9 [7.2-20.6] | 12.0 | 17.8 |
| R1 vanilla innerHTML | 6 | 1.1 | 0.5 | 1.2 | 4.3 | 1.2 | 8.4 [7.5-11.4] | 11.0 | 17.6 |
| R2 dioxus-web | 11 | 2.4 | 0.5 | 1.4 | 4.5 | 0.8 | 9.9 [9.0-11.7] | 13.2 | 18.8 |
| R3 Clay stack | 11 | 7.6 | 0.6 | 1.1 | 4.6 | 0.8 | 14.8 [13.4-19.1] | 21.1 | 23.9 |

**`syn7`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame, a commit every 5th frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1s static control | 10 [1-28] / 0 [0-3] | 5.9 / 9.9 | 1 [0-6] / 0 [0-0] | 6.6 |
| R1 vanilla createElement | 8 [4-90] / 1 [0-62] | 6.4 / 9.4 | 1 [0-20] / 0 [0-17] | 7.3 |
| R1 vanilla innerHTML | 6 [4-63] / 1 [1-15] | 6.0 / 9.0 | 2 [0-13] / 0 [0-2] | 7.1 |
| R2 dioxus-web | 15 [9-96] / 1 [1-23] | 6.9 / 10.8 | 5 [2-16] / 0 [0-3] | 8.6 |
| R3 Clay stack | 99 [99-99] / 8 [4-66] | 13.3 / 17.9 | 20 [20-21] / 2 [0-15] | 15.4 |

**`syn18`: per commit, ms (trace; the window is the commit's three frames)**

| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |
|---|---|---|---|---|---|---|---|---|---|
| R1s static control | 5 | 0.3 | 0.0 | 0.0 | 5.7 | 1.6 | 7.7 [7.4-8.4] | 12.0 | 18.2 |
| R1 vanilla createElement | 10 | 1.9 | 0.8 | 2.0 | 6.2 | 0.8 | 12.2 [10.9-12.9] | 18.2 | 18.4 |
| R1 vanilla innerHTML | 6 | 1.6 | 0.8 | 2.1 | 6.6 | 1.5 | 12.9 [11.2-19.8] | 18.7 | 18.1 |
| R2 dioxus-web | 11 | 3.2 | 0.8 | 1.9 | 6.1 | 0.8 | 13.5 [12.7-19.9] | 19.4 | 19.8 |
| R3 Clay stack | 11 | 12.2 | 1.1 | 2.0 | 6.7 | 0.8 | 23.2 [21.5-26.6] | 32.4 | 28.8 |

**`syn18`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame, a commit every 5th frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1s static control | 12 [5-27] / 0 [0-0] | 6.4 / 9.3 | 4 [1-5] / 0 [0-2] | 7.9 |
| R1 vanilla createElement | 82 [39-99] / 4 [2-6] | 9.2 / 16.5 | 20 [13-20] / 0 [0-1] | 11.7 |
| R1 vanilla innerHTML | 76 [37-99] / 4 [2-41] | 9.1 / 16.8 | 20 [19-22] / 0 [0-8] | 11.7 |
| R2 dioxus-web | 99 [80-99] / 7 [3-57] | 11.3 / 17.0 | 20 [20-22] / 1 [0-14] | 13.6 |
| R3 Clay stack | 99 [99-99] / 99 [98-99] | 21.7 / 29.7 | 21 [20-30] / 20 [20-22] | 23.5 |

**`corpus`: per commit, ms (trace; the window is the commit's three frames)**

| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |
|---|---|---|---|---|---|---|---|---|---|
| R1s static control | 5 | 0.3 | 0.0 | 0.0 | 7.8 | 2.7 | 10.9 [10.1-11.6] | 15.7 | 19.8 |
| R1 vanilla createElement | 10 | 1.7 | 0.5 | 0.9 | 3.4 | 0.8 | 7.4 [7.0-18.7] | 10.3 | 18.4 |
| R1 vanilla innerHTML | 6 | 1.0 | 0.5 | 1.0 | 3.4 | 1.3 | 7.1 [6.8-9.6] | 9.2 | 17.8 |
| R2 dioxus-web | 11 | 2.8 | 0.5 | 1.0 | 3.6 | 0.9 | 9.0 [8.1-26.0] | 12.3 | 19.3 |
| R3 Clay stack | 10 | 8.5 | 0.6 | 0.8 | 3.6 | 0.9 | 14.5 [13.5-28.2] | 20.7 | 24.8 |

**`corpus`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame, a commit every 5th frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1s static control | 75 [61-86] / 1 [0-37] | 9.0 / 14.1 | 20 [8-21] / 0 [0-1] | 10.9 |
| R1 vanilla createElement | 9 [3-61] / 1 [0-13] | 5.8 / 9.2 | 1 [0-6] / 0 [0-1] | 6.6 |
| R1 vanilla innerHTML | 4 [2-36] / 2 [0-4] | 5.3 / 7.8 | 1 [0-17] / 0 [0-9] | 6.6 |
| R2 dioxus-web | 19 [9-99] / 2 [1-97] | 7.1 / 11.4 | 6 [3-20] / 0 [0-20] | 8.6 |
| R3 Clay stack | 99 [99-99] / 10 [6-99] | 13.7 / 18.8 | 20 [20-21] / 2 [0-20] | 15.9 |

**`corpus-rust`: per commit, ms (trace; the window is the commit's three frames)**

| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |
|---|---|---|---|---|---|---|---|---|---|
| R1 vanilla createElement | 11 | 1.7 | 0.5 | 0.9 | 3.5 | 0.9 | 7.6 [6.8-9.3] | 10.1 | 18.3 |
| R1 vanilla innerHTML | 6 | 1.0 | 0.5 | 1.0 | 3.5 | 1.3 | 7.6 [6.9-8.1] | 9.8 | 17.8 |
| R2 dioxus-web | 11 | 2.8 | 0.5 | 1.0 | 3.5 | 0.9 | 9.0 [8.0-14.0] | 12.0 | 19.3 |
| R0 real Pierre | 4 | 2.5 | 1.1 | 1.3 | 4.4 | 1.6 | 11.5 [9.8-12.3] | 17.1 | 33.7 |

**`corpus-rust`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame, a commit every 5th frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1 vanilla createElement | 6 [3-75] / 1 [0-14] | 5.5 / 8.8 | 1 [0-11] / 0 [0-0] | 6.8 |
| R1 vanilla innerHTML | 4 [3-58] / 1 [1-11] | 5.5 / 8.1 | 0 [0-7] / 0 [0-1] | 6.2 |
| R2 dioxus-web | 24 [10-91] / 1 [0-25] | 7.1 / 11.6 | 8 [1-14] / 0 [0-2] | 9.1 |
| R0 real Pierre | 53 [41-99] / 2 [2-4] | 8.4 / 13.6 | 16 [13-19] / 0 [0-1] | 10.6 |

### R3: where the Clay stack's script time goes (ms per commit, median over rounds)
| | syn7 | syn18 | corpus |
|---|---|---|---|
| guest event parse + handler | 0.1 | 0.1 | 0.1 |
| guest VDOM diff (`render_immediate`, incl. `Row` renders, recording writer) | 2.4 | 3.7 | 2.5 |
| guest batch take | 0.0 | 0.0 | 0.0 |
| guest `surface.apply` call incl. wcmp crossing + host decode | 1.3 | 2.8 | 1.7 |
| of which `SurfaceTree::apply` (host) | 0.5 | 1.1 | 0.7 |
| => bridge + decode (derived: apply call - `SurfaceTree::apply`) | 0.8 | 1.7 | 1.0 |
| host `dispatch:scroll` total (from the host's own timer) | 4.0 | 6.8 | 4.65 |
| trace *script* total of the commit window (B) | 7.6 | 12.2 | 8.5 |
| => host-side remainder (derived: trace script - dispatch): host Dioxus diff of the surface tree, DOM apply, dioxus-web plumbing, harness | 3.6 | 5.4 | 3.8 |

The host's `render` timer records median 0 in all three (not informative). Guest numbers are the guest's own `Instant` timers (`phases-dump`, committing dispatches only, 0.1 ms clock). For reference R2's whole script (diff + DOM, no wire) is 2.4 / 3.2 / 2.8.

V8 CPU profile self time, grouped by function name (profile pass E, ms per commit with the profiler's own inflation, so read shares; `profile-groups.txt`, 11 rounds, browser-native `program` time excluded):
| group (ms per commit, profiled) | R2 syn7 | R3 syn7 | R2 syn18 | R3 syn18 |
|---|---|---|---|---|
| guest wasm (app, dioxus-core diff, guest allocator) | - | 2.69 | - | 4.63 |
| wcmp runtime (canonical ABI, host-call plumbing) | - | 0.94 | - | 2.01 |
| Clay host (SurfaceTree render/apply, clay_dioxus) | - | 0.91 | - | 1.74 |
| host Dioxus (dioxus-core / dioxus-web in the host wasm) | 0.87 | 1.41 | 1.33 | 2.56 |
| host allocator (dlmalloc) | 0.22 | 0.51 | 0.33 | 0.99 |
| DOM natives (remove, setAttribute, cloneNode, insertBefore, ...) | 0.83 | 1.14 | 1.38 | 1.75 |
| js glue | 0.39 | 0.50 | 0.63 | 0.78 |
| other wasm | 0.24 | 0.23 | 0.35 | 0.45 |
| gc | 0.13 | 0.20 | 0.31 | 0.33 |

R1 own timers (JS in the scroll handler, ms, 0.1 ms clock): `dom` syn7 total 1.1 (build 0.6, attach 0.4), syn18 1.6 (0.7 / 0.9), corpus 1.5 (0.95 / 0.4); `html` syn7 0.8 (build 0.15, attach incl. parse 0.6), syn18 1.35 (0.2 / 1.1), corpus 0.85 (0.15 / 0.7). R0: `file.render` is called once per commit and takes 1.2 ms (0.1 ms clock); window DOM 3,109 nodes / 1,738 elements against 2,965 / 1,583 for R1 `corpus-rust`; MutationObserver sees 100 nodes added and 100 removed per commit (two columns, code and gutter) against 50 / 50; its window moves one frame later than R1's (wall input -> window 33.7 ms vs 18.3 ms: the scroll event queues the render for the next animation frame).

### What dominates at each rung (observations from the tables above)
- **Browser floor (R1).** With plain JS the commit's frame already spends 6.4 ms (syn7) to 9.0 ms (syn18) on style + layout + paint, plus 1.1-1.9 ms of script, so script is 15-17% of the main-thread time and the browser alone is 6.4 ms of an 8.3 ms budget at 7 spans and over the whole budget at 18 spans. This is in headless Chromium with software compositing; whether a GPU/phone browser pays the same main-thread paint cost was not measured (**unverified**). Style + layout + paint scale with the nodes added (850 vs 1,950 nodes: 6.4 -> 9.0 ms), script barely does (1.5 -> 1.9). Fast scroll (50 rows per frame) puts 8 of 100 frames over 8.3 ms at 7 spans and 82 of 100 at 18 spans even for vanilla JS; with a commit every fifth frame (slow) vanilla JS is over 8.3 ms in 1 frame of 100 at 7 spans and 20 of 100 (every commit frame) at 18. `innerHTML` vs `createElement`: script 1.1 vs 1.5 ms (syn7), 1.6 vs 1.9 (syn18): parsing a string is cheaper than creating 850-1,950 nodes from script, but the totals are within the round spread.
- **R1 -> R2 (Dioxus-web).** +0.9 / +1.3 / +1.1 ms script (syn7 / syn18 / corpus), about 1.6x R1-dom's script, style/layout/paint unchanged. In the CPU profile the host-side Dioxus diff and the DOM natives are each about a third of R2's script, js glue (the sledgehammer interpreter) 15%, the allocator 8%. Fast-scroll frames over 8.3 ms: 15 of 100 (syn7), 99 (syn18), 19 (corpus). Dioxus-web fits the budget in most frames at 7 spans; at 18 spans nothing fits.
- **R2 -> R3 (Clay stack).** +5.2 / +9.0 / +5.7 ms script (7.6 / 12.2 / 8.5 ms in total), i.e. 3.2x / 3.8x / 3.0x R2's script and the only rung whose script alone exceeds 8.3 ms at 18 spans. The commit-window busy time is 14.8 / 23.2 / 14.5 ms against R2's 9.9 / 13.5 / 9.0. Fast-scroll frames over 8.3 ms: 99 of 100 in all three workloads, and over 16.7 ms: 8 (syn7), 99 (syn18), 10 (corpus). Slow scroll: every commit frame (20 of 100) is over 8.3 ms. The split above: the guest diff (2.4-3.7 ms) is now about what R2 spends on diff + DOM in total (2.4-3.2 ms), bridge + decode adds 0.8-1.7 ms, `SurfaceTree::apply` 0.5-1.1 ms and the host-side remainder (host Dioxus diff of the surface tree + DOM + plumbing) 3.6-5.4 ms, the largest single piece. In the profile the guest wasm is about 30% of the grouped self time, host Dioxus 16-17%, wcmp 11-13%, Clay host 11%, DOM natives 12-14%, allocators 6-7%.
- **R0 (real Pierre, `corpus-rust`).** 11.5 ms per commit window against 7.6 (R1-dom) and 9.0 (R2) for the same tokens. Pierre's script is 2.5 ms (R1-dom 1.7, R2 2.8) plus 1.6 ms other (that includes `ParseHTML` of its HTML chunks, 0.65 ms in a 30-commit trial trace) but its DOM costs 6.8 ms of style/layout/paint against 4.9-5.0 for the vanilla DOM of the same text (more nodes per line, grid/gutter structure, shadow DOM). Fast scroll: 53 of 100 frames over 8.3 ms (R1-dom 6, R2 24), slow: 16 (R1-dom 1, R2 8). Its window update lands a frame later than every other rung's.
- **Compared with earlier spikes.** The guest diff (`vdomDiff`) measured here in the browser inside wcmp, release, after the context-slot fix, is 2.4 / 3.7 ms for 7 / 18 spans at host load 1-3 (P4/W1 recorded 2-8 / 4-9 ms at load ~38). The host `dispatch:scroll` is 4.0 / 6.8 ms. The new information is that this is about half of the Clay-vs-vanilla gap: the rest is on the host side of the wire and in the browser, not in the guest.

### Observations (not decisions)
- At 18 spans per line (about 2,000 nodes added per 50-row chunk) plain JS misses 8.3 ms on style + layout + paint alone in this setup, so a 120 Hz target at that density needs fewer nodes or fewer rows added per frame regardless of runtime; P4's chunk-10 lever acts on exactly this term.
- At 7 spans vanilla JS and plain Dioxus-web both fit most fast-scroll frames (8 and 15 of 100 over 8.3 ms); the Clay stack misses 99 of 100. A target of "Dioxus-web parity" would be a script cost of about 2.4 ms per 50-row chunk; the Clay stack is at 7.6.
- The Clay stack pays for a second VDOM-shaped pass on the host (the surface tree re-render, 3.6-5.4 ms, inferred from function names in the profile) on top of the guest diff and the bridge; with the guest diff already near R2's total cost, host-side work and the wire (0.8-1.7 ms decode + 0.5-1.1 ms `SurfaceTree::apply`) are now the larger share than the guest.
- Every rung that reacts to the scroll event lands its window one frame after the scroll (17.6-19.8 ms from input to window with a 16.7 ms vsync, 23.9-28.8 ms for R3, 33.7 ms for Pierre); the browser's own scrolling is compositor-driven and not delayed.

### Caveats and what is unverified
- Software-composited headless Chromium, no GPU; main-thread paint is recording, raster is off-thread and not counted. 120 Hz was never actually run: budgets are applied to measured main-thread busy time at a 60 Hz vsync. Phone numbers are not extrapolated.
- R1s is not a clean floor: it holds 4,400 pre-rendered rows (74,801 nodes at syn7) and scrolls through them, so its tree is 22x larger than the 200-row window of the others; its paint is lower than R1-dom for syn7 and higher for `corpus` (7.8 vs 3.4 ms), which I could not explain. Treat it as a different measurement, not as a floor.
- R0 has 4 clean rounds (the other two had load > 10), R1-html 6, R1-static 5, r1-dom / r2 / r3 9-11.
- The `corpus` workload on R3 uses the micro's alternating rust/ts lines; R0 uses rust only, so R0 is compared with `corpus-rust` rows only.
- Trace "other" and "busy" include harness work (`Ladder.commits` polling, about 0.1-0.9 ms per commit in the CPU profile `js` category); the profiler's `program` category is not main-thread-only idle time, and was not used for any claim.
- The 100 us CPU profile inflates wasm time (R3 syn7: ~8.0 ms of wasm in the profile against 7.6 ms total script in the unprofiled trace), so group shares are indicative.
- The host-side "remainder" of R3 is derived by subtraction, not timed directly.

### Commands run (from the worktree root unless noted)
- Pierre: `npm pack @pierre/diffs@1.6.4` in `/run/media/asdf/p3N/tmp/x1/pkg`; `npm install --ignore-scripts ../pkg/pierre-diffs-1.6.4.tgz esbuild@0.25.5` in `r0-build`; `bench/ladder/build-r0.sh`.
- R2: `./x/agent-build.ts -- bench/ladder/build-r2.sh` (cold build 4 m 44 s).
- R3: `CARGO_RESOLVER_FEATURE_UNIFICATION=selected DAYFRAME_GUEST_PROFILE=release DAYFRAME_RAISED_LIMITS=1 ./x/agent-build.ts -- deno run --allow-all x/dayframe-web.ts --build` (guests and manifests; killed twice, once by a disk guard at < 6 GB free, once by hand; the native `debug` target of the manifest emitters, 4.0 GB and later 1.1 GB, was deleted between runs), then `DAYFRAME_RAISED_LIMITS=1 ./x/agent-build.ts -- deno run --allow-all bench/ladder/assemble-r3.ts --packages=<target>/spike-scroll/dayframe/packages`.
- Servers: `deno run --allow-all bench/ladder/serve.ts 4701 /r0/=... /r1/=... /r2/=... /corpus/=...` (PID 3697061) and `... serve.ts 4702 /=<target>/spike-scroll/dayframe_web/site` (PID 265624); both killed by PID at the end.
- Measurements: `bench/ladder/run-matrix.sh 4 r0 r1-dom r1-html r2 r3`, then `START=5 run-matrix.sh 2 ...`, then `START=7 run-matrix.sh 5 r1-static r1-dom r2 r3`; each is `npx playwright test -c bench/ladder/playwright.config.ts --workers=1` per rung. A first pass of four rounds over r0 / r1 / r2 only (kept as `results-a-busybug.jsonl`) was discarded: its `busy` column double-counted nested `RunTask` events.
- Tables: `AGG_CLEAN=1 python3 bench/ladder/aggregate.py results.jsonl`, `python3 bench/ladder/doc_tables.py results.jsonl`, `python3 bench/ladder/profile_groups.py results.jsonl`.


## X4 receivers

Question: how much of the Clay stack's per-commit cost (X1: R3 = 7.6 / 12.2 ms script per 50-row window shift at 7 / 18 spans against R2's 2.4 / 3.2) is the host re-projecting the guest's batches into a retained tree and re-diffing that tree in the host `VirtualDom`, and what does a receiver that applies the batches straight to the real DOM (web) or to the Blitz document (native) cost instead. Everything here is a prototype on branch `spike/pierre-scroll` (committed, not pushed); observations only. Raw results, logs and captured batches are in `/run/media/asdf/p3N/tmp/x4/` (`results.jsonl`: one line per rung x workload x round, 192 lines; `tables-clean.md`; `batches-<workload>.json`; `native-*.txt`).

### What was built
- **Web seam**: `clay_dioxus::SurfaceWriter::apply` (the `SurfaceSink` the engine adapters hand a guest's decoded `Vec<Mutation<ComponentRef>>` to) forwards to a *direct receiver* for surfaces whose owner name contains `pierre_spike_scroll` when `window.__clayDirect` is `"js"` or `"rust"`. The receiver is created in `MountedSurface` before the guest can commit; the host renders only an empty container (`<div id="clay-direct-N" style="display:contents">`) and a `use_effect` moves the receiver's detached root into it. No `SurfaceTree` is built for the surface, `SurfaceRoot`/`SurfaceNodeView` never render, and the host `VirtualDom` never re-renders for the guest's batches (`revision` never changes). Files: `src/clay_dioxus/direct.rs` (Rust side, both variants, registry, element commands), `src/clay_dioxus/direct.js` (the JS receiver and shared helpers), wiring in `src/clay_dioxus/lib.rs`.
- **Both variants run the same Dioxus mutation stack machine as `SurfaceTree`'s `WriteMutations` impl (`tree/write.rs`) on live DOM nodes**: `push-id`/`set-id`/`child`/`pop`, `create-element` (HTML or `createElementNS`), `create-text`, `clone-node` (`cloneNode(true)`), `append-children`/`replace-with`/`insert-after`/`insert-before` (m popped nodes, a fragment when m > 1), `set-attribute` (`style` namespace to `style.setProperty`, other namespaces `setAttributeNS`, `value`/`checked`/`selected` as properties, `Boolean(true)` as an empty attribute, `None`/`false` removes), `set-text`, `add/remove-event-listener`, `remove`.
- **(a) JS receiver (`r3a`)**: Rust flattens the decoded batch into one `Vec<u32>` of ops plus one string blob (offsets in UTF-16 units, buffers reused across batches) and calls `Receiver.apply(ops, strings)` once; wasm-bindgen passes the op array as a view of wasm memory and decodes the blob once. All DOM calls are made from JS.
- **(b) Rust receiver (`r3b`)**: wasm walks the decoded batch and calls web-sys per op (typed `Node` stack, `unchecked_ref` casts so there is no `instanceof` per op, `Closure` per listener).
- **Events and commands (both)**: a listener is a DOM listener on the element itself that builds the payload and calls the surface's `SurfaceEventHandler` (the same `dispatch` the tree path uses, so the guest, `mounted` bookkeeping and element-command gating are unchanged); the handler's `prevent-default` / `stop-propagation` are applied to the DOM event. `mounted` is queued and delivered once the container is attached. `element-command` (`scroll`, `scroll-by`, `set-focus`, `read` with an `element-reply` event) acts on the element directly. Payloads: `scroll` is built from the element (`scroll_top` ... `client_height`), `click`/`mousedown`/`mouseup`/`dblclick` carry `SerializedMouseData::default()`; any other event name throws.
- **Timers** (feature `spike-timing`, `window.__clayTiming`): `apply` (the sink call; same name as the tree path), `direct-encode`, `direct-js`, `direct-rust`, and `host-render` (below).
- **Native**: `bench/blitz-x4` (own workspace, `Cargo.lock` seeded from the root's, same dioxus / blitz fork revs). `dioxus_native_dom::DioxusState::writer(&mut doc)` is already a public `WriteMutations` writer over a document subtree "for a WebAssembly component guest sending its own mutation stream" (its doc comment, and `clay_surface`'s crate docs name it as the direct-Blitz path), so the native receiver is `clay_surface::replay(batch, &mut state.writer(&mut doc))`: no new receiver code was written for Blitz, only a harness around it.

### Step 1: the host remainder, measured
X1 derived "host-side remainder" by subtraction (3.6 / 5.4 / 3.8 ms for syn7 / syn18 / corpus). The `host-render` timer measures it directly: it starts when `MountedSurface`'s body runs (the revision signal woke it) and ends in a `use_effect` that depends on `revision`, which runs after the `VirtualDom` has diffed every dirty scope below it. It covers the surface projection (`SurfaceRoot` / `SurfaceNodeView` bodies), the host Dioxus diff and the mutation writes made before effects fire.

| | syn7 | syn18 | corpus |
|---|---|---|---|
| `host-render`, measured (R3) | 2.65 | 4.30 | 3.10 |
| X1's derived remainder (trace script - `dispatch:scroll`) | 3.6 | 5.4 | 3.8 |
| `SurfaceTree::apply` (host sink `apply` timer, R3) | 0.50 | 1.00 | 0.70 |
| scroll `EventDispatch`, top-level script event in the trace (R3) | 6.89 | 11.41 | 9.53 |
| script in every other top-level task of the commit window (R3) | 0.32 | 0.30 | 0.34 |

The browser trace shows the host render does not run in a later task: the scroll `EventDispatch` (guest + host render, inclusive) is 6.9 / 11.4 / 9.5 ms and every other top-level script event of the window is 0.3 ms (the harness's `FireAnimationFrame`). dioxus-web renders synchronously inside the event handler that set the signal. The measured figure is 0.7-1.1 ms below the derived one; the difference is what a "remainder" absorbed (harness, JS glue between the pieces), plus whatever happens after the effect fires. **Unverified**: whether the interpreter's final DOM flush happens before or after the effect callbacks; if after, `host-render` under-counts by that flush (cross-check against the trace: R3's `EventDispatch` less R3a's is 2.4 / 4.1 / 4.6 ms; R3's host sink + host render (3.2 / 5.3 / 3.8) less R3a's receiver (0.7 / 1.3 / 1.0) is 2.5 / 4.0 / 2.8, which agrees for syn7 and syn18 and leaves 1.8 ms of R3 corpus's dispatch unexplained; the guest timers, `host-render` and the parse step sum to within 0.5-0.7 ms of R3's `EventDispatch` for syn7 and syn18 and 2.1 ms short for corpus).

### Web results (Chromium 156, headless, software compositing, same harness and method as X1)
Interleaved rounds (round 1-6: r1-dom, r2, r3, r3a, r3b, plus r1-dom/r2 `corpus-rust` which are not tabulated; rounds 7-12 the same five rungs, each rung waiting up to 15 minutes for the 1-minute load average to drop to 6 before starting: `MAX_LOAD=6`), 70 commits after 10 warm-ups per page, medians over rounds of per-round medians, runs whose load average was above 10 at either end excluded (138 of 192 kept; kept runs: median 4.2, range 0.7-9.2). Rounds per cell are in the tables. The host binary is rebuilt (`wasm-release`, raised limits, `spike-timing`): R3 here includes the `host-render` timer and its extra effect, so its numbers differ slightly from X1's R3 (X1 R3 syn7: 7.6 script, 14.8 busy; here 7.2 / 14.1; syn18 12.2 / 23.2 here 11.7 / 21.9; corpus 8.5 / 14.5 here 9.8 / 17.1). Scroll coalescing is off (`__clayNoCoalesce`) for every R3 rung, as in X1. DOM census after the window is identical on all five rungs (3,401 / 7,801 / 3,025 nodes); one commit adds and removes 50 rows on every rung.

**Ladder summary (per commit, ms; delta vs the rung in the last column)**

| workload | rung | rounds | script | delta script | style+layout+paint | busy total | delta busy | frames >8.3 ms (fast, of 100) | frames >8.3 ms (slow, of 100) | delta vs |
|---|---|---|---|---|---|---|---|---|---|---|
| syn7 | r1-dom | 9 | 1.44 | - | 6.13 | 8.49 | - | 11 | 3 | - |
| syn7 | r2 | 10 | 2.34 | +0.90 | 6.33 | 9.65 | +1.16 | 16 | 8 | r1-dom |
| syn7 | r3 | 8 | 7.21 | +4.87 | 6.02 | 14.09 | +4.43 | 99 | 20 | r2 |
| syn7 | r3a | 9 | 4.76 | -2.45 | 6.02 | 11.68 | -2.40 | 91 | 20 | r3 |
| syn7 | r3b | 10 | 5.28 | -1.93 | 6.38 | 12.53 | -1.56 | 99 | 20 | r3 |
| syn18 | r1-dom | 9 | 1.97 | - | 10.06 | 13.39 | - | 98 | 20 | - |
| syn18 | r2 | 9 | 3.37 | +1.41 | 9.05 | 14.13 | +0.74 | 99 | 20 | r1-dom |
| syn18 | r3 | 8 | 11.68 | +8.31 | 9.22 | 21.91 | +7.78 | 99 | 21 | r2 |
| syn18 | r3a | 9 | 7.60 | -4.08 | 9.85 | 18.21 | -3.70 | 99 | 21 | r3 |
| syn18 | r3b | 9 | 7.90 | -3.78 | 9.63 | 18.33 | -3.58 | 99 | 21 | r3 |
| corpus | r1-dom | 7 | 1.79 | - | 5.10 | 8.01 | - | 9 | 0 | - |
| corpus | r2 | 9 | 2.71 | +0.92 | 4.82 | 8.61 | +0.60 | 16 | 4 | r1-dom |
| corpus | r3 | 9 | 9.83 | +7.12 | 6.10 | 17.13 | +8.52 | 99 | 20 | r2 |
| corpus | r3a | 9 | 5.29 | -4.54 | 4.92 | 10.96 | -6.18 | 94 | 19 | r3 |
| corpus | r3b | 8 | 5.57 | -4.26 | 4.68 | 11.28 | -5.86 | 79 | 18 | r3 |

**`syn7`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1 vanilla createElement | 11 [4-54] / 1 [0-5] | 6.5 / 9.9 | 3 [0-20] / 0 [0-7] | 7.3 |
| R2 dioxus-web | 16 [4-99] / 2 [1-15] | 6.9 / 11.9 | 8 [1-20] / 0 [0-6] | 8.9 |
| R3 Clay stack (host tree + Dioxus) | 99 [99-99] / 8 [3-69] | 12.1 / 18.0 | 20 [20-48] / 0 [0-29] | 14.0 |
| R3a JS receiver | 91 [32-99] / 4 [1-56] | 9.3 / 16.4 | 20 [17-20] / 0 [0-3] | 12.1 |
| R3b Rust receiver | 99 [28-99] / 9 [3-96] | 11.9 / 19.8 | 20 [17-20] / 0 [0-20] | 11.7 |
**`syn18`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1 vanilla createElement | 98 [20-99] / 4 [2-37] | 9.8 / 16.6 | 20 [13-21] / 1 [0-3] | 12.4 |
| R2 dioxus-web | 99 [55-99] / 7 [2-94] | 10.8 / 17.5 | 20 [20-20] / 0 [0-13] | 13.0 |
| R3 Clay stack (host tree + Dioxus) | 99 [99-99] / 98 [45-99] | 21.1 / 27.1 | 21 [21-22] / 20 [19-21] | 23.6 |
| R3a JS receiver | 99 [99-99] / 85 [10-99] | 18.0 / 24.5 | 21 [21-22] / 12 [1-21] | 19.5 |
| R3b Rust receiver | 99 [99-99] / 57 [11-99] | 16.8 / 24.1 | 21 [21-22] / 6 [2-20] | 17.0 |
**`corpus`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**

| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame): >8.3 / >16.7 ms | slow busy p95 |
|---|---|---|---|---|
| R1 vanilla createElement | 9 [3-51] / 1 [0-5] | 5.3 / 10.2 | 0 [0-3] / 0 [0-0] | 6.5 |
| R2 dioxus-web | 16 [6-100] / 1 [0-62] | 6.5 / 10.6 | 4 [2-20] / 0 [0-3] | 8.3 |
| R3 Clay stack (host tree + Dioxus) | 99 [98-99] / 10 [2-79] | 13.6 / 18.6 | 20 [20-20] / 3 [0-12] | 16.1 |
| R3a JS receiver | 94 [24-99] / 5 [1-98] | 9.7 / 16.8 | 19 [13-23] / 0 [0-20] | 11.3 |
| R3b Rust receiver | 79 [36-99] / 4 [1-14] | 9.5 / 16.5 | 18 [12-24] / 0 [0-7] | 10.8 |

**Where the Clay rungs' script time goes (ms per commit; guest timers use a 0.1 ms clock)**

| workload | rung | guest VDOM diff | guest `surface.apply` call (wcmp crossing + lift + host sink) | host sink `apply` | of which receiver encode | of which receiver JS call / Rust loop | host render (measured: surface body -> effects) | trace top-level script: scroll EventDispatch | trace top-level script: all other tasks |
|---|---|---|---|---|---|---|---|---|---|
| syn7 | r3 | 2.30 | 1.35 | 0.50 | - | - | 2.65 | 6.89 | 0.32 |
| syn7 | r3a | 2.40 | 1.50 | 0.70 | 0.00 | 0.70 | - | 4.46 | 0.33 |
| syn7 | r3b | 2.40 | 1.85 | 1.05 | - | 1.05 | - | 4.95 | 0.33 |
| syn18 | r3 | 3.65 | 2.65 | 1.00 | - | - | 4.30 | 11.41 | 0.30 |
| syn18 | r3a | 3.80 | 2.90 | 1.30 | 0.00 | 1.30 | - | 7.34 | 0.32 |
| syn18 | r3b | 3.80 | 3.50 | 1.80 | - | 1.80 | - | 7.58 | 0.31 |
| corpus | r3 | 2.50 | 1.70 | 0.70 | - | - | 3.10 | 9.53 | 0.34 |
| corpus | r3a | 2.40 | 2.00 | 1.00 | 0.00 | 1.00 | - | 4.98 | 0.33 |
| corpus | r3b | 2.40 | 2.30 | 1.30 | - | 1.30 | - | 5.30 | 0.33 |

Reading the table: the guest VDOM diff is the same on all three (2.3-3.8 ms). `surface.apply` call (guest's view, includes the wcmp crossing, the lift and conversion of `Vec<wit Mutation>`, the ingress budget check and the sink) minus the sink `apply` is the bridge: 0.85 / 1.65 / 1.0 ms on R3, 0.8 / 1.6 / 1.0 on R3a, 0.8 / 1.7 / 1.0 on R3b, so the bridge is untouched by a receiver. The raw-bytes variant the lane brief mentions (applying the guest's encoded batch without the lift) was **not built**: the lift happens inside the wcmp runtime before the sink is called, so it needs a runtime change; the 0.8-1.7 ms bridge is the upper bound on what it could remove.

| workload | rung | EventDispatch:scroll | EventDispatch:scrollend | FireAnimationFrame | v8.callFunction |
|---|---|---|---|---|---|
| syn7 | r3 | 6.89 | 0.00 | 0.32 | 0.00 |
| syn7 | r3a | 4.46 | 0.00 | 0.33 | 0.00 |
| syn7 | r3b | 4.95 | 0.00 | 0.33 | 0.00 |
| syn18 | r3 | 11.41 | 0.00 | 0.30 | 0.00 |
| syn18 | r3a | 7.34 | 0.00 | 0.32 | 0.00 |
| syn18 | r3b | 7.58 | 0.00 | 0.31 | 0.00 |
| corpus | r3 | 9.53 | 0.00 | 0.33 | 0.00 |
| corpus | r3a | 4.98 | 0.00 | 0.33 | 0.00 |
| corpus | r3b | 5.30 | 0.00 | 0.33 | 0.00 |

`EventDispatch:scroll` is the whole guest + host commit of the scroll event.

### Native (Blitz, headless)
`bench/blitz-x4/src/main.rs`. Input: the 85 wire batches of one r3a run (`__clayCapture`, the batches the JS receiver was given: 5 build the surface, 80 are the scroll commits of the ladder's commit loop; 3,657 / 8,057 / 4,101 mutations each). Each mode replays them into a fresh headless `BaseDocument` (viewport 1280x900, system fonts, release build, opt-level 3, no LTO; Ryzen 9 7940HS) and times every commit with `Instant`; 7 fresh documents per mode and workload, 560 commits per cell. What is timed per commit: **`today`** = `SurfaceTree::apply` (with the spike-raised limits) + host `VirtualDom` `mark_dirty(APP)` + `render_immediate` into a `MutationBuffer` (`SurfaceRoot` projection and diff) + replay of that buffer into the Blitz document through `DioxusState::writer` (the path `DioxusDocument::poll` takes) + `resolve`; **`direct`** = `replay` of the batch into `DioxusState::writer` over a container node + `resolve`. `resolve(0.0)` is Blitz's style resolution and layout; **nothing is painted** and the scroller's scroll offset is not moved (the browser's scrollTop is not replayed), so these are not frame times. The real `dayframe_desktop` window was not run. After the last batch both documents have the same tree (3,462 vs 3,463 nodes: `DioxusDocument` adds a `head`).

ms per commit, median over 560 commits (p95 of the commit total in brackets). Load average 2.6-2.7 for the parallel runs; 4.1-4.6 for the sequential ones (run with `nice -n 10` while the web matrix was running).

| workload | style threading | mode | tree apply | host diff | write into Blitz | receive total | resolve (style + layout) | commit total (p95) |
|---|---|---|---|---|---|---|---|---|
| syn7 | parallel | today | 0.44 | 1.41 | 0.68 | 2.53 | 4.67 | 7.30 (8.96) |
| syn7 | parallel | direct | - | - | 0.52 | 0.52 | 4.79 | 5.38 (6.27) |
| syn18 | parallel | today | 1.06 | 3.04 | 1.31 | 5.41 | 10.70 | 16.26 (18.24) |
| syn18 | parallel | direct | - | - | 1.25 | 1.25 | 10.52 | 11.78 (13.22) |
| corpus | parallel | today | 0.58 | 1.58 | 1.00 | 3.16 | 4.06 | 7.32 (9.23) |
| corpus | parallel | direct | - | - | 0.85 | 0.85 | 4.10 | 4.98 (5.95) |
| syn7 | sequential | today | 0.64 | 1.94 | 0.95 | 3.53 | 6.48 | 10.14 (13.39) |
| syn7 | sequential | direct | - | - | 0.73 | 0.73 | 6.13 | 6.93 (9.09) |
| syn18 | sequential | today | 1.36 | 3.78 | 1.65 | 6.80 | 13.64 | 20.67 (27.90) |
| syn18 | sequential | direct | - | - | 1.51 | 1.51 | 12.94 | 14.60 (19.98) |
| corpus | sequential | today | 0.85 | 2.23 | 1.49 | 4.57 | 5.15 | 9.88 (14.55) |
| corpus | sequential | direct | - | - | 1.31 | 1.31 | 5.52 | 6.84 (9.18) |

"Parallel" is blitz-dom's default Stylo traversal over its global rayon pool (16 hardware threads here); "sequential" is `StyleThreading::Sequential` on the calling thread, closer to a phone with few cores. Receiving costs 2.5-5.4 ms (parallel) today and 0.5-1.3 ms direct; `resolve` is 4.1-10.7 ms and is the same on both, so it is the larger term in both modes.

### What each receiver skipped (prototype scope, explicit)
- **Policy / validation.** None of `SurfaceTree`'s ingress checks run: no tag / attribute / listener vocabulary (`static_attribute`, `supported_tag`, `supported_listener`), no per-batch budgets (`MAX_BATCH_*`, string, node-slot, depth, retained bytes), no stack-arity or attachment checks, no cycle checks, no atomicity (a bad op throws mid-batch with the DOM half-updated; the tree path validates each mutation before applying it but also commits the surface on failure by clearing it). `PresentationHost::apply`'s `ingress_budget` and component-grant checks still run before the sink, because they sit above it. No per-element policy hook (the X2 report's `remote-dom` host receiver has one).
- **Component slots.** `mount-component` panics: the surface the micro uses has none. The tree path turns a slot into a host `ComponentMount`.
- **Events.** DOM listeners are per element, not delegated; with both a child and an ancestor listening for the same bubbling event the DOM would call the ancestor's listener on its own and the guest's bubbling would call it again (not exercised: the micro listens for `scroll` on one element and `click` on buttons). Payloads: `scroll` and `click` only (mouse default: no coordinates or modifiers); everything else throws. No stale-event check (`event_is_current` needs the tree). No `preventDefault` for passive listeners (scroll and wheel listeners are registered passive). No `mounted` for an element before the container is attached.
- **Element handles.** Element commands address `nodes[id]` of the receiver; `ElementHandles` / `MountedData` are not involved. Removed elements' ids stay in the id table until reused (bounded by the id space, 32,768 here).
- **Emulated shadow semantics.** The container is a light-DOM element. Real Pierre renders in a shadow root (X1 R0); attaching the receiver root in a shadow root, retargeting events, and style scoping were not done and their cost was not measured.
- **Attribute volatility.** `value`, `checked`, `selected` are set as properties; no other volatile semantics, no `dangerous_inner_html`, no `Any`-valued attributes (the tree path rejects them; the wire cannot carry them).
- **Native**: the harness replays batches into `DioxusState::writer`; it has no event path, no scroll offset, no paint.

### What a production receiver must add, with estimated cost
- **Whole-batch validation before the first DOM write.** Measured: the cheap half (tag / attribute / listener vocabulary from `render.rs`, stack arity of every op, string and batch budgets) over the captured batches in V8 (`bench/ladder/validate-cost.mjs`, Deno 2.9.7, warm, no DOM, second half of 20 reps): 0.04 ms (syn7), 0.11 (syn18), 0.13 (corpus) per commit, p95 0.06 / 0.14 / 0.19. The structural half (cycle, depth, attachment-target checks) was not measured; it needs parent links, which the DOM has (`parentNode`, `contains`) but which cost DOM reads per op, or a retained tree. The whole retained-tree path's validation + arena upkeep is `SurfaceTree::apply`: 0.5 / 1.0 / 0.7 ms in the browser (sink timer, R3) and 0.44 / 1.06 / 0.58 ms natively; that is an upper bound for validation.
- **Per-element policy** (the X2 study's per-element allow/deny): one lookup per `create-element` / `set-attribute` on top of the vocabulary check; same order as the vocabulary pass, so estimated at ~0.1 ms per commit (not measured).
- **Delegated events**: one root listener per event name that walks from `event.target` to the nearest node with an id (tens of steps), as Dioxus does; estimated negligible per event next to the dispatch (not measured). Needs payload serializers for the whole vocabulary (the host path's `with_html_event_groups!` list): not measured; the scroll payload (six property reads and one string) is the cheapest case, and the two variants share it.
- **Component slots**: a host-owned child container per slot with a nested receiver; the host `VirtualDom` is involved only when a slot appears or goes, not per batch. Not built, not measured.
- **Shadow root / style scoping**: unmeasured (see above).
- **Atomic apply on failure**: validate-then-apply already gives it for the vocabulary half; for DOM exceptions mid-batch it needs either the tree or a rebuild of the container from the guest's next full state. Not designed here.

### Observations (not decisions)
- At the syn7 workload the direct receivers move R3's script from 7.2 ms to 4.8 (JS) / 5.3 (Rust), busy from 14.1 to 11.7 / 12.5 against R2's 9.7 and R1's 8.5. Measured as share of the R3-to-R2 busy gap closed: 54% / 35% (syn7), 48% / 46% (syn18), 72% / 69% (corpus) for JS / Rust (computed from the summary table). At 7 spans the remaining distance to R2 is about 2.0-2.9 ms busy; the guest VDOM diff (2.4) and the wcmp crossing plus lift (0.8) are what is left, and the guest diff alone equals R2's whole script (2.3).
- The JS receiver is faster than the Rust one on every workload and cheaper per op: sink `apply` 0.70 / 1.30 / 1.00 ms against 1.05 / 1.80 / 1.30 for web-sys per op, with the encode step below the 0.1 ms clock. The Rust variant pays a wasm-to-JS call per DOM call; the JS variant pays one boundary call per batch and one blob decode. The totals in the summary table lie within each other's round spreads for syn18 and corpus (busy 18.2 vs 18.3, 11.0 vs 11.3).
- Neither receiver brings the commit into an 8.3 ms frame at 7 spans: busy is 11.7-12.5 ms, and in fast scroll 91-99 of 100 frames exceed 8.3 ms (R2: 16, R1: 11). The browser's own style + layout + paint (6.0-6.4 ms at syn7) is the same on all rungs and is by itself most of the budget. At 18 spans every rung is over budget.
- The host's own re-projection is smaller than the derived figure suggested (2.65 / 4.30 / 3.10 ms measured vs 3.6 / 5.4 / 3.8 derived), and it runs inside the scroll event handler, so it adds to the event's task rather than showing up as a separate task.
- On native Blitz the host re-diff and tree upkeep cost 2.0-4.2 ms of a 7.3-16.3 ms commit (parallel Stylo; 2.8-5.3 of 9.9-20.7 sequential), and the direct writer removes them; Blitz's own style + layout (`resolve`) is the larger term (4.1-10.7 ms parallel) and unchanged. The direct writer into Blitz needed no new code, only the already-public `DioxusState::writer`.
- Every number above is for one workload shape (a 50-row window shift of 3.4k-7.8k nodes); a surface that commits small batches (a text cursor, a toggled class) would pay the fixed parts (event, crossing, signal wake-up, host render of a small tree) rather than these per-node ones, and was not measured.

### Caveats and unverified
- Same environment caveats as X1 (headless, software-composited, 60 Hz vsync, 0.1 ms `performance.now` clock, a shared host: other agents' builds raised the load, which is why 54 of 192 runs were excluded and why waiting for load was added from round 7). Per-cell spreads are large for the frame-count columns (e.g. R3a syn7 fast `91 [32-99]`): a median of one noisy round-set; read the busy-time columns first.
- R3's numbers differ from X1's R3 (see above); the comparison between rungs is within this run set.
- The `host-render` timer's end point (effects) is not proven to follow the final DOM flush. The raw-bytes receiver, a shadow-root receiver, an events-complete receiver and a component-slot receiver were not built.
- The native numbers are headless replays of recorded batches, not a running `dayframe_desktop`; no paint, no real scroll offset; the sequential rows were taken at load 4.1-4.6 with `nice`, the parallel rows at 2.6-2.7.
- `validate-cost.mjs` measures a warm V8 loop outside the browser and the cheap half of validation only.

### Commands run (from the worktree root unless noted)
- Host build: `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 CARGO_RESOLVER_FEATURE_UNIFICATION=selected DAYFRAME_RAISED_LIMITS=1 ./x/agent-build.ts -- deno run --allow-all bench/ladder/assemble-r3.ts --build --packages=<target>/spike-scroll/dayframe/packages` (`cargo clippy --locked -p clay_dioxus --target wasm32-unknown-unknown --features spike-timing` passed first, one pre-existing warning; the native build of `clay_dioxus` with the non-browser `direct` stub was started (`cargo clippy -p clay_dioxus --all-targets`) and hit the 25-minute timeout during the cold native dependency build, so it is **unverified**).
- Servers: `deno run --allow-all bench/ladder/serve.ts 4701 /r0/=... /r1/=... /r2/=... /corpus/=... /=bench/ladder` and `... 4702 /=<target>/spike-scroll/dayframe_web/site`; both killed by PID at the end.
- Capture: `LADDER_CAPTURE=<x4>/batches-<workload>.json LADDER_RUNG=r3a LADDER_WORKLOADS=<workload> npx playwright test -c playwright.config.ts --workers=1` (from `bench/ladder`).
- Matrix: `bench/ladder/run-matrix.sh 6 r1-dom r2 r3 r3a r3b`, then `START=7 MAX_LOAD=6 bench/ladder/run-matrix.sh 6 r1-dom r2 r3 r3a r3b`; `python3 bench/ladder/x4_tables.py results.jsonl`.
- Native: `cd bench/blitz-x4 && CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 ../../x/agent-build.ts -- cargo build --release`; `blitz_x4 <batches.json> 7 [--sequential]`; `python3 bench/blitz-x4/summarize.py <native-*.txt>`.
- Validation cost: `deno run --allow-read bench/ladder/validate-cost.mjs <batches-*.json>`.
