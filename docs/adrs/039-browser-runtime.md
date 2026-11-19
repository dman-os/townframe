# ADR 039: The browser runtime — Send everywhere, n0_future, a leader-elected dedicated worker

**Status:** Accepted (operator, 2026-11-19).

**Evidence:** [`docs/scratch/browser-async-research.md`](../scratch/browser-async-research.md)
(subduction, keyhive, iroh 1.4, n0-future 0.3, `sqlite_rs`, read from source with
citations), and the prod-readiness ledger's "Browser story" section.

**Related:** [ADR 018: browser guest engine](018-browser-guest-engine.md),
[ADR 033: what a node is](033-what-a-node-is.md).

## Context

Most of the repo must run in the browser (wasm32-unknown-unknown): the node, big_repo,
big_sync/big_automata, the service stack and its users. Today it assumes tokio throughout
(`tokio::spawn`, `tokio::time`, tokio locks) and, in places, `future_form`'s
`Sendable`/`Local` split.

tokio cannot run in the browser. An async runtime has to own a loop that blocks: parking on
its I/O driver and timers. In the browser, JavaScript owns the event loop. The main thread may
never block, there are no sockets for mio, only Promise-returning JS APIs, and tokio's timer
reads `std::time::Instant`, which panics on wasm32-unknown-unknown. Threads exist only with
SharedArrayBuffer, cross-origin isolation and a std rebuilt with atomics. Async Rust in the
browser runs on the JS microtask queue instead (`wasm_bindgen_futures::spawn_local`), with I/O
and timers as wrapped Promises. That is single-threaded, and JS handles are `!Send`.

Two ecosystem answers exist:

- **Subduction and keyhive** split every async trait into a `Sendable` and a `Local` form
  (`future_form`), because applications implement their storage, signer and spawn traits in
  JavaScript, holding JS handles across awaits.
- **iroh** keeps every API `Send` and wraps JS handles in `SendWrapper` one layer down
  (n0-future's join handles and timers, the relay WebSocket stream). It spawns through
  `n0_future`, which calls tokio natively and `spawn_local` in the browser.

## Decisions

### 1. Shared async code is `Send`

Shared traits and futures are `Send`. Trait methods use native `async fn` in traits or
`-> impl Future + Send`, with static dispatch, boxing only where a trait is used as `dyn`. JS
handles (the browser SQLite client, WebCrypto keys, MessagePort, IndexedDB) are wrapped once,
at the adapter that owns them, with `SendWrapper`.

This is sound because the browser runs our code on one thread. Wasm threads are opt-in
(SharedArrayBuffer, COOP/COEP, atomics) and not planned. If they ever are, `SendWrapper`
panics where a value crosses threads, and that is the signal to revisit this decision.

`future_form` buys compile-time honesty (no runtime check), support for multi-threaded wasm,
and native `!Send` users. That suits a library embedded in unknown setups. We own every
implementor, so it costs us a form parameter and a boxed future on every trait call for no
gain. **`future_form` stays only where we implement subduction or keyhive traits**, which
require it. It is removed from big_automata now, and from big_repo when it is reworked.

### 2. One runtime surface: `n0_future` plus `tokio::sync`

- Spawning, `JoinSet`, abort-on-drop handles, timeouts, intervals and time come from
  `n0_future` (web-time in the browser).
- Channels and `watch` come from `tokio::sync`, which is pure Rust and works in the browser.
- Locks are `std::sync::Mutex` or surelock, never held across an await. tokio's `Mutex` and
  `RwLock` are not used (futurelock).
- Shared code never uses `tokio::spawn`, `tokio::time`, `tokio::net` or `tokio::task`.

No re-export crate: `n0_future` is already a workspace dependency, and the service stack
uses it.

### 3. Drivers are runtime-agnostic and live in their domain crate

State machines stay sans-IO. Their drivers (the loop that feeds events, spawns tasks and
persists state, like the old `big_sync/worker.rs`) use only the surface above, so one driver
runs natively and in the browser. A driver lives in its domain crate: big_automata's driver
goes in big_automata. A separate crate exists only where compilation units force it, never
to hold a runtime. No crate is named for tokio unless it really is native-only.

### 4. Hosting: a leader-elected dedicated worker

The node runs in a **dedicated Web Worker**. Each tab's worker contends for a Web Lock
(`navigator.locks`). The lock holder runs the node and owns the database, and the other tabs
talk to it over BroadcastChannel or a MessagePort. When the leader's tab closes, the lock
passes on.

- A service worker cannot host the node: the browser terminates it when idle, which drops
  long-lived iroh connections, and it has no OPFS synchronous access handles. It remains
  available for what it is for (offline assets, push).
- Which surface talks to the node, and how, is the embedder's choice. Lower layers do not
  assume a topology. Neither subduction nor iroh does; both work in a window or a worker.

### 5. Storage: SQLite on OPFS

The node's SQLite runs on OPFS through synchronous access handles (an opfs-sahpool-style
VFS). These exist only in dedicated workers, and have shipped in Chromium, Firefox 111+ and
Safari 16.4+. IndexedDB through an async VFS (Asyncify or JSPI) is the fallback: it works
anywhere, but with a larger binary and slower I/O. `sqlite_rs` becomes `sqlite_utils_rs` and
gains a wasm32-unknown-unknown backend, with its handle wrapped per decision 1.

### 6. Every crate states its browser build

Every crate a browser node links declares `[builds.wasm32-unknown-unknown]` in its
`guardrails.toml` and passes the wasm clippy. "Doesn't build for wasm" is fixed at the
dependency or macro, never by a `cfg(not(wasm))` gate on functionality. Example: our
`declare_service_protocol!` asked irpc for its QUIC half everywhere; it now emits `no_rpc` in
the browser, as iroh-blobs does.

## Consequences

- Drivers and services become testable in a real headless browser (wasm-bindgen-test in the
  Playwright farm), and the single-threaded executor matches deterministic simulation testing.
- big_automata's traits lose their form parameter; big_repo's `future_form` usage is removed
  in its rework.
- Code holding JS handles carries a latent panic if moved across threads. That is acceptable
  under decision 1's single-thread premise and documented at each wrapper.
- `big_sync/worker.rs`, `wflow_tokio` and big_repo's runtime need rewriting to the runtime
  surface when they are ported.
