# Browser async research: FutureForm vs Send-everywhere on wasm32-unknown-unknown

Status: Research notes. Findings are read from source; anything inferred is marked "(inference)". No decisions made.
Owner: prod-readiness lane; operator decides the runtime surface.
Scope: How subduction, keyhive, iroh 1.4.1 and n0-future 0.3.2 run async code in the browser, and what that means for `big_automata`, `big_repo/runtime2`, `big_sync`, `wflow_tokio` and `sqlite_rs`.
Updated: 2026-11-19

Paths: `SUB` = `/run/media/asdf/p3N/repos/subduction`, `KH` = `/run/media/asdf/p3N/repos/keyhive`, `REG` = `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f`, `TF` = this repo's `src/`. Nothing was built or run.

## 1. Why FutureForm, and how iroh avoids it

### What FutureForm is

- `FutureForm` has two forms: `Sendable` (`BoxFuture`) and `Local` (`LocalBoxFuture`). A trait is written once as `K::Future<'a, T>` and implemented for both with `#[future_form(Sendable, Local)]` (`REG/future_form-0.3.1/src/lib.rs:7-9,235`; README "Motivation").
- Subduction states the reason directly: "JavaScript's single-threaded model means futures can't be `Send`" (`SUB/HACKING.md:13-37`). Typical instantiations are `Sendable` + `FsStorage` + websocket for the CLI server, and `Local` + `JsStorage` + `JsConnection` for the wasm browser build (`SUB/HACKING.md:100-103`).
- Everything is generic over `F`: `Storage<F>` (`SUB/subduction_core/src/storage/traits.rs:71`), `Timeout<F>` (`SUB/subduction_core/src/timeout.rs:31`), `Spawn<F>` (`SUB/HACKING.md:141`), `Connection<F>`, policies.
- Keyhive does the same: `AsyncSigner<F>` (`KH/keyhive_crypto/src/signer/async_signer.rs:29`), `CiphertextStore<F,..>` (`KH/keyhive_core/src/store/ciphertext.rs:36`), `MembershipListener<F,..>`, `CgkaListener<F>`, `PrekeyListener<F>`.
- Keyhive notes that a sync signer implements both forms via `F::ready` (`async_signer.rs:20-26`). It also notes that `try_sign_async` is a free function because "the `Sendable` form requires `T: Send` while the `Local` form does not" and one trait method cannot express that (`async_signer.rs:40-47`).

### The concrete `!Send` types that force `Local`

All of these are held across `.await` points, or inside a value the future borrows.

| Type | Where | Why it is `!Send` |
|---|---|---|
| `js_sys::Function` event callback | `JsEventHandler(pub(crate) js_sys::Function)`, `KH/keyhive_wasm/src/js/event_handler.rs:21`; it is the `MembershipListener<Local,..>`, `CgkaListener<Local>` and `PrekeyListener<Local>` impl (`:40-76`) | `JsValue` is `!Send` |
| `web_sys::CryptoKey` | `JsSignerOptions::WebCrypto { signing_key: web_sys::CryptoKey }`, `KH/keyhive_wasm/src/js/signer.rs:168-176` | Opaque browser handle; `sign` goes through `JsFuture` |
| JS-supplied storage object | `JsStorage` plus `#[wasm_bindgen(method)] ... -> Promise` imports, `SUB/sedimentree_wasm/src/storage.rs:150-170`; `impl Storage<Local> for JsStorage`, `:195` | `Promise` and `JsValue` handles |
| IndexedDB | `WasmIndexedDbStorage(IdbDatabase)`, `SUB/sedimentree_wasm/src/storage/idb.rs:55-58`; uses `Rc`, `RefCell` and `futures::channel::oneshot` (`:3-12`) | `IdbDatabase`, `IdbRequest` are web-sys handles |
| `fetch` / `Response` | `SUB/subduction_wasm/src/transport/fetch_client.rs:16,55`: "Since `web_sys` types are `!Send`, this implements `HttpClient<Local>`" | web-sys |
| JS-callback transports | `JsFuture::from(self.js_send_bytes(..))`, `SUB/subduction_wasm/src/transport.rs:100-146` | `JsFuture` |
| MessagePort transport | `Rc<RefCell<MessageQueue>>`, `Rc<Port>`, `RefCell<Option<Function>>`, `SUB/subduction_wasm/src/transport/message_port.rs:57,75-78,96` | `Rc`, `RefCell` plus `Closure` |
| WebCrypto signer / policy callbacks | `JsFuture::from(subtle.sign_...)`, `SUB/subduction_wasm/src/signer/webcrypto.rs:111-269`, `policy.rs:132,198` | `JsFuture`, `Rc<Cell<..>>` (`webcrypto.rs:333`) |
| `Closure<dyn Fn()>` in `Rc<RefCell<..>>` | `SUB/subduction_wasm/src/transport/on_disconnect.rs:14` | `Closure` is `!Send` |

Subduction's wasm `Spawn` is `Spawn<Local>`: `fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle { wasm_bindgen_futures::spawn_local(Abortable::new(fut, reg)) }` (`SUB/subduction_wasm/src/subduction.rs:112-122`).

Two structural facts:

- The `!Send` data is app-injected JS, not the core. `subduction_core` and `keyhive_core` are generic over `F`, and the JS-facing crates pin `F = Local`.
- Neither project wraps JS handles in `SendWrapper`. grep for `SendWrapper`/`send_wrapper`/`unsafe impl Send` in `SUB` source and `KH` source found no uses. `send_wrapper` appears in `SUB/Cargo.lock` only via `n0-future` and `ws_stream_wasm`.

### How iroh avoids it

iroh keeps every public type and trait `Send` on wasm. The `!Send` JS handles are wrapped one layer down, and the cfg split is in task/time plumbing rather than in the type system.

- **n0-future wraps the JS handles.**
  - `JoinHandle`/`AbortHandle` hold `SendWrapper<Rc<RefCell<State>>>` (`REG/n0-future-0.3.2/src/task.rs:252-260,514`).
  - `Sleep` holds `Option<SendWrapper<JsValue>>` for the `setTimeout` id (`src/time.rs:36-39,72,144`).
  - The comment at `task.rs:249-258` states the contract: this is safe as long as all work stays on one thread. A handle moved across Web Workers sharing memory would panic on use.
- **`ws_stream_wasm` wraps the socket.** `WsStream` holds `SendWrapper<Rc<WebSocket>>`, `SendWrapper<Rc<RefCell<VecDeque<..>>>>` (`REG/ws_stream_wasm-0.7.5/src/ws_stream.rs:27-35`; `ws_meta.rs:19`). iroh-relay's `Conn` therefore holds a `Send` WebSocket on wasm (`REG/iroh-relay-1.4.1/src/client/conn.rs:76-91`, `src/protos/streams.rs:26-31`).
- **noq's runtime trait is `Send` on wasm.** `Runtime: Send + Sync` and `spawn(Pin<Box<dyn Future + Send>>)` (`REG/noq-1.4.0/src/runtime/mod.rs:19-24`). iroh's wasm impl calls `wasm_bindgen_futures::spawn_local(future)` on that `Send` future (`REG/iroh-1.4.1/src/runtime.rs:98-101`). The timer is `n0_future::time::sleep_until` (`:58-65`).
- **`boxed` is the only cfg-flipping Send type.** `n0_future::boxed::BoxFuture` is `futures_lite::future::Boxed` natively and `BoxedLocal` under `wasm_browser` (`REG/n0-future-0.3.2/src/lib.rs`, `pub mod boxed`). But iroh's own public futures use `n0_future::future::Boxed` (`REG/iroh-1.4.1/src/endpoint/connection.rs:33,266,432`), which is always `Send`. That works because the wasm-only inner pieces are made `Send` by `SendWrapper` (inference from the combination).
- **Cfg differences are about capabilities, not `Send`.** `wasm_browser` (`all(target_family="wasm", target_os="unknown")`, defined in each `build.rs` via `cfg_aliases`) removes direct QUIC/UDP, DNS, portmapper, net-report QAD and TLS; only relay-over-WebSocket remains. Evidence: `iroh/Cargo.toml:424-437` (`noq` and `portmapper` only for non-wasm), `address_lookup` DNS gated at `presets.rs:131`, `iroh-relay/src/client.rs:387-425` (WebSocket via `ws_stream_wasm`, auth token as a URL query param because browsers cannot set upgrade headers). Note noq itself still builds on wasm for its `Runtime` plumbing (`noq/src/lib.rs:56-58`).
- **iroh uses n0-future's `task::spawn` for its own actors** (`iroh/src/net_report.rs:31,860,926`). On wasm that has no `Send` bound (see section 2). Some paths still use `tokio::spawn`/`tokio::task::JoinSet` (`address_lookup.rs:1012`, `net_report.rs:499`) because they sit behind `cfg(not(wasm_browser))` or tokio's `rt` is only enabled on native (`iroh/Cargo.toml:437`).

### When do you need FutureForm?

Inference from the above:

- **FutureForm-style duality pays off** when all three hold:
  1. Your traits are implemented by users or embedders. Subduction ships a JS-facing library whose `Storage`, `Signer` and `Policy` are JS objects supplied by the app.
  2. Those implementations hold raw JS handles you do not control, including closures, `Promise` and `IdbDatabase`.
  3. You also need the `Send` form on native servers, for example tokio multi-thread.

  Duality means the trait is not locked to one form, and the same core logic runs in both. The cost is real: every trait carries `K: FutureForm`, boxed futures everywhere (`K::from_future` allocates), `+ ?Sized` and `Sendable`/`Local` macro expansion, and the "`T: Send` only for `Sendable`" problem (`async_signer.rs:40-47`).
- **"Keep everything `Send`, wrap JS handles" suffices** when the JS-touching surface is small, internal and owned by you. iroh is the example: a handful of types (`WsStream`, `Sleep`, `JoinHandle`) are wrapped once, inside a library. The cost is that `SendWrapper` panics if used from another thread, which is safe only under a documented single-thread-per-wasm-instance assumption (`task.rs:249-258`).
- **What we lose with Send-everywhere:** the ability to hold a raw `!Send` handle (a `JsValue`, an `Rc<RefCell<..>>` single-owner state) across `.await` without a wrapper; a wrapper per handle type; and a runtime panic, not a compile error, when the single-thread assumption is violated.
- **What we gain:** one set of traits, no `K` parameter, no boxing-for-form, `async fn` in traits and `impl Future + Send` signatures work as-is, and native-first crates compile unchanged for wasm.
- **What we lose with FutureForm-everywhere:** ergonomics (generic `K` threaded through every actor and test), and `Local` forbids moving state to tokio worker threads on native.
- **What we gain with FutureForm-everywhere:** compile-time enforcement of single-threadedness on wasm, and no `unsafe`-flavoured wrappers.

## 2. What n0-future 0.3.2 provides

Crate source: `REG/n0-future-0.3.2/src/{lib,task,time,maybe_future}.rs`. The `wasm_browser` cfg comes from `build.rs` and is `all(target_family="wasm", target_os="unknown")`.

| Item | Native | `wasm_browser` |
|---|---|---|
| `task::spawn`, `JoinHandle`, `JoinSet`, `AbortHandle`, `JoinError`, `Id` | re-exports of tokio (`task.rs:6-11`) | own shims (`task.rs:13-615`) |
| `task::AbortOnDropHandle` | `tokio_util::task::AbortOnDropHandle` | own struct (`task.rs:555-600`) |
| `time::{sleep, sleep_until, timeout, interval, Instant, Duration, ...}` | tokio re-exports (`time.rs:6-10`) | `setTimeout`-based `Sleep` (`:36-72`), `Timeout` (`:183-245`), `Interval` (`:292-400`); `Instant`/`Duration`/`SystemTime` from `web-time` (`:32`) |
| `boxed::{BoxFuture, BoxStream}` | `Send` | `Local` (`lib.rs` `pub mod boxed`) |
| `MaybeFuture` | present | same (`maybe_future.rs`); pending when `None`, for disabling `select!` branches |
| Re-exports | `futures-buffered::*` (`FuturesUnordered`, `FuturesOrdered`, buffered streams), `futures_lite::{io, pin, ready, Future, FutureExt, Stream, StreamExt}`, `futures_util::{Either, Sink, SinkExt, TryFutureExt, TryStreamExt}` | same |
| `future::now_or_never`, `split::split` | present | same |

### Send and spawn semantics on wasm (read from source)

- Wasm `spawn<T: 'static>(fut: impl IntoFuture<Output=T> + 'static)` has no `Send` bound; it calls `wasm_bindgen_futures::spawn_local` (`task.rs:606-615`). `JoinSet::spawn` also has no `Send` bound, and `JoinSet::spawn_local` is the same thing (`task.rs:72-101`).
- Native `spawn` is `tokio::spawn`, which requires `Send + 'static`. A crate that compiles on both targets with `n0_future::task::spawn` is therefore held to the native `Send` bound: the `Send` bound on native is the stricter check, and wasm just does not enforce it (so the wasm path accepts more than it would be checked against).
- There is no executor or `LocalSet`. Wasm tasks run on the JS microtask/event loop through `spawn_local`; there is no `LocalSet`, no `block_on`, and no thread-per-task. The task id counter is a global `Mutex<u64>` (`task.rs:30-35`).
- Abort is cooperative: `abort()` flips shared state that the wrapper future checks (`task.rs:358,536`). `JoinSet` drop calls `abort_all` (`:233-243`). Dropping a native tokio `JoinHandle` detaches; `AbortOnDropHandle` aborts.
- Handles are `Send` only through `SendWrapper`: using a `JoinHandle`/`AbortHandle`/`Sleep` from a different thread than the creator panics (`task.rs:249-258`; `SendWrapper` semantics).
- `time` on wasm needs a JS global with `setTimeout`; it works in `Window` and `WorkerGlobalScope` (inference from `js_sys`/`wasm-bindgen` use; I did not trace the exact global lookup).
- n0-future depends on `tokio` with no `rt` features on wasm (`Cargo.toml:60-68`, the `tokio` features list is empty for wasm and `rt/time/macros/test-util` only for non-wasm).
- Not provided: channels, `select!`, locks, `net`, `fs`, `block_on`. Those come from tokio's `sync` and `macros` features, which build on wasm. Our `service_client_iroh_rs/Cargo.toml:17-28` already uses exactly this split.

## 3. Browser topology assumptions

### subduction / keyhive

Neither assumes a specific topology; both are single-threaded-per-instance and avoid `window`-only APIs.

- `fetch` code probes `Window` then `WorkerGlobalScope`, with a comment about "service worker, web worker" (`SUB/subduction_wasm/src/transport/fetch_client.rs:4,112,160-175`).
- WebCrypto signer: "Works in both window and Web Worker contexts by accessing APIs through" the global scope (`SUB/subduction_wasm/src/signer/webcrypto.rs:7,24,44`).
- A `MessagePort` transport exists (`transport/message_port.rs`), which is the natural link between a page and a worker (inference about intended use; the file does not say so).
- Transports offered: WebSocket (`transport/websocket.rs`), HTTP long-poll (`transport/longpoll.rs`), `fetch`, MessagePort, and JS-callback transport (`transport.rs`). Storage is app-injected (section 4).
- Delivered via `wasm-bindgen`. `SUB/README.md:223-250` shows a page-level `new Subduction(storage)` snippet. `SUB/subduction_wasm/e2e/ephemeral.spec.ts` is a Playwright test; I did not read it for topology.
- Keyhive: `wasm-bodge` bundles for bundlers, plain browsers (base64-embedded wasm), Cloudflare Workers and IIFE (`KH/keyhive_wasm/HACKING.md:5-11`); tests run in Chromium and Firefox (`KH/keyhive_wasm/README.md:65`). The wasm code uses `Arc<Mutex<..>>` of `!Send` data (`keyhive_wasm/src/js/group.rs:28`; `SUB/subduction_wasm/src/subduction.rs:159-162`), which only makes sense on one thread (inference).
- No SharedWorker or service-worker code in either (grep for `SharedWorker`, `ServiceWorker` found only the `WorkerGlobalScope` fallbacks above).

### iroh

- Relay-only: browsers speak the relay protocol over `ws_stream_wasm::WsMeta::connect`, no QUIC/UDP, no DNS, no portmapper (`REG/iroh-relay-1.4.1/src/client.rs:387-425`; `REG/iroh-1.4.1/src/endpoint/presets.rs:131`).
- Network monitor registers listeners on `js_sys::global()` cast to `EventTarget`, not `window`, so it works in a dedicated worker too (`REG/netwatch-0.19.3/src/netmon/wasm_browser.rs:50,76`; the file also imports `web_sys::{EventListener, EventTarget}` at `:8`).
- No explicit mention of workers/SharedWorker/service workers in the iroh crates' wasm paths (grep for `Worker`/`window` in iroh-relay found nothing). Whether `ws_stream_wasm` and `web_sys::WebSocket` work in each worker kind is a browser fact, not shown in this source (inference: yes for dedicated and shared workers, not verified).
- Single-thread assumption is explicit in n0-future (`task.rs:249-258`).
- iroh's own browser guidance (`iroh/docs/`) contains no browser section (`docs/` has only `local_relays.md` and `relays.md`).

## 4. Storage in the browser

### subduction

- Trait shape: `Storage<Async: FutureForm + ?Sized>` with an associated `Error`, `Async::Future<'_, Result<..>>` methods for sedimentree ids, loose commits, fragments and batches (`SUB/subduction_core/src/storage/traits.rs:71-83`).
- Browser implementations:
  - `JsStorage`: a JS object with `saveCommit`/`loadAllFragments`/`saveBatchAll`... methods returning `Promise`s, implementing `Storage<Local>` (`SUB/sedimentree_wasm/src/storage.rs:150-195`). Load-time re-verification of blobs because "the JS app owns the store" (`:176-185`).
  - `WasmIndexedDbStorage`: an optional built-in IndexedDB backend, default db `@automerge/subduction/db`, object stores `sedimentree_ids`, `commits`, `fragments` (`SUB/sedimentree_wasm/src/storage/idb.rs:36-58`), enabled by the `idb` feature (`SUB/subduction_wasm/Cargo.toml:91`).
  - `MemoryStorage` for tests and examples (`SUB/README.md:223-230`).
  - No OPFS. grep for `opfs` found none in `SUB` sources.
- Documented assumptions: durability is "browser-dependent" for IndexedDB; per-commit atomicity is required (`SUB/design/assumptions.md:25-52`).

### keyhive

- No browser persistence in-tree. Keyhive is persisted as an `Archive` (serde) the app stores (`KH/keyhive_core/src/archive.rs:1-30`); the wasm README shows `CiphertextStore.newInMemory()` (`KH/keyhive_wasm/README.md:23`).
- Subduction's `KeyhiveStorage<Async: FutureForm>` persists archives by hash: `save_archive`, `load_archives`, `delete_archive` (`SUB/subduction_keyhive/src/storage.rs:79-109`). Same trait shape as `Storage`.

### our `sqlite_rs`

Read from source:

- `src/sqlite_rs/Cargo.toml`: `rusqlite`, `libsqlite3-sys`, `sqlite-vec`, `tokio`, `tempfile` and `utils_rs` are all under `cfg(not(target_arch = "wasm32"))`. Only `thiserror`, `serde` and `serde_json` are unconditional.
- `lib.rs:8-11` and `:19-48`: the `native` module (`Database`, `SqliteRuntime`, `Tx`, migrations) is `cfg(not(target_arch = "wasm32"))`. The portable module (`SqlValue`, `Row`, `ResultSet`, `SqlError`) and `query.rs` (`QueryExecutor`, `Executor`, `SqliteBackend`, `Sql`, `BackendRef`) build everywhere.
- `guardrails.toml:6-9` declares builds `native` and `wasm32-wasip3` only. There is no `[builds.wasm32-unknown-unknown]` contract.
- Inference: the `wasm32` cfg covers `wasm32-unknown-unknown` too, so the portable layer should compile there, with no executable backend. Not built.
- The only wasm backend is `impl_wit_sqlite_backend!` over generated `townframe:sqlite` WIT resources (`src/sqlite_rs/query.rs:387-393`, used by guests such as `src/plug_test_routines/lib.rs:46-48`). The host side is `src/wash_plugin_sqlite/lib.rs`, which serves the same WIT interface from native `rusqlite` over wasmtime.
- Native threading: `Job = Box<dyn FnOnce() + Send>`, an `std::sync::mpsc` worker pool and `std::thread` (`native.rs:310,799-824`), with `tokio::sync::oneshot` replies. This is not portable to wasm32-unknown-unknown as written.

`Send` bounds on the API:

- `IntoParams: Send` (`query.rs:10`); `QueryExecutor: Sync` returning `impl Future + Send` (`:141-155`); `SqliteBackend: Sync` with `+ Send` futures (`:303-319`).
- The WIT macro hard-codes `+ Send` on its returned futures (`:393,456,524`).
- So a browser backend whose connection holds a `JsValue`, a `web_sys` handle or a worker `MessagePort` cannot implement `SqliteBackend` as declared, without `SendWrapper` around the handle (inference).

What a browser SQLite would need (all inference; nothing in this repo references OPFS, wa-sqlite or sqlite-wasm; grep over `docs/` and `src/` found none):

- A wasm SQLite build, for example the official `sqlite-wasm` or `wa-sqlite`, with a persistent VFS.
- Persistence options:
  - OPFS with a sync access handle. `FileSystemSyncAccessHandle` exists only in dedicated workers (platform fact, not read from this repo). That forces SQLite to live in a dedicated worker and the rest of the app to reach it by message passing.
  - IndexedDB-backed VFS, which is slower but available on the main thread.
  - The `sqlite-vec` extension would need to be compiled into that wasm build.
- Therefore the browser SQLite handle is an RPC client (`postMessage`/`MessagePort` to a worker), not a local connection. Its futures await a `oneshot` from an `onmessage` closure, holding `Closure`/`Rc` state. That is naturally `!Send` unless wrapped.

## 5. Other crates

| Crate | Used by | Notes |
|---|---|---|
| `wasm-bindgen-futures` | subduction (`spawn_local`, `JsFuture`: `SUB/subduction_wasm/Cargo.toml:44`), keyhive wasm, n0-future (wasm target dep), iroh (`iroh/Cargo.toml:415`), our workspace pin `0.4.77` (`TF/../Cargo.toml:351`) | The wasm executor: `spawn_local` and `JsFuture` |
| `futures` / `futures-util` | subduction core+wasm, keyhive (`futures::lock::Mutex`, `ciphertext.rs:14`), `future_form` itself (`BoxFuture`, `LocalBoxFuture`) | `Abortable`, `oneshot`, `LocalBoxFuture` |
| `async-lock` | subduction (`SUB/Cargo.toml:55`, `subduction_core/Cargo.toml:17`, wasm crates) | Runtime-agnostic mutex/RwLock; works on wasm |
| `send_wrapper` | n0-future (wasm only, `Cargo.toml:87`), `ws_stream_wasm` (`ws_stream.rs:27`) | The `Send`-wrapper used by iroh's stack. Not used directly by subduction or keyhive sources |
| `web-time` | n0-future (wasm), noq (`noq/Cargo.toml:336`), iroh (`time` with `wasm-bindgen` feature, `iroh/Cargo.toml:411-413`), our workspace pin `1.1` (`TF/../Cargo.toml:352`) | `Instant`/`SystemTime` for wasm; n0-future re-exports it |
| `gloo-timers` | transitive only. It is in `SUB/Cargo.lock` via `backon` and similar; no direct dep in subduction/keyhive/n0-future (inference from lockfile) | Subduction implements its own `JsTimeout` over `globalThis.setTimeout` (`SUB/subduction_wasm/src/transport/longpoll.rs:30-36`) |
| `tokio` (wasm) | subduction core optional for tests (`subduction_core/Cargo.toml:32`); iroh native only (`iroh/Cargo.toml:437`); n0-future no-feature on wasm | Only `sync`/`macros`/`io-util` are wasm-safe in this stack |
| `ws_stream_wasm` | iroh-relay on wasm (`iroh-relay/Cargo.toml:401`) | Wraps `web_sys::WebSocket` in `SendWrapper` |
| `js-sys` / `web-sys` | all wasm crates | `Window`/`WorkerGlobalScope` handled by hand in subduction |

Our workspace already has `n0-future = "0.3.2"`, `wasm-bindgen-futures`, `web-time` (`TF/../Cargo.toml:351,352,377`). `service_core_rs` and `service_client_iroh_rs` already use `n0_future::{task::spawn, time::{sleep, timeout, Instant, SystemTime}}` (`TF/service_core_rs/context.rs:13-14,187`; `TF/service_client_iroh_rs/lib.rs:10,196-212,340`).

## Our current shape (for reference)

- `big_automata` is FutureForm-dual: `RevSource<Async: FutureForm>` ("`Sendable` for multi-threaded runtimes, `Local` for the single-threaded web node", `TF/big_automata/rev.rs:85-96`), `#[future_form(Sendable, Local)]` impls (`rev.rs:351-495`, `okv/memory.rs:410`), and `future_form` + `futures` in `Cargo.toml:42-45`. It has a `[builds.wasm32-unknown-unknown]` contract.
- `big_repo/runtime2` abstracts spawn and timers behind traits: `TaskRuntime<F>`, `TaskSet<F>`, `Timer<F>`, with a tokio-backed `Sendable` impl (`TF/big_repo/runtime2/tasks.rs:8-10,17,45,59-91`). The module doc says a wasm implementation could use `spawn_local` "without imposing `Send` or Tokio types on the actors". It does not exist yet.
- `big_sync/worker.rs:599` and `wflow_tokio/partition/{effect_worker,reducer}.rs` call `tokio::spawn` directly.
- `sqlite_rs` is `Send`-bound (section 4).

## Implications for us

These are recommendations, not decisions.

### 1. FutureForm-dual or Send-everywhere?

Recommend a split, not one answer for the whole repo.

- **Send-everywhere for shared domain code**, with `n0_future` as the runtime. This covers `big_sync`, `big_repo`, the service stack and `wflow`. The iroh and n0 stack already runs that way in the browser, and our service stack already follows it: `service_client_iroh_rs` compiles for `wasm32-unknown-unknown` using `n0_future::task::spawn`. Reasons:
  - We own all implementors of our traits. FutureForm's benefit (open set of third-party `!Send` implementors, as in subduction's JS-injected storage and signer) does not apply to us.
  - The `K` parameter, boxing and `#[future_form]` expansion are real recurring costs. `big_automata` already shows it (`Async: FutureForm` threaded through every trait).
  - On wasm the single-threaded event loop means `Send` is never exercised at runtime. The only cost of staying `Send` is wrapping the few JS handles.
- **Keep FutureForm only where we face subduction/keyhive.** `big_automata`'s `RevSource` and similar traits that wrap or are consumed by `subduction_core`/`keyhive_core` (`Storage<F>`, `AsyncSigner<F>`, `KeyhiveStorage<F>`) must stay generic over `F`, because those APIs are. Our adapter implementing `Storage<Local>` for browser can hold `!Send` state; `Storage<Sendable>` stays for native. This is the boundary where `Local` is forced, and it is where the fork pins already live.
- **Open question:** whether `big_automata` should stay dual. If its only reason is the web node and the web node's `!Send` part is solely the SQLite/OPFS client, then one `SendWrapper` around that client could let `big_automata` go `Send`-only. That depends on how many other `!Send` handles the web node ends up holding (storage, `MessagePort` transports, WebCrypto). I did not audit `big_automata`'s impls for that.

### 2. Where `!Send` is unavoidable

- **Browser SQLite.** It is an RPC client to a dedicated worker (OPFS sync access handles) or an IndexedDB-VFS connection. It holds `MessagePort`/`Closure`/`Rc`. It cannot satisfy `SqliteBackend: Sync` + `impl Future + Send` as written (`query.rs:303-319`). Options:
  - (a) Wrap the handle in `SendWrapper` (iroh's approach) and keep the traits `Send`. This is cheapest, and valid under the single-thread-per-instance assumption.
  - (b) Make `SqliteBackend` generic over `FutureForm`, which touches every query call site (`impl_wit_sqlite_backend!`, `native.rs`, generated queries).
  - Recommend (a) unless the SQLite client also needs `!Sync` interior state, in which case `SendWrapper` also needs `Sync` (it provides it with the same single-thread caveat).
- **WebCrypto signing (`CryptoKey`), JS callbacks, IndexedDB handles, `MessagePort`.** All `!Send`. Wrap them once at the adapter edge.
- **Topology caveat.** If the SQLite worker is a separate dedicated worker, the main-thread code and the worker are different wasm instances (or the worker is plain JS), and `SendWrapper` is never shared across them. If we ever run two wasm instances sharing memory, `SendWrapper` use would panic (`task.rs:249-258`). Pin down the browser topology before choosing (a).

### 3. Runtime surface

Recommend `n0_future` directly, with no new thin re-export crate.

- It already compiles on both targets and is already a workspace dependency used by the service stack. AGENTS.md also warns against symbol proliferation, and a pass-through crate would be exactly that.
- Use `n0_future::task::{spawn, JoinSet, AbortOnDropHandle}`, `n0_future::time::{sleep, timeout, interval, Instant, SystemTime}`, `n0_future::boxed::BoxFuture` (note: this one flips to `Local` on wasm; use `n0_future::future::Boxed` when a `Send` box is wanted on both), `n0_future::MaybeFuture`.
- Keep the existing `TaskRuntime`/`Timer` trait seam in `big_repo/runtime2` only if tests need a deterministic runtime. A tokio backend and an n0_future backend would both be `Sendable`.
- Direct `tokio::spawn` in `big_sync/worker.rs:599` and `wflow_tokio` would move to `n0_future::task::spawn`. Native behaviour is identical (re-export). On wasm it becomes `spawn_local`.
- Native-only crates (`wflow_tokio` host loops, `sqlite_rs::native`, wasmtime hosts) can stay on tokio. Only code that must also build for `wasm32-unknown-unknown` needs n0_future.
- Limits to document for contributors:
  - No `block_on` and no `LocalSet` in the browser.
  - tokio features on wasm must stay `sync`/`macros`/`io-util`, never `rt`/`net`/`time`/`fs`.
  - Handles from n0_future are `Send` only via `SendWrapper`, so do not move them across Web Workers.
  - Detached `JoinHandle` drop semantics differ slightly between the tokio and wasm shims; I read the wasm shim but did not compare them line by line.

### 4. Risks to flag

- `big_automata` builds for `wasm32-unknown-unknown` but `sqlite_rs` has no such build contract (`guardrails.toml:6-9`).
- Our `big_repo/runtime2/tasks.rs` module doc promises a wasm task backend that does not exist yet (`tasks.rs:8-10`).
- The `townframe:sqlite` WIT path (wasip3 guests) and a browser-worker SQLite path are two different backends. Only the first exists today.
