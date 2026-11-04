# Clay design-settling PR — working ledger

Status: Living ledger. Decisions are marked as decided; the in/out list is a proposal awaiting the operator.
Owner: Claude (manager) with Sonnet subagents; operator reviews.
Scope: Branch `feat/better-collections`, the `clay_*` / `micro_*` crates and ADRs 017/023/024/025.
Updated: 2026-11-03 (C0 found incomplete; C1 extended to finish the scope migration)

Normative design: ADRs 017, 023, 024, 025. `clay-architecture-critique.md` is history and the
findings index (F*, Q*, W*). This file is the live state of the PR: what is in scope, what is
decided, what is running, what is open. Update it whenever a checkpoint lands or a decision is made.

## Goal and boundaries

- Settle every open Clay design problem so the next PR can build real micros.
- Clay runs independently on mock data. **No node / big_sync / big_cabinet integration in this PR.**
- `../townframe-p3N-2` (`feat/nodules`) holds `big_plugs_core` and `big_nodule`. Read them only to
  keep the enablement/config *shape* aligned. Nothing here depends on them landing.
- Existing clay code is disposable. Rewrite rather than shim (AGENTS.md "Breaking changes").

## Decisions taken in this PR (beyond the ADR text)

- 2026-11-03: Publication kinds are Call and Presentation only. Refs are bound or lazy; every use
  is async; mount is sync (ADR 025 §§5–8). Calls are served by one exported
  `call(publication, request)` dispatched by name, not by per-publication exported resources.
- 2026-11-03: Micros are singletons; the micro id is the only identity (ADR 024 §1).
- 2026-11-03: Durable backing is an ordered KV store (prefix scan plus atomic batch): SQLite native,
  IndexedDB browser. `wflow_core::KvStore` is unsuitable (it has no scan or batch).
- 2026-11-03: Collections are independent consistency domains. Per-collection atomic commit, reader
  wakes deferred until every affected collection commits, no cross-collection epoch (ADR 017 §6).

## Checkpoints

| # | Item | State |
|---|---|---|
| C0 | Recover gen-34 B (`9f19a2fb`, scope-owned registration rows) | was broken; completed by C1 |
| C1 | Collapse `_deferred` (dispatch carries output `T`); migrate `clay_guest_session` + engines onto scopes | committed `e22f80b0`; 191/191 (clay_collection, clay_guest_session, clay_rt_wasmtime, clay_rt_wcmp_wasmi), clippy clean. Scopes close at teardown **after** `instance-root.close()` (ADR 024 §6), not at Closing |
| C2 | Lifecycle core: evolve `clay_host::Supervisor` (see "Two controllers" below) | committed `b2f380df` (134/134, guardrails clean) |
| C2b | Move MDK + shells (`dayframe_composition`, desktop, web) onto the Supervisor; delete `clay_micro_core` | committed (see "C2b done"); web Playwright spec updated, not yet run |
| C3a | Host side, no WIT: `Send` activation bridge in `clay_host`; bank refs Bound/Lazy; Call + Presentation publication kinds; in-process tests | committed `a7df27a1` (92/92 host side; 7 engine tests stubbed until C3b/C3c) |
| C3b | WIT reshape + Wasmtime adapter + lifecycle fixture (call + presentation publications, lazy consumer); owned `ContractIdentity` + per-call contract; drain queued activations when `drive` drops | committed `27a07a83` (123/123; 6 wcmp_wasmi tests stubbed until C3c) |
| C3d | Calls as store tasks (lock per burst); serialized per incarnation; waits-for graph refuses cycles (`call-cycle`); forced close mid-call skips guest close | committed `9e87066b` (129/129, 20x loop clean). Fallback baseline; superseded by C3e if the stream spike passes |
| C3e-spike | Prove `serve(inbox: stream<request{publication,payload,responder}>)` on Wasmtime + WCMP: concurrency, sync entry, nested await, drain-on-close | committed `44bbacd2`: all 5 points pass on Wasmtime + WCMP(wasmi), no fork changes |
| C3e | Replace per-call `call` export + `instance-root.close()` with the serve inbox; MDK owns the loop; drop serialization (keep waits-for graph only if still needed) | committed `e1044a06` (243/243 host+session+wasmtime+collection; call/close conformance 20x clean) |
| C3f | WCMP fork: store-task `spawn` (Wasmtime-like JoinHandle), branch `store-spawn` on dman-os | pushed `dman-os/store-spawn` @ `2a3de028` (7 new fork tests; fork Nix lanes broken by pre-existing wasm-bindgen 0.2.127 hash gap) |
| C3c | Repin WCMP to `store-spawn` + port onto the C3e model; check WCMP host-task starvation under guest `thread.yield` | agent running (fresh) |
| C4a | `clay_store`: `OrderedStore` trait (get / scan_prefix / atomic `apply(Vec<BatchOp>)`), `MemoryStore`, `SqliteStore` (sqlite_rs, one `clay_store_entry` table), `IndexedDbStore` (compile-only) | committed `bc52dd23` (17/17, clippy native+wasm clean) |
| C4b | Durable backing in `clay_collection`: scope id = Incarnation(counter) or Installation(durable key); async durable constructor loads before existing; privileged whole-scope `install`/`uninstall` (persist first, then project); incarnation scopes never persisted | committed `d67ff053` (128/128 clay_collection+clay_store; verified by me, agent report lost) |
| C4c | IndexedDB exercised in a browser (dayframe_web e2e: install, reload, rows present) | after C2b (needs a building web shell) |

**App crates are red** since `3e55cbee` ("narrow collections waist", before this session): it
deleted `clay_host::{HostedCollection, CollectionReader}`, which `clay_micro_core`, native `clay_mdk`,
`micro_settings*` and the `dayframe_*` shells still use. Decision: don't patch, because C2b deletes the
old controller and C6 rewrites the MDK collection API. Verify sets exclude those crates until then.
Wasm fixtures: `deno run --allow-all x/check-dayframe.ts --prepare-only`, then export the printed
`DAYFRAME_*` vars. `wit/deps` in a fresh worktree come from `deno run --allow-all x/wit-fetch.ts`.
| C5 | Manifest/catalog admission + Clay enablement/config service on mock storage, shaped like big_nodule | after C4 |
| C6 | Authoring foundation: one semantic `Context` (native + guest), OCI-owned manifest, drop `build.rs`/custom section, multi-presentation | after C3 |
| C7 | Validation micros: toolbar → completion → layout, exercising ADR 025 validation list | last |

## Two controllers (found 2026-11-03)

- `clay_micro_core::micros::{composition, CompositionBroker, MicroActivation}` is the **old**
  controller and the one the shipped app uses: `clay_mdk::{native,wasm,wcmp}`,
  `dayframe_composition`, `dayframe_desktop`, `dayframe_web/shell.rs`. Its model is pre-ADR-025:
  declarations plus `accept`/`accept_all`, and typed Rust `Provision` (`Any`) publications.
- `clay_host::Supervisor` (`src/clay_host/supervisor.rs`, gen-33 A) is the **ADR 024** controller.
  It is engine-neutral, built on `Factory`/`Root`, has strong requirement `Binding`s, staged init,
  and ordered close. Only `clay_guest_session/lifecycle_conformance.rs` uses it.
- Decision: evolve the Supervisor (C2) and delete the old controller (C2b). C2 deltas:
  - `Root::provision(contract)` becomes named publications returned at init (ADR 025 §8).
  - Remove `ActivationRegistration`/`RegistrationTemplate`/`install`/`uninstall`. Durable rows
    replace them (C4/C5).
  - Demand API: start-or-join one pending activation per micro, returning a strong lease. Dropping
    the last external lease triggers idle close; an in-flight call holds a temporary lease.
  - `status()` without activation; demand during `Closing` waits for the next incarnation.
- C2 brief (exact deltas, from reading `supervisor.rs`):
  1. Delete `accepts`, `registration_grants`, `routes`, `RegistrationTemplate`, `ActivationRegistration`,
     `RegistrationError`, `install`/`uninstall`, and the `Requirement.provider: None` path.
     A requirement becomes `{ provider: MicroId, publication: String }`, resolved by configuration.
  2. `Factory::initialize` returns `Initialized<P> { root: Box<dyn Root>, publications: BTreeMap<String, P> }`.
     `Root` keeps only `close`. `provision(identity, contract)` becomes `publication(micro, name)`.
     A requirement whose publication is missing → `Unavailable`; the provider stays up.
  3. `MicroId` newtype moves into `clay_host` (the old one dies with `clay_micro_core::micros` in C2b).
  4. New external strong edge: `acquire(micro, publication) -> Result<Lease<P>, StartError>`
     starts or joins the activation (existing `Starting`/`Initializing` + `Notify` join). `Lease` derefs to `P`.
     Retention count = consumer requirement bindings + live leases. At 0 and not eager → idle close.
     `Lease::drop` is sync, so it enqueues; a `Supervisor::drive()` future (spawned local by the host,
     `.unwrap()` at the top) runs idle closes. No timers yet (idle grace is deferred).
  5. Close order: mark `Closing` (new acquires wait via the existing `AwaitClosed`) → `root.close()` with
     dependencies held → drop the incarnation's publications (revokes them) → instance teardown
     (removes incarnation registration scopes) → release providers. Publications are dropped **before**
     scope removal (ADR 017 §6 fail-closed argument).
  6. Replace the `#[allow]` at `supervisor.rs:510`.
  7. Tests (`supervisor/tests.rs`): N concurrent acquires → one `initialize`; last lease drop → `close`
     exactly once; eager root survives lease churn; acquire during `Closing` → fresh incarnation;
     faulted not retried by demand, only by `retry`; unknown publication → `Unavailable`, incarnation
     stays; acquire resolves only after `initialize` returns (staged visibility); cycle → `Cycle`.
  8. Port `clay_guest_session/lifecycle_conformance.rs` and both engines' lifecycle tests off
     registrations: the "contributor" scenario becomes a requirement plus lease scenario. Lazy rows
     come in C3/C4.
- Implication for F6: typed native `Provision`s go away. Micro-to-micro behavior is only Call or
  Presentation over CBOR Lexicons, so Settings becomes portable by construction. Native-only
  platform micros keep an escape hatch for platform crates, not for micro-to-micro types.

## Open findings from the critique: proposed disposition (operator to confirm)

In scope (design-settling):
- F5/W3/Q5: configuration and boot composition model. The composition root names no micro; drop
  demo controls. Covered by C5.
- F6/W5/Q9: portable Settings and the isomorphic MDK. Covered by C6.
- F7/F10/W6/Q8: package identity by content digest; dedupe `clay_mdk::{wasm,wcmp}` and the
  `clay_rt_*` adapters. Identity is C5 (catalog); dedupe falls out of C3/C6.
- ADR 017 validation #3: one causal shared-row contribution collection. Design is in ADR 017 §10, no
  code. Needs an in-scope decision.

Proposed out of scope:
- ADR 017 validation #4: node-backed projection with host-owned optimism. Excluded (no node);
  ADR 017 stays Proposed on that item alone.
- W7 `dayframe_app` / mobile shell; W8 browser isolation; W9 hop-cost measurement.
- F8 (cross-hop ids): superseded by the ADR 017 rewrite. Close it.

## Open questions for the operator

- Is the causal writer (ADR 017 §10) part of settling the design here, or deferred with the node work?
- Confirm the in/out list above.

## Guardrails state

Pre-existing failures from `85835823 wip`: fixed `clay_guest_session` `test-utils` (native build
contract) and the `clay_mdk/manifest.rs` inline test size (moved to `manifest_tests.rs`). Still open:
`#[allow]` at `lifecycle_conformance.rs:92` (assigned to the C1 agent) and `clay_host/supervisor.rs:510`
(C2 rewrites it). `clay_store` items belong to the C4a agent. Note that `wit/deps` dirs are
git-ignored; C1 copied them from `../townframe-p3N-3` to build `wasm_utils_rs`. How to generate them
in a fresh checkout: TBD.

## Agent log

- C1: Sonnet/medium, collapsing `_deferred` methods in `clay_collection::registration` and updating
  callers. Verify: clippy over clay crates, nextest `clay_collection` + `clay_guest_session`.
- C4a: Sonnet/medium, new `src/clay_store` crate: `OrderedKv` trait (get / scan_prefix / atomic
  batch with DeletePrefix), in-memory + `sqlite_rs` + IndexedDB (compile-only), shared contract
  tests. Browser test of IndexedDB is deferred to a dayframe_web e2e in C4b.

## C3 design (2026-11-03)

Constraint found: engine store state must be `Send`. `WasiView: Send`, and `WasmStoreState` holds the
`CollectionSession`/`CapabilitySession`, which are why the bank is `Arc`/`Mutex`. The Supervisor is
`Rc`/`RefCell` on the local executor. So a guest host import that redeems a lazy ref cannot hold
the Supervisor directly.

- **Activation bridge (clay_host).** `Supervisor::activator() -> Activator<P>`: a `Clone + Send + Sync`
  handle holding an mpsc sender. `Activator::acquire(micro, publication).await` sends a request with
  a oneshot reply. `drive()` serves those requests concurrently (a `FuturesUnordered` inside the loop,
  so one slow activation doesn't block idle closes) alongside the idle queue. `Lease<P>` becomes `Send`:
  `Arc<AtomicUsize>` retention and an `Arc` idle queue with a tokio `Notify`. `Activator::status(micro)`
  reads an `Arc<Mutex<BTreeMap<MicroId, MicroState>>>` mirror that `Supervisor::set` updates, so
  status never activates and never round-trips. Channel lifetime: the supervisor outlives every
  incarnation (shutdown closes them first), so a failed send is an invariant break and panics.
- **Bank refs (clay_guest_session::capabilities).** `CapabilityRef` targets `Bound(publication)` or
  `Lazy { micro, publication, contract, activator }`. Methods: `status()`, `call(request).await`,
  `retain_call().await -> CallBinding`, `redeem_presentation().await -> PresentationComponent`.
  Lazy use = `activator.acquire` → `Lease<CapabilityPublication>` → a proxy that owns the lease
  (strong). `call` holds the lease only for the call. Bound use never activates; it is `revoked`
  after its incarnation closes. Contract checks happen at redemption, as today.
- **Publication kinds.** `PublicationInner` carries `kind: Call | Presentation` and its name.
  Presentation keeps today's `ComponentRef` path. Call needs a cross-incarnation call path: the
  engine registers a `CallTarget` (a `Send` handle into the provider incarnation's driver, which runs
  the guest's exported `call(publication, request)`) when it publishes. In C3a, test with an
  in-process `CallTarget`.
- **WIT (C3b).** `capabilities.wit` per ADR 024 §9; `micro-lifecycle.initialized { root,
  publications: list<named-publication> }` replaces `provisions`; export
  `call(publication, request)`; `presentation-events.mount` gains the publication name.
  `publish-presentation-component()` becomes `publish-presentation(name)` plus `publish-call(name)`.
- **Lazy minting from rows.** `CapabilitySession::mint_lazy(endpoint)` exists in C3a. Projection
  minting for durable rows is wired in C5 together with manifest admission.

## C3a notes carried forward
- Lazy `RefStatus` mapping: Active→Live; Idle, Initializing, Closing→Dormant; Blocked or unknown→Unavailable;
  Disabled→Disabled; Faulted→Faulted. A bound ref is Revoked once its publication or recipient closes.
- A cancelled requester (reply send fails) drops the lease, which queues an idle check, rather than
  panicking. That is accepted.
- `Activator::status` mirror is updated only in `Supervisor::set`, so it may show Active for an
  incarnation that ended outside supervision until the supervisor next observes it. Accepted
  (status is a snapshot).

## C3b decisions (2026-11-03)
- WIT package is 0.4.0 (breaking: sync `as-presentation-component` removed).
- Declared publications reach the engine via `InitialContext.publications` from the caller. C5 derives
  them from the manifest.
- Call errors: `capability(capability-error)` for bank failures and `provider(string)` for handler
  errors. A guest trap during a call is `provider(..)` and closes the incarnation. A provider gone
  mid-call is `Revoked`, since only a forced disable can cause it.
- MDK micros publish nothing by name until C6. That is intended.

## Guest concurrency finding (2026-11-04)
- On `wasm32-wasip3`, wit-bindgen (0.62, unchanged in 0.63) stores the export-task pointer in a libc
  TLS (`std`) or the `wasip3_context` hook. Our wasi-libc v34 does not swap it per CM task, so a
  second async export task (`call`, `initialize`, `instance-root.close`) entering while one is
  suspended asserts in `start_task`. Wasmtime itself allows the re-entry. Upstream LLVM PR #175800
  moves wasip3 SP/TLS into CM `context.get/set`; whether it is merged, and which rustc/wasi-sdk ship it,
  is unverified.
- Baseline committed (C3d): serialize, plus a waits-for graph.
- Proposed to the operator: an **inbox model**. One long-lived async `serve(inbox: stream<call-request>)`
  export started after `initialize`; requests are spawned inside that single task; closing the inbox =
  close (it replaces `instance-root.close()`). This gives concurrent calls and A→B→A today, with no toolchain
  bump. Requires proving `stream<record>` on our wit-bindgen/Wasmtime pins.

## Writes / materializer (2026-11-04)
- Registration scopes are the RAII contributor sets: incarnation-scope rows end with the incarnation;
  installation-scope rows end with uninstall.
- Missing: ADR 017 §9 host **materializer** with an explicit merge policy (union / grouping / priority /
  host-ordered LWW / reject collisions). Readers get the raw per-scope union today. Proposed for this PR.
- ADR 017 §10 field-level causal writer: proposed deferred until a workload needs it (operator to confirm).
- Out of scope (operator agreed 2026-11-04): node-backed optimism, `dayframe_app`/mobile, browser
  isolation, browser hop-cost measurement.

## Decision 2026-11-04: guest inbox model (operator approved)
The guest is an actor with a long-lived main loop: `serve(inbox)` runs for the incarnation, handlers run
concurrently inside it, and closing the inbox is close. This is the design, not only a workaround:
wit-bindgen-spawned work is scoped to its export task, so GUI and background work needs a long-lived
task anyway. Per-call exports may be added back once the wasip3 TLS gap is fixed upstream. The MDK
hides the loop (authors write `async fn` handlers). Costs we own: per-request cancellation signal,
backpressure, and per-call attribution. Risk to retire first: `stream<T>` support on the WCMP polyfill.

## 2026-11-04: materializer reverted; collection model under discussion
- C8 (merge policies inside `RegistrationCollection`) was reverted in `e9817a3b`. Collections are plain maps;
  union/priority/group are N collections or reader-side queries. See memory `collections-are-plain-maps`.
- Proposed model (operator discussing): a retractable LWW map. Each live scope holds its latest write per
  key; readers see the highest host-sequence write among live scopes; scope close reveals the next. The host's
  global order supplies the causal order (observed context = one sequence number). Values are written whole,
  and structure goes in keys.
- Operator: no per-key "withdraw contribution" for guests yet, so `remove` asserts absence.
- Open: two retention classes ("survives the writer" and "goes with the writer"), orthogonal to durability.
  Does the collection owner or the consumer decide?

## C3e provisional decisions (operator has not ruled)
In-flight limit 256 per incarnation (overridable via `InitialContext`); a cancelled handler's future is dropped
(no cancel token); orderly close waits for handlers without bound; a forced close with requests in flight
abandons `serve`; a guest trap during `serve` means callers see `Revoked`.

## TLS hook finding (2026-11-04, under investigation)
`clay_mdk/lib.rs` overrides `__wasilibc_task_hook` so every CM task shares one TLS block. This may be the
real cause of the "one suspended async export" abort that ADR 024 §7a attributes to wasi-libc. An agent is
verifying with a hook-free guest. Do not rely on the §7a TLS rationale until it reports.

## TLS hook finding: resolved (2026-11-04)
- Root cause of the "one suspended async export" abort is **our** MDK `__wasilibc_task_hook` (shared TLS,
  needed so Dioxus/MDK thread-locals survive from `activate` into `mount`) combined with wit-bindgen 0.62
  p3+`std` keeping task state in a `thread_local!`. Stock wasi-libc v34 is per-task TLS and works.
  ADR 024 §7a's toolchain rationale is **wrong**.
- Verified fix on Wasmtime 04bb391, in a scratch crate:
  - wit-bindgen with `default-features = false`. `std` is on today through the workspace dep's default
    features, reached via clay_mdk, micro_collection_probe and wasm_utils_rs.
  - The MDK defines `wit_bindgen_0_62_0_{get,set}_task_context` (a `CURRENT` pointer).
  - The hook parks the task state in CM context slot 0 on AsyncBlock and restores it on AsyncResume,
    exactly as stock v34 parks the TLS base.
  - Result: overlapping async exports complete out of order, and sync exports still see shared TLS.
- Consequence: calls can be ordinary reentrant async exports, one CM task per call. The inbox loop
  (`e1044a06`) and the C3d serialization (`9e87066b`) are unjustified and should be reverted pending
  operator go-ahead.
- Untested: WCMP / browser JSPI keeping slot 0 per task; cancelled parked tasks leaking TaskState.
  The symbol names embed the wit-bindgen version, so add a guardrail that `std` stays off on guest targets.
- C3c partial work (repin + inbox port) is uncommitted in the tree; the inbox part is now moot.

## Collection write semantics: operator correction (2026-11-04)
- Not CAS. Every write carries what it was based on (its Lamport timestamp) and is **always accepted**.
  Per key, the winning write has the highest Lamport timestamp: a write that observed another wins over it
  regardless of arrival order. Equal timestamps (concurrent writes) use a deterministic tie-break. CAS may be
  offered as an opt-in on top.
- Retraction: a dead scope's writes leave the candidate set, and the winner is recomputed by (timestamp,
  tie-break) over the rest.
- "Plain map" was an over-simplification: a collection has one fixed causal merge semantics, with no
  pluggable policies.
- Open: what timestamp a write with no `based_on` gets. Proposed: the MDK fills it from the writer's
  replica of that key.

## Blocker hunt (2026-11-04, running)
Two isolated investigations, both in scratch crates outside the repo:
1. Wasmtime: real Dioxus under overlapping async exports, cancellation, background work after return and
   cooperative threads, reentrancy, close with calls in flight, backpressure.
2. WCMP wasmi + browser JSPI: context slot 0 per task, overlap, starvation, cancellation.
Reverts of `e1044a06`/`9e87066b` wait for these results and operator go-ahead.
- Decided 2026-11-04: at the waist, a write with no `based_on` is a **concurrent creation**. The MDK
  defaults `based_on` to the writer's replica of that key, so an ordinary write supersedes everything
  its writer has observed. That matches the intuition from non-causal LWW structures, while writes the
  writer never saw are still resolved by (Lamport timestamp, tie-break).
- WCMP/browser result: per-call callback-ABI async exports work on WCMP wasmi and headless Chromium 156
  (JSPI engine). Slot 0 is per task (`Thread.context` in `concurrency/thread.rs:41`), overlap of 2 and 50
  calls works, and sync entry is fine. No blocker found for per-call exports.
  - Hazards (medium): a continuously self-yielding guest starves host tasks (`run_turn`,
    `store_context.rs:1097-1205`); wit-bindgen 0.62 yields only via `yield_async` or self-waking futures.
  - No host-side cancel of an export call: dropping the call future cancels nothing, and `JoinHandle::abort`
    only drops the host future, so the guest task runs to completion. Same as Wasmtime at that version, per
    PDD023.
  - Unproven: a real wit-bindgen + MDK-hook guest (the test was hand-written WAT emulating the protocol), the
    guest-side TASK_CANCELLED path, stackful/JSPI-blocking guests, non-Chromium browsers.
- Wasmtime result: no blocker forces a custom loop. Per-call async exports work with real Dioxus (out-of-order
  overlap, sync dispatch interleaved), background work after return (`spawn_local`), reentrancy, 2000
  overlapping calls, and concurrent close.
  1. (high, must configure) Guest wit-bindgen `default-features=false`, features `async, macros, realloc,
     async-spawn, inter-task-wakeup`. Without `inter-task-wakeup`, a cross-task wake panics the instance. Needs
     a guardrail that `std` stays off and that the versioned task-context symbols match.
  2. (medium) No host-initiated cancel of a call in this Wasmtime pin (#11833 open) or in WCMP. Guest-to-guest
     `subtask.cancel` works and does not leak (5001 cycles). Host-side cancel needs a guest-visible token.
  3. (medium) A guest trap poisons the instance; suspended calls never resolve. The host must fail
     outstanding calls itself.
  4. (low-med) Dropping a queued (unstarted) call future traps the store, and backpressure also blocks sync
     exports. Admission control belongs in the host.
  5. (low) A host import must not re-enter the same instance on its first poll; yield or spawn first.
  - Author rules: no RefCell/signal/Dioxus guards across `.await`; Dioxus global APIs after resume need
    `dom.in_runtime`; panics kill the instance.
  - Cooperative threads exist in Wasmtime behind a Config flag, but wit-bindgen 0.62 doesn't wrap
    thread.new/switch.

## Collection model decided (2026-11-04, operator approved)

- One causal map per collection. Per logical key, each live writer scope holds at most one candidate
  (a value or a tombstone) stamped `(lamport, writer id)`. Readers see the winner: the highest stamp.
- Stamp: `lamport = based_on.lamport + 1`, or `1` with no `based_on` (concurrent creation). Writer id is
  a stable, ordered identity, so the tie-break survives restarts. The MDK defaults `based_on` to the
  writer's replica of the key.
- CAS is opt-in: the write fails unless the current winner is exactly the expected stamp (or absent).
- Retention is the writer's scope, not the collection's or the consumer's: incarnation (dies on close),
  installation (dies on uninstall), permanent (never retracted, needs a manifest grant). Scope death
  retracts its candidates, tombstones included, and recomputes only the touched keys.
- A deleting micro that goes away un-deletes: accepted consequence of scope-owned tombstones.
- No `clear` and no per-key withdraw for guests. Write authority is a key-prefix grant; it replaces
  ADR 024 §13 `writer_policy`. Grants live in the host layer, not in `clay_collection`.
- Plan: C9a core rework in `clay_collection` (worktree agent), C9b consumers + WIT + grants + MDK
  `based_on` default after the per-call exports agent lands, C9c ADR 017 §9–11 / 024 §13 / 025 §4.

## C9a status (2026-11-04)

- Done on worktree branch `c9a-causal-map` (c9271e59, a86a32ae; 123/123, clippy clean, verified by
  manager). Not merged: it breaks consumers; merge after the WCMP port lands, then C9b.
- Installed scopes have an async `InstalledWriter`; durable writes serialize per key across the store
  await, sync incarnation writes that slip in make a durable CAS fail and the store row is restored.
- `install` replaces a scope wholesale. Supervisor rule for C9b/C5: manifest-declared rows and runtime
  writes use separate installation scope keys per micro, so an upgrade's reinstall never drops runtime
  rows (settings); uninstall retracts both.
- Open with the operator: how a writer names an observed tombstone. Readers never see tombstone stamps,
  so the MDK `based_on` default treats a re-creation after a seen delete as concurrent and it loses.
  Proposed (b): writes carry the writer's feed position; the host derives the observed lamport.

## Per-call exports landed (2026-11-04)

- Wasmtime 404cdd21, ADR 024 §7a 13b89a49, WCMP repin 1ebb05a7 (fork 7604dc8: `run_until_stalled`
  reports `Stalled::{Idle, Yielded}`; a yielded task starved later tasks), WCMP port a34f643e. Both
  engines run the same 15 conformance scenarios; 294/294 across the clay crates.
- Open: browser (JSPI) lane untested for per-call exports, and `Resuming` still maps to `Idle`; the
  fork's own nix lint/test lanes not yet run on 7604dc8; the guest cancel path is unexercised by host
  tests (no engine can cancel a guest task from the host).
- C9b (consumer migration, WIT stamps/basis, prefix grants, MDK default) running on `c9a-causal-map`.

## Tombstones replicate as rows (decided 2026-11-04)

- Operator: tombstones carry their stamp like any row. The registration projection value is
  `Stamped<Option<V>>`; a winning tombstone publishes an Upsert of `None`, so snapshots and pages carry
  it. `Remove` only when a key has no candidates left. The MDK replica keeps tombstone stamps (hidden
  from app reads) and defaults `based_on` to them, so re-creating after an observed delete wins.
- Kept per-key Lamport stamps. The operator notes a per-collection counter or the feed-position basis
  (option b) resembles big_sync core's keyed frontiers; expect these abstractions to collapse into one
  implementation later. Tombstone GC deferred (scope death retracts; permanent tombstones accumulate).

## C9 done (2026-11-04)

- C9a/C9b merged (e8ded252..10cae0f3), C9c ADRs b7ed9112. 309/309 across the clay crates, clippy,
  guardrails lint/api clean. Supersedes the earlier ledger notes on the ADR 017 §9 materializer and
  the §10 causal writer: both are gone; §10 is now "field-level merges are layers above the map".
- Grants: `RegistrationGrant { micro, prefixes }` enforced in `clay_guest_session` (`forbidden`); test
  mint sites pass a full grant until C5 manifest admission narrows it. Nothing mints installation
  scopes yet, so the manifest/runtime two-scope rule lands with C5.
- Tombstones count toward `rows_per_writer` (they are scope state). A micro's bindings share one
  WriterId; equal stamps between them break by scope id.
- Not compiled: native `clay_mdk` tests (known red from the `HostedCollection` removal, fixed in C2b).

## WCMP fork nix lanes (2026-11-04)

- The fork's devshell is broken since 687e486 (wasm-bindgen pinned to 0.2.127 with no
  `wasmBindgenHashes."0.2.127"` entry in flake.nix). Bootstrapping the hashes builds wasm-bindgen-cli
  through Nix and filled `/` (/tmp) on this machine. Deferred; 7604dc8 verified with plain cargo test only.

## C2b done (2026-11-04)

- Row attachments are bound publications or endpoints `(micro, publication, contract)`; durable rows
  store endpoints only; readers mint lazy refs through the session's `Activator`, which
  `Factory::initialize` now receives. Manifest contracts are `ContractIdentity` (namespace, name,
  version); `ContractRef` is gone.
- Manifest rows become installable via `native::declared_rows` (JSON payload, `{"$attachment": n}`
  stands for the attachment tag; attachments become endpoints to the package's own publications).
  The shell installs them into `<micro>/manifest`; `<micro>/runtime` is reserved for runtime writes.
- Shell: `dayframe_composition::CompositionRuntime` (catalog = manifest, eager, locked, requirements,
  factory), `shell.roots` collection, `serve()` runs `drive()` + eager start + ordered shutdown inside
  a `LocalSet`. Lifecycle panel keeps enable/disable/retry. Dropped: eager/enable toggles,
  preference-driven startup, accept/reject controls, the Wasm fuel-trap e2e flow.
- `clay_micro_core` deleted. `dayframe_web` still needs `--config resolver.feature-unification="selected"`
  for the wasm32 lane: `utils_rs` dev-deps pull tokio `full`/`net` into the unified graph.
