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
| C2 | Lifecycle core: evolve `clay_host::Supervisor` (see "Two controllers" below) | agent running (fresh) |
| C2b | Move MDK + shells (`dayframe_composition`, desktop, web) onto the Supervisor; delete `clay_micro_core::micros` composition/broker | after C2 |
| C3 | WIT + both engines: bound/lazy `capability-ref`, Call/Presentation publications, `initialized.publications`, async redemption | after C2 |
| C4a | `clay_store`: `OrderedStore` trait (get / scan_prefix / atomic `apply(Vec<BatchOp>)`), `MemoryStore`, `SqliteStore` (sqlite_rs, one `clay_store_entry` table), `IndexedDbStore` (compile-only) | committed `bc52dd23` (17/17, clippy native+wasm clean) |
| C4b | Durable backing in `clay_collection`: scope id = Incarnation(counter) or Installation(durable key); async durable constructor loads before existing; privileged whole-scope `install`/`uninstall` (persist first, then project); incarnation scopes never persisted | agent running (store agent reused) |
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
