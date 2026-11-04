# Nodule runtime and dispatch ledger

Status: Active
Owner: Townframe maintainers
Updated: 2026-11-04
Scope: Working ledger for the `feat/nodules` PR: decisions made with the operator, work in flight, to-dos, and questions awaiting review. Read this first after any context compaction, together with AGENTS.md and ADR 027.

## How to use this ledger

- Append decisions as they are made, with the date. Never silently rewrite one; mark it superseded instead.
- Keep **In flight** and **To do** current whenever work starts, finishes or is handed to a subagent.
- Decisions here feed ADR 027 (`docs/adrs/027-plugs-nodules-and-operation-contracts.md`, formerly 023) and the config scratch doc (`docs/scratch/nodule-config-datum-store.md`). The ADR is the normative record; this ledger is the working log.

## Decisions (2026-11-04, operator design session)

### D0. Branch state and process
- `feat/nodules` holds the config-drawer/nodule-reconciler work as commit `57a7de13` "wip". It does nothing to execute nodules; readiness stops at `ExecutorUnavailable`.
- `origin/main` (big_blobs, PR #77) is being merged in. Main removed `ObserveDatums` in favour of the long-poll `PollDatums` (main's ADR 023 "cursor consumption is long-poll", ADR 025), so `big_cabinet/datum_store.rs` is ported to `PollDatums`.
- Our ADR is renumbered 023 → 027 because of main's ADR 023.
- Grunt work goes to Sonnet medium-effort subagents, which are reused but retired before about 400k tokens. The manager verifies their diffs and reruns targeted checks.
- Main's code is fresh WIP and may be critiqued and improved where we touch it.

### D1. The runtime models services, facilities and lifecycle; everything else is a binding
- The runtime understands:
  - **Services:** operation NSIDs (query | procedure | subscription) plus their providers.
  - **Facilities:** granted resources such as private state, revisioned stores, network, artifacts and drawers.
  - **Lifecycle:** install, enable, upgrade, retire, and init.
- **Bindings** are consumer-owned metadata naming an operation NSID: commands (GUI/CLI/agents), views (Clay), processors (triage, deferred). The runtime carries them as open tagged manifest sections and does not read or validate them.
- **Initialization** is the one runtime-owned binding: "on revision R, invoke operation X, run mode M". Prior art is `daybook_core/rt/init.rs`.
- Cross-nodule calls invoke **operations**, not commands.

### D2. Dispatch is the runtime's XRPC reverse proxy
- **Placement:** the contract lives in `big_nodule_core` (the service's home); the host lives in `big_nodule`. The name stays "dispatch".
- **Behaviour:**
  - Stateless per call.
  - Does ocap admission, checking the presented capability against live caps (ADR 022).
  - Resolves NSID → provider and gates on readiness, then forwards.
  - Knows provider type and operation kind, because some capabilities are granted by type. Type-dependent grants are checked at bind time, modelled on wash-runtime `BindingSchema`/`narrows`.
- **Routing table:** modelled on wash-runtime `Router`/`DynamicRouter`. A sync snapshot lookup with a reverse index per provider. The reconciler drives registration; unbind is idempotent.
- **Provider kinds:** `BuiltIn` (typed service_utils client, no JSON) and `Wasm` (wash-runtime) now. `Posix`, the wflow service and `Remote` come later.
- **Guest model:**
  - A nodule is a wash workload with one HTTP-shaped XRPC handler component serving `/xrpc/<nsid>`.
  - The semantics are our own and aligned with service_utils_rs.
  - `wasi:http` p3 is the first transport; we may swap to our own WIT export of the same shape.
  - Lifecycle follows wash's.
- **Instances and authority:**
  - **Authority is a header.** The proxy checks it, and it becomes the dispatch's authority.
  - **One fresh instance per dispatch** (no pooling across dispatches). The store's host context is the request context.
- **Nodule-to-nodule calls:**
  - The guest makes an outgoing request to a reserved authority (`dispatch.local`).
  - Our `Router::route_local_egress` declines it, so it lands in our `OutgoingHandler`, which receives the workload id. The workload id maps to the nodule actor.
  - Dispatch admission then runs as usual. The header names the capability, and the host checks that the capability belongs to the actor.
- **Context headers:** deadline and idempotency key travel as headers. The guest SDK forwards them.
- **wash-runtime seam:** no public in-process entry exists (checked at the pinned `1bcaeba` and at upstream main `3de4e69`). Add pub `Ingress::dispatch_request(workload_id, Request<WasiBody>) -> Response<WasiBody>` over the private `resolve_local_target` + `dispatch_local` in our fork `dman-os/wasmCloud`.
  - The local checkout lives at `../wasmCloud` (jj), with a `[patch]` while local.
  - The operator pushes the fork, and then we repin. Follow `.agents/skills/fork-pinning-and-upstream-sync`.

### D2a. Dispatch contract sketch (manager's working proposal, 2026-11-04)
- **Wire contract:** `big_nodule_core::dispatch`, following the `protocol.rs` idiom: typed operation structs carrying an `Authorization`, declared with `declare_service_protocol`.
  - `org.example.daybook.dispatch.invoke`: `{ authorization, operation: Nsid, input: serde_json::Value, context: { deadline?, idempotency_key? } }` → `{ output: Value }`.
  - `org.example.daybook.dispatch.mintCapability`: `{ authorization, scope: DispatchScope { operations: set of NSIDs or authority prefixes }, rights }`. The ocap attenuation follows `MintNoduleCapability`.
  - `DispatchError`:
    - `Denied`;
    - `UnknownOperation`;
    - `NotReady { reason }` (retryable; reasons mirror per-service readiness);
    - `InvalidInput` (Lexicon edge validation);
    - `DeadlineExceeded`;
    - `Provider { status, body }` (the provider's own XRPC error).
- **Who calls dispatch:** NSID routing is *dynamic*: guests, XRPC ingress, the operator/CLI by NSID, and later tasks. In-process native callers of built-in services keep using typed service_utils clients with their capabilities directly and don't go through dispatch. So dispatch's JSON `Value` is the edge it would pay anyway.
- **Host-side (not wire):**
  - A `Router` snapshot `Nsid → ProviderEntry { kind, provider_id, operation_kind, readiness }`, with a reverse index by provider.
  - `register(provider_id, entries)` and idempotent `unbind(provider_id)`, driven by the nodule reconciler.
  - `ProviderKind::{BuiltIn(dyn handler: Value→Value), Wasm { workload_id }}`.
  - Admission: verify the capability (scope covers the NSID), then readiness, then forward.
  - For `Wasm`: build `POST /xrpc/<nsid>` (GET for queries) with authority, deadline and idempotency headers, then call `Ingress::dispatch_request`.

### D3. Durability via the wflow service (deferred)
- Durable work goes through a wflow **service** with start / status long-poll / message / cancel operations; it is not a bespoke routine kind.
- A durable procedure returns a job reference.
- **Convention:** durable document mutations default to wflow. The journal is the storage, and components stay stateless.
- **Pinning moves to providers:** a call resolves the current enabled revision for its duration, and wflow freezes its execution at start. This supersedes ADR §5's "dispatch pins".
- wflow support for nodules waits for a separate PR after the wflow rework in `../townframe-p3N` lands. Raise wflow design issues there.

### D4. Authority is ocap throughout
- Dispatch owns an operations namespace.
- The operator delegates to the nodule service, which mints per-actor "may invoke NSID" caps from config datums, as with drawer grants. `NoduleGrants` gets an operations section later.
- The design must not hard-code drawer-only grants.
- Readiness is per service/provider, not per nodule.

### D5. Scope relative to big_tasks
- Only triage/processors (distributed, and per-node via triage) wait on big_tasks.
- The big_tasks executor will later hand dispatch/wflow an already captured invocation. Tasks decide *which node*; dispatch decides *whether/how locally*.

### D6. Physical considerations
- No I/O on admission.
- Validate against the Lexicon at the edges only.
- No durable write per non-durable call, and no mirrors of terminal records.
- Keep the provider behind a service boundary so the Wasm host can move out of process.
- Guest long-poll subscriptions are an exception; host-served revisioned stores are preferred. Single-instance services come later.

## In flight

- Review fixes batch 2 (next): the upgrade wedge (accept the upgrade and mark settings `Invalid`), then reconcile perf. After that, admission narrowing (D1), then the dispatch work.
- wash-runtime fork seam (subagent) in `../wasmCloud`, a jj clone of `dman-os/wasmCloud`:
  - bookmark `townframe-changes`, based on the pinned `1bcaeba`;
  - adds pub `Ingress::dispatch_request` with a test;
  - not pushed.

## Done

- 2026-11-04: Review fixes batch 1:
  - Reconcile and watch failures now crash the state actor loudly instead of silently.
  - `Inventory` removed, with its unique tests ported to `NoduleConfig`.
  - `resolve_entry` reads the nodule id from the nodule contribution only.
  - `admit` is scoped to `scope.plugs`.
  - Open question for the operator: should admit and manage scopes be independent sets?

- 2026-11-04: ADR 027 revised with D1–D6 (`01ea305b`). Review proposals are listed under "Proposed, pending operator review" (§10).

- 2026-11-04: Merged `origin/main` as `a5f43a91`. The manager reran clippy on 8 crates (clean) and nextest on 6 crates (754/754).
  - datum_store was ported to PollDatums. A fresh page triggers one current read; the old read at enter-heads could roll state back (1 in 22 flake, now 60/60).
  - Added `Predicate::sole_doc` and filtering in `doc/observe.rs` `page_events` (a change to main's code).
  - Narrow integer formats now bound the primary inside their range; absent bounds tighten to the range. This deviates from "reject missing bounds" because macro primaries omit `maximum`. Operator to confirm.
  - Migration checksums are blake3 keyed by path, and `005` was correct.
  - Pinned with evidence: the `unused workspace dependency axum-extra` manifest warning comes from main's root `Cargo.toml:223`. Only the separate `btress_sysadmin`/`wasm_utils_rs` workspaces use it. It is not from the merge.

- 2026-11-04: Plugs/config/lifecycle design review written: `docs/scratch/nodule-lifecycle-review.md`. It has 7 questions for the operator. The "Fix (intended)" items there are pre-approved by direction (D1–D4) and the AGENTS rules. Its *(proposal)* items wait for the operator.

## To do (this PR)

2. Apply the review's intended fixes:
   - Narrow admission to services, facilities and lifecycle, with bindings carried opaquely.
   - Two-level readiness.
   - Upgrade-wedge fix: accept the upgrade with an `Invalid` settings slot.
   - Stop swallowing reconcile errors (`state.rs:216-218`).
   - Reconcile perf: cache admissions per `(plug, revision)`, reconcile only changed keys, serve reads from the last records.
   - Remove `inventory::Inventory`.
   - `resolve_entry` filters to the nodule contribution.
   - Scope `NoduleScope.admit` per plug.
4. Dispatch service contract in `big_nodule_core`: invocation, admission errors, provider kinds and readiness.
5. Dispatch host in `big_nodule`: routing table, admission, `BuiltIn` provider.
6. wash-runtime fork seam (`Ingress::dispatch_request`) in `../wasmCloud`, with a `[patch]`. **The operator must push it before CI can resolve.**
7. Nodule artifacts fetched and pinned through big_blobs, so readiness moves past `ExecutorUnavailable`.
8. `Wasm` provider: a wash workload per nodule revision, an instance per dispatch, the authority header, and the `dispatch.local` egress via `OutgoingHandler`.
9. Init as a lifecycle binding (run modes per `rt/init.rs`).
10. End-to-end test: an operator invokes a nodule operation, and a nodule calls another nodule's operation under caps (allowed and denied).

### Follow-ups found during the merge
- [perf] `DatumStoreHandle`'s `seen_head_hashes` is a set of every head ever seen and grows without bound. It came over from the pre-merge code. Bound it, for example by keeping only the current frontier/ancestry.
- [test] There is no dedicated test for `Predicate::sole_doc` page filtering. Add one in `big_cabinet` doc observe tests: a single-doc poll is not woken by writes elsewhere.
- [cleanup] Two ADRs are numbered 007 (`007-doc-branch-identity`, `007-plug-manifests-as-drawer-docs`), and lint doesn't flag it. Raise with the operator.
- [cleanup] The root `Cargo.toml` has many unused workspace dependencies flagged by cargo's `unused_workspace_dependencies` lint (`axum-extra`, `big_node`, `bon`, `clay_rt_wcmp_wasmi`, `micro_counter`, `once_cell`, `subduction_redb_storage`, `tower`, `tower-http`, `tracing-appender`, …). All come from main. Clean up if the operator agrees.

## Deferred

wflow service and durable procedures; POSIX providers; triage/processors; big_tasks; network/XRPC exposure; consumers of command/view bindings; single-instance services; generalising `NoduleGrants` to operation grants.

## Open questions for the operator

- The seven questions at the end of `docs/scratch/nodule-lifecycle-review.md` (catalog role, dependency split, manifest reshape, init redesign, downgrades, retired prefixes, carried drawer-grant authority).
- Live `SchemaRepo` registration of the nodule's own datum Lexicons. Nodules can't write their own datums without it. Is it in scope for this PR?

- Commit the in-progress decisions to ADR 027 directly, or in a separate "revision" section first?
- Integer-format width: merge chose "bounds ⊆ format range; narrow formats require bounds". Confirm.
