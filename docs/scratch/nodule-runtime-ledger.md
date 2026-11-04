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

### D2b. Manifest shape decisions (manager, 2026-11-04, from D1/D2)
- `Nodule.routines` is replaced by `handler: Option<Handler{artifact}>` plus `implements: Vec<Nsid>`. Commands, views and processors moved into the opaque `bindings: IndexMap<tag, Value>`, which is never validated and round-trips.
- `implements` may name **foreign** operations. That is provider composability, e.g. a search nodule implementing `org.example.search.query`.
  - A local NSID must be a declared operation in the nodule's own Lexicons.
  - A foreign NSID is only syntax-checked at admission. The contract is resolved at bind time, and dispatch reports the service unavailable until it is known.
  - `Admitted.operations: BTreeMap<Nsid, Option<OperationKind>>`, where `None` means foreign.
- An NSID in both `requires` and `implements` is refused, so the runtime dependency graph has no self-edges.
- A store `source` names an implemented operation; absent means the host writes the store. Initializer targets are implemented or required operations, and never a known subscription.
- Workflows have no manifest section until the wflow service PR.

### D2c. Dispatch slice 1 decisions (manager, 2026-11-04)
- The router entry state is `ProviderState::{Ready(ProviderKind), NotReady(Readiness)}`, so a not-ready provider is uncallable by construction.
- A nodule registers its `implements` with its own `InstalledNodule::readiness()`, which is `ArtifactsUnavailable` today; there is no hardcoded reason.
- A **disabled nodule is unbound**, not registered `NotReady(Disabled)`. Disabling is how an operator resolves two providers of one NSID (`NotReady(Ambiguous)`). Cost: callers can't tell "disabled" from "unknown".
- `Denied` is decided before lookup, so callers can't probe which operations exist. Authority prefixes match whole NSID segments.
- `InvocationContext.deadline_unix_ms` keeps the wall-clock form used by `RequestContext`.

### D2d. Slice 2 (Wasm provider) design (manager, 2026-11-04). The operator should review the retention choice.
- **Artifact retention via `Blob` datums in the node config document**, the existing drawer-native retention model (ADR 026). No new big_blobs pin API.
  - Portable: a mirror's config doc carries the references, so blob discovery can fetch the bytes.
  - The blob workers must also watch the config drawer (today only `present`/`originated`).
  - `big_plugs_core::Digest` and `BlobId` are the same BLAKE3 32-byte, `z`+base58btc format; no mapping is needed.
  - Known big_blobs gap, not ours: an `import_stream` staging claim isn't persistable, so a crash between import and adopt leaks an `upload:` claim.
- **The Wasm host lives in a new crate, `big_nodule_wasm`** (`role = "runtime"`), wired by big_node.
  - This keeps wasmtime/iroh out of `big_nodule` and keeps the provider behind a boundary that could move out of process (D6).
  - The host loads handler bytes from big_blobs (in-process `BlobService` store reader; `ReadBlob` is capped at 1 MiB) and reports readiness: artifacts missing vs ready.
  - `big_nodule` gets an **executor seam**. The reconciler asks the executor to ensure or retire a nodule revision's workload and registers whatever provider state it returns.
- **Workloads:** one wash workload per nodule revision. `pool_size` 0 means the `Ephemeral` policy, a fresh store per request, which matches the one-instance-per-dispatch rule. `host_interfaces = [wasi:http/handler@0.3.0]`. A custom `Router` plus `OutgoingHandler` handle `dispatch.local`.
- **Header contract (guest ↔ host):**
  - `authorization: Capability <cap-id>`;
  - `idempotency-key: <key>` (the IETF Idempotency-Key draft);
  - `x-request-deadline-unix-ms: <ms>`.
  The guest SDK forwards the deadline and idempotency headers on nested calls.
- **Test guest:** `src/test_nodule_guest` in the `src/wasm_utils_rs` workspace. It is a p3 `wasi:http/handler@0.3.0` component built with plain `cargo build --target wasm32-wasip3` and wit-bindgen (btress_sysadmin's async-export config), and consumed like `test_wflows` (a path under `$CARGO_TARGET_DIR/wasm32-wasip3/release/`, plus a CI build line).
- **Sub-slices:**
  - 2a: config-doc artifact `Blob` datums, blob workers on the config drawer, and the executor seam with a fake executor in tests.
  - 2b: `big_nodule_wasm`, the fork `[patch]`, the test guest and the e2e.
  - 2c: `dispatch.local` egress.

### D2e. Slice 2c: nodule-to-nodule calls (manager, 2026-11-04)
- **Grants:** `NoduleGrants` gains `operations`, a set of NSID authority prefixes (the same shape as `DispatchScope`). The nodule service mints one dispatch capability per nodule actor from it, delegated from its own operator-granted dispatch authority via `MintDispatchCapability`. It is applied, revoked and reconciled exactly like drawer grants (`applied_grants`): on grant change, disable, upgrade and uninstall.
- **The guest learns its own capability per request:** each delivered request carries `x-nodule-capability: Capability <id>`, the nodule's own operations cap (absent if none is granted). The caller's `authorization` belongs to the caller, so the nodule cannot reuse it, which prevents a confused deputy.
- **Egress:** `allow_outgoing_request` admits only `dispatch.local`, and `NoEgress` becomes the dispatch egress. `POST http://dispatch.local/xrpc/<nsid>` with `authorization` naming a cap. The caller identity is the workload's nodule actor (from the workload id, which is unforgeable). Dispatch checks that the cap's holder is that actor, then admits and invokes as usual. Deadline and idempotency headers pass through. Dispatch errors map back to XRPC statuses (the inverse of the provider mapping).
- No other egress, ever, in this PR.

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

- Operator principle (2026-11-04): settle runtime and dispatch design questions by following established reverse proxies and RPC systems (gRPC, Envoy, wash, XRPC), and name the precedent.
- Operator (2026-11-04): the fork push, repin and `[patch]` cleanup are deferred until later; don't block on them. The manager makes design calls and reports them.
- Next: init as a lifecycle binding (to-do 9).
## Done

- 2026-11-04: Slice 2c (D2e) plus gRPC deadline fail-fast.
  - `NoduleGrants.operations` (`OperationGrants` datum record, arrays; `DispatchScope` uses `BTreeSet` with `deny_unknown_fields`, which datums reject). `SetNoduleOperationGrants` op, `NoduleRecord.operations` status, migration `002_applied_operations`.
  - The nodule service's dispatch authority is minted by the operator at boot with scope `big_node::Config.nodule_operations` (the ceiling) and retracted at shutdown. Each nodule cap is minted from it, bound to the nodule actor.
  - Holder check: `Invocation.bound_signer`. Dispatch refuses a scoped handle presented by anyone but the actor it was minted for. Before this, dispatch handles were bearer tokens, so a callee could replay its caller's cap.
  - Wasm egress admits only `dispatch.local`. The signer comes from the workload id, and the guest supplies only the cap id. `x-nodule-capability` is delivered per request and updated in place on regrant. One `DispatchError` to HTTP mapping (403/404/503/400/504, provider passthrough, 502 for backend faults).
  - Reference cycle (dispatch, router, provider, ingress, egress, dispatch client): broken by the state actor unbinding all registrations at shutdown.
  - Manager review fix: a call from a workload stopped mid-request answers 503 `WorkloadStopped` instead of panicking the node, and the signer is dropped only after `workload_stop`.
  - Verified: clippy clean, 57 nodule tests, big_cabinet 399/399 in isolation (the agent's drawer-model timeouts were load).
- 2026-11-04: Slice 2b.
  - New `big_nodule_wasm` crate: one wash `Host`; the ingress router (`Sealed`) refuses all incoming routes and egress is denied (`NoEgress`) until 2c. Workload id `nodule/<plug>/<revision>`, `pool_size: 0` (ephemeral instance per request). Handler bytes come from `BlobService::read_all` (64 MiB cap), with a BLAKE3 digest `assert`.
  - Late bytes: `ensure` registers a waiter before its `stat`, and a task follows `open_availability` from the head read at boot (`BlobService::availability_revision`), so nothing polls and boot does not replay the announcement history (manager fix on review).
  - XRPC mapping: `POST http://nodule.local/xrpc/<nsid>`, `authorization: Capability <id>`, `idempotency-key`, `x-request-deadline-unix-ms`. 2xx JSON is the output, 400 is `InvalidInput`, other statuses are `Failed{status, body}`, transport errors are 502. Response cap 4 MiB.
  - `Provider::call` takes the operation NSID. Shutdown order: nodules, then wasm, then dispatch.
  - wash-runtime comes from a `file://` git `[patch]` to `../wasmCloud` @ `e7648c6e` (a path patch loses `--cap-lints allow` and trips `recursion_depth_exceeding_limit`). **CI cannot resolve it until the operator pushes `townframe-changes` and the rev is repinned.**
  - Dropped: the e2e assertion that an already-elapsed deadline gives `DeadlineExceeded` (a fast provider wins the first poll). The gated-provider dispatch e2e still covers deadlines.
- 2026-11-04: Slice 2a.
  - Handler artifacts are referenced by `Blob` datums in the config doc, keyed `org.example.daybook.blob/nodule.artifact/<plug>/<artifactKey>`. They're owned by `NoduleConfig` only under that slot prefix and written atomically with the install datum. Retention counts only top-level `Blob`/`CipherBlob` datums.
  - Blob workers also poll `config_id`, with Write, because they write cipherBlob/jwk representations into the doc.
  - Executor seam: `big_nodule/executor.rs`, `NoduleExecutor { kind, ensure(HandlerSpec) -> Hosted, retire }`.
    - The `Reensure` signal is a `Mutex<BTreeSet>` plus a `Notify`, not a channel, so it can't fail or block and stray requests are no-ops.
    - `ProviderKind::Executor { kind, provider }`.
  - Readiness now has `Ready`; `ArtifactState` has `Available`. Record readiness equals dispatch readiness.
  - Admission refuses a handler artifact without `size` (`ArtifactSizeRequired`). The manager accepted this, because `Blob.lengthOctets` is required.
  - Manager verified 53/53, plus clippy, fmt, lint and api.

- 2026-11-04: Dispatch follow-ups.
  - Authority actor-row cache in `AuthorityService`. Rows are insert-only (the manager verified there are no UPDATE/DELETE queries and no writers outside `authority/`), so no invalidation is needed. Liveness stays in the in-memory ledger, absence is never cached, and there's no TTL.
  - `Provider::call` receives the verified `Caller { authorization, actor, scope }`.
  - The NSID-prefix check is unified as `Nodule::authority_owns`.
  - Manager verified 79/79 targeted tests (authority, nodule crates, big_node minus blob_discovery), plus clippy, fmt, lint and api.
  - Open: should providers also see the handle's minting (originating) actor?

- 2026-11-04: Test guest `test_nodule_guest` (`d808dc42`).
  - A p3 `wasi:http/handler@0.3.0` component (1.4 MB release) with routes `org.example.test.echo` and `.relay` (outgoing to `dispatch.local`).
  - Imports `wasi:http/client@0.3.0` plus wasi cli/clocks plumbing, which the host must link.
  - Path: `$CARGO_TARGET_DIR/wasm32-wasip3/release/test_nodule_guest.wasm`. Built in CI after `test_wflows`. `wit/deps` is fetched by `x/wit-fetch.ts` from the crate's `wkg.lock`.

- 2026-11-04: Dispatch slice 1.
  - `big_nodule_core::dispatch` contract; `big_nodule::dispatch` service, with a `RwLock<Arc<Table>>` router and a reverse index.
  - Ambiguity is reported as `NotReady(Ambiguous)`; the `BuiltIn` provider; deadline via `tokio::time::timeout`.
  - Nodule `implements` are registered from the reconciler. big_node boots dispatch before nodules, and dispatch has its own actor and namespace.
  - Manager verified: 48/48, clippy, fmt, lint and api clean.

- 2026-11-04: Batch 3, admission narrowed and manifest reshaped (D2b). Manager verified: clippy clean, 35/35, lint and api clean.

- 2026-11-04: Review fixes batch 2.
  - Upgrade wedge fixed. An upgrade that invalidates settings is admitted and refused with the existing reasons. The settings datum is kept `Accepted`, because `diff` refuses to write `Invalid` slots. `configure` against the new revision repairs it.
  - Reconcile perf:
    - `AdmissionCache` keyed by `(plug, revision)`, with a byte-equal envelope check, owned by the state actor.
    - Reads only check applied-grant capability liveness (one `InspectCapability` per applied grant) instead of a full reconcile.
    - The authority-conflict check is O(n × depth).
  - Remaining costs: `InstalledNodule` clones per resolve, settings revalidated per resolve, hydrate on every snapshot.
  - Operator questions:
    1. Should a settings-only refusal keep applied grants? Today any non-Admitted plug loses them until repaired.
    2. There's no way to clear settings for a schemaless revision except uninstall.

- 2026-11-04: wash-runtime fork seam committed in `../wasmCloud` (jj, colocated clone of `dman-os/wasmCloud`, `upstream` remote added).
  - Bookmark `townframe-changes` = `e7648c6e`, directly on the pinned `1bcaeba`; author dman-os. Not pushed.
  - Adds pub `Ingress::dispatch_request(workload_id, Request<WasiBody>)` over `dispatch_local`. Order: service first, then component; unbound workloads error; the Router is not consulted. It does not apply `MeteredBody`/`record_response_status` from the wire path.
  - The integration test `test_dispatch_request_delivers_in_process` covers p2+p3 fixtures in-process and the unknown-id case; 15/15 in `integration_http_local_routing`.
  - Gotchas:
    - Build that fork with `RUSTUP_TOOLCHAIN=1.96.0`. The nix nightly 1.100 trips the fork workspace's deny-warnings on a recursion lint at the base rev (not our change). As a git dependency, Cargo caps the dep's lints, so townframe is unaffected.
    - Build fixtures with `cargo xtask build-fixtures <names>`.
  - Working copy parked on an empty "scratch" change above the bookmark.
  - **Operator action:** push `townframe-changes` to `dman-os/wasmCloud`; then we repin `wash-runtime` to `e7648c6e` and drop the local `[patch]`.

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

- [main] `src/big_node/e2e/blob_discovery/mod.rs` is not rustfmt-clean on origin/main (byte-identical here), so the flake `rust-fmt` check fails on it.
- [main] The big_node `blob_discovery` e2e tests take about 95–105s alone. Under a parallel run they exceed the default nextest kill (60s × 2) and aren't named `long_test`. Pinned 2026-11-04; they don't involve nodules.

- [perf] Dispatch admission makes 2–3 in-process authority IRPC calls per invoke (`ResolveAuthority`/`ResolveActor`), each a pooled SQLite point read in the authority service. That contradicts D6 "no I/O on admission". Fix with an in-memory actor-row cache in the authority service (main's code), invalidated on actor changes. To do before the PR is review-ready.
- [slice 2] `Provider::call` currently gets only `InvocationContext`. The Wasm provider needs the **caller identity/authority** (for the authority header). Pass the verified caller into providers. Dropping the future on deadline is the cancellation path.
- [cleanup] `owns_nsid` (`big_nodule/state.rs`) and `under` (`big_nodule/dispatch.rs`) duplicate the NSID-prefix check. Unify in `big_nodule_core`.
- [perf, minor] `Router::register` rebuilds the whole table per provider. Reconcile registers per nodule, so it's O(n²) per pass. Fine at today's scale; batch registration if it grows.

- [test-time] Four heavy `big_cabinet` drawer e2e tests from main hit the 120s nextest kill under load, and one even alone:
  - `model::deterministic_drawer_model_scenarios_match_reference`
  - `lifecycle::named_metadata_creation_replays_every_boundary_on_both_roots`
  - `delegation::three_group_root_invitations_bound_transitive_links_and_revoke_both_roots`
  - `lifecycle::sensitive_root_creation_replays_every_boundary_without_replacing_groups_or_docs`

  They are not `long_test`-named. Our branch adds a 4th local drawer (config) at boot, so **compare timings against an origin/main baseline** before calling it main's.

### Slice 2c follow-ups
- [authority] The service's dispatch authority is minted per boot and retracted on clean shutdown, so nodule caps rotate on every restart (the subagent's claim that they stay stable across restarts is wrong). After a crash, the previous boot's authority is never retracted: a live orphan cap. Fix by persisting the service authority, or by sweeping at boot.
- [convention] Nested calls inherit a deadline only if the guest forwards the header. gRPC propagates deadlines through the context automatically, so the guest SDK should do the same.
- [decided] Production default for `nodule_operations` is empty (deny by default, the ocap convention). Caps stay stable across revision upgrades, because the actor is the same.
- [nit] The egress request body cap reuses `MAX_RESPONSE_OCTETS`, so give it one message-size constant.

### Slice 2b follow-ups
- Decided (operator, 2026-11-04): follow gRPC. A call whose deadline has already elapsed fails fast with the deadline error, checked first in `invoke`, and the provider is never entered. This supersedes the slice 1 "provider entered once" test contract. Folded into slice 2c.
- Decided (manager): keep retrying a failed workload start on each reconcile; revisit with the reconcile perf work.
- [limits] Handlers run with wash default resource limits; no memory/CPU caps yet.
- [test] The late-bytes e2e polls `GetNodule` every 20ms because there is no reconciled signal to subscribe to.
- [main] `big_blobs` and `big_node/e2e/blob_discovery/mod.rs` are not rustfmt-clean on main (from #77); fixed in its own commit.

### Slice 2a follow-ups
- **[design, operator] The config drawer isn't blob-portable yet.** It's a parentless self-doc drawer, and ADR 026 gives such drawers no inventories, so no inventory pin is published and a mirror can't discover or fetch artifact bytes. Retention is local only. This undercuts D2d's "portable" rationale until big_blobs supports pins or inventories for self-doc drawers (or the config drawer gets a parent).
- [2b] Handler bytes must already be in the local blob store before the workers can encrypt and retain them. The install path (operator `PutBlob`/import, or OCI import) must put them there.
- [blobs] Removing a `Blob` datum leaves its worker-written cipherBlob/jwk datums in the doc. Retention is released, but the datums linger. That's big_blobs worker cleanup.
- Done (2026-11-04): the watch reconcile is skipped at already-applied heads. Trade-off: a failed grant apply at unchanged heads is retried only by `refresh_if_needed` and the dead-cap check, not by the watch. Its test uses 200 `yield_now`s to settle, which can only false-pass, never flake; there is no "watch event handled" signal to wait on.
- Done (2026-11-04): a handler-less enabled nodule is `Ready` and registers nothing.
- [main] `src/big_node/e2e/blobs/metadata.rs` isn't referenced by any `mod`, so it's dead test code from main.

## Deferred

wflow service and durable procedures; POSIX providers; triage/processors; big_tasks; network/XRPC exposure; consumers of command/view bindings; single-instance services; generalising `NoduleGrants` to operation grants.

## Open questions for the operator

- The seven questions at the end of `docs/scratch/nodule-lifecycle-review.md` (catalog role, dependency split, manifest reshape, init redesign, downgrades, retired prefixes, carried drawer-grant authority).
- Live `SchemaRepo` registration of the nodule's own datum Lexicons. Nodules can't write their own datums without it. Is it in scope for this PR?

- Commit the in-progress decisions to ADR 027 directly, or in a separate "revision" section first?
- Integer-format width: merge chose "bounds ⊆ format range; narrow formats require bounds". Confirm.
