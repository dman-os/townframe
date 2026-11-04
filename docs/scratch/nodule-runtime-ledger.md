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

### D7. Init as a lifecycle binding (manager, 2026-11-04, by convention; operator delegated)
Answers review question 4. Precedents: Kubernetes init containers and lifecycle hooks (the kubelet runs them directly; the pod is not Ready until they succeed; callers are not queued), and Helm install/upgrade hooks (re-run per release revision).
- **Shape:** `Initializer{key, target, mode}`. `key` is a new required string, unique per nodule. `mode` keeps `Once` (once per node per revision) and `EveryStart` (each time the nodule starts: node boot, or enable).
- **Done-ness:** host-local SQLite (big_nodule migration), keyed (plug, init key, revision). Per revision, like Helm. A new revision re-runs `Once` inits, so inits must be idempotent. No synced done-ness: running it again on a mirrored node is harmless by idempotency.
- **Idempotency key:** `nodule/<plug>/<revision>/<key>` in the `InvocationContext`, so durable document mutations made by an init dedupe (D3).
- **Who invokes:**
  - A **local** target (an operation the nodule implements) is called by the runtime directly on the nodule's provider from the executor, not through dispatch. This is the host running its own lifecycle hook, like the kubelet. The `Caller` is the nodule service.
  - A **foreign** target (a `requires` operation) goes through dispatch as an ordinary call by the nodule actor under its own operations cap (D2e), so it needs a grant like any other call.
- **Gating:** `Readiness` gains `Initializing` and `InitFailed`. Dispatch registers the nodule `NotReady(Initializing)` until all its inits for the current revision and start have succeeded. Callers get the retryable NotReady (503); nothing queues.
- **Ordering:** inits run in declaration order. A foreign target whose provider is not Ready leaves the nodule `Initializing` and is retried when that provider becomes ready (the next reconcile). A dependency cycle is reported as `InitFailed` on the record, not executed.
- **Failure:** `InitFailed`, with the error and attempt count on the `NoduleRecord`. It is retried on the next reconcile trigger (boot, config change, enable). There is no hidden retry loop or backoff yet; CrashLoopBackOff-style backoff is a follow-up.
- **Execution:** init calls run in tasks off the state actor (tied to its stop token), and results come back as actor messages. The actor never awaits a handler. A stale result (from a revision or start that has since been superseded) is dropped by checking an epoch.
- **Settings** reach a nodule through a facility, not an init argument (review §4). Out of scope here.

### D8. Operator direction on plugs, config and security (2026-11-04)
- **Config is node-local.** A node's nodule/plug configuration is its own. It syncs elsewhere for durable backup, but syncing it does not install or activate anything on another node. Multi-node convergence ("these nodes should run the same plugs") is a separate layer on top. The config-drawer blob-portability concern is therefore moot. D7 init (no `PerInstall`) is approved for the same reason.
- **A plug is the one package format** for every plugin kind: nodules (services), Clay micros, schemas (Lexicons), and future kinds, under one namespace. Kinds reference each other: a micro referencing a nodule service, everything sharing schemas.
- **Package dependencies live at the plugs layer** (review Q2). A plug that uses another plug's schemas or services references that plug explicitly. Nodules and micros do not carry their own dependency graph.
- **One auditable story across all plugin kinds: manifest → configuration → capability.**
  - The manifest *requests* (operations, collections, drawers and schemas it needs).
  - Configuration is the operator's *grant*.
  - Capabilities are *minted from grants*.
  - It must be answerable statically whether plug X can ever reach a given resource (for example a sensitive drawer), including transitively through the services it may call. This is schema-bound and capability-oriented throughout. Clay's C6 "declared → configured → granted" is the same story.
- **Open:** where to cut big_plugs from the schema service. `SchemaRepo` (`big_cabinet/schema.rs`) knows only identifiers and shapes, is fixed at open, and has no live registration. Plugs bring schemas.
- Next big work: finish big_plugs (delivery: import into big_blobs, plug store, dependencies, schemas), coordinated with the Clay agent (`../townframe-p3N-4`, session `townframe-p3n-4-9c`).

### D9. Clay agent's needs from big_plugs (2026-11-04, from `townframe-p3n-4-9c`)
- **Plug store API Clay needs:** list installed plugs that carry a `micro` section; get that section plus the component bytes by digest; watch install, upgrade and remove. "Current installed revision per plug" suffices; no version history.
- **Enablement, bindings and grants stay in each runtime's own desired-state datums** (Clay: `clay.micro.install/<micro>`, `clay.micro.bindings/<micro>`; nodules: `nodule.install/<plug>`), per ADR 025 §12. Share the request/grant/necessity *types* across kinds, not the datums. The audit reads every kind's grant datums plus the envelope requests.
- **`MicroManifest`** (`clay_mdk/manifest.rs:247`) becomes the plug's `micro` section verbatim; the sidecar JSON is its standalone form. The envelope should own id, version and artifacts, and Clay will drop its `id`/`package_version`.
- **Clay names things with `ContractIdentity{namespace, name, version: semver}`, not NSIDs**, and schema refs are bare strings. It is willing to rename to NSIDs resolving against plug-declared Lexicons.
- **Micros calling nodule operations is not designed in Clay yet.** Both agents recommend: a micro `requires` entry may name a nodule operation NSID, the grant is minted like a nodule's operation grant, and it surfaces as the same call handle as a micro Call. This needs operator sign-off.
- **Schema gap:** Clay needs Lexicons looked up by **schema id (NSID)**, not by datum tag: collection key/value/selector schemas and call request/response schemas are not datum tags. The registry must grow at runtime as plugs are imported. Clay doesn't validate against Lexicons yet, so nothing blocks it now.
- **Digest mismatch:** the Clay stand-in uses SHA-256 of the component bytes; the envelope and big_blobs use BLAKE3. Align on BLAKE3.
- Stand-in loader: `clay_mdk::package` (`<name>.component.wasm` + `<name>.manifest.json`). Our importer treats it as a local source, and Clay deletes its loader afterwards.
- Read first: ADR 025 §5–§7, §11, §12; ADR 024 §11–§15 (in `../townframe-p3N-4`).

### D10. big_plugs is a registry; runtimes are its node_modules (operator, 2026-11-04)
- **big_plugs is the catalog/registry and nothing more,** like npm: it versions what comes in and upholds semver. It does no enablement. Nodules and Clay micros are the "node_modules": each runtime owns its own enablement, downgrades, grants and readiness.
- **The registry is a drawer.** A plug lives in a document with Blob datums for its artifacts. Removing a plug from the registry just drops it from that drawer.
- **Retention is by reference, not leases.** Each enabling runtime has its own drawer (nodules: the config drawer, as in slice 2a) naming the plug documents and blobs it uses, so the plug stays pinned even after the registry drops it. No lease system.
- **Downgrades** are a runtime concern and are allowed (review Q5).
- **Historical NSID reservation / prefix backstops** (review Q6): not this PR. A later layer, possibly in a schema registry.
- **Review Q7 withdrawn:** no hole. Per `docs/wiki/drawer.md`, whoever holds a drawer delegates at or below their level, and creators hold Admin.
- **Schemas:** Cabinet's `SchemaRepo` stays a doc/drawer-service concern ("identifiers and shapes"). A schema registry keyed by NSID is a layer on top. Plugs, the schema registry, Clay and nodules are all downstream of plugs. Boundary agreed.
- **NSID is the schema id everywhere, aligned with AT Protocol** (a breaking change gets a new NSID). Clay contract and schema ids become NSIDs.
- **Strict dependency rule:** referencing another plug's prefix requires declaring that plug as a dependency. Access requests name the datum NSIDs they touch. Broad untyped access ("any datum in this drawer or doc") may come later, but must be explicit in the manifest so audits can detect it. The default is least access.
- Pending: micros calling nodule operations (D9).
- **Shared request vocabulary candidate:** Clay's collection request `{binding, contract, access, necessity: required|optional, resolution: configured|at-runtime}` (Clay `af37bbcc`, `clay_mdk/manifest.rs`). Nodules today have only `requires: [nsid]`, with no binding or necessity. Settle the shared names with the Clay agent before either side hardens them.

### D11. Registry provenance, versions and dependencies (operator, 2026-11-04)
- **Provenance is the document system.** There is no global plug authority. A plug's author publishes it as a document they sign (keyhive document identity), and the registry holds *the author's own document* in our drawer. A reference to a plug points at that specific author-signed document.
- **Layout:** one document per plug, with versions as immutable entries in it, or one document per version; the operator doesn't mind. Manager pick: one document per plug, one version datum per version label. A version is pinned by (document id, Automerge heads), so a pin is immutable by construction.
- **Immutability is expected:** a version label, once seen, never changes content.
- **Each node is its own arbiter of a legal upgrade.** Seeing a new version or a valid signature does not make it the latest or a correct upgrade. The node checks upgrade legality locally on accept (immutability, version order, and later Lexicon compatibility). Any ecosystem-level policy is a layer above.
- **Dependencies are referenced by plug id + semver range** (not by document id), so OCI and other non-document substrates stay possible. Resolving and fetching referenced plugs is a later layer above the catalog.
- **The catalog holds manifests and blobs only.** Downstream consumers (nodules, Clay) refuse enablement until every dependency resolves in the catalog.
- **Import sources:** author-published documents (primary), OCI, and Clay's local wasm + manifest pair.
- **The registry drawer is not node-local.** It is an ordinary drawer that can be shared and synced across nodes (unlike config, D8). Sharing it activates nothing: each node remains its own arbiter of which versions it accepts, and enablement stays in each node's own runtime drawers.

### D12. Malleable plugs: identity, naming, forks, substitution (ACCEPTED 2026-11-04 as ADR 028; manager + Clay agent)
Goal (operator): malleable, not an app store. Anyone may modify any plug or publish new ones at the drop of a hat, but nobody can masquerade as an official upgrade. There is no global authority, and identity, resolution and verification must not be mixed: a name may indicate verification but never dictate it.
- **Plug id = `<namespace>/<name>`, both required.** The name is human-readable. The namespace is a **DID** (any method: `did:key` for raw public keys, keyhive keys included; `did:plc`, `did:web`), so we are not tied to keyhive. Identity is independent of delivery (documents, branches, OCI).
- **Readable namespaces are aliases, resolution-side only (AT Protocol DID vs handle; Zooko's triangle/petnames):** a DNS handle is verified bidirectionally (the DID doc lists the handle, DNS lists the DID) and is display/indication only. Node-local petnames are allowed. An alias never appears in durable state; only the DID-based id does.
- **Publisher = the namespace DID's key holder**, who signs each version. An upgrade is accepted only within the same plug id, and only with **monotone version order**, so a validly signed older version can't be replayed as an upgrade (downgrade attack). A downgrade is an explicit node-local runtime action.
- **Rotation depends on the DID method:** `did:key` can't rotate, so it's fine for dev and node-local publishers. Long-lived publishers should use a rotatable method (`did:plc`, `did:web`).
- **Ids never lie, so every fork is a new id:** `did:me/photos` with `basedOn: did:up/photos@v`. A fork may be a branch document per `docs/wiki/docs.md` §Branches (a separate doc with `org.example.daybook.branch` datums pinning parent and `refHeads`); there are no Automerge-internal branches.
- **Drop-in replacement = explicit node-local substitution (Cargo `[patch]`):** "references to `did:up/photos` are satisfied by `did:me/photos`". The fork's code is *installed as* the upstream id: slot state, durable rows, endpoints and enablement stay keyed by upstream, and the install datum records `filledBy: did:me/photos@v (basedOn …)`. Removing the substitution reverts the slot to upstream with nothing orphaned. Only a substitution may hold upstream's NSID prefix; two holders without one are refused at enablement.
- **Coexistence (hard fork):** a fork enabled standalone is its own slot under its own id, must rename the NSIDs it changes, and needs explicit rebinding.
- **Grants never widen silently:** a substitution or upgrade re-checks the granted request *set*. Enablement is the user's choice and survives both. A widened required request leaves the plug enabled but not startable until configured; a widened optional request stays unbound until granted (ADR 028 §3).
- **Schemas/NSIDs (AT Protocol implement-vs-own):** NSIDs stay reverse-DNS names. A fork may implement upstream NSIDs unchanged, but changing a schema means a new NSID. Prefix "authority" is the enabled plug holding it on this node; DNS verification of a prefix may later indicate, never gate.
- **Clay mapping:** a micro id is the plug id it is installed as (`MicroManifest.id` removed; `<plug id>#<micro>` only if a plug ever carries more than one micro). Endpoints embed DID-based ids. Contracts stay NSIDs, separate from plug ids.
- **Dev loop:** a local build publishes under the node's own `did:key` namespace and is used in place of upstream via substitution.
- Clay-side: `WriterId` → (replica, micro id).

### D13. Dependencies by what you use (ACCEPTED 2026-11-04 as ADR 028 §4–§5)
- **Depend on NSIDs, not plugs (OSGi `Import-Package` over `Require-Bundle`; Debian `Provides:`).** Manifest requests name operation, datum, collection and contract NSIDs. Any enabled provider satisfies them (upstream, substituting fork, reimplementation).
- **Compatibility lives in the NSID (AT Protocol):** a breaking change means a new NSID; within an NSID, only compatible growth. A feature dependency is `{nsid, minRevision}`, using the Lexicon `revision` integer.
- **Plugs vendor the Lexicons they depend on,** not just the names (Protobuf/gRPC consumers carry .proto copies). This makes an incompatible view of an NSID detectable at catalog/registry time. AT Protocol Lexicon resolution (`_lexicon` DNS → DID → published Lexicon) becomes the arbiter later; we bootstrap on AT Protocol.
- **Resolution is recorded at enable time** in the runtime install datum, like a lockfile: NSID → provider plug id@version + schema revision. A provider change is a grant change and is re-consented.
- **Strictness is at the NSID level:** every NSID a plug touches outside its own prefix must appear in its manifest requests, or admission refuses.
- **Plug-to-plug dependencies are a delivery hint only** ("also fetch did:x/photos ^1.2"), for the future fetch layer.
- **Non-NSID dependencies must resolve through the substitution table,** never by digest. A digest pin appears only in lockfile/install datums. Open list with the Clay agent: byte-level artifact reuse (plug id + artifact key), WIT interface imports (a second interface namespace), Clay-specific references.
- **NSIDs without a domain:** see the open question to the operator (unverified local NSIDs; hosted per-user namespaces like Maven Central's verified `io.github.<user>`).
- An ADR is to be written from D10–D13: plug identity, naming, dependencies and malleability.

### P. big_plugs implementation plan (ADR 028; operator-approved 2026-11-04)
Slices run in order, one subagent reused across them while under 400k tokens. Each slice is reviewed and verified by the manager, then committed.
- **P1. DID namespaces in `PlugId`.** The namespace becomes a DID (validated syntactically: `did:<method>:<id>`; methods are open, `key`/`plc`/`web` are known). The name keeps the segment rules. Pick a canonical text form that is valid in datum key slots (`nodule.install/<plug>` etc.); check the key-slot grammar, and if `:` is not allowed there, use an escaping and state why. Migrate all users (about 12 files) and tests. No aliases or handles yet (resolution-side, deferred).
- **P2. Registry service (new crate `big_plugs`, contract in `big_plugs_core`).**
  - The registry is a **grouped** drawer (it syncs, so it needs ADR 026 blob inventories), created at node boot like the nodule config drawer.
  - One document per plug id. Versions are immutable datums `plug.version/<label>` holding the envelope, with `Blob` datums per artifact.
  - Operations: publish a local version (node-published; artifact bytes already in big_blobs), list, get plug, get version, remove version or plug, and watch (long-poll per ADR 023).
  - Domain capabilities like dispatch. Immutability: rewriting an existing version label with different content is refused on publish, and a synced conflicting version is surfaced and never silently accepted (node is arbiter). Boot wiring in big_node; e2e tests.
- **P3. Publisher signatures and import.** Versions are signed by the namespace DID. Import an author-published plug document into the registry, OCI import, and the Clay local pair. Accept rules: same DID, increasing version order, immutable content.
- **P3b. Importers** (after P3a): Clay's local `.component.wasm` + `.manifest.json` pair first, then an author-published document (`AddDoc`), then an OCI image layout from disk (network pull deferred). Also drop `MicroContribution.key` and refuse a second micro per envelope (one micro per plug, like the nodule; the micro id is the installed-as plug id).
- **Micro section key (confirmed to Clay):** the contribution member `org.example.daybook.plug.micro` `{schema, description}`, where `description` is the `MicroManifest` JSON minus `id`/`package_version`.
- **P4. Shared request shape for nodules.** `Nodule.id` is removed (the envelope is authoritative). `requires` becomes `{binding, contract, access, necessity, resolution, minRevision?}`. Strict NSID admission: a foreign NSID that is used but not requested is refused.
- **P5. Enable-time resolution and lockfile.** NSID → provider plug@version + schema revision recorded in the install datum. Enablement is refused until required NSIDs resolve. A provider change is re-consented.
- **P6. Substitution.** The node-local "X is satisfied by Y" map, the installed-as slot, and `filledBy` in install datums.
- **P7. Schema registry by NSID.** Fed by catalogued plugs, comparing vendored copies. Cabinet `SchemaRepo` gains live register/retire and reads from it, which closes the gap where nodules can't write their own datums.
  - **P7 design (implemented; unreviewed).**
    - **Data model.** Plugs already vendor Lexicons: `Nodule.lexicons` carries both the files under the plug's `authority` (held) and vendored copies of others', each with an optional `revision`. No envelope field was added. The registry is derived from enabled, admitted nodules and is not stored. A datum NSID's *provider* is the plug that holds the record, since vendoring or implementing is not owning (ADR 028 §5, Debian `Provides`). Boot schemas are native providers, lockfile provider `native`.
    - **Who registers, when.** The nodule state task, during every reconcile (`sync_schemas`). An enabled, admitted nodule registers its held datum records in `SchemaRepo` under its plug id (`register(owner, group)` replaces the owner's set atomically; `retire(owner)` drops it). Disabled, refused, uninstalled or substituted-away plugs are retired. Vendored records and type libraries are carried for ref resolution, never registered by the vendor. A refused registration (tag already known, Lexicon does not compile) refuses the nodule with the repo's reason, like an authority conflict. The boot set cannot be retired or shadowed. This closes the "nodule cannot write its own datums" gap.
    - **Conflict rule** (`lexicon_conflicts`, runs in `Inventory::resolve` after authority conflicts; the same stable symmetric refusal; precedents are AT Protocol "an NSID names one evolving definition", ADR 028 §4 compatible growth, npm/OSGi ranges). Copies agree when identical, or when both declare a `revision` and the revisions differ (the lower is a compatible subset; `minRevision` decides who needs which). Otherwise they conflict: a different body at one revision, or a body with no revision to order it. The authority holder's copy is the definition: it is never refused for a vendor's copy, and a vendor that differs, or out-revises it, is refused. With no holder, every copy in a conflicting pair is refused. Nothing is ever picked silently.
    - **Lockfile.** `lexicon_revision` is the holder's declared `revision` for the NSID (None for a native or undeclared one, which `minRevision` does not hold). Data requests go through the same `RequestStatus::evaluate` as operations: `NoProvider`, `RevisionTooOld`, `ProviderChanged` (re-consent) and the lock all apply, and a required data request with no provider holds the nodule at `ProviderMissing`.
    - **Not done.** Narrowing drawer/data grants by data requests; the SchemaRepo does not backfill reference edges for a datum registered after index backfill ran (new reference-declaring tags are indexed from new changes only); `set_enabled` does not fail on a Lexicon conflict as it does on an authority conflict (the record is Refused instead); the plug registry (catalogue) is not a feed, only enabled installs are; bad datum Lexicons surface at enable-time as `Refused`, not at `admit`.
- After P2 and P4: send the Clay agent the concrete type and section names.

### D14–D20. What a node is (operator, 2026-11-04; drafted as ADR 032)
- **D14.** A node owns its data and is its sync identity. There are no checkouts. Drawers are the realms, and export is explicit. The analogy is a network filesystem.
- **D15.** The generic `big_node` knows no users. User nodes and relay nodes are compositions in their own crates.
- **D16.** Three layers: services, clients and attachments. Services and node- or Clay-side code see only `Client<Proto>` handles. Listeners (UDS, TCP) and the peer endpoint are attachments.
- **D17.** Dispatch leaves `big_nodule` for `big_dispatch`, owned by `big_node`. It is the node's one router. Built-in services register as built-in providers under their existing XRPC NSIDs, and nodules register as now. The composition root wires services directly.
- **D18.** There are three access paths: in-process, UDS with a node-issued token handshake, and the network with a capability per call.
- **D19.** Nothing assumes one node per process, machine, directory or database:
  - Nodes are values; booting and stopping many in one process is tested.
  - No process-global node state.
  - Services are handed storage bindings and never open paths.
  - Services call each other only through clients, each as its own actor.
  - Durable truth only, so a node can scale to zero.
- **D20.** `big_node` e2e is the system test arena: harness, ladder, scale, seeded stress and nodules across nodes, driven through clients like a high-level user. The progress repo is out of scope.

### Plan N (the node PR, branch `feat/big-node`)
- **N1. `big_dispatch` crate.** Move the router, admission and provider registry out of `big_nodule`. Register docs, drawers, blobs, authority, plugs and nodule-management operations as built-in providers. `big_nodule` registers its Wasm providers into it. Tests: a built-in operation and a nodule operation are reached through the one router with the same admission.
- **N2. Listeners.** Serve the node router over UDS (token handshake that yields a capability) and TCP (`service_utils_rs` already has TCP serving; add a UDS acceptor). A `Client` over XRPC/UDS drives a node end to end.
- **N3. Resource bindings.** Node config moves from paths to bindings: a SQL handle with a scope key, a blob store, secrets. `big_repo` stops opening `StorageConfig::Disk` itself. Test: boot and stop 20 nodes in one process with nothing leaked (databases, files, tasks).
- **N4. Network attachment.** Move the test-only `big_node/e2e/blob_discovery/network.rs` (Iroh endpoint, peer links, blob serving) into `big_node` as an attachment.
- **N5. Harness and ladder suite.** An N-node harness that reuses `big_repo` test2's topology and nickname helpers. Port the scenarios of `daybook_core/sync/tests/ladder.rs` onto it.
- **N6. Scale, stress and cross-node nodules.** 10k-document replication, a seeded randomized stress test (`long_test` names), and a plug published on node A then installed and invoked on node B.

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
- big_plugs: P1–P7 done. P7 follow-ups:
  - drawer/data grant narrowing by data requests;
  - reference-edge backfill for schemas registered after index backfill;
  - `set_enabled` should fail early on a Lexicon conflict (today: Refused in the resolution);
  - a bad datum Lexicon is refused at enable, not at `admit`;
  - only enabled installs feed the schema registry, not the `big_plugs` catalogue (deliberate: catalogued is not consented).
- keyhive Arc cycle (group <-> DelegationStore): worked around by detach on stop; not fixing in the fork because upstream is rewriting keyhive. Check the rewrite for the same cycle when migrating.
- Temp-dir leak: the disk-filling part is fixed (`998b73d9`). The operator stopped the residual work (2026-11-04): `big_repo`'s own suite still leaves about 16 dirs of ~1 MB per run. The unverified WIP is parked in `git stash` ("wip: big_repo residual temp-dir leak"): test stops, `TempDir` ownership in the stress fixture, `SqliteRuntime::drain_workers`. Low priority.
## Done

- 2026-11-04: P6 substitution, `eee4f9b9`.
  - The `org.example.daybook.nodule.substitution/<slot>` datum and `SetNoduleSubstitution` (manage on both X and Y). Y runs in X's slot, and `NoduleRecord.filled_by` shows it.
  - Authority conflicts are judged among enabled slots at enablement (ADR 028 §5); installing a rival no longer fails. Dependents see no provider change.
  - Manager fix: the agent rewrote stored grants on substitution, which silently discarded operator config. Now `substituted_grants` computes them at resolve: shared requests keep any covering grant, filler additions need an exact operation grant (a prefix is consent for X's code), and clearing restores exactly what was stored.
  - Data and drawer request narrowing waits for P7.
- 2026-11-04: Clay wire fixes, `11e2d155`. `Request` necessity/resolution default to required/configured and are skipped on the wire when default. `micro_pair` maps sidecar `authority` to `Plug.authority`.
- 2026-11-04: temp-dir leak fixed, `998b73d9`.
  - Root cause is a keyhive_core cycle: a `Group` holds the hive-wide `Arc<Mutex<DelegationStore>>`, which holds a delegation to `Agent::Group(Arc<Group>)`. Every object in the cycle holds a `BigRepoKeyhiveListener` clone carrying storage and database.
  - `BigRepoStopToken::stop` now detaches the listener storage, and a leaked listener used afterwards panics. Nodule and node suites went from 60 leaked dirs per run to 0.
  - The `big_repo` suite still leaves 16; the same agent is on it.
  - **Operator decision pending:** fix the cycle in our keyhive fork (a `Weak` back-reference, or a teardown clearing shared stores) plus a pin bump. That needs a fork push, like wasmCloud's. Until then, keyhive objects (small) leak per `BigRepo`. There is also a smaller `async_channel` buffered-event cycle.
- 2026-11-04: P5 resolution and lockfile.
  - The lockfile is SQLite `nodule_resolution_lock` (node-local per D8; the consent is this node's) and appears on `NoduleRecord.requests`.
  - Resolution comes from the config snapshot plus router-native providers (a provider is a string; native ones have no version or revision). Built-in providers register at boot by contract, so router registration does not trigger reconciles.
  - Disabling drops locks, and re-enabling re-resolves (enabling is consent). `ConfirmNoduleResolution` re-pins only at `ProviderChanged`. Data requests wait for P7.
- 2026-11-04: P4b `Plug.authority`. An optional NSID authority prefix on the envelope (absent = holds nothing, so only vendor and request). Held = declared under the authority. Foreign Lexicons, refs, stores and init targets must be requested or implemented. Equal or nested enabled authorities conflict (trie). `Nodule::admit(authority)`. Verified 425/425 including big_cabinet_core.
- 2026-11-04: P4 shared request shape, `6431c8aa`.
  - `big_plugs_core::request::Request{binding, contract, access, necessity, resolution, minRevision}`. An unknown necessity counts as required. `access: None` means an operation call.
  - `Nodule.id` removed. Actor path segment = `<name>-<blake3(plug id)[..32 hex]>`. Migration 004 breaks pre-existing dev nodes (accepted: unreleased).
  - Grants are bounded by requests (`NoduleOperationGrantStatus.unrequested`). A required request without a grant gives `Readiness::NeedsConfiguration`, and inits do not run. Self-calls are not expressible.
  - Also: `micro_pair` no longer requires Clay's integer `schema` (`6be1c1f9`).
  - Gap being fixed in P4b: removing `Nodule.id` also removed the NSID authority prefix, so "holds" became declaration-based and a plug could claim a vendored foreign Lexicon. P4b adds `Plug.authority` (the NSID prefix, ADR 028 §5), restores strict vendored-Lexicon checks and prefix-nesting conflicts.
- 2026-11-04: P3b importers (OCI deferred).
  - One micro per plug. `big_plugs::import::micro_pair` maps Clay `id` to the plug name under the node DID and `package_version` n to `n.0.0`, with the component as artifact `component` (`application/wasm`). Contribution schema is `org.example.clay.micro.manifest` (pending Clay confirmation).
  - `ImportPlugDocument{doc_id, source_drawer}` uses `AddDoc` at Write, with handles retracted after. The registry actor must already reach the source drawer through an ordinary operator delegation.
  - Plug names allow `.` (manager decision; npm precedent; not leading, trailing or doubled).
  - **OCI deferred (manager decision):** `cargo x plug build-oci` and the old importer carry the old daybook `PlugManifest` (no DID, semver, contributions or signature), and nothing emits new envelopes yet. Shape when built, per the OCI 1.1 artifact convention: a config blob `application/vnd.townframe.plug.envelope.v1+json` holding the signed envelope, with layers as artifacts titled by artifact key and verified by digest.
- 2026-11-04: P3a signatures and accept rules.
  - `Plug.signature {alg: ed25519, value: multibase}` over `SIGNING_DOMAIN || revision`. `revision()` excludes the signature.
  - did:key-only verification; other DID methods fail closed.
  - Downgrade rule: a new label must be above every first-seen or tombstoned label. Synced unverified versions are listed with `unverified` and refused by `GetPlugVersion`.
  - Node publisher = did:key of the authority node key, plus a `GetPublisher` op. `PlugsScope.namespaces` narrows publishing to listed DIDs.
  - Manager fix: `AuthorityService::sign_as_node(domain, payload)` enforces a NUL-terminated domain prefix, because the same key issues attribution certificates.
  - Accepted: a synced older version is served (arrival order is not version order; downgrades are a runtime concern). Pre-P3 unsigned dev data is served from first-seen pins.
  - Main's heavy drawer tests (`named_metadata_creation_replays_every_boundary_on_both_roots` 116 s, `deterministic_drawer_model_scenarios_match_reference` 97 s in isolation) time out at nextest's 120 s limit when run alongside other crates.
- 2026-11-04: P2 plug registry, `e1bd3853`.
  - **Storage.** A grouped registry drawer (`CreateLayout::SingleGroup`) with `ciphertext`/`plaintext` inventories. Its id is persisted in `plugs_registry_drawer` (SQLite). `CREATE_DRAWER_OBJECT` is revoked after creation (manager fix).
  - **Datums.** Each plug has an identity datum `org.example.daybook.plug.identity/main {id}`. Versions are `org.example.daybook.plug/<semver>`, holding the `Plug` envelope. Artifacts are `org.example.daybook.blob/plug.artifact/<label>/<key>`.
  - **Derived state.** `Registry::follow` polls `InDrawer ∧ HasTag(identity)` (ADR 023) and folds each doc with `fold_doc`. State is owned by one task, and writes wait for the follower to confirm them.
  - **Immutability.** First-seen (revision + envelope) is kept per label in SQLite, and a differing synced datum is served as `conflict` with the first-seen content. Removal keeps the first-seen record as a tombstone (manager fix, npm convention): identical republish is allowed, different content refused. When two nodes create a doc for the same plug, the lowest `DocId` wins and the others are listed as `duplicate_docs`.
  - **Protocol.** `PlugsProtocol` (`org.example.daybook.plugs.*`): `mintPlugsCapability`, `publishPlugVersion`, `removePlugVersion`, `removePlug`, `listPlugs`, `getPlug`, `getPlugVersion`, `watchPlugs` (long-poll, `PlugsCursor{epoch, seq}`). `PlugsScope{publish}`.
  - **Accepted limits.** After a restart, the registry converges shortly after boot, because the index lags. A stale view can persist until the next `leave` event or restart. A crash between drawer creation and the stored id can leak one empty drawer after the 7-day command-ledger window.
  - **P3 handoff.**
    - Signature and accept rules go in `Registry::publish` (`src/big_plugs/registry.rs`), after `envelope.validate()`, beside the `first_seen` check in the first `query` closure, where `State` is available. A synced bad signature is marked in `fold_doc`.
    - Per-namespace publishing goes in `PlugsScope`.
    - Importing an existing author document uses `DrawersProtocol::AddDoc{capability, command, drawer_id, doc_id, access}` with a Write+ registry drawer capability minted from the plugs actor. The follower picks up any doc carrying the identity datum.
  - **Traps.**
    - `PollDatums` emits spurious `leave` events; never trust your own write until the follower confirms it.
    - The datum index is eventual.
    - The blob workers take a static drawer list at boot.
    - Command ids expire after 7 days.
    - Migration hashes are blake3.
    - tokio `Mutex`/`RwLock` are banned; use an owning task or a `Semaphore`.
    - `BigNode::boot` is at the 160-line budget.
- 2026-11-04: Init follow-ups. Waiters are woken from `serve` on a not-ready to ready transition, matched by NSID. `NoduleInitStatus.waiting_on`. Verified 67/67.
- 2026-11-04: Init (D7), `04681476`. Migration 003 `nodule_init_done` (one row per initializer, replaced per revision). `attempts` is per initializer and resets on success (Kubernetes `restartCount`). A foreign target whose provider is unknown or not ready waits (no attempt counted). Cycles are reported as `InitFailed` on both sides. Verified 66/66.
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

Items 2 and 4–10 of the original list are done (see Done). Left before the PR is review-ready:
1. **Operator:** push the `../wasmCloud` `townframe-changes` bookmark, bump the `wash-runtime` rev in `Cargo.toml`, and drop the `file://` `[patch]` block. CI can't resolve it until then.
2. Dispatch admission I/O: an in-memory actor-row cache in the authority service (see the [perf] follow-up below).
3. Orphan dispatch authority after a crash: persist it or sweep it at boot (slice 2c follow-up).
4. Compare the heavy `big_cabinet` drawer test timings against an origin/main baseline (our 4th boot drawer).
5. Small cleanups: unify `owns_nsid`/`under`; a separate egress message-size constant.

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
- Moot per D8: config is node-local; syncing it is backup, not activation.
- [2b] Handler bytes must already be in the local blob store before the workers can encrypt and retain them. The install path (operator `PutBlob`/import, or OCI import) must put them there.
- [blobs] Removing a `Blob` datum leaves its worker-written cipherBlob/jwk datums in the doc. Retention is released, but the datums linger. That's big_blobs worker cleanup.
- Done (2026-11-04): the watch reconcile is skipped at already-applied heads. Trade-off: a failed grant apply at unchanged heads is retried only by `refresh_if_needed` and the dead-cap check, not by the watch. Its test uses 200 `yield_now`s to settle, which can only false-pass, never flake; there is no "watch event handled" signal to wait on.
- Done (2026-11-04): a handler-less enabled nodule is `Ready` and registers nothing.
- [main] `src/big_node/e2e/blobs/metadata.rs` isn't referenced by any `mod`, so it's dead test code from main.

## Deferred

wflow service and durable procedures; POSIX providers; triage/processors; big_tasks; network/XRPC exposure; consumers of command/view bindings; single-instance services; generalising `NoduleGrants` to operation grants.

## Open questions for the operator

- Lifecycle review questions 1–7: resolved 2026-11-04. See ADR 027 §10 and ADR 028.

- The seven questions at the end of `docs/scratch/nodule-lifecycle-review.md` (catalog role, dependency split, manifest reshape, init redesign, downgrades, retired prefixes, carried drawer-grant authority).
- Live `SchemaRepo` registration of the nodule's own datum Lexicons. Nodules can't write their own datums without it. Is it in scope for this PR?

- Commit the in-progress decisions to ADR 027 directly, or in a separate "revision" section first?
- Integer-format width: merge chose "bounds ⊆ format range; narrow formats require bounds". Confirm.
