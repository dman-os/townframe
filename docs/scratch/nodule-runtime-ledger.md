# Nodule runtime and dispatch ledger

Status: Active
Owner: Townframe maintainers
Updated: 2026-11-04
Scope: Working ledger for the `feat/nodules` PR: decisions made with the operator, work in flight, to-dos, and questions awaiting review. Read this first after any context compaction, together with AGENTS.md and ADR 027.

## How to use this ledger

- Append decisions as they are made, with the date. Never silently rewrite one; mark it superseded instead.
- Keep **In flight** and **To do** current whenever work starts, finishes or is handed to a subagent.
- Decisions here feed ADR 027 (`docs/adrs/027-cartridges-nodules-and-operation-contracts.md`, formerly 023) and the config scratch doc (`docs/scratch/nodule-config-datum-store.md`). The ADR is the normative record; this ledger is the working log.

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

### D14–D20. What a node is (operator, 2026-11-04; drafted as ADR 033)
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
- **D21.** Networking and peers belong to `big_node`, as the successor of `IrohSyncRepo`:
  - One node key serves as both the keyhive identity and the Iroh endpoint key.
  - A peers service accepts and identifies peers, registers them with the RPC services, calls `set_peer` on the doc and blob workers, and dials and reconnects.
  - Who to dial, whom to accept and which partitions to sync are an embedder-supplied peer policy, because relays mirror by sponsorship.
  - The default policy is the ADR 019 cabinet query (mirroring policy ∩ the keyhive-access partitions from `big_repo`), implemented in N4 as `Drawers::mirrored` plus `big_repo::groups_with_member`.
- **D20.** `big_node` e2e is the system test arena: harness, ladder, scale, seeded stress and nodules across nodes, driven through clients like a high-level user. The progress repo is out of scope.

### D22. HTTP-shaped calls (operator, 2026-11-04)
- The caller's credential is a header and request context, never a body field: one credential per call, admitted by dispatch and passed through to the service (slice N2b).
- Narrowing tokens: the operator mints them through the router. They are mostly transient; persisted ones go in a 0600 file under the user's config dir (like gh or kubectl).
- UDS peer pid: credentials are an internal request extension, never a header a client could forge. If a hop ever needs headers, apply the reverse-proxy rule: strip client-supplied copies, then set them. Tokens minted over a UDS connection may be bound to that peer (holder binding, like `bound_signer`); pid binding uses pidfd, not raw pid, because of pid reuse.
- Platforms: CLI users are Linux; GUIs use the in-process path. Peer credentials stay behind `cfg(unix)` (Android has `SO_PEERCRED`). Windows later, over TCP with a token. Keep transports behind the acceptor abstraction, not Linux-specific code.
- Candidate HTTP-shaped moves (not decided):
  - `traceparent` (W3C) for `TraceContext`;
  - a deadline header for the call budget (gRPC's `grpc-timeout` is the precedent);
  - `If-Match` with doc heads or revisions for optimistic writes;
  - `Prefer: wait=<s>` (RFC 7240) for the ADR 023 long-polls.

### Plan N (the node PR, branch `feat/big-node`)
- **N1 (done).** `big_dispatch` and `big_dispatch_core` own the router, admission and providers. `Readiness` and `authority_owns` moved with them. One `ServiceProvider` adapter (via `Protocol::visit_unary`) registers authority, docs, drawers, blobs, plugs and nodules as `builtin:<service>`. Callers hold two capabilities (a dispatch handle plus the in-request `Authorization`); the dispatch caller's identity does not flow into services yet. Subscriptions are not routed through dispatch. Follow-up: the invoke/mint/`Echo` test helpers are duplicated in `big_dispatch/e2e.rs` and `big_nodule/e2e/dispatch.rs`.
- **N1. `big_dispatch` crate.** Move the router, admission and provider registry out of `big_nodule`. Register docs, drawers, blobs, authority, plugs and nodule-management operations as built-in providers. `big_nodule` registers its Wasm providers into it. Tests: a built-in operation and a nodule operation are reached through the one router with the same admission.
- **N2 (done).**
  - `/xrpc/<nsid>` is a fallback in front of dispatch, on the existing XRPC server and its envelopes. Dispatch refusals map to 401/403/404/503.
  - The caller presents `Authorization: Bearer <authority>.<signer>` (`BearerToken`, the existing minted dispatch handle).
  - `Anonymous` per listener: a token-less request over UDS is the operator (`Authorization::identity(root)`); over TCP it gets 401.
  - The UDS acceptor reports `PeerCredentials` (tokio `peer_cred`). The `big_node` `OwnerOnly` wrapper refuses another uid before any request is read.
  - The socket directory is 0700, a 0755 parent is refused, a stale socket is replaced, a live one is refused, and stop removes the socket.
  - `XrpcClient::unix`.
  - Open questions:
    1. Built-in services still require their capability in the request body, so a CLI needs the root `CapId` plus minted service caps. Dispatch admission does not pass the caller's identity into services.
    2. Minting narrowing tokens over the wire (dispatch `mintCapability` is not served through the router), and client-side token storage.
    3. Windows: no transport or token rule.
  - Not done: GET/query operations (e.g. docs `getDocAvailability`) through the fallback; TLS on TCP.
- **N2b (done).** The caller's credential left every request body: it is `Envelope.metadata.authorization` (`Authorization: Bearer <authority>.<signer>`, `Authorization::{header_value, from_header_value, metadata, from_metadata}` in `big_cabinet_core`; `BearerToken` is gone). Dispatch passes the admitted `Caller.authorization` to `ServiceProvider`, which sets it as the service request's metadata. Resource capabilities (`capability: CapId` on doc, drawer and blob data operations) stay in the body: they designate what is acted on. A missing or malformed credential is `ContextError::Unauthenticated` (every service error carries it); dispatch's XRPC edge maps it to 401. Consequence: an identity (the operator) satisfies dispatch and the service with one credential; a dispatch-scoped handle is not understood by a service domain, so a scoped caller can reach only operations whose body capability is the sole check (data ops), not the mint/admin ops. Direct (non-dispatch) service XRPC still answers every domain error with 400 (no per-error status mapping exists).
- **N2. Listeners.** Serve the node router over UDS (token handshake that yields a capability) and TCP (`service_utils_rs` already has TCP serving; add a UDS acceptor). A `Client` over XRPC/UDS drives a node end to end.
- **N3. Resource bindings (done).** Services are handed opened storage; none opens a path.
  - `big_repo::Config.storage` is a `big_repo::Storage { sql: Database, keyhive: KeyhiveStorage::{Memory, Files(KeyhiveFiles)} }`; `BigRepo::boot(config)` no longer takes a `SqliteRuntime`.
  - `big_blobs::Storage` is an opened store (`memory()` / `filesystem(root).await`); `big_node::Config.secrets` is an `Arc<SecretStore>` (no borrowed lifetime), one per node.
  - `big_node::Bindings::in_directory` is the "node in a directory" convenience; `big_repo::Storage::in_directory` is its repo-level piece, which daybook_core also uses.
  - A repo database may be shared by nodes with distinct scope keys. This exposed that the keyhive admission consumers' walker state and retention-reader ids had no scope; the scope key is now part of their namespace.
  - The node's own `sql` is not scoped (authority, blobs, cabinet, plugs, nodules tables): one per node. Open: namespacing the authority wrapping key and scoping those tables, if hosting wants one database for many nodes.
  - Test: `big_node` e2e `many_nodes` boots 20 nodes, stops them, reboots the durable ones and asserts no databases, temp dirs, sockets or tasks remain.
- **N4 (done). Peers and networking.** Delivered as `big_node::peers`; `node_identity_seed` already was the Iroh secret key bytes, so "one key" is a derivation (`SecretKey::from_bytes(seed)`) plus a boot assertion, not a config change. The cabinet query is `Drawers::mirrored` (mirror intent only); `big_node` bounds it with `big_repo::groups_with_member` (keyhive access). Open: BigRepo has no keyhive-change signal, so an incomplete access answer is re-asked on the sweep tick; `PartSynced` events are not reliably emitted by the worker (tests wait on `DocSynced`); simultaneous mutual dials are not tie-broken (as in `IrohSyncRepo`).
- **N4 spec.**
  - Use one node key for keyhive and Iroh.
  - Move the Iroh endpoint attachment (from the test-only `big_node/e2e/blob_discovery/network.rs` and `big_cabinet/peer_net.rs`) into `big_node`.
  - Add a peers service successor to `IrohSyncRepo`: accept, identify, register, `set_peer`, dial, reconnect with backoff.
  - Add the embedder peer-policy hook to the config.
  - Implement the cabinet per-peer partition query (ADR 019) as the default policy.
  - Port test helpers onto it.
- **N5 (done, with 2 pinned failures).**
  - `e2e/harness.rs` `Cluster`: durable nodes per nickname, peers-attachment topology, a shared mirrored drawer, notes and blobs through the service clients, waits pumped by `PeersEvent`s plus datum-index revisions, with no sleeps.
  - `e2e/ladder.rs`: 11 of 13 pass.
  - **Pinned finding:** `a_peer_that_was_shut_down_catches_up_on_returning` and `branches_made_apart_merge_and_converge` stall. A doc created while a peer was offline is never pulled after it reconnects: the receiver shows `NoLocalContent`, `sync_demand: false`, while the keyhive pull is healthy in both directions.
    - Suspects: the serving side's part replay or classification for the reconnecting peer, and the untie-broken mutual redial (`invalid handshake: aborted by peer`).
    - Note: `DocSynced` fires before the doc is materialized, so it is not a "readable" signal.
  - Correction to N4: there is a keyhive access signal. `KeyhiveAccessRevisionStore` is driven by a `ConcurrentDeltaWalker`, as `big_blobs/permissions.rs` does. The peers service must consume it instead of relying on the 15 s sweep (slice N4b).
  - **N4b (done).** `big_node::peers::access` feeds the actor from `KeyhiveAccessRevisionStore` through a `ConcurrentDeltaWalker` over `{scope_key}:big-node-peers-v1` walker state. A delta recomputes the parts of the peers it touches (a wanted group, a closure containing the peer, or any group subject); the sweep tick only redials. Test: `peers::a_grant_after_connecting_replicates_without_waiting_for_a_sweep` (sweep one hour; stalls without the arm).
  - **Pinned stall root cause (fixed).** Not keyhive and not mutual dial. `big_sync_core` pipelines replay subscription updates: a gen 1 delta can reach the responder before the cancelled gen 0 open (`UnknownSubscription`), the client resets and re-opens at gen 0 under the *same* subscription id, and when an older gen 0 lands late the responder answers the re-open as a repeat of it (none of its ids applied, the other request's verdicts returned). The client reads that as acknowledgement, then pages name ids the responder never took (`unknown replay target id`) and the peer's replay is dead. Fix: a reset re-opens under a fresh subscription id. Open: the client still resets 4-8 times per reconnect (it should not send a delta before the opening update is acknowledged); orphans count toward `MAX_REPLAY_SUBSCRIPTIONS_PER_PEER` (8) until the 10 minute TTL.
- **N5. Harness and ladder suite.** An N-node harness that reuses `big_repo` test2's topology and nickname helpers. Port the scenarios of `daybook_core/sync/tests/ladder.rs` onto it.
- **N6. Scale, stress and cross-node nodules.** 10k-document replication, a seeded randomized stress test (`long_test` names), and a plug published on node A then installed and invoked on node B.
- **N6 (done, with 3 pinned failures).**
  - Blobs permissions cursor: the walker namespace is `{scope_key}:big-blobs-permissions-v1`, threaded as `big_blobs::workers::WorkerConfig.scope_key` from `BigNode::boot`. Test: `big_blobs` `permissions::repos_sharing_a_database_keep_distinct_permission_cursors` (fails with the old unscoped namespace). The on-disk namespace changed, so an existing node resumes this cursor from the log's archived floor.
  - Harness: `Cluster` tracks live links (stop drops them; a restart without `reconnect` is a partition), `admit(owner, member, drawer, access)`, `try_create_note`, `is_running`, `is_partitioned`.
  - **Pinned: drawer placement is O(drawer size) per document** (was `e2e::scale::long_test_ten_thousand_notes_replicate`; folded into `e2e::costs`, see D25 slice 1). Creating a note costs about 3 roster writes (target, present and originated drawers). Each `WriteDatums` on a drawer doc grows linearly with its entries: `Gate::verify` of a new key walks all of the doc's history (`footprints` over `get_changes(&[])`), and `stage` hydrates the whole `Dmeta` (every datum's metadata), twice per write. Debug build: roster write 77 ms at 0 entries, 1.2 s at about 100. 200 notes took 735 s to create (quadratic) and 105 s to replicate after connecting. 10k cannot finish; the test times out. Replication after connecting looked roughly linear (about 0.4 s per 20 notes once the first batch landed). INSTR timings: `big_cabinet` `doc/create.rs`, `doc/write.rs`, `drawer/add_doc.rs`.
  - **Pinned: a member admitted by an owner who does not hold a doc can never decrypt it** (`ladder::a_member_admitted_by_an_owner_who_has_not_seen_a_note_reads_it`; stress `TEST_SEED=0xb160de5eed00001`). keyhive `add_member_with_manual_content` issues CGKA adds only for docs in the adder's own `self.docs`. A doc another member created while the admitter was away never gets the new member's CGKA leaf, and nobody issues it later: the holder learns the delegation but does not act on it. The receiver logs `missing entry encryption key while materializing` forever. Control: the same story with the owner reading the note before admitting passes.
  - **Root-caused 2026-11-06; fix pushed** (keyhive `4661560f` on `townframe-changes`, with subduction `a9c8697c` re-pinning keyhive to match; townframe pin bump in verification): keyhive_core `pseudorandom_in_range`/`clamp` (`principal/individual.rs`) shifted left and byte-swapped, so a prekey index was a multiple of `2^(8-digits)`; with 16 live prekeys it could pick index 16 == len, `.nth(16)` returned None, and `pick_prekey` reported a non-empty set as `NoPublishedPrekey`. Selection hashes (individual, doc), so it reproduced deterministically on seed 6. Fix: mask-and-reject sampling strictly below `max`; pinning test `pseudorandom_in_range_is_strictly_below_max_for_every_length` (failed `max 2: indices reached {0}` before). With the fork patched in, seed 6 passed 3/3 and seeds 2 and 5 passed. Open: `pick_prekey` should treat `nth` None as an invariant break, not `NoPublishedPrekey`; likely upstream too. Was: **Pinned (unexplained): `group member add failed: individual … has published no prekey to select from`** (stress `TEST_SEED=6`) when Alice admits Carol after Carol has been stopped and partitioned several times. Not root-caused.
  - Stress (`e2e::stress::long_test_seeded_partitions_restarts_and_grants_converge`, `TEST_SEED`, 4 nodes, 40 steps): seeds 1, 2, 3, 4, 5 and 7 converge (78–108 s); the default seed and seed 6 fail as pinned above. A member's create is refused until the drawer reaches it; the stress takes that as an unmet precondition.
  - Cross-node nodule (`nodule_wasm::a_plug_published_on_one_node_is_installed_and_invoked_on_another`): Alice publishes and admits Bob to her registry drawer at Read. Bob mirrors its blob inventory, reads the version datum from the synced doc, then admits, enables and invokes it via dispatch. Open: Bob cannot `ImportPlugDocument` from Alice's registry drawer. His registry actor needs a drawer grant, `GrantDrawer` takes Admin, and Bob's drawers actor is refused an Admin capability on a peer's drawer (no local grant) even when invited at Admin.

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

### D23–D26. After N6 (operator, 2026-11-05)
- **D23. CGKA adds for unseen docs: patch the keyhive fork now.**
  - The bug: keyhive's `add_member` issues CGKA adds only for docs in the adder's own `self.docs`, and receiving a delegation issues none. So a reader added by an owner who never saw a doc can never decrypt it.
  - The fix: a node that holds a doc issues the missing adds when it learns of a new reader. Upstream Keyline (`design/keyline/implementation.md`, "Integration / BeeKEM") reconciles readers on every local write.
  - We do not migrate to upstream yet, because they are still churning. Fork patches are disposable: they get reinterpreted at migration, so they may be pragmatic.
- **D24. Installing a plug uses the caller's capability.**
  - The caller of "install this plug" passes an object capability with read access to the doc or drawer that holds the plug. Services get no standing grants over arbitrary drawers.
  - Install takes a plug in whatever form it arrives. Do not special-case another node's registry drawer.
  - Open: what survives a reboot. Maybe a new class of temporary read grant scoped to one doc or drawer.
  - The operator (the authority root) is a cabinet superuser. It controls every drawer on its node with no explicit grant, and converts its standing into whatever grant or capability it wants. Certain authority actors may convert their standing into other grants. This is a root principal, not a broker. On Bob's node, the operator mints the handle on a drawer that synced in and passes it to install. Install keeps reference semantics (the doc is entered and follows its author).
- **D25. Scale is a benchmark, not a stress test.**
  - Set targets from back-of-the-napkin costs (bytes per note, SQLite transactions, signatures), then measure in release.
  - Measure memory as well (dhat; idle node; many nodes per process).
  - Rough targets: creating a note ~5–20 ms and flat with drawer size; replicating 10k notes in tens of seconds to a couple of minutes.
  - The stress test stays at the legacy daybook scale.
- **D25 slice 1 (infrastructure in place; release numbers not yet taken).** `big_node::e2e::costs` (the scale test is folded into it).
  - Mechanism: a counting `GlobalAlloc` (the dhat-rs `HeapStats` / divan `AllocProfiler` technique). It is process-wide and off unless a costs test turns it on. It needs no new dependency and no lockfile churn. dhat itself was rejected: it takes a global mutex per allocation, allows one profiler per process and has no per-interval peak. Wall time is measured over a scripted workload, not with criterion or divan, because those assume alike iterations and cannot reach the `cfg(test)` harness. Allocations are the counted work; wall time is only reported (the `BandRun` precedent).
  - `a_note_costs_no_more_to_create_in_a_fuller_drawer` (default run, about 50 s in debug) **pins the quadratic**: the median allocations for notes 6–10 against notes 26–30 must stay within 1.25×. Measured: 215,729 against 409,535 (1.9×). It fails until the fix lands.
  - `long_test_cost_table` (default `BIG_NODE_COSTS_NOTES=10`, about 110 s in debug) prints `COST` lines. Release: `BIG_NODE_COSTS_NOTES=N cargo nextest run --release -p big_node --no-capture -E 'test(/costs::/)'`.
  - Debug findings (x86 laptop, 10 notes):
    - Creating a note costs 1.4 s and allocates about 250 MiB in total (peak about 1 MiB).
    - INSTR split per note: two drawer `enter`s of about 250–550 ms each. Of that, a keyhive `grant_doc_access` takes about 200 ms; the roster `WriteDatums` takes 87 ms at 1 entry and 300 ms at 13. `stage` accounts for about 100 ms per call at 13 entries and runs twice.
    - Per-node footprint: a fresh node is 12 MiB of Rust heap and 73 MiB resident. Booting it takes 7 s and allocates 350 MiB.
    - Storage: a note is 1,065 B saved and 3,368 B stored. The drawer roster grows by 383 B saved per entry but about 5.8 KB stored per change. The node directory grows by 57 KB per note (WAL included).
    - Replication runs at 27 ms per note and is flat at 10 notes. Each dispatch `readDatums` takes about 25 ms on all three paths.
- **D27. The datum gate drops ADR 020 condition 2 from the default path** (operator, 2026-11-05).
  - Verification is: each signature verifies against X at its introducing change, and X at the combined signature heads equals X now. A new key must be absent.
  - Hidden losing-conflict operations are not checked, because the docs service offers no way to read conflicted values. If a conflicted-values read is ever added, forgery verification belongs on that path, not the default one.
  - This removes `Gate::footprints` (a full-history decode per write). ADR 020 needs amending.
  - Done (2026-11-05): `Gate::footprints` and the coverage branch are gone; `Gate::keys` (MergeDoc) now lists slots with content or metadata at the merged heads. ADR 020 §6 and the edge-case section are amended. Accepted cost recorded there: an unsigned erasure of a value together with all its signatures reads as Absent.
  - Cost 2 done alongside: a write validates and reconciles only the dmeta parts it names (`Dmeta::write_atomic`, `DmetaWrite`, member schemas from `CompiledSchema::member`). `a_note_costs_no_more_to_create_in_a_fuller_drawer` now passes: 183,219 allocations at drawer size 10 against 203,380 at 30 (1.11x; was 234,389 against 463,475).
  - Found alongside: `drawer::e2e::model` failed on `[AddChild 1<-0 Relay, AddChild 2<-1 Relay, RemoveChild 2/0]` (regression seed saved). `group_level` used the transitive `agent_access_on`, so unlink found a 2→0 delegation that only existed through 1. Fixed: it reads direct access.
  - Operator ruling on the unsigned-erasure cost: anyone with write access can always break a doc; the gate defends against forgery only. No erasure guard.
- **D29. Note-create cost findings** (2026-11-05, profiling agent): 777 ms is ~3 × 222 ms waits on the 250 ms `IDLE_POLL` of the keyhive admission tailers in `wait_for_keyhive_reconciliation` (one per `enter`; the incorporation sink wakes only the dispatcher). Background: the blob worker reprocesses every doc on each drawer policy tick (quadratic). Every local commit runs a full `doc.save()`. Fixes in flight: tailer wakeups, blob rearm.
- **D30. big_repo is a rewrite candidate** (operator, 2026-11-05): once upstream finishes the keyhive/subduction rewrite. Until then fix its obvious costs in place and flag every further big_repo wrong turn to the operator. Found so far: the admission reader polls with no wakeup (its comment claims `notify_one`), and every commit stores a full `doc.save()` snapshot as the head payload. Open in big_cabinet: ~340 `resolveActor` and ~40 capability mint/retract per note, ~800 write and ~2,600 read SQLite leases per note, 8.6k "caller dropped before response" in a stress seed, three serial placements per note, 7 s / 350 MiB debug boot.
  - big_repo polls found in the revisioned-store sweep (2026-11-05):
    - `keyhive_admission::Reader` sleeps `IDLE_POLL` 250 ms at the tail; the comment claims `notify_one` wakeups that do not exist. Fix in flight (append signal).
    - `keyhive_access_stream` `PARKED_REPROBE` 250 ms for a row waiting on a missing proof-chain link; its comment claims the admission log wakes on append. The link landing is likely itself an admission append; folded into the same fix.
    - Keyed-frontier stores (datum_set, facet_set, big_blobs projection, automerge frontier, doc_revision/doc_delta) share `big_sync` keyed frontier + `notify_changed`; no polling there. `big_sync_core::live_revision_watch` is used only by tests.
    - `native.rs:2407` calls `refresh_cache()` every 2 s on every node, a loop copied from upstream (subduction_keyhive runtime.rs / subduction_cli server.rs `cache_refresh_interval`). `refresh_cache` is just `ensure_cache_current`: generation-checked, and every cache reader (`initiate_sync_with_request`, `notification_targets`) already calls it on demand, and remote ingest bumps the hive generation like local mutation does. So the loop only pre-warms the rebuild off the sync path. Idle cost: `nested_store_generation_sum` locks every group and doc mutex every 2 s, O(groups + docs). Candidate: drop the loop, or rebuild only on a generation-change signal. Low priority.
    - `big_sync/worker.rs:467` waits for idle with a 50 ms poll; check whether anything outside tests uses it.
  - Landed 2026-11-05: admission readers wake on append (`d4d33097`; note create 767 -> 276 ms p50, reconcile wait 224 -> 38 ms). Blob roster writes re-reconcile only the named member (`6bd49930`; allocations per note 100k -> 125k across 30 notes instead of 460k -> 1M).
  - Landed `6d7c60f6` (ADR 025 §7): `PollDatums` delivers rows, move-ins and move-outs; `enter` and the per-request depth map are gone; the blob worker maps rows statelessly. Pinned by `drawer::e2e::queries::roster_write_polled_on_the_drawer_doc_delivers_only_the_changed_entry_rows` and `move_in_delivers_older_rows_move_out_leaves_and_resume_does_not_redeliver`. Open: a matching doc with zero selected rows is now invisible to a row-only consumer (add a marker event if one needs it); a row whose tag the predicate names is always a move-in (re-sends that doc's selected rows; watch the blob worker); the blob drawer worker now rearms only on Jwk rows, not on any new doc.
  - big_cabinet wrong turn: a `PollDatums` page is not a delta. Every page re-delivers the polled doc as `enter` with its whole selected state, so each roster write costs every poller O(roster), and the blob worker has to diff pages against the previous one to find what changed. A delta feed would remove both.
  - Pinned flake: `big_blobs sync::transfer_tests::completion_releases_capacity_for_the_ninth_eligible_transfer` timed out at 120 s twice at `-j 6` under load from other builds; passes alone (3.3 s) and at `-j 2`. Uses iroh routers plus Notify. Not instrumented yet.
  - Remaining note-create cost after the poll fix: CPU-bound, ~370 ms CPU for 276 ms wall; 38 ms reconcile per grant, three grants per note.
  - Cost table at `3d04999` (aarch64 release, 200 notes): create note p50 113 ms (was 226), first tenth -> last tenth 100 -> 122 ms (was 131 -> 316), allocations 83k -> 113k; the growth pin passes in release (72,255 vs 77,798). Reboot 1.66 s / 496 MiB. Connect 25.5 s (still the outlier; profiling). Node dir 32 KB per note.
  - Attributed (count-weighted allocation sampler, `31e7e5c5`; debug x86): ~1.15k allocations per existing entry per note.
    - ~650: `stage` hydrated the drawer doc's whole dmeta to pick aliases and `docUuid`. Fixed: `Dmeta::hydrate_for` with `DmetaRead`. The pinned test passes again at 1.02x (95,222 vs 97,398; ~109 per entry, likely the big_repo member walk below).
    - ~148: `Gate::verify` -> `reconstruct_current_claims`. It disappeared with the hydrate fix and was not root-caused; `Reconstruction::aliases` still scans all of `datumUuids` per reconstructed signature. Open: a key-to-aliases reverse index in dmeta would make both lookups constant, at the cost of a second copy of alias facts (dmeta docs warn against it).
    - ~110: big_repo wrong turn: the causal checkpoint worker calls `BigKeyhiveHandle::docs_for_agent` -> `transitive_members_short_locked`, which walks and enqueues every doc of the drawer's docs group per checkpoint (O(docs in group)). Flagged per D30, not fixed.
    - The sqlite INSTR caller sampler inflates the fixed cost by ~18%, so the pinned ratio understates the growth while it is on.
  - At `6d7c60f` (row stream): still failing, 82,295 at size 10 against 103,137 at 30, so the roster re-delivery was not the main source of the growth. 200-note table: create note p50 226 -> 188 ms (first tenth 113 ms / 101k allocations, last tenth 269 ms / 258k); reboot 1.51 s; connect 21.8 s (unchanged). Attribution agent running (debug build, allocation sites by drawer size).
  - Pinned again at `3571b59` (aarch64 release): `costs::a_note_costs_no_more_to_create_in_a_fuller_drawer` fails, 85,639 allocations at drawer size 10 against 108,482 at 30 (1.27x > 1.25x). The absolute growth is unchanged (~1.1k allocations per existing entry); the placement work cut the fixed cost, so the remaining O(drawer size) growth now dominates the ratio. Suspect: `PollDatums` re-delivering the whole roster per roster write (ADR 025 §7, in progress). Do not loosen the threshold.
  - Landed `c90a2cdf`: placement issues all three drawers' grants, waits for keyhive once, then publishes the roster entries concurrently. Release, 50 notes, quiet box: create note p50 149 -> 120 ms; allocations per note 135,648 -> 120,295.
  - Landed: commits store change bytes, not snapshots (drawer stored/saved 205:1 -> 8.4:1; node dir per note 147 KB -> 33 KB). The remaining ~33 KB per note is SQLite rows, WAL, indexes and keyhive events, not blobs: needs a per-table breakdown.
  - big_repo wrong turns flagged by the change-bytes agent (not fixed):
    - `persist_local_commits` (native.rs ~398): one transaction per change, each a full `mutate_tree_in_tx` with minimization; an `InsertBatch` mutation exists.
    - `load_doc_snapshot` re-walks and re-decrypts the whole history on every call, including every `ReattemptMaterialization` while pending: likely O(n^2) in history.
    - `register_bundle_lease` (doc_worker.rs ~708) swallows a send error and `registered_rx.await.ok()`: AGENTS channel rule.
    - hub `put_doc` saves the whole doc twice to detect a content mismatch; heads would do.
    - `process_pending_fragment_requests` keeps pending fragment work in memory only; after a crash those boundaries never get fragments (bounded growth).
    - Deletes in `mutate_tree_in_tx` rehydrate the tree from SQLite (rare, O(tree)).
    - Existing stores keep old snapshot blobs until a fragment covers them; nothing repacks.
  - Stress seed 1 timeout (aarch64 box, 2 of 8 runs, both the first run after a fresh build; 6 local runs pass). Triage, not pinned:
    - Keyhive CGKA update ops keep being minted to 240 s (~1,380 distinct events; passing runs ~320, all by 80 s). Nothing is stuck in pending; ops are still being produced.
    - Hypothesis: a causal-checkpoint feedback loop. A CGKA op admission triggers `EnsureCoverage(doc)` on every node; `reconcile_causal_coverage` (doc_worker.rs ~1484) persists a checkpoint whenever `current_causal_epoch` is None, via `persist_causal_checkpoint` (native.rs ~456), which `force_pcs_update`s and can mint a new update op; that op retriggers coverage on every node. Passing runs show it too (50–58 persists, ~32 with cur=None) but it damps. Nothing but timing damps it. Possibly amplified by D23's reader reconcile (more CGKA adds). INSTR `0a2a9618` logs each persist with epoch and known checkpoints.
    - big_repo wrong turn: the frontier worker's per-doc task is `replace`d on every admitted CGKA op row, cancelling the in-flight publish; the doc worker still serves the queued `AcquireHandle{Internal}` with a full load/decrypt and then finds the caller gone (8.2k–8.6k "caller dropped before response" in a failing run, ~300 in a passing one). Same keyed-replace in the causal checkpoint worker's `EnsureCoverage`.
    - Fixed `0c831218` (INSTR `1c29f8ed`): the root cause was a conflicted CGKA root (`try_pcs_key_hash` -> beekem `NoRootKey` while concurrent Updates conflict). Every node minted its own resolving `force_pcs_update`, and the concurrent mints re-conflicted every tree. Now, when the latest epoch holds updates from 2+ issuers, only the lowest MemberId mints; the others defer and are woken by the resolver's op through the CGKA-admission coverage trigger. Pinned by `test2::cgka::tier6_concurrent_pcs_updates_resolve_without_a_mint_loop` (5/5 failures without the election). Persists per seed 2 run: 40-71, was 783-1383.
      - Open: resolver liveness. If the lowest issuer is offline, lacks write access or lacks the doc, the others defer until it returns (a stall rather than a loop); a rank-delayed fallback would need a doc-worker timer. Also: after an `Add` with no concurrent updates every writer still mints once (one round, then the election applies); keyhive's `try_pcs_key_hash` discards the beekem error, so an upstream `root_conflicted` query would simplify this; seed 2 still takes ~190 s of the 240 s budget under 3x load.
    - Confirmed 2026-11-06 on seed 2 under load (`c087fd50`, 1 of 3 runs timed out): 783 checkpoint persists against 62 in a passing run, three docs about 500 each, 760 with `cur=None`; on the looping node the doc's `known` count climbs past 110 while `cur` stays None. The node keeps minting checkpoints and CGKA ops without ever deriving a current epoch. Fix agent running. Earlier next step: in a failing run, check whether `known` grows and `cur` stays None for one doc; read the keyhive fork's `force_pcs_update` for when the PCS root is underivable after concurrent update ops.
    - Seeds at `12c8127` on the aarch64 box: 2, 3, 4, 5, 7 pass; 1 timed out again (before the checkpoint INSTR); 6 hit the known prekey bug (`has published no prekey to select from`, invitePrincipal).
  - Connect/reboot profile (Sonnet agent, 2026-11-05; byte-weighted allocation-site sampler in `costs.rs`, `BIG_NODE_COSTS_SAMPLE=1`, INSTR `9e9dc63d`):
    - Landed `148c3e96`: `try_causal_decrypt` re-parsed keyhive's already-unwrapped ancestor plaintexts as `Envelope`s; the failed parse preallocated up to 1 MiB per ancestor per walk (~70% of reboot bytes, >= a third of connect bytes).
    - big_repo wrong turn, not fixed: `load_doc_snapshot`'s cold walk is N(N+1)/2 ancestor decrypts (drawer at 200 notes: `complete=21114`, 2.6 s of a 3.1 s reboot). Each item does its own full causal walk; the recovered-key cache holds only ancestors, so later items always miss. Fix: decrypt each item with its envelope's keys (`try_decrypt_content_keyed`) and fall back to the walk only for blocked items, or walk once from the frontier heads.
    - big_repo wrong turns, not fixed: `try_causal_decrypt` builds a fresh `NativeCiphertextStore` per call, so every walk re-runs two SQL loads per ancestor; `load_doc_snapshot` rebuilds the Automerge doc from scratch (~30 MB allocations per drawer load at 200 notes); `hydrate_tree` clones the whole `MinimizedSedimentree` per snapshot (not measured).
    - Connect attributed (2026-11-06, debug x86, N=25/50/100: connect 4.6/8.3/17.9 s):
      - big_repo wrong turn (D30), quadratic: every admitted `Delegated`/`PrekeysExpanded` row (~4 per note) runs a causal-checkpoint `ReconcileReaders` task that calls `BigKeyhiveHandle::docs_for_agent` (`keyhive.rs:448`), which walks every doc in the keyhive with `transitive_members_short_locked`, then `reconcile_doc_readers` per doc. Summed `docs_for_agent` time 7.8 s / 38 s / 176 s (N^2.2). Fix candidates: derive the agent's reachable docs from the delegation subject or an agent->docs index; coalesce per agent with a drain-then-run task (not `replace`, which would drop issued adds).
      - Landed: reader reconciliation walks a worker-owned reverse-membership index (`MembershipReach`) up from the admitted delegation's subject (debug, 50 notes: summed lookup 38 s -> 0.5 s). Open: the index only grows (revoked edges over-approximate); coalescing repeated group-wide reconciles per entry point; delete `docs_for_agent` (only test fixtures use it).
      - Landed (subduction `8dc7da49`, pinned): both nodes sync on connect, so two sessions compose requests before either response lands and the events cross twice; `ingest_events` now drops events keyhive already incorporated, under an ingest lock, and never drops pending ones (a keyhive ingest replays pending events). Debug, 50 notes: connect 7.35 -> 5.66 s. Pinned by subduction_keyhive `concurrent_connect_never_ingests_an_event_twice`. Open: `contains_incorporated_event` hashes all of a doc's CGKA ops per event (fine at 50 notes); a keyhive-side batched lookup if it shows up.
      - Superseded by `c9c595b9` (INSTR `3613c478`): test2 `converge()` syncs every link both ways until a full round changes no node's ledger or heads (at most 8 rounds, else panic with the per-round fingerprints). The four tier3 tests use it before their asserts. 150/150 topology stress at `-j 16`; at most 2 rounds were needed. `partial_mesh_partition_heal`'s one `owner_branch` miss never recurred, so whether converge covers it is unproven (INSTR in place). Earlier: resolved as a harness race (`5281d452`, INSTR `d1487329`): the reader mints a CGKA op when it materializes, and the owner then mints a new-epoch checkpoint; the tier3 relay/line scripts compared before either propagated. The tests now sync them explicitly; 120/120 topology iterations clean under `-j 16`. Product eventual propagation of those late facts is not separately tested. `tier3_partial_mesh_partition_heal` and `tier3_partition_then_heal` each failed once in the same stress runs; being classified. Was: pinned intermittent (pre-existing; reproduces with no filter, ~1-3% under `-j 16`): big_repo `test2::topologies::tier3_relay_replication` / `tier3_line_replication` diverge, `cgka_right_only=[one digest]` with Alice's ledger simply 4 events short (nothing pending). Likely relay-hop pull completion, not ingest. Needs INSTR: the keyhive Digest of the missing event and per-hop pull completion.
      - subduction_keyhive: the owner's counterpart ingests the same 1,030 events twice (session 1 via SyncResponse, then session 2 via SyncOps, identical hashes, `advanced=false`), 14.6 s of 17.9 s at N=100. Suspect: the advertised/requested set after session 1 is not refreshed from live held hashes (`advertised_events` snapshot or syncpoint cache, near `handle_sync_response`). Interim guard: drop already-held hashes before keyhive ingest.
      - keyhive ingest itself is linear (~7.5 ms per event in debug). Not measured inside: epochs per ingest call and `cgka_op_is_authorized` -> `transitive_members()` per op (needs fork INSTR).
    - Connect wall time is mostly keyhive membership ingest (~1.4 s of 1.56 s at 100 notes, ~10 events per note), in keyhive/subduction_keyhive; not attributed further. About 10 Missing-doc `AcquireHandle`/`ReconcileCausalCoverage` retries per note during connect: the intentional keyhive race, left alone.
    - Note-create growth (131 -> 316 ms over 200 notes, ~0.9k allocations per existing note per create): not attributed. Next: read the `big_cabinet=debug` `INSTR create/commit_write/enter timing` lines.
  - Landed `3f0476ec`: the cold snapshot load walks descendants first, so one causal walk covers a linear history (reboot at 100 notes 4.12 s -> 1.86 s; drawer 5,564 -> 107 decrypts). Unit test `cold_load_of_a_linear_history_takes_one_causal_walk` fails on the old order (40 walks).
  - Cost table at `6ddfdef` (aarch64 release, 200 notes): reboot 7.0 s / ~22 GiB -> 1.58 s / 473 MiB; connect+replicate 21.2 s / ~51 GiB -> 22.9 s / ~10 GiB (wall unchanged: keyhive membership ingest dominates, unprofiled); create note first->last tenth 142 -> 312 ms (unchanged). Node dir 30–41 KB per note across runs.
  - Refresh-loop experiment (aarch64, big_repo+big_node non-long, 2 runs each): same 11 failures with and without the 2 s loop, all `listeners`/`nodule_wasm`/`dispatch` tests failing at setup because the box has no `test_nodule_guest.wasm` built. Varying: ladder blob tests (known flake); `e2e::plugs::a_new_label_must_order_after_every_label_the_plug_has_or_had` timed out once without the loop; stressed 10/10 both ways, so load. The loop is removed.
  - Pinned: `big_repo test2::stress::tests::long_test_big_repo_tier10_stress_{3_editor_1_relay,4_editor}_converges` hit the 240 s nextest timeout alone at `-j 2`, both on `3f0476ec` (relay timed out, 4-editor passed at 218 s) and with its iteration order reverted (both timed out). The box was loaded by another agent's stress loop. Not yet split into slow vs hung; next: run alone on an idle machine with `RUST_LOG_TEST` progress lines.
  - Pinned flake: `big_node e2e::ladder::a_blob_added_before_connecting_replicates` hangs to the 120 s timeout in ~1 of 3 runs of the big_repo+big_node suite (with and without `148c3e96`); passes 10/10 alone. Fixed `92e946ab`: a peer's member doc named by the roster before its content synced in was parked by the blob worker, and none of its wake sources fired when the content landed. One node-wide Automerge-frontier reader (the facet-set pattern) now re-dirties parked rows. Pinned by `big_blobs workers::tests::a_member_parked_before_its_content_arrives_is_read_again_when_it_materializes` (hangs without the wake). Follow-ups: the worker's own writes re-dirty a row once (a no-op pass, kept for the mid-pass race); `utils_rs::testing::setup_tracing_once` prints nothing in unit tests (FIXME in place), so INSTR is invisible there.
- **D28. Follow-ups to D24** (operator, 2026-11-05):
  - Blob retention targets only inventories the node can write.
  - A write keyhive refuses for access becomes a typed error that the docs service maps to `Denied`.
  - The operator superuser is not capped at the node's keyhive level: keyhive enforces at write time.
  - The e2e harness keeps reading through the drawers actor's capability for now.
- **D26. Drawer keyhive architecture.** Revisit against upstream `design/keyline/patterns.md` together with the Keyline migration. Topics: supply roles at Edit rather than Admin, flat vs nested child drawers, rotating roles, the steward pattern.
- Naming: DONE. cabinet's self-doc `LocalDrawers.registry_id` (the known-drawers list, not the plug registry) is now `known_drawers_id`. The SQLite `local_drawers.role` value `registry` is persisted behind a CHECK constraint and kept as is (renaming needs a migration).

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

- 2026-11-06: four concurrent agents (D36).
  - keyhive `a84137d1`: `try_pcs_key_hash` returns `PcsKeyError` (`UnknownDocument`, `NoCgka`, `RootConflicted { issuers }`, `Underivable`). The issuers come from the root's actual versions, matched to the `Update` that minted each one. subduction `5f418129` re-pins it; townframe pin `f74260b0`. The election (`a4aec85e`) now reads `RootConflicted { issuers }` instead of the latest epoch's Update issuers. big_repo 446/446.
  - `c53ab910`: the frontier and causal checkpoint workers coalesce a per-doc burst into the running task plus one follow-up instead of replace-cancelling it; the doc worker skips a load whose requester has gone. Seed 2: doc-worker caller-dropped replies 287 -> ~40. Only the doc-worker half has a unit pin (no cheap seam for the workers).
  - `173786c0`: NDK step 1. `service_client_rs` is split into `service_client_core_rs` (Client<T>, transport traits, XRPC codec, bytes-only `HttpTransport`; native + both wasm targets) and `service_client_native_rs` (Direct/Irpc/Xrpc `Transport`, UDS/iroh clients). 7/7 core tests pass under wasmtime on wasip3.
  - `ca7b1caa`: ADR 033 revision and the PR draft `docs/scratch/big-node-pr.md`.

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
1. Done (checked 2026-11-06): `townframe-changes` is on `dman-os/wasmCloud` origin at `e7648c6e`, `Cargo.toml` pins that rev, and no `file://` patch remains.
2. Done (checked 2026-11-06): capabilities resolve, mint and retract in the in-memory cap ledger (`authority/cap.rs`), and actor rows are memoized in `actor_rows` (`cached_actor_row`); SQLite is only the durable store for actors and grants.
3. Orphan dispatch authority after a crash: persist it or sweep it at boot (slice 2c follow-up).
4. Compare the heavy `big_cabinet` drawer test timings against an origin/main baseline (our 4th boot drawer).
5. Done (2026-11-06): the NSID-prefix check is one function (`DispatchScope::authority_owns`), and guest requests and handler responses share one size cap (`MAX_MESSAGE_OCTETS`, `big_nodule_wasm`).

### Follow-ups found during the merge
- [perf] big_node blob_discovery tests (`selected_inventory_replacement_removes_old_inventory` and 3 others) hit nextest's 120 s timeout at `--test-threads 12` under load ~13. Not a stall: traced against a solo run, the same work (236 `reconcile_readers`, ~80 `ensure_coverage` tasks for 2 nodes and 3 drawers) runs 4-10x slower, no log gap over 1 s, and the waits subscribe before checking. Lever: the per-fixture keyhive work, not timeouts.
- [perf] `big_sync::rpc` caller-dropped replies: ~148 per seed 2 run, untouched by `c53ab910`.
- [build] big_dispatch emits a `recursion_depth_exceeding_limit` future-incompat warning on the `Send` proof of `Host::unary::<MintDispatchCapability>` (present at `3d04999`, before the client split). Clippy does not report it; it will become a hard error.
- [env] mold fails with `ftruncate: Invalid argument` linking `day` when day_cli is built with `--all-features` together with the service crates on the ntfs3 target disk; day_cli alone links and passes.
- [design] service client: guest subscriptions need a `!Send` stream (`BoxStream` today); whether the native `XrpcClient` should implement `HttpTransport` so guest and native unary paths share code.
- [design] keyhive_wasm `tryPcsKeyHash` still returns `Option` (`.ok()`); expose the error to JS if needed.
- [perf] `DatumStoreHandle`'s `seen_head_hashes` is a set of every head ever seen and grows without bound. It came over from the pre-merge code. Bound it, for example by keeping only the current frontier/ancestry.
- [test] There is no dedicated test for `Predicate::sole_doc` page filtering. Add one in `big_cabinet` doc observe tests: a single-doc poll is not woken by writes elsewhere.
- [cleanup] DONE: the two ADRs numbered 007 are split. `007-doc-branch-identity` became `034-doc-branch-identity` (its references moved with it) and `007-cartridge-manifests-as-drawer-docs` keeps 007, which about 100 code comments cite. The `ERROR_ADR_ID` exemption is removed.
- [cleanup] The root `Cargo.toml` has many unused workspace dependencies flagged by cargo's `unused_workspace_dependencies` lint (`axum-extra`, `big_node`, `bon`, `clay_rt_wcmp_wasmi`, `micro_counter`, `once_cell`, `subduction_redb_storage`, `tower`, `tower-http`, `tracing-appender`, …). All come from main. Clean up if the operator agrees.

- [main] `src/big_node/e2e/blob_discovery/mod.rs` is not rustfmt-clean on origin/main (byte-identical here), so the flake `rust-fmt` check fails on it.
- [main] The big_node `blob_discovery` e2e tests take about 95–105s alone. Under a parallel run they exceed the default nextest kill (60s × 2) and aren't named `long_test`. Pinned 2026-11-04; they don't involve nodules.

- [slice 2] `Provider::call` currently gets only `InvocationContext`. The Wasm provider needs the **caller identity/authority** (for the authority header). Pass the verified caller into providers. Dropping the future on deadline is the cancellation path.
- [test, pinned 2026-11-11] `big_node nodule_wasm::a_plug_published_on_one_node_is_installed_by_an_admin_member_of_its_drawer` hit the 120 s nextest cap once in a full `cargo nextest run -p big_node` on the x86 laptop right after `boot_with` (`2be21c68`), while every sync-heavy test ran 5-10x slower than alone. It passes alone in ~9 s (3/3) and the next full run was 67/67 (225 s). Not yet compared against a pre-change baseline, so a load-sensitive race in the two-node install path is not ruled out. Not reproduced: the full suite passed 3/3 on mothership (aarch64) at `22c9e4a7` (67/67, 210-240 s, partly under another lane's concurrent build). Re-open if it recurs.
- [test, pinned 2026-11-11] `big_blobs` `in_flight_keyhive_revocation_rejects_late_completion_and_new_admission` hit the 120 s nextest cap in a multi-crate laptop run (with `completion_releases_capacity_for_the_ninth_eligible_transfer`, already pinned above as load-sensitive); both pass alone in ~1 s. A full `big_blobs` run on mothership at `bbf83c01` passed 186/186 in 26 s, and big_node passed 67/67 in the same run, so both are laptop-load timeouts; re-open if they fail on an unloaded machine.
- [perf, relay] The relay policy's `wanted` will call `BigKeyhiveHandle::membered_for_agent` (`big_repo/keyhive.rs:699`), which walks every keyhive doc and group under a lock, and `peers::apply` re-runs it per peer on every group delta (`peers.rs:880`). Uncached for the alpha (no caches before fixes); measure it on the relay, and prefer an indexed keyhive query over a cache if it shows.
- **Relay alpha decisions (manager, 2026-11-11, ADR 038):** `RelayPolicy` lives in `big_node::peers` beside `MirrorPolicy`; `node.json` gains `"relay": true`; the alpha has a per-drawer command (grant the relay `Access::Relay` through `InvitePrincipal`) and defers the mother-node default. Step 1 is a two-node test proving a relay-access member holds a drawer it cannot read (proven for blobs, untested for docs) and settling whether a live connection carries the keyhive contact card.
- [relay, step 1 `410e087c`] A relay-access member holds a drawer's doc it cannot read. A live connection carries the keyhive contact card, so granting needs only the relay's node id. Deferred: a relay's read returns `DocsError::Pending { reason: "document materialization" }` where `Denied` would be honest (it never becomes readable); `big_repo::runtime2::native` logs "missing entry encryption key while materializing" at warn for this normal relay state, once per doc per attempt (demote it in an instrumentation commit).
- [test, pinned 2026-11-11] One unidentified test in `e2e::peers`/`e2e::blob_discovery` hit nextest's 480 s cap once in a laptop sweep during relay step 1; the sweep (`e2e::peers e2e::blob_discovery e2e::relay`) then passed 3/3 on mothership at `410e087c` (12/12, ~83 s each). Treated as laptop load; re-open if it recurs on mothership.
- [build] `mltools` reads `OLLAMA_URL`/`OLLAMA_USERNAME`/`OLLAMA_PASSWORD` with `env!` at compile time (`src/mltools/models.rs:23-25`), so a workspace build fails on any machine without that `.env` (mothership, CI) and the credentials are baked into the binary. Not ours; flag to its owner. Workspace clippy on mothership runs with placeholder values.
- [wasm] `RequestContext::expired()` is a tokio sleep in wasm-buildable `service_core_rs`; a guest-side caller panics ("no reactor running"). After `fda5d45d` only native code calls it (transports enforce deadlines, `Client` is runtime-free), but nothing stops a future guest call. Make it runtime-agnostic or move it to the native side.
- [test] The nodule test harness (`big_ndk_testing`) carries its own copy of big_node's e2e `config`/`secrets` helpers (~80 lines), because a public `big_node::testing` would put test helpers in a product crate's API. Fold the two copies if big_node's e2e can ever use the harness without a dev-dependency cycle.
- [perf, minor] Native `Transport` boxes the inner call future (`fda5d45d`) because nested client calls overflowed the debug-build stack; one allocation per call.
- [perf, minor] `Router::register` rebuilds the whole table per provider. Reconcile registers per nodule, so it's O(n²) per pass. Fine at today's scale; batch registration if it grows.

- [test-time] Four heavy `big_cabinet` drawer e2e tests from main hit the 120s nextest kill under load, and one even alone:
  - `model::deterministic_drawer_model_scenarios_match_reference`
  - `lifecycle::named_metadata_creation_replays_every_boundary_on_both_roots`
  - `delegation::three_group_root_invitations_bound_transitive_links_and_revoke_both_roots`
  - `lifecycle::sensitive_root_creation_replays_every_boundary_without_replacing_groups_or_docs`

  They are not `long_test`-named. Our branch adds a 4th local drawer (config) at boot, so **compare timings against an origin/main baseline** before calling it main's.

### Slice 2c follow-ups
- [authority] The service's dispatch authority is minted per boot and retracted on clean shutdown, so nodule caps rotate on every restart (the subagent's claim that they stay stable across restarts is wrong). There is no orphan after a crash: the capability ledger is in-memory only (`cap::State`, rebuilt empty at every `authority::boot`; no capability table exists, asserted in `reboot_loses_handles_but_preserves_actors_and_keys`), so a dead process takes every cap with it. The clean-stop retraction only kills the handles within the running process once the nodule service is gone. No persistence or sweep is needed (checked 2026-11-10).
- [convention] Nested calls inherit a deadline only if the guest forwards the header. gRPC propagates deadlines through the context automatically, so the guest SDK should do the same.
- [decided] Production default for `nodule_operations` is empty (deny by default, the ocap convention). Caps stay stable across revision upgrades, because the actor is the same.

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

- **D31. Transports and validation** (operator, 2026-11-06):
  - Remote clients use HTTP and WebSocket over iroh by default. The node needs no TCP listener for clients; any future TCP need is a narrow case with TLS terminated at a reverse proxy, so TLS on the TCP listener is dropped.
  - GET/query XRPC operations through the `/xrpc` fallback are plain to-do work, not an open question.
  - Dispatch validates operation input (and output) against the operation's Lexicon, as atproto's XRPC servers do. To do.
  - Lexicon integers: err on compatibility with the atproto spec, whose integer type has no `format`. A width format is our extension; missing bounds are accepted and tighten to the format's range. Fix the macro to emit bounds.
  - Windows work collects in `docs/scratch/windows-todo.md` and lands as one PR on a Windows machine.

- **D32. Rust data model creep in datums** (landed `f7f1f89c`: widths become bounds at declaration, `minimum >= 0` stores `Uint`, floats are one f64 decimal string, tuple/char/128-bit/pointer-sized/unit leaves no longer compile; a signed Rust field bounded to `minimum >= 0` is refused at declaration. Renamed `Float64` -> `F64` (Automerge naming, D33); have the macro refuse tuples with its own message; `schema_compat` in legacy `daybook_core/plugs.rs` still reads integer formats) (audit 2026-11-06; operator: integer widths have no justification). The pipeline is Rust type -> serde + schemars sidecar -> Lexicon projection, so schemars' Rust vocabulary leaks into the sidecar and codec even though the projection drops most of it.
  - Remove (no shipped users): tuple `DatumShape` impls (`datum/value.rs` `tuples!`); `char`/`u128`/`i128`/`usize`/`isize`/`()` leaf types.
  - Remove: integer width formats (`datum/input/integers.rs`, `int8..uint128`); only `u32`/`u64` ship and plain `minimum`/`maximum` carry them. Automerge `Uint` vs `Int` (needed by autosurgeon's u64/i64 reads) then derives from `minimum >= 0`.
  - Drop the f32/f64 width on floats (Automerge has one f64); keep floats as decimal strings (Lexicon has no float).
  - Review: `x-automerge: "uint"` u64-as-string (no datum user found).
  - Keep (operator, 2026-11-06): Automerge `uint` storage for unsigned values (signalled by `minimum >= 0` once widths go), and the union setup (adjacent/external tagging records, wrapper members, `empty: true`): an accommodation of Rust inside the Lexicon model, not a divergence.
  - Keep (justified by Automerge or Lexicon): text, counter, bytes, ms timestamps, string-keyed typed maps over `unknown`, `$type` unions, `knownValues`, `nullable`, open-union carrying.

- **D33 landed `eb217b24`:** keyring secrets are named by node id (`wrapping.<node-id>`, with `delete_wrapping_key` for tests); the socket is `$XDG_RUNTIME_DIR/day/<node-id>.sock`, or a 0700 `/tmp/day-<euid>`; `day init` is idempotent; `node.json` bootstrap peers (iroh endpoint tickets) are dialed at daemon start; the built-in `getNodeInfo` query reports node id, address and connected peers; `day peers add|ls`. Nit: `getNodeInfo` sits under the retiring `org.example.daybook` prefix.
- **D33. Operator answers on the `day` CLI and loose ends** (2026-11-06):
  - Every keyring secret is scoped by node id. Sockets are node-scoped in a standard per-user runtime location; making them configurable comes later.
  - `day init` on an existing node: the manager's pick is idempotent (exit 0, unchanged), with a refusal only when the directory holds something that is not a node.
  - Which peers auto-connect is user-node vs relay-node policy, but either way a node has a set of peers to dial at start: a bootstrap list.
  - Mother and sister nodes: only ADR 033 §2 names them (user-node composition, Dayframe); no design exists yet. They belong to the user-node composition ADR.
  - Drawers and documents stay separate authority domains (ADR 022 updated).
  - PollDatums gets no marker event for matching docs with no selected rows: the datum selection is a filter too.
  - Datum value newtypes use Automerge's scalar names (`F64`, not `Float64`); integers carry no widths, as in Lexicon.
  - Renames are pre-approved: "feel free to rename anything".

- **D34. "Plug" becomes "cartridge"** (operator, 2026-11-06): "plugin" implies a slight enhancement of the app, but many micros are big, self-contained apps. A cartridge (everyday "cart") is the one user-facing noun for both the store item and the distribution unit: it bundles micros, nodules and (later) workflows. Micros and nodules stay user-facing too, as the classes of thing a cartridge contains; the store disambiguates by them. Deferred: micro access to node services, until feat/big-node merges and Clay runs on top of the node. Also decided: one typed-call model across micros and nodules (`Operation` plus the generated `Client<Proto>`, with host-mediated backends). The code rename (`big_plugs*`, `Plug`, `plug_*` crates, ADRs 027/028) waits for the Clay `datum_core` PR, which touches the same crates.
  - **Rename in progress (2026-11-11):** pre-release, so a plain rename with no aliases. Crates `big_plugs*` become `big_cartridges*`, `Plug*` become `Cartridge*`, `plug_id` becomes `cartridge_id`, NSIDs `org.example.daybook.plug(s)` become `...cartridge(s)`, SQL via new migrations. Out of scope: the old daybook plug system (`daybook_core/plugs*`, `daybook_cli` plugs, `daybook_ffi`, `daybook_types` plug manifests, `plug_*_manifest`/`plug_*_routines`, `xtask/plug.rs`, `count_plug_pins*.sql`) and every `*plugin*` name. Entries below this one keep the old names as history.

- **D35. Commands for users, XRPC names for developers** (operator, 2026-11-06): user-facing, a cartridge offers *commands*; developer-facing, they are XRPC *methods* (query, procedure, subscription). The `Operation` trait becomes `Method` in its own wasm-buildable crate after the datum_core PR (#82) merges.

- **D36. Our keyhive fork is ours to fix** (operator, 2026-11-06): keyhive is being rewritten from scratch upstream, so fixes and workarounds go straight into our fork (`townframe-changes`); when the rewrite lands, compare our delta with it and port the fixes. Also: work concurrently where tasks are independent; don't block on one task.

- **D37. Closing open questions** (operator, 2026-11-06):
  - Live schema registration is not open: enabling a plug registers its Lexicons in `SchemaRepo` under the plug as owner and disabling retires them (`big_nodule/state.rs`). Already shipped.
  - Many nodes sharing one SQLite database: not a goal (each node gets its own database, as it would on Postgres). Dropped from N3.
  - Consent, permissions and actors, and the authoring naming questions (`cartridge-authoring.md`), are next week.
  - Test threads under load, the ledger cleanups and the small follow-ups: manager decides.

- **D38. Week of 2026-11-09: big_ndk, relays and intxt as the grounding** (operator, 2026-11-09/10):
  - **The backend framework is `big`**, as Clay is the frontend's: the `big_*` crates are the vocabulary nodules use and what nodes are built from. The nodule kit is **`big_ndk`** (beside `clay_mdk`, `wflow_sdk`), a working name until a better one appears. Naming roles as the operator sees them: daytxt is the app (the "OS" you interact through); dayframe is the user's computer (it runs nodules and services; Clay is the user agent to it); townframe is the communal product (subreddit-like); "node" names the networked role; the browser deployable is a *webnode*, not "webframe". Where frame ends and node begins is undecided.
  - **This week's goal:** deliver the NDK, then the day node and relay node compositions, built top-down from intxt's requirements rather than bottom-up from the runtime. The manager owns the NDK experience and relays; most implementation goes through Sonnet subagents. A third (Opus) lane will do intxt's backend: its datums, micro and cartridge.
  - **intxt alpha (operator):** users push intxts through their own node to a relay acting as their mother node; the web app is a Clay client of the relay and browses *public* intxt drawers only (no web sign-in until webnodes, which wait for upstream keyhive to stabilize). No sponsorship for the alpha.
  - **`day` never builds source.** Its input starts at built wasm plus manifest, and it stays language-neutral; tools that wrap cargo and misread custom builds are the failure to avoid. No extension system for now (git-style `day-xxx` on PATH is the escape hatch if ever needed). `day call` becomes `day exec` with the alias `x`.
  - **Progress (2026-11-11):** merged #82 (`datum_core`) and mbx tooling into feat/big-node (`0484534d`); `method_core` with `Operation`→`Method` (`a1f67718`, `a1b20739`); D39 header contract (`bc55f903`, `82a0d3e5`), workspace clippy clean. In flight: big_ndk v0 step 1 (crate, `Handler`, `Cx`, `export_nodule!`, wasi:http client; `test_nodule_guest` rewritten on it). Next: native test harness, `CartridgeBuilder` + custom-section manifest, then the intxt nodule's `Source::Patch` path, then R19 mounts. Waiting on the operator: squash INSTR, push feat/big-node, open its PR on #82.
  - **Native test harness (`big_ndk_testing`):** `big_ndk_testing::node().with_nodule(plug, nodule).boot()` boots a real in-memory `BigNode` through `boot_with`, hosting the nodules on a native executor that reports kind `wasm` and sends handler egress through `big_nodule::egress::call` with the nodule's actor capability (principle 6: tests only). Each hosted nodule owns a thread with a current-thread runtime and `LocalSet`. Accepted duplication: the boot config and secrets helpers are copied from `big_node/e2e/mod.rs` (about 80 lines). The fixture handlers moved to `test_nodule_handlers` so the wasm guest and the native smoke test share them.
  - **Queue (2026-11-11, operator stepped away; relay scoped to what intxt needs):** (1) in flight: object bodies for the 9 pending methods, `day call` -> `day exec` (alias `x`), the PollDatums gap test; (2) the relay alpha per ADR 038 §3 (a `day` relay mode on mothership, accept-all peer policy, membership-driven mirroring, plus the user side: add a mother node and give it `relay` on new drawers), proposal first; (3) ADR 037 step (a), holder binding in every domain; (4) D41 steps 2-4 (error names, validator, dispatch validation, GET); (5) ADR 037 steps (b)-(c), then the act-as-descendant env var intxt asked for. Open with the operator: the force-push of #84 (lease `8cfa1b16`).
  - **D41. Method Lexicons** (manager, 2026-11-11, by atproto precedent; proposal in the session transcript, summarised here):
    - Every `Method` carries an atproto Lexicon file (`{lexicon:1, id, defs:{main, operationError?, …}}`), emitted host-only by `methods!` / `service_method!` under the `schemars` feature from the request, output and error types, projected with the datum Lexicon projection and checked by `datum::operation::validate`. Assembly is in a new host-only crate, `method_lexicon`; method_core stays small and wasm-buildable.
    - The Lexicon is the single source: dispatch validates input and output against it at the router (the one choke point for built-ins and nodules), GET query strings are coerced by its `parameters`, and MCP `inputSchema` and CLI arguments are derived from it. Nodule methods have only a Lexicon, so built-ins validate the same way.
    - Query requests are flat primitives (a projection error otherwise). Bodies are objects: `Vec`/scalar outputs are wrapped, `()` means no `output`. Pre-release wire break accepted.
    - Errors: XRPC `error` is the variant name and `message` its Display; `operationError` stays as the typed body. A method lists its domain variants by outer name; `ContextError` and dispatch refusals are global common errors, not per-method entries.
    - No stricter sidecar layer for methods: Lexicon strength suffices, as in atproto.
    - Step 1 landed (`defe64ac`): 59 of 68 built-in methods publish a Lexicon that passes frame admission; 9 with non-object outputs are pinned in `PENDING_OBJECT_BODIES` until a separate output-wrapping commit (about 35 files). Accepted projections: internally tagged enums become an object with a closed tag `enum` and optional variant fields (wire unchanged, validation looser than the Rust type; atproto `$type` unions would be a later wire change); externally tagged payload variants become one optional property per variant; any-JSON values become `unknown`; errors list outer variant names, skipping the `ContextError` carrier; `operationError` is not published. No divergence from atproto (operator, 2026-11-11: we are a strict superset; extra metadata may only optimize storage or typing): every any-JSON position holds an object, because a datum's `main` is a record and method bodies are objects, so atproto `unknown` fits exactly. An earlier note here proposing to treat such positions as any JSON is withdrawn.
    - Step 1b landed: the 9 non-object outputs are wrapped (`BlobStat`, `CapabilityChange` (the `observeCapability` event), `CartridgePublisher`, `CartridgeList`, `QueriedDatumRefs`, `ListedDocs`, `KnownDrawers`, `ListedDrawers`, `NoduleList`); an absent `Option` is an absent field, a list is `{ <plural>: [...] }`. All 68 built-ins now publish a Lexicon and `PENDING_OBJECT_BODIES` is gone. The three drawer listing rows and bodies moved to `big_cabinet_core::drawer::listing` (module budget), re-exported from `protocol`.
    - Plan: (1) Lexicons for every built-in method, plus a frame-admission test chain; (2) the error-name wire change; (3) a Lexicon-position-to-validator compiler plus parameter coercion; (4) dispatch validation and GET on `/xrpc`; (5) the D40 contract section via `build.rs`, plus `day cart pack`; (6) `day mcp` and generated CLI subcommands.
  - **D40. The cartridge rename and where manifests live** (manager, 2026-11-11):
    - Rename scope: `big_plugs*` -> `big_cartridges*`, `Plug*` types -> `Cartridge*`, NSIDs and datum ids to `…cartridges.*` / `…cartridge`, `plugId` -> `cartridgeId`, new SQL migrations renaming tables and columns. Pre-release, so no aliases or compatibility reads. Out of scope: the old daybook plug system and the `plug_*` crates (superseded model), and every `*plugin*` name (wasmtime/wash plugins).
    - Manifests: supersedes D38's "manifest in one component's custom section" proposal. A cartridge bundles several components, and a manifest naming its own artifact's digest is circular. So each component carries its own contract (implements, requires, Lexicons) in a custom section, generated from the declarations crate by a native `build.rs` (the `micro_counter` / `dayframe.manifest` precedent, shared with Clay). `day cart pack` assembles the cartridge manifest by reading the sections (`wasmparser`) and hashing the finished components. `day` still never builds source, and a TypeScript component needs only the same section.
    - Prerequisite: `Method` types carry Lexicons (D31), the same work as GET through `/xrpc`, generated CLI subcommands and `day mcp`.
  - **D39. One XRPC header contract on every hop** (manager, 2026-11-11, by precedent; investigated against the code first). The four paths disagreed: client->node used `Bearer <authority>.<signer>` and relative `x-timeout-ms`; dispatch->nodule delivered `Capability <authority>` and absolute `x-request-deadline-unix-ms`; nodule->`dispatch.local` egress required `Capability ` and read only the absolute deadline (dropping `x-timeout-ms` silently); request id and trace context were never delivered and egress restarted them with `RequestContext::root()`. So a guest could not reuse `encode_request`. The contract:
    - `Authorization: Bearer <token>` (XRPC/atproto), scheme matched case-insensitively (RFC 9110). To a guest and from a guest the token is a bare capability id: the caller's signer never enters a guest, and egress always takes the signer from the host's per-workload table, so a nodule acts as its own actor presenting the caller's capability (the caller-authority decision in `big-ndk-intxt-sketch.md`).
    - `x-nodule-capability` stays (the nodule's own grant, for its private state), value a bare capability id.
    - Deadlines are relative `x-timeout-ms` on every hop, each hop re-deriving the remaining budget from its own context (gRPC `grpc-timeout`); no absolute wall-clock deadline on the wire.
    - `x-request-id`, `traceparent`/`tracestate` are delivered and parsed on every hop (Envoy, W3C Trace Context); `idempotency-key` unchanged.
    - Amended after implementation (`bc55f903`): `x-request-id` is per hop, not per chain. It is our admission and cancellation key (`CancellationAlreadyAdmitted` refuses a duplicate), and a nested call always runs while its outer call is in flight, so reusing it can collide. The chain is carried by W3C trace context: the nested call keeps the trace id and gets a new span. An exhausted budget has one error, `ContextError::DeadlineExceeded`; dispatch's separate `DispatchError::DeadlineExceeded` raced it and is removed.
    - `Provider::call` receives the request context (budget, ids, trace), which also closes the slice 2 follow-up of passing the verified caller into providers.
  - **MCP and CLI from Lexicons (manager proposal, operator leaning):** rather than an MCP server per nodule, `day mcp` exposes the methods a capability reaches as tools, and installed cartridges get generated `day` subcommands, both from the methods' Lexicons. With GET through `/xrpc` (query params need the Lexicon to type them) and D31 input checking, that is one piece of work with four outputs.

## Deferred

wflow service and durable procedures; POSIX providers; triage/processors; big_tasks; network/XRPC exposure; consumers of command/view bindings; single-instance services; generalising `NoduleGrants` to operation grants.

Big_cabinet read/write options discussed with the operator 2026-11-06, deferred because none is a pure win yet:
- Store the gate verdict per datum-set row at projection time (the gate is a function of doc and heads), so pages and `readDatums` at projected heads skip re-verification; other heads fall back to the gate. Saves CPU, not wire, on a local node. Superseded by an LRU (operator, 2026-11-06), itself deferred: no LRUs until the low-hanging costs are fixed, because a cache hides design wrong turns. the gate's verdict at doc heads H splits into (1) the current signatures verify — a function of the datum heads alone (the signatures' introductions; history there is immutable), the crypto-heavy half — and (2) the value at H equals the value at the datum heads, cheap and still per read. Cache (1) keyed by `(doc_id, datum_key, datum heads)`: entries never go stale, survive unrelated doc changes and cover historical versions; a new signature is a new introduction and so a new key. Reads keep running (2). Datum heads are the universal datum version id: the same key serves hydrated values. Port the old drawer cache (`daybook_core/drawer/cache.rs`: `(doc, facet uuid)` + heads match, `utils_rs::lru::KeyedLruPool` cost-weighted, second-hit admission) rather than writing a new one. Measure `verify` per read before adding it.
- `readDatums` patch mode (`since` heads -> `doc.diff` patches per datum), and a splice input format for text datums so a keystroke is not a full-value upload. Today full-string writes already become splices (`TextReconciler::update`), so merge intent is preserved; the gain is wire size for remote clients.
- Client-held Automerge (editors producing changes locally): only pays for remote, offline clients without a local node. Datums leaving the node must be signed either way (debounced checkpoint claims); delegated session keys via `ActorMetadata.delegation` beat handing private keys to subsystems.
- Automerge 0.12 authors (PR #54, stale against big_cabinet): defer until subduction checks the commit signer against the author footer. `Author` is an unauthenticated change footer; `set_author` re-randomizes the actor, so ADR 020's per-doc actor must be re-set after it.
- Two-phase write: today's dry stage + rollback; alternatives are a pure JSON->native claim mirror (one stage; also lets the authority check what it signs, no-op write detection, ETags) or staging on a clone held across the signing await (needs a per-doc writer gate: the clone's actor would collide on seq). Automerge has no detached change builder (`transaction*` take `&mut self`). Re-measure the dry stage after cost 2 before choosing.

## Open questions for the operator

- **`day` CLI (landed `39888307`), questions from the port:**
  1. **Wrapping key scope.** Resolved (operator): the wrapping key is the keyring entry `big_cabinet.authority` / `wrapping.<node id>`; `big_node::delete_secrets` lists every entry a node keeps. The `day` blackbox tests delete their node's entries when each case ends. An old unscoped `wrapping` entry is never read again: a node directory booted before this change would find no key and refuse to boot (`StateCorrupt`) instead of silently re-keying. No release has shipped, so nothing migrates.
  2. **Socket location.** Resolved (operator): `$XDG_RUNTIME_DIR/day/<node id>.sock`, falling back to `/tmp/day-<euid>/<node id>.sock`; one function (`default_socket_dir` in `day_cli/context.rs`), not configurable yet.
  3. **`day init` on an existing node.** Resolved (operator): idempotent, prints `node <id> already initialized at <path>` and exits 0; a `node.json` that is not a valid node is an error (exit 1) and is left alone.
  4. **`nodule_operations` default** in `node.json` is empty (nodules can invoke nothing); the big_node tests use `{authorities: ["org.example"]}`.
  5. **Peers.** Resolved (operator): `node.json` has an optional `peers` section (`bootstrap` endpoint tickets, `loopbackOnly`); the daemon attaches the peers service when it is present and dials the list. `org.example.daybook.node.getNodeInfo` (built-in provider `builtin:node`) reports the node id, its own ticket and the connected peers; `day status` and `day peers ls` read it. `day peers add <ticket>` edits `node.json` and takes effect at the next daemon start.
  6. **Identity seed at rest.** `node.json` holds the seed in a 0600 file, not the keyring; ADR 033 says data is encrypted at rest.
  - Gaps big_node exposes: no operation lists providers/operations, reports the node id, peers, readiness or version, or stops the node (the CLI uses SIGTERM plus the pid in its lock file).

- Lifecycle review questions 1–7: resolved 2026-11-04. See ADR 027 §10 and ADR 028.


- Resolved: ADR 027 was revised in place with D1–D6 (`01ea305b`).
- Resolved (D31): integer widths lean on spec compatibility.
