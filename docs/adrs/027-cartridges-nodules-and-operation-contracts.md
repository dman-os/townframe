# ADR 027: Cartridges, nodules, and operation contracts

**Status:** Accepted direction. The portable descriptions, the inert static catalog,
the standalone nodule inventory and the datum-backed management service are implemented
(`big_cartridges_core`, `big_nodule_core`, `big_nodule`, and Cabinet's operation-Lexicon
admission; see §8); dispatch, execution and artifact materialization are the staged work
of the `feat/nodules` PR (§8), and nothing executes yet. This records the agreed
responsibilities, authority model, versioning, and staged scope. Concrete wire/guest
contracts still needing specification are listed in §10; they do not reopen the
decisions below. The operator-approved runtime decisions D1-D6 of the
[runtime ledger](../scratch/nodule-runtime-ledger.md) are folded in as normative text
(2026-11-04). The [lifecycle review](../scratch/nodule-lifecycle-review.md) questions are resolved in §10.

**Amended by:** [ADR 028](028-cartridge-identity-naming-and-dependencies.md) on cartridge identity,
naming, dependencies, forks and substitution. Where this ADR describes package dependencies or
the catalog role, ADR 028 is authoritative.

**Updated:** 2026-11-04

**Related:** [Multiprocessing notes](../DEVDOC/log.md#2026-07-04--multiprocessing),
[ADR 007: cartridge manifests](007-cartridge-manifests-as-drawer-docs.md),
[ADR 011: task pools](011-local-first-task-coordination.md),
[ADR 019: drawers](019-cabinet-drawers-and-selective-mirroring.md),
[ADR 020: documents](020-documents-repo-and-datum-writes.md),
[ADR 021: schemas](021-datum-schema-language-and-automerge-codecs.md),
[ADR 022: authority](022-authority-actors-capabilities-and-grants.md), and
[drawer requirements](../fdrs/001-drawers-as-document-collections.md).

## Context

The new services are built alongside `daybook_core`, as Cabinet was, rather than
by modifying or relocating its live repositories. Existing code is evidence for
responsibility boundaries, not a requirement to preserve its dependencies or
manifest format. Stateful services should be usable from an embedded application
or through a daemon without changing their domain contracts. The default is one
owner for a service instance; multi-writer storage and cloud placement are separate
investments.

The current plugin surface already distinguishes management from execution, but
its contracts couple concerns that the new design separates:

- [`CartridgeManifest`](../../src/daybook_types/manifest.rs) carries package identity,
  facet schemas and display hints, local state, dependencies, workflow bundles,
  routines, views, commands, initializers, and processors.
- [`PlugsRepo`](../../src/daybook_core/plugs.rs) combines cataloguing, pinned
  enablement, manifest materialization, derived caches, and observation. Its
  [OCI importer](../../src/daybook_core/plugs/oci.rs) obtains executable artifacts,
  and its [validator](../../src/daybook_core/plugs/validation.rs) understands routines
  and command targets.
- [`Rt`](../../src/daybook_core/rt.rs) composes dispatch, workflow execution,
  initialization, Wasm host facilities, stateless views, and local state. These
  responsibilities are not all part of the package repository today either.
- The old drawer consults cartridges for schema validation, while cartridges attach the
  drawer after construction to read and write their own documents. Moving those
  mutually dependent repositories into crates would preserve the wrong boundary.
- Commands, initializers, and processors name routines in the same manifest.
  Routines declare execution details and requested access but do not identify
  independently declared XRPC operations they implement.
- [`BigNode`](../../src/big_node/lib.rs) already composes Cabinet services.
  [`SchemaRepo`](../../src/big_cabinet/schema.rs) admits datum schemas independently
  of cartridge execution, and the `service_*` crates supply shared service contracts,
  clients, and hosts.

The central distinction is: **declaring an operation, providing its implementation,
and choosing how to invoke it are three different things**. There is one nodule
surface per cartridge, not independently installed nodules per processor. Separate
service-instance lifecycles, background-worker contribution types, and a new
routine-to-routine messaging system are not required.

## 1. Packages, the cartridge service, and consumers

A **cartridge** is the application-level distribution, namespace, and versioning unit.
Rather than treating a nodule and a micro as sibling component kinds, a cartridge has
two consumer-specific facets:

- The **nodule** is the cartridge's singular **node-facing facet**: everything in the
  package that applies to BigNode. The runtime understands three things in it:
  **services** (Lexicon operation contracts and the providers implementing them),
  **facilities** (the resources a nodule is granted: private state, revisioned stores,
  artifacts, drawers, network) and **lifecycle** (install, enable, upgrade, retire,
  initialize). Everything else it carries (commands, views, processors) is a
  **binding**, described in §2. Its identity is 1:1 with the cartridge's identity.
- The GUI facet contains zero or more **micros**, where each micro is an individual,
  specifically addressed frontend contribution (such as a gallery view, picker, or
  inspector) namespaced under the cartridge.

Schema-only and micro-only packages are valid; neither requires executable node
behavior. Multiple artifacts, routines, or operations inside the node-facing facet
do not make a cartridge multiple nodules.

The **cartridge service is a catalog of static package information**: identities,
manifest revisions, version labels, metadata, distribution dependencies, and
content-addressed artifact/contribution references. It ingests package envelopes
from synchronized drawer documents ([ADR 007](007-cartridge-manifests-as-drawer-docs.md))
or import adapters (such as OCI registries or local bundles), verifies artifact
digests, and tracks whether referenced blobs are known, materialized locally,
missing, or corrupt. It does not own executable enablement, runtime configuration,
grants, network exposure, or nodule/micro readiness.

The shared package envelope carries schema-identified contribution descriptions
with immutable revision/content references. The catalog validates that envelope and
its distribution references without interpreting nodule routines, Clay components,
or their permission rules. A consumer interprets and admits the contribution types
it understands; unknown contribution kinds can be preserved without executing them.
A packaging tool may understand both facets to assemble a release, without making
the catalog depend on either runtime's implementation or a shared GUI/nodule enum.

The two consumers are independent:

- The nodule runtime consumes the node-facing description and owns nodule enablement
  and configuration.
- Clay's micro host consumes the frontend descriptions and owns micro enablement
  and configuration.

Both use the same catalog contract. Enabling a micro does not enable its package's
nodule or grant access to it; disabling a nodule does not erase the package or
implicitly disable every micro. A micro may declare dependencies on nodule
operations and invoke them through authorized service clients. This logical
relationship does not require either runtime to link the other's implementation.
The detailed micro-to-nodule integration is deferred.

BigNode understands the nodule contract, not Daybook packaging, micros, or OCI.
An integration makes catalogued nodule descriptions available to its runtime;
another application can install nodules using another distribution mechanism.

Keep these identities distinct:

| Identity | Meaning |
| --- | --- |
| Cartridge / nodule identity | Single package and node-facing extension identity, stable across ordinary upgrades |
| Version label | Semver label on a package release for discovery and dependency constraints |
| Manifest/contribution revision | Immutable content-addressed description selected for installation or dispatch |
| Operation NSID | ATProto Lexicon contract identity, independent of its provider |
| Contribution key within a nodule | A provider component, revisioned store, initializer or binding inside the nodule facet |
| Micro key within a cartridge | An individual GUI contribution, namespaced under its package |
| Node and actor identities | Network participation and cryptographic attribution |

Because the nodule is the node-facing facet of the cartridge, it shares the cartridge's
namespace rather than introducing a second independently versioned distribution
unit. Each installed nodule is provisioned a distinct local actor, not its own
network principal. Cabinet is library machinery and a conceptual collection of
drawers, not an identity to add to this table.

## 2. Manifest model: contracts, implementations, and uses

### Operation declarations (ATProto Lexicon)

We directly adopt ATProto Lexicon's three operation kinds—**`query`**,
**`procedure`**, and **`subscription`** (matching
[`XrpcKind`](../../src/service_core_rs/operation.rs))—alongside Lexicon datum
schemas:

- **`procedure`:** POST-style request/response operation that may mutate state,
  exercise capabilities, or trigger side effects. As in
  [`big_cabinet_core`](../../src/big_cabinet_core/doc.rs), early node and nodule
  surfaces primarily use `procedure` operations.
- **`query`:** side-effect-free read operation with typed parameters and output.
- **`subscription`:** cursor-resumable event/update stream contract.

All three declare typed input, output/event, and error shapes. A declaration can be
distributed by a nodule that has no implementation of it. Datums and operations use
the same schema language ([ADR 021](021-datum-schema-language-and-automerge-codecs.md))
but have different admission and runtime responsibilities.

An operation NSID does not name a routine, package, node, or transport. Where we
extend beyond vanilla ATProto XRPC is in how a nodule binds implementations,
invocations, and cursored state to those NSIDs. A service-group abstraction above
individual NSIDs is not required.

### What the runtime understands, and what it only carries

The runtime models exactly three things: **services** (operation NSIDs of kind
query | procedure | subscription, plus their providers), **facilities** (granted
resources such as private state, revisioned stores, network, artifacts and drawers)
and **lifecycle** (install, enable, upgrade, retire, and initialization). Admission
validates only identity and authority, Lexicons, implemented operations with their
handler artifact, facilities, and initializers.

Everything else is a **binding**: consumer-owned metadata that names an operation NSID.
Commands (invoked from GUI, CLI or agents), views (rendered by Clay) and processors
(triage, deferred) are bindings. The runtime carries them as open, schema-tagged
manifest sections and does not read or validate them, so a vocabulary member it does
not know inside a binding never refuses a nodule. Unknown vocabulary inside an
*executable* part (a provider or facility) makes that service unavailable with a
reason; it does not refuse the nodule either. The consumer of a binding validates it.

**Initialization** is the one binding the runtime owns, because it is lifecycle: "on
revision R, invoke operation X with run mode M". Its prior art is
[`rt/init.rs`](../../src/daybook_core/rt/init.rs). The redesign of its identity,
modes and gating is under review (§10, proposals), but the shape above is settled.

**Cross-nodule calls invoke operations, not commands.** A command is only a consumer's
saved way to invoke an operation; the callee never sees one.

### Routine implementations and long-polling

Retain **routine** for an executable implementation inside a nodule. Each routine
identifies the operation NSID it implements, artifact/export references, private
state requirements, and requested host facilities. These belong to the provider, not
the Lexicon contract.

A nodule's synchronous provider is a **Wasm component serving XRPC** (§3 "Dispatch"):
one handler component that answers `/xrpc/<nsid>` for every operation it implements.
Durable execution is not a routine kind of the nodule runtime. It is a separate
**wflow service** with start, status long-poll, message and cancel operations; a
durable procedure returns a job reference, and by convention durable document
mutations default to wflow, whose journal is the storage so components stay
stateless. The wflow service and nodule support for it are deferred (§8).

Wasm is an execution environment, whereas workflow is an execution semantic; a
workflow runs in Wasm. The current manifest's `routines` list with a `call | workflow`
execution kind is superseded by the handler-plus-`implements` shape in §3; reshaping
the serialized manifest is a proposal under review (§10).

**Long-polling over cursors is our primary primitive for live observation and
subscriptions**, matching [`big_sync_core`](../../src/big_sync_core/revisioned_store.rs).
Because a caller always resumes from a monotonic cursor (`after: Revision`),
long-polling is naturally implemented by an ordinary **asynchronous call** routine
(or a host-managed revisioned store reader) that returns immediately when revisions
after the cursor are available or waits up to a bounded hold window before returning
entries (`Entries` / `ReplayComplete`) and the advanced cursor. Consequently:

- Subscriptions do **not** require a third routine execution kind or a mandatory
  WebSocket push-stream guest ABI.
- A declared `subscription` or watch operation can be served by the host driving
  cursored long-poll calls against a revisioned store or `call` routine.
- Dedicated guest push-streaming bindings remain optional future work that can be
  designed in a separate PR if a use case cannot be expressed via cursored
  long-polling.

WASI and WASI HTTP provide host interfaces, not blanket permission to use them.
The runtime supplies only the authorized network destinations, storage, resource
clients, and other facilities. There is no second permission mechanism that treats
an imported interface or a requested manifest permission as an actual grant.

### Revisioned stores

A nodule can declare **revisioned stores** as a first-class node-facing contribution,
built on the [`RevisionedStore`](../../src/big_sync_core/revisioned_store.rs)
contract:

- The store owns strictly increasing, source-assigned revisions (never wall-clock
  timestamps) and an explicit `ReplayComplete` boundary between historical catch-up
  and live waiting.
- Each declared store specifies its key/selector shape, entry schema (referencing
  admitted Lexicon types), and whether it retains a keyed frontier (latest value per
  key) or an ordered log.
- A revisioned store backs cursored long-poll operations and subscriptions locally
  and across nodes without forcing consumers to share SQLite files or invent ad-hoc
  event buses.

### Bindings and initialization

Commands describe explicit invocation by a person, script or agent. Processors
describe reactive invocation and triggering policy. Both are bindings (see above)
whose targets are **operation NSIDs, not private routine keys**. A nodule also declares which external operation
NSIDs its routines or commands **require** (`requires`), separate from the
operations its routines **implement** (`implements`). The runtime resolves an
eligible implementation; the consumer never depends on the provider's private
routine names or storage layout.

| Contribution | Declaration |
| --- | --- |
| Search vocabulary | Declares the Lexicon operation `org.example.search.query` |
| SQLite search provider | Routine `search` `implements` `org.example.search.query` using its private index |
| Photo browser | Declares `requires: [org.example.search.query]`; command `findPhotos` targets `org.example.search.query`, optionally presenting its result |

A different provider can implement that operation without changing the command.
A nodule calling another nodule invokes the **operation** through dispatch (§3); a
command is never the cross-nodule interface, and there is no second routine
messaging protocol. Commands are not automatically durable jobs.
Initialization likewise declares an operation to invoke and a run mode. The runtime
owns its lifecycle intent and, once providers can run, its execution through
dispatch (§8); registration is not proof that initialization ran.

### Stateless presentation

Stateless views remain a **nodule contribution**, not something that requires a
micro. Nodules are the primary general-purpose feature plugin system for both GUI
and CLI use, not merely backup or background plugins. Micros supply bespoke frontend
composition when portable presentation is insufficient.

A command can associate its typed operation result with a stateless presenter:

```text
command -> operation -> typed result -> optional presenter -> portable view
portable view -> GUI renderer or CLI renderer
```

Scripts can obtain typed data without rendering. An operation or presenter
specifically returning a view declares that typed input and portable view output
explicitly, and its presenter logic executes as an ordinary stateless `call` routine
rather than a separate plugin execution engine. A stateless presenter produces a
portable description; it does not retain a frontend component instance. The feature
itself may still use persistent state.

[Today's ViewSpec](../../src/daybook_types/view.rs) demonstrates this boundary, not
a frozen choice of representation. The portable vocabulary needs terminal rendering
rules for visual elements and actions. Rendering must not invoke actions implicitly;
actions go through the same authorized operation path as other invocations. This
requires neither a CLI-specific plugin system nor importing Clay renderer types
into the nodule contract.

### Semantic manifest structure

The required separation is below. This fixes responsibilities and reference
semantics, not final serialized field spellings:

```text
PackageEnvelope
  package identity, version label, metadata
  immutable manifest revision, distribution dependencies, artifact references (with digests)
  contributions:
    nodule: optional schema identity + pinned NoduleDescription reference
    micros: zero or more keyed schema identities + pinned MicroDescription references

NoduleDescription
  nodule identity (derived from package identity)
  config schema: optional Lexicon schema and defaults for mutable runtime settings
  lexicons: declared datum definitions and operation contracts (query | procedure | subscription)
  -- understood by the runtime --
  requires: operation NSIDs and host facilities required by this nodule
  handler: the one XRPC handler component (artifact key) serving /xrpc/<nsid>
  implements: operation NSIDs declared in this nodule's Lexicons that the handler serves
  revisioned_stores: key, selector/entry schemas, frontier/retention mode, source (an implemented operation)
  initialization: key (unique), target operation NSID, run mode
  private_state: named SQLite/KV state declarations
  -- bindings, carried opaquely (an open map keyed by section tag) --
  commands: key, target operation NSID, optional stateless view key
  views: key, typed input schema, portable view output, presenter operation
  processors: key, target operation NSID, trigger policy

NoduleConfiguration
  selected immutable nodule revision, desired enablement
  validated runtime settings (conforming to config schema)
  installation-specific resource bindings (drawer grants, creation placements)
  provider bindings for required operation NSIDs
  per-contribution controls, discovery/invocation policy, network exposure policy

MicroConfiguration
  selected micro revision, desired enablement, frontend settings
```

The catalog owns `PackageEnvelope`, not either configuration. The runtime validates
the understood sections: operation implementations against admitted contracts,
settings against `config schema`, and implementation compatibility; it does not
require an implementation of the same name in the consuming manifest, and it does not
interpret binding sections. Processor descriptions are specified for future use even
though nothing executes them yet.

## 3. Service semantics and multiprocessing

The canonical service interface is **XRPC over HTTP**, with direct calls and IRPC
as optimized paths, following
[`service_utils_rs`](../../src/service_utils_rs/lib.rs). This is not protobuf/gRPC.
Validation, authorization, errors, cancellation, and operation semantics must agree
across paths; an in-process optimization cannot bypass hosted policy.

Use the existing division:

- `service_core_rs`: portable operation, protocol, and request-context contracts;
- `service_client_core_rs` and `service_client_native_rs`: clients usable without linking the native service host, the core buildable for wasm guests;
- `service_utils_rs`: hosting, middleware, routing, and lifecycle.

A client holds service access, not a database, mutex, filesystem path, or
`Arc<PlugsRepo>`. Internal handles are legitimate within the hosting process, not
the external nodule contract. Moving a client across processes does not create
another node identity or another owner of the same service state.

The existing operation machinery uses typed Rust requests and static descriptors.
Runtime-loaded Lexicons and providers need a dynamic admission/routing boundary
rather than being cast into compile-time protocols. It must reuse service semantics,
not introduce a competing RPC stack. A nodule must eventually be able to express
the [sample service's](../../src/service_utils_rs/sample/protocol.rs) queries,
procedure, revisioned reads, and cursored long-poll/subscription behavior as
operation implementations.

### Dispatch

**Dispatch is the nodule runtime's XRPC reverse proxy.** Its contract lives in
`big_nodule_core` (the service's home) and its host in `big_nodule`. For one call it
(1) does ocap admission by checking the presented capability against live
capabilities ([ADR 022](022-authority-actors-capabilities-and-grants.md)) and that its
scope covers the NSID, (2) resolves NSID to provider and gates on that provider's
readiness, and (3) forwards. It keeps no per-call state, writes nothing durable for a
non-durable call and does no I/O on admission. It validates input and output against
the Lexicon at the edges only.

**Routing table.** Modelled on wash-runtime's `Router`/`DynamicRouter`: a synchronous
snapshot lookup `Nsid -> ProviderEntry { kind, provider_id, operation_kind, readiness }`
with a reverse index per provider, so `unbind(provider)` is idempotent and cheap. The
nodule reconciler drives `register(provider_id, entries)` and `unbind`. Ambiguous or
missing providers are errors, never installation-order fallbacks (§5).

**Provider kinds.** `BuiltIn` is a native service reached through its typed
`service_utils_rs` client, with no JSON on the path beyond dispatch's own edge. `Wasm`
is a nodule component hosted by wash-runtime. `Posix`, the wflow service and `Remote`
are later kinds. Dispatch knows the provider kind and operation kind because some
capabilities are granted **by type**: those are checked **at bind time**, modelled on
wash-runtime `BindingSchema`/`narrows`, so a call does not re-derive them.

**Who calls dispatch.** NSID routing is dynamic, so its callers are guests, XRPC
ingress, the operator or CLI invoking by NSID, and later tasks. In-process native
callers of a built-in service keep using the typed client with their own capability and
bypass dispatch; the JSON `Value` at dispatch is an edge that path would pay anyway.

**Wire contract** (`big_nodule_core::dispatch`, in the style of `protocol.rs`, each
operation carrying an `Authorization`): `dispatch.invoke { authorization, operation:
Nsid, input: Value, context { deadline?, idempotency_key? } } -> { output: Value }` and
`dispatch.mintCapability { authorization, scope: DispatchScope, rights }`, whose
attenuation follows `mintNoduleCapability`. `DispatchError` distinguishes `Denied`,
`UnknownOperation`, `NotReady { reason }` (retryable, reasons mirror per-service
readiness), `InvalidInput`, `DeadlineExceeded` and `Provider { status, body }` (the
provider's own XRPC error). This is the working contract; names may move during
implementation.

**Guest model.** A nodule is a wash workload with one HTTP-shaped XRPC handler
component serving `POST /xrpc/<nsid>` (`GET` for queries). The semantics are our own,
aligned with `service_utils_rs`; `wasi:http` p3 is the first transport, and we may swap
it for our own WIT export of the same shape. Lifecycle follows wash's.

**Authority and instances.** Authority is a header: the proxy checks it, and it becomes
the authority of that dispatch. Each dispatch gets **one fresh instance** (no pooling
across dispatches), and the store's host context is the request context, so a guest
cannot leak authority between calls. Deadline and idempotency key travel as headers
that the guest SDK forwards.

**Nodule to nodule.** A guest makes an outgoing request to the reserved authority
`dispatch.local`. Our `Router::route_local_egress` declines it, so it lands in our
`OutgoingHandler`, which receives the workload id; the workload id maps to the nodule
actor. Dispatch admission then runs as for any caller: the header names the capability
and the host checks that the capability belongs to that actor. There is no other
cross-nodule path.

**wash-runtime seam.** wash-runtime has no public in-process entry (checked at the
pinned `1bcaeba` and at upstream `3de4e69`). The fork `dman-os/wasmCloud` adds
`Ingress::dispatch_request(workload_id, Request<WasiBody>) -> Response<WasiBody>` over
its private `resolve_local_target` and `dispatch_local`. Until the operator pushes the
fork, a local `[patch]` is used; follow `.agents/skills/fork-pinning-and-upstream-sync`.
Keeping the provider behind a service boundary lets the Wasm host move out of process
later. Guest long-poll subscriptions are the exception to stateless dispatch; host-served
revisioned stores are preferred, and single-instance services come later.

A workflow still has to fulfil the declared operation. A query or procedure returns
its declared result; a procedure designed to submit durable work may explicitly
return a task reference. A dispatch ID is not a substitute for a promised result.
Live observation is primarily served via **cursored long-polling** over revisioned
stores (`after` cursor + bounded hold window returning `Entries` or
`ReplayComplete`), which maps directly to `call` routines and host-managed
`RevisionedStore` readers without requiring WebSocket push streams into guests.
When a Lexicon `subscription` is exposed over HTTP/XRPC or local transports, the
host can drive it from that same cursored revision contract. Disconnecting a caller
does not by itself settle or undo already durable work.

## 4. Nodule runtime and ownership boundaries

The **nodule runtime** is the composition of node-side management and execution
facilities, following the existing `Rt` responsibility split. It need not be one
service or one crate. Its management/control surfaces can be implemented before
there is any guest execution backend.

It owns:

- Admission of services, facilities and lifecycle (not bindings, which it carries),
  and inventory of revisioned stores and implemented operations.
- Selected revisions, desired enablement, validated runtime configuration, and
  reconciliation.
- Actor provisioning and acquisition of permitted resource capabilities.
- **Dispatch** (§3): the routing table, provider selection, ocap admission and
  forwarding, local to this node.
- Whether operations or revisioned stores are exposed to local clients or other nodes.
- Initialization, executed through dispatch once a provider is ready.
- Host isolation and the authorized WASI/resource environment of its providers.

### Management lifecycle vocabulary

The management surface distinguishes these states and transitions:

- **Known (catalogued):** `PackageEnvelope` is present in the cartridge service; referenced
  blobs are tracked as materialized, missing, or corrupt. No actor or grant exists.
- **Admitted (installed):** The nodule runtime validates the services, facilities and
  lifecycle of the `NoduleDescription`, provisions the nodule's durable local actor,
  and initializes its `NoduleConfiguration` (disabled by default, with zero drawer
  grants). Registering the nodule's own datum Lexicons with `SchemaRepo` is **not
  done today**: `SchemaRepo` has no live-registration API, so a nodule cannot yet
  write its own datums. This is an open item (§10).
- **Selected & configured:** Operator/installer binds a specific immutable
  `NoduleDescription` revision, validates runtime settings against `config schema`,
  assigns standing drawer grants and provider bindings, and configures discovery,
  invocation, and network exposure policies.
- **Enabled vs disabled:** Toggles whether the selected revision may serve new
  invocations, commands, and triggers. Disabling stops new dispatches and live
  long-poll/subscription waits while preserving the durable actor, standing grants
  (unless explicitly revoked), admitted schemas, and private SQLite state.
- **Upgraded:** Moves the selected revision to a newer immutable `NoduleDescription`
  under the same nodule identity and durable actor. A call resolves the current
  enabled revision for its duration (§5); a workflow provider freezes its execution at
  start.
- **Uninstalled:** Retires configuration and revokes standing grants per operator
  policy without erasing admitted schemas required to decode historical documents or
  rewriting historical `dmeta` attribution.

### Policy gates and subjects

Discovery, invocation, and network exposure are separate decisions evaluated against
explicit caller subjects:

- **Local operator / host client:** administrative and interactive callers holding
  host authority.
- **Local nodule actor:** another installed nodule invoking an operation via
  `requires` / cross-nodule command routing.
- **Remote node / peer:** callers arriving over peer-to-peer or public network
  transports.

By default, cross-nodule invocation beyond declared and bound dependencies is
denied, and network exposure to remote nodes is off. An enabled nodule is not
automatically remotely served. Exporting an operation or revisioned store does not
grant drawer access or authorize every network caller to invoke it. The
nodule-runtime domain owns discovery/invocation policy using the existing capability
mechanism; resource domains still own their own grants. All call paths consult the
same gates.

Configuration is mutable **after installation** without rewriting an immutable
manifest. It must allow policy to evolve toward enabling a particular command or
disabling a particular processor, not make package-wide activation the only possible
control. No processors execute in this stage.

| Owner | Responsibility | Not its responsibility |
| --- | --- | --- |
| Cartridge service | Static package catalog, immutable revisions, import/publication, distribution references and observation | Enablement, runtime configuration, semantic nodule/micro validation, authority, or execution |
| Nodule runtime composed by BigNode | Nodule admission, configuration, actors, authorized routing/exposure, and integration with resource/execution services | OCI packaging, GUI composition, or implementing a competing task scheduler |
| BigCabinet | Documents, drawers, schema admission/codecs, and domain access policy | Discovering cartridges or requiring their code to read documents |
| Authority and actor services | Generic namespace capabilities, revocation, durable actor identities, protected signing | Global cartridge/drawer policy or treating actor creation as a grant |
| Dispatch (nodule runtime) | Local NSID routing, ocap admission, readiness gating, provider forwarding | Choosing which node runs work, durability, or a bespoke routine kind |
| wflow, a service behind dispatch | Durable execution: start, status long-poll, message, cancel; freezes the revision at start | Being a nodule routine kind or the dispatcher |
| BigTasks, planned | Allocating work **across nodes** (triage, processors, distributed coordination), then handing dispatch or wflow an already captured invocation | Local routing, admission, or assuming every task concerns a document |
| BigBlobs, planned | Artifact/blob storage and transfer | Deciding which provider to activate |
| Clay micro host | Micro configuration, execution, and frontend composition | Owning nodule grants or node-side dispatch |

Tasks decide *which node*; dispatch decides *whether and how, locally*. Only
triage/processors, which are distributed, wait on BigTasks.

**Readiness is per service/provider, not per nodule.** A nodule has a coarse state
(`Refused(reason)` or `Admitted(revision)`, `enabled`, observed versus applied heads,
grant status), and each service has `Ready`, `Disabled` or `Unavailable(reason)`
(artifacts pending or corrupt, host unavailable, initializing, init failed, or a
required provider missing). Dispatch reads only ready/not-ready and the reason, and
answers a not-ready service with a retryable `NotReady`. Artifacts are fetched and
pinned through big_blobs, which provides the first real transitions beyond today's
`ExecutorUnavailable`.

BigTasks may store its own state in documents and BigSync without every task being
a document processor. Cabinet-aware triggers use document services; generic tasks
do not need to interpret every datum type. BigTasks and BigBlobs are intended
boundaries, not crates already extracted by this ADR.

Portable package and nodule contracts, catalog implementation, and runtime management
must have non-circular dependencies. The catalog does not import runtime or Clay
implementation types. Execution descriptors shared with BigTasks live in a portable
contract, not duplicate definitions or a dependency back into the executor.
Final crate names are not selected; `big_cartridges` is a possible catalog name, not
permission to move Daybook types wholesale.

## 5. Schemas, versions, and provider pinning

A known schema does not require a running routine. Disabling execution does not
remove admitted schemas or make existing documents unreadable. Discovering a schema
or recommendation does not install code, grant authority, or enable execution.

Cabinet retains ADR 020/021: unknown datums remain representable subject to normal
authenticity and decoding gates, while local writes require an admitted schema.
Conflicting definitions must be reported, not selected by import order or active
provider. NSIDs identify contracts, not publisher authority.

Contract compatibility follows the datum rule: compatible evolution keeps the same
NSID; incompatible meaning gets a new NSID. A consumer names the operation contract,
not the provider's semver. Carried schema bundles have immutable provenance references
for reproducible admission and validation, without making a package version the
operation's semantic identity. Concrete compatibility validation remains §10 work.

**Pinning belongs to providers, not to dispatch.** Dispatch is stateless, so a call
resolves the **currently enabled implementation revision** for its duration through
runtime provider policy. A provider that has durable work pins for itself: the wflow
service freezes the selected nodule/manifest revision, handler and executable
artifact digests at start, so retries, restarts and upgrades do not silently
re-resolve a running workflow to newer code. A version label alone is not an immutable
pin. Explicitly changing the enabled revision affects new calls; publication of a newer
package does not itself change that selection. (This supersedes the earlier text that
a dispatch pins on admission.)

Multiple nodules can implement the same operation; version selection within one
provider does not decide between providers. Ambiguous or unavailable providers must
not fall back to installation order or an unselected revision. The exact binding
policy remains to be specified; provider-side pinning is settled.

Separate catalog knowledge, schema admission, selected/desired enablement, policy,
and local execution readiness. Missing selected bytes are pending, not authority to
run another version. Without an executor, inventory and policy can be applied while
execution remains unavailable.

`SchemaRepo` receives registrations at open and has no incremental registration API.
Live schema installation needs an explicit update contract, including index consumers,
or an explicitly restart-bound first version; this ADR does not assume it already
works.

## 6. Actors, capabilities, and installation policy

### Independent identities and resource grants

Each installed nodule has a **distinct durable local actor**, stable across ordinary
upgrades. AuthorityActors owns keys and node-signed attribution certificates; the
runtime provisions the actor and holds its identity capability with bounded rights.
Creating that actor or enabling its nodule grants no access to existing drawers.

A manifest describes needs and possible contributions, not granted authority.
Installation and later configuration bind actual drawers and other resources.
DrawerService owns actor entitlements over selected drawers and creation placements;
the generic authority table does not interpret cartridge policy. Children do not inherit
operator grants. Drawer roster membership, local actor grants, and Keyhive principal
membership remain distinct facts.

An invocation can provide **explicit resource access capabilities**. When the nodule
has no independent entitlement, those are the authority it uses. A nodule can also
exchange its own identity proof for access capabilities covered by its standing
grants, through an explicitly named domain mint operation. Identity is not access
by itself, and a manifest cannot turn a need into an entitlement.

No resource request searches the caller's or nodule's other grants after a presented
access capability is refused. A selected access capability covers the operation's
resources; signer identity cannot enlarge that scope. Invoking a service intentionally
using its own grants requires its invocation policy, not merely finding its NSID.

### Operation authority is ocap too

Dispatch owns an **operations namespace** in the authority service. The operator
delegates to the nodule service, which mints per-actor "may invoke NSID" capabilities
(scoped by NSID or authority prefix) from config datums, exactly as it applies drawer
grants. `NoduleGrants` gains an operations section for this later; the design must not
hard-code drawer-only grants. Presenting such a capability in the dispatch authority
header is the only way a guest or caller reaches an operation (§3), and discovery,
invocation and exposure remain separate gates.

The policy layer is not limited to today's datum-tag predicates or manifest ACLs.
It must support concrete drawers chosen after installation. Installation may provision
private output drawers, potentially organized by datum, and grant creation/write
access there without access to unrelated drawers. Such layout is configurable policy,
not a mandatory global drawer-per-datum rule or an implicit consequence of publishing
a datum definition. Grant administration itself requires authority.

### Explicit identity and access on writes

**Current behavior differs from the selected design.** ADR 022 binds authority and
signer separately at domain-capability mint time, then sends one combined capability
on resource calls. [`WriteDatums`](../../src/big_cabinet_core/doc.rs) has one
`capability` field; the [write handler](../../src/big_cabinet/doc/write.rs) obtains
its signer from that capability's private binding, not from the network presenter.
Identity proofs are rejected by resource handlers; they do not currently trigger
a polymorphic identity-to-grant fallback.

The selected replacement is a **two-capability mutation contract**. For document
creation/writes and other attributed mutations, the roles are explicit:

```text
MutationAuthorization
  identity: recognized actor identity/signing capability
  access: scoped resource access capability
```

| Input | What it proves | What it must not do |
| --- | --- | --- |
| Identity capability | Which actor is to sign this mutation, under explicitly held signing authority | Supply or enlarge resource access through the actor's grants |
| Access capability | Authority over the resources and operation being performed | Reveal or exercise the delegator's identity/signing authority |

The mutation path validates each through its owning domain, with liveness and rights
checks for both, and scope/containment checks for access. Neither slot accepts the
other role as an alternative. The ordinary read path needs scoped access, not a
signer. Mint/exchange operations have explicit contracts separate from resource use;
write does not implicitly mint access, resolve grants from identity, or choose an
identity hidden in the access handle.

For a caller-delegated write, the access handle is the caller's narrowed resource
authority and the identity handle is the nodule's own signing authority. Passing
access alone cannot impersonate the delegator. The host no longer needs to rebind
the access handle's signer merely to attribute execution to the nodule. Access-only
capabilities and separate signing authorization require a domain-contract change,
not ignoring the old bound signer while continuing to accept old combined caps.

The host keeps private keys and unrestricted signing interfaces out of guests.
An identity handle used in an authorized document mutation is not an arbitrary-message
signing oracle. A caller who explicitly delegates an identity/signing capability is
separately delegating that authority; this is never implied by transferring access.
Exactly which identity rights the new mutation endpoint requires, and the protected
service-to-signer call, need a concrete contract before implementation (§10).

Authorization, signing, staging, and commit must define their revocation/linearization
boundary. Two inputs must not permit signing or committing outside the checked access
scope, or let revocation race a successful authorization receipt. Retraction semantics
and the existing dependency DAG remain required; removing a signer binding from access
does not remove revocable support for that access.

This decision **replaces the single-capability, bound-signer resource-call portion
of ADR 022 and ADR 020 in design**, while retaining their authority table, actor,
domain-grant, and cryptographic attribution mechanisms. Their current implementation
is not changed by this documentation or silently described as already supporting two
inputs. The new runtime must not implement an adapter that accepts one cap as either
identity or access to conceal the mismatch. The necessary Cabinet protocol revision
must be explicit before wiring the new mutation path.

### Attribution is not an authorization audit log

Dmeta continues to record actor authorship and neutral node-certified attribution,
not the local authorization path. It does not embed caller OCAPs, grant lookups,
which subsystem permitted a mutation, or a runtime delegation audit chain. Node
authorization and actor attribution remain distinct; a certificate says the node
made the attribution, not that a particular binary executed. Changing local grants
does not rewrite documents or invalidate historical signatures.

### Lifetime, locality, and private state

Live handles are local to their issuing authority instance. Other processes may
use them through transports to that authority; another node cannot validate them
as portable credentials. Network exposure policy does not make handles transferable
across authorities. Remote invocation requires explicit destination authorization
and local capability issuance, not trying the origin's handles against another node.

Manifests, configuration, and task records retain stable identity, scope intent, and
implementation pins, not bearer handles for replay. Restart returns the same actor
with fresh handles and reacquires only still-authorized access. A pinned task is not
exempt from current revocation merely because it has retained its implementation.

Local SQLite state stays private to its nodule. Other nodules consume declared
operations, not its paths or tables. Today's cross-cartridge `local_states` dependencies
cannot be copied unchanged. SQLite is an explicit locality constraint; this design
does not promise transparent movement across hosts or add a generic SQL service.
Concrete state naming, concurrency, migration, and retention remain §10 work.

## 7. Revisioned stores and observation

Nodules can expose state-oriented operations and revisioned stores as well as
commands. This model pairs **revisioned stores**
([`RevisionedStore`](../../src/big_sync_core/revisioned_store.rs)) directly with
**cursored long-polling**:

- A caller always requests changes `after` a source-assigned monotonic cursor.
- While historical revisions exist after that cursor, reads return atomic
  `Entries` batches without waiting, followed by a single `ReplayComplete` boundary.
- Once caught up, a long-poll call holds up to a bounded window for new revisions
  and returns immediately when new entries arrive.
- This makes cursored long-polling implemented via asynchronous `call` routines the
  foundational observation primitive both locally and across nodes, without
  requiring stateful WebSocket push channels or a third guest routine execution kind.

A transport does not supply durable acknowledgement or exactly-once processing; the
caller's cursor is the resumption token. A revisioned store is not automatically
replicated; a custom BigSync backend additionally defines convergence, authority,
retention, and peer compatibility and is deferred.

The catalog exposes revisioned package knowledge and artifact materialization
status. The nodule runtime exposes revisioned desired configuration and its locally
applied projection. Clay owns the corresponding micro state. There is no shared
cartridge-service `active` boolean that claims to summarize all consumers.

A configuration-write receipt means committed intent, not materialized artifacts,
registered executable providers, completed initialization, or finished work. Applied
notifications describe state already visible to readers. Readiness separately reports
whether execution prerequisites are met, including the backend deferred in §8.
Notifications can wake clients; durable consumers need resumable cursored reads,
not an in-memory broadcast as their only history.

Policy updates, registration, activation, and revocation need coherent revision and
acknowledgement boundaries. The current cartridges code's separation of configuration
from pending-to-active materialization is useful evidence, but its cache and local
broadcast are not the new cross-process contract.

## 8. Standalone implementation and staged scope

### Current work: documentation and the portable descriptions

This change locks in the design, including future manifest/execution semantics.
It does not implement services or change `daybook_core`, `daybook_types`, their
consumers, authored legacy manifests, or current Cabinet resource handlers. There
is no requirement to adapt the old `Rt`, add a compatibility shim, or leave its
execution broken while the new system is being built.

The first slice of the implementation stage below ships with it, as two datum
crates built beside the legacy code: [`big_cartridges_core`](../../src/big_cartridges_core/lib.rs)
carries the `PackageEnvelope` (`org.example.daybook.cartridge`, with the nodule and micro
contributions as opaque schema-tagged descriptions) and
[`big_nodule_core`](../../src/big_nodule_core/lib.rs) carries the
`NoduleDescription` (`org.example.daybook.nodule`) and admits it. A nodule's identity
is an NSID authority prefix (for example `org.example.photos`): every Lexicon the
nodule declares is that NSID or lives under `<id>.`, which is how a reference in a
contribution is classified as local (must resolve) or foreign (bound by the runtime)
without a registry. Operation Lexicons (`query | procedure | subscription`) are
admitted by the Cabinet's [`big_cabinet_core::datum::operation`](../../src/big_cabinet_core/datum/operation.rs),
against the one Lexicon grammar ([`datum::grammar`](../../src/big_cabinet_core/datum/grammar.rs))
the datum compiler compiles with, so a schema position is refused identically by both;
the nodule keeps only authority resolution and the contribution checks, because an
operation main is not a record or object the datum compiler models. Neither crate
executes anything or owns configuration.

The catalog's "Known" state is likewise inert today: `big_cartridges_core::catalog` imports
validated envelopes and resolves a dependency requirement to the highest catalogued
version. It writes no files, provisions no actor, and never substitutes another
revision for one that is absent; importing a second envelope under an already-taken
version is a conflict rather than a newer revision. Artifact bytes are deliberately
out of scope here: the envelope still names each artifact by digest, but whether those
bytes are present or intact is the blob system's state to report once it lands, not a
second copy of it in the catalog.

`big_nodule_core::inventory` implements the standalone nodule-inventory slice on top of
catalogued envelopes: it admits a package's `Contribution::Nodule` description, enforces
non-overlapping nodule authority prefixes across installed packages, pins a single
selected revision per cartridge (preserving desired enablement and re-validating customized
settings across upgrades), validates mutable runtime configuration against the nodule's
declared `config schema`, keeps desired enablement (`enabled`) distinct from execution
readiness, and records required artifact declarations with materialization stubbed as
`Unavailable` until the blob and execution layers land.

[`big_nodule`](../../src/big_nodule/lib.rs) is the node-side service over that inventory
and the first piece of §6 that runs: `big_node` boots it beside the document and drawer
domains, under its own `nodules` subsystem actor and namespace, after `DrawerService`
ensures the four local drawers (`present_id`, `originated_id`, `known_drawers_id`, and the
`SelfDoc` node config drawer `config_id`) and grants `nodules_actor` write access to
`config_id`. Portable desired configuration lives as independently addressed datums in
that config drawer (`org.example.daybook.nodule.install/<cartridge_id>`,
`org.example.daybook.nodule.settings/<cartridge_id>`, and
`org.example.daybook.nodule.grants/<cartridge_id>` in `big_nodule_core::config`), projected
through [`DatumStoreHandle`](../../src/big_cabinet/datum_store.rs) (`AtHeads` mutations,
single-frontier hydration, and invalidation-driven partial refresh without touching
unowned keys or discarding unknown/invalid entries). Admitting a package writes its
install datum and reconciles the nodule's **durable local actor** through the
authority's `OpenActor`, at the path `nodules/<nodule id>` under the subsystem actor, so
the same nodule identity reopens the same actor across restarts, uninstall/reinstall, and
ordinary upgrades while each boot's handle is fresh; the service asks for `USE` alone,
the actor is created holding nothing, and the management record reports the actor's
identity and public key but never its capability. Management calls (`admitNodule`,
`configureNodule`, `setNoduleEnabled`, `setNoduleDrawerGrants`, `uninstallNodule`, `getNodule`,
`listNodules`, `mintNoduleCapability` under `org.example.daybook.nodules.*`) are
authorized by the operator's identity capability, or by a handle the service minted from
it carrying a `NoduleScope` (`admit`, and the set of cartridges whose enablement, settings,
and drawer grants the holder may read and change); a handle narrows only to what it
covers, and any other actor's identity is refused rather than looked up in a grant table.
Host-local SQLite retains only durable actor identity bindings and rebuildable
service-owned grant provenance (`nodule_actors` and `nodule_applied_grants`), never a
second copy of desired config. Foreign or invalid imported config is carried and reported
per nodule (`Readiness::Refused`, `error`, and `observed_heads` versus
`applied_heads`) rather than refusing the whole node's boot, while dynamic local drawer
grants apply, downgrade, and revoke service-owned grants within the reconciler's
delegated drawer authority without disturbing other grant sources or Keyhive participant
membership. Discovery and invocation policy, and the cursored observation of management
state (§7), remain ahead of this slice. Today readiness stops at `ExecutorUnavailable`
and nothing executes a nodule.

### Staged scope of the `feat/nodules` PR

This PR covers, in order: dispatch (contract in `big_nodule_core`, host and routing
table in `big_nodule`); the `BuiltIn` and `Wasm` providers (a wash workload per nodule
revision, an instance per dispatch, the authority header, and the `dispatch.local`
egress); nodule artifacts fetched and pinned through big_blobs so readiness moves past
`ExecutorUnavailable`; initialization as a lifecycle binding; admission narrowed to
services, facilities and lifecycle; and an end-to-end test where an operator invokes a
nodule operation and one nodule calls another's operation under capabilities, allowed
and denied.

**Deferred:** the wflow service and durable procedures (a separate PR after the wflow
rework); POSIX providers; triage and processors; BigTasks; network and XRPC exposure;
consumers of command and view bindings; single-instance services; generalising
`NoduleGrants` to method grants.

### First implementation stage: catalog and runtime management

The planned standalone work includes:

- Portable package/nodule descriptions and references, including `config schema`,
  `requires` / `implements`, revisioned store declarations, future routine kinds
  (`call | workflow`), commands, views, initialization, and processor declarations.
- The static cartridge catalog and its import/resolution/digest-verification/observation
  contract (inert subset present: import and resolution only; digest verification and
  materialization observation stay with the blob system).
- Nodule admission/inventory, selected revisions, desired enablement, validated
  mutable configuration, lifecycle transitions (§4), and revisioned management state
  (standalone inventory, revision pinning, config validation, and readiness separation
  present in `big_nodule_core::inventory`; portable config datums in
  `big_nodule_core::config`; the datum-backed, capability-gated reconciler service
  present in `big_nodule`; cursored observation remains).
- Durable per-nodule actor provisioning and dynamic local drawer grants (present:
  `big_nodule` opens each nodule's actor under the `nodules` subsystem actor and
  reconciles service-owned drawer grants against desired config datums).
- Discovery/invocation policy, network exposure controls by caller subject, and
  provider/routing metadata. (Guest handlers now run in this PR's Wasm provider;
  see "Staged scope" above.)

Management operations can be served through the existing service framework.
Registration of an operation is not an executable endpoint: without an executor,
invocation must report execution unavailable rather than claim success. Actor and
policy management can be exercised independently of guest code. The two-capability
mutation seam is documented here; an explicit Cabinet contract change is needed
before it can be exercised by a guest, not a reason to silently reinterpret today's
single-capability API.

### Later PRs: durable execution and tasks

Deferred beyond this PR: workflow execution (the wflow service), processors,
BigTasks and distributed coordination, remote execution, and direct routine
communication. Specifying their manifest model does not bring them into scope.

Executors integrate with the runtime's policy and provider-side selection, rather than
moving enablement back into the catalog. BigTasks can be staged without requiring
distributed task coordination in its first implementation.

### Relationship to existing documents and data

Retain ADR 007's known-versus-selected distinction, immutable revision pins, pending
materialization, and manifest distribution through documents, but move enablement
and runtime configuration to their consumers. OCI is a distribution adapter, not a
requirement of the node contract. Cabinet schemas remain independent of enablement.

New manifest datums follow ADR 020/021: branches are separate documents and reference
heads are separate from datum URLs, not legacy branch/head query parameters copied
from Daybook. The `@daybook/core` bootstrap and non-disablement rule are application
policy, not a mandatory nodule for every BigNode embedder.

Legacy data migration is not this PR. A future migration must explicitly handle
manifest/pin/configuration identities, durable actors and grants, and private state;
a fresh implementation is not permission to discard existing persisted data.

## 9. Use cases and alternatives

A feature should be usable through operations, commands, and portable views without
requiring a micro. A micro improves the experience; it does not make the backend
feature accessible in the first place. These cases constrain the design without
being promised implementations in the management stage:

| Use case | Nodule behavior | Host boundary |
| --- | --- | --- |
| RSS reader | Feed fetch/parse, deduplication, article operations/actions, revisioned article store, portable summaries | Authorized HTTP egress, scheduling, state, and selected document access |
| Relay sponsorship | Onboarding/policy procedures, adoption orchestration, usage/status operations and views | Trusted storage/quota/admission enforcement and scoped sync authority |
| ATProto PDS | Protocol procedures/queries, repository/account behavior, cursored event streams | Public ingress, protocol-compatible storage/authentication, signing and streaming |
| Chatbot, ingestion, receipts | Model orchestration, transformations, cursored/streamed or presented results | Model/egress permissions, durable execution, authorized documents and blobs |

The [existing TODO](../DEVDOC/todo.md) contains ATProto backup, chatbot, ingestion,
and receipt cases. Hosting a PDS is a stronger requirement than backup: XRPC vocabulary
alone does not supply the ATProto repository format, authentication, or wire semantics.
Relay policy and orchestration should be implemented as nodules wherever possible,
while resource-boundary enforcement remains trusted and cannot be bypassed by the
same guest whose writes it limits, as in
[ADR 005](005-relay-sponsorship-retention-admission.md).

Alternatives not selected:

- **Move `PlugsRepo` or `Rt` unchanged.** Preserves the old repository/schema cycles
  and expands this work into a Daybook runtime migration.
- **Catalog owns enablement/configuration for every contribution.** Forces it to
  understand two different plugin systems' lifecycle and policy concerns.
- **Treat nodules and micros as symmetric sibling components.** Obscures that a
  cartridge has one node-facing nodule facet (with many internal contributions) and zero
  or more individual GUI micros.
- **One independently hosted nodule per processor or service.** Adds identities and
  lifecycle units the package model does not need.
- **Replace routines with Lexicon declarations.** Loses the distinction between an
  interface and a provider implementing somebody else's interface.
- **Require a dedicated WebSocket push-stream guest routine kind for subscriptions.**
  Duplicates what cursored long-polling over revisioned stores and `call` routines
  already provide cleanly.
- **Commands target only private routines.** Prevents provider substitution.
- **Only micros can provide visual output.** Makes generally usable commands depend
  on a frontend-specific plugin runtime and excludes portable CLI presentation.
- **Implicit identity/access fallback or signer selection from access.** Obscures
  the selected authority/attribution split. Each mutation input has one explicit role.
- **New background-worker contributions or direct routine messaging.** Duplicates
  task/workflow responsibilities without an established requirement.
- **Every process opens every store.** SQLite alone does not coordinate repository
  caches, projections, or other stores.

## 10. Remaining contract details and deferred work

These require specification at their owning implementation seam, not invention of
a new package/runtime/authority architecture:

1. **Concrete schemas and APIs.** Final package/contribution/reference field
   spellings, portable crate ownership, compatibility validation, and management
   operation schemas. Describe initializer modes, revisioned store descriptors, and
   future processor triggers without implementing their execution.
2. **Provider policy.** Express explicit selection and any unambiguous automatic
   resolution for `requires`, distinguishing missing, denied, incompatible,
   ambiguous, disabled, and unavailable providers. Provider-side pinning and
   resolution of the enabled revision per call are settled; versioning is not a
   policy for selecting between different providers.
3. **Two-capability resource protocol.** Specify identity rights, access-only scope
   encoding, protected signing, atomic authorization/revocation boundaries, and
   the explicit replacement of Cabinet's bound-signer mutation DTOs. No identity
   fallback or caller-signer re-minting shortcut is allowed.
4. **Runtime configuration details.** Portable desired state (`NoduleInstall`,
   `NoduleSettings`, `NoduleGrants`) lives in the node's local `SelfDoc` config drawer
   while actor identities and applied-grant provenance remain host-local SQLite; exact
   per-contribution toggle granularity, schema admission mode (live or restart-bound),
   and how disabling/upgrading cancels or drains in-flight work when the execution PR
   lands remain to be specified.
5. **Guest and dispatch contracts.** The dispatch wire contract and `DispatchError`
   (§3) are a working sketch to finalise during implementation; also the guest SDK
   (header forwarding, `dispatch.local` client), cursored long-poll and
   `RevisionedStore` host bindings, initialization run modes and completion, wflow
   continuation and cancellation (later PR), and any optional guest streaming binding.
   Define capability parameters by role, never as arbitrary strings inferred from
   payloads.
6. **State and presentation, later PR.** Private store naming/migrations/concurrency,
   artifact retention for pinned work, capability reacquisition during retry, and
   portable view/action/terminal rendering vocabulary. Micro-to-nodule integration
   uses services but its frontend dependency/readiness behavior is deferred.
7. **Live datum-schema registration.** The nodule's own Lexicons are never registered
   with `SchemaRepo`, which has no live-registration API, so nodules cannot write their
   own datums. Whether this is in scope for the PR is an open operator question.
   Dispatch keeps operation schemas per provider for edge validation and does not need
   `SchemaRepo`.
8. **Physical considerations.** No I/O on admission; validation at the edges only; no
   durable write per non-durable call and no mirrors of terminal records; provider
   behind a service boundary.

Remote identity authentication/delegation, provider discovery across nodes, custom
BigSync backends, generic cross-nodule SQL, distributed tasks, and confidential cloud
execution are later work. Local routing/exposure controls must leave room for them
without claiming a portable capability-token format or implementing them now.

### Lifecycle review questions: resolved (2026-11-04)

The questions from the [lifecycle review](../scratch/nodule-lifecycle-review.md) were settled with
the operator; [ADR 028](028-cartridge-identity-naming-and-dependencies.md) records the cartridge-level
decisions in full.

- **Catalog role (review §1).** big_cartridges is a registry, like npm, and does no enablement. The
  registry is a drawer whose cartridge documents hold immutable versions and their `Blob` datums. Each
  runtime keeps the manifest it enabled (the inline envelope in `NoduleInstall`) and references
  the artifacts from its own drawer, so a cartridge the registry drops stays usable wherever it is
  enabled (ADR 028 §1).
- **Dependency split (review §1).** Manifests depend on the NSIDs they use, not on cartridges, in the
  shared request shape with `necessity` and `minRevision`. Cartridge-to-cartridge dependencies are a
  delivery hint for a future fetch layer, and enablement is refused until every required NSID
  resolves (ADR 028 §4).
- **Manifest reshape (review §2).** Done: one XRPC handler plus `implements`, with bindings
  carried opaquely (§2 above).
- **Initialization redesign (review §4).** Done, with run modes `Once` (per node per revision)
  and `EveryStart`. Done-ness is host-local and keyed by (cartridge, init key, revision), so there is
  no synced `PerInstall`. Readiness is gated by `Initializing`/`InitFailed` instead of queues.
  Local targets are called directly on the provider, and foreign targets go through dispatch
  under the nodule's method grant. Waiters are woken on any readiness transition.
- **Downgrades (review §5).** Allowed as an explicit runtime action. They are a runtime concern,
  not a catalog one. Automatic acceptance only ever moves forward (ADR 028 §3).
- **Retired prefixes (review §6).** Not addressed in this PR. Reserving historical NSIDs or
  freeing retired prefixes is a later layer.
- **Drawer-grant authority (review §6).** Withdrawn. Keyhive already lets whoever holds a drawer
  delegate at or below their own level, and creators hold Admin (see the
  [drawer wiki](../wiki/drawer.md)).

## 11. Evidence required from implementations

### Catalog and management stage

- A package envelope carries both the singular nodule facet and zero or more micro
  descriptions without the catalog importing either runtime's implementation or
  interpreting its grants and execution kinds; Clay-side micro host consumption is
  an architectural boundary invariant, not a deliverable of the node-side management
  PR.
- Nodule configuration evolves independently of the immutable package revision and
  validates settings against the nodule's declared `config schema`; catalog presence
  is not enablement or readiness.
- Each admitted nodule has a distinct actor that survives restart and ordinary
  upgrade, while its recreated handles are fresh and actor creation grants nothing.
- An installer can configure grants for selected drawers and later change them
  without changing the manifest or granting access to unrelated drawers.
- Missing or corrupt selected manifests/artifacts produce visible pending or error
  state, not an unpinned fallback. No executor means no executable-ready receipt or
  successful guest invocation, including initialization.
- Discovery, invocation authorization, and network exposure are separate gates
  evaluated per caller subject (operator, local nodule actor, remote node); exposing
  or enabling a contribution is not implicit drawer access.
- Two clients observe committed versus applied management state using the declared
  cursored revision contract, without opening duplicate owners of the repositories.
- Standalone services and contracts do not depend on or modify the Daybook runtime.

### Resource-contract and later execution stages

- A declaration-only nodule, a separate provider (`implements`), and a third
  consumer (`requires` + command/processor) work without references to another
  nodule's private routines or tables.
- Direct, IRPC, and XRPC paths agree on validation, results/errors, authorization,
  cancellation, and declared `query` / `procedure` / `subscription` semantics.
- A delegated access cap permits writes signed by the explicitly supplied nodule
  identity, but cannot reveal or exercise the access delegator's identity. Identity
  alone cannot write; access alone cannot choose the signer; swapped roles fail.
- Both identity and access revocation are honored at the declared mutation boundary;
  neither a mint race nor a retry bypasses withdrawal. Restart reacquires only
  current authority and never stores live bearer handles for replay.
- Dmeta retains actor/node attribution without a new capability/delegation audit
  chain. Policy changes do not rewrite untouched documents.
- A call resolves the currently enabled revision; durable providers (wflow) retain
  the exact implementation they froze at start across upgrade and retry. Missing pinned
  bytes never select newer code.
- A nodule calls another nodule's operation through `dispatch.local` under a held
  capability, and the same call without one is denied; an operator invokes a nodule
  operation by NSID; a not-ready provider yields a retryable `NotReady`.
- Disabled code does not make admitted schemas or existing documents disappear;
  writes with unknown schemas remain refused by Cabinet.
- Commands provide both scriptable typed output and portable GUI/CLI presentation
  without requiring a micro; rendering an action does not execute it.
- The sample service's procedure/query, revisioned reads, and cursored long-poll /
  subscription behavior can be provided by a nodule using `call` / `workflow`
  routines, declared revisioned stores, private state, and authorized host
  facilities—without a new plugin kind or a bespoke guest WebSocket ABI.

These are acceptance scenarios for their respective stages, not claims about current
code or a requirement to implement all stages in one PR.
