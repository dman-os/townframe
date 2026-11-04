# ADR 031: Manifest-declared collections and lazy capabilities

**Status:** Proposed. The collection and capability waists exist; manifest admission, durable registration scopes, lazy activation references, and the Clay enablement service do not.

**Builds on:** ADR 017's leased projection protocol, ADR 029's capability bank, ADR 030's micro lifecycle, and the plug envelope/catalog work in `big_plugs_core`.

## Context

Dayframe features are micros, including extension points such as toolbars, layouts, editors, completion providers, and automation routers. Clay therefore cannot define one host API or lifecycle hook for every future extension surface. It needs a small set of host mechanisms from which micros define their own vocabulary.

A contribution must often be discoverable before its provider runs. Starting every installed micro at application boot merely so it can announce its contributions would make startup execute arbitrary package code, introduce ordering and partial-registration failures, and require the host to persist or reconstruct the same deterministic output after every restart.

At the same time, a live WIT resource implemented by a guest cannot survive stopping that guest. Durable discovery must therefore store data and a logical activation endpoint, not a live component resource handle.

The plug envelope is the distribution boundary. `big_plugs_core` already separates immutable, content-addressed package revisions and opaque runtime-specific contributions from runtime-owned enablement, configuration, grants, and readiness. `big_nodule` further demonstrates the desired-state pattern: portable configuration lives in schema-governed datums, a runtime service reconciles it, durable actor/grant identities remain host-owned, and effective failures are reported without mutating the package envelope. Clay follows the same separation while using collections as its discovery and projection waist.

## Decision

### 1. A declared collection exists independently of a micro incarnation

Installing a plug revision admits its micro descriptions and registers their collection definitions in Clay's catalog. No guest component runs. A definition contains, conceptually:

```text
CollectionDefinition {
    id,
    contract_version,
    selector_schema,
    key_schema,
    value_schema,
    write_context_schema?,
    offered_capabilities,
    backing,
}
```

Schema fields refer to Lexicons in the plug manifest's schema section. Anything crossing a plug boundary is schema-bound; host-private collections may retain native types.

A collection is logically available as soon as its admitted definition is installed. Physical projection state, indexes, and backing connections may be created lazily. That optimization is not observable:

- an untouched volatile collection bootstraps as known-empty;
- a durable collection loads its current rows before reporting `bootstrap_complete`;
- a node-backed collection acquires only the upstream interest required by its leases.

The micro that declared the contract is not an implicit storage process. It is another participant and receives only the reader, writer, or administrator capabilities explicitly granted to it.

### 2. One read waist covers volatile, durable, and materialized sources

Persistence is a property of backing and ownership, not a second read protocol. Every source projects current state through ADR 017:

```text
reader.acquire(selector, resume?) -> projection-lease
projection-lease.read(...) -> transactional page
```

Initial backing classes are:

```text
Volatile       host memory; empty after host restart
Durable        host persistence; current rows reload after restart
Materialized   host adapter projects node, index, or other service state
```

The current Rust `Source` trait is only selector admission, row matching, and lease-scoped upstream interest. It is not a storage interface. Implementations will separate selection/interest policy from backing commit and materialization rather than stretching that trait into a persistence abstraction.

Durable backing needs only an ordered key-value store, not SQL. It needs two operations: a prefix scan, to load a collection's rows (or one scope's rows) at bootstrap, and an atomic multi-key batch, to commit or remove a whole registration scope in one transaction. Native hosts implement it over SQLite. The browser host implements it over IndexedDB, whose transactions and key ranges provide the same two operations. Keys order as `(collection, scope, local_key)`, so both a whole collection and a single scope are contiguous ranges. Row values are stored with their schema identity and provenance. The projection assigns fresh row versions on load, so persisted rows carry no cursor or projection state.

### 3. Shared collection contracts are schema-bound

A collection contract names Lexicons for keys, values, and selectors. A schema may also be used by datums, service requests, service responses, or guest-local query IRs; those roles do not require different schema languages.

Selectors and queries remain distinct:

- selectors narrow replication and authority at the host boundary;
- rich queries run over the guest's partial replica.

The host validates admitted writes and selectors structurally without understanding application semantics such as toolbar grouping or completion ranking. Contract-specific active admission callbacks are deferred until a concrete use case cannot be expressed by schema validation, declarative limits, and enablement policy.

### 4. Registration scopes own contributions; writer handles only convey mutation authority

A registration collection is the causal map of ADR 017 §9. Each scope holds at most one stamped
candidate per key, a value or a tombstone, and readers see the highest stamp. A writer is a revocable
capability to write into one scope; dropping or closing it releases that access and does not choose
retention.

Retention is the writer's scope, never the collection's or a consumer's choice:

```text
IncarnationScope
    incarnation closes -> retract the scope's candidates atomically

InstallationScope
    process/guest stops -> candidates remain (durable)
    owning plug is uninstalled -> retract the scope's candidates atomically

PermanentScope
    never retracted by lifecycle; requires an explicit manifest grant
```

Retraction includes tombstones and recomputes only the touched keys, so whatever a scope hid
resurfaces. Durability is a separate axis: installation and permanent candidates persist with their
stamps and writer identity, so winners recompute identically after a restart.

Every writer exposes the same operations:

```text
put(key, basis, value) -> stamp
delete(key, basis) -> stamp
```

A durable writer is stronger authority because its effects survive the incarnation. Ordinary
runtime micros receive incarnation-scoped writers; a runtime receives an installation or permanent
writer only through an explicit grant. Durable writes are applied to the store before they are
published, and concurrent durable writes to one key take turns.

Package installation writes manifest-declared rows into one installation scope and a micro's
runtime durable writes go into another, both keyed by the plug. Reinstalling replaces the manifest
scope wholesale (an unchanged row keeps its stamp, a changed one advances it) without touching
runtime rows; uninstalling retracts both.

This supersedes ADR 017's earlier rule that dropping the `registration-writer` resource itself retracts rows.

### 5. Publications are the only behavior a row can refer to

Micros are singletons: a micro id names its package, its configured instance, and the at-most-one live incarnation. Every behavior one micro offers another is a **named publication** of that micro. A publication has exactly one of two kinds. The set is finite and grows additively, like ADR 029's typed bank methods:

```text
Publication {
    name,                 // stable within the micro, declared in its manifest
    kind: Call | Presentation,
    contract,             // namespace/name/version; for Call, request and response Lexicons
}
```

- **Call**: one request/response operation. The contract names the request and response Lexicons. A toolbar button's action is a call whose response is the empty record. A completion query, an event handler, and a "save" command are also calls. The provider serves it through its exported call handler, keyed by publication name. The host never routes calls by contract vocabulary.
- **Presentation**: a mountable component. The consumer mounts it inside its own surface through `mount-component`. The provider serves it through its exported `mount`, which receives the publication name. One micro may publish any number of presentations; there is no per-micro singleton component.

A "lazy action" is therefore a lazy reference to a call publication, and a "lazy component" is a lazy reference to a presentation publication. There is no third lazy kind. A long-lived service relationship, such as a live completion session, is a retained call binding, not a new kind. A new kind is added only when an operation cannot be expressed as a call or a mount.

Requirements (ADR 030 §12) resolve to publications as well. A required binding and a lazy endpoint differ only in retention: a requirement is strong for the requiring incarnation's lifetime, while a lazy endpoint is weak until used.

### 6. A capability reference is either bound or lazy

ADR 029's `capability-ref` is the only attachment type. What it designates is a host fact:

```text
Bound(publication lease of one incarnation)
    revoked when that publication or incarnation closes

Lazy(micro_id, publication_name, contract)
    outlives incarnations; redemption activates or joins the micro
```

A runtime micro that publishes into an incarnation-scoped row produces bound references. A durable row declared by a manifest produces lazy references. Consumers use both through the same methods and need not distinguish them. `status()` exposes the difference without activating anything:

```text
Live          a compatible incarnation is active
Dormant       enabled, not running; use will activate
Disabled      not enabled; use fails
Faulted       last activation failed; use fails until retry generation changes
Unavailable   provider not installed or publication undeclared
Revoked       the reference itself is withdrawn
```

Because redemption may activate a provider, every redemption and invocation method is async:

```text
capability-ref.call(request) -> response          // strong for the call only
capability-ref.retain-call() -> call-binding      // strong while held
capability-ref.redeem-presentation() -> presentation-component   // strong while held
```

Presentation `mount`, `dispatch`, and `unmount` stay synchronous and may not suspend. A consumer redeems first, asynchronously, and then mounts the live `presentation-component` synchronously. A surface that renders before redemption completes shows a placeholder for that slot.

Calling a bound reference never activates anything; once its incarnation has closed it returns `revoked`. A lazy reference whose provider is `Closing` waits for a fresh incarnation and never enters the closing one. Concurrent use of lazy references joins one activation.

### 7. Durable rows persist endpoints, never references

A durable row stores the lazy target as data:

```text
LazyEndpoint {
    micro_id,
    publication_name,
    contract,
}
```

The endpoint names neither a package revision nor an entrypoint. The installed revision of `micro_id` is resolved at activation. Upgrading a package rewrites its installation scope transactionally, and an endpoint whose publication disappears reports `Unavailable`. Generation and enablement identities may exist inside the service. They are not part of the endpoint or the configuration surface.

A micro that writes a row at runtime attaches one of its own publications by name. The host checks the name against the publications the writer's manifest declares (`invalid-attachment` otherwise) and stores the endpoint `(micro_id, name, declared contract)`, the same value a manifest-declared row stores. The write needs no live publication or handle from `initialize`, so a row can be written at any time, from a task or a call handler as much as from `activate`. A writer forwarding a reference it holds attaches that reference instead, which stays in memory and is never stored durably.

When projecting the row to an authorized reader, Clay mints a recipient-local lazy `capability-ref` for each endpoint and places it in the row's attachment table. Copying the CBOR body conveys no authority. Uninstalling the owning package removes its durable scope, which removes the rows and revokes the references minted from them.

### 8. Initialization returns publications; there are no slots to poll

The manifest declares a micro's publications. `initialize` returns the publication leases it serves, by name, together with its `instance-root`:

```text
initialize(context) -> { root, publications: [(name, published-capability)] }
```

This is ADR 030 §5's staged visibility: nothing is reachable through the publications until `initialize` returns successfully, and the host exposes all of them atomically. A demand that started the activation for publication `p` succeeds if `p` is among the returned publications with the declared kind and contract. Otherwise that demand fails with `Unavailable`, and the incarnation stays up for other users. Callers awaiting the lazy reference receive the activation result directly. No secondary collection is polled.

A returned publication that the manifest does not declare, or that has a different kind or contract, is an initialization failure. A manifest publication that is not returned is simply not served by this incarnation.

### 9. Idle shutdown follows strong resource ownership

The lifecycle controller distinguishes:

```text
lazy capability-ref / durable row   weak; no retention
retained call-binding               strong while held
in-flight call                      strong for the call
redeemed presentation-component     strong while held or mounted
incarnation writer/publication      owned effect; cleaned with owner
```

When the last external strong edge disappears, policy may close immediately or after a bounded grace period. Closing:

1. rejects new outward publication and routes new demand to a future incarnation;
2. retains existing dependencies while `instance-root.close()` runs;
3. revokes live publications;
4. removes incarnation-owned registration scopes;
5. drops owned work and resources in reverse ownership order; and
6. releases outgoing strong bindings.

Abrupt engine loss skips guest cleanup but not host-owned scope and capability cleanup.

### 10. Durable discovery normally replaces a second ephemeral coordination collection

Static contributions use one durable row containing metadata plus a lazy endpoint. The target renders or indexes metadata without starting the provider and redeems behavior only when needed.

An ephemeral collection remains appropriate when the set itself is runtime-derived, such as open conversations, connected devices, active media sessions, or panels created from live documents. Targets may also explicitly retain all endpoints when eager behavior is desired; this is policy over the same resources, not another lifecycle mechanism.

### 11. Collection existence, permission, and runtime handles are separate

Knowing a collection definition or contract id conveys no authority. Access flows in three steps: *declared*, *configured*, and (later) *granted at runtime*.

**Declared.** A manifest lists collection requests, one per local binding name, in the shared request shape of ADR 028 §4: `{ binding, contract, access: read | write, necessity: required | optional, resolution: configured | atRuntime, minRevision? }`. `contract` is an NSID naming a collection contract (`CollectionContract.id`), never a concrete collection; write implies read; bindings are unique within a manifest. Today a declared collection's id is its contract's id, but nothing in the request relies on that, so a contract can later have several declared instances. The micro's code names only the binding.

**Configured.** Clay resolves each binding to a concrete declared collection or leaves it unbound. The resolution is stored per micro in a binding datum (tag `clay.micro.bindings`, key = micro id, in the same configuration document as the install datum), mapping binding to collection id (an NSID) or to `null` for deliberately unbound; unknown fields are carried. Configuration is final: an explicit entry wins even over the manifest default, and an entry naming a collection that does not implement the request's contract leaves the binding unbound rather than falling back. When the datum says nothing about a `configured` request, the manifest's default applies: a contract implemented by exactly one declared collection binds automatically, otherwise the binding is unbound. Access (read or write) always comes from the manifest and cannot be raised by configuration. A manifest-declared contribution row targets one of these bindings and lands in whichever collection it resolved to (ADR 030 §14). The factory reads the resolved bindings at each `initialize` and the activation context contains only those handles (`Context::collection(binding)`, `registration(binding)`; guests receive them under the same binding names).

**Lifecycle.** A micro with an unresolved *required* binding is not startable and is reported as `Unconfigured { missing }`. That is a configuration state, not a fault: neither the boot reconcile nor redemption of one of its publications starts it, and redemption fails as unconfigured. An unresolved *optional* binding starts normally with the binding absent. A change to the binding datum restarts the micro (the supervisor closes it, replaces its bindings, and eager roots start again); there is no hot rebinding, so an incarnation sees the resolution that was current when it started.

**Granted at runtime.** A request with `resolution: atRuntime` is declared only. The host never defaults it and, until dynamic consent exists, binds it only from an explicit entry in the binding datum. The future dynamic-consent path (a powerbox- or Android SAF-style picker) records its grant in configuration, so a granted binding arrives as the same handle through the same step. That picker is deferred; this ADR specifies no UI for it.

Reader grants constrain collection identity, selector authority, limits, row visibility, and attachment contracts. Writer grants additionally constrain registration scope, schemas, row/byte limits, and allowed attachments. Reset, raw inspection, migration, and revoking another scope require separate administrative capabilities.

Future dynamic consent, delegation, and debugger/catalog capabilities issue the same resources after policy evaluation. They extend grant issuance rather than introducing ambient global lookup.

### 12. Plug catalog and Clay enablement remain separate

The immutable plug envelope owns identity, version, content-addressed artifacts, schemas, and opaque Clay contributions. It does not own local enablement, concrete grants, configured bindings, readiness, or runtime faults.

Clay will have a runtime-owned desired-state service analogous in shape, but not policy, to `big_nodule`:

```text
portable desired state
    installed revision
    micro settings
    collection/requirement bindings
    grants and accepted durable contributions

host-local applied state
    runtime identities
    issued capability/grant records
    effective activation/readiness/fault state
```

The service admits package descriptions, validates schemas and artifact references, reconciles desired grants and durable registration scopes, and reports desired versus effective state. BigNode nodule policy is not reused: nodules own durable node actors and drawer grants, while micros are ephemeral Clay incarnations with collection and presentation capabilities.

The first implementation has no executable install or enable hook. Static manifest contributions are validated and applied transactionally by the service.

### 13. Examples

#### Toolbar

The toolbar provides a durable collection contract. A counter package contributes:

```text
key: increment
value:
    label: "Increment"
    icon: "plus"
    action: attachment 0
endpoints:
    0: LazyEndpoint(counter, "increment", toolbar.action@1)   // Call, empty request/response
```

The toolbar reads and renders the button while the counter is stopped. Pressing it calls attachment 0, which activates or joins the counter incarnation, holds a strong edge for the call, and then permits idle shutdown. No ephemeral toolbar collection or provider polling is required.

Durable rows are static. A button whose enabled, checked, or badge state changes at runtime takes that state from a separate volatile collection that the live provider writes under its incarnation scope, keyed by the same registration id. While the provider is dormant, the toolbar uses the durable row's defaults.

A custom toolbar widget references a Presentation publication. The toolbar redeems it asynchronously, mounts it synchronously, and the redeemed `presentation-component` retains its provider until it is dropped.

#### Completion providers

The editor provides a durable completion-source collection. Rows carry trigger/language metadata and a lazy reference to a Call publication whose contract takes a completion request and returns candidates. The editor filters metadata locally and calls only matching providers. A provider that wants to keep state between keystrokes is kept live by a `retain-call` binding held while the editor is focused. Request data or a narrowly scoped document capability supplies context; providers do not receive ambient editor authority.

#### Layout

The layout owns a persistent configuration datum containing stable panel registration ids and arrangement. A durable panel collection describes installed panels and lazy presentation endpoints. The layout resolves configured ids and activates only visible presentations. Missing or unavailable providers produce placeholders without destroying the user's arrangement.

#### Event-driven automation

An event-router micro provides a durable handler collection. Rows contain event selectors and lazy handler endpoints. Matching events activate handlers for one invocation, analogous to service-worker events or systemd activation, without making the host understand the event vocabulary.

## Rejected alternatives

### Start every micro at boot to register resources

Rejected because startup would execute arbitrary package code, declarations would not be statically inspectable, ordering and partial failure would become observable, and outputs would still require persistence or reconstruction.

### Persist WIT resource handles

Rejected because guest resources depend on a live component/store. Persist logical endpoint identity and mint fresh host capabilities instead.

### Let contributors edit target configuration documents directly

Rejected because ownership, uninstall cleanup, concurrent edits, schema migration, and the distinction between availability and user arrangement become ambiguous. Durable contribution collections hold availability; target-owned documents hold user choices.

### Add host APIs for every extension surface

Rejected. Clay transports schema-bound data and capabilities; target micros define toolbar, completion, panel, automation, and future vocabulary.

## Consequences

- Collection contracts remain available while declaring and contributing micros are inactive.
- Volatile and durable writers share mutation semantics; scope ownership alone changes retention.
- Static extension discovery does not consume guest memory.
- Resource use, not bespoke demand hooks, drives activation and shutdown.
- Package schemas remain inspectable and versionable without making Clay understand feature semantics.
- Durable backing and activation reconciliation add host machinery, but that machinery is implemented once rather than by every micro.

## Deferred

- Dynamic user-consent UI for granting new collection access;
- generic schema-bound request/response service capabilities;
- idle grace, freezing, preinitialized pools, and crash backoff;
- active target admission callbacks;
- uninstall policy for user-authored data distinct from package-owned registration rows;
- cyclic lazy activation beyond precise cycle failure; and
- browser Web Components and capability-bounded DOM subtree APIs.

## Validation before acceptance

This ADR becomes Accepted after tests demonstrate:

1. installing a manifest makes an untouched collection bootstrap known-empty without starting its declaring micro;
2. volatile and durable collections cross the same projection WIT;
3. writer-handle drop preserves rows while incarnation closure removes an incarnation scope atomically;
4. durable rows survive host restart and inactive providers, and uninstall removes only the owning installation scope;
5. schema-invalid selectors, keys, values, and attachments are rejected before state changes;
6. reading a lazy reference, including its `status()`, does not activate its provider;
7. concurrent calls and redemptions of lazy references perform one staged activation and receive the named publication returned by `initialize`;
8. retaining a redeemed presentation or call binding keeps the provider live, an in-flight call keeps it live until completion, and dropping the final strong binding causes orderly shutdown;
9. a bound reference whose incarnation closed reports `revoked` and never activates a new incarnation;
10. a toolbar contribution renders from durable metadata, activates on press or mount, and needs no secondary coordination collection; and
11. native, Wasmtime, and WCMP paths obey the same ownership and lifecycle contract.
