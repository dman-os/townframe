# ADR 025: Manifest-declared collections and lazy capabilities

**Status:** Proposed. The collection and capability waists exist; manifest admission, durable registration scopes, lazy activation references, and the Clay enablement service do not.

**Builds on:** ADR 017's leased projection protocol, ADR 023's capability bank, ADR 024's micro lifecycle, and the plug envelope/catalog work in `big_plugs_core`.

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
    writer_policy,
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

### 3. Shared collection contracts are schema-bound

A collection contract names Lexicons for keys, values, and selectors. A schema may also be used by datums, service requests, service responses, or guest-local query IRs; those roles do not require different schema languages.

Selectors and queries remain distinct:

- selectors narrow replication and authority at the host boundary;
- rich queries run over the guest's partial replica.

The host validates admitted writes and selectors structurally without understanding application semantics such as toolbar grouping or completion ranking. Contract-specific active admission callbacks are deferred until a concrete use case cannot be expressed by schema validation, declarative limits, and enablement policy.

### 4. Registration scopes own rows; writer handles only convey mutation authority

Physical registration identity is:

```text
(registration_scope, local_key)
```

A contributor cannot address another scope's rows. A writer is a revocable capability to mutate one scope with compare-and-set operations. Dropping or closing a writer releases that access handle; it does not choose row retention.

The scope owner determines cleanup:

```text
IncarnationScope
    incarnation closes -> remove all scope rows atomically

InstallationScope
    process/guest stops -> rows remain
    owning plug is uninstalled -> remove all scope rows atomically
```

Both writers expose the same operations:

```text
put(local_key, based_on?, value)
remove(local_key, based_on?)
clear()
```

A runtime durable writer is stronger authority because its effects survive that incarnation. Ordinary runtime micros receive incarnation-scoped writers. Manifest-declared durable contributions are applied by Clay's package/enablement machinery; a runtime receives a durable writer only through an explicit grant.

This supersedes ADR 017's earlier rule that dropping the `registration-writer` resource itself retracts rows.

### 5. Durable declarations contain logical endpoints, not live resources

A durable row may refer to behavior through a host-authenticated logical endpoint:

```text
LazyEndpoint {
    provider_plug_revision,
    micro_entrypoint,
    publication_name,
    contract,
}
```

The persisted representation is data plus provenance. It is not a serialized WIT handle or recipient-local capability reference.

When projecting the row to an authorized consumer, Clay mints a recipient-local lazy capability attachment. Copying the CBOR body conveys no authority. Uninstalling the owning package removes its durable scope and revokes its endpoints.

### 6. Lazy references are weak; redeemed live resources are strong

A lazy reference:

- is usable while its provider is inactive;
- does not retain or start the provider merely by existing in a collection row;
- starts or joins provider activation when redeemed or invoked;
- is host-owned and can report unavailable, disabled, failed, or revoked state.

A successfully redeemed live proxy or in-flight invocation creates a strong runtime binding to the provider incarnation. Holding that resource keeps the provider active. Dropping the last external strong binding makes the provider eligible for idle shutdown.

Concurrent redemptions join one `Starting` operation. They do not launch duplicate incarnations or poll a secondary collection for publication.

### 7. Named publication slots close the activation race

A logical endpoint resolves to a declared publication slot. During staged startup, the context supplies the provider with the slots demanded by activation:

```text
context.publications.require(contract, publication_name)
slot.fulfill(resource)
```

Successful `start` atomically commits fulfilled publications. A demanded required slot left unfulfilled makes startup fail. Callers awaiting the lazy endpoint receive that activation result directly.

If the provider is already active with a compatible publication, redemption binds to it. If it is closing, new demand waits for a fresh incarnation rather than entering the closing one.

### 8. Idle shutdown follows strong resource ownership

The lifecycle controller distinguishes:

```text
lazy endpoint / durable row     weak; no retention
redeemed live proxy             strong
in-flight service invocation    strong for the invocation
mounted presentation            strong while mounted/retained
incarnation writer/publication  owned effect; cleaned with owner
```

When the last external strong edge disappears, policy may close immediately or after a bounded grace period. Closing:

1. rejects new outward publication and routes new demand to a future incarnation;
2. retains existing dependencies while `instance-root.close()` runs;
3. revokes live publications;
4. removes incarnation-owned registration scopes;
5. drops owned work and resources in reverse ownership order; and
6. releases outgoing strong bindings.

Abrupt engine loss skips guest cleanup but not host-owned scope and capability cleanup.

### 9. Durable discovery normally replaces a second ephemeral coordination collection

Static contributions use one durable row containing metadata plus a lazy endpoint. The target renders or indexes metadata without starting the provider and redeems behavior only when needed.

An ephemeral collection remains appropriate when the set itself is runtime-derived, such as open conversations, connected devices, active media sessions, or panels created from live documents. Targets may also explicitly retain all endpoints when eager behavior is desired; this is policy over the same resources, not another lifecycle mechanism.

### 10. Collection existence, permission, and runtime handles are separate

Knowing a collection definition or contract id conveys no authority. At configuration/enablement time, Clay resolves manifest requirements into grants. At activation, the context contains only those resolved handles.

Reader grants constrain collection identity, selector authority, limits, row visibility, and attachment contracts. Writer grants additionally constrain registration scope, schemas, row/byte limits, and allowed attachments. Reset, raw inspection, migration, and revoking another scope require separate administrative capabilities.

Future dynamic consent, delegation, and debugger/catalog capabilities issue the same resources after policy evaluation. They extend grant issuance rather than introducing ambient global lookup.

### 11. Plug catalog and Clay enablement remain separate

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

### 12. Examples

#### Toolbar

The toolbar provides a durable collection contract. A counter package contributes:

```text
key: increment
value:
    label: "Increment"
    icon: "plus"
    action: lazy-command(counter/main, "increment")
```

The toolbar reads and renders the button while the counter is stopped. Pressing it invokes the lazy action, activates or joins the counter incarnation, holds a strong invocation edge until completion, and then permits idle shutdown. No ephemeral toolbar collection or provider polling is required.

A custom toolbar widget uses a lazy presentation endpoint. Mounting the redeemed presentation retains its provider; unmounting releases it.

#### Completion providers

The editor provides a durable completion-source collection. Rows carry trigger/language metadata and a lazy service endpoint. The editor filters metadata locally and invokes only matching providers. Request data or a narrowly scoped document capability supplies context; providers do not receive ambient editor authority.

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
6. reading a lazy endpoint does not activate its provider;
7. concurrent redemption performs one staged activation and returns the named publication;
8. retaining a redeemed presentation keeps the provider live, and dropping the final strong binding causes orderly shutdown;
9. a toolbar contribution renders from durable metadata, activates on press or mount, and needs no secondary coordination collection; and
10. native, Wasmtime, and WCMP paths obey the same ownership and lifecycle contract.
