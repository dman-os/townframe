# ADR 030: Micro lifecycle, lazy activation, and manifest contract

**Status:** Proposed. The lifecycle core is partially implemented; the WIT and revised
manifest shape are not.

**Amends:** ADR 014's binding-derived lifecycle and ADR 015 §§7–10. **Builds on:**
ADR 017's projection leases and ADR 029's cross-guest capability bank.

## Context

Dayframe micros are configured singleton services, not process-tree children. At most
one incarnation of a micro is live at a time, and the micro id is at once its package,
configured, and runtime identity. Dependencies form a graph:
requirements, accepted contributions, collection projections, invocations, and
capability references can connect otherwise unrelated micros.

We want the familiar parts of a process model — one initialization entry point,
owned resources, isolated failure, host supervision, and deterministic cleanup —
without inventing `fork`, parent/child ownership, or explicit `start`/`stop` APIs.
Enabled micros may remain dormant until an eager application root or lazy endpoint demand starts them. Once active, they remain live only while an external strong runtime resource retains them; losing the final edge makes them eligible for orderly idle shutdown. Disablement prevents new activation, while uninstall additionally removes installation-owned durable registrations.

Lazy startup creates a discovery problem. A contributor cannot run in order to tell
the host that running it would contribute a toolbar button. Packages therefore expose
inspectable declarations before activation. Installation and enablement may register
those declarations as activation policy; runtime initialization publishes the actual
resources.

The current implementation proves part of this model: eager roots, configured
requirement bindings, static contribution declarations, consumer `accept` /
`accept_all`, reachability-based liveness, idle eviction, incarnation-checked brokers,
and retained faults. It does not yet model bindings as typed strong/weak resources or
expose the complete lifecycle through WIT.

## Decision

### 1. Lifecycle nodes are micros

The graph node is the micro id, not a runtime incarnation. Micros are singletons, so the
micro id names the installed package, its configuration, and its at-most-one live
incarnation. The configuration surface exposes no separate instance, generation, or
enablement identity. The service may keep generations internally, for example to reject
stale cleanup, but they never appear in endpoints, rows, or configuration. Running one
package as several configured instances is out of scope until a concrete need appears.
Configuration names:

```text
ConfiguredMicro {
    micro_id,
    enabled,
    eager,
    requirement_bindings,
    collection_bindings_and_durable_scopes,
    configuration_grants,
}
```

A fresh runtime incarnation is created for each successful initialization attempt.
Changing only `eager` recomputes reachability without replacing an incarnation that
remains wanted. Upgrading the package, or changing grants or requirement bindings,
creates a new incarnation when the micro is next live.

Observable states are:

```text
Disabled
Idle
Blocked(missing requirements)
Initializing
Active
Closing
Faulted
UnavailableProvider
```

`Faulted` covers initialization failure and runtime failure. Demand churn does not
retry a fault. Retry requires an explicit retry generation or a relevant provider or
configuration generation change.

### 2. Roots and resource edges determine liveness

An enabled configured instance is live while held by an eager application root or a concrete strong runtime resource. Durable collection rows and lazy activation references are weak discovery data: they neither require a live provider nor keep one running. A disconnected cycle of live implementation resources cannot retain itself merely by reference counting; the lifecycle controller computes reachability from external roots.

Every host resource contract declares one lifecycle class:

```text
NoMicroRetention  // host state only; no micro target
Lazy              // may activate a target on use; does not retain while idle
Weak              // usable while target lives, inert after closure
Strong            // keeps the target incarnation live
OwnedEffect       // destroyed with its owner; not a target-retention edge
```

Examples:

- durable collection row and lazy endpoint attachment: `Lazy`;
- required live service binding: `Strong`;
- redeemed presentation proxy or mounted surface: `Strong` while retained;
- in-flight service invocation: `Strong` until completion;
- host-materialized projection lease: `NoMicroRetention`;
- guest-served live projection lease: `Strong`;
- incarnation registration scope, publication slot, and owned future: `OwnedEffect`.

The holder cannot promote a contract's retention class. Redemption converts a lazy logical endpoint into a live strong binding only after successful staged activation. Dropping the final external strong binding makes the provider eligible for orderly idle shutdown.
### 3. Reverse dependencies are durable collection contributions

A reverse dependency is not a separate runtime registry and does not require a live contributor to announce itself. The target micro defines a schema-bound durable collection contract. A contributor manifest declares a row for that contract containing static metadata and, when behavior is required, a logical lazy endpoint.

Installation admits the declaration and writes it under an installation-owned registration scope. The row remains visible while the contributor is inactive and across host restart. Uninstalling its plug removes that scope atomically. The target's own configuration may refer to stable registration ids for ordering, hiding, or placement; contributors do not edit the target's configuration document.

```text
manifest declaration
    -> schema and authority admission
    -> durable collection row
    -> target reads metadata without activating provider
    -> target redeems or invokes lazy endpoint on demand
    -> host starts or joins provider activation
    -> named publication slot is fulfilled
    -> live strong proxy/invocation
```

The durable row, lazy endpoint, live binding, and provider publication remain distinct and inspectable. Static rows need no secondary ephemeral coordination collection. Ephemeral registration collections remain appropriate when the contributed set itself is runtime-derived.

### 4. Activation demand is represented by resource use

The host does not define a growing vocabulary of toolbar, route, completion, or extension triggers. Target micros own those semantics in their collection contracts and Lexicons. Clay understands only generic demand:

```text
Demand =
    ExplicitRoot
  | RedeemLazyEndpoint
  | InvokeLazyService
  | MountLazyPresentation
  | RetainLiveBinding
```

Reading a durable row or holding its lazy attachment does not activate the provider. Redeeming, invoking, or mounting it starts or joins one activation attempt. A target that intentionally wants eager contributors may retain their endpoints explicitly; helpers may retain visible, selected, or selector-matching entries without changing collection semantics.

Package declarations and Clay configuration determine which entrypoint and named publication may satisfy an endpoint. Guests never send trusted provider identities in an invocation. Unknown application extension vocabulary remains data interpreted by target micros, not a host lifecycle enum. ADR 031 specifies manifest-declared collection and endpoint ownership.

### 5. Initialization stages outward visibility

The host creates a pending incarnation and calls one async initialization export. The
micro may acquire inward capabilities immediately: required provisions, projection
leases, writer leases, configuration documents, and capability-bank access.

Outward effects remain staged until initialization succeeds:

- provided contracts;
- contribution rows;
- capability publications;
- mounts; and
- guest-served collection sources.


A demanded lazy endpoint names a declared publication. `initialize` returns the publications it serves by name (ADR 031 §8); callers await the activation result rather than polling an ephemeral collection. A demand whose publication is not returned fails as `Unavailable`. Concurrent demand joins the same pending incarnation.
On successful return, the host atomically exposes staged publications. On error,
trap, cancellation, or dropped initialization, the pending incarnation scope is
dropped and all acquired/staged resources unwind through RAII. This is transactional
visibility, not a general rollback protocol for arbitrary external side effects.

The host owns lifecycle authority. The exported `instance-root` proves successful
initialization and is held by the host for the incarnation; it is not a guest-visible
`stop` capability.

### 6. Closing preserves dependencies through cleanup

When an active incarnation loses its final external strong binding, is disabled, or must be replaced, it enters `Closing` immediately or after a bounded host policy grace period:

1. reject new outward publications; new lazy demand waits for a fresh incarnation rather than entering the closing one;
2. stop ordinary delivery and request owned work to finish/cancel;
3. retain existing strong dependencies while incarnation cleanup runs;
4. drop the incarnation scope after cleanup;
5. dropping that scope releases outgoing strong bindings and owned effects;
6. recompute reachability and close newly unreachable suppliers.


The host invokes `instance-root.close()` exactly once while the incarnation's existing dependencies remain retained. The guest uses it to finish or cancel asynchronous owned work; returning does not grant authority to publish new outward resources. After it returns, traps, or is cancelled by host supervision, the host drops the incarnation scope and its resources. Abrupt engine loss may skip guest cleanup, so correctness still relies on host-owned RAII cleanup rather than on `close()` completing.
This naturally closes dependents before providers. Mandatory configured dependency
cycles are rejected. A dynamically formed unreachable strongly connected component
closes as a group; members must tolerate peers becoming revoked during cleanup.

Weak ADR 029 proxies remain owned and safely droppable after provider closure, but
become inert. Surface recovery may show a missing/error presentation or a frozen
render; the weak handle does not keep the provider alive.

## WIT shape

The following is architectural WIT, not a commitment to generated identifier spelling.
Interfaces use explicit versions and WIT stability annotations when fields or methods
are added.

### 7. Initialization world

```wit
package townframe:clay-micro@0.2.0;

interface lifecycle-host {
    resource initial-context {
        arguments: func() -> list<u8>; // Lexicon-compatible CBOR
        configured-identity: func() -> string;
        configuration: func(name: string) -> result<configuration-binding, access-error>;
        require: func(binding: string) -> result<capability-ref, access-error>;
        collections: func() -> collection-host;
        capability-bank: func() -> capability-bank;
    }
}

interface lifecycle-guest {
    resource instance-root {
        close: async func();
    }

    variant initialization-error {
        invalid-configuration(string),
        unavailable(string),
        rejected(string),
    }

    record named-publication {
        name: string,                  // declared in the manifest's publications
        capability: published-capability,
    }

    record initialized {
        root: instance-root,
        publications: list<named-publication>,
    }

    initialize: async func(context: lifecycle-host.initial-context)
        -> result<initialized, initialization-error>;

    // Publications are served through exports keyed by publication name.
    call: async func(publication: string, request: list<u8>) -> result<list<u8>, call-error>;
}

world micro-client {
    import lifecycle-host;
    import collections;
    import capability-bank;
    import documents; // transitional/domain-specific command surface
    import presentation;

    export lifecycle-guest;
    export presentation-events;
}
```

### 7a. Calls are ordinary component-model async exports

Every Call routed to a micro is one invocation of its async `call` export, and therefore
one component-model task. Clay adds no guest-side dispatch loop: the component model is
reentrant, so an instance holds many suspended tasks at once, and the host drives them in
bounded bursts (`drive_until_stalled` on Wasmtime, `run_until_stalled` on WCMP). Sync
presentation exports (`mount`, `dispatch`, `unmount`) enter between bursts while calls are
suspended. A call chain that comes back into the same micro (A→B→A) is just another task.
The host never re-enters an instance on the first poll of a host import that the
instance's own task is still running. Cross-micro calls are queued into the provider
incarnation's driver, which guarantees this.

Background work spawned by a call (wit-bindgen `spawn`) lives as long as that call's task:
the export may deliver its result and keep running its spawned futures. Work that must
outlive any single call runs in the task of `initialize`, or, once the toolchain exposes
them, on component-model cooperative threads.

Cancellation follows the component model: it is cooperative and delivered to the callee
task, which unwinds. In Rust that means its future is dropped and its destructors run.
Guest-to-guest cancellation (a caller dropping an import future) works this way on both
engines. The pinned Wasmtime and WCMP cannot yet cancel an export call from the host
(Wasmtime #11833), so a host-side caller that gives up is detached: the guest task runs
to completion and its result is discarded. A host-initiated cancel token is deferred
until a concrete need appears.

A guest trap ends the instance. The host fails every outstanding call on it, so callers
observe `revoked`, and runs host teardown. It never leaves callers waiting on a dead
store.

The guest runtime must keep thread-locals shared across tasks while keeping per-task
runtime state separate:

- The MDK overrides wasi-libc's `__wasilibc_task_hook` so all tasks share one TLS block.
  UI runtime state written in `activate`, such as Dioxus' runtime stack, must survive
  into `mount`, and stock per-task TLS would discard it.
- wit-bindgen keeps the current export task's state in a single slot that `start_task`
  requires to be empty. With `std`, that slot is a thread-local, which shared TLS turns
  into one global, so a second overlapping task aborts. Guests therefore build
  wit-bindgen without `std`. The MDK supplies the per-task slot, and its hook parks the
  value in component context slot 0 while the task is suspended, as stock wasi-libc
  parks the TLS base.
- Cross-task wakeups (close signals, channels) require wit-bindgen's `inter-task-wakeup`
  feature.

Authoring rules that follow (no guard held across `.await`, UI global APIs only inside
the runtime, panics end the instance) are documented with the MDK.

Rejected: a guest-side dispatch loop (one long-lived `serve` export multiplexing calls).
It re-implemented scheduling, cancellation and backpressure that the component model
already defines, diverged from how browsers and jco will run components, and its only
motivation was an abort that turned out to be our own TLS hook, not a toolchain limit.

A separate `get-args()` import may replace `initial-context.arguments()` if bindings
make it more ergonomic. It remains one-shot initialization input, not another lifecycle
phase.

Presentation `mount`, `dispatch`, and `unmount` remain synchronous/may-not-suspend.
Host-side long polls do not occupy the guest entry gate.

### 8. Collection read waist

```wit
interface collections {
    type cursor = list<u8>;

    record encoded-value {
        body: list<u8>,                    // CBOR
        capabilities: list<capability-ref>,
    }

    variant row-evidence {
        authoritative,
        pending,
    }

    record upsert {
        key: list<u8>,
        row-version: list<u8>,
        value: encoded-value,
        evidence: row-evidence,
        write-context: option<list<u8>>,
    }

    record remove {
        key: list<u8>,
        row-version: list<u8>,
    }

    variant change { upsert(upsert), remove(remove) }
    record transaction { changes: list<change> }

    record page {
        transactions: list<transaction>,
        through: cursor,
        bootstrap-complete: bool,
    }

    variant read-error {
        reset-required,
        source-unavailable,
        revoked,
    }

    resource projection-lease {
        read: async func(
            committed: option<cursor>,
            limits: read-limits,
            poll-timeout-ns: u64,
        ) -> result<page, read-error>;
    }

    resource collection-reader {
        acquire: async func(
            selector: list<u8>,
            resume: option<cursor>,
        ) -> result<projection-lease, acquire-error>;
    }
}
```

`committed` is `none` only for the first read on a lease. A `reset-required` result invalidates that lease. The guest drops it (releasing attachment references held only by its rows), clears that projection replica, and reacquires with the same selector and no resume cursor. Libraries may preserve a separate display cache during this process, but it is not part of the replica and must not be used as authoritative query state.

The host provides only readers already authorized for the guest. Selectors and values
are collection-specific CBOR. An empty successful page represents long-poll timeout.
Reset clears that projection and reacquires it.

Writer interfaces are separate capabilities rather than methods on `collection-reader`:

```wit
interface collection-registration {
    variant write-basis {
        concurrent,
        observed(stamp),
        compare-and-set(option<stamp>),
    }

    variant row-attachment {
        reference(capability-ref),
        own-publication(string),
    }

    record written-value {
        body: list<u8>,
        attachments: list<row-attachment>,
    }

    resource registration-writer {
        put: async func(key: list<u8>, basis: write-basis, value: written-value)
            -> result<stamp, mutation-error>;
        delete: async func(key: list<u8>, basis: write-basis)
            -> result<stamp, mutation-error>;
    }
}
```

A written value's attachment is either a held `reference` or `own-publication(name)`, one of the
writer's manifest-declared publications; the host stores its endpoint (ADR 031 §7) and answers a
name the manifest does not declare with `invalid-attachment`. Reads still deliver `encoded-value`
with minted `capability-ref`s.

`registration-writer` writes into a host-assigned registration scope with the causal-map rules of
ADR 017 §9; `mutation-error` adds `conflict(option<stamp>)` for a failed compare-and-set. There is no
key-level write authorization yet: any micro bound to a collection with Register may write any key,
so a micro can supersede another's row; authority will be scoped by the collection query language
once it exists (guest and host run the same query engine), and the grant will be the query.
Dropping the handle releases mutation authority only. The host retracts incarnation-scope
contributions when the instance closes and installation-scope contributions when the owning plug is
uninstalled, as specified by ADR 031.
Documents retain their domain command interface and heads-based settlement rather than
being forced through either generic writer.

### 9. Capability bank

```wit
interface capability-bank {
    variant ref-status { live, dormant, disabled, faulted, unavailable, revoked }

    resource capability-ref {
        status: func() -> ref-status;      // never activates
        call: async func(request: list<u8>) -> result<list<u8>, capability-error>;
        retain-call: async func() -> result<call-binding, capability-error>;
        redeem-presentation: async func() -> result<presentation-component, capability-error>;
    }
    resource call-binding {
        call: async func(request: list<u8>) -> result<list<u8>, capability-error>;
    }
    resource presentation-component;    // mounted synchronously via component-ref
    resource published-capability;

    publish-call: func(name: string) -> result<published-capability, capability-error>;
    publish-presentation: func(name: string) -> result<published-capability, capability-error>;
}
```

The two publication kinds, Call and Presentation, are defined in ADR 031 §5. A
`capability-ref` is bound or lazy (ADR 031 §6); every use is async because a lazy
reference may activate its provider. Typed publication/redemption methods are finite and additive. Collection CBOR contains
attachment-slot indices; `encoded-value.capabilities` carries recipient-local authority on reads and
`written-value.attachments` names what a write attaches.
A retained typed proxy is owned by the consumer but becomes inert when its publication
or provider closes.

### 10. Installation applies durable registration scopes

Phase one has no guest installer world and no separate activation-registry protocol. The package/enablement service consumes admitted manifest defaults and writes them through privileged durable collection administration:

```text
begin installation scope(plug revision)
    validate target collection contract and schemas
    put(scope, key, value + logical endpoints)
commit atomically

uninstall plug
    remove installation scope atomically
```

Ordinary micro guests never receive this administration authority. A future installer or configuration component may receive a narrowly scoped durable editor and propose one atomic configuration change set. It may not forge target scope ids, recipient capability references, or live runtime publications.
## Manifest shape

### 11. The manifest describes possibilities, not live state

The current manifest conflates contract binding names, static contribution payloads,
and activation routing. Replace it with an open, versioned vocabulary conceptually
shaped as:

```text
MicroManifest {
    schema: ManifestSchema,
    id: MicroId,
    package_version: Version,

    entrypoints: Entrypoints {
        initialize: InterfaceRef,
        presentation?: InterfaceRef,
        installer?: InterfaceRef, // reserved; not executable in phase one
    },

    provides: [ProvidedContract],
    requires: [RequiredContract],
    configuration_requests: [ConfigurationRequest],
    collection_contracts: [CollectionContract],
    collection_requests: [CollectionRequest],
    contributions: [ContributionDeclaration],
}
```

All named members have stable ids. Unknown members are carried so newer manifests can
round-trip through older tooling.

### 12. Contract declarations

```text
ProvidedContract {
    id,
    contract: ContractRef,
    lifecycle: provided-resource contract metadata,
}

RequiredContract {
    binding,
    contract: ContractRef,
    necessity: Required | Optional,
}
```

`binding` is the local stable name used by configured requirement bindings and
`initial-context.require(binding)`. A requirement does not grant authority and a
provider declaration is not a live provision.

Resource contract metadata declares `Weak` or `Strong` retention where obtaining the
resource targets another micro. Callers cannot override it. Ordinary provided services
used as required dependencies are strong.

### 13. Configuration and collection requests

```text
ConfigurationRequest {
    binding,
    datum_selector,
    rights,
}

CollectionContract {
    id,
    selector_schema,
    key_schema,
    value_schema,
    write_context_schema?,
    offered_capabilities: Read | Register | DomainCommands,
    backing: Volatile | Durable | Materialized,
}

CollectionRequest {
    binding,                     // unique local name; the only name guest code uses
    contract,                    // CollectionContract id
    access: Read | Write,        // write implies read
    necessity: Required | Optional,
    resolution: Configured | AtRuntime,
}
```

A contract has no writer policy: retention belongs to the writer's scope (ADR 031 §4), and write
authority is not key-scoped yet: any micro bound to a collection with Register may write any key,
so a micro can supersede another's row. Authority will be scoped by the collection query language
once it exists (guest and host run the same query engine), and the grant will be the query.

These are requested/declared shapes, not grants. Configuration chooses concrete
documents, suppliers, and the concrete collection each collection request binds to (ADR 031 §11
specifies resolution, defaults, and the unconfigured lifecycle state). Collection query IRs
remain contract-specific CBOR interpreted by host and guest libraries.

### 14. Contributions are durable collection rows with lazy endpoints

```text
ContributionDeclaration {
    id,
    target_binding,       // local RequiredContract.binding
    collection_contract,
    key,
    value,                // schema-compatible CBOR
    endpoints: [LazyEndpointDeclaration],
}

LazyEndpointDeclaration {
    attachment_slot,
    publication_name,
    contract,
}
```

The target binding identifies where configuration routes the declaration; it does not hard-code a configured target instance into the package. Installation resolves the binding, validates registration authority and the target collection's schemas, then writes the row under the plug revision's installation scope. The row remains visible while the contributor is inactive and disappears when that scope is removed on uninstall.

A lazy endpoint declaration contains no live authority. Projection mints an authorized lazy attachment for each recipient. Redemption activates or joins the contributor, and successful initialization fulfills the named publication slot. Static contributions therefore need neither `TargetLive` activation triggers nor a second ephemeral collection.
ADR 031 specifies registration-scope ownership, collection availability, schema admission, and the toolbar/completion/layout examples.

### 15. What does not belong in the manifest

The manifest does not contain:

- configured instance identity;
- enablement, eager-root, or retry state;
- concrete requirement suppliers;
- granted document ids or principals;
- target acceptance/order/hide policy;
- runtime cursors, resource handles, capability references, or actor ids;
- live contribution rows; or
- optimistic mutation state.

Those are configuration or runtime facts.

## Implementation status

Built today:

- stable configured identities and fresh incarnations;
- enablement and per-instance `eager` startup;
- configured requirement bindings and cycle rejection;
- static manifest contribution declarations;
- incarnation-owned `accept` / `accept_all` policy;
- reachability from eager roots through requirements and accepted declarations;
- idle eviction, fault retention, broker revocation, and staged provisions;
- incarnation-owned local futures.

Not built:

- manifest admission from the `big_plugs_core` envelope and a Clay desired-state/enablement service;
- durable collection backing and installation registration scopes;
- lazy activation attachments and named publication slots;
- the unified writer-handle versus registration-scope ownership rule;
- resource-driven idle `Closing` sequencing and async `instance-root.close()` across both engines;
- the revised context-centered MDK surface; and
- installer/enablement reconciliation.

Built checkpoints include the leased collection WIT, owner-scoped registration writers with writer-owned cleanup (now to be revised to scope ownership), and ADR 029 capability attachments/redemption. Those checkpoints do not yet implement ADR 031's durable or lazy lifecycle.

## Consequences

- Manifests remain inspectable without executing package code.
- Installation metadata is mutable configuration data rather than hard-coded host logic.
- Runtime references and liveness converge on one resource graph.
- Lazy startup retains Emacs/VS Code-style declaration-before-load malleability without
  arbitrary enablement hooks in the first implementation.
- The WIT stays small and language-neutral; Rust MDK semantics remain adapters.
- The revised manifest is a breaking interface change. Existing manifest structs and
  snapshots must be replaced rather than shimmed when implementation begins.

## Deferred

- executable installation/enablement hooks;
- installer upgrade/downgrade/uninstall migrations;
- activation carriers beyond explicit roots, lazy capability redemption/invocation, and presentation mounting;
- idle grace, freezing, and preinitialized pools;
- a lazy-activation responsiveness policy: intent prewarm (hover/focus/visibility of a lazy endpoint
  takes a short warm lease), a keep-warm TTL bounded by an LRU/memory budget across incarnations,
  and immediate pending feedback with queued calls while an incarnation initializes. Activation
  latency per engine (Wasmtime, browser) is to be measured before any budget is chosen;
- dynamic mandatory dependency cycles;
- guest-served collection sources; and
- automatic lifecycle edges for every mount/invocation until concrete contracts choose
  strong or weak retention.

## Validation before acceptance

This ADR becomes Accepted only after tests demonstrate:

1. a manifest and its collection definitions are inspected without instantiating the guest;
2. a durable contribution remains visible while its provider is inactive;
3. reading its lazy endpoint does not activate the provider, while redemption starts or joins exactly one staged activation;
4. a demanded named publication is returned without polling a secondary collection;
5. retaining the redeemed live resource keeps the provider active and dropping the final strong edge begins orderly shutdown;
6. `instance-root.close()` runs once with dependencies retained, after which incarnation scopes and publications are revoked;
7. failed/cancelled initialization exposes no staged publication;
8. disconnected cycles do not remain live and activation cycles fail precisely;
9. faulted micros do not retry from demand churn; and
10. native, Wasmtime, and WCMP micros exercise the same lifecycle contract.
