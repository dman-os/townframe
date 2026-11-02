# ADR 024: Micro lifecycle, activation registrations, and manifest contract

**Status:** Proposed. The lifecycle core is partially implemented; the WIT and revised
manifest shape are not.

**Amends:** ADR 014's binding-derived lifecycle and ADR 015 §§7–10. **Builds on:**
ADR 017's projection leases and ADR 023's cross-guest capability bank.

## Context

Dayframe micros are configured singleton services, not process-tree children. At most
one incarnation of one configured micro identity is live at a time, while several
configured identities may select the same provider package. Dependencies form a graph:
requirements, accepted contributions, collection projections, invocations, and
capability references can connect otherwise unrelated micros.

We want the familiar parts of a process model — one initialization entry point,
owned resources, isolated failure, host supervision, and deterministic cleanup —
without inventing `fork`, parent/child ownership, or explicit `start`/`stop` APIs.
Disablement is the only intentional stop. Enabled micros may remain idle until an
eager root or a resolved binding requires them.

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

### 1. Lifecycle nodes are configured instances

The graph node is a stable configured instance identity, not a provider/package id and
not a runtime incarnation. Configuration names:

```text
ConfiguredMicro {
    identity,
    provider,
    enabled,
    eager,
    requirement_bindings,
    accepted_activation_registrations,
    configuration_grants,
}
```

A fresh runtime incarnation is created for each successful initialization attempt.
Changing only `eager` recomputes reachability without replacing an incarnation that
remains wanted. Changing provider, grants, or requirement bindings creates a new
incarnation when the instance is next live.

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

An enabled configured instance is live exactly when reachable from an eager root
through strong binding resources. Incoming-reference counts do not determine
liveness; disconnected cycles cannot keep themselves alive.

Every host resource contract declares one lifecycle class:

```text
NoMicroRetention  // host state only; no micro target
Weak              // usable while target lives, becomes inert after closure
Strong            // keeps the target incarnation live
OwnedEffect       // destroyed with its owner; not a target-retention edge
```

The holder cannot promote a weak contract to strong. Examples:

- required provision binding: `Strong`;
- accepted reverse-dependency binding: `Strong`;
- guest-served projection lease: `Strong`;
- host-materialized projection lease: `NoMicroRetention`;
- in-flight required invocation: `Strong`;
- surface-mount/capability proxy: `Weak` by default;
- contribution writer, publication lease, and owned future: `OwnedEffect`.

A strong resource combines authority to use the target with the reason the target must
remain live. There is no separate runtime demand signal shadowing the reference.

### 3. Reverse dependencies have declaration, registration, and binding phases

A reverse dependency is not a live row and not code executed merely to discover that
code should run.

#### Package declaration

The package manifest declares a possible contribution and default activation
registration. It is inspectable without instantiating the guest.

#### Installation registration

Configuration grants the package manager authority to register only allowed subjects,
targets, and extension contracts. Installation writes a persistent record:

```text
ActivationRegistration {
    id,
    subject_configured_instance,
    trigger,
    requested_contract,
    declaration_id,
}
```

The record is data, not a live resource. Disablement makes it ineligible without
deleting it; uninstall removes it. The first implementation installs manifest defaults
and permits authorized configuration tools to add/remove records. Executable install
or enable hooks are deferred.

#### Runtime binding

When the trigger is active, the subject is enabled, and the target policy accepts the
registration, the host resolves it into a concrete strong binding. That resource pulls
the subject live. Successful initialization then publishes the actual contribution.

```text
manifest declaration
    -> authorized persistent registration
    -> active target acceptance
    -> strong binding target -> contributor
    -> contributor initialization
    -> live contribution row/publication
```

Declarations, registrations, bindings, and live rows remain distinct and inspectable.

### 4. Activation triggers are host-inspectable data

The initial trigger vocabulary is deliberately small:

```text
Trigger =
    TargetLive {
        target_configured_instance,
        extension_contract,
    }
  | ExplicitRoot
```

A target may accept all eligible registrations for an extension contract or a
configured subset/order. Registration authority plus target acceptance is required;
a package cannot unilaterally pin an arbitrary privileged target or inject itself into
an extension point.

Future trigger members may include command invocation, route mounting, guest-served
projection acquisition, or capability request. Each still resolves to an ordinary
strong resource edge. Unknown trigger members are carried by the manifest vocabulary
but are not activated by hosts that do not understand them.

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

On successful return, the host atomically exposes staged publications. On error,
trap, cancellation, or dropped initialization, the pending incarnation scope is
dropped and all acquired/staged resources unwind through RAII. This is transactional
visibility, not a general rollback protocol for arbitrary external side effects.

The host owns lifecycle authority. The exported `instance-root` proves successful
initialization and is held by the host for the incarnation; it is not a guest-visible
`stop` capability.

### 6. Closing preserves dependencies through cleanup

When an active incarnation becomes unreachable or disabled, it enters `Closing`:

1. reject new outward publications and new strong acquisitions;
2. stop ordinary delivery and request owned work to finish/cancel;
3. retain existing strong dependencies while incarnation cleanup runs;
4. drop the incarnation scope after cleanup;
5. dropping that scope releases outgoing strong bindings and owned effects;
6. recompute reachability and close newly unreachable suppliers.


The host invokes `instance-root.close()` exactly once while the incarnation's existing dependencies remain retained. The guest uses it to finish or cancel asynchronous owned work; returning does not grant authority to publish new outward resources. After it returns, traps, or is cancelled by host supervision, the host drops the incarnation scope and its resources. Abrupt engine loss may skip guest cleanup, so correctness still relies on host-owned RAII cleanup rather than on `close()` completing.
This naturally closes dependents before providers. Mandatory configured dependency
cycles are rejected. A dynamically formed unreachable strongly connected component
closes as a group; members must tolerate peers becoming revoked during cleanup.

Weak ADR 023 proxies remain owned and safely droppable after provider closure, but
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

    initialize: async func(context: lifecycle-host.initial-context)
        -> result<instance-root, initialization-error>;
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
    resource registration-writer {
        put: async func(local-key: list<u8>, based-on: option<list<u8>>, value: encoded-value)
            -> result<mutation-accepted, mutation-error>;
        remove: async func(local-key: list<u8>, based-on: option<list<u8>>)
            -> result<mutation-accepted, mutation-error>;
    }
}

interface collection-causal-write {
    resource causal-writer {
        mutate: async func(
            key: list<u8>,
            observed-context: list<u8>,
            mutation: list<u8>,
        ) -> result<mutation-accepted, mutation-error>;
    }
}
```

Documents retain their domain command interface and heads-based settlement rather than
being forced through either generic writer.

### 9. Capability bank

```wit
interface capability-bank {
    resource capability-ref;
    resource published-capability;

    publish-surface-mount: func(surface-mount)
        -> result<published-capability, capability-error>;
    publish-action: func(action)
        -> result<published-capability, capability-error>;

    surface-mount: func(borrow<capability-ref>)
        -> result<surface-mount, capability-error>;
    action: func(borrow<capability-ref>)
        -> result<action, capability-error>;
}
```

Typed publication/redemption methods are finite and additive. Collection CBOR contains
attachment-slot indices; `encoded-value.capabilities` carries recipient-local authority.
A retained typed proxy is owned by the consumer but becomes inert when its publication
or provider closes.

### 10. Installation registration API

Phase one has no guest installer world. The package manager consumes manifest defaults
and writes through a privileged host API:

```wit
interface activation-registry-admin {
    resource registration-editor {
        put: func(registration: list<u8>) -> result<_, registration-error>;
        remove: func(id: string) -> result<_, registration-error>;
        commit: func() -> result<_, registration-error>;
    }
}
```

Ordinary micro guests never receive this editor. A future installer component may
receive a narrowly scoped editor and propose one atomic registration/configuration
change set. It may not publish runtime resources.

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
    contributions: [ContributionDeclaration],
    activation_defaults: [ActivationRegistrationTemplate],
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
    offered_capabilities: Read | Register | CausalWrite | DomainCommands,
}
```

These are requested/declared shapes, not grants. Configuration chooses concrete
documents, suppliers, and allowed access. Collection query IRs remain contract-specific
CBOR interpreted by host and guest libraries.

### 14. Contributions and reverse activation

```text
ContributionDeclaration {
    id,
    target_binding,       // local RequiredContract.binding
    extension_contract,
    payload,              // Lexicon-compatible CBOR
    capability_slots: [CapabilitySlotDeclaration],
}

ActivationRegistrationTemplate {
    id,
    subject: Self,
    trigger: TargetLive {
        target_binding,
        extension_contract,
    },
    declaration_id,
}
```

The target binding identifies where configuration routes the declaration; it does not
hard-code a configured target instance into the package. Installation resolves `Self`
and the configured binding, validates registration authority, and writes the persistent
registration. Target acceptance then produces the runtime strong edge.

A static contribution payload may describe a toolbar item before the contributor runs,
but live capability slots are filled only during successful initialization. Manifest
payload cannot contain authority.

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

- the WIT above;
- resource-classified strong/weak lifecycle edges;
- persistent installation activation registrations;
- durable target acceptance configuration distinct from incarnation broker state;
- resource-driven `Closing` sequencing;
- capability bank and capability attachments;
- collection waist;
- async initialization returning `instance-root`; and
- installer/enablement code.

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
- triggers beyond target-live and explicit roots;
- idle grace, freezing, and preinitialized pools;
- dynamic mandatory dependency cycles;
- guest-served collection sources; and
- automatic lifecycle edges for every mount/invocation until concrete contracts choose
  strong or weak retention.

## Validation before acceptance

This ADR becomes Accepted only after tests demonstrate:

1. a manifest is inspected and reverse activation registered without instantiating its
   guest;
2. a live target resolves an accepted registration into a strong contributor binding;
3. dropping the target closes the contributor after dependent cleanup;
4. weak surface proxies survive as inert, droppable resources without retaining the
   provider;
5. failed/cancelled initialization exposes no staged publication;
6. disconnected cycles do not remain live;
7. faulted micros do not retry from demand churn; and
8. native and Wasm micros exercise the same lifecycle contract.
