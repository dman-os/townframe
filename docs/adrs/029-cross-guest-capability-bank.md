# ADR 029: Cross-guest capability bank

**Status:** Accepted. Implemented end-to-end (host bank, collection attachments,
typed presentation redemption) across the Wasmtime and WCMP engines, with the
validation tests below passing.

**Amends:** ADR 015's component-reference and presentation-slot direction. **Used by:**
ADR 017 for capability-bearing collection values.

## Context

Micros need to publish values that combine portable data with live authority. A
toolbar row may contain a surface mount, a menu entry may invoke an action, and a
composition row may refer to another host-mediated resource.

Neither serialization nor raw identifiers solve this safely:

- WIT resources are typed handles scoped to one component instance's resource table;
  they cannot be embedded in CBOR or copied into another guest's table.
- A process-global numeric ID is forgeable authority unless every use separately
  reconstructs grants, ownership, incarnation, and revocation.
- Moving the provider's resource to the first consumer prevents several consumers
  from using the same published capability.
- A generic collection waist cannot name every resource type in its row schema.

We want a bank-like operation: a provider deposits a typed resource and receives a
publishable reference; an authorized consumer redeems that reference into the
specific typed resource it expects. Resource acquisition and release must remain
RAII-shaped across guest boundaries.

## Decision

### 1. Publication retains authority in the host

The bank exposes typed publication methods for supported resource contracts:

```text
bank.publish_surface_mount(surface_mount) -> published-capability
bank.publish_action(action)               -> published-capability
bank.publish_menu(menu)                   -> published-capability
```

Publication consumes an owned typed resource; it does not retain a WIT borrow after the call. A provider that needs another local handle must explicitly retain or clone the resource first when that resource contract supports it. The returned publication lease controls the deposited host proxy.

`published-capability` is an owned publication lease. The host retains or proxies the
underlying typed resource; publication does not move it to a consumer. The lease
records:

- the resource contract and version;
- provider incarnation and owning scope;
- the host-side implementation/proxy;
- revocation state; and
- references derived for authorized recipients.

Dropping the publication lease revokes the publication and every derived proxy.
Closing the provider incarnation has the same effect. Cleanup is incarnation-checked,
so stale cleanup cannot revoke a replacement provider's publication.

### 2. Values carry recipient-local capability references

A generic `capability-ref` is a WIT resource, not a serializable ID. When a collection
projection delivers a value containing capability attachments, the host creates
recipient-local references authorized for that reader:

```text
EncodedValue {
    body: CBOR,
    capabilities: [capability-ref],
}
```

The CBOR body names attachment indices using a reserved tag. The attachment list
carries the authority. Copying the CBOR bytes alone conveys nothing, and arbitrary
bytes cannot be presented as a `capability-ref`.

A reference records the expected publication, recipient incarnation, allowed
contract, and revocation linkage. Guests cannot construct references or substitute a
trusted owner identity.

### 3. Redemption is typed and yields a derived proxy

Consumers redeem through typed methods:

```text
capability_ref.as_surface_mount() -> surface-mount
capability_ref.as_action()        -> action
capability_ref.as_menu()          -> menu
```

The bank checks the stored contract identity before creating the typed consumer-side
proxy. A reference cannot be reinterpreted as another resource kind. Contract
identity includes namespace, name, and version.

Redemption is not necessarily one-shot spending. By default it creates a derived
proxy while the publication remains host-owned, allowing several authorized consumers
and repeated local reads. A particular resource contract may explicitly define
one-shot redemption when its semantics require transfer.

### 4. RAII ownership and revocation are distinct

The recipient owns every successfully redeemed proxy and must drop it normally. If
the provider disappears first, the proxy remains a valid local resource handle whose
backing authority is closed. Operations on it return the contract's closed/revoked
state; the consumer then decides whether to drop it, show a missing-component
presentation, or retain a frozen/error representation.

This distinction preserves both facts:

1. the consumer really owns its acquired handle and controls its local lifetime; and
2. ownership of a handle cannot keep a dead provider implementation alive or make it
   callable.

For a surface mount, provider closure therefore does not secretly delete the
consumer's handle. Rendering observes that the implementor is gone and follows the
presentation recovery policy. In the usual collection path, the contributor's row is
also removed promptly, so the stale-handle interval is short.

Dropping a collection row releases the collection adapter's attachment references.
It does not forcibly drop a proxy the guest already redeemed and retained. The
underlying publication's revocation still makes such a proxy inert when its provider
or publication closes.

### 5. References are not ambient bearer IDs

An implementation may internally index publications with random or numeric IDs, but
the ID is not the authority boundary. Host checks are always made through a
recipient-local `capability-ref` resource table entry. The table binds:

- publication incarnation;
- recipient incarnation;
- contract identity;
- allowed operations; and
- revocation linkage.

Logging, serializing, or guessing an internal index cannot acquire authority.
References are not persisted across application restarts.

### 6. Capability attachment is transport-independent

ADR 017 uses CBOR plus an attachment table, but the bank is not collection-specific.
Another host API may return a `capability-ref` directly. The same publication can be
attached to several authorized values without exposing the provider's engine resource
or resource-table index.

Native and Wasm providers use the same semantic bank. Engine adapters translate the
host publication and recipient proxy into their local resource-table representation;
engine types never enter collection values or contract payloads.

## Failure model

Expected failures are typed:

- contract mismatch during redemption;
- provider/publication closed;
- recipient no longer authorized;
- resource-specific operation failure; and
- ingress/handle limits exceeded.

A closed provider is not a host invariant failure. Forged table indices, a host
returning the wrong typed implementation for a recorded contract, and stale cleanup
revoking a replacement incarnation are programming errors.

## Consequences

- Collection values remain portable CBOR while carrying real object capabilities.
- Authority is never inferred from a serialized identifier.
- Providers publish once; consumers receive independently owned typed proxies.
- Consumer RAII and provider revocation coexist: a retained handle can become inert
  without being silently removed from its owner.
- Generic transport knows only `capability-ref`; typed bank methods define the finite
  supported resource contracts.
- Revocation and invocation add one host indirection, accepted in exchange for safe
  cross-address-space lifetime and authorization.

## Deferred

- delegation from one recipient to another;
- independently retainable grants that intentionally outlive their publishing row or
  provider policy;
- persistent or network-transferred capability references;
- capability attenuation beyond the typed publication methods; and
- garbage-collection optimizations beyond ordinary resource drops and publication
  revocation.

## Validation before acceptance

This ADR becomes Accepted only after tests demonstrate:

1. typed publication and redemption across a real Wasm component boundary;
2. two consumers redeeming the same publication independently;
3. wrong-contract redemption failing without exposing the resource;
4. provider closure making retained proxies inert while leaving them safely droppable;
5. collection-row removal releasing attachment references;
6. stale cleanup leaving a replacement incarnation's publication alive; and
7. copied CBOR without its attachment table conveying no authority.
