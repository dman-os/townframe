# Big Cabinet Authority

[ADR 022](adrs/022-authority-actors-capabilities-and-grants.md) specifies the
namespace-capability model implemented by Authority, DocsRepo, and DrawerService.
Resource requests carry one scoped cap; identity and signer proofs belong at the
mint seam, with no per-call identity fallback.

## Boundaries

- The capability table owns temporary handles, namespace provenance, immutable
  dependencies, attenuation, inspection, and retraction. It does not parse drawer
  or document scopes or use actor identities to authorize generic minting.
- AuthorityActors owns durable identities and sealed signing keys, and associates
  exact temporary identity proofs with those actors.
- Domains own scope interpretation, standing grants, and the revocation of
  authority derived from them. Grant administration is itself
  capability-protected, not a privileged global ACL facility.
- Keyhive owns the node's remote membership and cryptographic access below Cabinet.
  Local capabilities neither bypass it nor replace its signed authorization graph.

Protocol declarations and DTOs are in `big_cabinet_core`/`service_core_rs`, clients
in `service_client_rs`, and hosting and transport adapters in `service_utils_rs`.
All transports use the same handlers.

## Boot and namespace provisioning

Boot returns an `Authority`, its client, the operator identity proof, the node's
attribution issuer, and a `StopToken`. The operator's identity and signing key are
durable; its proof is fresh for this authority instance. Stop dependants before
the authority; never persist a capability in a command ledger for later recovery.

The trusted embedder provisions each subsystem with two separate authorities:

1. Its actor proof for identity and signing.
2. Its namespace capability for issuing objects and managing that namespace.

The embedder may use a durable domain identifier as the namespace label. A label
is public metadata, not a way to open a namespace over the public protocol.
`AuthorityService::open_namespace` is an embedder API, not an RPC available to an
arbitrary caller. Actor recognition is not involved in validating the returned
namespace capability. A holder of an actor proof alone cannot mint in that actor's
purported namespace.

The actor registry has its own namespace, just like other domains. Opening a
child actor creates or retrieves its durable identity and issues an exact
recognized proof. It does not grant the actor access to any document or drawer.

## Issuance and recognition

`MintCapability` takes the issuer's namespace capability, opaque object data,
rights, an optional `parent` source, and additional `dependencies`. A parent must
have `narrow` and bounds the result's rights. Additional dependencies are liveness
gates, not rights ceilings: signer proofs and grant gates do not need `narrow`.
Trusted bootstrap issuance can omit a caller source. The new cap depends on the
namespace, parent, and every additional gate. A namespace holder is trusted to
validate object meaning; rights-bit subset checks alone cannot prove that one
JSON scope is narrower than another.

On use, namespace provenance is obtained from the authority's table, never from
caller-supplied metadata. Only the domain interprets the object. Generic
attenuation preserves namespace and object data and drops rights. It cannot
forge a namespace minter by choosing a special string.

AuthorityActors additionally requires an exact issuance association to recognize
an identity proof. A generic attenuation is not a recognized identity. This is a
domain-specific recognition policy; the generic cap table has no actor registry
lookup in its mint path.

Inspection deliberately omits dependency handles and namespace authority. An
opaque handle is bearer authority: keep it confidential, redact it in diagnostics,
and do not mistake an actor/document/namespace ID for such a handle.

## Revocation and lifetime

Dependencies form a DAG: a cap may depend on an issuer, a caller's authority, and
other policy gates. Retraction invalidates the handle and anything depending on
it, including caps from another namespace. Traversal must be bounded and deduped.
A sibling cap is not affected merely because it shares a namespace label.

Handles and identity issuance associations are in memory, not SQLite rows cleared
at startup. Restart requires reacquisition. Another authority instance cannot
adopt old handles or invalidate the first instance by truncating a shared table.
The durable actor registry and sealed keys are unchanged by handle expiry.

Domains use `Grants` for namespace-partitioned standing rows and instance-local
revocation gates. Trusted bootstrap and creation policy may write durable rows;
`GrantDrawer` writes a temporary delegation supported by the caller's live cap
and requires `delegate`. Temporary support is never promoted to durable storage.
The generic Authority protocol has no grant/check/revoke-grant RPCs.

Each domain lifetime owns one shared grant helper for its namespace. Its shutdown
token first stops admissions and workers, then retracts the namespace, including
cross-domain descendants justified by its grants. Restart reopens that namespace
with a fresh handle and reacquires scoped caps from durable grants. A stopped
helper cannot be used after namespace retirement.

Legacy durable-cap schemas and unmigrated central grant rows fail boot explicitly.
They are not silently cleared; durable identity/key/grant migration must be handled
before booting those stores with the new implementation.

## Domain API: mint with two proofs, use one cap

`authority` and `signer` are supplied only at the mint seam:

```text
identity proof or existing scoped cap ── authority ─┐
                                                  ├─ domain mint ─► scoped cap
actor proof ─────────────────────────── signer ────┘

Read / Write / Query / Observe(scoped cap, operation arguments)
```

The domain consults its standing grants only when the source is an identity
proof. An existing scoped cap is narrowed without consulting grants or borrowing
permissions from the signer. Resource operations require a domain cap and have
no identity fallback.

The resulting cap has an immutable signer binding. Its holder does not learn the
actor's proof or private key and cannot use the cap to sign arbitrary facet
claims directly. The domain checks the operation's scope before the signing
service uses the binding. Rebinding requires proof of the intended signer at a
new authorized mint. Ordinary attenuation never changes attribution.

The domain carries the originating actor separately from the bound signer.
Narrowing preserves this provenance; creation and replay policy must not substitute
the signer for the actor whose entitlement supplied the authority. This metadata
is opaque to the capability table and does not expose an actor proof.

A plug can therefore have a durable identity and no drawer grant rows at all.
The runtime mints a dispatch cap from the user's authority, binds the plug as its
signer, and gives only that scoped cap to the guest. A cap can be passed on; the
binding authenticates delegated signing authority, not which binary executed.

Grant-owning domains must couple minting to revocation: after a grant withdrawal
completes, it must not be possible to publish a cap justified by the old grant.
Opaque dependency gates provide cross-domain invalidation without repeating
grant checks on each use. Only the selected sufficient grant support becomes a
dependency; withdrawing an unused alternative does not invalidate the cap.
Withdrawing selected support invalidates the cap even if another grant could
justify a fresh one. Quiet observations terminate when their capability is
retracted and release any active document demand. Restoring a grant requires a
fresh capability and subscription; it never revives an ended stream.

## What does not change

Actor seeds remain sealed with ChaCha20-Poly1305 under the OS-keyring wrapping key
and bound to their registry row. Losing the wrapping key is a boot error rather
than a request to create replacement identities.

Facet claims and node-signed attribution certificates remain signed artifacts.
An old valid signature does not become invalid when a live cap is retracted.
Neither attribution nor a local cap proves historical Keyhive write authority.

Facet-set and facet-reference indexes resume from their durable frontier cursors;
restarting capability state does not rebuild those indexes. Durable command
recovery uses stable actor IDs and operation intent, not expired bearer handles.

## Regression coverage

Authority tests cover namespace/identity separation, independent live instances,
restart/reacquisition without key loss, dependency retraction across namespaces,
and generic attenuation of identity proofs. Grant-helper tests cover durable and
temporary support, withdrawal versus publication, replay, and domain retirement.
Docs and Drawer tests exercise scoped requests, signer non-escalation, delegated
actors without grants, attribution provenance, and quiet-stream access loss.
Core wire tests and document snapshots pin the single-cap request shapes. These
unreleased APIs intentionally break the old two-slot resource contract; there is
no compatibility fallback.
