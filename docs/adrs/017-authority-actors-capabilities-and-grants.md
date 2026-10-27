# ADR 017: Namespace capabilities, actor identities, and domain grants

**Status:** Accepted and implemented.

## Decision

The capability table provides handles, namespace provenance, attenuation, and
revocation. Actors, signing, and standing grants are domains built on that
mechanism, not prerequisites for generic capability minting. Co-hosting these
services does not give the generic table knowledge of actors or drawer policy.

Resource operations accept one domain capability. Identity proofs and a separate
signer proof are inputs to minting, not an alternative authorization path on every
read or write. The signer is bound when the domain capability is issued.

The protocols live in `big_cabinet_core` and `service_core_rs`; callers use
`service_client_rs::Client`, and `service_utils_rs` hosts the handlers. Direct,
IRPC, and XRPC calls enforce the same rules.

## Capability mechanism

A capability is an opaque random UUIDv4 bearer handle into the owning authority's
in-memory table. It is not a signed token, an actor UUID, a document ID, or a
self-contained credential another authority can verify.

The table records a handle's namespace, opaque object data, rights, revocation
state, and derivation dependencies. The object data belongs to its issuing domain:
`DocsRepo/scope/v2/{JSON}` and drawer scopes are examples, not grammars understood
by the generic table. Keeping data here avoids a second per-domain scope map; it
does not move scope interpretation into the table. Domain-specific mutable state
still belongs to the domain.

The trusted embedder gives each domain a **namespace capability**, separately
from its actor identity proof. Possession of the namespace capability authorizes
minting under that namespace. Naming a namespace UUID does not. The table must
distinguish this minting authority from an ordinary object whose text happens to
name a namespace. Generic minting must not resolve an actor or consult grants.
A stable namespace ID may be provisioned by the embedder; its handle is temporary.

Rights remain `use`, `narrow`, `delegate`, and `mintChild`. Generic attenuation
copies the namespace and object and drops rights only. It cannot change scope,
change the bound signer, or turn an ordinary cap into a namespace cap. A domain
mints changed object data after checking that its meaning narrows the authority
presented to that domain. Possessing a namespace minter is trusted authority to
name objects; callers never receive the service's namespace minter.

Inspection returns descriptive metadata, not parent handles, namespace-minter
handles, signer proofs, or other bearer authority. Debug output redacts handles.

## Derivation and revocation

There are two different facts in a domain mint:

- The namespace capability proves **who may issue this domain's objects**.
- The source capability proves **which authority this particular mint narrows**.

Recording only the issuer as the parent loses the second fact. A resulting cap
must stop working if either source or issuer is retracted. A bound signing proof
and any grant-dependent authorization gate are additional dependencies when
applicable. This is a dependency DAG, not necessarily a single-parent tree.

Dependencies are immutable and refer to already-live handles. Issuance rejects
unsupported dependency depth before publishing a handle. Liveness traversal is
bounded and deduplicates shared ancestors; diamonds must not cause exponential
work. A cap is live only while it and every dependency remain live. A holder may
retract its own handle or a handle derived from it. Retraction does not reveal
broader handles to the holder and does not invalidate historical signatures.

The optional `parent` is the authority being narrowed: it must have `narrow`, and
the new rights must be a subset of its rights. Additional `dependencies` are
liveness-only gates, with no `narrow` requirement or rights ceiling. A USE-only
signer proof can therefore bind attribution without limiting domain rights.

Generic attenuation preserves these dependencies. Domain narrowing records the
source even when crossing namespaces. This makes revocation cross-domain without
requiring each domain to sweep its own scope map after an actor is retracted.
Checking dependencies and publishing a new cap must be serialized against
retraction; a concurrent mint cannot escape its source's withdrawal.

A domain grant revocation also withdraws the live authority derived from that
grant, rather than relying on a grant lookup at every use. The grant-owning domain
must track that relationship, for example with opaque revocable gate capabilities
attached to its grants. A gate is a dependency, not a new identity or permission
that the generic table interprets. Mint and revoke must be ordered so that a
completed revocation cannot be followed by publication using the old grant.
Alternative grant paths may allow a caller to reacquire a cap; they do not revive
a retracted handle. Quiet streams must observe retraction and release their demand.

## Actor registry and signing

AuthorityActors is a domain with its own namespace capability. Its durable state
contains actor UUIDs, hierarchical paths, Ed25519 keys sealed under the node's
OS-keyring wrapping key, and node-signed attribution certificates. Opening the
same actor after restart returns the same identity and key, with a new handle.

An identity proof is an ordinary capability that AuthorityActors associates with
an actor in its instance-local issuance index. Recognition checks that exact live
handle and the requested right. Matching object text, namespace metadata, or an
ancestor's identity is not recognition. Generic attenuation does not copy the
association; `OpenActor` is the domain operation for obtaining a recognized proof
with reduced rights. This exact-issuance rule is actor-domain policy, not a special
rule used by the generic table to authorize minting.

The operator is the actor-path root. An actor proof with `mintChild` can open an
immediate child with bounded rights; actor creation does not create any grants.
An actor's UUID, key, certificate, and URL survive rotation of its proof. A missing
wrapping key is a boot failure, never permission to silently replace identities.

At domain mint time the caller presents:

```text
MintDomainCapability {
    authority: identity proof or an accepted source domain capability,
    signer: identity proof,
    scope,
    rights,
}
```

The mint independently proves the source's authority and the signer's identity.
A runtime may narrow the user's authority while binding attribution to a plug.
The signer's grants never enlarge the requested scope. The bound signer is
immutable under generic attenuation; rebinding requires another authorized domain
mint and a fresh proof of the intended signer.

The domain also records the originating actor of the authority independently from
the bound signer and preserves that provenance through narrowing. Creation and
recovery policy can use this stable actor ID without treating the signer as the
source of entitlement. The generic table does not interpret actor provenance.

Resource calls then carry only the resulting capability. The domain validates its
namespace, liveness, rights, and scope and uses the bound signer for attribution.
An identity proof is rejected by resource operations, even when that identity has
standing grants. There is no identity fallback after a scoped cap is refused.

Binding an actor UUID is not permission to expose the actor's signing proof. The
signing path must verify the domain's authority to use that binding after the
domain authorizes the operation. A guest holding a document cap must not acquire
an unrestricted `SignFacetClaim` oracle or an actor proof it can exchange for
broader authority. Identity signing remains an explicitly held actor-domain
capability. Signatures on facet claims and attribution certificates remain
cryptographic even though capability handles need no signatures.

A bound signer records whose delegated signing authority is used; it does not
prove which executable sent the request. Handing the cap to another holder also
hands over its scoped attribution authority. Runtime isolation and handle custody
must enforce any stronger claim about which plug executed.

## Grants are domain policy

A standing grant is durable entitlement to reacquire authority, not an additional
kind of capability in the generic table. Grant administration is protected by
ocaps like any other domain operation. The grant-owning domain interprets subjects,
resources, access levels, and containment and may use reusable storage helpers;
the generic table does not implement a global identity ACL.

DrawerService owns local actor entitlements over drawers and creation rights.
DocsRepo's standalone policy owns document and placement grants when no drawer
policy is configured. The drawer projection and grant rows may be joined in SQL
at mint time; a separate generic Authority grant RPC is not required. Grant checks
must be batched, not one authority-service call per query result.

**Not every actor needs a grant row on every drawer.** Only identities with an
independent standing entitlement need rows. A plug may have a durable actor and
key but no grants; its runtime supplies a scoped cap for each dispatch. Children
do not automatically inherit their parent's grants. A principal can delegate an
existing cap without writing a grant row at all.

Issuer-authorized standing grants survive restart. Temporary delegations written
through an object capability depend on that capability and disappear with it,
including at restart. Promoting such a delegation to a durable entitlement is not
implicit; no persistence-upgrade operation is part of this decision.

Drawer membership, local actor grants, and Keyhive principal membership are
separate facts. Local policy changes do not by themselves rotate remote keys or
alter a Keyhive roster. The existing drawer membership workflow still owns those
coordinated effects.

## Use-time behavior

Ordinary operations perform one capability resolution, not an unsuccessful domain
lookup followed by an actor lookup and grant evaluation. Namespace provenance and
scope identify the applicable domain. Multiple issuers are accepted only through
an explicit trust/delegation policy, never by trying every authority until one
accepts a string.

Scope still needs domain interpretation: a drawer-scoped cap may require current
membership SQL to determine whether a document is in that drawer. Removing
identity-grant checks from the hot path does not remove membership checks or let
an authorized origin reference authorize dereferencing its target. Composite
operations must cover every resource they affect.

## Lifetime and startup

| State | Lifetime |
| --- | --- |
| Actor/node identity, sealed keys, certificates | Durable |
| Issuer-authorized domain grant rows | Durable |
| Capability handles, namespace handles, issuance associations, signer bindings | Authority-instance local |
| Delegations dependent on a temporary object cap | At most that cap's lifetime |
| Facet-set and facet-reference indexes and their progress cursors | Durable |

Restart creates fresh handles and reacquires subsystem authority. Old handles
must fail even when the same durable actor or namespace exists. Running another
authority instance on the same durable actor store must not clear or adopt the
first instance's handle table. Durable command ledgers store stable actors and
operation intent, never a capability required for replay.

Facet indexes are not rebuilt at boot: their workers resume from durable frontier
cursors and idempotent settlement. Worker ownership and reverse-order shutdown are
lifecycle concerns, not reasons to discard those indexes.

## Implementation and evidence

This decision removes three earlier mechanisms: per-call
`Authorization { authority, signer }` with an identity-or-object fallback,
actor-proof-authorized generic minting, and the central Authority grant ledger.
`Authorization` remains only at domain mint seams; resource DTOs carry `capability`.

- Authority keeps handles and exact issuance associations in memory and validates
  the bounded dependency DAG atomically with publication and retraction.
- `Grants` stores namespace-partitioned durable rows and tracks temporary support
  and revocation gates. Each domain lifetime owns one shared helper per namespace;
  service shutdown retires that namespace after its host and workers stop.
- DocsRepo and DrawerService keep private signer bindings and carry scope and
  originating-actor provenance as opaque capability data. Their resource handlers
  reject identity proofs instead of consulting standing grants as a fallback.
- Authority, grant-helper, Docs, Drawer, and wire tests cover independent instances,
  restart/reacquisition, cross-domain retraction, stale grant proofs, withdrawal
  racing publication, signer non-escalation, quiet-stream cancellation, and
  delegated callers without independent grants.

There is no compatibility shim for the unreleased wire APIs. Durable identities,
keys, and issuer-authorized grants must not be silently discarded during storage
migration. BigRepo/Keyhive node authority remains below Cabinet and unchanged.
