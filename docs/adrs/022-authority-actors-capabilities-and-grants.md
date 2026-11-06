# ADR 022: Actors, capabilities, and grants

**Status:** Accepted and implemented; revised 2026-11-06 to explain the model from the caller's side and to match N2b (the credential header). The `Narrow` operation and the vocabulary changes in [Proposed](#proposed) are not implemented yet.

## The model

Three things are kept apart:

- An **actor** is *who*: a durable identity with an Ed25519 key. Examples: the operator, the docs service, a nodule.
- A **grant** is *what an actor is entitled to*, durably. For example, "actor A may Write drawer D". It is a role assignment in RBAC terms, stored and interpreted by the domain that owns the resource: drawers own drawer grants.
- A **capability** is *a live handle you present to act*. It is an opaque random id into the node's in-memory capability table that names an object, a set of rights, and its derivation. You get one by showing a domain your identity and an entitlement: a grant, or a capability you already hold. The domain checks the entitlement then, once.

At use time a domain checks only the handle, never the actor's grants. Grants are consulted when a capability is minted.

### The file-descriptor analogy

| Unix | Cabinet |
| --- | --- |
| user / uid | actor |
| file permissions, group membership | grants (drawer access: Relay < Read < Write < Admin) |
| `open(path)` → fd; the permission check happens here | mint → capability; the grant check happens here |
| `read(fd)`, `write(fd)` with no re-check | a resource operation with the capability; grants are not re-checked |
| fd rights fixed at open (`O_RDONLY`) | capability rights and scope fixed at mint |
| `openat(dirfd, "x")` | a docs capability scoped to drawer D, plus the `doc_id` in the request |
| `close(fd)` | retract |
| fds die with the process | handles die with the authority instance (restart means reacquire) |
| passing an fd over a Unix socket | handing a `CapId` to another caller: handles are bearer references |

Like an fd, a capability is both the **reference** (which object) and the **authority** (what you may do to it).

### What a request carries

Every request carries two things:

- **The `Authorization` header** (`Envelope.metadata.authorization`, `Bearer <authority>.<signer>`): *who is calling*. Dispatch admits the call on it. Domains use it at mint time to resolve the caller's identity and its grants (`DomainAuthority::Identity`), and to bind attribution.
- **A `capability` in the body**: *what the call acts on*. A docs capability for docs operations, a drawer capability for drawer operations. The body also names the specific object (`doc_id`), which must fall inside the capability's scope. Resource operations resolve the body capability as a scoped handle only (`DomainAuthority::Scoped`). An identity presented there is refused; there is no identity fallback.

The node operator is the root of the actor tree. It can mint a capability for anything on its node without a grant row (D24). It still acts through minted capabilities like everyone else.

### Worked flow: drawer-wide reads and writes

A docs capability scoped to a drawer (`DocScope { drawers: {D: Write} }`) covers every document in D. On each request the docs service asks one question: is this `doc_id` in a drawer the capability holds at the needed level (`doc.rs`, `allowed`)?

- Grouped reads and polls over D use the one capability.
- Writing N documents in D is N write calls with the same capability. Nothing is minted per document.
- Creating a document placed in D needs Write on D.

**Narrowing is how the glue hands out less, not something a worker does per write.** The nodule runtime or the clay micro runtime turns its context into capabilities. When it gives a component less than it holds, it narrows once per hand-off: one document out of the drawer, Read instead of Write, one drawer out of several. The component then just uses what it was given.

### Mapping to RBAC

| RBAC | Here | Difference |
| --- | --- | --- |
| a role (Reader, Editor, Admin) on a resource | a grant: `DrawerAccess` on a drawer | the same idea |
| a role check on every request | a check once at mint; requests present the minted handle | revoking the grant also kills handles minted from it, through a revocation dependency |
| service accounts with roles | subsystem and nodule actors | a nodule usually has no grants and gets a scoped handle per dispatch: it reaches only what its caller handed it |
| impersonation ("on behalf of") | a capability's bound signer, separate from the authority it narrows | a runtime can narrow a user's authority while signing as the plug |
| session tokens | capability handles | not signed; meaningful only to this node's authority |

### What it defends against

- **Confused deputy:** a service acts with the exact handle its caller passed, not with its own broader rights.
- **Ambient authority:** nothing works merely because of who you are. The operator's power is a power to mint, not a bypass.
- **Stale authority:** retracting a handle or revoking a grant kills every handle derived from it, across domains; derivation is a dependency DAG checked for liveness. A restart kills every handle.
- **Escalation through narrowing:** narrowing only drops rights or shrinks scope, and it cannot swap a bound signer.

What it does not defend against:

- **Handle theft.** A handle is a bearer secret. Local transports protect it (the 0700 socket directory and peer credentials). Remote clients ride iroh's encrypted, node-authenticated transport.
- **Offline or cross-node verification.** A handle means nothing elsewhere. Peers authorize through keyhive membership, not through these handles.
- **Time.** There is no expiry: the capability system has no clocks. A handle lives until it is retracted or the authority restarts. Dispatch deadlines bound how long a call runs, not what it may do.

## Mechanism

### Capability table

A capability is an opaque random UUIDv4 bearer handle into the owning authority's in-memory table. It is not a signed token, an actor UUID, a document ID, or a self-contained credential another authority can verify. The table records a handle's namespace, opaque object data, rights, revocation state, and derivation dependencies. Resolving, minting and retracting are in-memory operations on that table (`authority/cap.rs`); SQLite holds only durable actors and grants.

The object data belongs to its issuing domain. `DocsRepo/scope/v2/{JSON}` and drawer scopes are examples, not grammars the generic table understands. Domain-specific mutable state still belongs to the domain.

The trusted embedder gives each domain a **namespace capability**, separately from its actor identity. Possessing it authorizes minting under that namespace; naming a namespace UUID does not. Generic minting never resolves an actor or consults grants. A stable namespace ID may be provisioned by the embedder; its handle is temporary.

Rights are `use`, `narrow`, `delegate` and `mintChild`. Generic attenuation copies the namespace and object and drops rights only. It cannot change scope, change the bound signer, or turn an ordinary capability into a namespace capability. A domain mints changed object data after checking that its meaning narrows the authority presented to that domain. Callers never receive the service's namespace minter.

Inspection returns descriptive metadata, never parent handles, namespace minters, signer proofs or other bearer authority. Debug output redacts handles.

### Derivation and revocation

A domain mint records two facts:

- The namespace capability proves **who may issue this domain's objects**.
- The source capability proves **which authority this mint narrows**.

A resulting capability stops working if either is retracted. A bound signing proof and any grant-dependent gate are further dependencies when they apply, so derivation is a DAG, not a tree. Dependencies are immutable and refer to already-live handles. Liveness traversal is bounded and deduplicates shared ancestors. Publishing a new capability is serialized against retraction, so a concurrent mint cannot escape its source's withdrawal.

The optional `parent` is the authority being narrowed: it must have `narrow`, and the new rights must be a subset. Additional `dependencies` are liveness-only gates, with no rights ceiling. A USE-only signer proof can therefore bind attribution without limiting domain rights.

A grant revocation withdraws the live authority derived from that grant through a revocation gate the grant-owning domain attaches when it mints. A gate is a dependency, not a permission the generic table interprets. Mint and revoke are ordered so that a completed revocation cannot be followed by publication under the old grant. Quiet streams observe retraction and release their demand.

### Actors and signing

AuthorityActors is a domain with its own namespace capability. Its durable state holds actor UUIDs, hierarchical paths, Ed25519 keys sealed under the node's OS-keyring wrapping key, and node-signed attribution certificates. Opening the same actor after restart returns the same identity and key, with a new handle. A missing wrapping key is a boot failure, never permission to replace identities.

An identity proof is an ordinary capability that AuthorityActors associates with an actor in its instance-local issuance index. Recognition checks that exact live handle and the requested right. `OpenActor` is how a holder obtains a recognized proof with fewer rights. The operator is the actor-path root; a proof with `mintChild` can open an immediate child with bounded rights. Creating an actor creates no grants.

At domain mint the caller presents an authority (an identity proof, or an accepted source capability of that domain), a signer identity proof, a scope and rights. The mint independently proves the source's authority and the signer's identity. The signer's grants never enlarge the requested scope, and the bound signer is immutable under attenuation. The domain records the originating actor separately from the signer and preserves it through narrowing.

A bound signer records whose delegated signing authority is used; it does not prove which executable sent a request. Handing a capability to another holder hands over its scoped attribution authority. A guest holding a document capability must not obtain an unrestricted `SignDatumClaim` oracle or an actor proof it can exchange for broader authority. Datum claim signatures and attribution certificates are cryptographic even though handles are not.

### Grants

A standing grant is a durable entitlement to reacquire authority, not a kind of capability. Grant administration is itself protected by capabilities. The grant-owning domain interprets subjects, resources, access levels and containment; the generic table implements no global ACL. DrawerService owns local actor entitlements over drawers and creation rights. Grant checks at mint are batched SQL, not one authority call per result.

Not every actor needs a grant on every drawer. Only identities with an independent standing entitlement need rows. A plug may have a durable actor and key but no grants, and its runtime supplies a scoped capability per dispatch. Children do not inherit their parent's grants. A principal can delegate an existing capability without writing a grant at all.

Issuer-authorized grants survive restart. Delegations written through a temporary capability depend on it and disappear with it, including at restart.

Drawer membership, local actor grants and keyhive principal membership are separate facts. Local policy changes do not by themselves rotate remote keys or alter a keyhive roster; the drawer membership workflow owns those effects.

### Use-time behavior

A resource operation performs one capability resolution, not a failed domain lookup followed by an actor lookup and grant evaluation. Scope still needs domain interpretation: a drawer-scoped docs capability needs a current-membership lookup to decide whether a document is in the drawer. Composite operations (such as `MergeDoc`, which names a source and a target) must cover every resource they affect.

### Lifetime

| State | Lifetime |
| --- | --- |
| Actor and node identity, sealed keys, certificates | Durable |
| Issuer-authorized grants | Durable |
| Capability handles, namespace handles, issuance associations, signer bindings | Authority-instance local |
| Delegations that depend on a temporary capability | At most that capability's lifetime |

A restart creates fresh handles, and subsystems reacquire their authority. Old handles fail even when the same durable actor or namespace exists. Durable command ledgers store stable actors and operation intent, never a capability needed for replay.

## Proposed

These are not implemented.

1. **One narrowing verb for holders.** `Narrow { capability, rights?, scope? }` is routed to the capability's owning domain. It accepts any rights and scope the domain can prove are a subset, such as one document out of a drawer scope or Read out of Write. It replaces the separate "generic attenuation" and "domain mint from a source" in the public API; the mechanism underneath stays. This mirrors Zircon's `zx_handle_duplicate` (rights) together with fuchsia.io `Directory.Open` (scope).
2. **Plain vocabulary.** Identity proof becomes *identity*, domain capability becomes *capability*, standing grant becomes *grant*, and gate becomes *revocation dependency*. The mechanism terms stay inside this ADR's Mechanism section.
3. **Later, only if remote clients need it:** offline-narrowable tokens (macaroons, biscuit or UCAN) layered on `CapId`, so an iroh client can narrow without a round trip.

Considered and not adopted for now: making the docs service accept a drawer capability for that drawer's documents, or a mint that returns both. Most callers need only a docs capability; only admin callers (the CLI, settings) hold both. Revisit if those callers find two mints a burden.

## Implementation and evidence

This design removed three earlier mechanisms: per-call identity-or-object authorization with a fallback, actor-proof-authorized generic minting, and a central authority grant ledger. Since N2b the caller's identity travels in the `Authorization` header on every request. Resource operations still accept only a scoped capability in the body.

- Authority keeps handles and exact issuance associations in memory and validates the bounded dependency DAG atomically with publication and retraction (`src/big_cabinet/authority/`).
- `Grants` stores namespace-partitioned durable rows and tracks temporary support and revocation gates.
- DocsRepo and DrawerService resolve body capabilities as scoped handles (`capability.rs`, `resolve`); the identity-plus-grants path exists only at mint (`source`).
- Authority, grant-helper, docs, drawer and wire tests cover independent instances, restart and reacquisition, cross-domain retraction, stale grant proofs, withdrawal racing publication, signer non-escalation, quiet-stream cancellation, and delegated callers without grants.

There is no compatibility shim for the unreleased wire APIs. Durable identities, keys and issuer-authorized grants must not be discarded silently during storage migration. BigRepo and keyhive node authority sit below the cabinet and are unchanged.
