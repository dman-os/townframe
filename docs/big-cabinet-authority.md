# Big Cabinet Authority

This guide describes the **authority service**: actor identities, capabilities,
a generic grant ledger, and secret-backed signing. The model is specified in
[ADR 017](../docs/adrs/017-authority-actors-capabilities-and-grants.md). It is
not a document, drawer, branch, membership, or Daybook-migration guide; those
contracts are [ADR 014](../docs/adrs/014-cabinet-drawers-and-selective-mirroring.md)
and [ADR 015](../docs/adrs/015-documents-repo-and-facet-writes.md).

Authority is a `service_utils_rs` service. It is declared once as
`AuthorityProtocol` and every consumer reaches it through a
`Client<AuthorityProtocol>`, so the same hosted handler runs for an in-process
caller, another process on the node, and an XRPC caller. Nothing outside the
service can touch its state.

Every ocap is an opaque UUIDv4 handle, including `is actor` handles. There is no
actor-capability type: AuthorityActors remembers which exact handles it issued
for which actors in its own issuance index. “Actor capability” below is shorthand
for a handle recognized by that index, not a protocol variant. Names and ancestry
cannot forge an association; ordinary attenuation does not copy it.

## Booting

```rust
use big_cabinet::{Authority, boot};
use secrets_rs::SecretStore;
use service_utils_rs::StopToken;
use sqlx_utils_rs::SqlCtx;

pub async fn boot_authority(
    sql: SqlCtx,
) -> Result<(Authority, StopToken), Box<dyn std::error::Error>> {
    let secrets = SecretStore::boot().await?;
    Ok(boot(&secrets, sql).await?)
}
```

`Authority` carries the service, the `Host` that fronts it, the client its
consumers use, the operator root capability, and `node_issuer` — the key a
verifier must expect on this node's attribution certificates. The returned token
stops the host (joining in-flight calls) before the service. The root capability
is the same on every boot.

## Opening a subsystem

A child's path is its parent's path plus one segment; the operator's path is
empty. `OpenActor` creates the actor on first use and returns the same identity
every time after, so an embedder simply opens its subsystems at every boot:

```rust
use big_cabinet::authority::{Authority, OpenActor, Rights};
use service_utils_rs::RequestContext;

pub async fn start_runtime(
    authority: &Authority,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: authority.root,
                path: "rt".into(),
                rights: Rights::all(),
            },
        )
        .await?;
    // runtime.url is "db+actor://<node>/rt"; runtime.capability is the
    // runtime's own actor capability; runtime.certificate is the node-signed
    // attribution certificate dmeta stores.
    let _ = runtime;
    Ok(())
}
```

Only `rt`'s actor capability can open `rt/plugs`, and the child's rights stay
inside the parent capability's. Retract a child's capability and open it again
to rotate the handle; the actor's key and URL do not change.

## Issuing and recognising capabilities

A domain service holds a client and its own actor capability. When a caller
presents *its* actor capability, the service resolves it, asks its own ledger
namespace what that actor holds, decides containment, and mints an object
capability from its own actor capability:

```rust
use big_cabinet::authority::{CheckGrant, MintCapability, ResolveActor, Rights};

// inside DocumentsRepo, holding `authority: Client<AuthorityProtocol>` and its
// own actor capability `docs`:
let caller = authority.call(ctx.child(), ResolveActor { capability: presented }).await?;
let held = authority
    .call(ctx.child(), CheckGrant {
        via: docs,
        subject: caller.uuid,
        object: format!("write drawer {drawer_id}"),
    })
    .await?;
if !held.contains(Rights::USE) {
    return Err(/* unauthorized */);
}
// DocumentsRepo knows the doc is in the drawer; the authority never does.
let edit = authority
    .call(ctx.child(), MintCapability {
        issuer: docs,
        object: format!("edit doc {document_id}"),
        rights: Rights::USE,
    })
    .await?;
```

When the cap comes back, `InspectCapability` reports its object, rights, and
`issuer` namespace UUID; DocumentsRepo accepts it only if that UUID is its own.
The namespace UUID grants nothing by itself. Inspection does not disclose the
parent handle, which would give the caller broader authority.

A holder may `AttenuateCapability` to hand on fewer rights over the same object,
never a different object. Attenuating an `is actor` handle is also a generic table
operation: the result is live but is not an identity handle recognized by
AuthorityActors. Use `OpenActor` through the parent to request another recognized
handle with reduced rights.

## Granting

The issuer writes grants in its own namespace with its actor capability. To let a
user share what it gave them, the issuer mints the user an object capability with
`delegate`; grants written through it are bounded by it and vanish when it is
retracted:

```rust
use big_cabinet::authority::{GrantAccess, MintCapability, Rights};

let owner = authority
    .call(ctx.child(), MintCapability {
        issuer: docs,
        object: format!("write drawer {drawer_id}"),
        rights: Rights::USE | Rights::DELEGATE,
    })
    .await?;
// later, the user shares with a plug:
authority
    .call(ctx.child(), GrantAccess {
        via: owner.id,
        subject: plug.uuid,
        object: format!("write drawer {drawer_id}"),
        rights: Rights::USE,
    })
    .await?;
```

## Signing a facet claim

DocumentsRepo authorizes the write against the object capability it was shown,
then signs as the acting actor with that actor's own capability:

```rust
use big_cabinet::authority::SignFacetClaim;

let signed = authority
    .call(ctx.child(), SignFacetClaim { actor: plug_actor_cap, claim })
    .await?;
```

`claim` is a `big_cabinet_core::dmeta::FacetClaim` (v2: origin document, facet
key, chosen UUIDv7, **actor UUIDv7**, operation, native value CID,
predecessors). The service refuses unless the capability is that actor's own,
with `use`, and the claim names that actor. Core verification
(`SignedFacetClaim::verify`, `Dmeta::verify_facet`) never consults this service,
so a legitimate historical claim stays verifiable after its grants are revoked.

## Operations

| Operation | Needs | Establishes | Does not establish |
| --- | --- | --- | --- |
| `OpenActor` | parent actor cap, `mintChild` | The persistent child actor at `path` and a live actor cap for it. | Any grant. A new actor holds nothing until an issuer says so. |
| `ResolveActor` | actor cap, `use` | Which actor the presenter is. | That the actor's plug binary executed. |
| `MintCapability` | actor cap, `narrow` | A new object, issued by that actor. | That the object means anything — only the issuer knows. |
| `AttenuateCapability` | ocap, `narrow` | A copy of the same object and issuer with fewer rights. | AuthorityActors' exact-ID association. |
| `InspectCapability` | the id | That the cap is live, and its object, rights, and issuer. | Parent handles or actor identity. |
| `RetractCapability` | the cap or an ancestor | That it and its derivations are dead. | That historical signatures are invalid. |
| `GrantAccess` / `RevokeAccess` | `delegate` | A standing entitlement in the issuer's namespace, or its withdrawal. | Anything about documents. |
| `CheckGrant` | issuer's actor cap, `use` | The union of standing rights for this exact subject and object. | Containment, Keyhive access, roster membership, byte presence. |
| `SignFacetClaim` | actor cap, `use` | That this actor's key signed this exact v2 claim. | That the node may write the document, or that any drawer authorizes it. |

## Trust boundary

A cap id is an opaque UUIDv4 **bearer handle**, like a file descriptor. All three
transports carry the same handle to its owning authority. No signed token wrapper
is needed. Keep handles confidential, admit callers at the transport boundary,
and hand them only the handles they should exercise. A separate authority cannot
verify a handle without its owning table. Attribution certificates are unrelated
to this handle format; see ADR 017.

## Durability

The service's tables live in the `SqlCtx` it is booted on. Writes are single
`BEGIN IMMEDIATE` transactions on the writer pool; inspection, resolution, grant
checks, and signing read from the reader pool without waiting for writers.

Node and actor seeds are sealed under one wrapping key held in the OS keyring and
stored beside their registry rows, so creating an actor is a single transaction
with nothing to reconcile after a crash. Boot refuses to continue if the keyring
lost the wrapping key rather than issuing new identities.

There is no backward compatibility with the deleted v1 `CapabilityToken` wire
format or the earlier keyring-held actor keys.
