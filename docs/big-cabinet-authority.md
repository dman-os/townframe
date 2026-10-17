# Big Cabinet Authority

This guide describes the **current qualified Authority primitives**. It is not a document, drawer, branch, membership, or Daybook-migration guide. The qualified Authority source is `src/big_cabinet/authority.rs` at SHA-256 `3339b8bfb2a301bee26ab7b265f8f4ec33bb016ebfc7b4b28ef926ba026218c4`. The retained runtime qualification is 30 Authority tests; Schema qualification is separate (13 tests). This document does not rerun those checks.

## What the Authority boundary does

`AuthorityRepo` is a durable, local capability registry. It owns:

- the node and operator signing identities;
- registered actor public identities;
- node-signed `CapabilityToken` values and their persisted ancestor chain;
- current revocation and current coarse drawer-operation authorization; and
- actor-authored `FacetClaim` signing after current capability authorization.

The public types are re-exported by [`big_cabinet::lib`](../src/big_cabinet/lib.rs#L1-L15): `AuthorityRepo`, `CapabilityToken`, `Grant`, `DrawerId`, `ActorClaim`, and the associated identity/error types.

`DrawerId` is the actual `big_sync_core::ObjKey`/Core `DocumentId`, not a fixed-width wrapper. Its identity is its byte string, so a real ID may be variable length. The v1 token wire format remains externally tagged JSON with drawer IDs as byte arrays; it must not be replaced with `ObjKey`'s human-readable string form. The custom `Grant` serializer and borrowed `UnsignedToken` are implemented in [`authority.rs`](../src/big_cabinet/authority.rs#L209-L410).

## Constructing an authority and minting a capability

Use the real platform `SecretStore` for a service process. `SecretStore::boot` opens the OS-backed keyring; it does not return private key material. `AuthorityRepo::boot` takes that store and a durable SQLite state path, then returns the stable operator root capability.

The following example uses only public APIs. `document_id_bytes` must come from the caller's actual BigRepo/Core object identity. The example does **not** create a document or drawer, and an arbitrary byte vector is not evidence of a real document workflow.

```rust
use std::{path::Path, sync::Arc};

use big_cabinet::{AuthorityRepo, DrawerId, Grant};
use secrets_rs::SecretStore;

pub async fn mint_for_existing_document(
    state_path: impl AsRef<Path>,
    document_id_bytes: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(SecretStore::boot().await?);
    let (authority, operator) =
        AuthorityRepo::boot(Arc::clone(&store), &state_path).await?;

    // create_actor creates and registers a new actor; only its public identity leaves
    // the AuthorityRepo boundary.
    let actor = authority.create_actor().await?;
    let operation = Grant::WriteDrawer(DrawerId::new(document_id_bytes));
    let token = authority
        .delegate(&operator, actor, vec![operation.clone()])
        .await?;

    // Portable cryptographic verification does not consult local revocation.
    token.verify(&authority.node_public_key())?;
    // Current verification checks the persisted row, revocation state, and ancestors.
    authority.verify(&token).await?;
    // Authorization checks this current token against this requested operation.
    authority.authorize(&token, &operation).await?;

    Ok(())
}
```

`delegate` is explicit and downward. The operator root may issue grants; a non-operator actor may only attenuate grants it owns and may not create a lateral sibling path. `Grant::Delegate { drawer, grants }` is the typed nested-delegation form when a caller needs to delegate a bounded set through a drawer. The Authority layer does not establish the corresponding BigRepo/Keyhive access or publish a drawer entry.

## Actor keys and signing handles

`create_actor` durably records a provisioning intent, creates a named Ed25519 key in `secrets_rs`, binds its public key to the actor registry, and returns only `[u8; 32]` (`ActorPublicKey`). The actor key name and private seed remain inside the secret store. On a later delegation or actor-claim signing operation, AuthorityRepo opens the persisted named key as a `SigningKeyHandle` and binds it to the registered public key before signing.

The standalone public signing-handle lifecycle is:

```rust
use std::sync::Arc;
use secrets_rs::{SecretStore, SigningKeyHandle};

pub async fn sign_without_exporting_a_key(
    store: Arc<SecretStore>,
    message: &[u8],
) -> Result<[u8; 64], secrets_rs::SecretsError> {
    let handle =
        SigningKeyHandle::get_or_create(store, "example.service", "example-key").await?;
    let public_key = handle.public_key().await?;
    handle
        .bind_public_key(public_key)
        .sign(message)
        .await
}
```

That handle example is an application-owned signing key; it is **not** registered as an AuthorityRepo actor. The current public Authority API intentionally provides no “register this arbitrary handle” operation: use `create_actor` when the key must participate in AuthorityRepo delegation. `SigningKeyHandle::open` is the non-creating form used for a persisted key during reopen/recovery. The handle API is documented in [`secrets_rs`](../src/secrets_rs/src/lib.rs#L490-L589), and its private seed is never returned.

## Actor facet claims: current authorization versus historical verification

`ActorClaim` pairs a `Grant::WriteDrawer` operation with a Core `FacetClaim`. `sign_actor_claim` first checks the token against **current** AuthorityRepo state, opens the registered actor signer, and returns a `SignedFacetClaim`; it does not return a private key. Core supplies the claim's real document ID, facet key, UUID, operation, value CID, and predecessor IDs. The claim encoding and `SignedFacetClaim` constructors are public in [`big_cabinet_core::dmeta`](../src/big_cabinet_core/dmeta.rs#L184-L399).

```rust
use big_cabinet::{ActorClaim, AuthorityRepo, Grant};
use big_cabinet_core::{DocumentId, FacetKey};
use big_cabinet_core::dmeta::{value_cid, ClaimOperation, FacetClaim};
use serde_json::json;
use uuid::Uuid;

pub async fn sign_and_verify_claim(
    authority: &AuthorityRepo,
    token: &big_cabinet::CapabilityToken,
    document_id_bytes: Vec<u8>,
    write_drawer: big_cabinet::DrawerId,
) -> Result<(), Box<dyn std::error::Error>> {
    let claim = ActorClaim {
        operation: Grant::WriteDrawer(write_drawer),
        claim: FacetClaim {
            origin_document: DocumentId::new(document_id_bytes),
            facet_key: FacetKey::from_text("org.example.note", "main")?,
            chosen_uuid: Uuid::new_v4(),
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!({ "body": "example" }))?),
            predecessors: Vec::new(),
        },
    };

    let signed = authority.sign_actor_claim(token, &claim).await?;
    authority.verify_actor_claim(token, &claim, &signed)?;
    Ok(())
}
```

`verify_actor_claim` verifies the historical actor signature, the node-signed token, and the exact actor/claim context. It deliberately does **not** require current capability authorization, so a legitimate historical claim remains verifiable after the capability is revoked. That historical result is not permission to perform a new write.

## Reopen, durability, and recovery boundaries

The `state_path` passed to `AuthorityRepo::boot` is the durable SQLite owner boundary. A second process or a reopened handle reads current token rows, digests, ancestor links, and revocations from that store rather than trusting a process-local cache. Reopening the same path returns the persisted operator root and can verify previously persisted capabilities:

```rust
let (reopened, _root) = AuthorityRepo::boot(store, &state_path).await?;
reopened.verify(&token).await?;
```

Boot also reconciles durable node/operator boot intents and unfinished actor-provisioning intents. A cancelled or interrupted creator is completed or reconciled by the next serialized owner when the exact key identity is available. `ProvisioningUnresolved` means the keyring/database outcome cannot be established safely; `CommitUnknown` means a commit outcome requires reconciliation. These are recovery signals, not permission to manufacture a new identity, delete a key, or silently treat an uncertain operation as successful. The recovery tests and implementation cover durable intent, restart, cancellation, and ambiguous-publication boundaries; they do not define the separate recovery-principal design in ADR 006.

The keyring and SQLite state are separate durable systems. AuthorityRepo preserves exact public-key bindings and reports unresolved cross-store outcomes rather than pretending they form one atomic transaction. The private seed remains in the OS keyring throughout boot, delegation, claim signing, and reopen.

The independent legacy qualification in [`authority.rs` tests](../src/big_cabinet/authority.rs#L2446-L2630) inserts a literal old full-token JSON row and its payload digest directly, then reopens, verifies, and authorizes it. This demonstrates lossless v1 Authority-token wire compatibility for the qualified variants; it is not a migration or re-signing procedure.

## Do not conflate these checks

| Check | Establishes | Does not establish |
| --- | --- | --- |
| `CapabilityToken::verify(node_key)` | The token's v1 payload and node signature match the supplied node identity. | Current revocation, persisted-row existence, ancestor validity, actor authorization, or Keyhive access. |
| `AuthorityRepo::verify(token)` | Current local persisted token row, digest, ancestor chain, revocation state, and node signature. | Document existence, drawer membership, physical bytes, or lower-layer node authority. |
| `AuthorityRepo::authorize(token, grant)` | This current local capability authorizes this coarse requested operation. | A BigRepo/Keyhive grant, official roster inclusion, or a successful document mutation. |
| `AuthorityRepo::sign_actor_claim(...)` | Current capability authorization plus an actor signature over the Core claim. | That the node can write the document, that the claim was applied, or that the actor is a Keyhive principal. |
| `AuthorityRepo::verify_actor_claim(...)` | Historical signature and exact token/claim subject context. | Current permission to issue another write or a current membership decision. |
| BigRepo/Keyhive checks | Lower-layer node authority and effective access. | Actor capability issuance, actor attribution, or this local AuthorityRepo policy. |

Official drawer roster inclusion, Keyhive access, and local byte presence are separate facts. The current Authority primitives do not create real documents, populate `DrawerRepo`/`DocumentsRepo`, establish target-drawer or branch workflows, admit membership, or prove current document history/native-content qualification. Branches remain separate physical `DocumentId` identities and are not implicitly admitted to a drawer, as described by [ADR 007](../docs/adrs/007-doc-branch-identity.md). The broader drawer and document contracts remain proposed in [ADR 014](../docs/adrs/014-cabinet-drawers-and-selective-mirroring.md) and [ADR 015](../docs/adrs/015-documents-repo-and-facet-writes.md).

The two product choices referenced by the surrounding task remain unanswered. This guide makes no choice about them and does not infer a policy from the Authority primitives.

## Source references

- [Authority public exports](../src/big_cabinet/lib.rs#L1-L15)
- [Authority grant/token wire and borrowed payload implementation](../src/big_cabinet/authority.rs#L209-L410)
- [Authority boot and actor lifecycle](../src/big_cabinet/authority.rs#L1335-L1565)
- [Current token verification, delegation, authorization, revocation, and claim APIs](../src/big_cabinet/authority.rs#L1849-L2180)
- [SecretStore and SigningKeyHandle public APIs](../src/secrets_rs/src/lib.rs#L116-L145) and [key handles](../src/secrets_rs/src/lib.rs#L490-L589)
- [Core claim and signed-claim types](../src/big_cabinet_core/dmeta.rs#L184-L399)
- [ADR 014: drawer/Keyhive boundary](../docs/adrs/014-cabinet-drawers-and-selective-mirroring.md)
- [ADR 015: document, node authorization, and signed-claim boundaries](../docs/adrs/015-documents-repo-and-facet-writes.md)
- [ADR 007: physical document and branch identity](../docs/adrs/007-doc-branch-identity.md)
