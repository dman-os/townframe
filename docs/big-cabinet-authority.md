# Big Cabinet Authority

This guide describes the **current AuthorityRepo primitives** — actor
identities, capabilities, a generic grant ledger, and secret-backed signing,
as specified in [ADR 017](../docs/adrs/017-authority-actors-capabilities-and-grants.md).
It is not a document, drawer, branch, membership, or Daybook-migration guide.
Drawer and document contracts are [ADR 014](../docs/adrs/014-cabinet-drawers-and-selective-mirroring.md)
and [ADR 015](../docs/adrs/015-documents-repo-and-facet-writes.md).

## The four parts

`AuthorityRepo` owns four parts that stay independent inside it:

- **Capability table.** A `CapId` names a row `{ object, rights, parent,
  root_actor }`. `object` is an opaque string named by the minting domain
  (`doc:<id>`, `drawer:<id>`, `branch-of:<id>`, `is subsystem <path>`);
  `rights` is a set over a generic vocabulary (`Use`, `Narrow`, `MintChild`,
  `Delegate`). Minting a child requires the parent to allow it and the child's
  rights to be a subset; whether one object is narrower than another is the
  minting domain's decision, made before the mint call. Caps are table rows,
  not signed tokens — the table is the check.
- **Actor registry.** An actor is a persistent subsystem: a `Uuid7`, an actor
  URL, its Ed25519 public key, and a node-signed attribution certificate
  binding them. The subsystem's cap is rooted at that actor.
- **Grant ledger.** Opaque rows of `(subject, object, access)`. The only query
  is exact match; containment (doc in drawer, child drawer) is resolved by the
  caller first. Deleting a row revokes. This is the only persistent
  authorization state.
- **Signer.** `sign_as(cap, claim)` walks the chain to the root actor and signs
  a dmeta v2 facet claim with that actor's key. A cap with no root actor cannot
  sign.

## Boot, subsystems, and minting

`AuthorityRepo::boot(store, path)` opens the durable SQLite state, reconciles
any unfinished actor provisioning, and returns the repo plus the operator root
cap. From it:

```rust
use std::{path::Path, sync::Arc};

use big_cabinet::AuthorityRepo;
use secrets_rs::SecretStore;

pub async fn boot_authority(
    state_path: impl AsRef<Path>,
) -> Result<(AuthorityRepo, big_cabinet::CapId), Box<dyn std::error::Error>> {
    let store = Arc::new(SecretStore::boot().await?);
    let (authority, root) = AuthorityRepo::boot(store, state_path).await?;
    Ok((authority, root))
}
```

A subsystem is created with `create_actor(&root, "<child/path>")`. The caller
supplies the child path; the full actor URL is `db+actor://<node>/<path>`, and
the registry rejects a path that does not extend the minting subsystem's own
path. The returned `Actor` carries the `Uuid7`, the URL, the public key, the
node-signed attribution certificate for the binding (the bytes dmeta stores as
`ActorMetadata.delegation`), and the subsystem cap.

Grants and caps then compose in the domain services, not here. A domain
service given an "is subsystem X" cap asks the registry who X is
(`actor_of`), asks the ledger whether that actor may act on an object the
service named, and mints its own cap parented to its own subsystem cap.
`AuthorityRepo` never learns what an object string means.

## Signing a facet claim

DocumentsRepo is the only facet-claim caller. It evaluates the presented cap
itself — object, rights, and the drawer membership that authorizes the claim's
document — then calls:

```rust
let signed = authority.sign_as(&edit_cap, claim).await?;
```

`claim` is a `big_cabinet_core::dmeta::FacetClaim` (v2: origin document, facet
key, chosen UUIDv7, **actor UUIDv7**, operation, native value CID,
predecessors). `sign_as` checks the claim's actor UUID equals the root actor's
UUID, and returns a `SignedFacetClaim` whose actor key is that actor's
`MultibaseKey` and whose signature is native `SignatureBytes`. Current
capability state is consulted; a legitimate historical claim remains verifiable
after revocation, because core verification (`SignedFacetClaim::verify`,
`Dmeta::verify_facet`) does not consult this repo at all.

## What a check establishes

| Check | Establishes | Does not establish |
| --- | --- | --- |
| `verify_issued(cap)` | This cap is a row this repo issued, unretracted, whose parent chain narrowed or held, rooted where it claims. | That the holder may do anything in particular — object meaning belongs to the minter. |
| Ledger `has_grant(s, o, a)` | This exact grant row currently exists. | Containment, Keyhive access, roster membership, or byte presence. |
| `sign_as(cap, claim)` | The root actor's key signed this exact v2 claim, and the claim names that actor's UUID. | That the node may write the document, or that any drawer authorizes it — DocumentsRepo checks that before calling. |
| BigRepo/Keyhive checks | Lower-layer node authority and effective access. | Actor capability issuance or attribution. |

## Durability

The private seed never leaves `secrets_rs`; the repo holds named signing
handles and signs through them. Boot reconciles durable node/operator and
actor-provisioning intents left by a cancelled or interrupted owner, completing
or reconciling them rather than manufacturing a new identity, deleting a key,
or silently treating an uncertain outcome as success. `ProvisioningUnresolved`
and `CommitUnknown` are those reconciliation signals. Keyring and SQLite are
separate durable systems; exact public-key bindings are preserved across the
boundary and unresolved cross-store outcomes are reported, not hidden.

There is no backward compatibility with the deleted v1 `CapabilityToken` wire
format: token and revocation rows are dropped at boot, and actor keys survive
because they live in the secret store independently of these tables.
