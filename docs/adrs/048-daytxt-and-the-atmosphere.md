# ADR 048: daytxt and the atmosphere: the border

**Status:** Draft (not proposed; for the operator's review). Written 2026-11-17 from the operator's direction that day on the PDS and spaces; nothing here is decided except where it says so.

**Related:**
- [ADR 026: drawer-native blobs](026-drawer-native-blobs.md)
- [ADR 037: holder-bound capabilities and channel identity](037-holder-bound-capabilities-and-channel-identity.md)
- [ADR 038: node orientations and the relay alpha](038-node-orientations-and-the-relay-alpha.md) (the relay)
- [ADR 043: client pairing and operator authentication](043-client-pairing-and-operator-authentication.md) (clients and identity)
- Ledger D48, `docs/scratch/nodule-runtime-ledger.md`

## Context

The goal (operator, 2026-11-17): daytxt relies on the atmosphere (atproto) for public, always-available experiences and infrastructure. The question is where the border between the hive (keyhive-governed, E2EE, drawers) and the atmosphere sits, and which data crosses it in which direction. It is not "local-first atproto".

Two directions cross it:
- **Outward projection.** Hive data is derived into an atproto repo so that atproto clients and a CDN can read it. Into a public repo: a Merkle search tree (MST), cacheable, readable by anyone. Into a space repo (atproto Spaces): credential-gated, an LtHash set commitment, deniable MACs instead of signatures.
- **Inward.** Other people's records are synced into drawers (entrain-style), and a PDS is a backup target for the user's own data.

## Decision (proposed)

### 1. Three tiers (the operator's open question)

| Tier | Where it lives | Who reads it |
|---|---|---|
| Hive only | drawers, keyhive-encrypted | members |
| Space projection | a space repo, behind the space authority | members, by credential, through atproto clients |
| Public projection | a public repo (MST) | anyone |

Whether these three are the right tiers, and whether the middle one is worth building before Spaces leave alpha, is **open**. The hive tier needs nothing from this ADR.

### 2. The relay's roles

The relay (ADR 038) is the border host. It plays up to three roles, each separable:
- **PDS / repo host.** To start, an off-the-shelf PDS (rsky-pds or atproto-crates; tranquil only if AGPL is acceptable) as a separate service, not code in the node. It holds the user's atproto signing key and accepts application writes, and drawers are what it projects from.
- **Space authority host.** An app gets read access by asking the space host for a short-lived credential (`authorizeSpaceSync`, at most 60 minutes). Under the managed policy the host asks an outside service (`checkAccess`) before issuing it; the relay answers from keyhive membership, so the hive stays the source of truth for who may read.
- **Syncer.** Inward direction: pulls records into drawers and pushes backups.

### 3. Authorship mapping

Spaces have no co-edited records: a record has one author. Hive datums can be co-edited (Automerge). So:
- a **shared** datum projects as its merged state into the space authority's repo, authored by the authority;
- a **single-author** record projects into that member's own repo.

Either way the projection is of the merged state at some heads, so co-editing stays inside the hive and never reaches atproto as concurrent writes.

### 4. Durable versus derived

Durable, in Automerge: records as atproto JSON (the datum value). Derived, rebuildable: the MST, CAR files, commits, LtHash state and oplogs. Migration between PDS hosts is therefore re-derivation, not a copy: the projection must be deterministic for the same heads (so two derivations agree on the repo's content), and an experiment must show it.

### 5. Key custody

On the relay (the operator's delegate for the border): the repo signing key, the `#atproto_kem` key, and the space authority key. With the user: the rotation keys, which can take the identity back from a misbehaving relay. Where the relay keeps its keys is the pending secret-store design (file/env-backed, for headless relays; needs the operator's sign-off, ledger D48). Until it exists, border projection stays an experiment.

### 6. Rejected: every node is a PDS

Considered and rejected (operator, 2026-11-17). Federated commits must come from one publisher, so a PDS on each node could not publish; it would only serve the user's own apps, which the always-on relay PDS already does, and stock atproto apps gain nothing from it. Local-first programming targets nodules and daytxt instead, and the PDS is where the atmosphere side begins. Local nodes instead relate to the PDS as OAuth clients (an authenticated app, like any other atproto client), and drawers stay the node's primitive.

### 7. Status of atproto Spaces

Alpha. The proposal is 0016 and it is not on atproto's main branch. The sync authorization method was renamed `authorizeSpaceSync` (2026-11-12), which shows the surface still moves. Anything built on it is an experiment behind a derivation boundary (section 4), so a change upstream costs a re-derivation, not a migration.

## Experiments

1. The spaces-alpha PDS plus its bulletin app, with a `checkAccess` backed by keyhive membership: does the hive answer fast enough, and does revocation in the hive close space access promptly?
2. Deterministic projection: derive a repo from a drawer at fixed heads on two machines and compare MST roots.

## Open

- The three tiers (section 1), including whether a space tier is needed at all.
- Whether an inward sync is one drawer per followed repo or one per record collection.
- Who holds the authority key when a hive has several relays (ADR 038).
- The secret-store design (section 5).
