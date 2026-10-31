# ADR 026: Drawer-native blobs

**Status:** Proposed. The decisions marked *settled* were agreed with the operator
during design review. The ones marked *proposed* await confirmation before
implementation. Nothing here is implemented; the exploratory `big_blobs` code in the
tree predates this ADR and is paused.

This ADR supersedes parts of [ADR 001](001-blob-inventories.md),
[ADR 003](003-cipherblob.md) and [ADR 005](005-relay-sponsorship-retention-admission.md),
and amends [ADR 019](019-cabinet-drawers-and-selective-mirroring.md). Each of those
ADRs carries a note naming the sections replaced here. The discussion trail is
`docs/scratch/big-blobs-drawer-design.md`.

## Context

The legacy blob stack (`daybook_core::blobs`) predates drawers. It keys everything on
repo-wide Keyhive groups:

- a default inventory doc in a `blob_inventories` authority group (ADR 001 §3–§4);
- encryption domains named by Keyhive group (`grp:<group_id>`);
- JWKs in separate key documents granted to a decryptor group, in a keys drawer, with
  a staging, commit and registration sweep (ADR 003 §19);
- an account document naming one sponsorship group `S_docs` (ADR 005 §1).

Cabinet drawers (ADR 019) now give every collection a meta group (the descriptor and
roster) and a docs group (the content). This ADR re-bases blobs on that structure. It
keeps ADR 001's pin shape, ADR 003's representation format, salt derivation,
virtual provider and release path, and ADR 005's retention mechanics.

## Decision

### 1. Encryption exists so that relays can serve blobs (*settled*)

Encryption exists so that relays without plaintext access can serve blobs. Anyone with
plaintext access to a doc may hold its keys.

ADR 003 §19 moved the JWK out of the content doc so that "a retention domain can be
given the cipherBlob ... without ever being given the key". That assumes a party that
can **read** the content doc but must not decrypt. In the drawer model that party does
not exist:

- relays hold content docs at Keyhive **Relay** (ciphertext only), and learn
  representation digests from inventories (§5), never from the cipherBlob;
- anyone with Read on the doc already sees the Blob datum and is entitled to the
  plaintext.

The separation protected nobody and cost a key doc per domain, a keys drawer, and the
§19 staging, commit and registration sweep. All of that is dropped.

### 2. The JWK lives next to the cipherBlob (*settled*)

- The JWK datum lives in the same doc as the cipherBlob that uses it. ADR 003 §5–§6's
  co-location is restored.
- `keyRef` stays a general URI, so a separate key store can return later without
  changing the cipherBlob abstraction. Co-location is the default, not a schema
  restriction. An in-doc `keyRef` is accepted, which reverses ADR 003 §19's refusal of
  `self`.
- A blob may have several cipherBlobs. A branch whose author cannot read the origin's
  cipherBlob or JWK mints its own (§4).

### 3. The domain is the doc (*settled*), and slots are keyed by blob (*proposed*)

ADR 003 §7 introduced domains to keep identical plaintext in different audiences from
producing correlatable ciphertext. The default domain is the **document**:

- a doc in drawers X and Y has one `DocumentId` in both docs groups. A relay holding
  both already links them by doc id, so separate ciphertext per drawer hides nothing;
- with the JWK co-located, every reader of the doc reads every slot's key, so
  per-drawer keys would not narrow decryption either;
- identical plaintext in **different docs** (branches and copies) gets separate slots
  because those are different docs. That meets ADR 003 §7's motivation.

*Proposed* slot keying: `org.example.daybook.cipherBlob/<blob datum key>`, with its key
at `org.example.daybook.jwk/<same key>`, plus a discriminator if a doc ever needs
two representations of one blob.

- One JWK per blob slot means no plaintext equality is visible even inside a doc, and
  rotation is per blob. ADR 003 §19's `keyScope` disappears.
- A reader finds the cipherBlob for a Blob by naming convention inside the same doc.
  The encryptor **no longer writes `Blob.urls`**. ADR 003 §19 step 6 is dropped, and a
  `?via=` URL stays legal as an optional hint, not as the resolution mechanism.
- ADR 003 §19's `grp:<group_id>` domain naming is withdrawn.

### 4. A doc you cannot write is branched (*settled*)

To sponsor or encrypt a doc you cannot write, you branch it and work on the branch.

- A companion doc that only carries cipherBlobs is rejected. Read-only is a general
  blocker, since you will eventually want other edits too, and a branch handles all of
  them.
- Adopting someone else's public doc into your sponsored set is the same act.
- The branch is an ordinary writable doc (`BranchDoc`, ADR 007), so encryption is
  in-doc (§3) and resolution needs nothing extra.
- If the origin's owner already encrypted it, the origin can be sponsored without a
  branch: its in-doc slots are readable, so its pins can be written.
- The relay stores a branch as its own sedimentree, so an origin and a branch that are
  both sponsored are retained twice. That is accepted, and counted in quotas.
- *Proposed, optional:* the branch datum records why it exists, as an open string
  vocabulary (for example `forkReason: readOnlyOrigin`), so tools can show "your
  writable copy of X". Branching works without it.

### 5. Inventories are drawer meta docs (*settled* shape, *proposed* details)

The drawer is the sync and collection domain, but a relay still needs a readable list
of representation digests. Inventories stay, under that name.

- **An inventory is always a separate doc.** The drawer's metadata doc carries only a
  reference to it, so pin churn never grows the metadata doc's history, which every
  participant and relay syncs. This follows ADR 019's roster-shard pattern: an extra
  metadata-class doc that the meta group can read, and that counts for the drawer only
  through an entry on the metadata doc.
- **The reference is a fixed slot per kind:** `org.example.daybook.drawer.metaDoc/<kind>
  { docId }`; for blobs, `metaDoc/blobInventory`. Concurrent creators write the same
  datum, and Automerge last-writer-wins leaves one value. `kind` is an open string
  vocabulary; Cabinet stores and projects it without interpreting it, and
  `big_blobs_core` owns `blobInventory`. The slot is in the `drawer.*` namespace, so
  only DrawerService writes it.
- **The inventory doc carries** `org.example.daybook.blobPin/<C> { lengthOctets }`, with
  ADR 001 §2's shape and strict omissions, and optional config
  `org.example.daybook.blobs.inventory/main { extra }`. `big_blobs_core` owns both
  datums.
- **Creation is a DrawerService op** (name to be fixed, for example
  `CreateMetaDoc { drawerId, kind, datums }`), because only DrawerService can make a doc
  metadata-class and then reference it. The order is delegation before entry, as for
  `docEntry`:
  1. create the doc;
  2. `grant_doc_access(doc, metaGroup, Admin)`, as `create_collection` does for the
     metadata doc;
  3. write the slot.

  It requires drawer Write, because writers maintain inventories. It refuses self-doc
  drawers (§6). It is ledgered like `create_collection`. The op does not project
  anything: the drawer projection picks the slot up from the change stream
  ([ADR 024](024-indexes-are-fed-by-the-change-stream.md)).
- **Keyhive access comes from the meta group.** Relay-level principals get Read on the
  meta group when invited or linked as a child (`meta_access`), so relays read pins.
  Write participants get Edit, so they write pins.
- **Cabinet reach** comes from a projected `big_cabinet_drawer_meta_docs (drawer_id,
  kind, doc_id)` row (primary key `(drawer_id, kind)`, indexed by `doc_id`) and a
  matching `ancestors.sql` branch at the Admin bound the metadata doc itself gets. The
  projection admits the row only when Keyhive shows the doc delegated to the drawer's
  meta group. Otherwise any drawer writer could name an arbitrary doc and gain Cabinet
  reach to it.
- **An inventory is not a `docEntry`.** `docEntry`s go into the docs group, which a
  Relay-level principal holds only as ciphertext, so it could not read the pins.
  Inventories would also show up as drawer content.
- **An inventory pins the representations of its drawer's own docs only:** its
  `docEntry`s and its self-doc children. Single-group children pin theirs in their own
  inventories, and a relay unions them while walking (§8).

**Concurrent creation** (*proposed*). Every datum key gets an alias in
`Dmeta.datum_uuids`. A writer reuses the smallest alias it can see and mints one only
when it sees none (`doc/pipeline.rs` ~167). Two writers that each create
`metaDoc/blobInventory` without seeing each other leave **two aliases** on the key after
the merge: durable, signed evidence of the race. The slot's merged value decides the
winner, and the projection admits only that doc, so the loser never gains Cabinet
reach. Cleanup:

1. every writer's pin worker, on seeing the slot's value change, re-adds its whole
   observed pin set to the winner, as adds only;
2. the loser's creator, which knows the doc from its op ledger row, revokes the meta
   group's grant on the loser, dropping it from the drawer's Keyhive closure;
3. other peers take no destructive action.

Between the merge and the re-seed, a relay may see the winner without the loser's pins.
Release is reactive (ADR 003 §19), and a missing pin is not an observed removal, so
nothing held is deleted. Two aliases mean a race only for write-once slots like this
one. A re-creation after removal reuses the visible alias, and keys rewritten all the
time (`docEntry/*`) collect aliases under ordinary concurrent writes.

**Laziness trigger** (*proposed*). A `blobInventory` slot on the metadata doc is what
makes the drawer's writers maintain cipherBlobs and pins for its Read+ entries.
Sponsoring a drawer you can write creates the inventory if none exists. Sponsoring a
drawer you can only read relies on an existing one. Without it, the relay keeps the
docs but not their blobs, and that is surfaced as a status.

**Pin maintenance is delta-driven** (*proposed*). Several writers maintain one
inventory, and a writer whose view lags must not delete pins for facets it has not
seen. A pin is removed only on an observed cipherBlob removal or an observed removal
of the doc from the drawer. Identical concurrent adds merge, because pins are keyed by
representation digest. Relay-level entries, docs we cannot read, contribute no pins.

### 6. Self-doc drawers: children allowed, never in a meta group, no inventory (*settled*)

- A self-doc drawer may be a child drawer, but it is never delegated to any meta group.
  Its descriptor is its content doc. The meta group is readable by Relay-level
  participants (§8), and a self-doc doc holds content, not just drawer metadata.
- Linking a self-doc child at `access` delegates the parent's **docs group** on the
  child doc at `access`, the same Keyhive shape as a `docEntry`. There is no meta-group
  delegation and no `meta_access` widening of Relay to Read. Today `AddChild` refuses a
  self-doc child with `SelfDoc` (`child_delegations` → `groups(child)`); that changes.
- A self-doc drawer has no meta group, so `CreateMetaDoc` refuses it and it has no
  inventory. The containing drawer's pin worker treats it as one of the parent's own
  docs: it maintains the cipherBlob and JWK slots inside that doc and pins them in the
  **parent's** inventory. §4 still applies: a writer who cannot write the doc branches
  it.
- *Proposed:* a relay that sponsors a lone self-doc drawer carries the doc but no blobs.
  To mirror its blobs, put it in a single-group drawer.
- The amendments to ADR 019 state the roster side: the child entry stays `{ access }`,
  with no layout stated and none derived. A child always names a drawer, whose shape is
  a fact of its own descriptor; the projection only checks that the child's Keyhive
  delegations match that descriptor.

### 7. Blob sync partitions are per drawer (*settled*)

The second job of ADR 001 inventories, minting a Keyhive group to hold Big Sync blob
partitions, moves to the drawer's groups. Each drawer with an inventory has its own
blob partitions:

- the **ciphertext** part's access is Keyhive Read on the drawer's inventory doc, which
  means the meta group's members, relays included;
- the **plaintext** part's access is Keyhive Read on the drawer's docs group, whose
  readers are entitled to plaintext and sync it between themselves, derived from the
  drawer's Blob datums.

Both are seeded from live Keyhive state before following the access-delta stream
(ADR 013). Group parts and `WorkerGroupScope` are not the gate for which docs a worker
reads; that is §9's predicate.

### 8. Sponsorship and relay visibility

- *Settled:* **sponsorship is a private relationship between an account and its
  relay.** Each account names **one root sponsorship drawer** to its relay, replacing
  ADR 005's `S_docs` group, and grows its sponsored set by adding child entries. Two
  collaborators may sponsor the same drawer to different relays without coordinating.
- *Settled:* **sponsorship is not private in effect.** Once a drawer is on a relay,
  other full-read participants may fetch from it. Keys travel inside the docs, so they
  can decrypt.
- *Proposed:* **the relay walk:**
  1. start at the root sponsorship drawer's metadata;
  2. follow child meta groups, which it holds at Read;
  3. in each drawer, read the metadata doc: descriptor, roster and `metaDoc` slots;
  4. read the referenced `blobInventory` docs through the same meta-group Read;
  5. adopt the docs closure with ADR 005's adoption worker, made transitive over child
     docs groups;
  6. retain the union of inventory pins.

  Because the relay reads child metadata, it can tie each inventory to its drawer, and
  dedupe across accounts that sponsor the same drawer, by drawer id and by
  representation digest.
- *Proposed:* **publicness contract.** Anything granted Read to a drawer's meta group
  (descriptor, roster, inventories) is visible to every participant, including
  Relay-level ones. No private datums (names, descriptions, icons, notes) go on a
  single-group drawer's metadata doc; private drawer metadata belongs in a `docEntry`. A
  self-doc drawer has no separate metadata. Whether it is linked as a child or listed
  as an entry, it is reached only through the parent's docs group, so its content stays
  ciphertext to Relay-level participants. Only the parent's roster entry is metadata.
- *Proposed:* only sponsored drawers are visible to a relay.

### 9. Workers observe through `PollDatums`

big_blobs workers read member docs through
[ADR 025](025-cabinet-datum-predicates-and-polling.md)'s `PollDatums`, from a cursor
they persist. For example, the per-drawer pin worker uses the predicate
`And[InDrawer { X, transitive: false, atLeast: Read }, HasTag(blob)]` with the datum
selection `{blob, cipherBlob, jwk}`. Roster changes arrive on the same cursor, so a doc
joining or leaving X is an `enter` or `leave` event for the worker. Every big_blobs
index follows ADR 024: it is written only from what the worker observes.

### 10. Concurrent encryptors (*settled*)

The salt is derived from (JWK secret, plaintext digest, framing), so once a slot's JWK
exists every device produces byte-identical ciphertext. The only race is two devices
minting the first JWK for one slot concurrently.

- There is no competing set and no manual resolution. Automerge last-writer-wins gives
  every replica the same winner.
- Pins derive from the merged cipherBlob, so the loser's pin leaves the inventory, its
  pair is released by the normal reactive path and the `PairRoots` drain, and a relay
  collects the loser's uploaded ciphertext after its grace period.
- A late concurrent write from an offline device can flip the winner. The cost is a
  re-upload, which is accepted.
- The JWK and the cipherBlob are separate datum keys, so their winners may come from
  different devices. `keyRefHeads` keeps that safe: the cipherBlob resolves its key at
  the heads it pinned. That is an invariant to test.

## What stays from the superseded ADRs

- ADR 001: multihash digests (§1); the pin datum's shape and strict omissions (§2);
  the transfer and GC semantics (§7); the separation of logical identity, keys and
  transport.
- ADR 003: everything except §19's key documents, `keyScope`, domain naming,
  ordering steps 2–4 and 6, and "one worker per group". That includes the RFC 8188
  format, derived salt, reproducible generation, single-pass creation, virtual
  provider, `PairRoots` ledger, deliberate release path, rotation, length hiding and
  random access. The remaining §19 ordering is: §11 pass and pair registration, then
  the JWK and cipherBlob write into the content doc (one Automerge transaction, which
  is now the commit point), then the pin.
- ADR 005: onboarding, abuse limits, cursor acks, size accounting and the rejection
  taxonomy. Only the sponsorship root (§1) and the adoption walk (§3) change.

## Open and deferred

- **Public drawers** may want inventories that also pin plaintext representations,
  so relays can serve public blobs unencrypted. ADR 001 pins allow it by shape.
  Deferred, along with ADR 001's inline pins in public content docs.
- **Shadow mirrors:** give a relay only inventory docs and group ids for a drawer,
  without its full metadata. Recorded, not designed.
- **Preferring the candidate the relay already holds** when concurrent encryptors
  race. That needs relay retention knowledge on the writer, for example ADR 005 cursor
  acks.
- **Keyline-native rosters** would remove the Automerge roster from relay-visible
  metadata, but not the metadata doc or the inventories. Nothing here depends on the
  roster choice.
- The retention ledger, single store owner and node lifecycle are specified in the
  replacement plan (`docs/scratch/big-blobs-replacement-plan.md` §4.5–§4.6) and become
  part of this ADR once it is accepted.

## Consequences

- Positive: no key docs, keys drawer, staging sweep or `blob_inventories` group. Blob
  access follows drawer access, and relays need only the meta-group Read they already
  get.
- Positive: read-only docs, branches and multiple audiences all reduce to "one doc, its
  own slots".
- Cost: Cabinet changes first: meta docs and their projection, self-doc children, the
  frontier-fed projection (ADR 024), and `PollDatums` (ADR 025).
- Cost: anyone who can read a doc can read its keys. That is the stated ground rule,
  not a leak.
