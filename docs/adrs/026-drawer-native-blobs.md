# ADR 026: Drawer-native blobs

**Status:** Accepted design; implementation is in progress. Sections below preserve the reasoning trail, while the 2026-11 drawer-metadata and mirroring amendment immediately below is authoritative where older single-inventory or per-drawer-part text conflicts.

This ADR supersedes parts of [ADR 001](001-blob-inventories.md),
[ADR 003](003-cipherblob.md) and [ADR 005](005-relay-sponsorship-retention-admission.md),
and amends [ADR 019](019-cabinet-drawers-and-selective-mirroring.md). Each of those
ADRs carries a note naming the sections replaced here. The discussion trail is
`docs/scratch/big-blobs-drawer-design.md`.


## 2026-11 amendment: named inventories, opaque bytes, and two metadata audiences

A grouped drawer has public metadata, docs, and sensitive metadata groups. Its descriptor-bearing doc is the public metadata root; it also names a separate sensitive metadata root. Relay paths receive Read on public metadata, Relay on docs, and no sensitive-metadata membership. Read+ paths receive the corresponding level on all three. Child delegation applies that table transitively.

Inventories are named meta docs, referenced by `drawer.metaDoc/blobInventory/<name>` slots. Public-root inventories are visible to Relay-level participants; sensitive-root inventories are not. Inventories and their transport objects are representation-agnostic: an inventory is only `blobPin/<digest> { lengthOctets }`, and a serving peer handles opaque digest-addressed bytes without deciding whether they are plaintext or ciphertext. Cabinet records root, kind, name, doc id, and backing delegation; it carries no blob-specific `representation` field.

Writer and mirror policy supply representation intent. By convention a public inventory named `ciphertext` receives encrypted-representation digests and a sensitive inventory named `plaintext` receives plaintext digests. The root determines the audience, not the byte format. An opaque relay stores and serves exactly what its selected public inventory names. A Read+ node mirroring ciphertext may decrypt into one locally stored plaintext copy and virtually reproduce the advertised ciphertext digest; that local provenance and restart state are not inventory metadata.

Explicit authorized public sharing of an inventory is intentional disclosure, including when sensitive and public references name the same document. The aliases share one pin set and effective Keyhive audience, not isolated contents or history. Cross-root aliases are permitted and retain independent writer claims; they do not establish an inventory-wide representation restriction. Public inventories may intentionally contain unencrypted or publicly decryptable blobs. Sensitive-only non-disclosure guarantees do not undo an explicit public grant.

Every blob part is keyed by the inventory document: `inventory_part(inventory_doc)`. The earlier drawer-keyed plaintext part and docs-group part seeding are withdrawn. Writer targets and local mirror selections are separate. Mirror state is durable per `(inventory_id, digest)` so pin-first and object-first arrivals, restart, deselection, and identical digests in different inventories cannot cross-unlock or lose work.

Mirror selection starts at a root drawer, selects an inventory name, and inherits through the official child-entry closure unless locally overridden. Each inventory still pins only its directly containing drawer's docs and self-doc children; only discovery is transitive. Success means actual bytes for root, child, and grandchild inventory pins arrive from a root-only grant. Missing metadata, inventory, authority, pins, or bytes is incomplete rather than successful empty coverage.

Self-doc drawers have no groups, metadata documents, companion inventories, or inventory pins of their own. Each immediate containing grouped drawer inventories its self-doc children's blobs, independently of other parents, and never grants either parent metadata group access to the self-doc. The standalone companion proposal is withdrawn; parentless self-doc inventory mirroring is outside this scope.

The current byte transport's virtual provider receives only a hash, not an authenticated requester or inventory. Current plaintext protection therefore consists of Keyhive-gated sensitive-inventory discovery plus strict non-disclosure of sensitive inventory ids, pins, plaintext digests, part membership, and availability to Relay participants. The final hash request is not a second authorization check; this security-through-obscurity boundary is explicitly accepted for this phase. Requester-aware exact-inventory byte authorization is deferred defense-in-depth, not a blocker for plaintext inventories.

The implementation and verification sequence is normative in `docs/scratch/big-blobs-drawer-design.md` §17. In particular, the real two-node A→B→C byte test, both arrival orderings, restart recovery, and sensitive-metadata non-disclosure are acceptance requirements; a local projection/reach test is insufficient.

## Context

The legacy blob stack (`daybook_core::blobs`) predates drawers. It keys everything on
repo-wide Keyhive groups:

- a default inventory doc in a `blob_inventories` authority group (ADR 001 §3–§4);
- encryption domains named by Keyhive group (`grp:<group_id>`);
- JWKs in separate key documents granted to a decryptor group, in a keys drawer, with
  a staging, commit and registration sweep (ADR 003 §19);
- an account document naming one sponsorship group `S_docs` (ADR 005 §1).

Cabinet grouped drawers (ADR 019) have public metadata, docs, and sensitive metadata
groups, with public and sensitive metadata roots. This ADR re-bases blobs on that
structure. It keeps ADR 001's pin shape, ADR 003's representation format, salt
derivation, virtual provider and release path, and ADR 005's retention mechanics.

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
- The branch is an ordinary writable doc (`BranchDoc`, ADR 034), so encryption is
  in-doc (§3) and resolution needs nothing extra.
- If the origin's owner already encrypted it, the origin can be sponsored without a
  branch: its in-doc slots are readable, so its pins can be written.
- The relay stores a branch as its own sedimentree, so an origin and a branch that are
  both sponsored are retained twice. That is accepted, and counted in quotas.
- *Proposed, optional:* the branch datum records why it exists, as an open string
  vocabulary (for example `forkReason: readOnlyOrigin`), so tools can show "your
  writable copy of X". Branching works without it.

### 5. Inventories are named drawer meta docs

The drawer is the sync and collection domain. Named inventories provide digest lists
for independently selected audiences; Cabinet's Phase A metadata machinery is generic.
Blob distribution, mirror state, and transitive discovery have separate Phase B–D gates
in [the design log](../scratch/big-blobs-drawer-design.md) §17.

- **An inventory is always a separate doc.** A metadata root carries only its
  reference, so pin churn does not grow that root's history. A grouped drawer has
  public metadata, docs, and sensitive metadata groups, with the descriptor-bearing
  public root and a separate sensitive root. The sensitive root's existence and id are
  public discovery facts; its contents are not.
- **References are fixed kind-and-name slots on the selected root:**
  `org.example.daybook.drawer.metaDoc/<kind>/<name> { docId }`; for blobs,
  `metaDoc/blobInventory/<name>`. Concurrent creators of one `(root, kind, name)`
  write the same datum, and Automerge last-writer-wins leaves one value. `kind` and
  `name` are open string vocabularies; Cabinet stores and projects them without
  interpreting blob semantics. `big_blobs_core` owns `blobInventory`; DrawerService
  alone writes references in the `drawer.*` namespace.
- **The inventory doc carries** `org.example.daybook.blobPin/<digest> { lengthOctets }`,
  with ADR 001 §2's shape and strict omissions. Optional config
  `org.example.daybook.blobs.inventory/main { extra }` is opaque to Cabinet and reserved
  for representation-independent policy. It is not required for discovery or admission
  and does not declare plaintext versus ciphertext. `big_blobs_core` owns both datums.
  There is no `MetaRepresentation` on `DrawerMetaDoc`, no inventory-local representation
  assertion, and no Cabinet representation/root validation. Writer and mirror policy
  decide which digests to place in each inventory; the root determines audience only.
- **Creation is a ledgered `CreateMetaDoc` operation** requiring drawer Write+ and
  selecting root, kind, and name. It creates the target, grants that root's group Admin
  on it, then publishes the reference on that same root. Delegation precedes publication.
  Self-doc drawers have no metadata documents (§6). Per
  [ADR 024](024-indexes-are-fed-by-the-change-stream.md), the projection observes the
  reference through the change stream rather than an imperative projection write.
- **Keyhive access comes from the selected root's group.** Relay paths receive Read
  on public metadata, Relay on docs, and no sensitive-group membership. Read+ paths
  receive the corresponding level on all three groups, transitively through child links.
  Conventional public `ciphertext` and sensitive `plaintext` names aid selection;
  neither the spelling nor the root is a byte-format declaration.
- **Cabinet reach** uses `big_cabinet_drawer_meta_docs` rows recording drawer, root,
  kind, name, and target doc id, keyed by `(drawer_id, root, kind, name)`. Projection
  admits a reference only from its canonical root and only with Admin backing
  delegation from that root's group. Otherwise a writer could name an arbitrary doc
  and gain Cabinet reach to it. Inventory access never grants drawer authority.
- **Neither inventories nor metadata roots are `docEntry`s.** There is no
  inventory-to-docs-group delegation; `scoped_docs` remains roster/child based.
- **An inventory pins only its containing drawer's docs and self-doc children.**
  Grouped children have their own inventories. Writer targets include every inventory
  of the directly containing drawer the writer is authorized to maintain, independently
  of local mirror selection. Only mirror discovery is transitive (§8).

**Concurrent creation** (planned cleanup). Every datum key gets an alias in
`Dmeta.datum_uuids`. A writer reuses the smallest alias it can see and mints one only
when it sees none. Two concurrent creators of the same
`metaDoc/blobInventory/<name>` slot on the same root leave two aliases after merge.
The merged value decides the winner; only that target is projected. Cleanup:

1. every writer's pin worker re-adds its observed pin set to the winner, as adds only;
2. the loser's creator uses its ledger row to revoke the selected root group's grant
   on the loser;
3. other peers take no destructive action.

Between merge and re-seed, missing pins are incomplete coverage, not observed removals
or permission to delete retained bytes. Alias multiplicity is evidence of concurrent
creation only for a write-once slot, not for routinely rewritten roster keys.

**Laziness and pin maintenance.** Inventories are independently lazy-created. A writer
may create a missing target; a read-only sponsor relies on an existing one. A missing,
denied, or unavailable selected inventory is pending/incomplete, never successful empty
blob coverage. Writer policy chooses the representation to maintain for each target.
Several writers may maintain one inventory; a lagging writer must not delete pins for
state it has not observed. Only authoritative observed removals drive pin removal.
Identical adds merge by digest; unreadable content contributes no newly derived pins.

### 6. Self-doc drawers use immediate parents' inventories

- A self-doc child is never delegated to either parent metadata group. Its descriptor
  is its content doc, not a metadata-only root. Linking it at `access` delegates only
  the parent's **docs group** on that doc, at `access`, with no `meta_access` widening
  of Relay to Read.
- Each immediate containing grouped drawer's writer targets cover the self-doc's blobs.
  CipherBlob and JWK slots remain in the child content doc; pins go into every immediate
  parent's named inventories with independent claims. §4 still applies when the writer
  cannot write the doc.
- A self-doc drawer has no groups, metadata documents, companion inventories, or pins of
  its own. `CreateMetaDoc` refuses it. Readable content already names its blobs; no
  reverse metadata discovery or paired companion-ACL operation is introduced.
- The former standalone companion proposal is withdrawn. Inventory-based mirroring of
  a parentless self-doc is outside scope and cannot report successful empty coverage.
  Read+ on a naming inventory remains the universal metadata access requirement; the
  final stock hash fetch has the accepted weaker boundary in §7.
- The child entry stays `{ access }`, with no duplicated or derived layout. A child
  always names a drawer whose shape is a fact of its own descriptor; projection checks
  that its Keyhive delegations match that descriptor.

### 7. Blob sync partitions are per inventory

`inventory_part(inventory_doc)` is the only blob-part mapping for all inventories.
The drawer-keyed `plaintext_part(drawer)` and docs-group part seeding are withdrawn;
removing those old implementation paths is Phase B, not a second accepted mapping.
Seed each known writer-target or mirrored inventory part from live Keyhive standing on
that exact inventory document before following ADR 013's access-delta stream, including
inventories with no current pins. Read+ means Read, Write/Edit, or Admin on that doc,
not Read on content docs and not a portable `big_blobs/read` capability. A drawer-level
Relay normally has Read on a public inventory through public metadata membership.

Writer targets and mirror selections are separate durable relations. Phase C records
selection, pin observation, object observation/admission, bytes, and completion per
`(inventory_id, digest)`. Pin-first parks a want; object-first quarantines a candidate.
A mirror fetch requires selection, a pin, and object observation in the same inventory's
part. A writer-target claim alone cannot trigger a fetch, and an identical digest in
another inventory cannot unlock the pair. Restart and late completion recheck current
claims and authority before announcement; missing or denied reads are not removals.

Transport handles opaque digest-addressed bytes. Big Sync can authorize inventory/part
metadata, but the stock iroh-blobs virtual provider is `reader_for(hash)`: it has no
requester, part, or inventory identity and cannot recheck exact-inventory authority.
The accepted interim plaintext boundary is Keyhive-gated sensitive-inventory discovery
and strict non-disclosure of sensitive inventory ids, pins, plaintext digests, part
membership, and availability to Relay participants. The final hash GET has no second
authorization check. This security-through-obscurity boundary is explicitly accepted;
requester-aware exact-inventory byte authorization is optional Phase F hardening, not
a plaintext-inventory blocker. Group parts and `WorkerGroupScope` do not scope content
observation; that is §9's predicate.

### 8. Sponsorship, mirror discovery, and relay visibility

- **Sponsorship is a private relationship between an account and its relay.** Each
  account names one root sponsorship drawer, replacing ADR 005's `S_docs` group, and
  grows its sponsored set through child entries. Collaborators may sponsor the same
  drawer to different relays independently. Sponsorship product workflows remain
  planned; Phase A metadata alone does not implement them.
- **Sponsorship is not private in effect.** Other authorized readers may fetch retained
  representations from a relay. Keys remain inside content docs; an opaque relay need
  not interpret the bytes it stores or serves.
- **The Phase D mirror walk** selects an inventory name at the root and inherits that
  selection through the official child-entry closure, with explicit local per-drawer
  overrides. A Relay path reads descriptor, roster, and named public-root references
  through each child's public metadata group; it gets no sensitive-group route. Read+
  paths may discover both roots. Discovery is durable and cycle-safe, reacts to child
  links, slot replacement, and authority changes, and does not mirror unrelated readable
  inventories. Each selected inventory pins only its own drawer's docs and self-doc
  children; adoption of the content-doc closure still follows the docs-group paths.
  Missing metadata, selected inventory, authority, pins, or bytes is incomplete coverage.
- **Coverage is the union of selected inventory pins, not a local reach query.** The
  gate requires two real nodes, owner A→B→C, only A granted to the mirror, and actual
  root/child/grandchild ciphertext bytes arriving without direct child invitations.
  Authorized plaintext coverage, both arrival orderings, restart, dynamic closure,
  self-doc child coverage, and sensitive non-disclosure have their own §17 gates; none is
  implied by Phase A. Byte storage may deduplicate by digest while admission and claims
  stay per `(inventory_id, digest)`.
- **Publicness is per root.** Public descriptor, roster, child architecture, and public
  meta docs are visible to Relay-level participants. Private names, notes, plaintext
  pins, and other sensitive state belong on sensitive meta docs, not the public root.
  Neither metadata root nor an inventory grants content-doc access. A self-doc child is
  reached through the parent's docs group only; its content stays ciphertext to Relay.
- **Successor resolution is discovery only.**
  `org.example.daybook.drawer.successor/main { drawerId }` lives on the old
  descriptor-bearing doc: public root for a grouped drawer, content doc for a self-doc.
  Explicit resolution follows a cycle-checked chain and returns the new drawer id. It
  grants no authority, copies no roster or children, moves no blobs, and never silently
  retargets mirror selection; successor access is established independently.

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

- **Public-drawer plaintext serving policy and inline pins in public content docs**
  remain deferred. This does not defer Read+ plaintext inventories under sensitive
  roots or impose a representation field on the generic pin format.
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

- Positive: no key docs, keys drawer, staging sweep or `blob_inventories` group.
  Inventory discovery follows the selected metadata root's audience; relays get public
  metadata Read and no sensitive membership. Final stock hash fetches retain the
  explicitly accepted weaker boundary, pending optional hardening.
- Positive: read-only docs, branches and multiple audiences all reduce to "one doc, its
  own slots".
- Cost: Cabinet changes first: meta docs and their projection, self-doc children, the
  frontier-fed projection (ADR 024), and `PollDatums` (ADR 025).
- Cost: anyone who can read a doc can read its keys. That is the stated ground rule,
  not a leak.
