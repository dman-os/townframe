# ADR 014: Cabinet, Drawers, and Selective Mirroring

**Status:** Proposed

## Why this exists

[The drawer FDR](../fdrs/001-drawers-as-document-collections.md) describes what users do with drawers. This ADR says what Cabinet stores, what Keyhive grants mean, and how a node syncs a subset without downloading the entire origin drawer. The current singleton `DrawerRepo` mixes drawer metadata, document creation, facets, and branches. Cabinet replaces only its drawer-management role. DocumentsRepo will own document creation, edits, branch operations, and validated facet writes in a separate ADR. XRPC search and public indices likewise belong to a separate ADR.

A document can be in several drawers; a node can know a drawer without mirroring it; a peer can have access without storing its bytes. These are not interchangeable states.

## Drawer identity and layouts

`DrawerId` is the logical `DocumentId` of its descriptor. Neither its Keyhive group nor a hosting node identifies the drawer. Cabinet supports:

1. **Single-document:** the descriptor is also the only document. No separate collection group is needed.
2. **Single-group:** the descriptor names one collection group G. Document owners can give G access to their documents; G's members thereby gain the corresponding, possibly narrower, document access.
3. **Managed participants:** the same one collection group, with additional reader, writer, and administrator groups for managing people. It does not partition the documents; use child drawers for separately governed collections.

We do not initially implement the self-group layout (descriptor document as collection group). A single-document drawer may grow a collection group; a single-group drawer may add participant groups. Both retain the same `DrawerId`. Layout transitions must be recoverable and must not claim completion before descriptor and authority changes are effective.

A branch is a separate document with its own ID. Branching does **not** automatically put that ID in every drawer containing the parent. A creator authorized to edit a drawer can admit a new branch immediately as part of the DocumentsRepo–Cabinet branching operation; other branches require an explicit addition. Revise [ADR 007](007-doc-branch-identity.md) §6 accordingly.

## Official members are not just access grants

Keyhive answers **who may access D**, not **whether a drawer writer approved listing D**. In Keyhive's authority direction, `D → G` is a grant *over D* to G. A person with authority over D can issue it even if they cannot edit drawer G. The grant makes D discoverable as a candidate to holders of G, but cannot by itself be an official drawer addition. Conversely, `G → D` gives D (and its downstream principals) authority over G. Even at Relay, it exposes G's Relay-visible membership metadata. We do **not** create that reverse grant just to list a document.

Cabinet uses a **drawer-writer-controlled Automerge roster** for official membership. A drawer has a roster registry document; it identifies the active roster-shard documents. Each shard holds one membership facet per admitted logical document ID (including an explicitly admitted branch ID), not one facet containing a giant set. The registry has one registration facet per shard. Only authorized edits to registered roster shards count as official entries; a Keyhive grant to the metadata group cannot register an arbitrary document as a shard. DocumentsRepo will define and enforce the write gate, including how an Automerge change's actor maps to an authorized writer. A group grant or unverified facet author is not curator approval. The roster is authoritative for listing, not for access.

A drawer add normally has **two independent effects**: an authorized roster edit admits D, and someone authorized over D grants `D → G` at the desired level so G's members can access it. These are not one atomic cross-system transaction. A roster entry may arrive before the grant, or a grant may arrive before the roster edit. Cabinet reports respectively **listed but not accessible** and **accessible candidate, not officially listed**; it does not infer the missing half. If a document is separately shared, an official roster entry need not mint a redundant grant just to record admission. Revoking access and removing an entry are likewise different operations. A failure halfway through an add remains visible and retryable rather than falsely reported complete.

A holder of G may discover documents reachable through `D → G` grants in Keyhive, without first loading the roster. This is a **candidate set**, not necessarily the drawer's complete or approved roster: the holder can also reach documents through other paths, and a document owner can grant to G without the drawer writers' consent. To enumerate official members, intersect verified roster entries with observed access when an accessible listing is requested; keep roster-only entries visible as pending/unavailable where the reader is allowed to see the roster. A document reader may see the recipient group ID in D's authority metadata, but `D → G` alone does not grant that reader G's roster or identify what purpose G serves. We must not promise that G's identifier is secret.

For public documents, per-document `D → Public` Read grants allow reading without making a collection group Public Read: current Keyhive Read holders can subdelegate. The drawer roster remains writer-controlled. An authenticated public index can project it for scalable enumeration, but is not its source of truth. A public grant by a stranger neither edits the roster nor makes their document an official drawer member. A drawer that deliberately wants open submission needs an explicit admission policy; do not accidentally get one by equating Keyhive reachability with curator approval.

## Relationships and location

The only drawer-to-drawer relationships this ADR needs are **Child** and **SubsetOf**. They are claims made by a drawer writer, not automatic Keyhive grants. A child need not know all its parents. If a parent is meant to supply access to a child, Cabinet also arranges the appropriate explicit grants, bounded by the author's own authority. A mirror is another node's copy of the **same DrawerId**, not a second drawer. No undefined catch-all `Related` relation is introduced.

Drawer writers may publish **home-node facets** for finding the descriptor and content. A home is a likely contact point, not an exclusive owner, not a mandatory mirror, and not proof that all member bytes are present. Mirror announcements from other nodes are separate, potentially untrusted hints. Cabinet returns candidate providers; the embedder attempts connections and byte sync. Node addressing and XRPC discovery are specified elsewhere.

## Known drawers and each node's policy

Cabinet uses two kinds of ordinary syncable documents, not SQLite-only lists or Keyhive groups used as settings:

- A **shared known-drawers document** contains IDs explicitly saved by its writer. It may be shared with another person: the recipient learns those IDs (and any deliberately supplied labels), but gains no access to the referenced drawers. A drawer discovered via Keyhive or a materialized descriptor is a candidate until saved.
- **Each node's configuration document** records which drawers that node wants to mirror, which subsets it selects, and its local retention preferences. The laptop can mirror more than the phone. A newly authorized node may read another node's configuration as advice, but never executes that other node's choices as its own; copying policy requires an explicit action.

SQLite may hold derived indices, durable worker cursors, and pending work. The shared known list and per-node intent must themselves sync as documents. A node policy is not an access grant, a byte inventory, or a relay's decision to accept a sponsorship request. The relay controls its own accepted-retention policy.

## Facets and their owners

These are the **semantic payload shapes Cabinet needs**, not final lexicon wire schemas. DocumentsRepo owns validated writes and migrations. IDs below are logical document IDs; group references are Keyhive IDs.

```text
DrawerDescriptor { version, layout, content_group?, metadata_group,
                   roster_registry_doc_id }
RosterShardRegistration { drawer_id, shard_doc_id }  // one facet per shard
DrawerMember { drawer_id, member_doc_id }             // one facet per member on a shard
DrawerRelation { source: DrawerId, target: DrawerId, kind: Child | SubsetOf }
DrawerHome { drawer_id, node_id, address_hints[] }     // one facet per home node
KnownDrawer { drawer_id, label? }                      // one facet per saved drawer
NodeDrawerPolicy { version, node_id, drawers: map<DrawerId,
    Know | Mirror | Selected { subset_drawer_id }>, retention_preferences? }
```

The descriptor carries `daybook.drawer`. It points to a roster registry, whose `daybook.rosterShard` facets each register one roster document. A registered shard contains `daybook.drawerMember` facets keyed by member document ID. A `daybook.drawerRelation` facet makes a Child or SubsetOf claim by its authorized writer. `daybook.drawerHome` facets on the descriptor advertise owner-approved home nodes individually; an unrelated mirror's self-advertisement is a separate, untrusted claim, not a canonical home. The shareable known-list document uses `daybook.knownDrawer` facets; the per-node configuration document carries `daybook.nodeDrawerPolicy`. Neither the home nor the known-list facets assert byte availability or access.

Every descriptor, roster registry, and registered roster shard grants **Read to the drawer's metadata group M**. M's group part is the narrow ongoing subscription through which readers discover metadata and roster changes, including new shards; it need not contain or download member-document bytes. Other direct grants are allowed: M is the common read path, not a promise that these documents have no additional readers. Metadata owners may Edit the descriptor; roster writers may Edit the registry and shards without Edit on the descriptor. An optional roster-writer group R can carry those Edit grants across all roster documents; no separate R is needed where the same people edit everything. The drawer's content group G is separate from M. Simply granting an arbitrary document Read to M does not authenticate it as a roster shard: it must also have a valid registry entry.

A single-document drawer can keep its initial registry and member facet on the descriptor itself; there is no requirement to allocate multiple documents just to share one document. When it grows or roster writers need narrower Edit authority than descriptor editors, create a separate registry and first shard and update the descriptor reference. Further registered shards bound the size of any one roster document we must hydrate, not the total drawer history. When a shard's **encoded Automerge document size** approaches a measured hydration or sync-cost threshold, roster writers may create another shard; do not use current visible member count as the limit. Automerge retains historical changes, so ten thousand live entries can be much smaller or larger than a shard with a long add/remove history. A provisional 10,000-entry figure is a benchmark workload, **not** an enforced entry cap or final sharding threshold. Instrument serialized size, load time, and update cost before choosing a byte threshold; the measurement API may need to be added.

In this compact single-document layout, Read of the descriptor also means Read of its content; use a separate descriptor and roster when those audiences must differ. A registered shard may be visible through M before its registry entry or vice versa. Neither order authorizes an unregistered shard, and Cabinet retries discovery after the missing half arrives.

There is no globally exclusive active shard pointer. Under a partition, two writers may create and register new shards concurrently; the merged registry includes both. Writers choose a registered shard with capacity according to the metadata currently available to them; an existing shard may grow beyond its preferred size (even from roughly 10,000 to 20,000 entries) and remains valid. Do not reject writes or discard a concurrently created shard to enforce a soft limit. Cabinet enumerates the union of registered shards and deduplicates document IDs. Removal must account for every visible occurrence of an ID across shards, including duplicates created concurrently; the DocumentsRepo ADR must pin concurrent add/remove semantics before implementation. Rotation or compaction of old history under hard partitions is separate work, not an implicit part of opening a new shard.

## Subsets: why there is a catch-up worker

Suppose an origin drawer O contains many documents, but this node wants only some. It creates a **subset drawer S**, with its own descriptor, roster, collection group and `SubsetOf(O)` claim. S need not grant its members the same access that O grants its members. S's actual selection is **S's roster**, not every Keyhive edge a source happens to hold.

In big_repo, all keyhive groups are mirrored into big_sync parts that are then used to enable massive collection sync. To avoid subscribing to the whole origin part, and pay unnecessary wire costs, the subset owner also shares a **source-visible optimization group** with authorized nodes carrying O. It advertises the selected document IDs as Keyhive metadata, so each source's group-part worker can project a narrow big_sync part from documents that source already holds.

The source need not download S's documents just to project this part. Granting origin nodes visibility into that group must not silently give them S's reader/editor rights; source-visible membership and S's access group have different jobs. A newly projected old document must produce a part membership change even though that document was not edited. The embedder subscribes to the narrow part; it does not subscribe to every selected object forever. An origin-accessible peer is only a candidate source: group metadata alone is no proof of retrievable sedimentrees.

**Why the worker exists:** a source may put D into its source-visible group *before* S's owner adds D to S's roster. The part event can arrive and its cursor advance while D is not selected. The later roster edit causes no new source part event. Therefore **each newly admitted S roster entry schedules durable current-state catch-up for that D**, whether or not the source is online, and whether or not the part cursor has advanced. On reconnection, bounded keyed work checks local `/seds`, compares the required heads with an authorized provider, fetches missing sedimentree content and requested blobs, and retries until the selection is actually available. After seeding, the narrow part carries subsequent changes. The worker does not fetch every unselected origin document, nor create thousands of permanent object subscriptions. An unsolicited edge in the optimization group cannot add D to S's roster or cause unbounded fetch work.

!> the language here of "the source is online" and adjacent language usage here is wrong. again, we're not tracking drawer source nodes here. after all, any node that mirrors the drawer can satify our need provided they're carrying the byets.
!> again, since the hwole big_sync part subscription is downtream of "which drawers is this new node I connected to authorized for"...
!> the worker can then work on the same prinicple. I.e. when a new node connects, we look at the pending sync docs we have, we look at the subset they correspond to, we look at the origin of the subset and see if the new node is eligible for it and take it from ther

For relay sponsorship, **compose the same subset design**: the customer's requested selection is a proposal; the relay admits only what fits its quota to its own accepted subset/retention policy and exposes an optimization group to sources. A customer's grants to that group do not expand the relay's accepted roster. The same machinery also supports an ordinary node retaining a friend's drawer. `Relay` authority allows ciphertext retention and relevant Keyhive metadata sync, not plaintext decryption. A relay must distinguish requested, accepted, fetched, and durably retained entries in receipts and accounting; a part replay cursor proves none of the latter three by itself.

An item can be listed, authorized, projected on a source, locally present in `/seds`, current to the required heads, and retained under policy at different times. A cursor acknowledges observed part events; it must not certify adoption or available bytes. Removing one subset or sponsorship reason cannot evict bytes another mirror, sponsorship, or explicit pin still needs. Explicit local eviction need not edit any roster, though an active mirror policy will fetch the content again. Revocation may stop future sync without erasing an existing local copy; local byte lifecycle is a separate decision.

## Cabinet versus the sync embedder

Cabinet reads descriptors, rosters, known lists, and node policy through DocumentsRepo. It uses Keyhive for current access and candidate-document discovery. `DocFacetSetIndexRepo` can wake it when drawer facets materialize, but that index cannot remember an ID whose descriptor has not arrived. `KeyhiveAccessRevisionStore` can wake it for access changes; Cabinet does not maintain a second authoritative access graph. big_repo's group-part worker projects authority changes into parts, without proving bytes exist there.

For a peer, Cabinet answers **which drawer and optimization parts are worth considering given this node's own policy and present authority**. The embedder (for example `IrohSyncNode`) picks peers, initiates big_sync subscriptions, handles retries/backoff, and reports availability back to Cabinet's catch-up work. The relay embedder additionally handles customer requests, quotas, and acceptance. XRPC indices sit above Cabinet. Neither home hints nor another node's mirror settings direct the embedder to assume a peer stores all contents.

## Verification and follow-up

Before implementation is considered complete, test (1) grants to G without roster entries and entries before grants, including revocation; (2) a doc reader who sees `D → G` but cannot read the roster; (3) a branch explicitly added by an authorized drawer writer; (4) an old document added to a remote-projected subset, including offline selection and source pre-addition; (5) unsolicited optimization-group edges under a bounded relay quota; and (6) overlapping local retention reasons and missing bytes despite a replayed cursor.

[ADR 005](005-relay-sponsorship-retention-admission.md) must distinguish relay request, acceptance, and retained bytes. [ADR 012](012-bigsync-reconciliation-strategy.md) must not claim that full-origin replay or an acknowledged part cursor suffices for attenuated selection catch-up. The DocumentsRepo ADR will specify document edits, branch creation, facet schemas, and lexicon write-gating; the XRPC ADR will specify public projections and authentication of official listings.

<details><summary>Review comments addressed (original text)</summary>

> !> i don't like this, it complicates it. I think we should distinguish that giving someone access to a drawer means giving them rights to modify it? 
> !> I guess your solution is to have a curator private mirror group that specifies the actual collection set and any other grants on the reader readable group is pruned?
> !> this does make our subset sync implementation even more critical
> !> for the non public case, we can have the cabinet remove any offending unauthorized writers from the read set but!!!!
> !> AHH, this wont work. Because it's all p2p. Not everyone getting the drawer will get it from our node so we can't control it.
> !> rotations under hard partitions are tough so we're not yet designing that
> !> well, if the creator of the new branch doc has access on the drawer, it should be able to immediately add it to the drawer. this does indeed make branching a doc repo + cabinet repo interaction
> !> what does related mean? also, mirrors doesn't make sense. a mirror is just the same drawer, not a separate drawer id.
> !> we probably need a facet to indicate what canoincal/home nodes the drawer might be able to be located at?
> !> the term private here can be confusing. for subsets, the subsest has it's own collection group but those don't necessarily mirror the agent grants of the origin. the source vsiible mirror is given visiblity to all the agents from the original drawer allowing us to sync it from any node that might carry the orignal drawer.
> !> again, we're using private-accepted set here. I think what we should do is use the subset drawer relationship design and compose that for the relay usecase.
> !> or in other words, subsets can delcare new group
> !> yeah no, my description of this is much clearer than yours. this doesn't explain why we need the worker well. On selection changes? What selection? This should be stated in the context of subsets. Terrible fucking prose really. The entire document really! Use clearer language!

</details>
