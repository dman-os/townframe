# big_blobs × drawers — design log

Status: Design log. Decisions through §14 are what ADRs 023–026 and the §14 amendments were written from. `afe09255` landed the first drawer-native blob cut; that commit is not a blank check. §15 critiques that cut against later operator direction and authorizes no code. Where this log and an ADR disagree, the ADR wins until it is amended.
Owner: operator + big_blobs track
Updated: 2026-11-01
Scope: Drawer-native blob design, and the Cabinet changes it needs. [big-blobs-replacement-plan.md](./big-blobs-replacement-plan.md) defers to this file for everything drawer-related. Where this log and an ADR disagree, the ADR wins; this log keeps the discussion trail and the items still [open]. The current gap list is §15, not the pre-cut backlog in §14; §16 is the coherent resolution proposal for §15.

ADRs written from this log:
- [ADR 023](../adrs/023-cursor-consumption-is-long-poll.md): cursor consumption is long polling (§13).
- [ADR 024](../adrs/024-indexes-are-fed-by-the-change-stream.md): indexes are fed only by the change stream (§11 "local writes take the remote path", §12 items 3 and 6).
- [ADR 025](../adrs/025-cabinet-datum-predicates-and-polling.md): Cabinet datum predicates and `PollDatums` (§11, §12 items 1–2).
- [ADR 026](../adrs/026-drawer-native-blobs.md): drawer-native blobs (§2–§7, §12 items 4–5).

Legend: **[settled]** operator agreed · **[proposed]** awaiting operator · **[open]** undecided · **[flag]** fact or defect to remember · **[corrected]** an earlier claim in this discussion that was wrong.

---

## 1. Grounding facts (verified in tree)

- **[flag] The metadata-group / collection-group split already exists.** ADR 019 and `DrawerLayout::SingleGroup { meta_group, docs_group }` implement it:
  - `meta_group` holds the metadata doc (Admin). `src/big_cabinet/drawer/create.rs` (`create_collection`).
  - `add_child` delegates parent docs group → child docs group at the entry's access, and parent meta group → child meta group at `max(access, Read)`. `src/big_cabinet/drawer/add_child.rs` (`meta_access`, `child_delegations`).
  - A `Relay` child is therefore "enumerable retention": the relay gets Read on child metadata and Relay on child docs. That is ADR 019 "Child entries".
- **[flag] ADR 019's roster-sharding design already says how to put extra metadata docs into a drawer.** Every descriptor, roster registry and roster shard grants Read to the metadata group M, and a doc only counts once it has a registry entry ("a Keyhive grant to the metadata group cannot register an arbitrary document"). Inventories fit this pattern exactly (§4).
- **[flag] Today a self-doc drawer cannot be added as a child.** `add_child` → `child_delegations` → `Core::groups(child)` returns `DrawersError::SelfDoc` (`src/big_cabinet/drawer.rs`, `groups`). **[corrected]** I read this as a rule. It is an implementation gap: self-doc drawers *may* be child drawers. The real rule is that they never go into the parent's **meta group** (§4, §12 item 5).
- **[corrected] Keyline power is not "attenuated by minimum, full stop".** It is `min` along a route and `max` across routes (keyhive `design/keyline/README.md` "Attenuation"; `implementation.md`: "Attenuation along a route is `min`; combination across routes is `max`"). My earlier claim that Relay on a collection group means "can never get Read on anything inside it" was wrong. A second route, such as the meta group, grants the wider level. ADR 019 relies on exactly that.
- **[flag] `ObserveScope::Drawer` (`src/big_cabinet/doc/observe.rs`) delivers datum changes for member docs, not just membership.** It reads the datum-set frontier (all of it, or one tag's namespace) and filters entries by the drawer's readable set, transitively through children via `scoped_docs`. Docs joining get their current datums replayed; docs leaving get `AccessLost`. Defects:
  - **Membership recheck is not incremental.** Every tick of the global `DrawerAccess::subscribe_changes()` `watch<u64>`, and every capability `Live`, makes **every** Drawer-scoped observer recompute its full transitive readable set (`recheck_drawer` → `readable_in_drawer`) and diff it. Cost is O(observers × closure) per unrelated drawer edit anywhere on the node.
  - **Only one optional tag filter.** A blob worker needs Blob + CipherBlob (+ inventory tags). That means multiple streams, or an unfiltered stream filtered client-side.
  - **Read is required.** The readable set is Read-level, which is fine for writers maintaining inventories. Relay-side consumers do not use this path.
- **[flag] The legacy facet-set index does not have the recompute problem, because it never scoped by drawer.**
  - `src/daybook_core/index/facet_set.rs` selects by `FacetSetSelector::{All, Tag, Tags, FacetTags, Documents, Routes}`, so multi-tag reading is supported.
  - Group scoping happens one layer down, at the doc-delta source (`DocDeltaSelector.source: AutomergeFrontierSelector`): a Keyhive group becomes a group-part subscription. Membership arrives as part events, so it is incremental by construction.
  - The legacy encryption worker re-arms on the eligibility group's part events the same way (ADR 003 §13).
  - Cabinet's `ObserveScope::Drawer` replaced that with recompute-on-global-tick. That is a regression in shape, not an inherited one.
- **[flag] Cabinet's store is not what limits tag filtering.** `DatumSetStore::open_tags` already takes many `(tag, after)` pairs, and `query_docs` takes a doc set. The protocol surfaces (`ObserveScope`, `QueryDatums.selector: DatumSelector`) expose only one selector.
- **[flag] A richer predicate language exists, stranded in `daybook_types`.** `src/daybook_types/manifest.rs` defines:
  - `DocPredicateClause::{HasTag, HasReferenceToTag, FacetFieldMatch{json_path, op, value}, And, Or, Not}`, with requirement planning (`append_requirements` → `FacetsOfTag`/`FacetManifest`) and `ApproxInterest`/`Exact` evaluation;
  - `DocChangePredicate::{Any, Added, Deleted, Changed/Added/RemovedFacet{Tags,Keys}}`.

  Only plug processors and triage use them (`daybook_core/rt/triage.rs`). Cabinet has nothing equivalent; it has `DatumSelector`, `DatumRefSelector`, `ObserveScope`. **[settled]** This is reference material only and stays untouched. Cabinet gets its own narrower, index-only language (§11).
- **[flag] Encryption mutates the Blob datum.** ADR 003 §4/§19 step 6 write `db+blob:///<P>?via=<cipherBlob>` into `Blob.urls`. Read-only docs cannot take that write, and ADR 003 never addresses this.
- **[flag] Why the JWK was moved out of the content doc.** ADR 003 §6 originally co-located the JWK and cipherBlob in one Keyhive-protected doc. §19 moved the JWK into a key document "granted to the decryptor group", so that "a retention domain can be given the cipherBlob ... without ever being given the key". The model assumed a serving tier that could **read** the content doc but must not decrypt. In the drawer/Keyhive model that tier does not exist:
  - Relays hold content docs at **Relay** (ciphertext only) and learn representation digests from the inventory, never from the cipherBlob.
  - Anyone with Read on the doc already sees the Blob datum and is entitled to the plaintext.

  The separation therefore protects nobody and costs a key doc, a keys drawer, and the staging/commit/registration sweep in §19 steps 2–4.
- **[settled] The §19 addendum's reasoning for separate JWK documents is rejected.** "Anyone who may serve a representation must know which representation to serve", and "a retention domain can be given the cipherBlob without the key", both assume a party with Read on the content doc who must not decrypt. No such party exists. Servers (relays) learn representations from inventories and hold content docs only as ciphertext.
- **[flag] Why domains were invented.** The ADRs say it consistently:
  - ADR 003 Context: "the same plaintext blob may have multiple encrypted representations for different storage or sharing domains".
  - ADR 003 §7: a private relay domain vs a shared project domain, with distinct keys, "prevents the representation digest itself from trivially revealing that both domains contain identical plaintext".
  - ADR 003 §15: re-encrypt into a new domain to break correlation with an existing one, and deliberately add no predecessor link.
  - ADR 001 "Multi-Domain Re-Encryption": "preventing metadata leakage across access domains".

  The motivation is **anti-correlation of identical plaintext across audiences**. The `grp:` group naming came later (§19) as the Keyhive-group way to name an audience. §3 below shows that per-doc slots satisfy the original motivation.

## 2. Encryption ground rule

- **[settled] Encryption exists so that relays without plaintext access can serve blobs.** Anyone with plaintext access to a doc may hold its keys.
- **[settled] The JWK lives next to the cipherBlob.** Key docs and the keys drawer are dropped. `keyRef` stays a general URI, so a separate key store can come back later without changing the cipherBlob abstraction. Co-location is the default, not a schema restriction.
- **[settled] A blob may have several cipherBlobs.** "One cipherBlob per blob" is not a rule. Use case: someone branches a doc and has no access to the original's cipherBlob/JWK, so they mint their own.

## 3. Domains: what a cipherBlob slot is keyed by

- **[settled] The default domain is the document, not the drawer and not the relay.** Reasoning:
  - A doc that sits in drawers X and Y is the same `DocumentId` in both collection groups. A relay holding both already links them by doc id, so separate ciphertexts per drawer hide nothing from relays.
  - With the JWK co-located in the doc, every reader of the doc can read every slot's key. Per-drawer keys therefore do not narrow decryption either.
  - Per ADR 003 §7, domains matter when the same plaintext sits in **different docs** with different audiences: branches and copies (§5). Those get their own slots naturally, because they are different docs. This meets the original anti-correlation motivation (§1).
- **[proposed] Slot keying:** `cipherBlob/<blob facet key>` (plus a discriminator if more than one is ever needed in one doc), with the JWK at `jwk/<same key>`. One JWK per blob slot means no plaintext equality is visible even inside a doc, and rotation is per blob. ADR 003 §19's `keyScope` disappears. Resolution from a Blob is then a naming convention inside the same doc, and the encryptor **no longer mutates `Blob.urls`**. Writable and read-only docs then differ only in *where* the cipherBlob lives (§5), not in whether the Blob gets edited. `?via=` stays legal as an optional hint, not as the resolution mechanism.
  - **[flag]** This reverses ADR 003 §19's `grp:<group_id>` domain naming and the refusal of in-doc (`self`) keyRefs. ADR amendment required.
- **[open]** Sanity check against "a reader who only knows doc D through drawer X must be able to tell which ciphertext X's relay holds": under per-doc slots every drawer's inventory pins the same C, so X's relay holds C whenever X's inventory lists D. This holds as long as inventories are per drawer (§4).

## 4. Inventories are drawer metadata

- **[settled] Inventories stay as a concept, under that name.** The drawer is the sync and collection domain, but a relay needs a **readable** manifest of representation digests, and that cannot be avoided.
- **[settled] The second job of inventories goes away.** That job was minting a Keyhive group to hold big_sync blob partitions; the drawer's groups take it over. The big_sync blob partition for a drawer uses the drawer's group as its holder group.
- **[corrected]** An earlier revision of this log recorded "pins are datums on the drawer's metadata doc" as settled. That was wrong.
- **[settled] An inventory is always a separate document. The drawer's metadata doc carries only a reference to it.**
  - Pin churn then never grows the metadata doc's Automerge history. Every participant and relay syncs that history to learn the descriptor and roster.
  - This is ADR 019's roster-shard pattern: an extra metadata-class doc that the meta group can read, and that counts for the drawer only through an entry on the metadata doc.
- **[proposed] Shape.**
  - **Reference datum on the metadata doc: one fixed slot per kind**, `org.example.daybook.drawer.metaDoc/<kind> { docId }`. For blobs that is `metaDoc/blobInventory { docId }`.
    - **[corrected]** The earlier shape `metaDoc/<docId> { kind }` gave every concurrent creation its own key, so the race left two live references. With a fixed slot, concurrent creators write the **same datum**. Automerge LWW leaves one value, and Cabinet projects only that value (concurrent creation, below).
    - `kind` is an open string vocabulary (see `lexicon-authoring`) used as the key id. Cabinet projects it but does not interpret it. `big_blobs_core` owns `blobInventory`.
    - The reference lives in the `drawer.*` namespace, so only DrawerService writes it (§12 item 4).
  - **Inventory doc datums:**
    - `blobPin/<C> { lengthOctets }`: ADR 001 §2 shape, with the same strict omissions.
    - Optional config `org.example.daybook.blobs.inventory/main { extra }`.
    - Both are owned by `big_blobs_core`; Cabinet stores them without interpreting them.
  - **Creation is a DrawerService op**, because only DrawerService can make a doc metadata-class and then reference it. The order is delegation-before-entry, the same as docEntries:
    1. Create the doc.
    2. `grant_doc_access(doc, meta_group, Admin)`, as `create_collection` already does for the metadata doc.
    3. Write the reference datum.
    4. Project the reference.

    The required level is drawer **Write**, because writers maintain inventories (laziness trigger, below).
  - **Keyhive access comes for free from the meta group.** Relay-level principals get Read on the meta group when invited (`principal.rs` → `meta_access`) or linked (`add_child.rs`), so relays read the pins. Write participants get Edit, so they write pins.
  - **Cabinet reach needs one change.** `ancestors.sql` reaches a drawer's own metadata doc because the drawer id *is* the metadata doc id. An inventory doc has a different id, so it needs a projected `big_cabinet_drawer_meta_docs` row and a matching union branch in `ancestors.sql`. Its writers then reach it at the drawer's level (§12).
  - **Why not a docEntry:** docEntries go into the docs group. A Relay-level principal holds that group only as ciphertext, so it could not read the pins. Inventory docs would also show up as drawer content in queries.
- An inventory pins the representations of the drawer's **own** docEntries only. Children pin theirs in their own inventories, and the relay unions them while walking the meta-group closure (§7).
- **[proposed] Concurrent creation: detect it through dmeta creation UUIDs, then retire the loser.** (The operator's direction; this replaces the earlier "smallest doc id, no cleanup" proposal.)
  - **The mechanism, verified in tree** (`doc/pipeline.rs`, the alias choice around line 167): every datum key gets an alias in `Dmeta.datum_uuids: BTreeMap<Uuid7, DatumKey>`. A writer reuses the smallest alias it can see for the key, and mints a new one only when it sees none. Two writers that each create `metaDoc/blobInventory` without seeing the other therefore each mint an alias. After the merge, the key has **two aliases**. That is durable, signed evidence that a concurrent creation happened, and it survives restarts.
  - **Who won:** the slot's merged value (Automerge LWW). That is independent of which alias is smaller, and only the value counts. The Cabinet projection admits only the doc the slot currently names (still subject to the delegation check below). The loser never gets a `meta_docs` row, so it loses Cabinet reach immediately and relays never read it.
  - **Cleanup, in order:**
    1. **Every writer's pin worker re-seeds the winner.** When a writer's pin worker sees the slot's value change, it re-adds its whole observed pin set to the new doc, as adds only. That is safe for a writer whose view lags (no removals), and it covers pins anyone wrote into the loser before seeing the merge.
    2. **The loser's creator retires the loser.** It knows the loser is its own doc from its `CreateMetaDoc` ledger row. When it sees the slot name another doc, it revokes the meta group's grant on the loser, which drops it from the drawer's Keyhive closure. The ledger row makes this restart-safe.
    3. **Other peers** that see ≥2 aliases only check that the projection names the slot's value. They take no destructive action, because only the creator knows which doc it made.
  - **[flag]** Between the merge and the re-seed, a relay can briefly see the winner without the loser's pins. ADR 003 §19 release is reactive, and the relay keeps blobs it already holds until an observed removal. A missing pin is not a removal, so nothing is deleted.
  - **[flag]** ≥2 aliases means concurrent creation *for this slot* only because the slot is written once, at creation. A re-creation after a removal reuses the visible alias (verified in the same code), so it does not register as a race. Keys that are rewritten all the time (`docEntry/*`) also collect aliases under concurrent writes, so alias count is not a general race detector.
- **[flag]** DocsRepo does not stop a Write-level actor from writing `drawer.*` datums (`docEntry`, `childEntry`, and now `metaDoc`) on a metadata doc directly, which bypasses DrawerService's delegation-before-entry ordering. A `metaDoc` reference to an arbitrary doc id would give every drawer writer Cabinet reach to that doc through `ancestors.sql`. Remote writers bypass DocsRepo entirely, because the projection trusts signed metadata-doc content.
  - Recommendation: the projection admits a `metaDoc` row only if Keyhive shows the referenced doc delegated to the drawer's meta group. That is the same check ADR 019 states for registry entries.
  - `docEntry` already has the same exposure. It predates this work and is noted here, not fixed.
- **[proposed] Laziness trigger = a `blobInventory` reference on the metadata doc.**
  - Writers of a drawer that carries one maintain cipherBlobs and pins for its Read+ entries.
  - Sponsoring a drawer you can write creates the inventory if none exists.
  - Sponsoring a drawer you can only read relies on an existing inventory. Without one, the relay keeps the docs but not their blobs, and that is surfaced as a status.
- **[proposed] Pin maintenance is delta-driven, never a full-set reconcile.** Multiple writers maintain one inventory. A writer whose view lags must not delete pins for facets it has not seen. A pin is removed only on an observed cipherBlob removal or an observed doc removal from the drawer (ADR 003 §19 "release is deliberately reactive"). Concurrent identical adds merge, because pins are keyed by C.
- **[flag]** Relay-level docEntries (docs we cannot read) contribute no pins: their cipherBlobs are unreadable to us.
- **[settled] Self-doc drawers may be child drawers, but never enter the parent's meta group.** (Operator correction: an earlier entry here said "never children".)
  - **Why:** a self-doc drawer's descriptor *is* its content doc. The meta group is readable by Relay-level participants (§7 publicness contract), and a self-doc doc holds sensitive content, not just drawer metadata. So it must never be granted to any meta group.
  - **Delegation for a self-doc child at `access`:** the parent's **docs group** gets `access` on the self-doc doc itself, the same Keyhive shape as a docEntry. There is **no** meta-group delegation, because the child has no groups and the parent's meta group never touches the doc. The `meta_access` widening of Relay to Read is for single-group children only.
  - **What the child entry adds over a docEntry:** it lists the doc *as a drawer*, with its own identity and grants, and `scoped_docs` yields it through its existing self-doc branch. Keyhive sees the same thing either way.
- **[settled] Self-doc drawers have no inventory; the containing drawer pins their ciphertext.**
  - A self-doc drawer has no meta group (`Core::groups` returns `SelfDoc`), so `CreateMetaDoc` rejects it with `SelfDoc`.
  - Whether it is linked as a child or listed as a docEntry, the parent's pin worker treats a self-doc drawer as one of the parent's own docs. It maintains the cipherBlob/JWK slots *inside that doc* and pins them in the **parent's** inventory. The §5 rule still applies: a writer who cannot write the doc branches it. So `InDrawer { transitive: false }` (§11) means own docEntries **plus own self-doc children**.
  - Ciphertext therefore always belongs to a containing single-group drawer. A self-doc drawer is a pure specialisation: one doc, one audience, plaintext to whoever can read it.
  - **[proposed]** If a relay sponsors a lone self-doc drawer, it carries the doc but no blobs. To mirror its blobs, put it into a single-group drawer.
- **[corrected]** An earlier revision proposed `DrawerChildEntry { access, layout }`, so that the parent's roster would state the child's layout. The operator objected: the layout duplicates the child's own descriptor, the two can disagree, and the child could no longer change its layout without every parent rewriting its entry.
- **[corrected] The child entry stays `{ access }`, and no shape is derived either. The earlier `[proposed]` block here — "the projection derives the child's shape from Keyhive into a new `shape` column (migration 002)" — rested on two premises the code contradicts; both questions to the operator are withdrawn.**
  - **A child is always a drawer.** `pub type DrawerId = DocId` (`big_cabinet_core/drawer/protocol.rs:26`); `AddChild { parent_id, child_id: DrawerId, access }` names the child's *descriptor doc*. Bare docs are a different op, datum and table: `AddDoc` → `docEntry` → `big_cabinet_drawer_docs`. The projection routes on the datum tag (`big_cabinet/drawer.rs:745`, `:767–778`), so "is this link a doc or a drawer?" is never a question, and no Keyhive-derived discriminator is needed.
  - **Relay-level readers never consult the child's layout.** `readable_in_drawer` filters to `held >= DrawerAccess::Read` (`big_cabinet/doc/query.rs:95`) and `Relay(0)` orders below `Read(1)` (`big_cabinet_core/drawer.rs:354–359`), so `scoped_docs` never reaches a Relay-held child. The premise "a Cabinet node at Relay needs the shape to list the child" was false: such a node does not list the child at all (today's behaviour, and the `[flag]` below about root self-doc drawers at Relay already accepted it). A relay's meta-group walk and ciphertext retention never read the child's layout either.
  - **What admission does check.** The projection verifies the child's Keyhive delegations against the child's own descriptor layout — `selfDoc`: the parent's docs group on the child doc and **no** meta-group access; `singleGroup`: both group-to-group delegations. Mismatch is not admitted; a meta group holding a self-doc doc is refused loudly as a defect. This is a consistency check, not a second source of truth: the layout stays in the descriptor, and `scoped_docs.sql`/`ancestors.sql` keep their existing `json_extract(drawer.descriptor_json, '$.layout.$type')` joins, which every row that reaches them can read.
  - Where the node cannot yet see the backing (Keyhive state lagging the roster datum), the entry stays listed-but-pending and flips on the next recheck — see the §11 [corrected] item. No `shape` column, no migration change beyond the meta-docs table.
- **[open]** Public drawers: should an inventory also pin **plaintext** representations, so relays can serve public blobs without encryption? ADR 001 pins are representation-agnostic, so this is allowed by shape. Deferred.

## 5. Read-only docs: branch them

- **[settled] Convention: you cannot write a doc you want to sponsor or encrypt, so you branch it and work on the branch.**
  - A companion doc that only carries cipherBlobs is rejected. Read-only is a general blocker: you will eventually want edits beyond encryption, and a branch handles all of them.
  - Adopting someone else's public doc into your sponsored set is the same act: branch it.
  - The branch is an ordinary writable doc, so encryption is in-doc (§3). Resolution needs no extra mechanism.
  - Cabinet already has this: `BranchDoc` in `big_cabinet_core::doc`, and the `org.example.daybook.branch` datum with parent refs and pinned fork heads (ADR 034).
- **[proposed]** The branch records **why** it exists: a reason on the branch datum such as `forkReason: readOnlyOrigin`, a string vocabulary that stays open. Tools can then show "your writable copy of X" and offer to re-sync from the origin.
- **[flag]** The relay stores the branch as its own sedimentree, so the origin's history is retained twice if both are sponsored. That is acceptable, and noted for quota accounting.
- **[flag]** If the origin's owner already encrypted (an in-doc slot exists on the origin), sponsoring the **origin** works without a branch: we can read its cipherBlobs and pin them.

## 6. Concurrent encryptors (winner selection)

- **[flag] The race is narrow.** The salt is derived from (JWK secret, P, framing), so once a slot's JWK exists every device produces byte-identical ciphertext. The race is only two devices minting the first JWK for the same slot concurrently.
- **[settled] No competing set or manual resolution for now.**
  - Automerge LWW gives every replica the same winner.
  - Pins are derived from the merged cipherBlob, so the loser's pin leaves the inventory, and its pair is released by the normal reactive path and the PairRoots drain.
  - The loser's ciphertext, if already uploaded, is collected by the relay after its grace period.
- **[flag]** A late-arriving concurrent write (an offline device) can flip the winner later. The cost is a re-upload. This is accepted.
- **[flag]** The JWK and cipherBlob are separate datum keys, so their LWW winners can come from different devices. `keyRefHeads` keeps that safe: the cipherBlob resolves its key at the heads it pinned. This is an invariant to test.
- **[open] Future optimization:** prefer the candidate the relay already holds (the operator's original motivation for manual resolution). Needs relay retention knowledge on the writer, e.g. ADR 005 cursor acks.

## 7. Sponsorship and relay visibility

- **[settled] Sponsorship is a private relationship between an account and its relay.** Each account tells its own relay about **one** root sponsorship drawer and grows the set through child entries (`drawer.md` relay sponsorship use case). Two collaborators may sponsor the same drawer to different relays without coordinating.
- **[settled] Sponsorship is not private in effect.** Once a drawer lives on a relay, other full-read participants may fetch from that relay. Keys travel inside the docs, so they can decrypt.
- **[proposed] Relay walk.**
  1. Start at the root sponsorship drawer's metadata.
  2. Follow child meta groups (Read, per `meta_access`).
  3. In each drawer read the metadata doc: descriptor, roster, and `metaDoc` references.
  4. Read the referenced `blobInventory` docs, which are readable through the same meta-group Read.
  5. Adopt the docs closure (ADR 005 adoption worker, made transitive over child docs groups).
  6. Retain the union of inventory pins.

  Because the relay can read child metadata, it can correlate an inventory with its drawer and dedupe across accounts sponsoring the same drawer, by drawer id and by C.
- **[proposed] Publicness contract (to add to `drawer.md` / ADR 019).**
  - Anything granted Read to a drawer's meta group — descriptor, roster, inventories — is visible to every participant **including Relay-level ones**.
  - No private datums (names, descriptions, icons, notes) on a non-self-doc drawer's metadata doc. Private drawer metadata belongs in a docEntry.
  - A self-doc drawer has no separate metadata. Linked as a child or listed as a docEntry, it is reached only through the parent's docs group, so its content stays ciphertext to Relay-level participants. Only the parent's roster entry (its access) is metadata.
- **[proposed] Only sponsored drawers are visible to a relay.** Drawers outside a sponsorship closure stay invisible.
- **[open] Future privacy extension ("shadow mirrors").** Give a relay only inventory docs and group ids for a drawer, not its full metadata. Recorded, not designed.

## 8. Keyline-native rosters (direction, not this PR)

- **[flag]** A pure-keyline roster (an edge counts iff its signer has current write standing in the drawer) would remove the Automerge roster from relay-visible metadata. It does **not** remove:
  - the metadata doc: evaluators must read the drawer architecture to know which group's standing judges an edge, and advanced RBAC layouts such as public wikis make that architecture-specific;
  - the inventory docs and the `metaDoc` references to them.

  The meta group therefore stays either way, and the inventory design in §4 does not depend on the roster choice.
- **[open]** The generalisation applies to any composition, not only relays: giving someone access to a drawer must give them Read on child drawer metadata. `add_child` already enforces this through `meta_access`.

## 9. Impact on existing ADRs (to amend once settled)

- **ADR 001:**
  - no default inventory doc and no `blob_inventories` authority group;
  - an inventory is a separate doc owned by the drawer's meta group, referenced from the drawer's metadata doc;
  - partitions use drawer groups;
  - plaintext sync between readers is derived from the drawer's Blob datums.
- **ADR 003:**
  - co-located JWK (§5–6 restored, §19 key docs dropped);
  - per-doc slot keying instead of `grp:`;
  - in-doc `keyRef` accepted;
  - `Blob.urls ?via=` becomes optional;
  - read-only docs are branched (§5);
  - the §19 separate-key-document reasoning is dropped.
- **ADR 005:**
  - the account doc names one root sponsorship drawer instead of `S_docs`;
  - adoption follows the drawer closure transitively and reads inventories.
- **ADR 019 / `drawer.md`:** the publicness contract (§7), plus `metaDoc` references, their projection, their reach, and the delegation check at admission (§4).
- **ADR 034:** an optional branch reason (§5).

## 10. Defects and follow-ups noted during the discussion

- `ObserveScope::Drawer` recomputes the full readable set on every global drawer tick (§1). This is a regression from the legacy part-event scoping. Fix in §11.
- `ObserveScope::Drawer` / `QueryDatums` take a single selector although the store supports more (§1). Fix in §11.
- **[flag] Observe cursor defects (verified, `doc/observe.rs`).** `ObserveDatums { after }` and `DatumObserved::Entry { revision }` / `ReplayComplete { through }` already form a cursor over the datum-set frontier revision, which itself sits over the AFW (`automerge-frontier`) cursor. That is the legacy facet-set shape. Two paths break it:
  - **Stamping ahead of the reader.** `recheck_delivered` and `recheck_drawer` (lines ~325 and ~389) stamp access- and membership-triggered events with `store.latest_revision()`. That can be ahead of the page the reader has consumed. A consumer that persists the stamp and resumes from it skips the entries in between.
  - **Membership is not in the stream.** The Drawer scope computes `drawer_readable` at open from current state. Resuming with `after > 0` misses docs that joined while the consumer was away (their datums are older than `after`), and sends no leave for docs that left.

  Fix in §11 (one cursor).
- **[flag] Authorization rebuilds the projection on a miss (verified).**
  - `Core::effective` (`drawer.rs:489`) and `Core::reach` (`drawer.rs:512`) read `ancestors` from the projection. On a miss (`bounds.len() <= 1` or empty), they run `sync_discovered`, which queries every announced `drawer` descriptor from datum_set, and then `ensure_drawer` → `discover_drawer`, which hydrates and rescans rosters breadth-first. Then they ask again.
  - `access.rs` ~186–227 does the same through `PolicyOperation::Refresh`.
  - That is a reload-everything fallback on the hot read path, and it is the reason the projection was being maintained on the write path at all.
  - With the projection fed only by the change stream (§12 item 3), a miss means "not reachable as of the projection's cursor". The answer is `Denied`, and a caller that just wrote the roster waits on the projection's cursor first.

- **[flag] Self-doc children: one local gap, and one unchecked projection (verified).**
  - **The local gap.** `AddChild` with a self-doc child fails with `SelfDoc` (`link` → `child_delegations` → `groups(child)`), before it delegates or publishes anything (`e2e/membership.rs` ~450). It should succeed, with docs-group-only delegation (§4, §12 item 5).
  - **The unchecked projection.** The roster rescan (`drawer.rs` ~740–790) projects every `childEntry` without checking delegation shape. A remote writer of a parent can publish a child entry that Keyhive does not back, and `ancestors.sql` then gives holders of a capability on the parent Cabinet reach onto that child. This is the same exposure as the §4 `docEntry` flag.
  - **[corrected]** An earlier revision proposed dropping `scoped_docs.sql`'s non-root self-doc branch and making `ancestors.sql` require single-group children. Both are wrong now that self-doc children are legitimate; those branches are what make them work. The fix belongs in admission: the projection checks a `childEntry`'s Keyhive delegations against the child's descriptor layout (§4) before admitting it. The queries themselves do not change — they keep reading `descriptor_json`, which every row that reaches them can read (`readable_in_drawer` filters to ≥ Read).

## 11. One datum query language in `big_cabinet_core`

- **[settled] It is new code in `big_cabinet_core`, not a port.** `daybook_types` and `daybook_core` are not touched. `daybook_types::manifest` is reference material only, and it disappears with the legacy stack.
- **[settled] Every clause must be answerable from indexes.** No predicate may need a document hydrated or a datum value read. That rules out `FacetFieldMatch`/`FieldMatch`; consumers that need value filters apply them to the datums delivered to them. The sources a predicate may use:
  - `big_cabinet_datum_set_entries`: `(doc_id, datum_key, datum_tag)`, indexed by tag and by key;
  - the drawer projection (`big_cabinet_drawer_docs`, `big_cabinet_drawer_children`, `big_cabinet_drawers`, plus `big_cabinet_drawer_meta_docs` from §4), walked with recursive CTEs;
  - the caller's capability level on a named drawer: one capability check per drawer clause, rechecked on capability `Live`;
  - `big_cabinet_datum_ref_edges` for `HasReferenceTo` (below).
- **[proposed] Clauses.**
  - `HasTag(tag)`, `HasKey(key)`;
  - `IsDoc(doc_id)`: one named doc. This is today's `ObserveScope::Doc` / `DatumSelector::Doc`, which the UI and the peer tests use to watch one doc;
  - `InDrawer { drawer_id, transitive, at_least }`:
    - `transitive: false` means the drawer's own docEntries plus its own self-doc children (§4), which is what a per-drawer pin worker needs;
    - `at_least` is the access we have (Relay/Read/Write), computed as `min(our grant on the drawer, CTE level along the best route)`;
  - `And`, `Or`, `Not`.

  This is how "which access we have" stays answerable from indexes: the access level is only defined relative to a drawer. A drawer-free "docs we can write anywhere" clause would need capability grants in SQL, and they are not there. **[flag]** That clause is left out on purpose.
- **[settled] Drawer membership is a clause, not a scope.** The request's authority bounds what may be returned. A caller below node level must anchor the predicate with a top-level `And` containing an anchor clause, and that clause is also the authorization check. `Not` likewise needs a positive sibling in its `And`, so a query never enumerates "every doc without X".
  - **[proposed] The anchors:**
    - `InDrawer` at `at_least >= Read`: one capability check on the drawer, then the reach CTE;
    - `IsDoc(doc_id)`: one `AccessRequest::Read` check on that doc, the same check today's `Doc` scope does before it opens.

    Today's unanchored paths, `ObserveScope::Tag` and `QueryDatums` without a `drawer_id`, authorize every candidate doc one by one. They stay legal only for a capability with node-wide Read, which needs no per-doc check. A narrower capability states its anchor. That removes the per-candidate `authorize` loop in `doc/query.rs` and the per-batch check in `doc/observe.rs`.
- **[settled] `HasReferenceTo` is included.** Shape: `HasReferenceTo { target_tag }`, optionally `{ target_doc }` too. It joins `big_cabinet_datum_ref_edges` (origin side = the candidate doc) with the target's `datum_set_entries` row for the tag. The edge table has no target tag column, so the join with `datum_set_entries` on `(target_doc, target_datum)` is what supplies it.
  - **Verified:** datum_ref is **not** a separate DB. `Indexes::boot` hands the same `sql` to `DatumSetStore::new` and `DatumRefStore::new`, so the join is one statement.
  - **The loose coupling is in the cursors.** datum_ref is a separate consumer of the datum-set frontier, with per-tag cursors (`big_cabinet_datum_ref_progress`), durable parked routes, and its own `settled` watch. At any moment its edges can lag the datum-set revision, so a query cannot say "edges as of revision r".
  - **[settled] Tie them: extract edges inside the datum-set worker.**
    - `DatumSetStore::prepare` already loads the doc at the delta's heads (`with_document_read`, `DocView`), and already defers a pending doc by returning `None`.
    - For changed slots whose tag declares references, the same `DocView` runs the gate verify + `hydrate_value` + `declared_references`, the code `datum_ref::edges_from` has today. `settle` then replaces the route's edges in the same write transaction that updates `datum_set_entries`.
    - Edges and entries then share one revision.
    - Deleted: datum_ref's per-tag cursors, its parking (datum_set never settles a doc whose heads are missing), its second doc load per route, and the `settled` watch. `DatumRefStore` shrinks to the edge-query surface.
    - **Costs:**
      - datum_set takes `Arc<SchemaRepo>`.
      - The gate and hydration run in the datum_set hot path, but only for reference-declaring tags.
      - A build that adds a new reference-declaring tag needs a one-time backfill at boot. Proposal: a `big_cabinet_datum_set_ref_tags` table records which tags have been extracted. On boot, an unseen reference tag re-extracts from its `datum_set_entries` rows (`doc_heads` are stored) before the worker runs. This replaces what the per-tag cursors do today.
  - **[rejected] Fallback:** keep datum_ref separate, and have readers wait for its progress ≥ r. Parked routes break that wait, because a tag's cursor moves past a parked route.
- **[proposed] A query has two halves.**
  - The predicate picks docs.
  - A datum selection (tag/key sets) picks which datums of the matching docs are delivered.

  The blob worker is `And[InDrawer{X, transitive: false, at_least: Read}, HasTag(Blob)]` with delivery `{Blob, CipherBlob, Jwk}`.
- **[proposed] The index does not carry membership. A CTE answers it.**
  - The datum-set tables and the drawer projection live in the same SQLite `Database`: `big_node` passes one `config.sql` to both `Indexes::boot` and `DrawerAccess::open`. `scoped_docs.sql` and `ancestors.sql` are already recursive CTEs over that projection.
  - `QueryDatums` compiles the predicate into **one** SQL statement:
    - a reach CTE (a `scoped_docs` variant, without recursion when `transitive: false`) for each `InDrawer`;
    - `EXISTS` subqueries on `datum_set_entries` for tags and keys;
    - the datum selection as the outer projection.

    No doc set is materialized in Rust, and there is no `open_all`.
  - No closure table. **[flag]** If CTE cost on deep drawer DAGs ever dominates, a materialized closure table is the escalation. Not now.
- **[settled] Observe takes a cursor, like the AFW / legacy facet-set delta workers.** The cursor is the datum-set frontier revision (`ObserveDatums { after }`, `Entry { revision }`, `ReplayComplete { through }`). That frontier is itself driven by the AFW cursor (`automerge-frontier` walker state), so AFW stays the one change stream and everything is layered over it. The rule a resumable consumer needs: **an event stamped r says everything at revisions ≤ r has been delivered**. §10 records the two places that break that rule today.
- **[proposed] One cursor for datums *and* membership: the drawer projection becomes a frontier-fed index.**
  - Roster facts are already datums: the descriptor, `docEntry/*`, `childEntry/*` and `metaDoc/*` on metadata docs. So they are already in the datum-set frontier, at their own revisions.
  - Today `drawer::State` is filled by a rescan of the metadata doc plus direct writes from DrawerService ops (§1). It has no revision.
  - **Proposal:** the projection consumes the frontier on the roster tags (`drawer`, `drawer.docEntry`, `drawer.childEntry`, `drawer.metaDoc`) and stores a progress revision in the same transaction as its rows. It replaces the rescan, `replace_roster`, and the bare `watch<u64>` with a `progress: watch<u64>`.
  - Like datum_ref today, it loads values from the doc. Admission checks run here:
    - the §4 delegation check for `metaDoc`;
    - the fixed-slot value;
    - the §4 `childEntry` delegation-vs-descriptor consistency check (§12 item 5).
  - **[corrected] "Admission is a function of two streams" was an [open] item here; the code already answers it, and the earlier framing was wrong on two counts.** It cited ADR 013's `KeyhiveAccessDelta` as the second stream — but ADR 013 is **Status: Proposed** and Cabinet has no consumer of that stream (`big_repo/runtime2/keyhive_access_stream.rs` feeds only the legacy `daybook_core/blobs/permission_writer.rs` and the paused `big_blobs/permissions.rs`). Cabinet's actual second stream is the **authority/capability stream** (ADR 015 §3), and `doc/observe.rs`'s pump already merges it with the frontier in one select loop (~230–300): frontier `Entry`/`ReplayComplete` → page; `authority_changes: ClientStream<ObserveCapability>` → `CapabilityStatus::Live` ⇒ `Wake::MembershipChanged` ⇒ `recheck_delivered`/`recheck_drawer`; `Retracted` ⇒ `Err(DocsError::Denied)`. §13 removes the pump but keeps the discipline: grant/capability changes recheck the observer's drawer once per poll (line below), and an entry whose Keyhive backing has not arrived yet is listed-but-pending (ADR 019) and flips on the next recheck. No second observer cursor, no projection-owned revision, nothing blocking on Keyhive, and no operator decision pending.
  - **Observer.** It opens **one** frontier reader on the union of the delivered tags, the tags the predicate references, and the roster tags. For each page at revision r:
    1. wait until projection progress ≥ r (and edge progress, if datum_ref is not folded);
    2. for datum entries, re-evaluate the predicate for the page's docs in one SQL statement (`doc_id IN page`);
    3. for roster entries, first check that the changed drawer is the observer's `InDrawer` drawer or a descendant of it (one `ancestors` CTE). The affected docs are then one doc for a `docEntry` change, or `scoped_docs(child)` for a `childEntry` change (a doc reachable through two children must survive losing one path). Re-evaluate those docs in one statement;
    4. stamp every resulting event — enter (the doc's current selected datums), datum change, or leave — with **r**, never with `latest_revision()`.
  - **Why resume becomes correct.**
    - A join after `after` is a roster entry with a revision > `after`, so it is replayed and the doc's datums are delivered then, even though they are older.
    - A leave after `after` is replayed as a leave. That is idempotent for a consumer that never had the doc.
    - Correctness on resume does not depend on any per-session state. §13 removes the delivered-set dedupe, the pump's only state, along with the pump.
    - Delivery is at-least-once and converges. The projection may be *ahead* of r when a page is evaluated. That can only bring a later fact forward; the later page re-evaluates it.
  - **Replaces** the earlier "typed changes on a broadcast channel, `Lagged` → resync" proposal and its change-log alternative. A revisioned table now exists for free: it is the frontier.
  - **[settled] No projection sits on the write path.** (Operator correction: the earlier "ops wait for projection progress" cost was wrong-headed. Most roster writes are remote, so a local write path can never give a consistent world view anyway.)
    - **[settled] Principle: local writes take the remote path.** Remote changes can only reach an index asynchronously, through the change stream. So local changes are applied to the index the same way: written to the doc, then picked up by the same watcher. There are not two code paths maintaining one incremental structure, an op-side direct write and a stream-side rebuild. The index's state is a function of the stream, and nothing else writes to it. This is the "separate address space" the operator means: indexes live behind the stream, not in the op's transaction. It applies to the drawer projection, datum_set (and with it the folded edges), and any big_blobs index.
    - Drawer ops write the metadata doc and return its receipt (heads). They do not write `drawer::State`, and they do not wait for it.
    - Op idempotency checks (`publish_entry`'s and `publish_child`'s "already listed at this level?") read the metadata doc they are about to write, not the projection.
    - The projection, like every index, is built asynchronously from the change stream. A caller that needs to read its own write waits on the projection's cursor (long poll, §13) until it covers the receipt's heads. That is the caller's choice; the op does not do it.
  - **Grant and capability changes** are not datum facts and stay outside the cursor. With long polling (§13), every poll re-authorizes and recomputes `at_least` on the observer's drawer. Below Read, the poll is `Denied` and the cursor does not move. A level change applies from the next page on. There is no live `AccessLost` push and no in-pump capability subscription.

## 12. What changes in `big_cabinet` before `big_blobs` work starts

Settled context:
- `daybook_core` is deleted before deploy, and neither `daybook_types` nor `daybook_core` is touched.
- Keyline-native rosters are not in this PR.

**Cabinet changes required:**

1. **A `query` module in `big_cabinet_core`.** It holds the predicate (§11 clauses), the datum selection, and Lexicon-style serde spellings. It is new code with no daybook port.
2. **`QueryDatums` / `ObserveDatums` become one long-poll op, `PollDatums { capability, predicate, datums, after, limit, hold_ms }` (§13).** This replaces `DatumSelector`, `ObserveScope`, the `stream` operation and the per-subscription `DatumPump`, which is an interface break. Callers are Cabinet-internal only:
   - `doc/query.rs`, `doc/observe.rs`, `protocol_wire.rs`;
   - doc and drawer e2e tests, plus the wire snapshot.

   Queries compile to one SQL statement joining `datum_set_entries` with the reach CTE. A poll opens only the namespaces it needs.
   An `IsDoc` anchor takes a BigRepo `DocInterest` for the duration of each poll. Today's `Doc` scope holds one for the stream's life. BigRepo keeps an interest alive for `SYNC_INTEREST_GRACE` (30 s, `big_repo/interest.rs:12`) after its last holder drops it, which is longer than the 15 s hold cap, so a consumer that keeps polling keeps the doc in sync without any server-side session.
   `DatumSetStore::query(DatumSetQuery)` stays. It is the store's internal point lookup, used by `drawer.rs` and the `big_node` e2e, and is not a protocol surface.
3. **The drawer projection becomes a frontier-fed index (§11, one cursor).**
   - `drawer::State` consumes the datum-set frontier on the roster tags, with a progress revision stored in its own transaction. It replaces the rescan, `replace_roster`, and the direct `put_doc`/`put_child` writes from ops.
   - DrawerService ops neither write nor wait for the projection; their idempotency checks read the metadata doc.
   - Authorization answers from the projection only. The on-demand `sync_discovered` / `ensure_drawer` rescans on a miss go away (§10 flag).
   - A poll reads one frontier over its datum and roster tags. It waits for projection progress ≥ the page revision and stamps events with that revision.
   - This removes `recheck_drawer`'s `latest_revision()` stamp, `readable_in_drawer`'s per-tick full recompute, and the resume membership gap (§10).
   - Grant changes recheck only the observer's drawer, once per poll.
    - Admission of `metaDoc` and `childEntry` rows reads Keyhive; a late Keyhive change re-admits an entry on the next authority-change recheck or poll (§11 [corrected] item).
4. **Drawer meta docs (§4).**
   - **The op.** A DrawerService op creates a metadata-class doc under a drawer: create → `grant_doc_access(doc, meta_group, Admin)` → write the fixed slot `drawer.metaDoc/<kind> { docId }` → project.
     - It requires drawer Write and rejects self-doc drawers with `SelfDoc`.
     - It is ledgered like `create_collection`. The ledger row is also how the creator recognises a losing doc and retires it (revoke the meta group's grant) once the slot names another doc.
     - Name TBD, e.g. `CreateMetaDoc { drawer_id, kind, datums }`.
   - **Migration `002`:** `big_cabinet_drawer_meta_docs (drawer_id, kind, doc_id)`. Primary key `(drawer_id, kind)`, because one slot per kind holds exactly one row; indexed by `doc_id`.
   - **Projection.** The frontier-fed projection (item 3) projects the slot's merged value only. The row is admitted only when Keyhive shows the doc delegated to the drawer's meta group.
   - **Reach.** An `ancestors.sql` union branch: `SELECT drawer_id, 3 FROM big_cabinet_drawer_meta_docs WHERE doc_id = :target_id`. This is the same Admin bound the drawer's own metadata doc gets, so drawer writers reach the inventory at their drawer level.
5. **Self-doc children (§4, §10 flag): allowed, but never in the meta group.**
   - **`AddChild`.** For a self-doc child, `child_delegations` returns one delegation: the parent's docs group on the child doc, at `access`. There is no meta-group delegation, and no `meta_access` widening. Single-group children keep both delegations.
    - **Child entry.** `DrawerChildEntry` stays `{ access }`; no stated layout, and no derived shape column either (§4 [corrected]).
    - **Projection admission.** The projection checks the child's Keyhive delegations against the child's own descriptor layout: `selfDoc` means the parent's docs group is delegated on the child doc and the parent's meta group holds **no** access to it; `singleGroup` means both group-to-group delegations. Mismatch is not admitted. A self-doc doc that the parent's meta group can access is a defect and is refused loudly. Where the node cannot yet see the backing, the entry stays listed-but-pending and flips on the next recheck (§11 [corrected] item).
    - **Queries.** No change. `scoped_docs.sql`'s self-doc branch and `ancestors.sql`'s recursion keep reading `big_cabinet_drawers.descriptor_json`. Every row that reaches those branches is readable: `readable_in_drawer` filters to ≥ Read (`doc/query.rs:95`), a single-group child's descriptor is meta-group readable at parent Read+, and a self-doc child's descriptor is its content doc, readable at child Read+ through the docs-group delegation and projected as a drawers row by adoption. The earlier `shape`-column query rewrite rested on the false premise that Relay-level readers consult the child's layout; they never do.
   - **[flag]** A *root* self-doc drawer held only at Relay has neither a descriptor row nor a children row on that node, so `listDocs` at Relay returns nothing for it. That is today's behaviour too, and nothing on the big_blobs path needs it.
   - **Meta group invariant.** No meta group is ever delegated a self-doc doc:
     - `AddChild` (above) never does it;
     - `CreateMetaDoc` rejects self-doc drawers;
     - `AddDoc` only grants the docs group.
   - **Tests.** One test pins that a self-doc child is linked with docs-group-only delegation, appears in `scoped_docs` of the parent, and leaves the parent's meta group without access to it, including after a Relay-level invite on the parent. The existing `e2e/membership.rs` ~450 `SelfDoc` assertion on `AddChild` flips.
6. **[settled] Fold datum_ref edge extraction into the datum-set worker (§11, `HasReferenceTo`).**
   - Edges are written in `settle`'s transaction at the same revision as the entries.
   - datum_ref's cursors, parking and `settled` watch go away.
   - A boot backfill covers newly reference-declaring tags.
   - **[settled]** by the operator.

**Cabinet changes not required (verified):**
- **Keyhive visibility of meta docs already works.** Invite at Relay grants Read on the meta group (`principal.rs`, `meta_access`), child links do the same (`add_child.rs`), and the new op grants the meta group Admin on the doc.
- **No Keyline rosters.** The Automerge inline roster stays.

**Optional, defer unless wanted:**
- A branch reason on the `org.example.daybook.branch` datum (§5). Branching works without it.
- A DocsRepo guard reserving `org.example.daybook.drawer.*` tags for DrawerService (§4 flag). The projection-side checks in items 4 and 5 cover `metaDoc` and self-doc `childEntry`; the guard would also cover local `docEntry` writes.

**Docs to amend alongside:**
- Done; see §14.

## 13. Long polling, not server push

- **[settled] House rule: anything with a cursor is consumed by long polling.** The caller names a cursor, a page limit and a hold. The server answers at once if it has data, holds the request up to the hold if it has none, and returns the next cursor either way. Server push (subscription streams) needs a stated reason.
  - **Why:**
    - the consumer's own request rate is the flow control, with no shared buffer acting as back-pressure;
    - any interruption resumes from the cursor;
    - nothing has to be "opened" or kept alive on the server;
    - revocation is just the next request failing authorization;
    - the same shape covers paging a query and waiting for new data.
- **[flag] Where it is documented today:** only for big_sync.
  - ADR 012 §9, "Event delivery is a client-driven long-poll page, not a push stream" (`012-bigsync-reconciliation-strategy.md:691–775`): paging is the flow control; there are no unbounded responses and no shared-buffer back-pressure; WebSockets are "an optimization for transport latency, not a protocol requirement".
  - `docs/wiki/big_sync.md:157–170`.

  Nothing in `AGENTS.md`, `CONTRIBUTING.md` or the service-framework docs states the rule repo-wide, and ADR 009 (`009-datum-set-routing-frontier.md:149`) describes `ObserveDatums` as a subscription.
- **What exists, verified:**
  - **The pattern to copy is big_sync's.** `BigSyncRpcClient::replay_page` (`big_sync_core/rpc.rs:37–45`), with `ReplayPageRequest { targets[cursor], limit, hold_ms }` and a server cap of 15 s / 1024 events (`big_sync/rpc.rs:34–51`). The responder is `replay_page_round_with_update` (`big_sync/part_store.rs:497–870`): it answers at once with data, otherwise opens a reader and `select!`s it against the hold, and an expired hold is an ordinary empty page. Clients ask for a hold only when caught up.
  - **The local reader contract is already long-poll shaped.** `RevisionedStoreReader::next` / `KeyedFrontierReader::next` return a bounded page or wait inside the call (`big_sync_core/revisioned_store.rs:28–79`, `keyed_frontier.rs:113–122`; the SQLite implementation waits on a `Notify` at `big_sync/keyed_frontier/sqlite_read.rs:191–224`).
  - **Cabinet uses server push.**
    - `ObserveDatums` is a `stream` operation (`big_cabinet_core/doc.rs:399`). Over XRPC it becomes a WebSocket subscription (`service_utils_rs/xrpc/router.rs:568–702`). The handler spawns a `DatumPump` per subscription with a bounded `mpsc(64)` (`big_cabinet/doc/observe.rs:72–96`). That is a pull loop on the server re-exposed as a push stream.
    - The other `stream` operations are `ObserveDocAvailability` (`big_cabinet_core/doc/availability.rs:37`) and `ObserveCapability` (`big_cabinet_core/authority.rs:313`), which `ObserveDatums` subscribes to internally (`observe.rs:24–33`). Drawers and Blobs have none.
  - **XRPC has no server-side hold for request/response.** `x-timeout-ms` is a deadline, not a hold.
- **[settled] `ObserveDatums` becomes one unary long-poll op, and `QueryDatums` folds into it.**
  - **Shape:** `PollDatums { capability, predicate, datums, after, limit, hold_ms } → DatumPage { events, through, caught_up }`. `events` are the enter, datum and leave events of §11, each stamped with its revision. `through` is the cursor to send next. `caught_up` replaces `ReplayComplete`.
  - **Server side:**
    - It opens a frontier reader at `after` on the needed tags, reads one bounded page, and holds only when the page is empty (capped like big_sync).
    - It keeps no per-consumer task, channel or delivered set. It re-authorizes on every request, so a lost capability is the next answer, `Denied`, and the in-pump `ObserveCapability` subscription goes away.
    - A hold also wakes on a capability change on the node, so a revocation cannot hide behind a 15 s hold.
  - **Stateless is correct.** §11 already makes every event a function of the page it came from:
    - a datum entry is re-evaluated against the predicate;
    - a roster entry yields enters and leaves;
    - leaves are idempotent.

    The per-session delivered-set dedupe was the only state, and it is not needed for correctness.
  - **`QueryDatums` is a poll from `after = 0` with `hold_ms = 0`, paged until `caught_up`.** A frontier replay from 0 yields the latest entry per key, i.e. the current state. One operation then covers paging and waiting. **[flag]** A multi-page query is not a point-in-time snapshot: later pages may carry newer revisions. That is the cursor contract, not a defect: the consumer continues from `through` and converges.
  - **In-process consumers** (the big_blobs pin worker) use the same op through the Direct client. There is no separate Rust-only subscription path.
- **[flag] XRPC admission.** Unary handlers take a permit from the router's 64-slot semaphore (`router.rs:241–251`), so a held poll occupies one for up to the hold, as a WebSocket subscription does today for its whole life. Held polls need their own admission class or cap, as big_sync caps in-flight handlers (`big_sync/rpc.rs:81–88`).
- **[open]** `ObserveDocAvailability` and `ObserveCapability` have no cursor today. Converting them needs a revision on availability and capability state (a capability status `Live`/`Retracted` per id, or availability per doc). Proposal: give each a revision and long-poll it, as a follow-up after `PollDatums`. They are not on the big_blobs path once `PollDatums` re-authorizes per request.
- **[settled] Where the house rule is written down:** a short repo-wide ADR, "Cursor consumption is long-poll", that generalises ADR 012 §9 to every service surface and states the exceptions bar. Add a one-line pointer in `CONTRIBUTING.md`, and amend ADR 009's `ObserveDatums` paragraph.

## 14. Where the decisions landed, and the work items

Amendments made alongside ADRs 023–026:

- ADR 019: self-doc children, the child-entry admission check (delegations vs the child's own descriptor), meta docs and their slot, the publicness contract, and a pointer to ADR 024 for the frontier-fed projection.
- `docs/wiki/drawer.md`: the same rules in the wiki's terms, plus the relay walk and inventories.
- ADR 009: §6 and §8 now describe `PollDatums` and the folded edges, not a subscription and per-tag cursors.
- ADR 012 §9: points at ADR 023 as the repo-wide rule.
- ADR 001: inventories are drawer meta docs; the `blob_inventories` group and the default inventory doc are withdrawn; partitions use drawer groups.
- ADR 003: §19's key documents, `keyScope`, `grp:` domains, the staging steps and `Blob.urls` resolution are withdrawn; the JWK is co-located.
- ADR 005: the account doc names one root sponsorship drawer instead of `S_docs`, and adoption walks the drawer closure and reads inventories.
- ADR 034: an optional branch reason.
- `CONTRIBUTING.md`: a pointer to ADR 023 for cursor consumption.
- [big-blobs-replacement-plan.md](./big-blobs-replacement-plan.md): the drawer-native observation and permission sections point at ADRs 023–026.

Work items, in dependency order. Nothing here is authorized for code yet.

**Cabinet (ADR 024, ADR 025):**

1. `big_cabinet_core::query`: the predicate, the datum selection, the wire spellings, and the closed-union refusal of unknown clauses.
2. `PollDatums` replaces `QueryDatums` + `ObserveDatums`: delete `DatumSelector`, `ObserveScope`, `DatumObserved`, `AccessLost`, the `stream` operation and `DatumPump`; migrate `doc/query.rs`, `doc/observe.rs`, `protocol_wire.rs`, the doc and drawer e2e tests and the wire snapshot.
3. Fold datum_ref edge extraction into the datum-set worker: `SchemaRepo` on `DatumSetStore`, extraction in `prepare`, edges written in `settle`'s transaction, the `big_cabinet_datum_set_ref_tags` boot backfill, and the deletion of `datum_ref`'s cursors, parking and `settled` watch.
4. Frontier-fed drawer projection: roster-tag consumption with a progress revision, the `metaDoc` slot projection, and the delegation admission checks (`metaDoc` delegation, fixed-slot value, `childEntry` delegations vs descriptor). Late-Keyhive re-admission follows the existing authority-stream recheck discipline (§11 [corrected] item); no open question remains. Also: deletion of the rescan, `replace_roster`, the op-side `put_doc`/`put_child`, `sync_discovered`/`ensure_drawer`/`discover_drawer` and `PolicyOperation::Refresh`.
5. Migration `002`: `big_cabinet_drawer_meta_docs` and the `ancestors.sql` meta-docs reach branch. No `shape` column and no `scoped_docs.sql`/`ancestors.sql` rewrite: the queries keep their descriptor joins, which every row that reaches them can read (§12 item 5).
6. `CreateMetaDoc` op with its ledger row and the loser-retirement path; `AddChild` allowing self-doc children with docs-group-only delegation (flipping `e2e/membership.rs` ~450).
7. Read-your-writes wait surface (ADR 024, still open).
8. XRPC admission class for held polls (ADR 023 rule 5).

**big_blobs (ADR 026), after 1–4:**

9. Datums and slots: `cipherBlob/<blob key>` and `jwk/<blob key>`, no `Blob.urls` mutation, in-doc `keyRef`.
10. Per-drawer partitions and their permission seeding; drop the group-part gate.
11. The pin worker over `PollDatums` with `InDrawer { transitive: false }`: delta-driven maintenance, re-seed on an inventory-slot change, relay-walk inventory union.
12. Branch-on-read-only path, with the optional `forkReason`.
13. Retention ledger and single store owner (plan §4.5–§4.6).
14. `big_node` wiring and lifecycle; delete `daybook_core::blobs`.

**Follow-ups outside this PR:** revisions for `ObserveDocAvailability` / `ObserveCapability`; relay serving of a lone self-doc drawer; plaintext pins for public drawers; relay transitive adoption.

## 15. Critique of the first cut (`afe09255`, 2026-11-01)

Operator direction after the first drawer-native blob cut. This section does not amend [ADR 019](../adrs/019-cabinet-drawers-and-selective-mirroring.md) or [ADR 026](../adrs/026-drawer-native-blobs.md). Where it and an ADR disagree, the ADR still wins for code. Nothing here authorizes an implementation.

The old header said implementation was paused and that nothing was authorized for code. The pause sentence is stale for `afe09255` (per-inventory ciphertext parts, the pin worker, self-doc children). The authorization sentence is not: further code is still not authorized. §14 is the pre-cut backlog, not the current gap list.

### Acceptance bar

- **[settled] Mirror a drawer from another node and get every blob the mirrored inventories name**, including blobs named by child drawers. A local `node.grant` and a SQL CTE are not that test.
- **[flag] No two-node Keyhive transitivity test exists.** `DRAWERS=3` in `src/big_cabinet/drawer/e2e/model.rs` can form A→B→C, and `effective_bob` walks the child graph, but nothing asserts depth ≥ 2 and nothing asserts that blob bytes arrived because a remote inventory named them. `non_transitive_poll_includes_self_doc_children_but_not_child_drawer_docs` covers the local predicate only.

### Already solved for a ciphertext-only relay (do not re-open)

- Ciphertext objects go into `inventory_part(inventory_doc)`, keyed by the inventory doc id (`src/big_blobs/workers.rs`, `inventory_part`, `distribute_parts`). Membership is seeded from that doc's Keyhive closure (`src/big_blobs/permissions.rs`, `seed`).
- Pins omit the plaintext digest (ADR 026 §5).
- A self-doc child is not granted to the parent's meta group (ADR 026 §6, ADR 019 "Self-doc children").
- A denied read parks and does not shrink retention. On the writer, absence is not deletion.

Big Sync already refuses a fetch when the asker has fetch access on none of the object's parts (`PartScope::FromObject` in `src/big_sync/part_store/sqlite.rs`). `is_fetch_access` treats Relay as fetch (`>= Access::Relay`). That is any-part fetch access, not the serve rule below.

### Parts: one part per inventory, not per drawer

- **[settled] Reject one group part per drawer.** One Big Sync part per inventory. A node chooses which inventories to mirror. Every blob a drawer retains is written into all of that drawer's inventories; the representation differs per inventory (ciphertext in a relay inventory, plaintext in a reader inventory).
- **[flag] ADR 026 §7 still wins for code, and the cut implements it.** `plaintext_part(drawer)` is keyed by drawer id and seeded from the docs group (`permissions::seed_drawer`, called from `distribute_parts`). That is the rejected group part. The ciphertext side is already per inventory doc, which matches this direction.
- **[flag] The fixed `metaDoc/blobInventory` slot names only one inventory per drawer.** Multiple inventories need a naming and discovery change. **[open]** Whether that is multiple kinds, keyed slots, or references on separate metadata docs.

### Serve gate

- **[settled] Serve an object only if the asker has Read or stronger on the inventory the request arrived on.** The pin names the representation; the inventory doc is the access object. The gate is access level, not the word `Read`: a writer or admin of that inventory also passes. Not "any part that contains the digest", and not "any inventory that names it".
- **[flag] Unimplemented.** `permitted_parts` for `PartScope::FromObject` unions every live part of the object the principal can fetch, and Relay counts. A principal who can fetch one part of an object can be served it without a check that the request named the inventory whose part they hold.

### Two metadata groups

- **[settled] Even if a relay obtains the plaintext inventory, it still cannot pull the drawer's docs.** No inventory→docs delegation, and `scoped_docs` must not reach those docs through that inventory doc.
- **[settled] Therefore a plaintext inventory must not sit in the relay-readable meta group.**
- **[proposed] Two metadata groups.** The existing meta group stays relay-readable and holds the ciphertext inventory and other public metadata. A second group, not widened from Relay to Read, holds the plaintext inventory and future sensitive metadata.
- **[open] Where the pointer to the plaintext inventory may live.** It cannot be a `metaDoc` slot on the relay-readable metadata doc. ADR 019 "Publicness" says everything granted Read to the meta group, including every meta doc, is visible to Relay-level participants. A slot on a doc the second group can read is the obvious place; that doc does not exist.
- **[flag] Conflicts with ADR 019 and with `CreateMetaDoc`, and neither is amended.** There is one meta group. An invite at Relay grants Read on it (`meta_access`). `create_meta_doc` always `grant_doc_access(..., meta, Admin)` (`src/big_cabinet/drawer/create.rs`). ADR 026 §8's relay walk, and §7 of this log, assume that same Read reaches `blobInventory`.

### Self-doc blob mirroring

- **[settled] Mirroring a self-doc drawer's blobs is required.** "Put it in a single-group drawer" (ADR 026 §6, proposed) is not a substitute for the acceptance bar.
- **[proposed] Working assumption: a single-doc drawer has no Relay-level access.** The operator finds "no Relay on a single-doc drawer" odd. Record it as an assumption, not a rule.
- **[flag] Do not encode "self-doc forbids Relay" as the serve rule.** The gate is Read or stronger on the naming inventory. Forbidding Relay is only the absence of a relay-readable naming doc. The cut does not mirror a root self-doc's blobs: `src/big_node/blob_boot.rs` creates `blobInventory` only for the boot drawer and configures workers with `drawers: vec![present_id, originated_id]`. A parent's non-transitive poll includes that parent's self-doc children (`poll_drawer`, `transitive: false`). It does not cover a root self-doc that is not one of those two drawers and has no inventory of its own.

### Successor

- **[settled] A successor datum only resolves the new drawer id.** It is not a grant, not a child copy, and not a blob move.
- **[open] Where that pointer lives.**
- **[flag] No successor datum exists in drawer code.**

### Pin versus part, both orders

- **[open] The mirror protocol, in both orders:**
  1. A pin arrives before the object is advertised in a part: park the want. Absence is not deletion.
  2. An object is seen in a part before any inventory pins it: park it, do not serve it onward, do not delete it. Pull it when a later inventory update names it.

  Removal and not-yet-seen stay distinct.
- **[proposed] Pending wants and seen-but-unpinned objects are keyed by `(inventory, representation)` and survive restart.** A pin in one inventory must not unlock an object merely because the same digest was seen in another inventory's part.
- **[flag] Writer `reconcile_parts` is not that protocol.** Unless `preserve`, it removes an object from `inventory_part(document)` when the object drops out of that writer's pin set (`src/big_blobs/workers.rs`). That is the writer's own ledger. A big_sync keyhive-membership retry storm is a different race and is intentional (AGENTS.md); do not "fix" it as if it were this one.

### Still not the acceptance bar

- Workers poll non-transitive membership of the drawers they are handed, plus one `PollDatums` per inventory (`poll_drawer`, `poll_inventory`). They do not walk a remote sponsorship closure.
- `retention::inventories_for` joins `big_cabinet_drawer_docs` to `big_cabinet_drawer_meta_docs` on kind `blobInventory`. That is the local projection, not a mirrored grant.
- No sponsorship or relay walk is implemented. §7 and ADR 026 §8 stay proposed.
- The roster is still `DrawerRoster::inline()`. ADR 019's registry-and-shard design stays deferred.
- Concurrent-creator retirement and re-seed are specified (ADR 026 §5) and not implemented.
- No depth ≥ 2 child-chain assertion, as under the acceptance bar.

## 16. Resolution proposal for the first-cut critique (2026-11-01)

Except where a requirement is restated as **[settled]**, this section is **[proposed]**. It resolves the design questions in §15; it does not amend ADR 019 or ADR 026 and does not authorize code. "Read+" below means Read, Edit/Write, or Admin. A representation id is the digest named by a pin.

### 16.1 Metadata roots and named inventories

A grouped drawer has three groups and two metadata roots:

- the existing docs group;
- the existing meta group, now explicitly the **public metadata group**;
- a new **sensitive metadata group**;
- the existing descriptor-bearing drawer doc is the **public metadata root**;
- a separate **sensitive metadata root doc** holds sensitive metadata references.

The drawer descriptor names the sensitive group and sensitive root. At drawer creation, the sensitive metadata group gets Admin on the sensitive root, analogously to the public metadata group's grant on the public root; principal access is attenuated by membership level. The sensitive root's existence and doc id are public discovery facts; its contents, including plaintext inventory ids, are not. Hiding even the existence of sensitive metadata is not a requirement. The sensitive root has no descriptor, roster, doc entries, or child entries.

Access and delegation follow one table:

| Principal's drawer access | Docs group | Public meta group | Sensitive meta group |
| --- | --- | --- | --- |
| Relay | Relay | Read | no membership |
| Read+ | corresponding level | corresponding level | corresponding level |

Child links apply the same table recursively: parent docs → child docs and parent public meta → child public meta as today, plus parent sensitive meta → child sensitive meta only for Read+. Thus a Relay participant can discover ciphertext inventories transitively without receiving sensitive metadata, while a Read+ participant can discover both.

- Replace the single `metaDoc/blobInventory` slot with fixed **kind-and-name** slots: `org.example.daybook.drawer.metaDoc/blobInventory/<name> { docId }`. Concurrent creators of one `(kind, name)` still write the same key, preserving the LWW winner and loser-retirement design.
- Ciphertext inventories are canonical on the public root and plaintext inventories on the sensitive root, including additional names. The conventional names are `ciphertext` and `plaintext`; a name is a discovery and selection convention, never an authorization rule.
- Inventories are opaque pin sets with no representation config. Writer policy chooses the digest to pin; metadata roots and names do not assert or validate representation. Inventories are independently lazy-created; a drawer need not have both conventions.
- `CreateMetaDoc` requires Write+ on the drawer, takes root, kind, and name, grants the new doc Admin to that root's group, then writes the reference on that same root. Projection admits a grouped-drawer reference only when that root's group has backing Admin delegation, with delegation before publication.
- The plaintext convention uses sensitive metadata. Deliberately sharing an inventory through public metadata discloses its opaque pins and bytes; a sensitive alias cannot undo that disclosure. Neither metadata roots nor inventories are docEntries. There is no inventory → docs-group delegation. Drawer authority may reach a meta doc for Cabinet operations, but inventory access never creates drawer authority, and `scoped_docs` remains roster/child based.
- Extend the existing `singleGroup` layout with the sensitive group and root rather than adding a second layout with the same meaning.

### 16.2 Self-doc drawers use their immediate parents' inventories

The standalone companion proposal is withdrawn. A self-doc drawer has no groups, metadata documents, companion inventory, or inventory pins of its own. Its readable content already names its blobs; introducing companion metadata would require content access for discovery or reverse discovery, plus coordinated ACL machinery without a corresponding requirement.

Each immediate containing grouped drawer inventories the self-doc's blobs through its own writer targets. Multiple parents retain independent inventory claims. A self-doc child never enters either parent metadata group, and transitive mirroring covers its blobs through its immediate parent's selected inventory, not through a child inventory.

Inventory-based mirroring of a parentless self-doc is outside this scope. Selecting one as an inventory-mirror root must not report successful empty coverage. The universal inventory Read+ gate is unchanged.

### 16.3 Mirror selection and transitive coverage

**[settled]** Coverage is the union of pins in the selected inventories discovered from the mirrored root, including descendants. Cabinet projections guide discovery; actual access comes from Keyhive delegations.

- A mirror policy selects an inventory name for a root drawer. That name selection is inherited down the official `childEntry` closure unless the node overrides it for a particular drawer. The node therefore chooses which inventory to mirror without mirroring every inventory it can read.
- Maintain a durable, cycle-safe discovery state keyed by drawer id and inventory id, including `part_id → inventory_id`. Re-evaluate on roster changes, inventory-slot replacement, and Keyhive access revisions.
- A newly linked child expands coverage under the inherited or overridden selection. Removing the last selected path stops new mirror admission from that inventory; it does not implicitly delete retained bytes or alter shared writer targets.
- A grouped child with missing metadata, selected inventory, access, pins, or bytes is **pending/incomplete**, never successful empty coverage. A self-doc child requires no child inventory; its coverage is evaluated through the containing parent's selected inventory.
- Pin maintenance remains non-transitive: each inventory pins its own drawer's docs and self-doc children. Only mirror discovery is transitive.

The acceptance matrix is:

1. **Two-node A → B → C:** grant only the root on the owner, mirror on the other node, and assert actual bytes arrive for pins from root, child, and grandchild inventories without direct child invitations or local standing grants. Run this for both Relay/ciphertext and Read+/plaintext access.
2. **Dynamic closure:** add a deep child and replace an inventory slot after initial mirroring; new pins arrive. Restart resumes discovery. Unrelated readable inventories are not mirrored.
3. **Self-doc child:** its blob is named by the parent inventory and arrives; the parent meta group still has no access to the self-doc.
4. **Parentless self-doc:** no companion metadata or self-doc pins are created. Inventory-mirror coverage reports this root mode as unsupported/incomplete rather than successful empty coverage. Self-doc child coverage uses every immediate containing grouped drawer's own inventory.
5. **Negative and pending:** missing inventory or bytes reports incomplete. Revocation stops new authorized fetching and is not treated as a byte-deletion signal.
6. **Both race orders:** for pin-first and object-first, prove same-inventory admission, restart persistence, no cross-inventory unlocking for an identical digest, and no stale announcement after the last applicable claim is deselected, after removal or revocation, or after a late completion. A writer-target claim alone never triggers a mirror fetch.
7. **Serving isolation:** with the same representation in inventories A and B, Read+ on A does not authorize a request bound to B; unauthorized requests disclose neither availability nor membership. Read+ on a plaintext inventory does not authorize drawer content docs, and Relay cannot read the sensitive root or plaintext inventory.
8. **Successor:** resolution returns the new id for both layouts; no grant, children, blobs, or mirror selection move implicitly.

### 16.4 Representation distribution and the two orderings

`inventory_part(inventory_doc)` is the only blob-part mapping, for ciphertext and plaintext alike. The drawer-keyed `plaintext_part` and docs-group seeding are withdrawn from this proposal.

Writer-side pin maintenance targets every inventory of the directly containing drawer that the writer is authorized to maintain, independently of local mirror selection. Self-doc children use the parent's writer targets. Every retained representation goes to every writer-target inventory of the corresponding representation. Mirror selection controls only local discovery and admission; it never removes pins or objects from a shared inventory. Different inventories may have different audiences, and global possession of bytes never substitutes for admission into a particular inventory.

Mirror state is durable and keyed by **`(inventory_id, representation_id)`**:

- writer-target and mirror-selection claims;
- `pin_observed` for this inventory;
- `object_observed` or admitted for this inventory's part;
- source/payload hints and fetch/completion state.

Local production counts as producer object admission. **Fetch eligibility** requires a current mirror-selection claim, `pin_observed`, and object observation in that inventory's part; a writer-target claim alone never causes a fetch. **Serve/re-advertise eligibility** requires a current writer-target or mirror-selection claim, `pin_observed`, object admission, and available bytes. Existing local bytes may avoid a download only after the mirror pair becomes fetch-eligible.

1. **Pin first:** record a want. The absent object is pending, not deleted.
2. **Object first:** record a quarantined candidate. Do not pull, re-advertise, or serve it until a pin in the same inventory arrives. That later pin schedules the fetch only with a current mirror selection, even if the part cursor was already consumed.

An object observed through one inventory never completes a pair for another. Explicit pin or object removal clears only that fact for that pair. Mirror deselection removes fetch eligibility and removes serve/re-advertise eligibility only when no writer-target claim remains. Bare writer-target or mirror-selection claims do not retain bytes. Retention claims come only from local ownership, currently byte-requiring admitted pairs, and quarantined object-observed pairs with local bytes. Deselection or absence of a pin does not delete quarantined bytes. Authoritative object removal clears object-derived retention claims; physical deletion occurs only when no retention claims remain.

- Persist state transitions before acknowledging the inventory or part cursor.
- On restart, seed every locally maintained or mirrored inventory's ACL before serving, resume its inventory poll and part subscription, and enqueue every fetch-eligible-but-incomplete mirror pair.
- A late fetch completion rechecks mirror selection, pin, object state, and the authorization under which the fetch was initiated before part membership or announcement. Revocation does not itself authorize physical byte deletion.
- Denied, pending, undecodable, or incomplete reads preserve prior state. Only a complete authoritative inventory snapshot justifies pin-removal transitions, and only a complete authoritative part snapshot or equivalent authoritative removal event justifies object-removal transitions.
- Writer targets and mirror selections are separate relations. Writer-target changes drive pin maintenance and producer-side part distribution; mirror deselection changes only local mirror state and never shrinks shared writer targets.

### 16.5 Exact-inventory serving and doc isolation

**[settled]** Serve only when the authenticated asker has Read+ on the exact inventory whose part carried the request. The pin names the representation; the inventory doc is the access object.

- Every blob replay, bucket, summary, and byte request is **part-bound**. Resolve that part through the locally maintained or mirrored inventory-part registry and check Read+ on that exact inventory. Then check that the `(inventory, representation)` pair currently satisfies §16.4's serve/re-advertise rule and that bytes are available.
- Object-only and multi-part union requests are not valid blob requests. The binding must survive through the byte-fetch provider; a hash-only provider route cannot implement this rule and must not be used as a fallback.
- A principal whose only grant on the inventory is Relay is denied. A relay participant normally passes for a ciphertext inventory because the public meta group gives it Read on that doc.
- Check authorization on every request, including after a byte download was initiated by another task. Do not change unrelated document-sync `is_fetcher` semantics globally.
- An unauthorized request discloses neither object availability nor part membership.

Read+ on a plaintext inventory can authorize its plaintext blobs, but it still grants no drawer content docs. There is no inventory → docs delegation, and exact-document inventory polling stays separate from `InDrawer` content polling.

**[flag]** The current object-only `FromObject`/`AnyOf` union, Relay-inclusive fetch predicate, hash-only byte provider, `sync_obj` download-before-admission, and retention announcement from global digest possession all violate this proposal and must be addressed together when implementation is authorized. The duplicate logic in `big_repo/store/sqlite.rs` is in scope as well as `big_sync/part_store/sqlite.rs`.

### 16.6 Successor pointer

Use one fixed singleton on the **old** descriptor-bearing doc:

```jsonc
"org.example.daybook.drawer.successor/main": { "drawerId": "zNewDrawer" }
```

For a grouped drawer that is the public metadata root; for a self-doc drawer it is the content doc. It is neither a child entry nor an inventory/meta-doc slot. Explicit resolution may follow a cycle-checked chain, but only returns the new drawer id. It creates no grant, copies no roster or children, moves no blobs, and never silently retargets an existing mirror policy. Access to the successor is established independently; a user-confirmed mirror migration may be built later as a separate operation.

With this section, §15 has no unaddressed design question. What remains before code is operator approval of these proposals and the corresponding ADR 019/026 amendments.

## 17. Implementation handoff: settled boundaries, phases, and gates

This section is the active implementation scope for the §16 correction. It records the design clarifications made after reviewing the first cut. Implementers should not silently choose a different boundary: either satisfy the gate or amend this section and ADR 019/026 first.

### 17.1 Representation has one owner

An inventory is representation-agnostic: it is a document of `blobPin/<digest> { lengthOctets }` entries and does not declare whether those digests name plaintext or ciphertext. The **metadata class that references it** supplies that meaning:

- a named inventory referenced from the public metadata root has the public audience and is conventionally selected for ciphertext;
- a named inventory referenced from the sensitive metadata root has the Read+ audience and is conventionally selected for plaintext;
- the conventional names `ciphertext` and `plaintext` aid selection, but the root — not the spelling and not an inventory-local field — determines the audience class; writer and mirror policy determine which representation is placed there;
- a missing, denied, or unavailable reference/inventory is pending or incomplete, never an empty usable inventory.

Cabinet owns this generic classification through the root, kind, name, target doc id, and backing delegation. `big_blobs` consumes the class when deciding which representation to pin and distribute. There is no `MetaRepresentation` on `DrawerMetaDoc` and no duplicate inventory config assertion to reconcile. The optional inventory config remains available for future representation-independent policy, but it is not required to state plaintext versus ciphertext.

An authorized public reference and backing delegation are intentional sharing of the target inventory, even when another public or sensitive slot names the same document. Such aliases share one pin set and the inventory's effective Keyhive audience; references do not partition its contents or retained history. Do not reject cross-root aliases, suppress one root's writer claims, or impose an inventory-wide representation class. Each reference keeps independent writer claims, and opaque pins may name intentionally unencrypted or publicly decryptable bytes. Sensitive-only non-disclosure guarantees do not override an explicit authorized public grant.

### 17.2 What “Read+ on the inventory” means

“Read+” means the authenticated peer's **Keyhive standing on the exact inventory document is Read, Write/Edit, or Admin**. It does not mean a portable `big_blobs/read` capability and it does not mean Read on the drawer's content docs.

A drawer-level Relay participant normally gets Read on the public metadata group and therefore Read on a public ciphertext inventory. It passes the ciphertext inventory gate even though its docs-group standing is Relay. It gets no membership in the sensitive metadata group and therefore cannot discover or read a plaintext inventory through the official drawer path. Read+ on an inventory never grants access to drawer content docs because there is no inventory-to-docs delegation.

### 17.3 Stock iroh-blobs cannot perform the exact-inventory check

The current iroh virtual provider is called as `reader_for(hash)`. It receives the digest but no authenticated peer, requested part, inventory id, or capability. It can answer “bytes/not found”, but it cannot decide whether this requester has Read+ on the inventory whose part named the digest. Registering or removing a hash globally is likewise not an audience-specific authorization mechanism.

Big Sync can authorize and filter inventory/part metadata before scheduling a transfer, but that alone does not authorize a direct stock iroh-blobs GET by hash. Therefore exact-inventory authorization cannot be honestly implemented as a check inside `CipherBlobProvider` or as a late hash-only fallback.

The accepted interim boundary is intentionally weaker:

1. Keyhive access controls discovery of inventory documents. Relay participants can read public metadata and ciphertext inventories, but have no route to the sensitive root or plaintext inventories.
2. A plaintext digest is disclosed through the sensitive inventory only to Read+ participants. The stock hash endpoint does not perform a second inventory authorization check; protection against an unauthorized relay fetching plaintext therefore relies on the relay never learning the plaintext digest through the official protocol. This is explicitly security through obscurity/unguessable-content addressing, accepted for the current phase rather than misdescribed as exact-inventory authorization.
3. `big_blobs` and Big Sync must still avoid leaking sensitive inventory ids, pins, plaintext digests, part membership, or availability through public metadata, logs, summaries, object-only events, or ciphertext inventory payloads.
4. A future hardened transport can carry the exact inventory/part identity and recheck Read+ before streaming bytes. That is a defense-in-depth follow-up, not a blocker for the current plaintext inventory implementation under this threat model.

Accordingly §16.5's exact-inventory byte-serving rule is deferred as hardening. Current acceptance is authority-gated inventory discovery plus strict non-disclosure of sensitive inventory contents. Metadata and pin authorization remain real Keyhive checks; only the final hash-based byte request lacks requester-aware authorization.

### 17.4 Phase A — Cabinet metadata roots and successor

Deliver §16.1 and §16.6, with Cabinet kept representation-agnostic.

Required design properties:

- three groups and two roots use the §16.1 access table;
- Relay gets no sensitive-group route, including transitively;
- kind-and-name slots are projected per root;
- delegation precedes reference publication and projection checks the backing delegation;
- `DrawerMetaDoc` carries no blob-specific representation field; the public/sensitive root is the inventory class;
- successor resolution is cycle-checked and moves no authority, roster, mirror policy, or blobs;
- authorize caller Read on each drawer before reading its successor datum. For A→B→C with access only to A, return `SuccessorResolution::AccessRequired { drawer_id: B }`, not B as a terminal successor. A's readable pointer already discloses B; the service must not inspect B, reveal C, or reveal B's participation in a cycle. Cycle errors may mention only drawers already authorized and visited.

Verification gate:

- focused `big_cabinet_core` datum/serde/admission tests;
- focused `big_cabinet` drawer membership, delegation, lifecycle, projection, and successor tests;
- tests for Relay versus Read+ on a parent→child→grandchild chain, including absence of sensitive membership for Relay;
- crash/replay tests for sensitive-root and metadata-doc creation;
- successor A→B→C tests with access only to A return access-required B without inspecting B or leaking C; replacing B's hidden pointer with a cycle does not change the result. Once B is authorized, traversal may inspect B; cycle errors name only authorized, visited drawers;
- `cargo clippy --all-targets --all-features -p big_cabinet_core -p big_cabinet`;
- `cargo x guardrails api --all --check` after intentionally blessing and reviewing the interface break.

### 17.5 Phase B — Inventory-only representation distribution

Deliver §16.4's writer side before remote mirror discovery.

Required design properties:

- `inventory_part(inventory_doc)` is the only blob-part mapping;
- delete `plaintext_part(drawer)` and docs-group part seeding;
- writer targets are every directly containing drawer inventory the writer may maintain, grouped by the public/ciphertext or sensitive/plaintext root that references each inventory;
- self-doc children use the containing drawer's writer targets;
- writer targets and mirror selections are different durable relations;
- missing, denied, or unavailable inventory references cause no destructive reconciliation.

Verification gate:

- one drawer with multiple named inventories receives only the configured representation in each;
- the same digest in two inventories remains two inventory-specific claims;
- self-doc child distribution uses the parent inventories;
- removal from one inventory does not remove another inventory's object or pin;
- restart preserves writer-target effects;
- focused `big_blobs` nextest and `cargo clippy --all-targets --all-features -p big_blobs`.

### 17.6 Phase C — Durable mirror state and both orderings

Deliver the §16.4 mirror state machine, still without claiming transitive coverage.

Required design properties:

- durable state is keyed by `(inventory_id, representation_id)`;
- state distinguishes selection, pin observation, object observation/admission, bytes, fetch, and completion;
- cursor acknowledgement follows the durable state transition;
- pin-first parks a want; object-first quarantines a candidate;
- neither ordering fetches or serves until the same-inventory pair is eligible;
- no digest observed in inventory A unlocks inventory B;
- restart and late completion re-evaluate current selection, pin, object, and authorization facts;
- missing/denied/incomplete observations never masquerade as removal.

Verification gate:

- pin-first and object-first tests, each across restart;
- identical digest in two inventories cannot cross-unlock;
- deselection, explicit removal, revocation, and late completion leave no stale advertisement;
- a writer-target claim alone never starts a mirror fetch;
- retention remains until all actual byte-owning claims are gone.

### 17.7 Phase D — Root selection and transitive coverage

Deliver §16.3 and the primary product acceptance test.

Required design properties:

- a mirror policy selects an inventory name at a root and inherits it through official child entries, with explicit per-drawer override;
- discovery is durable and cycle-safe, and records `part_id → inventory_id`;
- discovery reacts to child-link, inventory-slot, and Keyhive-access changes;
- only mirror discovery is transitive; writer pin maintenance remains per containing drawer;
- missing metadata, selected inventory, access, pin state, or bytes is surfaced as incomplete rather than successful empty coverage.

Verification gate:

- two real nodes, owner A→B→C, mirror granted only A, and actual root/child/grandchild **ciphertext bytes** arrive without direct child invitations;
- adding a deep child and replacing a selected inventory after mirroring brings in new bytes;
- restart resumes discovery without a boot-only full-scan dependency;
- unrelated readable inventory names are not mirrored;
- self-doc child bytes arrive through the containing parent's selected inventory;
- revocation stops new admission without treating retained bytes as an implicit deletion.

This phase is the minimum gate for saying “drawer mirroring gets all ciphertext blobs.” A local SQL reach test is not a substitute.

### 17.8 Phase E — Withdrawn standalone companion proposal

Do not implement self-doc metadata documents, companion inventories, reverse discovery, or self-doc inventory pins. Each immediate containing grouped drawer inventories its self-doc children's blobs. Parentless self-doc inventory mirroring is outside this scope; it must not be reported as successful empty inventory coverage. This replaces the earlier paired-share/backfill/revocation requirement.

### 17.9 Phase F — Optional hardened part-bound transfer

This defense-in-depth phase is not a blocker for plaintext inventory use under §17.3's accepted threat model. It replaces security-through-obscurity at the final hash fetch with requester-aware authorization when the product threat model requires it.
When implemented, it delivers §16.5 as follows:

Required design properties:

- replay, summary, object request, byte stream, and completion preserve one exact part/inventory binding;
- the responder resolves the part to an inventory and checks current Read+ standing on that exact inventory;
- no object-only, `AnyOf`, `FromObject`, or hash-only fallback is valid for this blob path;
- the byte provider/store remains storage machinery behind the authenticated protocol, not the authorization boundary;
- direct stock iroh-blobs serving is unavailable to untrusted peers;
- plaintext inventory access grants no content-doc access.

Verification gate:

- with the same digest in inventories A and B, Read+ only on A cannot request through B;
- Relay-only on the exact inventory is denied;
- unauthorized requests reveal neither availability nor part membership;
- authorization revoked during a transfer causes completion to fail closed;
- Read+ plaintext transfer works while the same principal still cannot read drawer docs solely through inventory access;
- focused Big Sync/Big Repo authorization tests plus a real two-node byte-transfer test.

### 17.10 Final integration gate

Before this stack is a dependency for plugs or relay product work:

- ADR 019 and ADR 026 describe the implemented three-group, named-inventory and phased-transfer design; stale “per drawer plaintext part” text is removed;
- the replacement plan and public API snapshots match the code;
- the full §16.3 acceptance matrix is accounted for as passing, explicitly gated, or deferred without being claimed complete;
- targeted nextest and clippy pass for `big_cabinet_core`, `big_cabinet`, `big_blobs_core`, `big_blobs`, `big_sync`, `big_repo`, and `big_node` as affected;
- run `cargo x guardrails lint`, `cargo x guardrails api --all --check`, and `cargo x guardrails doctor`;
- run scoped `rustfmt --check` and source-validity checks for changed files in the default devshell, excluding generated SQL bindings; this workstream does not run `nix flake check`;
- review `cargo x guardrails plan <changed files...>` and run the affected narrow tests rather than substituting a blind full-workspace test;
- legacy `daybook_core::blobs` ownership is removed only after its remaining consumers have migrated and the new node end-to-end path passes.

### 17.11 Transport identity versus local storage representation

A peer serving a blob does not need to classify its bytes as plaintext or ciphertext. The transport object is an opaque digest plus bytes. Inventory documents are likewise opaque pin sets. Public/sensitive roots control who can discover an inventory; they do not change the blob transport protocol or require a relay to interpret the representation.

Representation matters at the writer and mirroring edges:

- A writer policy decides which digest to pin into each named inventory. By convention the public `ciphertext` selection pins the encrypted representation and the sensitive `plaintext` selection pins the plaintext representation, but the inventory format is identical.
- A relay or opaque mirror may retain and serve exactly the bytes named by its selected inventory without understanding them.
- A Read+ node selecting a ciphertext inventory may download ciphertext, decrypt it into locally stored plaintext, and register a virtual ciphertext provider. It then retains one physical plaintext copy while continuing to satisfy requests for the inventory's ciphertext digest by deterministic virtual serving. This is a local disk optimization; it does not rewrite the remote inventory from ciphertext to plaintext and does not require the serving path to know an audience-wide representation class.
- A node selecting a plaintext inventory may store and serve those bytes directly. Again, the store sees only an opaque digest.
- Mirror policy must record enough local provenance to rebuild a virtual provider after restart: selected inventory, advertised digest, resulting local plaintext digest, accepted key/framing source, and the retention claims that keep them alive. That provenance is local durable state, not inventory metadata and never goes to a relay.
- The same digest may appear in several inventories. Admission and retention remain keyed by `(inventory_id, digest)` even though the byte store deduplicates globally by digest.

Tests must describe observable behavior rather than rely on a relay interpreting representation labels: which inventory exposed a digest, which bytes were transferred, whether the authorized reader can reconstruct plaintext, whether virtual serving reproduces the exact advertised digest, and whether restart preserves that ability.

### 17.12 Exact continuation order for implementing agents

An agent resuming this work should read, in order:

1. this document's §§15–17, treating §17 as the active correction when older text conflicts;
2. `docs/scratch/big-blobs-replacement-plan.md` §§4–8 for service, retention, lifecycle, and verification invariants;
3. ADR 019 for drawer authority and child delegation, noting that its one-meta-group text must be amended to §16.1's public/sensitive roots;
4. ADR 026 for encryption and inventory history, noting that its per-drawer plaintext part and unnamed-inventory text are superseded here;
5. `AGENTS.md` and `CONTRIBUTING.md` for the required targeted build, guardrail, and formatting commands.

Then continue in this order:

1. **Finish Phase A before touching blob distribution.** Remove blob-specific `MetaRepresentation` from Cabinet reference/protocol/state/SQL and tests. Keep `MetaRoot`, kind, name and doc id. Complete three-group/two-root delegation, projection admission, crash replay and successor tests. Amend ADR 019/026 in the same phase so code and accepted docs no longer disagree.
2. **Phase B: replace both old part mappings.** Delete `plaintext_part(drawer)` and docs-group seeding. Make every target an `inventory_part(inventory_doc)`. Writer policy maps named public/sensitive inventories to the digest it intends to pin; the inventory does not declare or validate representation.
3. **Phase C: introduce the durable pair state before adding transitive discovery.** Persist `(inventory_id, digest)` selection, pin, object, byte, fetch and completion facts; prove pin-first, object-first, restart and no cross-inventory unlocking. Do not bolt these races onto the existing `reconcile_parts` loop without a durable state transition.
4. **Phase D: add mirror-root selection and the transitive child walk.** Selection names an inventory convention and inherits through child entries. Prove the two-node A→B→C test with actual bytes before claiming drawer mirroring works.
5. **Do not add self-doc companions.** Self-doc children use their immediate containing grouped drawers' inventories; parentless self-doc inventory mirroring remains outside scope (§17.8).
6. **Integrate local virtual serving.** An authorized Read+ mirror of ciphertext may store plaintext and virtually serve the advertised ciphertext digest. Persist enough provenance to restore provider registration after restart and keep both advertised and local digests rooted under the owner-aware retention ledger.
7. **Do not block the above on Phase F.** Requester-aware exact-inventory byte authorization is deferred hardening under §17.3. Nevertheless, add negative tests proving the official protocol does not disclose sensitive inventory ids or plaintext digests to Relay participants.
8. **Run each phase's gate before beginning the next.** Report exact commands and results. A phase is not complete merely because a local projection row or unit test exists; use the real multi-node tests named in the gate.

The implementation report after each phase should contain: changed interfaces, durable schema/state transitions, tests added, exact commands run, unresolved product/design questions, and any accepted gate that was not exercised. Agents must report a design conflict rather than silently inventing a compatibility shim or weakening an invariant.
