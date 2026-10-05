# PR49 leak leg 2 — the pending-allocation drain (investigation + design, NOT landed)

Read-only investigation report. No implementation landed. Complements
`docs/scratch/pr49-final-leg.md` §3 (the landed PairRoots skeleton) and
`docs/DEVDOC/todo.md` "The other half of the leak". All line refs verified against
the working copy `@` this session. The `TEMP-INSTRUMENTATION(prekey-dive)` hunks in
`src/big_repo/keyhive.rs` were left untouched.

Terminology mirrors §3 of the final-leg doc: an **allocation** is a reserved
document identity; its durable records are (1) the `DocReservation` (keyhive secret
storage) and (2) the pending-group authority (`pending_documents_group` membership
on the document); **registration** is the drawer's `docs.map` / branch-ref write;
**completion** drops both durable records.

---

## 1. Crash-window map from ID allocation to completion

### The production pipeline (all allocation sites go through the drawer)

Every production allocation is spawned by a drawer mutation:

| site | allocation surface | registration write | completion call |
|---|---|---|---|
| content docs and **key documents** | `prepare_add_doc` (`drawer/mutations.rs:37`; parents at `:44` include the pending group) | `docs.map` entry commit (`mutations.rs:216-250`) | `complete_allocated_doc` (`:268-275`) |
| branch docs | `ensure_branch_at_heads_from_branch` (finalize via `finalize_allocated_doc_from_parent` at `mutations.rs:818`) | branch ref durable in the drawer doc (`:930-935` comment) | `complete_allocated_doc` (`:939`) |

`register_existing_doc` (`mutations.rs:305`) registers without allocating a new id,
so it cannot leak. The non-reserved `create_doc`/`create_doc_with_parents` path
(`big_repo/lib.rs:933`) bypasses reservations entirely — see design question Q6.

Pipeline in order, each step a crash window:

1. **`reserve_doc_id`** (`big_repo/keyhive.rs:894`): generates the Ed25519 signing
   key whose verifying key is the doc id, persists a `DocReservation` (struct at
   `big_repo/keyhive_storage.rs:51`) — `{magic, doc_id, signing_key, parents,
   initial_keys: [], initial_content: None}` — into sqlite secret storage under
   `SecretBlobKind::Reservation` (`save_doc_reservation`, `keyhive_storage.rs:995`;
   legacy on-disk `reservations/` files are merged in at `:700-724`). The
   reservation is the **only** enumeration for this state
   (`list_doc_reservations` `keyhive_storage.rs:1033` → `BigRepo::reserved_doc_ids`
   `lib.rs:832`).
2. **stage** (`runtime2/hub.rs:379-386` → `runtime2/native.rs:1248` →
   `stage_reserved_doc` `keyhive.rs:919` → `stage_doc_reservation`
   `keyhive_storage.rs:1052`): writes the serialized initial automerge plus the
   inherited `initial_keys` into the reservation. Idempotent (identical re-stage is
   a no-op; different content refuses). Hub ordering comment: *"Stage the plaintext
   before creating the Keyhive authority. This is the recovery record for a crash in
   any later step"* (`hub.rs:378-380`). Skipped only when the sedimentree is already
   persisted (`already_persisted` short-circuit, `hub.rs:382-387`).
3. **finalize authority** (`finalize_document_authority` → `finalize_reserved_doc`
   `keyhive.rs:956`): creates the Keyhive document with the reserved signer via
   `generate_doc_with_reserved_signer` (`../keyhive` `keyhive.rs:429` →
   `Document::generate_with_reserved_signer` `principal/document.rs:237`) and
   persists the doc's CGKA/delegation events (`persist_document_events`,
   `keyhive.rs:1084`). The reserved signing key is needed **only up to this call**
   (later group ops are signed by their callers' signers — see §3 note), and the doc
   is enumerable from here on as a live authority (`group_document_ids`, `get_document`).
4. **sedimentree persistence** (the `PutDoc` path inside hub finalize, after the
   authority). Guarded on the other end: `complete_document_authority` refuses to
   run before the sedimentree exists (`native.rs:1284-1295`).
5. **register** — the drawer's durable registration write (window between step 4
   and 5 is the caller's `prepare_add_doc` returning).
6. **complete** (`CompleteAllocatedDoc` → `hub.rs:462` → `native.rs:1284` →
   `complete_reserved_doc` `keyhive.rs:1036`): revokes the pending group's access on
   the doc (`revoke_group_from_doc` → `revoke_doc_access`) and deletes the
   reservation (`keyhive_storage.rs:1090`). The pending group
   (`PENDING_DOCUMENTS_GROUP_KEY`, `daybook_core/authority.rs:11`) is granted to
   every allocation purely as the in-flight marker (`mutations.rs:44`) and is the
   second durable record the drain must resolve.

### Window (a) — crash before staging

State: `DocReservation` with `initial_content: None`, no Keyhive document (the
identity is a private, never-published pubkey), no sedimentree, no events, nothing
registered. Enumeration: the reservation row only. The boot sweep
(`recover_pending_documents`, `daybook_core/authority.rs:194`) calls
`recover_allocated_doc` (`lib.rs:852`), which reads `staged_doc_content`
(`keyhive_storage.rs:1076`), gets `None`, and returns `Ok(false)`; the sweep's
"Allocation without staged content remains a GC candidate" branch
(`authority.rs:212`) is **named but never implemented** — the reservation row and
the identity key leak permanently. This is the purest form of the leak and the
cheapest to close, because the identity provably never left the machine (§2).

### Window (b) — staged but not registered

Two sub-states, both crash sites between step 2 and step 5:

- **(b1)** staged, authority never created (crash inside a finalize attempt):
  a retry or boot finds staged content. Boot recovery
  (`recover_allocated_doc` → `finalize_allocated_doc_with_keys`) recreates the
  authority **and** the sedimentree (idempotent, exercised by
  `staged_document_reservation_recovers_after_repository_reopen`,
  `big_repo/test.rs:695`) and then deliberately **stops**: the comment at
  `lib.rs:843-849` says finishing belongs to whoever registered the document.
  Nothing ever registers — the creator died. LEAK: reservation + pending-group
  governance + (after recovery) a live authority and sedimentree for a document
  no `docs.map` entry names. Grows every boot is not the issue; the records simply
  never go away.
- **(b2)** staged, authority + events + sedimentree durable, crash between finalize
  returning and the drawer's `docs.map`/branch-ref commit (`batch_add_inner`,
  `mutations.rs:216`; test-only commit failure injection at `mutations.rs:222-227`):
  identical leak shape with the authority fully built. Key documents hit (b2) too:
  `create_representation` (`blobs/encryption_worker.rs:1726`, key doc at `:1762`)
  and `rotate_representation` (`:1608`, key doc at `:1688`) each create the JWK
  holder doc via `drawer_repo.add(...)` — a full pipeline including registration —
  so a crash anywhere inside that `add` orphans it before registration.

### Window (c) — registered but not completed

Crash between the drawer's registration commit (`mutations.rs:214-250` / branch-ref
comment at `:930-935`) and `CompleteAllocatedDoc` (`:268-275`). The registration is
durable and **replicated** (docs.map rides the drawer doc), the reservation and
pending-group authority leak. The boot sweep re-runs recovery (idempotent no-op at
finalize since the doc exists) but nothing completes: `recover_pending_documents`
has no docs.map access and `complete_allocated_doc` is documented as owned by the
registration caller (`lib.rs:912-917`). Unlike (b), this window **is** positively
checkable and completable by a drain: the registration read (`docs.map` entry /
branch ref) settles whether to run the exact completion step the caller would have
run.

### Window (d) — key documents / JWKs dragged in by an allocation

Where exactly are they created:

- **Not in `reserve_doc_id`**: it creates only the reservation record; the doc-id
  signing key material never leaves the sqlite secret blob until finalize.
- **Not in `finalize_allocated_doc`** (nor in `big_repo` generally): a finalized
  doc's group + CGKA live *inside* the doc principal
  (`../keyhive` `principal/document.rs:237-272` — no separate key documents are
  emitted at this layer), and subduction_keyhive only registers the doc in the
  hive's in-memory doc map plus persisted events.
- **Only in the encryption worker**: `create_representation`
  (`blobs/encryption_worker.rs:1762-1774`) and `rotate_representation`
  (`:1685-1697`) create a fresh JWK-holder key document via `drawer_repo.add(...)`
  (a full allocation pipeline), then write the JWK facet (`write_jwk_facet`,
  `:1876`) — **replicated doc content**, no secret-store copy — and only later the
  `cipherBlob` facet whose value names it via
  `db+facet:///{key_doc_id}/{jwk_facet_key}`.

Is there ANY enumeration for them once orphaned:

- **Before registration** (crash inside the key doc's own `add`): the reservation
  + pending group enumerate the *allocation*, but nothing identifies it as a key
  doc or names the content doc/branch whose cipher facet would have referenced it.
  The PairRoots row for the representation names the **content** doc as provenance
  (`encryption_worker.rs:1750-1756`), not the key doc — so the landed drain covers
  the rooted pair but is blind to the key-doc allocation. A ledger row is needed
  to carry that provenance (see §3).
- **After registration** (the JWK written, the cipher facet write crashed, or the
  retry mints a fresh key and abandons the earlier doc — retries mint new random
  keys, `encryption_worker.rs:1764-1774`): the key doc is enumerable via the
  drawer (`DrawerRepo::list`, `drawer/queries.rs:491`), but "*is this JWK still
  claimed*" has no index — the key_ref lives only inside `cipherBlob` facet
  *values*, the exact tag+value-index limitation already recorded for the pair
  drain's no-provenance remainder (PR49 final-leg §8 trap + `todo.md` facet-set
  bullet). So a **registered-orphan key doc has no effective enumeration of claim
  status today**; either a ledger row per key-doc attempt or a facet key_ref index
  is required for the positive "nothing names this" read.
- Additional: the non-reserved `create_doc` path leaves **no record at all** if its
  caller crashes before registering (repo-init config/drawer docs, the
  `permission_writer.rs:657` trait surface) — outside this drain unless decided
  (design question Q6).

---

## 2. Safety analysis for each drain action

### Ordering proof: a reservation without staged content can never have been registered

Verified across all three registration writers:

- `batch_add_inner` (docs): the `docs.map` commit happens only after
  `prepare_add_doc` returns, and `prepare_add_doc` awaits `finalize_allocated_doc`;
  hub finalize orders **stage → finalize authority** (`hub.rs:374-396`), stage
  before anything else touchable.
- `ensure_branch_at_heads_from_branch` (branches): `finalize_allocated_doc_from_parent`
  (`mutations.rs:818`) precedes the branch-ref write.
- `register_existing_doc`: does not allocate.

So `initial_content == None` **and** no Keyhive document ⇒ registration was
never attempted, let alone committed. Delete of a window-(a) reservation cannot
strand a registered document. Extra props of the release:

- The doc id is a **private** pubkey until finalize publishes it (the id is only
  ever named in events created at finalize; nothing else knows it — reservation
  rows are local-secret storage, never synced). Deleting loses nothing that any
  other process or peer could have seen.
- The reserved signing key is dead weight after finalize (later ops are signed by
  caller-provided signers; keyhive group ops take `signer: &S` per call,
  `principal/group.rs`), so dropping a reservation that *did* finalize does not
  lose signing ability — but that state is handled by the (b)/(c) rows, not delete.

### Revoked vs deleted vs kept — mirroring the PairRoots rule

The PairRoots rule: *release only on a positive "nothing names this" read; keep
and warn when uncheckable; release never deletes bytes*. The allocation analog of
each:

| PairRoots analog | Allocation drain analog |
|---|---|
| release = drop the two tags, hand to store GC | release = revoke the pending-group authority (`revoke_group_from_doc`) + delete the reservation row. Never deletes the identity events, sedimentree, or doc content — those are the "bytes". |
| provenance names the document whose facet was expected to claim | provenance (ledger row) names the caller kind and, for key docs, the content doc/branch whose cipher facet was expected to name the `key_ref` |
| uncheckable → keep and warn | window (b) after the creator died → nothing can prove a retry is still coming; keep and warn (or a decided resume/retire policy — Q2) |
| two independent negatives before releasing | window (a): staged-absent + document-absent + (defensively) not registered in docs.map; window (c): registered positively read from docs.map |
| release-bias asymmetry (wrong-release unroots a live pair; wrong-keep only postpones) | wrong-release here is **worse**: it destroys the identity signing key (window (a) delete) or strands replicated content on peers; wrong-keep only grows rows locally. The table must therefore be keep-biased everywhere except the provable reads. |

Per-state verdicts:

- **(a)** reservation, no stage, no authority, not registered: **DELETE** — both
  negatives are positively readable and the ordering proof above closes the
  registered false-positive. This is the release the leak exists for.
- **(a')** reservation, no stage, no authority, *registered*: unreachable by the
  ordering proof — keep-and-warn, never delete (defensive row; investigate loudly).
- **(a'')** reservation, no stage, authority exists: also unreachable (stage
  precedes authority; `already_persisted` implies an earlier stage) — keep-and-warn.
- **(b1/b2)** staged, unregistered: **KEEP and warn.** Deleting would discard
  recoverable content ((b1) recover rebuilds it each boot — cheap and idempotent)
  and, at (b2), a live authority whose events may already be synced to peers
  (drawer-group membership was granted at allocation, `mutations.rs:44-48`, so
  event sync is not gated on registration). Retiring or resuming this state is a
  product decision (Q2), not a drain safety judgment.
- **(c)** registered regardless of stage/authority: **COMPLETE** — run
  `complete_allocated_doc` exactly as the registration-owning caller would
  (revoke pending group + drop reservation), after ensuring the authority exists
  (recover, idempotent). The registration read is the positive check.
- **Key-doc release candidate** (kind `key_doc` in the ledger, JWK written,
  provenance facet check says the producing path's cipher facet never names the
  `key_ref`): the *minimal* release is nothing — a registered key doc has no
  local-only GC to hand content to; deleting/tombstoning it replicates a deletion
  to peers and destroys key material (`write_jwk_facet` writes into replicated
  content). Mirror verdict: **keep-and-warn** until a tombstone/replication policy
  is decided (Q4). The ledger + provenance at least makes it countable and loud,
  which is what the uncheckable branch of PairRoots buys.

---

## 3. Drain design proposal (not landed)

### Substrate

**Recommendation: a new sqlite ledger row in local state, analogous to
`blob_pair_root`** — the `DocReservation` stays the identity layer (signing key +
parents + staged content), the new row stays the intent/provenance layer.

Proposed shape (mirror of `PairRoots`, `daybook_core/blobs/pair_roots.rs:44-71`):

```sql
CREATE TABLE IF NOT EXISTS doc_allocation (
    doc_id TEXT NOT NULL PRIMARY KEY  -- hex, reservation doc_id spelling
  , kind INTEGER NOT NULL             -- 0=content 1=branch 2=key_doc
  , naming_doc_id TEXT                -- for key_doc: the doc whose cipher facet
  , naming_branch TEXT                --   was expected to name the key_ref
  , created_at INTEGER NOT NULL
) STRICT
```

- Written by the allocation-owning callers **before** `allocate_doc` (required
  parameter on the allocate path, exactly like `PairRoots` is required in
  `register_pair`/`install` so no path can leak unrecorded — final-leg §3).
  `big_repo`'s `allocate_doc`/`create_doc` cannot know the intent (it does not
  know domains/branches), so the row is written by `daybook_core` (drawer /
  encryption worker) and `big_repo` exposes `unresolved`/`clear` the way
  `PairRoots` does.
- Cleared by `complete_reserved_doc` (`keyhive.rs:1036`, the one place every
  allocation leaves).
- Join semantics with the reservation: reservation-without-row = pre-ledger
  leftover (keep-and-warn); row-without-reservation = already completed (clear the
  row).

**Why not extend `DocReservation`**: the struct is non-self-describing bincode
with no version field (`keyhive_storage.rs:51-64`, deserialize at `:1029`),
lives in AEAD secret storage it does not belong in (provenance is not secret and
is daybook-layer intent the keyhive storage layer cannot spell), and any field
addition hard-breaks deserialization of existing rows. Extending it is viable only
if the operator accepts a reservation format bump (Q1).

### Where the drain runs

Two-piece, reusing existing owners:

1. **`DrawerRepo::load` (or drawer boot right after it)** — the registration read
   is native here (`get_entry` `drawer/queries.rs:526`, list `:491`). For each
   reservation: registered ⇒ complete (`complete_allocated_doc`); not registered ⇒
   consult the ledger row's kind/provenance for the §2 table.
2. **`recover_pending_documents` (`daybook_core/authority.rs:194`)** keeps its role
   for staged-but-unregistered rows (recreate authority, stop) and gains the
   window-(a) release (reserve-only rows: delete the reservation) plus
   keep-and-warn logging for the (b)/(a')/a'') rows.

Running it at authority boot (before the drawer loads) is simpler but requires
reading the drawer doc's docs.map through `big_repo` primitives; running it in the
drawer puts the registration check where it lives. Not settled — Q7.

### Per-row decision table (pure fn, unit-testable like `drain_action`, `pin_worker.rs:587`)

Inputs: `staged`, `authority` (keyhive doc / sedimentree), `registered`
(docs.map or branch ref), `provenance` (ledger row present), `kind`.

| kind | staged | authority | registered | provenance | action |
|---|---|---|---|---|---|
| content/branch | no | no | no | any | **delete reservation + row**, warn |
| content/branch | no | no | yes | any | keep-and-warn (ordering says unreachable) |
| content/branch | no | yes | any | any | keep-and-warn (unreachable; `already_persisted` path) |
| content/branch | yes | no | no | any | keep (recover sweep re-finalizes next boot; a live retry may claim) — resume-registration is Q2 |
| content/branch | yes | yes | no | any | keep-and-warn (creator died mid-add; retire/resume is Q2) |
| content/branch | * | * | yes | any | **recover (idempotent) then complete** (window (c)) |
| key_doc | * | * | not registered | any | same content rules, plus: the creating attempt's retry mints a fresh key, so keep-and-warn until Q2/Q4 decide |
| key_doc | * | * | registered, naming facet names the key_ref | — | keep (a real cipher facet claims it) |
| key_doc | * | * | registered, naming facet does not name it | — | keep-and-warn (release = tombstone = replicated deletion — Q4) |
| any | * | * | * | row present, reservation absent | clear row |
| any | * | * | * | row absent, reservation present | keep-and-warn (pre-ledger leftover) |

Error-direction bias matches §2: every ambiguous cell is a keep.

### Test plan (extend the existing files; no new integration tests)

Precedent: `reserved_document_crash_windows_are_recoverable` (`big_repo/test.rs:792`)
and `staged_document_reservation_recovers_after_repository_reopen` (`:695`).

1. **Decision-table unit test** — pure fn over `staged`/`authority`/`registered`/
   `provenance`/`kind`, every row of the table above, in the drain owner module
   (mirror of `drain_action` test, `pin_worker.rs:2171-2178`).
2. **Window (a) release** — extend the `big_repo/test.rs` crash-window test:
   allocate, reopen, run the drain, assert the reservation is gone and nothing
   else changed (no keyhive doc, no sedimentree).
3. **Window (c) completion** — with the docs.map failure injection
   (`take_fail_next_drawer_doc_commit`, `mutations.rs:222`) or a big_repo-level
   "register without complete": reopen with the drain, assert reservation gone,
   pending group revoked, doc still served by the drawer.
4. **Window (b) keep** — staged, unregistered, reopen with the drain: reservation
   still present, authority recreated by recovery, `docs.map` still empty, warn
   logged (assertable via the existing tracing capture pattern).
5. **Required-provenance loud failure** — analogous to the PairRoots loud
   no-record failure (`pair_roots.rs` test): an allocation path without a ledger
   row fails loudly in tests.
6. **Ledger round trip** — row written before allocate survives a reopen
   unresolved; cleared on completion; `unresolved()` listing is empty on a clean
   boot.

---

## 4. Product/design questions NOT settled from code (not answered here)

1. **Substrate** — extend `DocReservation` (accepting a bincode format bump; the
   struct has no version field, existing rows hard-fail deserialization) vs the
   new local-state ledger row proposed in §3; and which layer owns the
   provenance (big_repo cannot spell "which content doc's cipher facet will name
   the JWK").
2. **Window (b) policy** — staged/authoritative but never registered: resume
   registration deterministically from the staged content (the staged automerge
   carries the branch/branches facets, so the `DocEntry` is derivable without the
   original args), retire the allocation, or keep-and-warn forever? This is the
   only branch where the leak is not a cheap row but a live document.
3. **Window (c) auto-completion at boot** — completing a docs.map-registered
   allocation without its original caller is behavior, not recovery plumbing; is
   it approved?
4. **Registered orphan key documents** — tombstone/drawer-`del` is a replicated
   deletion that reaches peers and destroys key material; is retirement of these
   ever wanted, or is countable keep-and-warn the accepted long-term state?
5. **Positivity check scope for key docs** — is "the producing path's provenance
   facet does not name the `key_ref`" sufficient, or is a global
   all-cipherBlob-facets read required (which wants the facet key_ref index)?
6. **The non-reserved `create_doc` path** (`big_repo/lib.rs:933`, repo-init docs,
   `permission_writer`'s trait) leaves no crash record: in scope for the ledger
   or out?
7. **Drain ownership/ordering** — authority boot sweep (`authority.rs`, pre-drawer)
   vs `DrawerRepo::load`; and does a docs.map read through `big_repo`-level
   primitives pre-drawer exist cleanly enough to prefer the single-site drain?
8. **Unreachable-state proof** — can "reservation without staged content +
   authority exists" ever occur via the `already_persisted` hub short-circuit
   (`hub.rs:382-387`) plus sync-fed sedimentree content? Reasoned unreachable in
   §2, but the table treats it keep-and-warn; is a pinning test wanted now?
9. **Pending-group revocation volume** — a boot drain over many leftover
   allocations mass-emits pending-group revocation ops that sync to the group's
   members (AGENTS.md warns about treating notification storm classes as bugs).
   Should the drain gate/batch these, or is per-row revocation correct?