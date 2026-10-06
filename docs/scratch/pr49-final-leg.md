# PR #49 — final leg (encrypted blobs)

Living scratch doc for the current work. The parent agent owns this file; subagent lanes must
**not** edit it. Update it as items close.

## HANDOFF INDEX — read this first after any compaction

**Before acting, refresh on `AGENTS.md` (repo root) and the docs it links.** Its rules are load-bearing
here: never run `git` (jj only; jj mutations are the parent's alone); no pushes by the agent; no backcompat
seams; findings that are programming errors should crash rather than be swallowed; keep instrumentation
until green; test conventions (in-crate `e2e`/unit modules, nextest, no skips); the SQL style note (commas
first); and the **big_sync ↔ keyhive racing** warning (waves of `big sync object task failed;
rescheduling` with a local-policy rejection are the intentional retry mechanism, not a bug).

**Terminology.** A **pair** is the two named tags `ct:<C>` → C and `pt:<C>` → P that root a ciphertext
representation and the plaintext serving it (`blobs/encrypt/store.rs`, `set_pair_tags`/`drop_pair_tags`).
A **bao outboard** is the store's hash tree for a blob's bytes: a *virtual* ciphertext entry keeps only C's
outboard plus the provider name, which is why a key-holder stores no ciphertext. An **in-flight record** is
a durable row written *before* a side effect and dropped once resolved — the pair-root row below, and the
operator's "outboard of incoming blobs" idea for blob announcements (same primitive, different side effect).

### 1. Tree, build and verification recipe

- Repo `/home/asdf/repos/rust/townframe-3`. Bookmark `feat/encrypted-blobs` = `svxwtwvv 128ab23a`; `@` carries
  all post-head work uncommitted (see "Where things stand" above for the chain).
- Builds: `flock /tmp/townframe-cargo-validation.lock env CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target cargo …`
  (clippy `-p daybook_core --all-targets --all-features`; tests `cargo nextest run -p daybook_core -E '<filter>'`).
- Other sessions build on this box: queue on the lock, never kill their processes. `./x/disk-watch.ts` runs in a
  tmux session and wipes `<target>/debug` when the repo FS drops below 4 GiB, so builds are sometimes cold.
- Test classes (`.config/nextest.toml`): `long_test` = 60s × 4 = 240 s ceiling; `long_af_test` = 60s × 8 = 480 s.
  The class is chosen by the test *name prefix*, so moving a test between classes is a rename.

### 2. Landed this session — parent-verified

- **`src/big_repo` settle machinery removed** (the diff the operator called "sus"/"hacky"); the race it papered
  over is fixed at its source: `add_member_to_group`'s preflight tolerates `NotFound`/`PendingMaterialization`
  because the encryption worker creates key documents at runtime.
- **`docs.map` crash window closed**: `FinalizeAllocatedDoc` no longer completes an allocation; completion moved to
  `Runtime2Cmd::CompleteAllocatedDoc`, called by the drawer *after* its writes land; `recover_allocated_doc`
  recreates the authority and stops. Three crash-window tests rewritten (3/3).
- **`MAX_SUMMARY` errors out** (`PeerSummaryError::TooManyParts`) instead of silently degrading.
- **ADR 003 §9 rewritten** (one framing-bound formula + canonical input list; no `.v2`), §11 claim at `:182`
  qualified (the identical-plaintext equality leak is now stated as a metadata property, with domain separation
  as future work), cubic `:292`/`:295` dispositions.
- **Review findings**: cb-1, cb-3, cb-4, cb-5, cb-7 fixed; cb-2 deferred by design (a pre-PR repo cannot boot);
  cb-6 is the labelled `.expect(ERROR_IMPOSSIBLE)` matching main; `is_system_managed` kept per the operator.
- **`no_blobs` opt-out removed** from the sync tests: 39 call sites flipped to the production shape, 2 kept as
  documented exceptions (`iroh_blob_pin_sync_replicates_and_fetches_blobs` — workers-on would pin its blobs and
  hollow out the on-demand-fetch assertion; `long_af_test_iroh_clone_sync_batch_100_docs_with_blobs` — see §7).
  Measured: the opt-out bought no time (boot-only 10.145 s off vs 9.669 s on; doc-plane 24.976 s off vs 19.868 s on).
- **The 100-docs test moved to the `long_af_test` class** (rename only; filter is prefix-based).
- **Codec serving-path allocation fix**: `PlainPair::encrypt_record` (`blobs/encrypt/serve.rs`) now reserves the
  frame's exact length (`content + RECORD_OVERHEAD + pad_zeros`) and encrypts in place via the new
  `Cipher::encrypt_record_in_place`; the owned-`Vec` `encrypt_record` is a thin wrapper, so the bulk encode paths
  are untouched. clippy 0/0; 49/49 encrypt/codec/serve/cipher tests.

### 3. The pair-root leak ledger — LANDED and verified

Mechanism: a pair's tags are the store's GC roots, and the only release trigger is the **encryption-inventory
diff** — which can only release pairs the inventory ever recorded. A crash between the tags and the pin leaves
`C` and `P` rooted with nothing that will ever claim them, permanently (ADR 003 §19). `PairRoots` is the durable
row written *before* the tags so the window is recoverable.

Landed (verified: clippy 0 errors / **0 warnings**, blob tests **97/97**, 155 skipped):

- `blobs/pair_roots.rs` (new): `pub struct PairRoots { sql: SqlCtx }`, one `STRICT` table
  `blob_pair_root(cipher_hash TEXT PRIMARY KEY)`, `boot`, `init_schema`, `record_before_root`, `clear`,
  `unresolved`. (Fork fixes to my file: `use iroh_blobs::Hash;` — `Hash` is *not* in the daybook_core interlude —
  and `pub(crate)` → `pub` because the type appears in `pub` signatures. No `#[allow(dead_code)]` was added.)
- `blobs.rs` declares the module; `blobs/encrypt/serve.rs` `register_pair`/`install` take a **required**
  `roots: &PairRoots` and call `record_before_root(c_hash)` **before** `set_pair_tags`. Required, not optional, so
  no path can root a pair without a record.
- Threaded call sites: `encrypt/store.rs` (`add_encrypted_stream`, `add_encrypted`), `encrypt/download.rs`
  (`download_encrypted`), `encryption_worker.rs` (Ctx field `pair_roots: PairRoots` built as
  `PairRoots::boot(sql.clone())` + 4 rooting sites), `encryption_worker/tests.rs` (its 2 `Ctx` literals),
  `encrypt/tests.rs` (12 sites + `test_pair_roots()` helper), `key_source.rs` (3 sites + helper).
- That dead-code warning is gone: `unresolved`, `clear`, `attach_provenance` and `has_pair_tags` all have callers now.

The drain shipped with the ledger: `attach_provenance` (an `UPDATE` from both install paths in
`encryption_worker.rs`, so a rooted pair's row names the document whose facet is expected to claim it),
clear-on-pin in `replace_doc_branch_state`, and `Ctx::drain_pair_roots` at pin-worker spawn, decided by a
pure `drain_action` table:

| rooted | pin recorded | a facet names C | action |
|---|---|---|---|
| no | — | — | clear the row |
| yes | yes | — | clear the row (the pin machinery owns it now) |
| yes | no | yes | keep (the facet machine will derive the pin) |
| yes | no | no | release the tags, clear the row |
| yes | no | uncheckable | keep and warn — never release blind |

The facet check reads facet *values* (`get_doc_with_facets_at_branch` +
`digest_str_to_blob_id_lenient`), because a `cipherBlob` facet's key id is `{domain}/{facet}` and the
representation digest lives in its value.

Tests: the decision table; a provenance round trip across a reboot plus the loud no-record failure
(`pair_roots.rs`); the release path (`test_pair_root_drain_releases_only_unclaimed_pairs`); the keep path
with a real facet (`test_pair_root_drain_keeps_a_pair_a_facet_still_names`).

**What still remains of the leak:** orphan key documents — the pending-allocation drain itself LANDED
(see §3b), with the ledger shape superseded by the staging facility.

### 3b. Landed — the drawer staging facility (the pending-allocation drain)

Landed 2026-10-25 by a worker lane, operator-approved. `pr49-leak-leg2.md` §1/§2 stand (the window
map and the ordering proof); its §3 `doc_allocation` SQL ledger is **SUPERSEDED** — no ledger row was
built. The facility removes the leak by construction instead of ledgering intent:

- **Allocation** (`drawer/mutations.rs`): both allocation sites — `prepare_add_doc` and
  `create_branch_at_heads_from_branch` — now allocate with parents = `[pending_documents]` ONLY. No
  content_docs / encrypted_blob_docs / drawer grant at genesis. The pending group has zero members
  (`authority.rs`), so between allocation and finalize nothing advertises the doc: its events cannot
  leave the node (the only other founding member is the creating agent, which
  `generate_doc_with_reserved_signer` always enlists as head coparent — this is why pending-only
  genesis is generatable at all).
- **Finalize grants** (the embedder's commit sequence): the advertising groups are granted AFTER
  finalize and BEFORE the registration write — content_docs + encrypted_blob_docs (+ drawer group:
  unconditionally for `prepare_add_doc`, only for `BranchKind::Replicated` branches) — copying
  `register_existing_doc`'s grant shape. Completion (`complete_allocated_doc`) stays after the
  registration, so the order is grant → register → revoke-the-pending-coparent.
- **Boot sweep** (`recover_pending_documents`, `daybook_core/authority.rs`): reads each reservation's
  registration off the drawer's durable surfaces (`drawer::registered_allocation_shapes`: the
  drawer doc's `docs.map` keys = content docs, entry branch refs = replicated branches, and the
  `drawer_local_branches` SQL table = local branches; local-branch DDL is created on demand) and
  drives the engine **`BigRepo::drain_pending_allocations`** (big_repo, `lib.rs`) with one
  `AllocationRegistration` per reservation:
  - `Registered(grants)` → mechanically replay the finalize grants (idempotent; a grant the group
    already reaches the doc with Admin access is skipped) then complete: revoke the pending
    coparent, drop the reservation. This is the registered-not-finalized crash window.
  - `Unregistered` → **discard**: revoke the pending coparent (skipped when the doc was never
    finalized) and delete the reservation. Purely local — no peer can hold authorization, and
    sedimentree/events/bytes are never deleted; the staged initial content dies with the
    reservation record it lives in.
  - `Unknown` (drawer doc unreadable at this boot stage: fresh clone, mid-init) and ordering
    contradictions (registered without staged content) → **keep and warn**, never released
    blind. Reservations absent from the caller's map count as Unknown.
- **Out of scope, unchanged**: `register_existing_doc` (no allocation); the non-reserved
  `create_doc`/`create_doc_with_parents` path (leak-leg2 Q6); the TEMP-INSTRUMENTATION(prekey-dive)
  hunks in `big_repo/keyhive.rs`. The encryption worker's key docs flow through `drawer.add` and are
  covered by the facility automatically.

Tests (all narrow; no new integration tests): `big_repo/test.rs` gains the four sweep windows —
`the_boot_sweep_{discards_an_unregistered_staged_allocation,
discards_a_reservation_that_was_never_staged, completes_a_registered_allocation_by_replaying_its_grants,
keeps_reservations_it_cannot_safely_release}` — and the drawer pins the grants→registration order
with the injected drawer-doc commit failure
(`the_drawer_commit_failure_pins_the_granted_not_registered_window`). Verified: clippy
`-p big_repo` and `-p daybook_core` `--all-targets --all-features` 0 errors / 0 warnings; crash-window
family 7/7; `drawer::tests::` 33/33; encryption-worker add/rotation tests 3/3. All builds/tests
serialized on `/tmp/townframe-cargo-validation.lock` with the shared target dir.

Residual notes (flagged, not decided): a crash between the finalize grants and the registration
leaves a granted-but-unregistered doc that the sweep discards WITHOUT revoking the advertising
groups (the minimal discard contract) — that dead doc stays listed in content_docs/ebd forever.
Pre-upgrade leftovers of the OLD genesis-shape allocation may have pushed events to peers before
their unregistered boot discard; peers keep the orphan, which is the status-quo leak, not a
regression. `ensure()` also runs mid-session (plug imports), where the sweep can race a live
in-flight add (alloc→stage window): the raced add fails loudly rather than corrupting anything —
the boot-time sweep is race-free because the drawer is not serving yet.

### 3c. Landed — the external transactional add (two-phase staging + the cipherBlob claim)

Landed 2026-10-25 by the staging worker lane (operator-approved design; the
operator's contract, verbatim: "an `add` method that creates it only locally
without adding it to the rest of the groups, swept at next boot, providing
external systems transactional semantics by requiring them to come back and
either discard their previous add or commit it"):

- **`DrawerRepo::add_temporary(AddDocArgs) -> StagedAdd`** — pending-only
  genesis (`prepare_add_doc`), no grants, no `docs.map` entry, no completion.
  Validates its arguments at `FacetWriteScope::System` scope (the caller is
  system machinery — ordinary `add`/`batch_add` keep their user-scope gate in
  `batch_add_inner`). `batch_add_inner` now composes the same two halves:
  prepare + `commit_staged_adds`, one code path, two spellings.
- **`commit_temporary(&StagedAdd)`** — the finalize grants (content_docs +
  encrypted_blob_docs + drawer), the `docs.map` registration (the
  `fail_next_drawer_doc_commit` test seam lives in this commit path), then
  `complete_allocated_doc`. Same sequence, mechanically, as `batch_add`.
- **`discard_temporary(&StagedAdd)`** — idempotent; reverts any advertising
  grants a crashed commit left (`big_repo::discard_reserved_doc`, the sweep's
  discard primitive — the drain's `Unregistered` arm now calls it), revokes
  the pending coparent, deletes the reservation. A discard of a committed
  receipt is a logged no-op: the caller picked commit, and un-registering a
  committed doc is a delete, not a discard.
- **Encryption worker reorder** (`create_representation` +
  `rotate_representation`): the key document is staged via `add_temporary`
  **with the JWK facet in its staged initial content** (this is the "written
  while still the node-local staging doc" requirement — the JWK goes into the
  genesis, validated at system scope, so no staging-doc facet write interface
  had to exist), the cipherBlob facet into the content doc is the claim and
  the operation's commit point (keyRef + the staged heads),
  `commit_temporary` is the operation's only replicated act, the resolution
  URL is ensured last. `Ctx::write_jwk_facet` is gone.
- **Sweep classification** (`registered_allocation_shapes` → new
  `AllocationRegistrationRead`, `authority.rs:recover_pending_documents"): the
  read now also collects **claims** — reservations named by a durable
  `cipherBlob` facet's `keyRef`, found by the bounded tag enumeration +
  facet-value read of the pin worker's drain, over the branch docs of every
  registered content doc, paid only when reservations exist at boot. A
  claimed-but-unregistered reservation is registered-in-spirit: the sweep
  first replays the `docs.map` registration
  (`register_claimed_allocations`; actor derived exactly as
  `DrawerRepo::load`'s `drawer-repo`-scoped user path) and then lets the
  drain replay the grants and complete it; the claim read's own unreadability
  (a registered branch doc not materializable) downgrades the classification
  to keep-and-warn. No claim (only when every claim surface was readable) →
  discard; unreadable / contradictory → keep-and-warn unchanged.
- **ADR 003**: §19's ordering block, its "no rescanning mechanism" paragraph
  and §15's implemented-shape paragraph now record the staging-claim-commit
  ordering and the transient pre-commit unresolvable `keyRef` window (retries
  resolve once the commit's events propagate, because `keyRefHeads` pins the
  staged heads the commit replicates). §16 does not state the ordering — not
  amended.

Tests (drawer/tests.rs): `a_temporary_add_grants_nothing_and_registers_nothing`,
`committing_a_staged_add_matches_batch_add`,
`discarding_a_staged_add_is_idempotent_and_leaves_a_commit_intact`,
`the_boot_sweep_commits_a_claimed_temporary_key_document`,
`the_boot_sweep_discards_a_staged_key_doc_with_no_claim`. Verified: clippy
`-p big_repo -p daybook_core --all-targets --all-features` 0 errors / 0
warnings; drawer tests 41/41 (33 prior untouched + 5 new + the commit-failure
pin test); big_repo boot-sweep family 10/10; encryption-worker add/rotation
tests 16/16. Note for the parent: a concurrently-running relay-retention lane
briefly left `sync/tests.rs` uncompilable mid-run (its file — fixed by that
lane, not touched here); the full `daybook_core` test-target compile is green
on the final tree.

Residual notes (flagged, not decided): committing the same receipt twice
would panic on the no-op automerge commit (`tx.commit()` → `None` → "commit
failed") — the contract is one commit-or-discard per receipt and the sweep
owns crash windows, but if two-phase callers outside the repo's machinery are
ever exposed (FFI) this deserves a guard or an explicit refusal. The sweep's
claim scan reads replicated branch docs only (local `/tmp` branches never
carry facet writes the worker authors). The `docs.map`-entry write in
`register_claimed_allocations` is a fourth inline copy of the drawer-doc map
write (alongside `batch_add`, `register_existing_doc`, `delete_branch`) — a
shared helper could consolidate them later.

### 4. Not started, already decided

- **Relay retention test (Tier 1) — LANDED** (`sync/tests::relay_granted_only_the_encrypted_inventory_cannot_decrypt_but_retains_and_re_serves_ciphertext`): a told node granted keyhive `Access::Relay` on the encryption inventory learns C, pulls C's bytes, holds no P; typed `DocLookup::Missing` proves it never received the key document; the origin stopped, it re-serves C to a third told node that was never connected to the origin. Design-fix (2026-10-26): the loud-decrypt-failure leg (`stage_relay_cipherblob` + `get_decrypted` through `DocKeySource`) was removed — it exercised the Tier-2 shape through a **fabricated** drawer state (a cipherBlob `FacetRaw` staged on the relay's own drawer that no relay plane can produce); §19's loud-decrypt-failure coverage belongs to a future Tier-2 receive-half test, not to a fabricated drawer fixture. Design reading settled (2026-10-26, operator): told/relay nodes boot WITHOUT the blob workers BY DESIGN — the drawer-dialect machines (pin worker authoring BlobPin facets into inventory docs, encryption worker) are the origin-side authoring path; a relay replays the part store derived from the config-told inventories (`IrohSyncRepo::peer_partition_ids`) with membership replayed from the serving side's permission-writer rows (fetch predicate `is_fetcher`, `>= Access::Relay`, so the Read grant the relay holds on the inventory documents — the sibling told test's provisioning route — retains exactly like a formerly asserted Relayed row). The pin worker's new boot ensurer (`ensure_configured_inventory_branch`) makes that configuration real: it refuses a spawn whose configured inventories are not local drawer branches, so the once-silent `resolve_doc_id_for_branch_doc_id` fallback can no longer boot a drawer-dialect reconcile against foreign config ids — the relay test asserts that refusal as an explicit policy leg, and the relay/ consumer/ told-sibling nodes all run workers-off accordingly (the sibling told test's earlier workers-on shape survived only because its empty desired sets never patched into the branchless foreign inventories). Deferred product decision (recorded, not blocked): eager cross-node retention ADVERTISING via the presence plane remains "a later, deliberate decision" per ADR 003 §13 — the relay's retention pull today is part-driven big_sync replay from peers it is connected to, which the test exercises directly (origin→relay, then relay→consumer with the origin stopped).
- **Tier 2 — DECIDED (operator, 2026-10-26): the receive half stays unwired** (`download_encrypted`, `FsDownloadLedger`, `CipherReader` remain test-only primitives), acceptable as long as a proper replication test covers the encrypted-replication path — the Tier-1 relay test above is that coverage. When Tier 2 ever wires the receive half, the §19 loud-decrypt-failure coverage the staging used to stand in for belongs to that shape's own test.
- **The 6 newest review findings** (2026-10-25 14:36–15:37Z) — dispositions in `docs/scratch/pr49-replies.md`
  (rounds 14–15). Fixes agreed: the codec serving-path allocation fix (§9 step 3), comment the facet-index reason,
  make `announce_held_blobs` conditional. No change for the `serve.rs:139` clone question. Out-param deferred.

### 5. Decisions made (with reasons)

- Leak substrate **SQL** (a row is not a GC root; the tag namespace *is* the liveness oracle, so a ledger inside it
  is self-referential — that is what sank the reverted attempt).
- **One row per C** (the tags and the release trigger are per-C; the same C can be named by several documents).
- **Release drops pins only, never bytes** (dropping the tags hands the pair to the store GC; deleting bytes would
  destroy P, which documents serve).
- **Two independent negatives before releasing**, and *leave-and-log* on anything ambiguous. Being wrong in the
  release direction unroots a live pair; being wrong in the leave direction only postpones.
- `PairRoots` required (not optional) in `register_pair`/`install`: an optional handle silently reintroduces the leak.
- Provenance via an `UPDATE` rather than a new parameter (churn), with the conservative no-provenance rule.
- Class move for the 100-docs test; `is_system_managed` kept; no `.v2` salt bump; domains deferred to their own PR.

### 6. Deferred / follow-ups (all mirrored in `docs/DEVDOC/todo.md`)

- **The other half of the leak: orphan key documents** — the pending-allocation drain itself landed
  (see §3b) and shared no ledger. What remains: the claim-status enumeration of
  registered-then-abandoned key documents.
- Download-ledger location + a sweep for abandoned attempts (`FsDownloadLedger` has no production root today).
- `announce_held_blobs` made conditional on an unclean shutdown (`plane_may_lag` marker; no existing crash signal)
  — the journal/outboard variant is the stronger long-term form.
- The facet-set index carrying blob-specific state (mixing concerns) — same reason the representation digest would
  need indexing to close the no-provenance remainder of the drain.
- Store GC decision (`options.gc` unset: releasing roots frees nothing yet).
- Domain separation for the identical-plaintext equality leak; rotation coparent-revocation limitation.
- Pending GitHub thread replies (the operator's side; drafts live in `docs/scratch/pr49-replies.md`).

### 7. Flaky or failing, with evidence

- **Pin-path flake, load-sensitive, REPRODUCED with its assertion (three sightings):**
  `iroh_blob_pin_sync_replicates_and_fetches_blobs` fails at ~18 s (twice) and passes at 32–48 s, under
  load. The captured error is a keyhive **prekey** race, not the pin path and not the AGENTS-documented
  reschedule class: `error finalizing allocated doc in big repo` → `keyhive doc creation failed: individual
  0x7a7d701a7e6ae429021bb52e7a6418360faee7c909b1af1e1621bdae01c24e9f has published no prekey to select from`,
  raised at the test's `drawer.add` (`sync/tests.rs:1121`) — document creation picks a prekey for the peer
  member before that peer's prekey has propagated, even though `wait_for_sync_convergence` had returned.
  Needs a decision on where the tolerance belongs (allocation path wait/retry vs. the test) and attribution
  under the stress runs. It then passed 3/3 isolated (~47 s) and 4/4 full-set runs (two at `-j 32`, load 5–11),
  so load is the trigger. The fork separately saw
  `blobs::pins_part_worker::tests::blob_pin_facet_key_that_is_not_a_digest_is_ignored` fail once, and its diagnosis is
  structurally confirmed: that test waits on the **part store** (`pins_part_worker`) and then asserts the **SQL pin
  rows** (`pin_worker`) — two independent workers, so it can legitimately read `[]`. Its wait is fixed:
  `wait_for_pin_row_count` now runs before the assertion. Chase the rest only with a real repro under contention.
- **`long_af_test_iroh_clone_sync_batch_100_docs_with_blobs`** fails in the production shape by hitting the 240 s
  ceiling (captured: `(test timed out)`, `1 timed out` at 240.129 s, with a docs-worker retry storm —
  `policy rejected commit … error=document not found` + `big sync object task failed; rescheduling … worker=daybook-docs`
  still climbing 200 ms before the kill). Hence the class move and the workers-off exception.
- **Load-only ladder flake:** `ladder::iroh_sync_single_blob_created_before_connect_replicates` — 1 FAIL in a wide
  sweep, **9/9 PASS** isolated (`/tmp/pr49-parent-verify11.log`). Needs the stress runs to attribute.

### 8. Traps discovered (do not rediscover the hard way)

- **Spelling.** `PairRoots` stores `Hash::to_hex()`; facet/pin state uses the **facet spelling**
  (`blob_id_to_digest_str` = base58 multihash). Convert via `Hash::from_str` → `BlobId::new(*hash.as_bytes())`.
  Tag names use the `Hash` Display form: `format!("{TAG_CT_PREFIX}{c_hash}")`.
- **A `cipherBlob` facet's key id is `{domain_id}/{blob_facet_id}`** (`encryption_worker.rs:851`), not the
  representation digest — the digest lives in the facet **value** (`cipher.representation.digest`). A tag+id index
  lookup therefore cannot answer "which facets name C" (the same reason `facet_set_doc_blob_facets` exists for
  `Blob` digests).
- **Two independent pin workers**: `pin_worker.rs` writes the SQL pin state; `pins_part_worker.rs` writes the part
  store. Never assume one implies the other without waiting.
- **Reads materialize.** `BlobsRepo::get_path`/`get_bytes` → `ensure_hash_materialized` → `ensure_local_blob` fetches
  a missing blob straight from active peers, so a byte read proves nothing about the sync plane. Assert presence with
  `has_blob_on_disk`/`wait_for_blob_replicated` *before* reading. (`has_blob_on_disk` checks the **data file**, so it
  is false for a virtual ciphertext entry even though the outboard exists.)
- **Ciphertext is stored only where the key is absent** (relays/mirrors fetching the pinned part). Key-holders store P
  plus C's outboard. The pair tags are written only by key-holders (`register_pair` runs after the key is resolved).
- `ensure_local_blob` has a `blobs().has(hash) → put_from_store` branch (`blobs/sync.rs:62`) that would copy a blob
  into the daybook object store — worth a test if anything ever reads a C digest locally.
- `sync/tests.rs:794` carries a stale `TEMP-INSTRUMENTATION: localising why the blob scope does not replicate`
  despite its byte assertions passing — resolve or delete it.
- A `cipherBlob` facet is **system-managed**: authoring one in a test needs
  `update_at_heads_with_scope(.., FacetWriteScope::System)`; an ordinary `add`/`update` is refused
  (`ordinary facet writes cannot modify system-managed facet ...`).
- `BranchPath` is `camino::Utf8Path` (unsized), so a helper takes `&BranchPath` and callers pass
  `BranchPath::new(..)`, which yields `&BranchPath` — never the bare type.

### 9. Immediate next steps, in order

1. ~~Land §3~~ done: clippy 0 errors / 0 warnings, blob tests 97/97 (three new tests: the decision table, the release path, the keep path).
2. ~~Fix the cross-worker wait~~ done: the test now waits on the record it asserts (`wait_for_pin_row_count`).
3. ~~Codec serving-path allocation fix~~ done: the frame is reserved exactly and built in place (`serve.rs`, `codec.rs`); 49/49 encrypt/codec tests.
4. Relay retention test (Tier 1) and the Tier 2 product decision.
5. Stress runs under load (distributed logic only fails under load; a single pass proves nothing) — the operator
   wants a greening pass, and the 100-docs/class question is settled there.
6. The remaining review-item fixes (facet-index comment; `announce_held_blobs` conditional) and the thread replies.
## Where things stand (2026-10-25)

- Repo `/home/asdf/repos/rust/townframe-3`; **jj only, never git**; jj mutations are the parent's alone.
- `@` = `lspqtwrx aace5a3f`, carrying all post-head work uncommitted (28 files, +1735/−471); `jj st` lists it.
- Bookmark `feat/encrypted-blobs` = `svxwtwvv 128ab23a` "fix: address feedback".
- Chain (child → parent): `e9f0a696` → `128ab23a` → `7724aa3a` → `b9d9ac8b` → `7acaf616` (user "wip: wip")
  → `9bf950b4` (§15 rotation) → `d9498be` (cross-channel ordering + told-not-cloned) → `50555758`
  → `d35d1bc8` (EncryptedBlobWorker) → `main`.
- PR: <https://github.com/dman-os/townframe/pull/49>. `gh` is unusable in this checkout (no `.git`).
- Parent-verified gates on the current tree: clippy 4 crates **0 errors / 0 warnings**; `big_repo`
  **427/427**; encrypt **42/42**; the three crash-window tests **3/3**; big_sync summary/decide **5/5**;
  daybook_core **196/197** — the single failure is the load-only ladder flake below (9/9 PASS isolated).
- **cb-8 audit DONE** (fork `dd0cd6e6`, re-verified): exactly **one** of the 44 node sites violated the rule the
  helpers document — `sync/tests.rs:220`'s `shutdown_stops_the_inventory_writer_after_the_workers_that_serve_from_it`
  ran workers-off while asserting the stop order of tasks that never spawned. Flipped, PASS 9.755 s; clippy
  `--all-targets` 0/0; both ladder blob tests PASS too.
- **Operator call pending — the 4-node stress cluster:** `long_af_test_iroh_sync_randomized_four_node_stress_converges`
  is all-workers-off and its final check (`assert_blob_parity`, `stress.rs:1250-1268`) uses the vacuous
  `wait_for_blob_bytes` (a read materializes a missing blob from peers). It **is** in the CI regime
  (`.config/nextest.toml` only widens its slow-timeout). Hardening it alone should go red because nothing
  derives the pins; the fix is workers on the seeds + the hardened assertion, validated by a long run. Not
  flipped, not run — its current state is unknown.
- **Load-only flake:** `ladder::iroh_sync_single_blob_created_before_connect_replicates` — 1 FAIL in the wide
  sweep, **9/9 PASS** isolated (`verify11`). Needs the stress runs to attribute; not greened.
- Standing constraints from the operator: no pushes by agent; no backcompat seams; no sleeps at
  command start / no sleep-poll loops; forked (context) subagents, never fresh; keep instrumentation
  until everything is green; never run git.

## Open item 1 — the `src/big_repo` diff (operator: remove it)

`jj diff --stat -r 'main..@' -- src/big_repo` = **7 files, 201+/14−**:

| file | what the hunk does |
|---|---|
| `big_repo/keyhive.rs` | new `authority_peer_keys()`; `explain_missing_prekeys` / `create_doc` / finalize comments re-written to say the settle ran |
| `big_repo/lib.rs` | `generate_group` settles parent channels before reading prekeys |
| `big_repo/runtime2/handle.rs` | doc comments; `await_keyhive_channel_settled()` façade |
| `big_repo/runtime2/hub.rs` | `await_keyhive_channels()`; settle inserted into `AllocateDoc` (finalize path) and `create_doc`; `Runtime2Cmd::IsConnectedPeer` arm |
| `big_repo/runtime2/io.rs` | new `reserved_doc_parents()` trait method |
| `big_repo/runtime2/messages.rs` | `IsConnectedPeer` cmd; `fresh_internal_waiter_id` |
| `big_repo/runtime2/native.rs` | `reserved_doc_parents` impl |

**Removal landed by lane `8b95f253`, which then hit the same 30-min wall.** Parent-verified on disk:
`jj diff --stat -r 'main..@' -- src/big_repo` = **0 files**; `await_keyhive_channels`, `IsConnectedPeer` and
`reserved_doc_parents` are all absent. Evidence from the lane's own run chain (`/tmp/removal-chain{2,3}.log`):
**34/34 twice** under 2× oversubscription, and the **890-test 4-crate regime with the machinery removed shows
zero failures: **890/890 passed** (232.7 s) — including `iroh_live_sync_bidirectional_after_clone` (**PASS 62.5 s**,
the exact test whose failure restored the machinery last time) and
`a_peer_holds_its_revoked_branch_until_the_delete_lands_on_the_doc_channel` (**PASS 31.9 s**). The previous
falsification does not reproduce. Stray edit reverted: `daybook_core/Cargo.toml:131` comment.

Operator's position: the diff is not useful; remove it.
**Decision rule: calm/narrow green is NOT evidence** — that is exactly the trap above. Evidence must
come from a load regime (the `sync::` module in parallel, not a single test alone).

Investigation lanes: `8b95f253` (`src/big_repo` diff **removed**; timed out at the 30-min wall after landing
it) and workflow `52ad310a` (3 forked lanes: big_sync/big_sync_core verdict = KEEP; subduction revocation
check = item 3 corrected; generation-compare envelope explanation).

## Open item 2 — unsettled decisions (never answered by the operator)

- **(a) Un-swallow the settle-round failure** — `src/big_repo/runtime2/hub.rs:386-389`:
  `if let Err(error) = done { tracing::warn!(...) }` then `Ok(())`. A failed settle silently leaves
  the pre-settle window open. Recommendation was to propagate as `Res`. Violates the no-swallow doctrine.
- **(b) Restore two weakened test asserts** — `assert_peer_summary_arm`
- **(b) Two weakened test asserts** — `assert_peer_summary_arm` (`src/big_sync/rpc.rs`): the match arms
  went from `(Permitted, Ok(..))` / `(Refused, Err(ListPartsError::UnkownParts { unkown_parts }))`
  asserting `unkown_parts == [part_id]` to `(Permitted, answer)` / `(Refused, answer)` asserting only
  that the answer map is empty. That is consistent with the intentional removal of the explicit
  `refused` wire set (absence = refusal), but no arm now catches an unexpected shape. Separately, the
  FakeRpc cursor-advertisement `assert_eq!` in `src/big_sync_core/tasks/decide_peer_strat.rs` was
  **deleted** (replaced by the new `omit: Set<PartKey>` fake mode), so "the asker must advertise the
  cursor it holds" is now unasserted.
- **(c) Settle-ordering regression test** — none pins the ordering. Injecting point exists
  (`inject_runtime2_evt_for_test`, `edge.rs:1308`); `/tmp/pinesc/r1-round3.log` holds the repro shape.
  Also: `grant_doc_access` (`keyhive.rs:1152`) has no settle and leans on manual content (comment only).
- **(d) Per-call-site review of the 11 `open_sync_node(..., false)` flips** — the rename landed
  (`open_sync_node_no_blobs`), but which call sites genuinely need workers off was never decided:
  **17 sites** across `sync/tests.rs`, `sync/tests/ladder.rs`, `sync/tests/stress.rs` lost ambient
  blob-worker coverage by default.

## Decisions taken this session (2026-10-25)

- **big_repo diff: remove it.** Operator: "these probably aren't useful, let's remove them" — pending lane
  `8b95f253`'s load-regime verdict. If removing it re-opens the `MissingPrekeys` race, the lane must restore it
  byte-exact and bring the evidence back rather than shaping a workaround. Note this makes (a) moot: un-swallowing
  `hub.rs:386-389` only matters if the settle machinery stays.
- **big_sync / big_sync_core diffs: under investigation.** Operator is "super suspect": the net effect may be
  test churn only. Lane 1 of workflow `52ad310a` owns the verdict. (b)'s two weakened asserts and (d) live in
  these two files, so their disposition waits on that verdict.
- **(d) partially agreed** — operator: "some sites need blobs. in fact, another agent flagged that." Also
  coderabbit cb-8, same finding. Per-site review is now required, not optional.
- **flake:** the flaking test and the `TEMP-HUNT` helper are both untouched by this PR (verified: 0 hits in
  `jj diff -r 'main..@' -- src/daybook_core/sync/tests.rs`), so per the operator's rule it is ignored as
  main-side harness behaviour. The PR's *own* shutdown surfaces (boot cancel-guard, feeder `FeederError::Shutdown`)
  stay in scope if shutdown-specific symptoms appear there.

### Round 2 (2026-10-25, after reviewing the lane verdicts)

- **cb-1 — policy confirmed, code and comment aligned.** One drawer per node and no doc-set differentiation
  yet, so every doc created on a node is eligible for its local encryption set; sponsorship narrows eligibility
  to the sponsored set (ADR 005). `repo.rs`'s load path now grants `encrypted_blob_docs` over the three
  inventories as well (they already received it at allocation via `prepare_add_doc`), and the "deliberately
  NOT" comment is replaced. Closed as by-design, not a defect.
- **cb-7 — no version bump.** A `.v2` bump in the PR that introduces the concept is meaningless: the context
  string is back to `daybook.cipherblob.salt.v1`, frozen from the first release. `rs` is now hashed as the
  **4-octet** big-endian value the RFC header carries (it was `u64` → 8 octets), matching ADR §9's list.
- **cb-4 — ADR aligned.** §9 carries exactly one formula (framing-bound) plus the canonical input list and a
  version-pin paragraph that states the bump is not a bump; the "pure function of `(K, P)`" claims are gone and
  the stale salt note in `encrypt.rs:21` is corrected. Cubic's ADR nits fold into this.
- **cb-8 + `ensure_local_blob_from_active_peers`** — the helper has **zero callers in `src/`**: an unwired
  eager-retention holdover. Lane `blob-test-fidelity` owns the helper's fate and the worker-less test fixes.
- **Revocations** — delivery verified correct (item 3 corrected). Lane `revocation-e2e` adds the missing
  narrow-audience test.
- **Domains — DEFERRED to their own PR.** The equality leak is a privacy/metadata oracle (identical plaintext
  within a key scope ⇒ identical representation digest), not a confidentiality or integrity break: the salt
  binds (key, P-digest, framing), so different plaintexts never share a (key, nonce).
- **Crash windows — proposed strategy** (lane `crash-window-strategy` verifying): **T1** make the key durable
  before any tag is rooted (JWK facet written ahead of `set_pair_tags`) *and* make the key-doc identity
  derivable from (content doc, domain) via a reserved id so a retry can find it; **T2** a bounded local intent
  journal `(doc_id, domain, c_hash, stage)` in the local-only presence partition — no key material — written
  before the tags, cleared when the pin lands, so boot recovery can complete or release; **T3 rejected**: a
  blind sweep over `ct:`/`pt:` is unsafe, because a transiently incomplete inventory view makes a *live* pair
  look unreferenced and releasing it makes readers hit an unrooted C. No iroh-blobs change needed — the leak is
  roots, and dropping a tag is the release.

## Lane verdicts (2026-10-25)

### `big_sync` + `big_sync_core` → **KEEP** (lane `bbd7e187`); the suspicion is half-right

Production delta vs main: **+79 / −51** (test: +132 / −40). The load-bearing change is the **responder**
(`src/big_sync/rpc.rs:1326-1367`): on main an unreadable part failed the *whole batch*
(`Err(ListPartsError::UnkownParts)` over the request), and the asker keeps every part named in that error
Pending and re-decides with backoff (`big_sync_core/lib.rs:1464-1485`) — so a granted part riding a request
with one ungranted neighbour is starved **indefinitely**, since the ungranted part stays ungranted and every
retry reproduces the same whole-batch refusal. The PR's per-part shape routes the refused part to
`PeerPartStratDecision::Unkown` (pre-existing, `decide_peer_strat.rs:131`) → per-part retry, readable part
proceeds. Only production consumer: `DecidePeerStrategyTask::run`; the asker side was already in main
(verified: the `.await??` → `.await?` at `decide_peer_strat.rs:112-116`).

- **The `refused` set does not exist at head** — dropped mid-PR; the convention is "absence from `parts` =
  refusal". Verified: `PeerSummaryResult` has only `parts` (`big_sync_core/rpc.rs:324-332`).
- **A (fix, 2 lines):** `big_sync_core/rpc.rs:11-13` still links `[`PeerSummaryResult::refused`]`, a rustdoc
  link to a removed field. Verified.
- **B (note → addressed 2026-10-25):** `MAX_SUMMARY_PARTS = 64` (`rpc.rs:1252`, enforced `:1285-1300`) used to answer an
  over-cap request with an **empty** map = every part refused and retried, indistinguishable from an idle peer. It now
  answers `PeerSummaryError::TooManyParts { requested, cap }`, following the `ListPartsError` payload-level `Result`
  precedent (`big_sync_core/rpc.rs:44-50`), and the consumer fails the round loudly naming the count and ceiling
  (`decide_peer_strat.rs:112-131`). Verified: `big_sync rpc::tests::an_over_ceiling_summary_request_is_refused_by_name`.
- **C (fix):** the FakeRpc cursor assert is now **conditional**
  (`decide_peer_strat.rs:367-378`, `if let Some(asker_cursor) = …`), so it stops firing exactly when the
  asker omits the cursor — the case the contract exists for. Verified. Lane's read-only check says an
  unconditional restore compiles and the module's tests pass (verify8/9).
- **D (no action):** the old panic arm is unreachable now that `answer` is not a `Result`.
- **Caveat:** change 1 is a **wire-format change** on the big_sync ALPN (mixed-version peers cannot decode
  each other's `PeerSummary`) — consistent with "no ALPN bump, mixed versions fail".
- **Parent verification — done** (rounds 11/12). `a_refused_part_of_a_summary_batch_leaves_the_readable_part_decided`
  passes (verify8, 5/5 with the `peer_summary`/`decide` filters), the FakeRpc cursor assert is unconditional again and
  the module compiles clean (verify9, clippy 0 errors), and
  `told_not_cloned_inventory_part_is_refused_until_the_inventory_document_is_granted` passed in the verify2 sweep.

## Open item 3 — post-PR queue (non-blocking)

1. **Crash-window store seam / GC** — a post-install fault leaves a rooted-but-unreferenced pair.
   Declared and tested; collection needs store-level deletion we cannot reach today: iroh-blobs
   `blobs().delete` is `pub(crate)`, `gc_run_once` is private, `BlobsRepo` has no evict.
2. **Domain `keyScope`** — only `keyScope: Document` is implemented. Domain-shared keys need a
   config surface + writer find-or-reuse; the reader (`DocKeySource`) is deliberately scope-agnostic.
3. **CORRECTED 2026-10-25 — revocations *are* notified; this entry was a misreading.** "unclassified →
   zero peers selected → never pushed" read `selected` (the classifier's *attribution* result) as the fanout
   decision. Three mechanisms cover it, all verified in code:
   **(a)** keyhive's Phase 3 deliberately emits revocations for **fully revoked** agents
   (`keyhive/keyhive_core/src/keyhive.rs:1608-1634` — "so that those revocation events can be synced to the
   revoked peer"), which the classifier rolls into `agent_hashes` and selects;
   **(b)** a connected peer with no `agent_hashes` entry is selected conservatively
   (`subduction/subduction_keyhive/src/cache.rs:200-215` — a peer we cannot attribute is indistinguishable
   from a recipient whose identity failed to resolve, "select it conservatively and name it in the
   diagnostic");
   **(c)** a fully unattributable batch is fanned out caller-side to every connected peer except the row's
   source (`runtime2/keyhive_dispatcher.rs:340-352` `unattributed`; `:400-412` `fallback_selected`,
   `reason=unclassified_fallback`; comment: "Unattributable hashes … wake everyone").
   Notifications are payload-free dirty hints and *every* class travels by pull — by design (`big_repo/rpc.rs:20-32`,
   `test2/keyhive_rpc.rs:1-14`). A lost hint is covered by connect-time catch-up (`hub.rs:2588-2602`; the
   notification flag is forced true in production, `lib.rs:351-354`).
   **The real open item is a missing test, not missing delivery.** Every e2e notification test uses a
   *public*-audience event (`create_doc_with_parents(initial, vec![public_agent()])`,
   `create_group_with_parents(Vec::new())`), so **nothing pins a narrow-audience / revocation wake** and
   nothing would fail if (a)/(b) regressed. Wanted (townframe-owned, `src/big_repo/test2/keyhive_rpc.rs`):
   `Pair::boot` with two real agents, name the real coparent, revoke it, assert its keyhive converges
   **without** a manual `sync_keyhive_with_peer`. Lane `70c1ecab`; no `../subduction` change needed (verified
   clean at the pinned rev `44cd1474`).
   Operator question: the classifier's "handle `unclassified` conservatively" obligation is documented but
   unenforced (one caller today) — leave it documented, or move the conservative wake into the library?
4. **Generation-compare reply envelope — what it is, per lane `8a54ab58`.** The window is real but narrower
   than the proposal implies, and **a generation stamp on the reply alone buys nothing**: hive generations are
   *per-node counters*, so the initiator has nothing to compare the responder's number against. A detector
   needs **two samples of the responder's counter, taken on the responder**; the serve-time sample already
   exists (`sync_responder_total`) and the exchange's tail deliberately *echoes* it rather than re-reading
   (`subduction_keyhive/src/protocol.rs:1255-1261`), so there is no later sample. Two residual windows:
   **(A)** the responder advanced after taking its serve snapshot — t_serve→t_complete spans a full round
   trip (tens of ms under load: enough for a coparent prekey to land); **(B)** a **peer-initiated** inbound
   exchange is in flight (the hub only tracks rounds *it* initiates, and correctly refuses to resolve our
   waiters for the peer's — `hub.rs:2953-2958`).
   Options: **A** re-sample in the exchange's tail (protocol-surface; the totals carry *syncpoint* semantics,
   `protocol.rs:744-760`, so it needs a new field, not a redefinition); **B** put a generation on the change
   notification (covers both windows, but promotes a documented best-effort, non-fatal hint into a
   correctness signal — `big_repo/rpc.rs:20-32`); **C** hub-local, zero wire change — when a change
   notification for that peer was latched during the round (`keyhive_notif_pending`, `hub.rs:2879-2906`),
   don't resolve the round's waiters and let the follow-up round admit them (`start_keyhive_sync` already
   clones queued waiter ids, `hub.rs:2780`).
   **Not needed by this PR**: the residual failure is the same loud `MissingPrekeys` the settle narrows, with
   retries on scheduled paths; nothing in the correctness argument depends on closing it. Operator questions:
   give it an owner now or park it with sponsorship? if pursued, A/B/C — and C needs a bound, since the
   settle has *no* deadline at that layer (`hub.rs:347-351`).
5. **iroh-blobs pin** — already a git rev (`Cargo.toml:228`, `dman-os/iroh-blobs@b3d64103`); the
   machine-local `[patch]` is commented out (`Cargo.toml:279-280`). Only re-check that no local path
   ever ships.
6. **`ensure_local_blob_from_active_peers`** — `src/daybook_core/sync.rs:1151`, kept dead: the
   eager-retention path was never wired (the presence plane is deliberately local-only).

## Open item 4 — garden note (garden, not a blocker)

`reserved_facet_blob_digest_never_becomes_a_pin` flaked once under heavy load: teardown
channel-close panic at ~`sync/tests.rs:508` (the test panics when a channel closes during teardown
rather than treating closure as the designed end). Passes alone and in the blob sweep. Candidate for
a teardown-hardening pass, out of scope for this PR.

## Open review findings — PR #49 (tracked 2026-10-25)

Head `128ab23a` (= bookmark `svxwtwvv`). 44 review threads: 18 resolved, **26 open** (9 cubic, 11 coderabbit,
6 dman-os). The latest coderabbit batch (`2026-10-04T04:00:53Z`) carries `commit.oid = 128ab23a`, so its
non-outdated findings are live against head; 3 are outdated. Fetched with
`gh api graphql … reviewThreads(first:100){isResolved isOutdated path line comments{…commit{oid}}}`.

Categories: **RESOLVE** act now · **DISCUSS** needs the operator · **VERIFY** claim to check · **IGNORE** moot.

### coderabbit — live against head

| id | path | finding | category |
|---|---|---|---|
| cb-1 | `drawer/mutations.rs:46` | `prepare_add_doc` passes `encrypted_blob_docs_group` as an allocation parent to **every** new doc, so the three inventory docs get it too — while `repo.rs:507` says "The inventories are deliberately NOT". Either that comment is stale or inventories must not get the grant at allocation. | **DISCUSS** |
| cb-2 | `repo.rs:492` | load path could leave `encryption_inventory_doc_id` unset → boot bails "refusing to run the blob encryption worker". Post-de-seam the field is a concrete `DocumentId` (`config.rs:21-30`) and there is no inventory-less repo; a pre-PR repo simply cannot boot — accepted by "no backcompat needed at all". | **IGNORE** (deliberate) |
| cb-3 | `encrypt/codec.rs:373` | "cap the wire record size before buffering". Already bounded: `validate_record_size(rs)?` at `codec.rs:368` (pre-auth, before `record_len` becomes the buffer budget; `MAX_WIRE_RECORD_SIZE` = 1 MiB). | **IGNORE** (stale) |
| cb-4 | `docs/adrs/003-cipherblob.md:334` | §9 text above the new v2 block still gives the **v1** salt derivation (`BLAKE3-derive("…salt.v1", K ‖ P)`) and calls the salt a pure function of `(K,P)`, contradicting the framing-bound v2. | **RESOLVE** (doc) |
| cb-5 | `pr49-fix-lanes3.workflow.js:37` | Biome/pnpm check fails on the leftover workflow file (top-level `return` in a module). **Correction to the earlier entry:** the file *is* committed in the PR head — `jj st` shows `D pr49-fix-lanes3.workflow.js` — so this would have failed CI, it is not moot. Deleted in the working copy. | **RESOLVED** (in `@`) |
| cb-6 | `big_sync/rpc.rs:1360` | per-part loop ends in `.unwrap()`; a store error panics the handler and drops every other part's answer. Old code unwrapped too, but the new loop makes up to 3×64 store calls per request. | **DISCUSS** (same file as item 1's suspicion) |
| cb-7 | `encrypt/keys.rs:58` | salt hashes `rs` as 8 octets (`u64::to_be_bytes`) but ADR §9 fixes it at 4 octets (the RFC header width). `.v2` is new here and no ciphertext persists across the bump → align at zero cost. | **RESOLVE** (choose 4 octets, or amend §9) |
| cb-8 | `sync/tests.rs:543` | `open_sync_node_no_blobs` disables the pin-deriving workers and `wait_for_blob_bytes` can fetch from active peers, so these tests can pass without exercising blob-scope sync. Same finding as item 2(d); operator **agreed**. | **RESOLVE** |

### coderabbit — outdated

- `encryption_worker.rs` (oid `50555758`): crash/failure after `install` leaves a rooted pair nothing releases →
  **IGNORE**, ADR §19 "the retry IS the collector"; already replied on the PR.
- `drawer.rs` (oid `b9d9ac8b`): stale branch aborts `DrawerRepo::load` via `migrate_doc_authority` →
  **IGNORE**, the backfill was deleted in the final round.
- `big_sync_core/rpc.rs` (oid `b9d9ac8b`): version the ALPN for the changed `PeerSummary` response →
  **IGNORE**, operator decided no ALPN bump.

### other open threads

- cubic ×9: 3 live, all `docs/adrs/003-cipherblob.md` (`:182`, `:292`, `:295`) — ADR wording / AI-prompt blocks,
  likely duplicates of cb-4; 6 outdated against `blobs/encrypt.rs` (pre-split file).
- dman-os ×6 — live: `daybook_types/doc.rs:398` (`is_system_managed` abuse → operator 2026-10-25: **keep as-is** (the encryption worker is the only writer of those facets, so the predicate is the right gate)),
  `big_repo/lib.rs:938` + `runtime2/hub.rs:348` (the big_repo machinery → lane `8b95f253`); outdated:
  `big_sync_core/rpc.rs` (refused set — "a missing part in the response should be enough"), `sync/tests.rs`
  (bool params → rename landed), `drawer.rs` (zero backcompat).

## Commands

- Sweep: `cargo nextest run -p daybook_core --no-fail-fast -E '(test(blobs::) | test(plugs::) | test(sync::)) and not test(stress)'`
- Build regime: serialize on the shared target —
  `flock /tmp/townframe-cargo-validation.lock cargo <args>` with
  `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target`.
- Discriminating test for item 1: `sync::tests::iroh_live_sync_bidirectional_after_clone`
  (plus `sync::tests::a_peer_holds_its_revoked_branch_until_the_delete_lands_on_the_doc_channel`).

## Log

- 2026-10-25 — doc created. Item 1 lane `8b95f253` launched (context fork): remove the `src/big_repo` diff,
  load-regime evidence required, restore byte-exact if the race returns.
- 2026-10-25 — workflow `52ad310a` launched (3 forked lanes): big_sync/big_sync_core net-delta verdict;
  subduction revocation push-carrier (owns fixes, writes in `../subduction`); generation-compare reply
  envelope (read-only explanation).
- 2026-10-25 — review tracker added. `gh` auth is `dman-os` and works through `gh api` (`gh pr view` fails:
  no `.git` in this jj-only checkout). PR head `128ab23a`.
- 2026-10-25 — workflow `52ad310a` completed: `bbd7e187` (big_sync/big_sync_core → KEEP, A/B/C/D),
  `70c1ecab` (revocation: item 3 **corrected**, real gap is a missing narrow-audience test, no subduction
  change), `8a54ab58` (envelope: per-node counter ⇒ a stamp alone buys nothing; option C cheapest, not needed
  by this PR). Parent re-verified the contradicting claims directly in code before recording them.
- 2026-10-25 — `8b95f253` (big_repo removal) still running; deferred its parent verification until it stops.
- 2026-10-25 — `8b95f253` **timed out after landing the removal**. Parent-verified on disk: `src/big_repo` is
  0-diff vs main. Its run chain: 34/34 twice, then the 890-test 4-crate regime with the machinery removed at
  **890/890 passed** — `iroh_live_sync_bidirectional_after_clone` PASS 62.5 s (the test that restored the
  machinery last time), `a_peer_holds_its_revoked_branch…` PASS 31.9 s. Prior falsification did not reproduce.
  Reverted its stray `daybook_core/Cargo.toml:131` comment edit.
- 2026-10-25 — parent applied: cb-1 (policy comment + inventory grants), cb-7 (`.v1`, 4-octet `rs`), cb-4 (ADR §9
  single formula + `encrypt.rs` salt note), big_sync findings A (stale rustdoc link) and C (restored the
  unconditional FakeRpc cursor assert). Verification chain launched to `/tmp/pr49-parent-verify1.log`
  (encrypt module tests, the big_sync_core per-part pin, clippy on the three crates).
- 2026-10-25 — workflow `bdf3de58` launched (3 forked lanes): crash-window strategy (read-only), revocation
  narrow-audience e2e (writer), blob test fidelity + helper fate (writer).
