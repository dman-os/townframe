# PR #49 — replies to the operator

**Convention:** the operator reads _this file_, not the chat transcript. Newest
entry first. The parent agent appends here; subagent lanes must not edit it.
Chat messages only carry the short action summary.

---

## 2026-10-25 — round 16: the pair-root leak ledger is landed

The half-built item is in: the ledger records a pair before its tags, and the drain resolves pairs a crash
left rooted with nothing to claim them (ADR 003 §19).

**Shape.** `PairRoots` gained `doc_id`/`branch_path` columns (nullable) and `attach_provenance`, an `UPDATE`
called from both install paths in `encryption_worker.rs` right after `install`. Deliberately not a new
parameter: those sites had just been threaded with `roots`, and an `UPDATE` is what keeps the invariant
("every rooted pair has a row") intact while the provenance stays best effort. The pin write retires the rows
it owns (`replace_doc_branch_state`, after its transaction commits), and `Ctx::drain_pair_roots` runs at
pin-worker spawn, before the machines.

**The decision, in one table:**

| rooted | pin recorded | a facet names C | action |
|---|---|---|---|
| no | — | — | clear the row |
| yes | yes | — | clear the row (the pin machinery owns the release now) |
| yes | no | yes | keep (the facet machine is about to derive the pin) |
| yes | no | no | release the tags and the row |
| yes | no | uncheckable (no provenance) | keep and warn — never release blind |

Releasing is the dangerous direction (it unroots a live representation), so the uncheckable case leaves the
row and logs instead. The facet check has to read facet *values*: a `cipherBlob` facet's key id is
`{domain}/{facet}`, so the representation digest lives in the value, not the key, and no tag+id index can
answer it.

**Verified:** `cargo clippy -p daybook_core --all-targets --all-features` → 0 errors, 0 warnings (the
intended `clear`/`unresolved` dead-code warning is gone, because both now have callers); `nextest -E
'test(blobs)'` → **97/97 passed**, 155 skipped. New tests: the pure decision table; the release path
(`test_pair_root_drain_releases_only_unclaimed_pairs`, which also pins the leave-and-warn case); the keep
path with a real facet (`test_pair_root_drain_keeps_a_pair_a_facet_still_names`); plus a provenance round
trip across a reboot and the loud failure when provenance is attached with no record.

**What is left of the leak:** the other half — orphan key documents and pending allocations that nothing
registers. That needs the same shape (a durable record written before the side effect) and has no equivalent
of it yet.

**Also done this round:** the cross-worker race in
`pins_part_worker::tests::blob_pin_facet_key_that_is_not_a_digest_is_ignored` — it waited on the part store and
then asserted the SQL pin rows, two independent workers of the same machine, so it could legitimately read
`[]`. It now waits on the record it asserts (`wait_for_pin_row_count`).

**And the codec fix the operator flagged:** `PlainPair::encrypt_record` — the serving path's per-64-KiB-record
encrypt — no longer does `read_bytes_at` → `to_vec()` and then grows that buffer through `push`/`extend` (up to
three allocations per record). It reserves `content + RECORD_OVERHEAD + pad_zeros` once and encrypts in place
through the new `Cipher::encrypt_record_in_place`; `Cipher::encrypt_record` (owned `Vec`) is a thin wrapper over
it, so the bulk encode paths are byte-for-byte unchanged. clippy 0/0; 49/49 encrypt/codec/serve/cipher tests.

**Open next:** the relay retention test and the Tier 2 wiring
decision; and the other half of the leak.

**One more finding, captured rather than guessed at.** In the final 97-run the same blob-sync test failed at
17.9 s, and this time I have the assertion text that was missing before: `error finalizing allocated doc in big
repo` → `keyhive doc creation failed: individual 0x7a7d701a7e6ae429021bb52e7a6418360faee7c909b1af1e1621bdae01c24e9f
has published no prekey to select from`, raised at the test's `drawer.add` *after*
`wait_for_sync_convergence` had returned. So it is a keyhive **prekey propagation** race — not the pin path, and
not the AGENTS-documented reschedule class (that one is a local-policy rejection, and rescheduling is its designed
handling). It is load-triggered: it has also passed 3/3 isolated and in 4/4 full runs. It needs a call I would
rather not make silently: either the allocation path waits/retries while a member's prekey is missing, or the test
does. Worth knowing: the removal of the big_repo settle machinery may be what used to cover it.

---

## 2026-10-25 — rounds 14–15: the 100-docs failure explained, and the six new findings triaged

### Round 14 (re-landed): the 100-docs test now sits in the `long_af_test` class

`long_test_iroh_clone_sync_batch_100_docs_with_blobs` → `long_af_test_...` (the class filter is prefix-based:
`long_test` = `60s x 4` = 240 s ceiling, `long_af_test` = `60s x 8` = 480 s). References updated in
`sync/tests.rs:1390` and `blobs/permission_writer.rs:1149`; no CI filter drops the renamed test; the stopped
diagnosis fork's transient flip was reverted, so both of its nodes are workers-off again.

The failure was a **timeout**, and the captured log says so plainly: `(test timed out)` / `1 timed out` at
**240.129 s** — not an assertion, and not the 141 s the first fork's prose claimed. The 200 ms before the kill
are a retry storm: `policy rejected commit … error=document not found` plus
`big sync object task failed; rescheduling … worker=daybook-docs`, task ids still climbing. That is the
big_sync↔keyhive race AGENTS.md documents, landing on the *docs* worker — the production shape multiplies doc
deltas (one facet write per representation) and starves the doc worker until the ceiling fires. The class change
is the right lever; whether it converges inside 480 s is a greening-pass question.

### Round 15: six new findings (2026-10-25 14:36–15:37Z), and one correction of mine

- `serve.rs:139` "full bytes clone or rc clone, 64 KiB every time?" — `Bytes::clone()` is refcount-only, so the
  cache-hit path is free. The cost is the single-slot record cache (an evicted record is re-encrypted), not the
  clone. No change.
- `download.rs:1` "GC for failed downloads? tmp dir?" — valid. Spill state is
  `<ledger root>/<base64url(C)>/{spill.bin,meta.bin}`, removed on success, **never swept** when abandoned (a
  retry of the same C reuses it, so it is bounded by distinct C's). There is no production root at all: the only
  construction sites are two tests with a tempdir, and the struct's doc says it lives outside the blob store on
  purpose. Deferred to the shared-ephemeral-dir decision, recorded in `todo.md`.
- `codec.rs:1` "more idiomatic?" + `:174` "out param for the result?" — **correction:** the `to_vec()` churn is
  in `encrypt_raw_ikm`, the reference-vector primitive with no production caller, and `Cipher::encrypt_record`
  already encrypts in place. The allocation that matters is on the serving path
  (`PlainPair::encrypt_record`, `serve.rs:145-180`): `read_bytes_at` → `to_vec()` → a frame extend that reallocs,
  per 64 KiB record. Fix there instead: reserve `want + 1 + pad_zeros` and extend from the Bytes. Out-param stays
  deferred; no codec rewrite.
- `facet_set.rs:1189` "why blobs in the facet-set index?" — because a `Blob` facet's key is
  `{tag: Blob, id: DEFAULT_FACET_ID}`: the digest lives inside the facet *value*, so a tag+id index cannot
  answer "which docs name digest D" without parsing every Blob facet. `facet_set_doc_blob_facets` extracts it in
  the same hydration pass, and the encryption worker's presence line reads it (`encryption_worker.rs:616`).
  Kept; recorded as future work — it does mix concerns.
- `blobs.rs:367` "sus backcompat seam? re-play every blob for every peer?" — not per-peer (the sink is the local
  part store, one row per blob) and the *write* is an idempotent no-op. But the **walk** is real: a two-level
  `read_dir` over `<root>/objects` touching every blob, at every boot (`rt.rs:334`). It is load-bearing
  reconciliation — a crash between the disk write and the membership would otherwise be invisible forever. Fix
  planned: reconcile only after an unclean shutdown (`plane_may_lag` marker set at boot, cleared on clean stop).
  No existing crash signal to reuse: `surelock` is just keyed locks and there is no `clean_shutdown` anywhere.
  Deferred until after the leak work lands, since it touches shutdown ordering.

---

## 2026-10-25 — round 13: the `no_blobs` knob measured (it buys nothing), and the leak's real decisions

### Is the `no_blobs` opt-out worth having? Measured: no.

- `spawn_blob_workers` is a **production** config field (`daybook_core/rt.rs:77`) gating three workers
  (`rt.rs:375` pins-part, `:389` pin, `:417` encryption). It **does not exist on main** — this PR introduced the
  field and every test opt-out. Both real callers pass `true` (`daybook_ffi/rt.rs:72`, `daybook_cli/lazy.rs:347`);
  only the tests and `test_support.rs:561` pass `false`, so those tests boot a node shape production never runs.
- Cost, measured in one lock session with the flip reverted in the same run: boot-only
  `bootstrap_ticket_in_tests_omits_relay_addresses` `no_blobs` **10.145 s** vs workers-on **9.669 s**; doc-plane
  `iroh_sync_between_copied_repos` `no_blobs` **24.976 s** vs workers-on **19.868 s**. Workers-on is not slower —
  both deltas sit in the noise and point the other way. Booting the workers is free; they idle on channels.
- Recommendation: delete the test-side opt-out and run the production shape everywhere; keep the field only if a
  real observer-node caller ever wants it (none does today). The one behavioural exception is
  `iroh_blob_pin_sync_replicates_and_fetches_blobs` — workers-on would pin the blobs into the scope and hollow out
  the on-demand-fetch assertion it exists for — so it gets a locally named helper with the reason at the site
  rather than a global flag. The 4-node stress cluster is the genuinely unmeasured case, and there workers-on is
  the point.

### Leak ledger: designed, unimplemented, nothing running on it

Zero `record_pending_pair` / `claim_pending_pair` / `clear_pending_pair` / `list_pending_pairs` in `src/`. The one
attempt wired the record half only — rows accumulate forever, and each row is itself a GC root — so it was
reverted, and the work was then parked on the operator's "aside from the leak fixes". Reporting gap on my side:
the design was recovered but never re-assigned.

Two decisions, the second being the substantive one:

- **D1 — where the ledger lives.** The todo entry names two different homes: "copy the `DocReservation` shape"
  (SQL, keyhive storage, swept at boot by `authority.rs`) *and* "`pending:<C>` rows in the store's tag namespace".
  Recommendation: the SQL/reservation shape, because the tag namespace *is* the GC's liveness oracle and a ledger
  inside it is self-referential — the mistake the reverted attempt made.
- **D2 — what "release" means.** Row + document gone → drop the tags; row + document present → re-run completion
  (idempotent). Recommendation for "drop": **pins only, never bytes** — remove the tags we wrote and let the store
  GC reclaim the ciphertext if nothing else roots it. That closes class 7 with no destructive boot-time operation
  and no settledness proof, plus one guard: only pairs whose key material we own, never received blobs.
- Same mechanism as the pending-allocation drain (enumerate a durable local record at boot, resolve, complete or
  drop) — land both in one pass over a shared drain skeleton.

---

## 2026-10-25 — round 12: honest accounting of the 25 open review threads

You pushed back on whether items were being waved away as "stale/moot" from memory. You were right in one
place: I had grouped cubic's three live ADR threads as duplicates of cb-4. Two are duplicates in substance
(`:292` the framing is now bound into the salt, `:295` the canonical input-encoding list is in §9);
**`:182` was not**, and it is fixed now — §11's "does not correlate their ciphertexts" is qualified to
*different* representations, and the identical-plaintext case is stated as what it is: §9's determinism makes
salt/CEK/ciphertext/`representation.digest` byte-identical within one key scope, so digest equality is an
observable metadata property rather than a confidentiality break. Separating representations that share a JWK
into distinct encryption domains remains future work.

I then re-checked every unresolved thread against the code instead of the tracker. The 25 break down as:

- **Fixed, verified against the tree now — 15.** coderabbit cb-1, cb-3 (`validate_record_size(rs)?` runs
  before `record_len` becomes the buffer budget, `codec.rs:368`), cb-4, cb-5, cb-7. cubic `:292`, `:295`,
  plus all six outdated code findings, each checked: both plaintext-sink sends return `Err` with a comment
  saying a dead consumer must not escalate to the panic handler (`download.rs:113-142`); the ADR states that
  resumability requires the ledger; `validate_record_size` enforces both bounds (`> overhead`, `<= 1 MiB`);
  `finish` accepts the overhead-sized final record and has a dedicated empty-plaintext test; the truncated
  `HEADER_LEN` comment is gone; `RECORD_SIZE` survives only as the default plus test uses. dman-os bool-param
  rename.
- **Fixed this round — 1.** cubic `:182`, above.
- **Open — 2.** cb-8's per-site audit (fork `dd0cd6e6`, in flight). coderabbit `encryption_worker.rs` ("a
  crash after `install` leaves a rooted pair nothing ever releases") — not invalid: the ADR's
  retry-is-the-collector argument covers the pinned case only, and the unpinned/crash case is the ledger work
  you set aside.
- **Deferred by your decision — 1.** cb-2 (a pre-PR repo cannot boot; no backcompat seam).
- **No code owed — 4.** coderabbit `drawer.rs` (the backfill was deleted), coderabbit `big_sync_core/rpc.rs`
  ALPN (no bump, your call), dman-os `drawer.rs` "zero backcompat" (satisfied), dman-os `big_sync_core/rpc.rs`
  missing-part (that *is* the shipped convention).
- **Reply-only — 2.** dman-os `big_repo/lib.rs:938` ("sus") and `runtime2/hub.rs:348` ("hacky"): both point at
  the settle machinery, which is gone; what remains in that subtree is the race fix and the `docs.map` fix.
- **Kept by your call — 1.** `daybook_types/doc.rs:398` `is_system_managed`.

### The flaky ladder test is load-only, not a standalone reproducer
`verify11` ran `iroh_sync_single_blob_created_before_connect_replicates` 6× and its sibling 3× in isolation:
**9/9 PASS** at 15-31 s each. It failed once in the wide 197-test sweep only. It therefore needs the stress
runs to attribute; I have not greened it, and I am not calling it test noise.

### Paste-ready replies for the non-code threads

- **cubic `docs/adrs/003-cipherblob.md:182`** — Fixed. §11 no longer claims uncorrelation unconditionally: it
  holds *across different representations* (a different plaintext, or the same plaintext under a different
  framing). The identical-plaintext case is now stated as the deliberate consequence of §9's determinism —
  salt, key, ciphertext and therefore `representation.digest` are byte-identical within one key scope, so
  digest equality reveals plaintext equality there. Called a metadata property, not a confidentiality break;
  separating representations that share a JWK into distinct domains is future work.
- **cubic `:292`** — Fixed. The salt now binds the persisted framing fields (4-octet big-endian `rs` and the
  padding-domain octet), so two framings of the same plaintext under the same JWK derive different salts,
  CEKs and nonces. §9 §19 carry the formula and the canonical input list.
- **cubic `:295`** — Fixed in the same rewrite: §9 lists the canonical byte encodings — raw 32-octet JWK
  secret, the raw 32-octet BLAKE3 digest (the multihash *payload*, deliberately not its framed spelling),
  `rs` as 4 octets.
- **coderabbit `codec.rs` (cap the wire record size before buffering)** — Fixed: `validate_record_size(rs)?`
  runs on the pre-auth header parse, before `rs` becomes the buffering budget, and enforces both bounds
  (`> RECORD_OVERHEAD`, `<= 1 MiB`).
- **coderabbit `repo.rs` (create the inventory for existing repos)** — Won't fix by design. That is a
  backcompat seam for a repository shape that predates cipherBlob entirely, and the PR's stated stance is
  that such a repo cannot boot. If we ever need the migration it is a one-shot inventory creation at open.
- **coderabbit `encryption_worker.rs` (crash after `install` leaves a rooted pair nothing releases)** — Valid,
  not closed here, and deliberately not half-fixed. The pinned case is collected by the retry path (ADR 003
  §19); the real window is a crash between `set_pair_tags` and the key becoming durable/findable. The fix is
  a durable `pending:<C>` row written *before* the tags, claimed once a facet names the pair and drained at
  boot. Record-only rows would themselves be GC roots, which is why this lands as one change.
- **dman-os `big_repo/lib.rs:938` ("every diff in big_repo related to this seems sus")** — You were right
  about the original diff. It was the channel-settle machinery (`IsConnectedPeer`, `reserved_doc_parents`,
  `await_keyhive_channels`) papering over a finalize race; it is **removed** — `main..@ -- src/big_repo` no
  longer contains it — and the race is fixed at its source instead: `add_member_to_group`'s preflight no
  longer bails when a document it names is absent from the snapshot, because the encryption worker creates key
  documents at runtime. What remains in that subtree is that preflight fix and the `docs.map` crash-window
  fix.
- **dman-os `runtime2/hub.rs:348` ("heavy handed and hacky")** — Agreed, and it is gone. `FinalizeAllocatedDoc`
  used to complete the allocation (revoke the staging parent, delete the reservation) before the drawer had
  written `docs.map`, so a crash in between left a document with durable authority, no reservation, no staging
  membership and no registration — unreachable and unrecoverable. It no longer completes anything; completion
  moved to `Runtime2Cmd::CompleteAllocatedDoc` after the drawer's writes land, and recovery recreates the
  authority and stops.
- **dman-os `daybook_types/doc.rs:398` (`is_system_managed`)** — Keeping as-is: the encryption worker is the
  only writer of those facets, so this predicate is the right gate.
- **coderabbit `sync/tests.rs` node audit (cb-8)** — Done — one violation found and flipped (verdict below). The 4-node stress cluster is the one decision left open.

### cb-8 audit verdict (fork `dd0cd6e6`, re-verified by me)

The audit applied the rule the helpers' own doc comments already state — workers on for a node whose inventories
a peer pulls or whose blob plane is under test; `no_blobs` for a node that only fetches bytes. **Exactly one
site violated it, and it is flipped:** `sync/tests.rs:220`
`shutdown_stops_the_inventory_writer_after_the_workers_that_serve_from_it` ran workers-off while asserting the
stop *order* of `blob_sync_worker_stop` / `blob_inventory_permission_stop` — ordering tokens of tasks that
never spawned. The other 43 sites obey the rule, and the receivers of the four real blob tests are deliberately
`no_blobs`: arrival is the sync side, and those tests now assert presence before read, so a peer fetch cannot
satisfy them.

Re-verified myself: clippy `-p daybook_core --all-targets` **0 errors / 0 warnings**; the flipped test
**PASS 9.755 s**, `peer_partition_ids_advertise_every_blob_inventory` **PASS 8.661 s**, both ladder blob tests
**PASS 18.45 s / 31.41 s**.

**One item is left for your call: the 4-node stress cluster.**
`long_af_test_iroh_sync_randomized_four_node_stress_converges` runs every node workers-off, and its final blob
check (`assert_blob_parity`, `stress.rs:1250-1268`) is the vacuous pattern — `wait_for_blob_bytes` polls
`get_path`, which pulls from a peer when the file is absent, so the assertion materializes what it asserts.
Facts that bear on the decision:

- the test **is in the default/CI regime** (`.config/nextest.toml` only widens its slow-timeout; it is not
  excluded), so this is CI-visible, not a local-only shape;
- `wait_for_blob_replicated` (`tests.rs:905`) is the shared presence-before-read helper already used in
  `ladder.rs`;
- with every node workers-off, hardening that assertion alone should go **red**, because nothing derives the
  pins that make the blob a synced part — the bytes only ever arrive by the fetch path the vacuous helper
  hides. That red is the honest signal; the fix is to run the workers on the cluster (seeds first:
  `open_cluster_nodes` `stress.rs:358`, `init_and_copy_repo_cluster` `:311`) **and** harden the assertion;
- that can only be validated by a long 4-node run, and a red there is as likely to be a real product gap as a
  test-shape problem. **I have not run it** — my sweeps excluded `stress` by filter — so its current state is
  unknown to me too. Flagging rather than guessing, and not flipping the heaviest test in the tree behind your
  back.

---

## 2026-10-25 — round 11: three forks hit the wall; `docs.map` fixed, leak ledger reverted with its design recovered

Your instruction was: fix `docs.map`, solve the worker leaks, make `MAX_SUMMARY` error out, use forks. Three forks ran;
all three hit the 30-minute wall and I recovered each one from its diff and transcript.

### (A) `docs.map` crash window — fixed, and I finished it

The fix is what round 10 prescribed, and it is in the tree:

- `FinalizeAllocatedDoc` no longer takes an allocation's `pending_group` and no longer completes anything
  (`big_repo/runtime2/{messages,handle,hub}.rs`).
- A new `Runtime2Cmd::CompleteAllocatedDoc` / `BigRepo::complete_allocated_doc(doc_id, pending_group, content_heads)`
  drops the allocation's two durable records: it revokes the pending-group authority and deletes the id reservation.
- The drawer calls it *after* its own writes have landed — after the `docs.map` commit in `batch_add_inner`
  (`drawer/mutations.rs:267-280`) and after the branch-ref update in the replicated-branch path (`:938-946`).
- `recover_allocated_doc` no longer completes: it recreates the authority and stops, because it has no caller that
  registered the document. Both records survive a crash, so the next boot finds the allocation again.
- `reserved_doc_ids`' doc comment now says what it really is: reservations outlive finalization.

The fork left the tree not compiling: it changed those signatures without updating the seven call sites in
`src/big_repo/test.rs`, and it passed a `DocId` where a `DocumentId` is required (`drawer/mutations.rs:274`). I also
introduced a moved-value error of my own when reordering the handles insert (`:266`). All three are fixed.

Those three crash-window tests now assert the new invariant instead of the old one, which makes them its
documentation: finalize leaves the reservation *and* the pending-group authority in place
(`allocate_and_finalize_pending_document_lifecycle`, `reserved_document_crash_windows_are_recoverable`), recovery
leaves both alone and stays repeatable (`staged_document_reservation_recovers_after_repository_reopen`), and only
completion drops them.

One residue, now in `todo.md`: because recovery no longer completes, an add that dies before its registration stays
pending forever — enumerable and idempotent, but never re-registered and never discarded. The drain for that is the
next item on this thread.

### (B) The leak ledger — reverted, design recovered

The fork wired only the *record* half: `record_pending_pair` before `set_pair_tags` in `encrypt/serve.rs`, with
`claim_pending_pair` / `clear_pending_pair` / `list_pending_pairs` never called. A row is itself a GC root and nothing
cleared it, so that state would root every pair this node ever creates, forever — strictly worse than no ledger. It
also did not compile (`claim_pending_pair`'s error mapping was missing). I reverted its three files to the PR head
(`jj restore --from 128ab23a -- blobs/encrypt/store.rs blobs/encrypt/serve.rs`, plus the re-export block in
`encrypt.rs`), keeping my own salt/`rs` work in `encrypt/keys.rs` and the salt doc note in `encrypt.rs`.

The recovered design is in `todo.md`, and it is worth keeping because it was nearly complete: rows as
`pending:<C>` / `pending:<doc_id>:<C>` tags in the store's own tag namespace, so a row shares the durability of the
tags it guards and is a GC root exactly while the pair needs one; `record_pending_pair` immediately before
`set_pair_tags`; `claim_pending_pair` once a facet names `C`; `clear_pending_pair` on release; and a boot drain over
`list_pending_pairs` that drops the tags of any row no facet names. That drain closes class 7 (never pinned, invisible
to the diff-driven release) as well as the crash windows.

### (C) `MAX_SUMMARY_PARTS` — errors out now

`PeerSummaryError::TooManyParts { requested, cap }` follows the `ListPartsError` / `LeafBucketsRequest` payload-level
`Result` precedent (`big_sync_core/rpc.rs`), and the over-cap arm answers with it instead of an empty parts map that
downstream could not tell from "this peer has nothing pending". In the tree; verification in flight when this was
written.

---

## 2026-10-25 — round 10: `pending_documents` is a staging group after all, and `docs.map` has a hole

### Correction to round 9 (you were right)

`pending_documents` **is** a staging group, and its membership **is** transient. I read the allocation side and missed
`complete_reserved_doc` (`src/big_repo/keyhive.rs:949-983`): at completion, if the document is still in the pending
group it is **revoked** from it (`revoke_group_from_doc`), and only then is the reservation deleted. So the group's
members at any moment are exactly "allocated but not yet completed" — a `/tmp` branch, as you said.

That also explains the `authority.rs` comment, which I misread as "the group can never be a list". Both indexes are real
and cover *different* windows: a merely reserved id has no Keyhive document yet (so the group cannot name it, and
`reserved_doc_ids()` is the only handle), while a finalized-but-not-yet-complete document is only in the group. Round
9's "nothing ever removes a document from it" was wrong.

The chain, runtime side:

- `FinalizeAllocatedDoc` (`runtime2/hub.rs:349-455`): `stage_allocated_document` (comment: *"This is the recovery
  record for a crash in any later step"*) → `finalize_document_authority` (creates the Keyhive doc) → `GetDocHandle` /
  `PutDoc` → `complete_document_authority` (`:448`) → `complete_reserved_doc` (revoke staging parent + delete
  reservation).
- The drawer writes `docs.map` only *after* that returns (`drawer/mutations.rs:203-228`, `batch_add_inner`, once per
  `prepare_add_doc`).

### The hole: `docs.map` registration is covered by neither sweep

A crash between `complete_document_authority` (`hub.rs:448`) and the drawer's `docs.map` commit leaves: Keyhive
authority durable, reservation deleted, staging-group membership revoked, **no `docs.map` entry**. On reopen
`reserved_doc_ids()` no longer lists it and the group no longer contains it, so `recover_pending_documents` cannot find
it: an orphan that exists in Keyhive, is a member of `content_docs` / `encrypted_blob_docs` / `drawer_group`, and is
unreachable from the drawer. Nothing reconciles Keyhive documents into `docs.map` (`register_existing_doc` is only
called ad hoc, `plugs.rs:601` and `plugs/mutations.rs:45`).

Severity: the window is two awaits wide, so rare — but permanent, and it is our blob-pair class in the opposite
direction: the staging record is released one step *before* the state it vouches for is durable.

Fix shapes, cheapest first:

- (a) Move `complete_document_authority` to after the `docs.map` commit, so the drawer drives completion once its own
  transaction has landed. Restores "reservation and staging membership survive until the document is registered" —
  exactly the rule our pair-tag ledger needs.
- (b) Boot sweep over staged rows: `stage_allocated_document` writes them, but the storage layer today exposes only a
  read (`staged_doc_content`, `keyhive_storage.rs:1076`) with no delete and no enumeration. For any id with no
  reservation, re-register into `docs.map` if the Keyhive doc exists. The `Branch` facet on the branch document records
  `document_id` + `branch_id` (`drawer/mutations.rs:60-64`), so the entry is reconstructible from the branch document
  alone.
- (c) Both.

Not ours and not in this PR (the whole flow is main's); logged in `todo.md`.

### On "a separate one like it for our own"

Right shape, and the granularity has to match: a Keyhive group's members are documents, so a staging *group* is the
natural index for "documents allocated but not yet complete". For blob *pairs* the analogous object is not a group but
one local staging **document** whose facet enumerates pending pairs — same lifetime semantics (written before the
irreversible step, dropped only after the real state is durable), and it inherits the doc layer's crash safety instead
of needing a new sqlite table. The hole above is the requirement either way: never drop the staging
membership/row before the state it vouches for has landed.

### Parent verification of the race fix (lane recovered from the wall)

Lane `18c7b570` timed out at the 30-minute wall; its fix and its log survived (`/tmp/race-fix-verify2.log`: 24 PASS / 0
FAIL, including 20 clone-bootstrap runs, `big_repo::test2::keyhive_rpc` 4/4, clippy clean). Its change to
`add_member_to_group`: the preflight loop no longer fails on `GetDocError::{NotFound, PendingMaterialization}` (logs and
continues), and the `affected_docs` check no longer bails on a document that joined the group between the snapshot and
the grant (that bail was the whole-grant failure, `affected document was not preflighted`). My own run
(`/tmp/pr49-parent-verify5.log`): clone-bootstrap **6/6 PASS** (it failed 1-in-4 before the fix), both ladder blob tests
PASS, `iroh_blob_sync_validates_bytes` PASS, clippy on `big_repo` + `daybook_core` in flight at write time. The fix is
shipped here as its own change per your call.

---

## 2026-10-25 — round 9: `pending_documents` traced (it is not ours, and it is not a GC list)

You asked who uses `pending_documents`, whether it is the "things not added to `docs.map`" list, and whether it
predates the Keyhive pre-key allocation work. Traced:

**Provenance — main's, not ours.** `src/daybook_core/authority.rs` has exactly **one** commit on `main` that touches
it (`dbcf6381efb8 refactor(big_sync): string keys, per part auth (#51)`); this PR adds 33 lines to that file (the cb-1
load-path grants). So the group and the boot recovery walk predate the encrypted-blobs work and are not a consequence
of the Keyhive pre-key allocation work.

**What it is: a creation parent, not a list.** Every locally created document is allocated with it as an ancestor:

- `prepare_add_doc` (`drawer/mutations.rs:41-48`): parents `[pending_documents, content_docs, encrypted_blob_docs, drawer_group]`.
- `finalize_allocated_doc(branch_doc_id, doc_am, pending_documents)` (`:128`) — passed as the parents for the finalize.
- replicated branch path: allocate `:703-717`, `finalize_allocated_doc_from_parent(…, pending_documents, …)` `:800-805`.

Nothing ever removes a document from it, and it carries no payload. It is the local *"this node owns it"* parent group;
its name suggests a stage that the code does not implement.

**The list you were thinking of exists, and it is `DocReservation`.** `reserve_doc_id` (`big_repo/keyhive.rs:814-835`)
writes a durable local reservation (doc id + ephemeral signing key) *before* anything else; `reserved_doc_ids()`
(`big_repo/lib.rs:831`) enumerates them; `recover_pending_documents` (`authority.rs:193-211`) walks them at boot and
finalizes each with `pending_documents` as parents; `delete_doc_reservation` runs after finalization
(`keyhive.rs:980`, `keyhive_storage.rs:1759`). The comment at `authority.rs:194-197` says why the group cannot serve as
the list: *"the pending group cannot enumerate a merely reserved public key before a signed Keyhive authority exists."*

**`docs.map` is a third thing.** It is the drawer's Automerge map `doc_id → DocEntry` (`drawer.rs:297`,
`drawer/events.rs:188` prefix handling, `docs.map_deleted` tombstones). A reserved-but-unfinalized document is in
neither `docs.map` nor Keyhive: it exists only as the reservation row plus whatever blobs were staged under it. That is
exactly the state crash-window 1 in `big_repo/test.rs:742-760` exercises.

**[Correction to round 6.]** I claimed the "temp-group idea is already half-built" and blamed `prepare_add_doc` for
*also* allocating into `content_docs`. Reading `:41-48` shows it allocates into all four groups **at once**, so nothing
stages and nothing later promotes: documents are published into their domain groups at creation. There is no
create-then-publish path for documents, and `pending_documents` membership is permanent. The half-built thing I was
reaching for is a better analogue: the reservation.

**On "a variation of /tmp branches":** right in the way that matters. Like an unshared temporary branch, a reservation
is local, referenced by nothing outside the node, and therefore cheap to discard — cleanup is deletion, not
reconciliation. The difference is where the discardable unit sits: the reservation is discardable, the group
membership is not. `recover_allocated_doc` returns `false` for an allocation with no staged content and the loop does
`// Allocation without staged content remains a GC candidate` (`authority.rs:206`) — so the GC work is **named and not
implemented**, for reservations too. The repo now has this gap twice: reservations-without-content, and our pair tags.

**Root ledger — approved and recorded.** T2 is a go. The pattern to copy is `DocReservation`, not the drawer group:
a durable local record written before the irreversible step (rooting the `ct:`/`pt:` tags), deleted after the release
completes, replayed/swept at boot. Two points to carry over: (a) the record must live somewhere already crash-safe on
the same device — reservations ride the local secret store (`store/sqlite/secret_blobs.rs:24`, today `reservations/`);
(b) ship the discard half this time, since the precedent stopped at a comment.

---

## 2026-10-25 — round 8: blob-scope tests made honest, and the race they exposed

### The lane's work

- `src/daybook_core/sync.rs`: the dead eager-retention helper `ensure_local_blob_from_active_peers` is
  **deleted** — zero callers repo-wide, and the live equivalent is `BlobsRepo::ensure_hash_materialized`.
- `sync/tests.rs` and `sync/tests/ladder.rs`: vacuous reads replaced by presence-before-read, and the seeds now
  run the production blob workers. (`sync/tests.rs` around :570 also gained a legitimate precondition: the
  plaintext inventory's `BlobPin` facets must exist before the clone, since a blob scope does not exist until
  the pin worker routes the `Blob` facet into the inventory.)

**The vacuity, exactly — cb-8 named the wrong hop.** `wait_for_blob_bytes` does no fetching; it polls
`BlobsRepo::get_path`. The fetch is production code one hop down: `get_path` (`blobs.rs:568`) calls
`ensure_hash_materialized` when the object file is missing, and that (`blobs.rs:423-437`) walks
`active_peer_ids()` calling `ensure_local_blob` — a direct download from a peer. So any test that *read* bytes
could pass with the blob scope delivering nothing at all. With the workers off and presence checked first, both
ladder tests reported *"full sync waiter satisfied"* and then received **no bytes at all** within 60 s: the old
green came entirely from the on-demand fetch.

### Parent verification — 3 of 4 green, and the failure is a real race

- The four blob-scope tests: **3 passed / 1 failed** in my own run; `clippy -p daybook_core --all-targets` clean.
- The failure is `clone provision rpc failed: affected document was not preflighted: <doc id>` (the lane saw the
  sibling spelling, `document <id> is not found`). It is not a test artefact.
- **Mechanism** (`src/big_repo/lib.rs:951-975`): `add_member_to_group` preflights the group's document set
  (`keyhive.group_document_ids(group)` → `into_ready` → capture heads), then calls keyhive's
  `add_member_to_group`, then requires every id in the returned `affected_docs` to be present in that snapshot —
  and **bails** if not. Between the snapshot and the call, a document can join the group; with the blob workers
  running, the encryption worker creates a key document, so the affected set is larger than the preflight set.
  Enabling the workers in a test is what surfaced it.
- Why it is PR-relevant: this worker is the first thing in the tree that creates documents at runtime, and the
  clone-provision path is exactly what real told/clone nodes use. The honest test now forces the fix: with the
  workers off the test fails *deterministically* (no pins ⇒ nothing to replicate), so making the test green and
  fixing the race are the same job.
- **Correction to the lane's report:** it believed another writer had added a test to `sync/tests.rs` around
  :2474-2760. Checked: that file's diff adds and removes **no** test function. It misread a pre-existing sibling
  test that already used `wait_for_blob_replicated`.

Lane `18c7b570` now owns the race: reproduce, pin the mechanism, fix it if the fix is clearly product-side, and
report options if it needs a decision (it may: the preflight exists to capture heads *before* the grant, and a
document that appeared in between can only be handled by re-snapshotting or by capturing its heads after the
call).

---

## 2026-10-25 — round 7: revocation notification e2e landed, parent-verified

Lane `af6e9efd` delivered `revocation_notification_reaches_the_revoked_member_without_manual_sync` in
`src/big_repo/test2/keyhive_rpc.rs` (+~130 lines, two local helpers). I re-ran it rather than reading it:

- `keyhive_rpc` module: **4/4 passed**; the new test alone **1/1 twice** (~1.1 s each).
- `clippy -p big_repo --all-targets`: **clean**.
- Wire evidence from the lane's instrumented run: the revoked member ingested `8afa8f9a:revok` plus the
  revocation's CGKA ops, in rounds only the creator's notification can have triggered — no
  `sync_keyhive_with_peer` after boot, and the connection predates the document, so connect-time catch-up
  cannot cover for a missing notification.

### Finding: a creation coparent cannot be revoked by its creator

The requested shape (name the real coparent, then revoke it) fails with
`revoke failed: Proof missing to authorize revocation` (`big_repo/keyhive.rs:1184`). Verified in the pinned
keyhive:

- `Document::generate*` wraps the genesis delegations in an `EphemeralSigner` — the document's *own* signing
  key, discarded after generation (`keyhive_core/src/principal/document.rs:198,250`) — so a document's
  coparents are delegated by the document, not by the creator;
- `Group::revoke_member` authorizes a non-root signer only when the target's issuer is the signer, an ancestor
  proof was issued by the signer, or a membered-intermediary proof exists; none hold for a coparent, so it ends
  at `RevokeMemberError::NoProof` (`keyhive_core/src/principal/group.rs:626`).

The test therefore revokes a member the creator itself *delegated* to (creator-issued → supported), which is
still a narrow-audience revocation batch — the coverage gap. Making creation-parent revocation work is a
separate keyhive-side change (issue coparent delegations from the creating agent, or add an explicit proof
path). Recorded in `todo.md`.

### The honest limit of the new test

The harness subscriber connects with no application peer identity, so the dispatcher falls back to
conservative fan-out (`big_repo/rpc.rs:136`: *"BigRepo RPC subscriber has no registered application peer
identity; Keyhive change notifications for it fall back to conservative fan-out"*). The test pins
delivery-and-application, **not** the attributed selection path (`agent_hashes[revoked_agent] → selected`): a
regression there would still be woken by the fan-out. Pinning it needs the harness to register a subscriber
identity, or an assertion on the `KEYHIVE_DISPATCH_DIAG` reasons. Recorded, not done.

---

## 2026-10-25 — round 6: the leak class, the temp-group idea, and why there's no GC

### A. Your temp-group idea is already half-built — the missing half is the *deferred grant*

`pending_documents` already exists, and it is exactly the temporary group you described:

- it is **local** (`authority.rs:11`: `PENDING_DOCUMENTS_GROUP_KEY = "local.authority.pending_documents_group"`);
- a boot sweep already exists: `recover_pending_documents` (`authority.rs:194-212`) walks
  `reserved_doc_ids()` and finalizes each allocated-but-unfinalized document, with the comment
  *"Allocation without staged content remains a GC candidate"*.

What defeats it is one line: `prepare_add_doc` (`drawer/mutations.rs:37-48`) allocates with
`[pending_documents, content_docs, encrypted_blob_docs, drawer_group]`. `content_docs` is the *sharing* group, so
a document is advertised to its audience **at creation** — before it has content, a branch, or anything worth
sharing. That is why a crashed or abandoned document replicates, and why the encryption worker's key document
(the same `drawer_repo.add` path, `encryption_worker.rs:1749`) leaves a replicating orphan on peers.

So your fix, concretely: **split the allocation parents into local vs advertising.** Keep the local ones at
allocation (`drawer_group`, `encrypted_blob_docs`, `pending_documents`) — the worker's access has to be causal,
that decision stands — and move the advertising grant (`content_docs`) to the completion step, where
`register_existing_doc` already grants exactly that set. Then:

- a document that never completes never leaves the node;
- `reserved_doc_ids()` plus the pending group are its GC signal, so the drawer can discard it locally;
- the leak in the *catalogue* stops being a fleet-wide problem and becomes local dead state.

This is a genuine DrawerRepo facility, as you said. Two cautions to design in: the sweep currently *finalizes*
anything with staged content (it is a recovery, not a discard), so a discard path must be a separate, explicit
action; and every doc-creation path (including the clone/bootstrap flows and the replicated-branch allocation
at `mutations.rs:711`) has to move its advertising grant, or the inconsistency just reappears elsewhere.

### B. The warehouse leak: a root ledger, not a sweep — and why GC doesn't help

Why there is no GC, in two parts:

1. **It's opt-in and we don't opt in.** The fork's store has a GC; `run_gc` is spawned only when
   `options.gc` is `Some(..)` (`iroh-blobs/src/store/fs.rs:1724,1769`), and nothing in `daybook_core` sets it.
   Deliberately, and correctly for now: our staging holds an entry alive with a `TempTag` *"until a named tag
   roots the entry"* (`encrypt/store.rs:96-100`), so a GC tick between import and root would delete the entry we
   are about to reference. Enabling GC has to come after the ordering fix, not before.
2. **Even enabled, it cannot collect our leaks.** The GC's mark phase seeds its live set *from the named tags*.
   Our leaks are named tags we lost track of. GC answers "which bytes are unreachable from the roots"; our
   problem is "which roots are unwanted". A pair whose only record is its own tags is, by construction, live to
   the GC.

So the warehouse-side answer is a **root ledger**, not a scan of the projection: one row per pair, written
before `set_pair_tags`, deleted once the pin lands, holding no key material. The drain rule needs no
settledness proof because the releasing branch only fires when there is nothing to lose:

- row present **and** the document is gone from the local doc set → `drop_pair_tags(c_hash)` (idempotent on an
  absent tag);
- row present **and** the document is still here → re-run the completion path, which either finishes the
  representation or, if it is no longer eligible, releases it.

That is also the only mechanism that can release a pair that was **never pinned** (your case 7), because the
diff-driven release path structurally cannot see it.

### C. What I'd do, and where it belongs

- **This PR:** nothing more. Record both as follow-ups.
- **Next PR (crash windows):** T1 — reorder so the key document + JWK are durable *and findable* before any tag
  is rooted (needs the derivable key-doc id, since today the id is random and the only pointer is the facet
  written last) — plus T2, the root ledger. That closes 3/4/6/7.
- **Its own PR (drawer facility):** deferred advertising grants + temp-document discard, as in (A). It touches
  every doc-creation path and the clone/bootstrap flows, so it deserves its own review.

---

## 2026-10-25 — round 5: verification status, and your `wait_for_blob_bytes` suspicion confirmed

### Verified green (parent, on the tree carrying today's edits)

- `blobs::encrypt` module: **42/42 passed**.
- `clippy -p daybook_core -p big_sync_core -p big_repo --all-targets`:
  **clean**.
- `big_sync_core`'s per-part pin
  (`a_refused_part_of_a_summary_batch_leaves_the_readable_part_decided`): **1/1
  passed** — the restored FakeRpc cursor assert holds.
- The `src/big_repo` removal: the full 890-test 4-crate regime passed
  **890/890** earlier.

### Your `wait_for_blob_bytes` suspicion is confirmed, and fixing it unmasked three real failures

The `blob-test-fidelity` lane deleted `ensure_local_blob_from_active_peers`
(zero callers — confirmed) and replaced the byte read in the two ladder tests
with an explicit _presence_ assertion before reading
(`wait_for_blob_replicated(_, 60s)` then a direct `get_bytes`) — exactly cb-8's
fix. With the bypass gone, three tests fail:

- `sync::tests::iroh_clone_bootstrap_syncs_blob_scope`
- `sync::tests::ladder::iroh_sync_single_blob_created_before_connect_replicates`
- `sync::tests::ladder::iroh_sync_single_blob_created_while_connected_replicates`

They passed before **only because the read could materialise the blob from an
active peer**. So the part-sync replication those tests claim to exercise does
not deliver in those scenarios. That is a genuine finding rather than a
regression from the tightening — the lane is on it now (resumed, with the
compile fixed).

### Also done this round

- `docs/DEVDOC/todo.md`, under Sync: blob prioritisation by opening object parts
  (with your retention caveat — eject the part once we hold the blob), "consider
  irpc over iroh-http", and a GC note (a rooted-but- unreferenced pair is
  invisible to mark-and-sweep by construction, and the store GC is disabled).
- cb-6: the three unwraps in the per-part loop are now
  `expect(ERROR_IMPOSSIBLE)`.
- ADR §15 and §19 corrected where they claimed the residue was collectable: the
  retry is the collector _from the point the facet exists_, and a pair whose
  only record is its own tags is a permanent leak — which for `pt:` means the
  plaintext stays.

### My mistake, logged

I broke `repo.rs:514` (a dropped closing paren) with a batch edit while three
lanes were compiling, and that killed two of them at the 30-minute wall. Fixed
within minutes, both resumed. Rule from now on: no multi-line structural edits
to files other lanes are compiling against.

---

## 2026-10-25 — round 4: crash safety ELI5, and the prekey question

### A. The ordering, ELI5

Two **separate** storage systems are involved and we cannot write to both in one
transaction:

- the **catalogue** — the document layer (automerge facets: `JWK`, `cipherBlob`,
  `BlobPin`, `Blob.urls`);
- the **warehouse** — the blob store. A blob there survives only if something
  _roots_ it, and our roots are two named tags: `ct:<C>` → the ciphertext,
  `pt:<C>` → the plaintext that serves it.

A representation is a key, a ciphertext `C` and a plaintext `P`. Making one
servable means: put `P` in the warehouse → encrypt it → put `C`'s outboard in →
root both with the two tags → _then_ publish the catalogue entry that names `C`.

Because we can die between any two writes, the rule is: **order them so every
prefix is safe to crash at** — concretely, _a reader must never be able to
follow a pointer to something that isn't there_. The catalogue entry is the only
pointer to `C`, so it goes last: until it exists nothing can reference `C`, and
dying earlier leaves something invisible rather than something broken.

Release is the mirror image, and its order is the interesting part:

```text
remove the cipherBlob facet  →  the derived pin disappears  →  drop the ct:/pt: tags
                                                                     ↑ dropped BEFORE the removal is written
```

Why that way round — dying in the gap gives one of two states:

- **tags dropped, pin row still there** → a reader gets "not found" until the
  retry lands the removal. Harmless, self-correcting.
- **pin row gone, tags still there** → the tags root a blob nothing will ever
  mention again: a permanent leak, and because `pt:` roots the plaintext, **the
  plaintext stays on disk forever, even after the document is deleted**.

So we choose the first.

### B. The bug the lane found — and why "inert leftovers" was wrong

Rooting the tags is the _first_ surviving act, but the key that makes `C` usable
is written **later**, and a retry cannot find it:

- the key document's id is a random ed25519 key (`big_repo/keyhive.rs:815`);
- the only durable pointer from "this content document" to "that key document"
  is `keyRef` inside the `cipherBlob` facet, which is written **last**.

So a crash after the tags exist leaves a pair that is (i) rooted forever —
release is diff-driven from pin rows, and a pair that was never pinned is
invisible to that diff — and (ii) unrecoverable, because the retry mints a
_fresh_ random key, derives a _different_ `C`, and therefore never touches the
old tags.

I told you earlier this residue was "dead storage, not a correctness hazard".
That came from the ADR and it is **wrong on two counts**, both verified by the
lane:

1. `pt:<C>` roots the _plaintext_, so the leak is a **retention/privacy**
   problem after a delete, not just disk.
2. iroh-blobs' GC can never collect it: the GC's mark phase seeds its live set
   _from the named tags_ (`encrypt/store.rs:50-60`;
   `iroh-blobs/src/store/gc.rs:111`), so a leaked pair is by definition "live"
   to it. (The store GC is not even enabled — `options.gc` is unset.) ADR §19's
   "eventual collect is GC-era work" is unimplementable as written.

### C. Does ordering alone solve every crash class? No. The complete map

| # | crash after                                                 | today                                                                   | with ordering + a findable key                                                                                                                                  |
| - | ----------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 | nothing written                                             | none                                                                    | none                                                                                                                                                            |
| 2 | outboard written, tags not                                  | unrooted entry, accumulates silently (GC disabled)                      | same — accept, or enable GC deliberately                                                                                                                        |
| 3 | tags rooted, key document not yet created                   | **permanent leak, unrecoverable**                                       | **solved**                                                                                                                                                      |
| 4 | key document + JWK durable, facet not yet written           | **permanent leak, unrecoverable** — the key is durable but _unfindable_ | **solved only if the retry can find the key**, which needs a derivable key-doc id or a recorded pointer written before the tags — a reorder alone is not enough |
| 5 | facet durable, resolution not                               | fully recovered                                                         | fully recovered                                                                                                                                                 |
| 6 | rotation: any of 3/4/5 for the _new_ pair                   | one leaked pair per attempt                                             | solved as 3/4                                                                                                                                                   |
| 7 | created and released so fast the facet's net delta is empty | **permanent leak** — never pinned, invisible to the release diff        | **not solved by ordering**                                                                                                                                      |

So ordering (plus a findable key) closes 3, 4 and 6. Not closed by ordering: a
document deleted before any retry reaches it, and case 7. Both are closed by the
same small thing — **one row written before the tags**
`{doc, blob, key_doc_id, c_hash, phase}`, deleted at the commit point, drained
at boot by either finishing the pair or dropping its tags. **No key material in
it.**

Order of work I recommend: (1) reorder + make the key-doc id derivable (closes
the leak class for live documents; needs a small big_repo/keyhive API addition);
(2) the one-row journal (covers 3/4/7, including documents that disappear); (3)
fix the ADR where it is now known wrong (§15:557, §19:594-598, §19:843-849).

### D. Your prekey question: how can we read a principal whose prekey hasn't landed?

Short answer: **we cannot learn a principal from the projection without its
prekeys — that property is intact and it is exactly what the earlier PR built.**
My phrase "read of the local projection" was too loose. Precisely:

- **Where a coparent's name comes from: the caller.** `create_doc` /
  `generate_group` are handed a `parents` list (the settle passes
  `crate::keyhive::authority_peer_keys(&parents)`), i.e. it comes from our own
  config, a local user action, or a delegation we ingested earlier — not from
  the projection.
- **What generation then does:** it looks that id up in the _local_ keyhive
  projection and asks for a prekey to select. `MissingPrekeys` means the id is
  known but its **own** registration/prekey op is absent here — see
  `explain_missing_prekeys`: the _identifier_ travels inside other principals'
  events (a delegation names the delegate), but the individual's node "is only
  ever born from the individual's own op".
- **Your property:** when a _served exchange_ teaches us about a principal, that
  principal's prekey material rides the same exchange (`close_references` /
  `prekey_closure_for_agent`). So once an exchange is ingested we never hold the
  name without the prekey.
- **The gap is ingest timing, not projection content.** The exchange carrying
  that material can be (A) served before the peer published it, or (B) still in
  flight — a peer-initiated exchange is not a round we can await. Until it
  lands, the id we were handed locally has no prekey here.
- **So the error is legitimate and loud**, and the retry after the exchange
  lands succeeds. That is why removing the settle machinery did not fail the
  890-test suite: it turns a rare wait into a rare retry.

Net: the property you remember is real and verified; the settle was covering a
race in our own pipeline rather than a protocol hole; and the envelope proposal
only makes that wait exact — it is not a prerequisite for this PR.

---

## 2026-10-25 — round 3

### 1. Blob prioritisation via object parts → recorded in `todo.md`, next PR

Your shape: instead of iterating connected peers and pulling
(`ensure_local_blob_from_active_peers`), mirror what `doc_workers` do for docs —
**open object parts for the blob on every connected peer**, which makes
big_sync_core's replay prioritise it, and if we are authorised for it the bytes
arrive through the normal replication path.

Retention differs from docs because blobs never change: keeping the part is
useful if the blob lands later, but we cannot let every blob-read path add a
part, because unlike doc workers there is no lifecycle that removes it. The
natural lifecycle is **eject the blob part once we hold the blob**. Recorded
with that caveat.

Separately: lane `blob-test-fidelity` is establishing whether
`wait_for_blob_bytes` is a test-only bypass. It does look wrong — it can satisfy
a blob-scope assertion without blob-scope replication ever happening.

### 2. Crash windows — does ordering solve every class?

Ordering closes the class that actually leaks and leaves two residues, one of
them accepted.

- **Closed by ordering (T1 — key durable before the first tag):** every crash
  _and_ every non-crash failure from the moment the key is durable onward.
  Everything downstream is a deterministic function of (key, plaintext,
  framing), so a retry re-derives the identical `C`, finds the registered pair
  and completes the facet write. Today the pair tags are rooted at step 1 and
  the key is written at step 2 — that is exactly the window that leaves a pair
  nothing will ever release.
- **Not closed by ordering:** a document deleted before any retry reaches it.
  That needs a journal (your "current working items" intuition) or explicit
  acceptance. The journal must hold **no key material** — the JWK facet stays
  the only secret store.
- **Already safe by construction:** release. `release_pairs` runs _before_ the
  removal write, so a crash leaves released-but-still-declared (a reader sees
  not-found until the retry lands) rather than written-but-unreleased (a root
  nothing will ever claim again). A crash _between_ the two tag deletions leaves
  `pt:<C>` rooted — dead storage, not a hazard.
- **Required for T1 to hold at all:** the retry must be able to _find_ the key
  doc. If the key-doc id is random and only reachable through the cipherBlob
  facet (written last), a retry mints a fresh key and orphans the old pair
  forever. The fix is to derive the key-doc id from (content doc, domain) via a
  reserved id — the primitive daybook already uses — or to make a durable
  pointer precede the tags. Lane `crash-window-strategy` is confirming which of
  these the current code gives us.

So: ordering is necessary and closes the leak for live documents; the residue is
deleted-before-retry, which a no-secrets intent journal closes and which we
could equally accept as dead storage.

### 3. Generation / prekeys — is the earlier PR's fix not real?

It is real, and you are right. At the pin, subduction's serving path attaches
the **prekey closure** for every agent a served batch names (`close_references`
/ `prekey_closure_for_agent`,
`subduction/subduction_keyhive/src/protocol.rs:1513+`; keyhive pin `1063446` is
literally "fix: prekey issues"). A peer therefore cannot be _taught_ about a
hive graph principal without also receiving that principal's prekey material.
The previous session verified this in code.

What the settle/envelope discussion is about is a **different and narrower**
thing — not a hole in that property: our own read of the _local_ projection can
happen while an exchange is still being ingested (window A: the peer published
after taking its snapshot; window B: a peer-initiated exchange is in flight).
Both surface as the same loud `MissingPrekeys` and both are retried.

And yes — this is the same area the big_repo machinery was bolted onto. It was a
stopgap for that race; it is now removed; and the 890/890 run (which includes
`iroh_live_sync_bidirectional_after_clone`, the very test that made us restore
it last time) says the race does not reproduce. The envelope proposal exists
only to make that wait exact, so it is **not** a prerequisite for this PR.

### 4. cb-6 — the unwraps

Agreed: they are suspect. They become `expect(ERROR_IMPOSSIBLE)`. A store error
inside that per-part loop is not a recoverable condition, and today it panics
the handler and drops every other part's answer for the batch.

Also recorded in `todo.md`: consider irpc over iroh-http, since we have no way
to express a 500-class error across the RPC surface.
