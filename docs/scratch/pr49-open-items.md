# PR #49 — open items (successor to the pr49-* scratch files)

Written 2026-10-07 when the four `pr49-*` scratch files were retired. This file carries only what is
still live, plus the decisions taken at retirement so nobody re-litigates them. The history
(`pr49-final-leg.md`, `pr49-replies.md`, `pr49-leak-leg2.md`, `pr49-prekey-dive.md`) is gone; nothing
in it is needed to act on the items below.

## Decisions taken at retirement

- **Keyhive pin — HALT, upstream owns it.** The prekey cycle-seeding fix is *not* in the pinned rev:
  `Cargo.toml:177` pins `1063446711` ("fix: prekey issues", bookmark `townframe-changes`), which still
  carries the pre-fix gate `heads.is_empty() && !rotate_key_ops.is_empty()` at
  `keyhive_core/src/principal/individual/op.rs:88`. The fix (`wmwskyrz`, gate reduced to
  `!rotate_key_ops.is_empty()`) exists on a fork branch only and is not an ancestor of the pin. Upstream
  is working on it, so townframe takes no action; the boot census stays withheld until a pin bump.
- **4-node stress cluster — GREEN, closed.** CI ran it *with blob workers on*
  (`open_cluster_nodes` → `open_sync_node` → `boot_sync_node(rtx, true)`) and it PASSED at **339.4 s**
  of its 480 s `long_af_test` ceiling in full run `37516178446` (JUnit `nextest-results` artifact, zero
  skipped). The older ledger entry calling it "all-workers-off with a vacuous assertion" is stale: the
  workers derive the pins, so the read is no longer load-bearing. No hardening now.
- **Load-only ladder flake — ignored for now** (`ladder::iroh_sync_single_blob_created_before_connect_replicates`;
  1 fail in a wide sweep, 9/9 isolated). Operator call.
- **`announce_held_blobs` re-announce — closed.** Re-announcing identical state no longer writes: the
  store refuses a repeated payload write that is not a change
  (`set_obj_payload` guard in `big_sync/part_store/sqlite.rs`, `big_repo/store/sqlite/sedimentree.rs`,
  pinned by `assert_repeated_payload_write_is_not_a_change_contract`). The unclean-shutdown skip
  (`plane_may_lag`) stays unbuilt and parked; `announce_held_blobs` itself is still needed because the
  presence plane is local-only with no journal of what was already announced.
- **Orphan key documents — covered for this PR.** The temporary-document drawer facility *is* the
  mechanism: both encryption-worker key-doc sites (`create_representation`, `rotate_representation`)
  create via `DrawerRepo::add_temporary` + claim, so the reservation and the boot drain
  (`recover_pending_documents` → `drain_pending_allocations`) cover the creation windows. The adjacent
  bug fixed on 2026-10-07 was itself an orphan-key-doc bug: a claim the scan *found* was not replayed
  when the scan was incomplete, so the drain completed the allocation with no `docs.map` entry while the
  claim's `keyRef` still named the document (`daybook_core/authority.rs`, regression test
  `the_boot_sweep_registers_a_found_claim_even_when_the_claim_scan_is_incomplete`).
- **Conservative wake — what the old ledger's question actually was (no action for now).** Subduction's
  public classifier API states a policy for a value it *returns*: `VisibilityTargets::unclassified` —
  "Changed hashes absent from the visible projection — pending (awaiting dependencies) or lagged.
  **Callers must treat these conservatively (notify all connected peers except the known source) rather
  than silently dropping them**" (`subduction_keyhive/src/protocol.rs:145-150`, restated `:2156-2165`).
  The library does not apply it. The one caller that does is our dispatcher, in its own module doc's
  words — "Unclassified hashes — events the visibility projection does not attribute to any viewer
  (contact-card prekey ops are the known case) — fall back to waking every connected peer except the
  source" (`keyhive_dispatcher.rs:15-19`, implemented `:402-404`, with `source_suppressed` for echo).
  **Why it is documented rather than enforced, and why "just move it into the library" does not work:**
  the governing clause is "except the known source", and the source is not part of the classifier's
  input. `VisibilityBatch` carries only `connected` and `changed` (`protocol.rs:127-133`); the
  `&self.peer_id` the classifier also receives is the *local* peer, a different thing. The source
  attribution lives in the caller's durable admission log: rows are grouped by `row.source_id`
  (`keyhive_dispatcher.rs:275-287`) and `is_source = Some(peer) == source.as_ref()` (`:400`). Any
  enforcement variant therefore needs the source handed to the library (e.g. `VisibilityBatch { source }`,
  or `notification_targets(..., source)` returning an already-fanned-out target set), which is a public
  API change in subduction → upstream, parked with the pin halt. Wrapping `unclassified` in an opaque
  type does not help on its own: without the source it cannot exclude it either.
  Distinguish the *other*, library-side mechanism: a **peer** the projection cannot attribute (no
  `agent_hashes` entry, batch carries a locally-visible change) is added to the selected set by the
  library itself (`cache.rs:186-215`), so a caller cannot forget that one. Our `rpc.rs:122-137` names
  the same path when a subscriber's transport identity has no registered application peer: the
  conservative path "still notifies, but the misconfiguration must be visible rather than silent".
- **The generation-compare envelope — parked, and what it is.** The responder samples
  `sync_responder_total = local_events.len() + our_pending_hashes.len()` at serve time
  (`subduction_keyhive/src/protocol.rs:931`), sends it in `SyncResponse`, and re-uses *the same variable*
  in the tail's `SyncConfirmation { confirmer_total }` (`:1255-1263`) — even though the tail has just
  logged `advanced` from ingesting our ops. The initiator's convergence test compares that value against
  the *syncpoint* it remembers for the peer (`local_syncpoint_for_sender == sender_total &&
  sender_syncpoint == our_total && digests_match`, `:741-744`), never against a fresh sample, so a stale
  echo satisfies it. Stamping "the responder's generation" into the reply envelope buys nothing alone:
  generations are per-node counters, so the initiator has nothing to compare the responder's number
  against — a delta needs two samples of the responder's counter, both taken on the responder (serve +
  tail), and only the first exists. Options: **A** re-sample in the tail (needs a new field, not a
  redefinition — the existing total carries syncpoint semantics); **B** a generation on the change
  notification (covers the second window too, but promotes a documented best-effort hint into a
  correctness signal); **C** hub-local, no wire change — when a change notification for that peer was
  latched during the round (`keyhive_notif_pending`, `hub.rs:2856` insert / `:2977` consume), don't
  resolve the round's waiters and let the follow-up round admit them. Second window: the hub refuses to
  resolve waiters for peer-initiated inbound rounds (`hub.rs:2918`, `:2927`), so an inbound exchange in
  flight is invisible to our round's verdict. Consequence: a "converged" verdict can be one generation
  stale, surfacing as the loud `MissingPrekeys` at the next allocation on a scheduled retry path, so
  nothing correctness-critical depends on closing it. If ever pursued, C first, with a bound — the
  settle layer has no deadline.

## Still live (deferred — not this PR)

1. **Facet `key_ref` / value index.** Unblocks three things at once: the claim-status enumeration of a
   *registered* key doc no claim names (the key_ref lives only inside `cipherBlob` facet **values**),
   the claim-scan cost the 2026-10-07 narrowing only reduced, and the pair-drain no-provenance remainder.
2. **Store GC** (`options.gc` unset): releasing roots frees nothing yet; iroh-blobs `delete` /
   `gc_run_once` are not reachable from `BlobsRepo`.
3. **Domain `keyScope`** (only `Document` implemented) + the rotation coparent-revocation limitation —
   own PR.
4. **Download-ledger location** + a sweep for abandoned attempts (`FsDownloadLedger` has no production root).
5. **Garden:** `reserved_facet_blob_digest_never_becomes_a_pin` teardown-channel-close flake
   (`sync/tests.rs:508`) — candidate for a teardown-hardening pass.
6. **`assert_peer_summary_arm` weakened arms** (`big_sync/rpc.rs`): nothing now catches an unexpected
   answer shape (absence = refusal is intentional, but no arm pins the shape).
7. **Upstream:** keyhive prekey cycle-seeding pin bump, once upstream lands the fix.
8. **Operator side:** the PR's review threads, and the "paste-ready" reply drafts that used to live in
   `pr49-replies.md` round 12 — posted or dead, they are no longer on disk.
