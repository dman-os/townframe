# big_repo improvement ledger

Status: Active. Opens a dedicated big_repo lane.
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: Costs, wrong turns and lifecycle defects in `big_repo` (and its keyhive and subduction seams), collected from the node lane's ledger (`docs/scratch/nodule-runtime-ledger.md`, D29, D30 and the To do list) so a big_repo lane can start from one list. Read AGENTS.md first, especially "big_sync ↔ keyhive racing".

## Standing context

- **big_repo is a rewrite candidate** (operator, 2026-11-05, D30). The rewrite waits for upstream's keyhive and subduction rewrite. The awaited piece is subduction's keyhive integration, expected to be rebased onto the keyhive rewrite soon (operator, 2026-11-10). Until then: fix obvious costs in place, and record every further wrong turn here rather than silently fixing it, so this ledger is the rewrite's evidence base.
- **Our forks are ours to fix** (D36): `../keyhive` (jj, branch `townframe-changes`) and `../subduction`. Fix and instrument freely. When the rewrite lands, diff our delta against it and port what still applies.
- Entries below are as recorded when found. Items marked "landed" are fixed. Re-verify an item against current code before working it: several were found before #85's fixes.

## Already landed (for the rewrite's comparison)

- Admission-log readers wake on append instead of a 250 ms poll (`d4d33097`, in #85 as `bea62705`). Note create p50 767 -> 276 ms.
- A commit stores its own change bytes, not a whole-doc `save()` (`88e6109d`). Drawer stored:saved 205:1 -> 8.4:1, node dir per note 147 KB -> 33 KB.
- Descendants-first cold snapshot walk (`2cd3261e`). Reboot at 100 notes 4.12 -> 1.86 s, drawer snapshot decrypts 5,564 -> 107.
- `try_causal_decrypt` takes keyhive's ancestor plaintexts instead of re-parsing them as envelopes (`148c3e96`). That parse preallocated up to 1 MiB per ancestor per walk.
- Reader reconciliation walks a worker-owned reverse-membership index (`MembershipReach`) up from the delegation's subject, instead of `docs_for_agent` over every doc. Summed lookup 38 s -> 0.5 s at 50 notes.
- Frontier and causal checkpoint workers coalesce a per-doc burst into the running task plus one follow-up instead of replace-cancelling (`c53ab910`). Doc-worker caller-dropped replies 287 -> ~40 on seed 2.
- Subduction drops already-incorporated keyhive events under an ingest lock (`8dc7da49`). Connect at 50 notes 7.35 -> 5.66 s.

## Open: costs

- `commit_delta` persistence is ~39% of a small datum write (2.75 ms release) and grows with doc history even with dmeta off (node lane, 2026-11-10, `~/scratch/gatecost*` on mothership). A loose-commit row is ~1.7 KB: the stored blob is the change plus ~770 B fixed, plus a 232 B signed header. Per change, a plain header is ~141 of 185 B (48 B actor, 32 B dep, 36 B command-id message). Not yet profiled to a cause.
- `persist_local_commits` (`native.rs` ~398) runs one transaction per change, each a full `mutate_tree_in_tx` with minimization. An `InsertBatch` mutation exists.
- `load_doc_snapshot` re-walks and re-decrypts history on every call, including each `ReattemptMaterialization` while pending. It also rebuilds the Automerge doc from scratch (~30 MB allocations per drawer load at 200 notes). Check what remains after `2cd3261e`.
- `try_causal_decrypt` builds a fresh `NativeCiphertextStore` per call, so every walk re-runs two SQL loads per ancestor.
- `hydrate_tree` clones the whole `MinimizedSedimentree` per snapshot (not measured).
- The hub's `put_doc` saves the whole doc twice to detect a content mismatch; comparing heads would do.
- Deletes in `mutate_tree_in_tx` rehydrate the tree from SQLite (rare, O(tree)).
- `native.rs` ~2407 calls `refresh_cache()` every 2 s on every node, a loop copied from upstream (`cache_refresh_interval`). Every cache reader already calls `ensure_cache_current` on demand, so the loop looks redundant.
- `big_sync/worker.rs` ~467 waits for idle with a 50 ms poll. Check whether anything outside tests uses it.
- `contains_incorporated_event` hashes all of a doc's CGKA ops per event (fine at 50 notes). A keyhive-side batched lookup if it shows up.
- `MembershipReach` only grows (revoked edges over-approximate). Repeated group-wide reconciles per entry point are not coalesced. `docs_for_agent` survives only for test fixtures and should be deleted.
- The remaining ~33 KB per note is SQLite rows, WAL, indexes and keyhive events. It needs a per-table breakdown.
- Each keyhive `grant_doc_access` costs about 200 ms of a note create, and its reconciliation wait is the dominant cost of a grant (debug figures from D29). `grant_doc_access` carries `INSTR` timing lines to squash before merge.

## Open: lifecycle, correctness and liveness

- **Keyhive Arc cycle** (group <-> `DelegationStore`). Worked around by detaching listener storage on stop (`998b73d9`); not fixed in the fork because upstream is rewriting keyhive. Check the rewrite for the same cycle. A smaller `async_channel` buffered-event cycle also exists.
- **Residual temp-dir leak:** big_repo's own suite leaves ~16 dirs of ~1 MB per run. WIP parked in `git stash` ("wip: big_repo residual temp-dir leak"): test stops, `TempDir` ownership in the stress fixture, `SqliteRuntime::drain_workers`. Low priority.
- `register_bundle_lease` (`doc_worker.rs` ~708) swallows a send error and `registered_rx.await.ok()`, against AGENTS' channel rule.
- `process_pending_fragment_requests` keeps pending fragment work only in memory, so after a crash those boundaries never get fragments (bounded growth).
- Existing stores keep old snapshot blobs until a fragment covers them; nothing repacks.
- **Causal checkpoint feedback loop** (stress seed 1, 2 of 8 runs on aarch64): CGKA update ops keep being minted until the 240 s timeout. Hypothesis: `reconcile_causal_coverage` persists a checkpoint whenever `current_causal_epoch` is None, `persist_causal_checkpoint` calls `force_pcs_update`, which can mint a new op, which retriggers coverage on every node. It damps only by timing. Re-check after `c53ab910` and the `RootConflicted` election (`a4aec85e`).
- **Election resolver liveness:** if the lowest issuer is offline, lacks write access or lacks the doc, the others defer until it returns (a stall). A rank-delayed fallback would need a doc-worker timer. After an `Add` with no concurrent updates every writer still mints once.
- **Grant-then-offline (known property, not a bug; operator 2026-11-10):** a granting node that goes offline before its post-grant causal checkpoint syncs leaves the new reader with ciphertext and no key, until a node with write access and the current key comes online. Peer acks were rejected. The relay e2e (`1d10b144`) now waits for the phone's read, so its "dev offline" claim is hollow. Open: have the test stop dev once the relay's store holds the checkpoint.
- **Unpinned:** `e2e::nodule_wasm::a_cartridge_published_on_one_node_is_installed_and_invoked_on_another` hit 120 s once under load, stalling ~15 s in, around keyhive sync after `publishCartridgeVersion` (`~/relay-fix-bg.log` on mothership). Not diagnosed.

## Seams to watch in the rewrite

- The "big_sync ↔ keyhive racing" note in AGENTS.md: rescheduling is the intended mechanism, and the real failures are in the keyhive pull pipeline.
- `wait_for_keyhive_reconciliation` is local only (the projection watermark reaching the admission watermark). Nothing observes peer receipt, by design.
- The scope key is part of keyhive admission consumers' walker state and retention-reader ids, because a repo database may be shared by nodes with distinct scope keys.
