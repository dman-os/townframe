# Trimming `src/big_repo/test.rs` — findings and port plan

**Status: deferred. Nothing in this document has been executed beyond what §2 lists as done.**
This is a scratch plan, not an ADR. Delete it (or promote the durable parts to an ADR) when
the work lands.

Branch: `refactor/big-sync/byte-keys-and-part-auth`. Head at time of writing: `7ee31c40`.
Measurements are from that revision.

---

## 1. The shape of the problem

`src/big_repo/test.rs` is 4106 lines:

| region | lines |
|---|---|
| test bodies (32 tests) | 2017 |
| non-test top-level items (the stale harness) | 1961 |
| imports, module attrs, blank lines | 128 |

The stale harness is as large as the tests it serves. It predates `test2/`, which has a
better fixture family (`Node`/`Pair`/`Topo`), runs over the **real** big_sync RPC, and
carries tier classification. Every test left in `test.rs` drives the bespoke harness
(`SyncRepoNode`, `run_sync_case`, `run_sync_backend_case`, `boot_repo`, `_boot_disk_repo`,
the in-process `StressBigSyncRpcClient`), so the two assets cannot be retired separately:
each ported test is also a piece of harness that gets deleted.

Two defects are being corrected at the same time as the move:

1. **Not using the harness.** The bespoke double bypassed authorization — the real worker
   checks `read_denied(ReadTarget::Part(part_id), asker)` then `summarize_parts(readable)`
   (`src/big_sync/rpc.rs:1050-1070`), while the double called `summarize_parts(req.inner.parts)`
   with *no asker* and forged the asker from harness bookkeeping. Part-level access control
   was therefore untested on every tier.
2. **Not classified into tiers.** The 32 tests map onto tiers 1/4/5/6/7/8, contract tests
   that belong beside the impl, and single-node invariants that belong in an `e2e` module.

## 2. Already done

| commit | what |
|---|---|
| `34e4a24e` | dropped 22 covered tests (round 1, forks A/B/C), removed orphaned `use autosurgeon::Prop` |
| `7ee31c40` | dropped 5 covered tests + 3 orphaned helpers (round 2, fork D) |
| `dd2f313d` | `test2` runs over the real big_sync RPC: nodes `spawn_big_sync_rpc`, dial `IrohBigSyncRpcClient`, endpoint key = node identity seed, part read granted explicitly in the connect helpers (`Pair::boot_ungranted` for denial tests) |
| `1feaa485` | `BigSyncWorkerHandle::set_replay_hold_ms`; `test2` asks for 200 ms, production keeps 15 s |

File went 5526 → 4106 lines; 27 tests deleted; 397 tests pass; clippy clean.

Triage artifacts (per-test body comparisons, verdicts cited below): `/tmp/triage-a.md`,
`/tmp/triage-b.md`, `/tmp/triage-c.md`, `/tmp/triage-d.md`. 23 of the 32 remaining tests
have an artifact verdict; the 9 `big_repo_sync_backend_*` were not triaged because they need
no comparison (they are a relocation, §4 group A).

## 3. The stale harness inventory (what dies with the ports)

| lines | item | dies when |
|---|---|---|
| 248 | `impl SyncRepoNode` | the last `SyncRepoNode` test ports (§4 C/D) |
| 242 | `impl WireBigSyncRpcClient for StressBigSyncRpcClient` | same |
| 232 | `run_sync_backend_case` | §4 A |
| 175 | `run_sync_case` | §4 D (last user is the FLAG test #1) |
| 119 | `run_restart_reconnect_case` | §4 D |
| 109 | `create_shared_sync_doc` | §4 C/D |
| 104 | `run_remote_change_listener_without_live_handle_case` | §4 D |
| 98 + ~230 | `run_sync_backend_*_case` (4 more helpers) | §4 A |
| 53 | `apply_local_sync_mutation_and_assert_notifications` | §4 D |
| 46 + 43 | `_boot_disk_repo`, `boot_repo` | §4 B/C/D |
| 44 + 28 + 19 | `write_sync_doc_value`, `apply_sync_mutation_in_place`, `apply_sync_mutation` | §4 A/C/D |
| 29 | `wait_for_document_access_notification` | §4 C (domain-listener tests) |
| ~150 | remaining helpers (`recv_change_batch`, `recv_head_batch`, `read_json_doc`, `wait_for_json_doc`, `wait_for_doc_handle`, `initial_content_heads`, `change_hash`, `new_sync_doc`, `make_sync_doc_value*`, `sync_item_note`, `sync_note_snapshot`, `connect_sync_pair`, `wait_for_pair_full_sync`, `set_doc_actor`, accessor helpers, `SyncMutation` plumbing) | §4 |

## 4. Port map for the 32 remaining tests

Cost: **S** = body rewrite onto existing fixtures; **M** = needs a small fixture addition or a
contract decision first; **X** = the port is really a deletion (see §5).

### Group A — SyncBackend contract: 9 tests, 122 lines, target `src/big_repo/backend/test.rs` (S)

`big_repo_sync_backend_{returns_noop_when_heads_match, applies_remote_update,
applies_remote_update_with_empty_part_hints, applies_remote_update_with_multiple_part_hints,
returns_noop_when_remote_payload_is_missing, fetches_missing_doc_when_remote_payload_is_missing,
applies_remote_update_when_remote_payload_is_missing, adds_missing_doc,
recovers_from_put_doc_conflict}`.

They use `contract::assert_sync_backend_case`-style plumbing and **zero** harness symbols — no
node, no transport, no tempdir. Pure relocation beside the impl, replacing `run_sync_backend_case`
and its four sibling helpers (~460 lines out). Their `SyncBackendScenario` fixtures may be
reusable from `big_sync::backend`'s own test module rather than re-derived.

### Group B — single-node invariants: 5 tests, ~290 lines, target `src/big_repo/e2e/*` (S)

No peer involved; `boot_repo`/`_boot_disk_repo` + `tempdir` only. Per AGENTS these belong in a
crate-local `e2e` module, not in a sync tier file. Targets: `Node::boot_with_config(storage)`
for the disk case.

| test | lines | verdict | notes |
|---|---|---|---|
| `causal_coverage_deduplicates_per_epoch_and_rotates_at_unchecked` | 48 | KEEP | causal-coverage accounting asserted nowhere else |
| `local_boundary_commit_stores_fragment_and_prunes_covered_loose` | 59 | KEEP | overlaps `fragmentation.rs:108`, but asserts loose-fragment pruning |
| `startup_audit_repairs_update_persisted_without_checkpoint` | 54 | KEEP | persists a PCS update without its checkpoint, reopens, repairs |
| `with_document_handles_concurrent_writers` | 73 | KEEP-PORT | 8 tasks × 25 writes = exactly 200; stronger tier0 invariants apply after the port |
| `create_doc_with_group_parent_uses_public_group_api` | 55 | **FLAG #4** | nearest `capability.rs:404 tier6_delegate_before_define`, `cgka.rs:198 tier6_parented_document_handle_is_immediately_writable`; decide keep or fold |

### Group C — multi-node scenarios onto existing fixtures: 11 tests, ~1150 lines (S/M)

Fixture surface already available: `Pair::boot`, `boot_persistent`, `boot_without_keyhive_notifs`,
`boot_ungranted`, `connect`, `disconnect`, `restart_left/right`, `Node::boot_with_config`,
`boot_with_config_and_hidden`, `boot_with_scopes`, `boot_with_frontier_workers`,
`Topo::boot_line/star/triangle/relay`, `heads::tier0_invariants`.

| test | lines | verdict | target |
|---|---|---|---|
| `bucket_band_reconciles_after_offline_reopen` | 113 | KEEP | tier4/5: `Pair::boot_persistent` + `disconnect` + `restart_*` |
| `allocate_and_finalize_pending_document_lifecycle` | 93 | KEEP | tier5/e2e — no tier2 test touches allocation at all |
| `staged_document_reservation_recovers_after_repository_reopen` | 65 | KEEP | tier5 (`stage_reserved_doc` + reopen + `recover_allocated_doc`) |
| `reserved_document_crash_windows_are_recoverable` | 90 | KEEP | tier5 (crash-window matrix: durable reservation before content, finalize, reopen) |
| `concurrent_bidirectional_keyhive_sync_is_safe` | 30 | KEEP | tier1 / `keyhive_rpc.rs` |
| `concurrent_writers_with_edit_access_converge_after_bidirectional_sync` | 135 | **FLAG #7** | tier1: is one-round bidirectional convergence a property to pin? |
| `three_node_key_rotation_propagates_to_existing_reader` | 171 | **FLAG #6** | tier6 over `Topo::boot_line`; no candidate asserts an *existing* reader decrypting post-rotation writes from two authors |
| `grant_doc_access_checkpoint_survives_reopen_and_sync` | 200 | KEEP | tier5/8 — three unique assertions incl. historical read at an explicit head |
| `client_keyhive_decrypts_postwrite_blob_after_edit_grant_sync` | 83 | **FLAG #5** | fold `content_ref == postwrite_head` into `tier8_postwrite_blob_decrypts_after_edit_grant`, then delete |
| `authorized_peer_reads_encrypted_doc_after_keyhive_change_notification_without_reboot` | 69 | X (domain listener) | §5 |
| `granted_doc_requires_manual_sync_after_keyhive_notification` | 99 | X (domain listener) | §5 |

### Group D — notification/restart scenarios: 7 tests, ~426 lines

| test | lines | verdict | target |
|---|---|---|---|
| `sync_with_peer_local_write_emits_notifications_while_connected` | 76 | KEEP | tier7 (change listener; live family) |
| `sync_with_peer_remote_change_notifies_without_live_handle` | 17 | KEEP | tier7 (absence half is unique) |
| `sync_with_peer_survives_repo_restart_with_live_connection` | 22 | KEEP | tier5 |
| `sync_with_peer_both_diverged_loses_remote_change` | 23 | **FLAG #1** | name says one branch is lost, body asserts convergence; last user of `run_sync_case` |
| `sync_with_peer_remote_change_notifies_with_live_handle_and_listeners` | 116 | change half KEEP / head half X | tier7 for the change half (§5) |
| `sync_with_peer_local_change_without_change_listener_only_emits_heads` | 105 | X | §5 (entirely head-family semantics) |
| `remote_change_and_head_notifications_survive_handle_reopen` | 67 | change half KEEP / head+origin X | §5; it is the only user of `with_document_with_origin` |

### Group E — no longer needed

The two fixture additions the first estimate budgeted (`subscribe_head_listener` and the
domain/access filter helpers in `test2`) are **not needed** — §5 removes their consumers.
Change-listener coverage already exists in `notifications.rs` (tier7), so the remaining
notification ports use the existing change-listener helper only.

## 5. Notification families: what AFW obsoletes

Measured at `7ee31c40`, exclusions: `test.rs`.

| family | emitted from | consumers |
|---|---|---|
| `BigRepoChangeNotification` (`subscribe_change_listener`) | `doc_worker.rs`, gated by `has_change_listener_interest` | **live**: `daybook_core/stores.rs:70`, `daybook_core/drawer.rs:262` |
| `BigRepoHeadNotification` (`SedimentreeHeadsChanged`, `ColdSedimentreeHeadsUpdated`, `DocPendingSedimentreeHeads`) | `doc_worker.rs`: `notify_sedimentree_heads_changed`, `notify_cold_sedimentree_heads_updated`, `notif_pending_heads` (:1979, called from :1321/:1325) | **only `test.rs`** |
| `BigRepoDomainNotification` (`BigRepoAccess`, `BigRepoDomainFilter`) | `hub.rs`: `notify_document_added_to_group`, `notify_member_added_to_group`, … | **only `test.rs`** |
| `BigRepoLocalNotification` (`subscribe_local_listener`) | internal | AFW itself (`automerge_frontier_worker.rs:184`) |

Neither the head nor the domain family is exported past `big_repo`: nothing in `daybook_core`,
`daybook_cli`, or `daybook_compose` subscribes to them, and there is no uniffi/binding surface.
They are emit-only APIs: production fires them into channels whose only readers are their own
tests.

**AFW supersedes the head family's purpose.** Its module doc states the job: it "maintains the
Automerge Frontier partition log" — per-document heads published durably, gated on keyhive
admission events and membership watermarks, tailing a durable revision cursor. That is the
sync-visible record of "this doc's heads are now X" that head notifications used to provide
in-process. AFW's own input is the *local* listener (`LocalFilter { doc_id: None }`), not the
head listeners. Both plausible consumers are therefore elsewhere: peers read the frontier log,
the app uses the change listener.

**AFW does not cover the domain family** — keyhive group-membership events are a different
concern — but that family is in the same emit-only state.

Consequences:

- Do **not** port the emit-only families; porting would enshrine an API whose only consumer is
  its own test. That removes 5 tests: the two domain-listener tests, the two head-only tests,
  and the head/origin half of the reopened-handle test.
- `with_document_with_origin` (`lib.rs:1371/1375`) loses its last user once
  `remote_change_and_head_notifications_survive_handle_reopen` goes.
- Removing the families is a **product change**, not dead-code cleanup: the doc worker computes
  pending heads on a sync path purely to feed the channel, and `hub.rs` emits group events. It
  deletes roughly 400 lines across `changes.rs`/`lib.rs` (enums, filters, `subscribe_*_listener`,
  dispatcher plumbing, registration macros) plus the emit call sites.
- Order of operations if the app ever needs access notifications: wire a consumer first, then
  add tier7-style coverage. Not the reverse.

## 6. Defects the ports will surface (i.e. why this is not a mechanical move)

- `sync_with_peer_both_diverged_loses_remote_change` — name says a branch is lost, body asserts
  convergence via concurrent bidirectional sync, the shape the ladder deliberately sequences
  around. Decide the contract before porting (FLAG #1).
- The old tests assert single-key JSON equality where the ladder asserts tier0 parity
  (`heads::tier0_invariants`). "Correcting defects" here means strengthening, not relocating.
- The bespoke double forged the asker and skipped part authorization (§1.1) — already fixed by
  making `test2` use the real RPC; nothing in group C/D may reintroduce an in-process client.
- `SyncRepoNode` semantics differ from `Node`/`Pair`: fresh repo per node plus a manual iroh
  accept loop, no authorization check. Ports must go through the granted connect helpers, with
  `Pair::boot_ungranted` only for denial tests.
- The harness's client-side hold (`REPLAY_HOLD_MS = 200` in `topo.rs`) exists because a parked
  live long-poll is invisible to `wait_for_quiescence`; do not "fix" a porting hang with a
  server-side clamp (`1feaa485`, `dd2f313d`).

## 7. Open decisions (blocking specific ports, not the whole plan)

1. `sync_with_peer_both_diverged_loses_remote_change` — which contract? Keeps `run_sync_case` alive.
2. The 9 `big_repo_sync_backend_*` — relocate to `src/big_repo/backend/test.rs` (recommended) or keep in `test.rs`?
3. ~~Port the 5 head-notification cases~~ — resolved by §5: they die with the families.
4. `create_doc_with_group_parent_uses_public_group_api` — keep, or fold its delta into `tier6_delegate_before_define`?
5. `client_keyhive_decrypts_postwrite_blob_after_edit_grant_sync` — fold the `content_ref == postwrite_head` pin into tier8, then delete (recommended)?
6. `three_node_key_rotation_propagates_to_existing_reader` — port to tier6 or drop?
7. `concurrent_writers_with_edit_access_converge_after_bidirectional_sync` — is one-round bidirectional convergence a property to pin?
8. **New:** remove the emit-only notification families (§5) — yes/no, and does the app want access notifications?

Also unresolved from the wider work: should `wait_for_quiescence`/`wait_for_full_sync` treat a
parked live long-poll as in-flight work? Current answer is "no; the client knob handles it".

## 8. Proposed commit sequence

1. `test: move the big_repo sync-backend contract cases beside the impl` — group A, deletes `run_sync_backend_case` + 4 helpers.
2. `test: relocate the single-node big_repo invariants into the e2e module` — group B (minus FLAG #4).
3. `test: port the offline/restart/allocation scenarios onto the test2 fixtures` — group C keepers.
4. `test: port the remaining notification and restart scenarios` — group D keepers (change-listener half only).
5. `refactor: remove the emit-only head and domain notification families` — §5, separate because it is a product change (pending decision 8).
6. `test: delete the big_repo bespoke harness and test.rs` — `SyncRepoNode`, `StressBigSyncRpcClient`, `run_sync_case`, `boot_repo`, `_boot_disk_repo`, builders, constants.

Each commit independently green. Net: ~2000 test lines relocated onto shared fixtures,
~1960 harness lines deleted, plus the notification-family surface.

## 9. Method rules for executing this

- Verification per commit: `cargo clippy -p big_repo -p big_sync -p big_sync_core --all-targets --all-features`, then the affected tiers via nextest (`-p big_repo`), plus the `daybook_cli` clone smoke when `test2`'s harness is touched.
- Deletion rule that has held so far: only delete where a **named** candidate asserts the same
  or stronger outcome, with the bodies compared. Same-name is not evidence (the `edge.rs:551`
  head-roundtrip case is weaker than the one it resembles; `tier7_doc_id_filter` is strictly
  stronger than the doc-id-filter test it replaced).
- Forks get explicit candidate lists; a fork's report is verified, never trusted (the first
  attempt produced 713k tokens, claimed this work as its own, and contradicted a deterministic
  bisect; two of its DELETE claims were spot-checked and held).
- No cargo in forks; bounded reads and greps only.
- `test.rs` line numbers shift as deletions land — re-derive the inventory before acting on any
  table in §3/§4.
