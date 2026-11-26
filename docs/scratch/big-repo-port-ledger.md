# big_repo port ledger

Status: Living ledger for the file-by-file port of `src/big_repo` into the new `big_repo::runtime`
module (replacing `runtime2`). Companion to `prod-readiness-ledger.md` (Phase 0, W2, W3) and
`big-automata-port-ledger.md` (same table format).
Owner: the automata lane (prod-readiness).
Scope: `src/big_repo` (62,317 lines of Rust in 77 files, as of `abc4f999`), plus the cabinet-side
automerge usage in the last section.
Updated: 2026-11-26. Read-only mapping: nothing here was built or run. Every claim is a `file:line`
reading of code, or says it is unverified. "Proposed home" columns are proposals for the operator.

Plan being served: `big_repo::runtime` is sans-IO and single-threaded-capable (browser,
wasmCloud), DST-testable, with storage (one SQLite database) as the coordination point. Local
multi-doc writes commit in one SQL transaction; remote writes flow through subduction.

Line-count totals by area: runtime2 20,159; store/sqlite 4,721; keyhive glue (`keyhive*`,
`handler.rs`, `wire.rs`, `rpc.rs`) 4,713; other (lib, changes, interest, ephemeral, backend,
encrypted_blob) 4,768; tests 27,956 (45% of the crate).

All paths below are relative to `src/big_repo/` unless they start with another crate name.
Upstream paths: `../subduction` is the `dman-os` fork checkout at the pinned `5f41812`; `../keyhive`
is the fork at `a84137d`; `automerge` is the registry crate 0.11.0 (`Cargo.lock:1125`).

Summary of the largest findings (details in the sections):

1. The "one SQLite database" is not quite true: keyhive **archives are files** on disk
   (`keyhive_storage.rs:1-8,886-899`), DEKs come from the OS keyring or 0600 files, and the
   keyhive object graph lives in memory (`keyhive.rs:150-157`, `MemoryCiphertextStore`).
2. A local write spans at least 5 SQL transactions per commit, plus a checkpoint pre-step, a
   fragment post-step and an async frontier publish; memory is mutated first at four places.
3. The sedimentree rows and the sedimentree-heads payload already commit in one SQL transaction
   (`store/sqlite/sedimentree.rs:642`). That is the part-store write the new design needs, and it
   exists today.
4. Local commits are not pushed to peers by subduction (`store_commit` has no network,
   `subduction.rs:1346`); propagation rides the part-store revision into big_sync
   (`backend.rs:~100`). Fragments use `add_fragment`, which does push.
5. Correction to the Decisions bullet: remote commit content does **not** skip the doc worker.
   `hub.rs:2309-2358` routes every received session to a (lazily spawned) worker, and on a
   writable node that worker hydrates the document and may write checkpoints
   (`doc_worker.rs:1508`). Only CGKA notifications skip cold workers (`hub.rs:2957`).
6. Keyline touches 5 files for `StaticEvent` bincode (6 decoders), 8 sites for keyhive's bincode
   `Envelope`, the `EncryptedContent` DRISL codec, 4 `Public` sites (2 behavioural), and the
   author-at-arrival gate that lives entirely in `subduction_keyhive` (`policy.rs`, not big_repo).
7. big_repo already materializes keyhive access into SQL (`reconcile_group_part_batch`,
   `store/sqlite/sedimentree.rs:171`). Read-time Keyline authorization would have to invalidate
   that projection by closure, not by event.
8. Hydration is a whole-history decrypt + `load_incremental` per cold load, with the doc worker
   evicted after 3 s idle (`runtime2/types.rs:11`). A `(doc, heads)` datum projection is hard
   mainly because remote writes cannot be projected in the commit transaction, and because the
   automerge heads and the sedimentree heads deliberately differ.
9. The cabinet touches automerge in exactly two places (the docs service and the datum-set index);
   big_blobs and big_nodule never do. Datum values are record-shaped, and no production schema
   declares Text or Counter. A datum write without hydration is **feasible with automerge's public
   API** for map/scalar/delete ops, if the projection stores op IDs (details in the last section).

---

## 1. Inventory

Status words: **keep**, **port** (into `runtime`), **big_automata** (moves there), **delete**,
**test-only**, **replace** (new design supersedes). All are `not started` unless noted.
"Consumers" are other crates found with `rg -w` on the symbol (external crates only). `runtime2`
is a private module, so its consumers are `BigRepo` (`lib.rs`) and the re-exports named.

### 1.1 runtime2

| File | Lines | Role | Consumers | Proposed home |
|---|---:|---|---|---|
| `runtime2/mod.rs` | 188 | `Runtime2Config`, `TransportConnect`, `DocHeadState`, `MaterializationState`; re-exports | `DocHeadState`/`MaterializationState`: big_cabinet, daybook_core (`MaterializationState`) | port: config and states become `runtime` types |
| `runtime2/hub.rs` | 4271 | the hub actor: command/event loop, connections, keyhive sync rounds, doc-worker registry and eviction, quiescence, `finalize_allocated_doc` | via `BigRepo` | port, rewritten sans-IO: it awaits injected IO inline (`hub.rs:342-470`) so it is not sans-IO today |
| `runtime2/doc_worker.rs` | 2314 | per-doc actor: hydrate (`load_doc_snapshot` 742), commit gate (1041), sync apply (1402), checkpoints (1523, 1616), fragments (1213) | via `BigRepo` | port, reshaped: hydration cache plus per-doc encrypt sequencer; the actor stops being the correctness boundary |
| `runtime2/doc_worker/tests.rs` | 1127 | worker tests with `FakeRecoveryDocIo` / `FakeChainDocIo` (the only other `DocIo` impls) | - | test-only (port with the worker) |
| `runtime2/handle.rs` | 795 | `Runtime2Handle`: typed command senders (`commit_delta` 209) | `BigRepo` | port |
| `runtime2/messages.rs` | 617 | `Runtime2Cmd`, `Runtime2Evt`, `DocWorkerMsg` | `Runtime2Evt` re-exported under `test-support` | port, shrinks with the new core |
| `runtime2/io.rs` | 355 | `DocIo`/`RuntimeIo`/`Timer`/`Clock` seams (the IO traits) | - | port: the seams become the sans-IO effect/storage surface |
| `runtime2/native.rs` | 2829 | the only real IO backend: `NativeBigRepoIo` (persist 483, checkpoints 538, hydrate 883, causal decrypt 1044), iroh `TransportConnect`, keyhive maintenance loop, `spawn_native_runtime2` | via `BigRepo::boot` | split: SQL-bound persistence into `runtime` storage; iroh transport stays a native adapter; keyhive maintenance into `runtime`; `Sendable`-only today |
| `runtime2/support.rs` | 817 | type aliases (`BigRepoSubduction`, `BigRepoPolicy`), iroh connect helpers, `StagedAutomergeIngest`, encrypt helpers (465-722), `persist_cgka_updates_durably` (351), `CausalCheckpoint` codec (bincode, 299/308) | - | split: encrypt/decrypt and checkpoint codec port; iroh helpers stay in the adapter |
| `runtime2/types.rs` | 903 | `LiveDocBundle`, `DocLookup`, `BigRepoSyncPolicy` (idle TTL 3 s at :11), `WorkerGroupScope`, public error types | `DocLookup`: big_cabinet, big_node, daybook_core; `WorkerGroupScope`: daybook_core; `NoWriteAccess`: big_cabinet | port selectively; the re-exported error/lookup types stay public API |
| `runtime2/lease.rs` | 169 | `DocLease`, worker entries | `DocInterest` (big_blobs, big_cabinet) | port or replace with the new registry |
| `runtime2/tasks.rs` | 305 | `TaskRuntime`/`TaskSet`/`TokioTaskRuntime`/`TokioTimer` | - | replace: a sans-IO core spawns nothing; the native adapter owns tokio |
| `runtime2/test_support.rs` | 214 | in-memory paired transports | - | test-only |
| `runtime2/automerge_frontier_worker.rs` | 1397 | AFW: publishes materialized heads into the frontier scope (publish_heads 278) | `automerge_doc_obj_id`: big_blobs, daybook_core | replace or port: see section 2 (A11) and section 5; uses `ConcurrentDeltaWalker` + `TokioKeyedScheduler` + `Outbox` |
| `runtime2/doc_revision_store.rs` | 652 | `AutomergeFrontierRevisionStore` (a `RevisionedStore` over the frontier scope) | `AutomergeFrontier{RevisionStore,Event,Selector}`: big_blobs, big_cabinet, daybook_core | big_automata rev stream; fate follows the frontier decision |
| `runtime2/keyhive_admission.rs` | 314 | admission log as a `RevisionedStore`; parks on `tokio::sync::watch` (:37) | - | big_automata rev stream (process-local wakeup is a multi-process blocker) |
| `runtime2/keyhive_access_stream.rs` | 1365 | ADR 013 access delta stream; reads keyhive at read time, no durable projection | `KeyhiveAccess{RevisionStore,Delta,Selector}`, `AccessSubject`: big_blobs, big_node, daybook_core | keep the concept: it is the Keyline-shaped pattern |
| `runtime2/keyhive_dispatcher.rs` | 513 | fan-out of payload-free "keyhive changed" hints per peer from the admission log; `Public` use :297 (diagnostic only) | - | port; uses `protocol.notification_targets` / `all_agent_events` |
| `runtime2/group_part_worker.rs` | 1026 | admission rows to group parts: writes membership + access rows (`reconcile_group_part_batch`) | `group_part_id`: big_cabinet, big_node, daybook_core | port as an admission-stream consumer; the SQL projection of access is the Keyline friction point |
| `runtime2/causal_checkpoint_worker.rs` | 545 | seats readers and maintains causal coverage from admission rows; decodes `StaticEvent` (bincode :104, :352) | - | port as a consumer |
| `runtime2/prekey_janitor.rs` | 128 | prekey pool policy | - | port |
| `runtime2/prekey_janitor_worker.rs` | 442 | admission-log consumer driving the janitor; `SerialDeltaWalker` (:107) | - | port; the one `SerialDeltaWalker` user |

### 1.2 store / sqlite

| File | Lines | Role | Consumers | Proposed home |
|---|---:|---|---|---|
| `store.rs` | 1 | `pub mod sqlite` | - | keep |
| `store/sqlite.rs` | 552 | `SqliteBigRepoStore` (wraps big_sync `SqliteCore`), `KeyhiveIncorporationSink` (:281), `publish` wake (:295), admission watermark | `SharedPartStore` (big_blobs, big_cabinet, big_node, daybook_core); `SqliteBigRepoStore` only named in big_sync docs | port: becomes the `runtime` storage module with an explicit `Tx` surface |
| `store/sqlite/sedimentree.rs` | 1378 | `Storage<Sendable>` impl for subduction; `mutate_tree_in_tx` (:642); `reconcile_group_part_batch` (:171); causal-ciphertext index | - | keep and expose `*_in_tx` entry points: this is the batch-in-one-transaction substrate |
| `store/sqlite/events.rs` | 492 | keyhive event log, admission rows, retention; `save_keyhive_event` (:69), `append_admitted_events` (:117) | - | port; folds into a SQL-backed Keyline |
| `store/sqlite/parts_cursors.rs` | 936 | `impl HostPartStore for SqliteBigRepoStore` | `shared/frontier/derived_part_store`: many | big_automata (`big_automata_sqlite` part store) |
| `store/sqlite/secret_blobs.rs` | 196 | AEAD'd secret blobs (CGKA secrets, reservations, prekey sidecar), `begin_write` :124 | - | keep |
| `store/sqlite/ids.rs` | 113 | blob/key encodings | - | keep |
| `store/sqlite/tree_cache.rs` | 215 | transaction-scoped minimized-tree cache; epoch guard | - | keep; a stale-cache hazard once several processes share the database |
| `store/sqlite/tests.rs` | 3607 | 68 tests: store, replay, parts, admission | - | test-only; the part-store tests belong with `big_automata_sqlite` |
| `sqlite/gen.rs` | 838 | generated query bindings (`cargo x sql gen`); 103 `.sql` query files | - | regenerate, never hand-edit |
| `sqlite/migrations/001..004` | (SQL, 262) | `001_init.sql` holds **big_sync's part-store schema** (`big_sync_*`, `delta_walker_progress`); `002` holds subduction trees/commits/fragments and the keyhive event log; `003` secret blobs; `004` watermark | - | split: part-store tables follow the part store |

### 1.3 keyhive glue, wire, rpc

| File | Lines | Role | Consumers | Proposed home |
|---|---:|---|---|---|
| `keyhive.rs` | 1649 | `BigKeyhiveHandle` (in-memory `Keyhive`), group/doc/grant APIs, `can_write` (:666, uses `Public`), `MembershipReach`, `transitive_members_short_locked` (:1532) | `BigKeyhive{Handle,Group,Agent,Authority}`: big_cabinet, big_node, daybook_core | port; the largest Keyline rewrite |
| `keyhive/tests.rs` | 592 | 10 tests | - | test-only |
| `keyhive_storage.rs` | 1853 | archives on files, events and secrets in SQLite, DEKs from keyring/file; `DocReservation` (serde+bincode :56) | `KeyhiveFiles`: big_node | port into the SQL-backed Keyline; the FIXME at :11 (load all archives) goes away |
| `keyhive_listener.rs` | 200 | keyhive listener to `Runtime2Evt`; `on_cgka_op` fires before the op is durable | - | port |
| `keyhive_conn.rs` | 91 | adapter from `Authenticated` conn to `KeyhiveConnection` | - | keep in the native adapter |
| `keyhive_files.rs` | 40 | `KeyhiveFiles { root, secrets }` | big_node | keep |
| `handler.rs` | 306 | composed wire handler (Sync + Ephemeral + Keyhive) | - | keep in the adapter |
| `wire.rs` | 134 | `BigRepoWireMessage` codec over `MESSAGE_SCHEMA`/`EPHEMERAL_SCHEMA`/`KEYHIVE_SCHEMA` | - | keep; follows the keyhive schema bump |
| `rpc.rs` | 440 | `REPO_SYNC_ALPN` keyhive-changed hints (irpc) | `spawn_repo_rpc`: big_blobs, big_cabinet, big_node, daybook_core | keep in the adapter |
| `backend.rs` | 341 | `impl big_sync::SyncBackend`: per-object sync task calls `runtime.sync_doc_with_peer_receipt` | `BigRepoSyncBackend`: big_cabinet, big_node, daybook_core | port: the driver side of big_automata; also the propagation path for local writes |

### 1.4 changes, interest, policy, blobs, ephemeral, crate root

| File | Lines | Role | Consumers | Proposed home |
|---|---:|---|---|---|
| `lib.rs` | 1762 | `BigRepo`, `Config`, `Storage`, `boot_inner` (:446), `BigDocHandle` (:1519, `with_document_with_origin` :1583), re-exports | all | port: split the facade from the handle; the document face stays here |
| `changes.rs` | 1746 | `ChangeListenerManager`, change/local/domain notifications and filters (module is `#[expect(unused)]`, `lib.rs:47`) | `BigRepoChangeNotification`, `BigRepoChangeOrigin`: daybook_core; `BigRepoLocalNotification`: big_cabinet, daybook_core; `BigRepoLocalFilter`, `BigRepoDocIdFilter`: big_cabinet, daybook_core; `BigRepoDomainNotification`: none | port; trim the unused half |
| `interest.rs` | 249 | `DocInterest`, `DocSyncDemand(Driver)`; 30 s sync grace (:12) | `DocInterest`: big_blobs, big_cabinet; `sync_demand`: big_cabinet(_core) | port |
| `interest/tests.rs` | 444 | 2 tests | - | test-only |
| `access_policy.rs` | 6 | doc-comment-only module; policy is built into the part store | - | delete |
| `encrypted_blob.rs` | 105 | DRISL codec of `EncryptedContent` behind a `0x02` discriminator | - | port; serde-dependent (Keyline) |
| `ephemeral.rs` | 559 | ephemeral topic pub/sub over `subduction_ephemeral` | `BigEphemeral`: none outside | keep (or defer) |

### 1.5 tests

| File | Lines | Role |
|---|---:|---|
| `test.rs` | 3915 | original 35-test suite; 86 big_sync references (the most of any file) |
| `test2/access_matrix.rs` | 1318 | tier 2: grants by level (uses `public_agent`, :926) |
| `test2/capability.rs` | 2240 | tier 6: capability/adversarial |
| `test2/cgka.rs` | 1834 | tier 6: CGKA, causal ordering |
| `test2/conn_lifecycle.rs` | 493 | connection lifecycle |
| `test2/convergence.rs` | 355 | tier 4 |
| `test2/edge.rs` | 1743 | tier 9 edge cases |
| `test2/encryption.rs` | 677 | tier 8 PCS |
| `test2/ephemeral.rs` | 116 | tier 9 |
| `test2/fragmentation.rs` | 247 | tier 6 |
| `test2/freeze.rs` | 46 | quiescence freeze |
| `test2/keyhive_rpc.rs` | 328 | keyhive-changed hint (uses `public_agent`, :61, :152) |
| `test2/ladder.rs` | 577 | tier 1 |
| `test2/notifications.rs` | 1212 | tier 7 |
| `test2/restart.rs` | 825 | tier 5 |
| `test2/revocation.rs` | 642 | tier 6 |
| `test2/stress.rs` | 1553 | tier 10 randomized |
| `test2/topologies.rs` | 1585 | tier 3 (uses `public_agent`, :620, :814, :944, :1201, :1566) |
| `test2/mod.rs`, `harness/mod.rs` | 23 + 23 | module wiring |
| `test2/harness/{converge 83, dump 58, fixtures 517, heads 183, keyhive 350, log_nickname 65, topo 1178}` | 2434 | harness; `fixtures.rs:69` builds the `Public` agent |

Disposition of the tests: all **test-only**. They are 45% of the crate, most of them end-to-end
over real subduction/iroh with sleeps/polls (`fixtures.rs:55-63` busy-waits at 10 ms). The
sans-IO `runtime` should get a DST harness instead (see `.agents/skills/state-machine-verification`);
the tier suites become the conformance checks run against it. Per AGENTS.md, no test is skipped.

---

## 2. The write path (Phase 0 step 1)

Legend: **[A]** atomicity or durability lost here. **[M]** memory mutated before persist.
**[H]** async hand-off (channel or spawn).

### 2.1 Local write: cabinet to durable state

| # | Hop | Evidence | Loss |
|---|---|---|---|
| 0a | Cabinet stages the write on a dry-run: `with_document`, `transaction_at(base)`, `tx.rollback()` | `big_cabinet/doc/write.rs:108,133,136` | none, but a whole extra pass |
| 0b | Cabinet signs the claims outside the lock | `write.rs:144` | |
| 0c | Cabinet's real write: `with_document`, re-checks the condition, `set_actor`, `transaction_at(&base)`, `tx.commit_with(options)`. The command id rides as the change's message | `write.rs:147-159`; condition `pipeline.rs:333-364` | **[M]** the change is applied to the hydrated doc here |
| 1 | `BigDocHandle::with_document_with_origin` runs the closure under a sync lock, then reads `get_changes(&before_heads)` as raw change bytes | `lib.rs:1583,1612,1627` | **[M]** `operation(&mut doc)` at :1612 mutates the live bundle before anything is durable |
| 2 | `Runtime2Handle::commit_delta` sends `Runtime2Cmd::CommitDelta` and awaits a oneshot | `lib.rs:1656`, `runtime2/handle.rs:209` | **[H]** |
| 3 | Hub routes to the doc worker; spawns it lazily | `hub.rs:809`, `route_to_doc_worker` `hub.rs:3197` | **[H]** |
| 4 | Worker gates: handle validity, then `has_doc_write_access` (keyhive read) | `doc_worker.rs:1041,1077` | **[A]** a check-then-act: a revocation can land between this and the persist |
| 5 | Pre-step: if any parent predates the current epoch, write a causal checkpoint first (`ensure_frontier_checkpointed`), then rewire the batch's parents onto it | `doc_worker.rs:1123,1616`; `native.rs:538-670` | **[A]** a separate durable commit before the main batch (see 5a-5c) |
| 5a | `force_pcs_update` rotates the doc's CGKA in memory | `native.rs:394` | **[M]** |
| 5b | `persist_cgka_updates_durably`: secret blob tx, then one event tx **per event**, then one admission tx per exchange | `native.rs:409,652`; `support.rs:351`; secret `secret_blobs.rs:124`; event `store/sqlite/events.rs:69` via `keyhive_storage.rs:1162`; admission `events.rs:128` via `sqlite.rs:292` | **[A]** ≥3 transactions per update |
| 5c | Checkpoint commit: `subduction.store_commit` | `native.rs:662` | **[A]** its own transaction |
| 6 | `persist_local_commits`: **a sequential loop over the batch**: for each commit, encrypt, persist any CGKA update, `store_commit` | `native.rs:483,490-528` | **[A]** a crash mid-loop persists a prefix (H3) |
| 6a | Encrypt: `try_encrypt_content_keyed` calls `new_app_secret_for` on the in-memory CGKA, inserts the app key into `known_decryption_keys`, inserts the rotated secret, and fires `on_cgka_op` | `support.rs:552-615`; `../keyhive/keyhive_core/src/keyhive.rs:1099`; `principal/document.rs:693-698` | **[M]** four in-memory mutations before any persist |
| 6b | `on_cgka_op` sends `Runtime2Evt::CgkaOp` to the hub immediately, before the op is durable (6c); the hub starts materialization retries for existing workers | `keyhive_listener.rs` (`on_cgka_op`); `hub.rs:2207-2226` | **[H]** observable before durable. The FIXME at `hub.rs:2205` says a rotation may be several ops |
| 6c | CGKA op persisted (as 5b) | `native.rs:514` | **[A]** |
| 6d | `subduction.store_commit`: seal, `insert_commit_locally` (persist, then apply to the resident tree, under one shard lock), minimize | `native.rs:522`; `../subduction/subduction_core/src/subduction.rs:1346`; `subduction/ingest.rs:395-440` | persists in its own transaction |
| 6e | The storage transaction: tree row, causal-ciphertext index, commit row, **sedimentree-heads payload, and `/seds` membership**, all in one `BEGIN IMMEDIATE`; then a post-commit in-process wake | `store/sqlite/sedimentree.rs:1042-1056` (`save_loose_commit`), `:642-815` (`mutate_tree_in_tx`), wake `store/sqlite.rs:295-312` | **atomic** for these rows. The wake is a process-local `Notify` |
| 7 | Pending fragment requests are kept in worker memory | `doc_worker.rs:1169` | **[A]** lost on crash (H10) |
| 8 | In-memory notifications: sedimentree heads, local heads, patches | `doc_worker.rs:1176-1200` | **[H]**; lost on crash, consumers must reconcile from the frontier |
| 9 | Fragments: `process_pending_fragment_requests`, `store_fragment` re-reads loose commits from storage, encrypts, and calls `subduction.add_fragment` (store **and push to subscribers**) | `doc_worker.rs:1203,1213-1262`; `native.rs:923-962` | **[A]** a separate transaction. It runs before `resp.send` (:1203 precedes :1206), so commit latency includes it, and a failure here fails the whole commit although the commits are already durable |
| 10 | Response to the caller | `doc_worker.rs:1206` | |
| 11 | AFW publishes materialized heads: wakes on a part revision from step 6e (`on_part_delta` :840) or a local notification (`on_local_notifications` :892); calls `acquire_internal_doc_handle` (a hydrate if the worker was evicted), waits up to 30 s for the bundle's CGKA op count to catch up, reads `doc.get_heads()` from the live bundle | `automerge_frontier_worker.rs:278-374,296,321,373` | **[H]** asynchronous, may defer |
| 11a | Writes the frontier scope: `set_obj_payload` (tx), `add_obj_to_parts` (tx), `remove_obj_from_part` (tx **per stale part**) | `automerge_frontier_worker.rs:399,415,422`; `store/sqlite/parts_cursors.rs:135,455,463` | **[A]** three or more transactions, in a *separate* scope (`lib.rs:462-474`), at-least-once, with the walker ack a further transaction (`acknowledge_source` :785) |

Fan-out for **one** cabinet datum write on a node that must checkpoint: roughly 1 checkpoint
(≥3 key/event/admission transactions + 1 commit) + per commit (≥3 if it rotates + 1 commit) + 1 per
fragment + 3+ for the frontier. Subduction already has `store_commits_batch`
(`../subduction/subduction_core/src/subduction.rs:1642`, one `save_batch`), and the store has an
`InsertBatch` mutation (`store/sqlite/sedimentree.rs:660`) and `save_batch`
(`sedimentree.rs:1235-1258`); `persist_local_commits` uses neither (H3).

How local commits reach peers: `store_commit` is the no-network variant
(`subduction.rs:1330-1340`); no subduction push happens for a commit. The sedimentree-heads payload
written at step 6e bumps a part revision; big_sync's per-object task calls
`BigRepoSyncBackend::sync_obj`, which (after a head-equality shortcut) calls
`sync_doc_with_peer_receipt` (`backend.rs:~100-150`). So propagation is a consequence of the part
store, not of the write call. Fragments are the exception (`add_fragment`, step 9).

Crash-recovery that exists today: document creation stages a reservation row before any durable
effect and re-runs byte-identically (`hub.rs:365-466`, `keyhive.rs:1060-1135`); keyhive local
events rely on "startup full-projection reconciliation repairs any crash between mutation,
persistence, and durable admission" (`../subduction/subduction_keyhive/src/protocol.rs:1834-1838`).
There is none for step 5-6 prefixes; they are idempotent by content hash, and the cabinet's boot
pass reads the command id off the document (`big_cabinet/doc/commands.rs:11-20`).

Other write paths, for completeness. Create: `lib.rs:947` `allocate_id`, `:967` `commit_id`,
`hub.rs:342-470` (`stage_allocated_document`, `finalize_document_authority`, `persist_initial_document`
`native.rs:457-481`, then `delete_doc_reservation`): five separate durable steps, with
`initial_content.save()` called up to three times (`hub.rs:387,432,440`; H9). Grant:
`keyhive.rs:1311-1343`: in-memory `add_member_with_manual_content`, then one event transaction per
CGKA op and per delegation.

### 2.2 Remote write arriving through subduction

| # | Hop | Evidence | Loss |
|---|---|---|---|
| R1 | Subduction handler authorizes the put: the **commit's author** must hold `Edit` (pinned fork) | `../subduction/subduction_keyhive/src/policy.rs` (`authorize_put_with`, pinned `5f41812`) | arrival-time author gate (Keyline section) |
| R2 | `insert_commit_locally` into the same `Storage` impl: the same single transaction as 6e | `subduction/ingest.rs:395`; `store/sqlite/sedimentree.rs:1042` | atomic |
| R3 | `SyncSessionObserver::on_sync_session` does `try_send` into the hub's event channel and **drops the session with a warn** if the channel is closed | `runtime2/native.rs:2077-2092` | **[H]** best effort |
| R4 | Hub: `handle_sync_session_observed`; empty sessions are skipped; content-carrying ones go to `route_sync_session_apply`, which **lazily spawns a doc worker** | `hub.rs:2309-2358,2358-2455` | **[H]** |
| R5 | Worker `apply_sync_session`: if `Live`, hydrate the persisted tree (`hydrate_tree`), decrypt only the received refs, `apply_blobs_to_live` (`load_incremental` per blob), notify, retry blocked refs. If not live the content stays persisted but unhydrated | `doc_worker.rs:1402-1522` (1447, 1467, 1492) | **[M]** the live doc runs ahead of nothing (storage was first), but notifications are lost on crash |
| R6 | Then `reconcile_causal_coverage`: **on a node with write access, this hydrates the document even without a handle** and may mint a checkpoint (a local write, steps 5a-5c) | `doc_worker.rs:1508,1523-1612` | **[A]** a remote write triggers local writes in separate transactions |
| R7 | AFW as step 11 | | **[H]** |
| R8 | Keyhive events arriving: ingest into the in-memory keyhive **first**, then persist the event bytes, then report admission ("Publish events to durable storage only after the in-memory projection has ingested them") | `protocol.rs:1812-1822` | **[M]** by design; the sender retries on a crash |

Correction to the Decisions bullet in `prod-readiness-ledger.md` ("remote commits already skip the
doc worker; `hub.rs:2957`"): that line is `retry_existing_doc_materialization`, which is the CGKA
notification route. Content routes through R4-R6. Storage-side, R1-R2 are indeed pure storage
operations.

### 2.3 What this says about the new design

- The commit unit for the SQL transaction is already there: `mutate_tree_in_tx` takes a `Tx` and
  writes rows + heads payload + membership. The gap is the in-memory keyhive/CGKA mutations
  (6a), the per-event keyhive transactions (5b), and the secret blob. A SQL-backed Keyline plus
  CGKA secrets/ops in the same database removes all three, if encrypt can *stage* its mutations.
  `try_encrypt_content_keyed` as written mutates four things (6a), so it needs either staging in
  keyhive (we own `../keyhive`) or a "prepare / apply-after-commit" split.
- `known_decryption_keys` insertion at encrypt time (`document.rs:698`) means memory holds the key
  of a commit that may never persist; harmless (the key is derivable) but it is the same
  memory-ahead-of-disk class.
- Step 6b must move after commit. Today any observer of `CgkaOp` sees an op that is not durable.
- AFW's frontier is a second, asynchronous projection of what the commit transaction could write
  directly (the heads), except that AFW writes *automerge* heads (a hydrated-doc fact) and the
  commit writes *sedimentree* heads. They differ on purpose when key-only checkpoints exist
  (`doc_worker.rs:~1085-1092` comment).

---

## 3. big_sync usage

Method: 440 lines contain `big_sync` (including `big_sync_core`) across 36 files; 241 in production
files and 199 in test files (computed by file, so it differs from the lead's ~332). Production
use, grouped by what replaces it in big_automata:

| Group | Sites | Count (prod lines) | Replacement |
|---|---|---:|---|
| `SyncBackend` impl | `backend.rs` (`impl big_sync::SyncBackend` :83; `sync_obj`; `remove_obj_from_parts` is a deliberate no-op :90) | 18 | big_automata `driver` (the host side). `sync_obj` becomes the driver's "settle this object with that peer" callback; the head-equality shortcut moves with it |
| `HostPartStore` impl and part store | `store/sqlite/parts_cursors.rs` (`impl HostPartStore` :4), `store/sqlite.rs` (`SqliteCore`, `encode_access`, `ReadTarget`, `PartStoreStats`), `lib.rs` (`SharedPartStore = Arc<dyn HostPartStore>` :178), `sedimentree.rs` (`ObjPayload`, `MemberState`) | ~75 | big_automata `part_store` + `big_automata_sqlite`; the three scopes (`main`, `:automerge-frontier`, `:derived`, `lib.rs:455-486`) become three part stores |
| Frontier and delta-walker family: `RevisionedStore` | impls in `keyhive_admission.rs:45`, `doc_revision_store.rs:88`, `keyhive_access_stream.rs:214`, `automerge_frontier_worker.rs:452` (`LocalPartRevisionStore`) | ~60 | big_automata `okv` / `rev` stream: "a source implements `latest_rev`, `read`, `watch`; nobody writes a reader again" |
| `ConcurrentDeltaWalker` + `SqliteDeltaWalkerStateRepo` | AFW (:160,:179,:189), `group_part_worker.rs:114`, `causal_checkpoint_worker.rs:100` | ~30 | **needs the walker machine (build at first user)**: durable progress + retention registration (`events.rs:8-26,299`) + at-least-once ack. These three are the first users |
| `SerialDeltaWalker` | `prekey_janitor_worker.rs:107` | 5 | same: **walker machine**, serial variant; the only user, so a candidate to fold into the concurrent one with concurrency 1 |
| `KeyedFrontier` | none in big_repo production (cabinet's `index/datum_set.rs` uses `SqliteKeyedFrontier`) | 0 | `okv` evaluation already claims it (big-automata ledger, "Keyed frontier to okv") |
| `TokioKeyedScheduler` | AFW :211,:536,:1072; `group_part_worker.rs:127,225`; `causal_checkpoint_worker.rs:131,212` | 7 | big_automata keyed `Scheduler`; the tokio binding stays in the native adapter |
| `Outbox` | AFW :217,:547; `group_part_worker.rs:136,239` | 4 | big_automata `durable_outbox` (correlated outbox; ledger status "partial") |
| `Retry` | AFW (`scheduler::Retry`) | 1 | big_automata `scheduler` |
| `PeerKey`/`PartKey`/`ObjKey`/`BuckId` ids | `lib.rs:11-14`, many | ~20 | big_automata `ids` |
| `rpc::*` wire types | `store/sqlite.rs` (`BucketSummary`, `LeafBucket*`, `PartPage`, `PartEvent`, `ObjChanged`, ...) | ~20 | big_automata `rpc` (the part store's read verbs return them) |
| Walker-state SQL | `store/sqlite/events.rs:36-40`, `delta_walker_progress` table in `001_init.sql` | 3 | the walker machine's state repo |

Observations:

- big_repo is the **only user of `SerialDeltaWalker`** and one of three users of
  `ConcurrentDeltaWalker` (the others are the cabinet and blobs); the walker is the part of the
  frontier family the big_automata ledger marked "out of scope?" and that big_repo cannot do without.
- `LocalPartRevisionStore` (AFW :452) and `AutomergeFrontierRevisionStore` are projections that
  "re-implement reader plumbing" (big-automata ledger, `RevisionedStore` review); they are `map`
  combinators over `okv`.
- Process-local wakeups: `local_revision_wakeups: Notify` (`store/sqlite.rs:~105`),
  `admission_watermark: watch` (`:~120`), `keyhive_admission.rs:37`. All are same-process only
  (multi-process blocker 1/2). The candidate fix in the prod ledger (commit hook + `PRAGMA
  data_version`) lands in the storage module.
- `sync_obj` returns a `Noop` completion when advertised heads equal local heads and the doc is
  `Materialized` (`backend.rs:~120-140`), which makes the frontier/heads payload part of the sync
  contract, not just an index.

---

## 4. Keyline impact

Sources: local branch `upstream/keyline-take-2` in `/run/media/asdf/p3N/repos/subduction`
(commits `7d70668` port, `f644dac` "Check the requestor, not the author, on put", `199505e`,
`34d625d`, `0e47aa4` access trait #380, `a08f3bd` codec). The PR body itself was **not** read:
`gh pr view 342 -R inkandswitch/subduction` hung with no network access here and was killed. The
statements below about PR #342's behaviour are from those commit messages and the branch's
`policy.rs`/`access.rs`, not the PR text.

What the branch does (from the commit messages): `keyhive_core` 0.12 membership as Keyline
(`Power`: Relay < Read < Edit < Admin), no public agent, serde dropped for canonical `KH` frames,
events/archives/contact cards as frames, stored old-format entries skipped and kept, content ref
becomes `ChangeId` (a sedimentree commit id), `KeyhiveAccess` trait (`access.rs`), put checks the
**requestor** at `Relay`, wire schema `SUK\x01` (a `SUK\x00` frame is rejected).

### 4.1 Sites

| Concern | Site | What changes |
|---|---|---|
| `Public` principal | `keyhive.rs:670` (`can_write`), `native.rs:790` (`has_doc_fetch_access`) | behavioural: both grant through `Public`; they go away, or become the "app-public" capability pattern |
| | `runtime2/keyhive_dispatcher.rs:297` | diagnostic only (`dispatch_diag()` :262/:292); delete |
| | `test2/harness/fixtures.rs:69` `public_agent()`, used by `test2/keyhive_rpc.rs:61,152`, `access_matrix.rs:926`, `topologies.rs:620,814,944,1201,1566` (6 tests rely on public access) | rewrite those tests around an explicit reader principal |
| | outside the crate: `daybook_core/drawer.rs:500-522`, `daybook_core/sync/tests/stress.rs:1603-1622` | follow |
| `StaticEvent` bincode decode of the event log | `runtime2/causal_checkpoint_worker.rs:104,352`; `automerge_frontier_worker.rs:164`; `group_part_worker.rs:841`; `keyhive_access_stream.rs:439`; `prekey_janitor_worker.rs:87`; `keyhive.rs:613` (`event_subject_id`) | the log rows are written by `subduction_keyhive` (`protocol.rs:1857`, `bincode_serialize`). With frames these become `keyhive_codec` decodes; no on-disk compatibility needed (operator, 2026-11-17). Seven decoders is a symbol-proliferation smell: one decode helper |
| keyhive `Envelope` bincode (the plaintext+ancestor-keys wrapper inside each ciphertext) | encode `support.rs:465,516,612,722`; decode `native.rs:998,1036,1128,2792` | keyhive's own causal decrypt also deserializes it (`document.rs:768+`); moves with `keyhive_content` envelope bodies (#322). **This is the stored ciphertext format**: every commit already on disk is unreadable after the switch, which is allowed (no installs) |
| `EncryptedContent` codec | `encrypted_blob.rs:6-36` (DRISL via serde) | serde-derived on keyhive types; breaks when serde goes away |
| `CausalCheckpoint` | `support.rs:299,308` (bincode of our own struct) | independent of keyhive; keep or re-encode |
| `DocReservation` | `keyhive_storage.rs:56,994,1024,1043` (serde+bincode of our own struct) | independent of keyhive |
| `Access` type and levels | `keyhive.rs` (63 keyhive refs, 42 access/membership refs), `runtime2/keyhive_access_stream.rs`, `store/sqlite.rs:535` (`is_fetch_access`), `encode_access`/`decode_access` (`store/sqlite/sedimentree.rs:277`, `parts_cursors.rs:736,766`), `lib.rs` grant APIs, plus external `big_blobs/permissions.rs`, `big_cabinet`, `daybook_core` | `Access` becomes `Power`; "Write" is "Edit"; the part-member access column is a persisted encoding of it |
| Membership walks | `keyhive.rs:210-285` (`MembershipReach`), `:1519-1640` (`transitive_members_short_locked`, `groups_with_member` :1629), `docs_for_agent` :538 (H6 quadratic) | replace with `Keyline::closure`/`access_for_doc`; a SQL-backed Keyline gives `docs_for_agent` as a CTE |
| Keyhive in-memory type | `keyhive.rs:150-157` (`MemoryCiphertextStore`), `BigKeyhiveHandle::restore_from_storage_archive` :304 | the archive file format changes (`34d625d`); `keyhive_storage.rs` archives |
| Wire | `wire.rs` (`KEYHIVE_SCHEMA`), `handler.rs` | schema `SUK\x01`; both ends must upgrade |
| Policy type | `runtime2/support.rs:48-50` (`BigRepoPolicy = SubductionKeyhive<...>`), `lib.rs:~505` | generic over the new `KeyhiveAccess` trait (#380); our own impl can be SQL-backed |
| Author-at-arrival rejection | **none in big_repo.** The gate is `authorize_put_with` in the pinned `subduction_keyhive/policy.rs` (author needs `Edit`, error "insufficient access to put: requires at least Edit"). big_repo's own gates act on the *local* principal: `doc_worker.rs:1077,1524`, `native.rs:740-770` | the branch changes the pinned gate to requestor ≥ Relay |
| Diagnostics | `backend.rs:~170-260` (`LocalAuthorization` probing after a remote `Unauthorized`), `describe_local_policy_state` (`backend.rs:~19`) | rewrite against `Power` |

### 4.2 What read-time authorization would require of materialization

Today materialization applies **everything it can decrypt**, with no author check:
`load_doc_snapshot` (`doc_worker.rs:742`) decrypts every item in the tree and
`from_decrypted_plaintexts` (:304) loads them all; `apply_blobs_to_live` (:1654) does the same for
received blobs. The commit's author (the subduction signer of the commit, stored in the `signed`
column, `sedimentree.rs:538`) is never consulted. Under "whether an author's write counts is
decided at read time from commits and certificates", materialization would need:

1. **The author of each change.** The subduction author is the *node* signer, not a user
   (`subduction.rs:1346` seals with `self.signer`); the per-datum signer is the cabinet's actor
   inside dmeta. Which does Keyline judge? If the node key, the signed column must be surfaced
   to the materializer; if the actor, the cabinet gate (`big_cabinet/doc/gate.rs`) already does
   this per datum and the runtime need not.
2. **A deterministic filter over the change DAG.** "Which changes count" depends on the
   revocation's `retain` watermark (per-subject content heads, Keyline shift 7), so it is a
   function of *(doc history, Keyline closure of the doc)*. The materializer must apply the
   filter causally: excluding a change excludes its dependents or needs author masking
   (`maskAuthor`, automerge #1581).
3. **A projection key that includes the authorization state.** Any cache keyed `(doc, heads)`
   (AFW's frontier today, the proposed datum projection) is stale when a revocation lands
   without a new commit. The key becomes `(doc, heads, keyline digest/version of the closure)`.
4. **Invalidation by closure, not by event.** `reconcile_group_part_batch`
   (`sedimentree.rs:171`) persists access rows per part per principal, fed by the group-part
   worker from admission rows. Under Keyline a single certificate can change reach for arbitrarily
   many documents, so the "dirty hint, then recompute the closure" pattern of
   `keyhive_access_stream.rs` (reads keyhive at read time, `:1-30`) is the one to generalize; the
   persisted access rows should be derived caches with an explicit digest, not authority.
5. **Re-evaluation on late certificates.** A write is held until its certificates arrive
   (today `PendingMaterialization`, `doc_worker.rs:1269`); it must also flip when a *revocation*
   arrives. The hub's CGKA-op path (`hub.rs:2207`) is the existing trigger shape.
6. **Relay sponsorship is unaffected** (put needs Relay of the connected peer only), but relays
   still must not decrypt; read-time filtering cannot run on a relay, so a relay serves everything
   and readers filter.

---

## 5. Hydration cost, from code paths only

Reading a document: `BigRepo::get_doc` (`lib.rs:751`) to `GetDocHandle` (`hub.rs:803`) to the
worker's `acquire_handle` (`doc_worker.rs:603`) to `take_or_load_transient_doc` (:937) to
`load_doc_snapshot` (:742). The worker is evicted after 3 s idle
(`runtime2/types.rs:11`, `hub.rs:3275`), so every cold read pays this, and AFW pays it
again for any doc it must publish (`publish_heads` :296).

### 5.1 Cost sites

| # | Site | Evidence | Cost |
|---|---|---|---|
| C1 | Tree: resident cache `get_cloned` **clones the whole `MinimizedSedimentree`** (the metadata tree); a miss loads all loose-commit and fragment metas from SQL (no blobs) | `native.rs:883-921` (clone at :889, load :896-909) | O(tree) copy per hydrate |
| C2 | `ensure_minimized` and `topsorted_blob_order` over the tree | `doc_worker.rs:748-751` | O(tree) |
| C3 | Ciphertext fetch: one SQL read per item, **loose commit and fragment tried separately**, then `blob().clone().into_contents()` copies the blob out of the `Blob` | `native.rs:1521-1550` (clone :1536, :1546); row decode `sedimentree.rs:538-557` (`Signed::try_decode`, `Blob::new`), `:1093` `try_from_trusted` | 1 SQL round trip + 1 copy per item |
| C4 | Decode of the DRISL envelope copies the ciphertext into owned `Vec`s | `encrypted_blob.rs:23-36`; `native.rs:1030`, `:1062` | 1 copy per item |
| C5 | Recovered-key fast path: another `ciphertext.clone()` (`native.rs:1032`), AEAD decrypt, `bincode::deserialize` of the `Envelope` (copies the plaintext into `Envelope.plaintext`, :1036) | `native.rs:1009-1042` | 2 copies + AEAD |
| C6 | Causal path (`try_causal_decrypt`): entrypoint decrypted **twice** (`native.rs:1098` directly, then again inside keyhive's `try_causal_decrypt_content`, `document.rs:~782` calls `try_decrypt_content`), and its `Envelope` deserialized **twice** (`native.rs:1128`, keyhive `document.rs:~785`); `try_decrypt_content_keyed` clones the ciphertext again (`document.rs:746`) | `native.rs:1044-1230` | double AEAD + double decode + 2 copies, all under `kh_doc.lock()` held across the awaits at :1097 and :1141 |
| C7 | `NativeCiphertextStore` is **built per call** (H5): `load_and_cache` tries `load_fragment` then `load_loose_commit` (2 SQL lookups) per ancestor per walk; `contains_content` loads **full blobs** only to test presence (`load_fragment` + `load_loose_commit` in a `tokio::join!`) | `native.rs:1089`; `:183-214`, `:216-270` | 2 SQL + full-blob transfer per ancestor, no cross-call cache |
| C8 | Keys are remembered into keyhive's in-memory `known_decryption_keys` map with no bound | `native.rs:1211-1218`; `document.rs:721` | memory grows with history |
| C9 | The worker's `plaintexts: HashMap<Vec<u8>,Vec<u8>>` holds **every decrypted change of the document at once**, then moves them into `pending` | `doc_worker.rs:767,777-840,858-883` | peak memory ∝ whole history (H4's "30 MB per drawer load") |
| C10 | Decompress + apply: `Automerge::new()` then `doc.load_incremental(&plaintext)` per change/bundle in a fixed-point loop; automerge decompresses each change chunk and applies its ops | `doc_worker.rs:304-330` (`load_incremental` :322) | O(changes) decompress + column decode + op apply; no bulk `load` |
| C11 | Remote arrival on a writable node: `reconcile_causal_coverage` hydrates even without a handle | `doc_worker.rs:1508,1523-1545` | a full hydrate per remote write batch |
| C12 | Writes also copy: `get_changes(&before_heads)` and `change.bytes().into_owned()` (`lib.rs:1627-1634`), `index_causal_ciphertext` **decodes the whole blob to read one hash** (`sedimentree.rs:513-535`), `stage_automerge_ingest` copies each change (`support.rs:~245`) | | write-side copies |
| C13 | `put_doc` path: `initial_content.save()` up to three times (`hub.rs:387,432,440`) | H9 | create-side |
| C14 | The cabinet read path wraps a *read* in `with_document` (which does `get_heads` twice) and runs the gate per datum, whose `get_all_at` rebuilds a clock from explicit heads per call ("costing time proportional to the document", `big_cabinet/doc/gate.rs:388-394`) | `big_cabinet/doc/read.rs:36-60` | cabinet-level, beyond big_repo |

Staleness note on the prod ledger's items: H2 ("every commit stores a full `doc.save()`") does not
match `lib.rs:1615-1634`, where each loose commit carries only its change bytes; and H4's N(N+1)/2
is already addressed by walking descendants first (`doc_worker.rs:757-776` comment). Both are
worth re-measuring before they are treated as open.

### 5.2 What a measurement needs

The harness exists: `src/big_node/e2e/costs.rs` (805 lines): a counting `GlobalAlloc` (:66),
`measure()` (:225), a `Table` (:290), the `long_test_cost_table`, run in release with
`BIG_NODE_COSTS_NOTES=200 cargo nextest run --release -p big_node --no-capture -E 'test(/costs::/)'`.
It counts allocations (deterministic) and reports wall time. For hydration it needs:

1. A **cold-hydrate scenario**: N docs times M commits, then boot a fresh `BigRepo` on the same
   storage (or wait out the 3 s TTL) so the worker, `recovered_keys` and the resident tree are
   cold; separate the cases "keys cached" vs "cold keyhive".
2. **Phase counters inside `load_doc_snapshot`**: the INSTR lines already exist
   (`doc_worker.rs:743-744`, the final `INSTR load_doc_snapshot` info line with `instr_cached`,
   `instr_causal`, `instr_covered`, `instr_walk`); summarize with `x/instr-summary.ts`. Add
   per-phase timers for SQL read, decode, AEAD, bincode, `load_incremental`.
3. **Bytes allocated per byte hydrated** (copy count) from the counting allocator, by size class,
   to confirm C3-C6.
4. Release wall vs CPU, `strace -c` for SQL round trips (`perf-investigation` skill), and the
   instruction-count tier from the `benchmarking` skill for CI.
5. The two other scenarios the prod ledger names: cold index build over N encrypted docs
   (W5) and a drawer-scroll read of thousands of datums (cabinet level, C14).

### 5.3 Candidate levers

| Lever | Targets | What it needs |
|---|---|---|
| L1 Longer-lived hydrated cache (bytes-bounded LRU instead of 3 s TTL) | C1-C10 on warm reads | memory budget; invalidation when another process writes (`data_version`); the bundle's validity/broken flags already model staleness |
| L2 Persisted plaintext snapshot per `(doc, heads)` (`doc.save()`), loaded with `Automerge::load` | C3-C10 | encryption at rest under a local key (`secret_blobs` has the DEK machinery); invalidation on revocation (whiteout); recomputation for remote merges |
| L3 Materialized datum projection `(doc, heads)` written in the commit transaction | C14 and cold-index builds | see 5.4 |
| L4 Single decrypt+decode of the entrypoint | C6 | a keyhive API taking an already-decrypted entrypoint (we own `../keyhive`); bincode `Envelope` goes away with Keyline anyway |
| L5 One `CiphertextStore` per hydrate, existence queries instead of blob loads, one SQL for both kinds | C7 | `NativeCiphertextStore` lifetime change; `list_commit_ids`/`contains` queries (exist: `Storage::list_commit_ids`) |
| L6 Cheap blob handoff (`Arc<[u8]>` or take-by-value) | C3-C5 | a subduction `Blob` API change (cheap clone), or reading straight into the decode buffer |
| L7 Stop decoding to index | C12 | pass `pcs_update_op_hash` from the encrypt site into `insert_commit_rows` |
| L8 Batch persist (one transaction for N commits and their CGKA ops) | section 2 | `store_commits_batch` exists upstream; keyhive staging for the ops |
| L9 Parallel decrypt | C5-C6 | breaks the single-threaded goal and `kh_doc.lock()` is held across the walk; native-only |
| L10 Hydrate the doc from the fragment bundle only for cold docs; keep loose commits incremental | C9-C10 | sedimentree fragmentation depth tuning; fragments are already automerge bundles (`support.rs:213-259`) |

### 5.4 A `(doc, heads)` datum projection written in the commit's SQL transaction: what makes it hard

1. **Remote writes cannot be projected in their commit transaction.** Subduction stores the
   ciphertext (R2) before anyone decrypts; decryption needs keyhive/CGKA state that may not have
   arrived (`PendingMaterialization`, `doc_worker.rs:1269`). So there are two writers of the same
   table: the local commit (synchronous) and a worker for everything else. The key must make the
   second idempotent against the first.
2. **Which heads?** The projection key is the *automerge* heads of the merged document, but the
   commit transaction knows only the sedimentree heads, and the two differ on purpose when
   key-only checkpoints exist (`doc_worker.rs:~1085-1092`; the "head-divergence flake" note on
   `head_state`, :1315). A local write is made at base heads B on a document whose merged heads
   are H; the post-write heads include everything the node has merged. Only a hydrated doc (or a
   maintained op index) knows them, so the writer needs the current merged state at commit time.
3. **Two writers in different processes/transactions** each compute from their own base. Rows form a
   DAG of head sets; "current" needs a compare-and-set against the true merged heads, i.e. an
   explicit current-pointer row updated in the transaction, plus a rule when heads diverge.
4. **The schema is the cabinet's, not big_repo's.** What a "datum value" is (signatures, `dmeta`
   with lists, CID of the native encoding) is defined in `big_cabinet_core`. big_repo's commit
   would have to call a cabinet-supplied projection function inside its transaction, or the datum
   face moves into the cabinet and uses a big_repo "batch of rows + commits in one transaction"
   primitive. The second is the cleaner layering.
5. **Authorization state in the key** (section 4.2 item 3), and the revocation/whiteout rebuild.
6. **Plaintext at rest.** The ciphertext is what relays hold; a plaintext projection is only
   legitimate on nodes that can already decrypt and must not exist on a relay or for
   `partially_decrypted` docs (`doc_worker.rs` `partially_decrypted`; heads of the decryptable
   subset only).
7. **Write-lock length.** SQLite has a single writer; computing the projection inside
   `BEGIN IMMEDIATE` lengthens the lock. It must be computed before the transaction and passed in,
   which requires the base to be known before BEGIN, which is item 3 again.
8. **Reads that are not datum-shaped.** `BigDocHandle::with_document_read`/`hydrate_path_at_heads`
   have external callers (big_cabinet, big_node, daybook_core, rt, tables); a projection serves
   only the cabinet's datum face.
9. **A first build is still a full hydrate** (the cold index build in W5). The projection fixes
   steady state, not backfill; and a schema change in a datum type (`schema_compat`) forces a
   rebuild.
10. **Historical reads.** The docs service reads `At::Heads` (`doc.rs:478`, `read.rs`, `list.rs:45`
    uses `fork_at`); a projection of current state does not serve them.

Note that the cabinet already has a *membership/version* projection of datums:
`big_cabinet/index/datum_set.rs` stores `(doc, datum_key, doc_heads, datum_heads, clock)` fed from
the frontier stream (tables `big_cabinet_datum_set_docs/entries`,
`index/datum_set/sqlite/migrations/001_init.sql`). It does **not** store values; reads still hydrate.

---

## 6. Cabinet's actual automerge usage: scoping a key-value datum face

Question: `big_repo::runtime` offers a document face (the live automerge object) and a datum face
(KV-shaped); reads come from a projection keyed `(doc, heads)` written in the commit transaction;
writes are a batch of key to value puts/deletes across N docs in one transaction.

### 6.1 Who touches automerge, and how

Direct automerge access via `BigDocHandle` exists in **two places only**:

1. The cabinet docs service: `big_cabinet/doc.rs` (`handle` :465, `present_keys` :476 uses
   `get_at`/`map_range_at`), `doc/{read 126, write 182, pipeline 570, merge 139, branch 252,
   create 157, list 81, inspect 204, gate 428, claim_history 797}.rs`, and
   `big_cabinet_core/{dmeta/storage.rs 675, dmeta/native.rs 401, datum/codec.rs 1017 +
   codec/{target,walk,validation}.rs, datum/lexicon.rs}` which hold the storage codec.
2. The cabinet datum-set index: `big_cabinet/index/datum_set.rs` (1469): `with_document`
   at :959 and :1028; it diffs `get_changes(old_heads)` and decodes every op with
   `change.decode().operations` (:732-775), classifying by object path
   (`parents_at(..).path()` :751).

Everyone else goes through the docs protocol and sees only opaque `ChangeHash` tokens:
**big_blobs** (`documents.rs` uses `ReadDatums`/`WriteDatums` with `WriteMode::IfDocUnchanged`,
`:381`; `retention.rs:617`; `workers.rs:435` only watches `AutomergeFrontierEvent`),
**big_nodule** (only boots `BigRepo`; `e2e/schemas.rs:155` is a schema fixture), the cabinet's
own `datum_store.rs` client (`WriteMode::AtHeads`, `:415`) and the drawer service (all writes via
`WriteMode::AtCurrent`, `drawer.rs:733`, `drawer/create.rs:113`, `big_cartridges/registry.rs:990`).
`big_blobs/permissions.rs:153-237` uses raw automerge in a test fixture only. (daybook_core is
the legacy app and also uses `with_document_read`/`hydrate_path_at_heads`; it is outside this
question.)

Document layout (`big_cabinet/doc/pipeline.rs:119-260`): `ROOT["datums"]` is a map; each datum key
is a member holding the value; one reserved member `org.example.daybook.dmeta/main`
(`gate.rs:~47`) holds dmeta: `datums` (per-key metadata including a **`signatures` list**,
`updatedAt`/`deletedAt` lists), `docUuids`/`datumUuids` alias binding maps, and `actors` (each with a
`createdAt` list) (`dmeta/storage.rs:130-190,277-330,383-460,596-662`).

APIs in production code (not tests):

| Operation | Where | Notes |
|---|---|---|
| `transaction_at(PatchLog::null(), &base)`, `commit_with(message=command id)`, `rollback` | `write.rs:133-159`, `read.rs:73` (a read pinned at heads), `datum_set.rs` projection | every datum write is at explicit base heads; read-at-heads goes through a transaction because automerge offers no other pinned read (`datum_set.rs:1024-1027` comment) |
| `set_actor` | `write.rs:149` (actor derived per node and doc) | a per-actor `seq` is implied |
| Map `put`/`put_object`/`insert`/`delete`, `get_all` (conflicts), `map_range` | `pipeline.rs:125-152`, `dmeta/storage.rs` throughout | `ensure_object` creates maps and lists |
| Lists: `insert_object`, `insert`, `delete`, `length_at` | `dmeta/storage.rs:596-662` (signatures, timestamps), `claim_history.rs:494` | dmeta lists; also `x-automerge="list"` on one datum field |
| `get_all_at` / `get_at` / `map_range_at` / `list_range_at` / `length_at` | `gate.rs:98-107,398-420`, `claim_history.rs:478-779`, `inspect.rs:147`, `doc.rs:478` | conflict-aware historical reads |
| `get_changes`, `get_change_by_hash`, `get_change_meta_by_hash` (`max_op`), `get_heads`, `get_missing_deps` | `gate.rs:378-386` (`datum_version`), `merge.rs:86-89`, `datum_set.rs:732,958` | a datum's **version is a set of change hashes plus a Lamport clock**, not a revision number |
| `fork_at`, `merge` | `branch.rs:71,197`, `merge.rs:61,94`, `list.rs:45` | document-level CRDT operations |
| `parents_at(...).path()`, `change.decode().operations` | `datum_set.rs:732-775` | change-level op decoding already in production |
| Text, counters, marks | codec supports them (`datum/codec/target.rs:162-188,340-370`, `dmeta/native.rs:236-246`) | **declared by no production schema**: the 54 `#[macros::datum]` types in production crates are records; the only automerge storage marker is `x-automerge="list"` on `Blob.inline` (`big_blobs_core/datum.rs:16`). Text/Counter appear in codec tests and `big_nodule/e2e/schemas.rs:187` only |

Datum shapes (all KV/record-shaped): blobs (`Blob`, `BlobPin`, `Representation`,
`big_blobs_core/datum.rs`), drawer docs and rosters (`Drawer`, `DrawerRoster`, `DrawerDocEntry`,
`DrawerChildEntry`, `DrawerMetaDoc`, `KnownDrawerEntry`, `DrawerSuccessor`,
`big_cabinet_core/drawer.rs:25-417`), dmeta, cartridges, nodule config (`big_nodule_core/config.rs`).

### 6.2 What is KV-shaped and what needs CRDT semantics

| Class | Evidence | Share |
|---|---|---|
| Plain map-of-values: a datum is a JSON record of scalars and nested maps, written whole-value with per-field diff against the base (`reconcile_json_atomic`, `target.rs:101-`), read by `hydrate_value` | all 54 production types | the **datum payload itself**, effectively all of it |
| KV-shaped but with a signature protocol on top: each write also rewrites the datum's metadata (`signatures`, `updatedAt`, alias bindings, actor binding) in the same change, signed over the CID of the native value | `pipeline.rs:179-260` | every write; the *metadata is not KV in storage* (lists) but could be, by layout |
| Needs the change DAG: datum version = introducing change hashes + `max_op` clock; `IfDatumsUnchanged` compares them; the gate reconstructs claims at "introduction" heads | `gate.rs:1-34,378-386`, `claim_history.rs`, `pipeline.rs:343-359` | verification and conflict detection; an automerge **change exists per write** in any design that keeps `ChangeHash` as the version |
| Needs conflict surfacing: concurrent writers to one slot yield several current signatures and values; the gate reads every conflicting envelope (`get_all_at`) and reports all authors; whole-datum LWW by op id decides the visible value. The docs service itself offers no read of conflicted values (`gate.rs:19-22`) | `gate.rs:84-113,398-420` | needed for correctness of verification, not exposed to users |
| Genuinely collaborative CRDT: text, lists of independent elements, counters, marks | codec support only; no production schema; `Vec<T>` fields (5 in the drawer/blobs/cartridges/nodule types) are lists whose concurrent inserts would merge | **none required by a production datum today**; dmeta's own lists are replaced wholesale |
| Document-level: `fork_at` (branch), `merge`, history reads at arbitrary heads | `branch.rs`, `merge.rs`, `list.rs:45`, `At::Heads` | **must stay on the document face**. Merge brings the source's history in unexamined |

Conclusion: the *payload* is KV; the *machinery around it* (versions as change hashes, signatures
in lists, conflict-aware gate, branch/merge, historical reads) is not. A datum face can serve
current-state reads, `AtCurrent`/`IfDocUnchanged`/`IfDatumsUnchanged` writes, and listings.
`AtHeads` writes (`datum_store.rs:415`, used by the cabinet's datum-store client) and
`At::Heads` reads need the document face or a hydrate fallback.

### 6.3 Can a datum write skip hydration?

Version: automerge **0.11.0** from crates.io (`Cargo.toml:287`, `Cargo.lock:1125`); a commented-out
git "fragments" branch override exists (`Cargo.toml:410`); autosurgeon is a fork
(`Cargo.toml:397`).

What a change must contain (from `automerge-0.11.0/src/legacy/mod.rs:295-350`,
`change.rs:201-221`): `Change::from(ExpandedChange)` builds the binary change from
`ExpandedChange { operations: Vec<Op>, actor_id, hash: Option, seq, start_op: NonZeroU64, time,
message, deps: Vec<ChangeHash>, extra_bytes }`, and `Op { action: OpType, obj: ObjectId, key: Key,
pred: SortedVec<OpId>, insert: bool }`. **Every field is `pub`**, `OpId::new(counter, &ActorId)`
is `pub`, `Change` is re-exported at the crate root (`lib.rs:298,306`), and
`Automerge::apply_changes` / `load_incremental` (`automerge.rs:927,997`) ingest it. The conversion
panics only on out-of-order preds, which `SortedVec` prevents. Production code already uses the
inverse direction, `Change::decode()` (`datum_set.rs:735`), and builds `ObjId::Id(counter, actor, 0)`
from legacy ids (`datum_set.rs:751`).

What the writer must know without a loaded doc:

| Needed | Source today | In a projection |
|---|---|---|
| `deps` = current heads | `get_heads` | `(doc, heads)` row: yes |
| `start_op` > max op of all deps | `get_change_meta_by_hash(..).max_op` (`gate.rs:381`) | store `max_op` for current heads |
| `seq` and actor for the writing actor | the doc's per-actor change count | store last `seq` per actor (the actor is derived per node and doc, `write.rs:149`) |
| `obj` ids: the op ID of `datums` and of every nested map/list the write touches | `get_all` returns `(Value, ObjId)` (public) | store, per datum, the object ids of its subtree |
| `pred` for each put/delete: the op IDs of **all currently visible ops at that key, including conflicting ones** | `get_all` / `map_range` return value op ids | store, per object and key, the visible op id set |
| list element ids for deletes inside lists (dmeta signature/timestamp lists, `Vec` fields) | `list_range`, element ids | store per list; inserts at the head need none |

So the projection would hold an **op-id index of the live state** (obj, key to visible op ids), in
addition to values. It can be maintained *without loading the document*: decode each applied
change with `Change::decode()` and update `visible(obj, key) = ops not named in a later op's pred`
(causal order is given by the sedimentree topsort). For map keys, scalars, nested-map creation and
deletes this is exact. Counters: an increment's pred is the counter's put op id (also tracked).
Lists and text need element-order state (the sequence tree), so those are **not** feasible without
either storing the sequence or hydrating.

Verdict:

- **Feasible with the public API** for writes made at *current* heads consisting of map puts,
  nested-map puts, deletes, and (with extra tracking) counter increments, provided the projection
  stores `(heads, max_op, per-actor seq, obj ids, visible op ids)`. Evidence: public fields and
  `From<ExpandedChange> for Change` above; no `unsafe`, internals or fork needed.
- **Needs a fork or hydration** for lists and text edits (element ids) and for any write at
  historical heads (`AtHeads`, `transaction_at(base != current)`), because pred sets must be those
  visible at the base.
- **Unverified (no code was run):** (a) whether `Automerge::apply_changes` validates `obj`/`pred`
  against the doc or silently applies a malformed op (a wrong pred would not be caught at
  construction and could diverge replicas); (b) byte-equality of a hand-built change with one
  produced by `Transaction::commit` (matters only if two nodes derive the same change). A
  round-trip unit test in `big_cabinet_core` is the first thing to write: build the change from the
  projection and from a transaction on the same doc, apply both to fresh docs, and compare heads
  and `save()` bytes. The AGENTS.md guidance applies: write the test before the design.
- **dmeta's lists are the practical obstacle**: every datum write replaces `signatures`
  (`storage.rs:596-662`: delete old elements, insert a new map), so a no-hydrate write must
  delete the existing signature list elements by element id. Re-laying dmeta (per-datum scalar
  `signature` fields instead of a list, `updatedAt` as a scalar) removes the need for list element
  ids entirely and is allowed (no compatibility, no installs), but it changes the gate's
  "introductions of the signatures currently covering it" model, which relies on a list so that
  concurrent signers both persist (`gate.rs:1-34`). That is a cabinet design decision, not a
  runtime one.
- The cabinet signs over the CID of the *native value read back after the mutation*
  (`pipeline.rs:196-213`), so the value bytes are computed on the writer; no hydration is needed
  for that if the value is known (it is: it is the write's own input merged with the stored value
  for per-field diffs, which needs the stored subtree, hence the projection holds values too).

### 6.4 What the cabinet's write machinery would shrink to

Today:

| Piece | Size | Exists because |
|---|---|---|
| `Failure::{Refused, Ambiguous}` | `pipeline.rs:30-47`, `write.rs:164-165` | memory is ahead of disk: "the change may already be committed in memory while its persistence failed" |
| Command rows `pending to committed to done` with request digest | `doc/commands.rs` (276), `pipeline.rs:60-80` (`replay`) | idempotent retry **and** crash recovery |
| Boot pass `recover` / `recover_branch` | `pipeline.rs:371-570` (~200 lines) | read the outcome off the document: the command id is the change message (`write.rs:155`); a merge happened iff the target holds the source heads |
| Two-pass write (dry-run staging then commit, condition checked twice) | `write.rs:108-160`, `pipeline.rs:333` | refusals must surface before signing; the in-memory doc can move between passes |
| Create via reservation | `doc/create.rs`, `lib.rs:947-1013`, `hub.rs:342-470` | five durable steps (reservation, keyhive doc, events, sedimentree, delete reservation) |
| Drawer op ledger | `drawer/ledger.rs` (571, tests from :400), 7 `Op` variants (:36-87), `resume_*` per op (`drawer.rs:1067-1081`), 12 non-test `crash_at` points (`CrashPoint` :99-120 lists 8), `add_doc.rs:150-262` | an op spans roster datums in several drawer docs plus keyhive grants; `RosterWritten` ("the projection row never followed"), `RosterRemoved` ("the Keyhive grant was never revoked"), `DocGranted` |
| Revoke-then-grant narrowing | `add_doc.rs:~140-160` | no atomic "change access" |

Under a datum face with N-doc atomic commit (commit rows + roster datums + keyhive grants in one
SQL transaction, which needs the SQL-backed Keyline and staged CGKA):

- `Failure::Ambiguous` disappears: a failure is atomic. `Refused` is the only class.
- The command row shrinks to an **idempotency table** `(command, request digest, receipt)` written
  in the commit transaction. No `pending/committed` states, no boot pass, no change-message
  convention, no `recover*`. A retry either finds the receipt or finds nothing and runs again.
  (`CommandInFlight` remains only for concurrent duplicates in one process.)
- The two-pass write collapses to read the projection, check the condition as a SQL
  compare-and-set inside the transaction (replacing the in-memory re-check), sign, commit.
- Create becomes one transaction (allocate id, keyhive doc, events, sedimentree, initial datums);
  `allocate_id`/`abandon_allocation`/reservation rows go.
- Drawer ops (`AddDoc`, `RemoveDoc`, `AddChild`, `RemoveChild`, `Invite`, `Revoke`, `Create*`)
  become one transaction each: grant + roster entry in every affected drawer doc. The resumable
  saga, `Op` payloads, `CrashPoint`s and `resume_*` functions go; so does the revoke-then-grant
  window. What stays: the receipt/idempotency row; the post-commit *wait* for keyhive
  reconciliation (`add_doc.rs:~165`, `grant_doc_access` awaits derived state), which is a read of
  eventually-consistent derived state and not a saga step; and capability/authorization checks.
- `branch` and `merge` stay on the **document face** (`fork_at`, `merge`, history reads). Branch
  is create-with-initial-content plus a backlink write; merge brings foreign history unexamined
  and re-encrypts under the target's key, so it is not a datum op. Their command rows and recovery
  (`recover_branch`, `Detail::{Branch, Merge}`) remain, though they too can lose the
  Ambiguous class if their storage step is one transaction.

Caveats: (a) all of this requires the same prerequisites as section 2.3 (SQL-backed Keyline and
CGKA state; staged encrypt), without which the datum face would still leave keyhive events in
separate transactions; (b) the datum face must also emit the change that peers see (the
no-hydrate change builder of 6.3, or a hydrate), since remote peers expect automerge changes in
the sedimentree; (c) the gate/verification half of the cabinet (`gate.rs`, `claim_history.rs`)
still needs history for *remote* writes and is unaffected.

---

## 7. Open questions for the operator

1. **Atomicity scope.** Is "one SQL transaction for a local multi-doc write" meant to include
   keyhive state (CGKA ops, secrets, delegations) from day one, or may the first cut leave keyhive
   in its own transactions behind a durable intent row? Everything in section 2 turns on this.
2. **Keyhive archives as files.** Archives live on disk, DEKs in the OS keyring or files
   (`keyhive_storage.rs:886-899`), and the keyhive graph is in memory. For "one SQLite database as
   the coordination point", do archives move into SQL (they are compacted snapshots of the event
   log), and does the in-memory graph stay as a cache of the SQL Keyline?
3. **Remote content and the doc worker.** Remote writes already route to a doc worker
   (`hub.rs:2309-2358`), contrary to the Decisions bullet. Do you want remote content to stop
   hydrating on arrival (so `reconcile_causal_coverage`'s checkpoint minting, `doc_worker.rs:1508`,
   moves to a separate scheduled task), or is eager coverage on writable nodes intended?
4. **Propagation of local commits.** Local commits propagate through big_sync (a part revision
   triggers `sync_obj`), not through subduction push; fragments push. Keep this, or push commits at
   commit time and let big_sync reconcile? It decides whether the part-store write is part of the
   commit contract.
5. **AFW's fate.** Should the automerge-frontier scope survive (as an asynchronous projection of
   hydrated heads, ADR 024's change stream) or be replaced by a projection written in the commit
   transaction? The two heads notions differ with checkpoints (section 5.4 item 2); which is the
   public one?
6. **Who is the "author" for Keyline's read-time check?** The subduction signer is the node, the
   cabinet signs per actor in dmeta. Should the runtime filter by node signer, or leave per-datum
   authorship entirely to the cabinet gate?
7. **`Public` replacement.** Six big_repo tests and two daybook_core sites depend on the `Public`
   agent. Do we adopt the upstream "app-public" pattern (reader principal with a shipped secret)
   now, or only delete `Public` and rewrite those tests around explicit principals?
8. **Wire and archive compatibility with the Keyline subduction.** Moving to upstream
   `subduction_keyhive` (`SUK\x01`) breaks the wire with our fork. Do we wait for upstream to
   release, carry our own port, or rebase the fork onto `keyline-take-2`? (The PR text was not
   readable here; please confirm the intended review state of #342.)
9. **`AtHeads` writes and `At::Heads` reads.** They need the document face. Is it acceptable that
   the datum face rejects or falls back to hydration for them? Who uses `AtHeads` besides the
   cabinet's `datum_store.rs:415` client?
10. **dmeta layout.** Replace the `signatures` list with scalar fields to allow no-hydrate writes
    (6.3)? That changes the concurrent-signer semantics the gate relies on.
11. **Datum projection placement.** Does the projection live in big_repo (a callback inside the
    commit transaction) or in the cabinet on top of a big_repo "N docs of commits plus rows in one
    transaction" primitive (section 5.4 item 4 recommends the latter)?
12. **Quadratic and stale H items.** H2 and the N(N+1)/2 half of H4 look stale against the code
    (section 5.1 note). Re-measure before the dispositions are filled, or fix first?
13. **Part-store schema in big_repo migrations.** `001_init.sql` carries big_sync's schema. When the
    part store moves to `big_automata_sqlite`, whose migrations own it, and how do the three
    scopes share one database?
14. **Test strategy.** 28k lines of end-to-end tests over real iroh/subduction. Do you want the
    sans-IO `runtime` verified by DST plus a trimmed conformance subset of these tiers, with the
    rest deleted as the machines are ported?
15. **Runtime shape evidence.** The only `DocIo` implementations are `NativeBigRepoIo` and two test
    fakes, all `Sendable` (`native.rs:451`, `doc_worker/tests.rs:251,454`); `doc_worker.rs:67,197`
    and `types.rs:323,574` use tokio types directly. Confirms the lead's suspicion that runtime2
    is not single-threaded-capable today; do you want the `Local` form proven (a wasm build of the
    worker with a non-Send fake) before the rewrite?
