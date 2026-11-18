# big_automata port ledger

Status: Living ledger for the file-by-file port of big_sync into big_automata.
Owner: operator + prod-readiness lane (pairing). Scope: `src/big_sync_core`,
`src/big_sync`, `src/big_automata`. Companion to `prod-readiness-ledger.md` (W4,
W7). Updated: 2026-11-17

How we work: the operator reviews each file and leaves FIXMEs; the lane does the
mechanical port and the reworks the operator approves. No cargo during pairing
until the crate compiles. Line counts are as of `main` at `014c965a`.

Status words: **ported** (reviewed, matches or deliberately diverges),
**rewritten** (new shape agreed, not yet compiled), **copied** (verbatim,
unreviewed), **partial**, **not started**, **out of scope** (moves elsewhere,
see the redesign section).

## big_sync_core (the machine)

| File                                                           | Lines | Role                                                                                                                                           | big_automata                                         | Status        | Notes                                                                                                                                                                                                                                                         |
| -------------------------------------------------------------- | ----: | ---------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lib.rs`                                                       |  5866 | `BigSyncMachine`, `PeerState`, `SyncStatMachine`, and four impl sections: peer, cursor, bucket, sync support                                   | `lib.rs` (skeleton), `peer.rs`, `stat.rs`            | partial       | 6 of 8 `handle_evt` arms are `todo!()`                                                                                                                                                                                                                        |
| ↳ `PeerState` + "peer support" (`lib.rs:319–407`, `1171–1648`) |  ~570 | peer lifecycle, part strategy decisions, replay subscription and pages                                                                         | `peer.rs`                                            | copied        | Verbatim, beside the operator's started `handle_update_peer_evt`/`handle_remove_peer_evt`; names don't line up yet (`PeerPartStrat` vs `PeerPartStrategy`, `objs` vs `objects`); `PeerState` is private. Was the "most ambitious piece"; parked.              |
| ↳ "cursor support" (`lib.rs:1650–3008`)                        | ~1360 | turns `CursorMachineCommand`s into tasks: sync/remove worker coalescing per object, zombie-removal deferral (`pending_removals`), replay pages | —                                                    | not started   | Owns the coalescing contract `cursor.rs` now documents: report every cursor a coalesced task held                                                                                                                                                             |
| ↳ "bucket support" (`lib.rs:3009–3382`)                        |  ~370 | drives `BucketMachine` tasks                                                                                                                   | —                                                    | not started   |                                                                                                                                                                                                                                                               |
| ↳ "sync support" (`lib.rs:3383–…`)                             | ~1200 | sync task completion, stat updates                                                                                                             | —                                                    | not started   |                                                                                                                                                                                                                                                               |
| ↳ `SyncStatMachine`                                            |  ~400 | full-sync waiters, per peer/part stats                                                                                                         | `stat.rs` (404)                                      | partial       |                                                                                                                                                                                                                                                               |
| `cursor.rs`                                                    |  1052 | `CursorSyncMachine`                                                                                                                            | `cursor.rs`                                          | rewritten     | `JobKey { Sync, RemoveFromPart }`; 19 tests ported/adapted; fully rejected touch now emits nothing                                                                                                                                                            |
| `watermark.rs`                                                 |  1013 | `WatermarkMachine` (lanes, payload, supersede closure)                                                                                         | `watermark.rs` + `watermark/tests.rs`                | rewritten     | Lanes dropped (kind lives in the key); W-1 (slots opened ready) and W-2 (`unwrap_or_default` admission) fixed; 11 example tests + oracle proptest. Delta-walker features (`settle_job`, `force_finish`, multi-waiter batch slots) deliberately not ported yet |
| `bucket.rs` | 778 | `BucketMachine` (per part; FIXME says it should be per peer) | `bucket.rs` | copied | Verbatim except `obj_key`/`part_key` and `on_obj_sync_completed(&ObjKey)` (no `SyncJobEvt`); 3 tests ported; no type errors (borrowck unverified until the crate type-checks) |
| `scheduler.rs` | 681 | task table, retries | `scheduler.rs` + `scheduler/tests.rs` | rewritten | `Scheduler` only so far. `KeyedScheduler` (via `TokioKeyedScheduler`) has 10 users: big_repo runtime2's `automerge_frontier_worker`, `causal_checkpoint_worker` and `group_part_worker`, plus seven daybook_core blob and index modules. It's live infrastructure, not legacy. **Ported as `scheduler/keyed.rs`** on the new `Scheduler` (sans-IO: the caller supplies `now` and reports `complete(task)` / `fail(task, min_delay, now)`; stale reports return `None`; `replace`/`replace_with`/`cancel`/`park`/`wake` kept because `group_part_worker`, `pin_worker` and `facet_set` still wake). 13 unit tests + model proptest; 71/71 crate tests pass. `TokioKeyedScheduler` itself reads `Instant::now()` internally: audit item. `park`/`wake` was dropped by the frontier worker in favour of delayed retry (an unwoken park stalls the walker's contiguous prefix). big_blobs avoids it by consuming walkers serially and durably parking (`projection.rs::retry_parked`). Verbs: `queue`, `complete`, `stop`, `retry`, `tick`, `next_due`, drains; the `stopped` licence set and both respawn variants folded into `retry`. 13 unit tests + model proptest; all pass |
| `tasks.rs`                                                     |   194 | task seeds and kinds                                                                                                                           | `tasks.rs` (134)                                     | partial       |                                                                                                                                                                                                                                                               |
| `tasks/decide_peer_strat.rs`                                   |   724 | cursor-vs-bucket decision                                                                                                                      | `tasks/decide_peer_strat.rs` (275)                   | partial       |                                                                                                                                                                                                                                                               |
| `tasks/replay_page.rs`                                         |   324 | replay page task                                                                                                                               | —                                                    | not started   |                                                                                                                                                                                                                                                               |
| `tasks/list_bucket.rs`                                         |   122 | bucket listing task                                                                                                                            | —                                                    | not started   |                                                                                                                                                                                                                                                               |
| `tasks/leaf_buckets.rs`                                        |   109 | bucket leafing task                                                                                                                            | —                                                    | not started   |                                                                                                                                                                                                                                                               |
| —                                                              |     — |                                                                                                                                                | `tasks/set_peer_part_cursor.rs` (43)                 | new           | Cursor persistence as a task                                                                                                                                                                                                                                  |
| `outbox.rs`                                                    |   216 | correlated command outbox                                                                                                                      | `outbox.rs` (178)                                    | partial       | review                                                                                                                                                                                                                                                        |
| `mpsc.rs`                                                      |    97 | channel helpers                                                                                                                                | `mpsc.rs` (99)                                       | ported?       | review                                                                                                                                                                                                                                                        |
| `ids.rs`                                                       |   464 | `ByteKey`, `ObjKey`, `PartKey`, `PeerKey`, `BuckId`                                                                                            | `ids.rs` (381)                                       | partial       | review                                                                                                                                                                                                                                                        |
| `rpc.rs`                                                       |   951 | wire types                                                                                                                                     | `rpc.rs` + `rpc/{bucket,replay,summary}.rs`          | partial       | `BigSyncRpcClient<K: FutureForm>` trait added                                                                                                                                                                                                                 |
| `peer_routes.rs`                                               |   185 | pending-part takeout, part teardown on route change                                                                                            | —                                                    | not started   | Folds into `peer.rs`                                                                                                                                                                                                                                          |
| `part_store.rs`                                                |   185 | thin `PartStore`/`PartStoreReadOnly` traits + contract tests                                                                                   | `store.rs` (aliases only)                            | not started   | See redesign                                                                                                                                                                                                                                                  |
| —                                                              |     — |                                                                                                                                                | `fingerprint.rs` (259)                               | new           | Seeded SipHash fingerprints                                                                                                                                                                                                                                   |
| —                                                              |     — |                                                                                                                                                | `okv.rs` (23)                                        | new           | FIXME: replacement for `KeyedFrontier` (ordered KV)                                                                                                                                                                                                           |
| `keyed_frontier.rs`                                            |   153 | keyed collapsed change frontier contract                                                                                                       | —                                                    | out of scope? | Frontier family, see below                                                                                                                                                                                                                                    |
| `revisioned_store.rs`                                          |   659 | revisioned source boundary                                                                                                                     | —                                                    | out of scope? | Frontier family                                                                                                                                                                                                                                               |
| `live_revision_watch.rs`                                       |    65 | live tail of a revisioned source                                                                                                               | —                                                    | out of scope? | Frontier family                                                                                                                                                                                                                                               |
| `serial_delta_walker.rs`                                       |   754 | serial settlement over a revisioned source                                                                                                     | —                                                    | out of scope? | Delta-walker family (ADR 008)                                                                                                                                                                                                                                 |
| `concurrent_delta_walker.rs`                                   |   691 | concurrent settlement                                                                                                                          | —                                                    | out of scope? | Delta-walker family; the only user of watermark batch slots                                                                                                                                                                                                   |
| `delta_walker_state.rs`                                        |   466 | durable walker progress contract                                                                                                               | —                                                    | out of scope? | Delta-walker family                                                                                                                                                                                                                                           |
| `delta_walker_sparse_state.rs`                                 |    26 | sparse per-consumer memory                                                                                                                     | —                                                    | out of scope? | Delta-walker family                                                                                                                                                                                                                                           |
| `tokio_keyed_scheduler.rs`                                     |   318 | tokio executor for keyed commands                                                                                                              | —                                                    | out of scope  | tokio driver crate                                                                                                                                                                                                                                            |

**Frontier and delta-walker family.** About 2.8k lines in big_sync_core plus
about 3k in big_sync (`keyed_frontier/*`, `delta_walker_state.rs`). These are
not the sync machine. big_repo's frontier workers and others consume them.
`okv.rs` already claims the `KeyedFrontier` replacement. **Decide where this
family lives** before porting.

## big_sync (host side: drivers, storage, transport)

| File                                                                                                    |               Lines | Role                                                                                                                                                                                            | Proposed home                                                | Status        |
| ------------------------------------------------------------------------------------------------------- | ------------------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------- |
| `lib.rs`                                                                                                |                  79 | crate root                                                                                                                                                                                      | —                                                            | —             |
| `worker.rs`                                                                                             |                1846 | tokio driver: runs the machine, spawns tasks, owns channels                                                                                                                                     | `big_sync_tokio`                                             | not started   |
| `rpc.rs`                                                                                                |                3151 | irpc server and client, subscription sessions, long poll                                                                                                                                        | protocol logic → core responder; transport → `big_sync_iroh` | not started   |
| `backend.rs`                                                                                            |                 296 | `SyncBackend` trait (what big_repo implements)                                                                                                                                                  | core                                                         | not started   |
| `part_store.rs`                                                                                         |                1100 | `HostPartStore` trait. **Holds about 360 lines of responder protocol as default methods** (`replay_page_round_with_update`, `page_event_is_readable`, `read_denied`, page assembly and budgets) | split: responder logic → core; storage → store contract      | not started   |
| `part_store/contract.rs`                                                                                |                 323 | store contract tests                                                                                                                                                                            | store contract crate (conformance suite)                     | not started   |
| `part_store/host_contract.rs`                                                                           |                4861 | host contract tests                                                                                                                                                                             | store contract crate                                         | not started   |
| `part_store/memory.rs`                                                                                  |                2432 | in-memory store                                                                                                                                                                                 | store contract crate (reference implementation)              | not started   |
| `part_store/sqlite.rs` + `sqlite_core.rs` + `sqlite_read.rs` + `sqlite_write.rs` + `sqlite_frontier.rs` |                4623 | SQLite store                                                                                                                                                                                    | `big_sync_sqlite`                                            | not started   |
| `sqlite/`                                                                                               | ~470 SQL + `gen.rs` | migrations, queries                                                                                                                                                                             | `big_sync_sqlite`                                            | not started   |
| `keyed_frontier.rs` + `keyed_frontier/*`                                                                |                2729 | frontier implementations                                                                                                                                                                        | frontier family (see above)                                  | out of scope? |
| `delta_walker_state.rs`                                                                                 |                 423 | SQLite walker state                                                                                                                                                                             | frontier family                                              | out of scope? |
| `trap.rs`                                                                                               |                 206 | test trap for tasks and store                                                                                                                                                                   | test support                                                 | not started   |
| `test.rs`, `test/stress.rs`, `stress_support.rs`, `test_support.rs`                                     |                4447 | whole-machine tests                                                                                                                                                                             | whole-machine test suite (W7: DST belongs here)              | not started   |

## Redesign: the part store and crate split

See the discussion in chat (2026-11-17); decisions are recorded here once made.

**What the machine relies on from the store today (the implicit contract):**

1. **One node-global, monotonically increasing transaction cursor.** Every write
   is stamped with it. Cursors are "cross-part and peer global" (`part_store.rs`
   note).
2. **Collapsed history.** Only the _latest_ event per (object) is kept, plus
   per-part removal tombstones. Replay yields `ObjChanged` (payload + every
   part) or `ObjRemovedFromPart`.
3. **Per-part authorization, with access transitions stamped on the same
   cursor** (re-stamps, `access_changes`), so `part_dirty_count` can count what
   a principal is behind on.
4. **Incrementally maintained bucket fingerprints per part** (`upsert_bucket`,
   hash-prefix index), shared across peers.
5. **Object parts:** virtual `o:<objkey>` routes, readable only via some part
   containing the object.
6. **Stateful long-poll subscription sessions** whose target set is maintained
   on the serving side (ADR 012 decision 9, ADR 023), plus page budgets.
7. **Durable per-peer, per-part cursors** (`big_sync_peer_cursors`).
8. **Live wakeups** when a new cursor commits.

Items 1–5 are storage semantics. Items 6 and 8 are protocol and session logic
that happens to live in the store trait's default methods.

**Decisions (operator, 2026-11-17):**
- The shape stays close to today. The concrete change is that **the store conformance
  suite** (`part_store/contract.rs`, `host_contract.rs`, with `memory.rs` as its reference
  implementation) **moves into the core crate** (big_automata).
- Web: not a concern for this port. Running big_sync on the web belongs to the big-node-on-web
  goal. The part store keeps using rich SQLite semantics.
- Responder: when we reach `rpc.rs` and the `HostPartStore` default methods, the lane brings
  sketches for a sans-IO responder. Keep them simple; no contortions for the sake of sans-IO.

## Build and test status

- 2026-11-17: `peer`, `stat`, `tasks` and the `BigSyncMachine` items in `lib.rs` are disabled
  with `#[cfg(any())]` (marked `FIXME(port)`) until their ports land. The crate compiles
  clean (warnings are dead code) and **43/43 tests pass** (`cargo nextest run -p
  big_automata` on mothership, worktree `~/scratch/wt-automata`, via `ms-run automata`).
- Failures that run surfaced, all pinned and fixed:
  - `WatermarkMachine::release_stream` panicked on a stream it had never seen (shrunk by the
    oracle proptest; pinned as `release_stream_on_an_unknown_stream_is_a_no_op`).
  - `ByteKey::new` / `ByteKey::from_str` panicked for keys shorter than 32 bytes
    (`copy_from_slice` into the whole buffer); 4 ids tests.
  - `utils_rs::hash::decode_base58_multibase_onto` panicked on empty or non-ASCII-led input
    (`&source[0..1]`); now an error. Shared crate: all 14 callers benefit; `utils_rs`
    clippy clean.
  - `outbox` tests: the port dropped `#[should_panic]` from
    `out_of_order_completion_panics`, and kept a stale `assert!(outbox.is_empty())` after
    replacing the original's `drain()`.

## Per-file review notes

### `bucket.rs` (reviewed 2026-11-17; copied)

The operator is generally happy with it. Agreed directions:
- **Keep per-request seeded leaf fingerprints.** A fresh SipHash seed per request lets
  8-byte leaf fingerprints stay safe. A fixed truncated digest is open to offline
  birthday collisions: a malicious writer finds two colliding versions with about 2^32 work
  and partitions peers. **Change:** compute the leaf fingerprint as `SipHash(seed,
  stored_digest)` from a full-width payload digest persisted at write time, instead of
  re-serializing the JSON payload per entry per request on both sides.
- **Batch the N+1 reads.** `filter_objects` (FIXME "too expensive") reads `obj_payload` and
  `obj_parts` per entry; `filter_buckets` reads one `get_bucket_summary` per bucket. Replace
  with `bucket_summaries(part, ids)` and `obj_states(part, objs)` (digest plus membership).
- **Leaves dominate the wire, not lists.** In the recorded sparse case (3 changes in 20k
  objects): 276 summaries (the whole level, since the responder can't prune without the
  requester's fingerprints) versus 230 leaf entries (about 78 per dirty bucket at working
  level 2). List cost is capped by the level, so leave it. Two options for leaves:
  - **(key hash, seeded fp) leaf entries:** about 17 B instead of about 42 B. Fetch full
    keys only for mismatches, or have the requester send its (key hash, fp) list and get
    back only the differences.
  - **Adaptive leafing:** a dirty bucket above a small length threshold (for example 32),
    with remote depth left, gets its 16 children listed instead of being leafed.
- **Decided: leaf entries become (key fp, seeded payload fp)**, with full keys fetched only
  for mismatches (about 2.5x fewer leaf bytes for one extra round trip on dirty buckets).
  **Rejected: the requester sending its own entries.** Bytes are about the same, only the
  direction flips. It saves a round trip at best, and it moves comparison CPU onto the
  responder, which is often a hot relay.
- **Threat model for fingerprints:** writers inside a part (multi-writer drawers, public
  wikis) can be malicious. A colliding digest lets one silently partition peers on an
  object. Relays can withhold anyway, so fingerprints don't defend against them.
- **Stays per part:** authorization is per part, and a cross-part incremental fingerprint
  can't exist because it would include objects a peer may not see.
- **Cleanups:** drop the defensive dedup `HashSet` rebuilt per page in `on_obj_page` (its
  FIXME); drop `changed_at` from `BucketSummary` on the wire (only read by an
  `unreachable!`).
- Rateless IBLT over (key hash, digest) remains the escalation if measured costs demand it.

**Testing** (agreed): bucket machine + in-memory responder over two memory part stores, no
network.
- **Unit tests:** `calc_working_level`, `summaries_agree`, `filter_buckets` against a fake
  store, leaf budget and refill scheduling.
- **proptest model test:** random overlap between the two stores (shared objects, changed
  payloads, deletions and tombstones, per-part membership differences), driven to
  `UpgradeToCursor`.
  - **Completeness:** the emitted sync and remove set equals the oracle difference.
  - **Minimality:** no object is emitted twice.
  - **Bounds:** active jobs stay within `ACTIVE_SYNC_JOB_TARGET`.
  - **Termination:** the walk always reaches `UpgradeToCursor`.
- **Wire accounting:** round trips, summaries and leaf entries against the true difference,
  for fixed seeds in the dense and sparse regimes, snapshotted with insta. This is the first
  consumer of the event recorder.
- **Adversarial:** a crafted fingerprint-cancelling pair is still caught by the count check
  in `summaries_agree`.

### Tasks (reviewed 2026-11-17)

Agreed with the operator:
- **Tasks return their result** (`async fn run(self, cx) -> MachineTaskMsg`) and the driver
  forwards it. `main_tx` in `TaskCtx` goes: it assumed a samod-like design with cross-task
  messaging that never materialized.
- **`TaskCtx` carries the one peer's RPC client**, not a `Map<PeerKey, Rpc>`.
- Some original tasks were long-lived (a responder push stream before the long-poll design).
  Today's tasks are all bounded.
- `decide_peer_strat` keeps its multi-round loop in async code. Tests drive it with a fake
  `BigSyncRpcClient` and a summary store. **The event recorder must also wrap the RPC client
  and store traits**, or those rounds are invisible.
- **Clock injected sans-IO**, as BigRepo does. Caveat: BigRepo's `Clock::instant()` returns
  `std::time::Instant`, which cannot be constructed on `wasm32-unknown-unknown`. Both should
  share a web-safe instant type.
- **Cursor writes: batch + coalesce.** The machine never waits on persistence: the original
  `handle_cmd_success` only pops the serial outbox, and replay is at-least-once, so a lagging
  persisted cursor costs only extra replay after a crash.
- **Scheduler ported (2026-11-17).**
  - **Names:** the operator prefers `queue` (it doesn't spawn immediately).
  - **No attempt budget:** it would make the machine model failures.
  - **Retire verbs:** `complete`, `stop`, and `retry(id, seed, min_delay, now) -> Option<TaskId>`.
    A report for a task stopped while running is stale and returns `None`.
  - **Stop queue:** only tasks the driver may be running enter it.
  - **`next_due`:** O(log n) through a due-time index.
  - **Determinism:** `BTreeMap`s and `Vec`s throughout.
  - **Time:** `Instant` is a type alias with a FIXME for the web-safe type.
- **Testing:** `table_tests!` decision table for `decide_peer_strat` (fake RPC client);
  scheduler unit + proptest; one happy-path and one error test per RPC task.

### `RevisionedStore` (reviewed 2026-11-18)

**Vocabulary:**
- **`RevisionedStore`:** associated types `Revision`, `Entry`, `Selector`, `Error`,
  `Reader<'a>`; verbs `latest_revision`, `open(selector, after)`.
- **`RevisionedStoreReader::next(limits) -> RevisionRead`.**
- **`RevisionRead { Entries { revision, entries }, ReplayComplete { through } }`** and
  **`RevisionReadLimits { max_entries }`**.
- **`KeyedFrontierRevisionReader`:** an adapter that splits frontier pages into one read per
  revision.
- **A contract suite.**

**Reach:** 13 production implementors, **each writing its own stateful `Reader`** (big_blobs
projection, cabinet `datum_set`, big_repo `keyhive_admission` / `keyhive_access_stream` /
`doc_revision_store` / AFW `LocalPartRevisionStore`, daybook `plugs/events` / `doc_delta_store`
/ `facet_set`, ...), and about 35 consumer files (delta walkers, live watches, e2e harnesses).

**Criticism:**
1. **Same stateful-reader shape as the frontier.** The cursor lives inside the reader, the
   replay boundary is captured at `open`, and live waiting happens inside `next()` on each
   backend's own wakeup. 13 hand-written readers means 13 places for boundary and
   missed-wakeup bugs.
2. **The contract contradicts itself.** The module docs say `Entries` is one complete atomic
   revision. The contract suite says a backend may batch several revisions in one `Entries`,
   stamped with the highest revision. Progress-only empty `Entries` are "a per-backend
   capability". Consumers must tolerate every variant.
3. **One revision per `next()`.** The adapter re-groups each frontier page into a `BTreeMap`
   and dribbles it out through a `VecDeque`. `max_entries` bounds the backend page, not what
   the consumer gets per call.
4. **"Inert" in name only:** readers block.
5. **Invariants nobody checks:** a non-monotonic revision is silently dropped by the concurrent
   walker (documented); a double `ReplayComplete` is checked only in the adapter.
6. **Unused generality:** `Revision` is generic but always `u64`; `Error` is generic but
   mostly `eyre::Report`.

**Found while reviewing (2026-11-18):**
- **`LiveRevisionWatch` has no users** outside its own module and tests. Dead code.
- **`keyhive_admission::Reader` breaks the documented contract.** It returns several admission
  rows (each its own seq, so its own revision) in one `Entries` stamped with the max seq. Its
  query isn't bounded by the replay boundary either, so it buffers overflow rows in a
  `VecDeque`. Its wake ordering is careful and correct, but bespoke.
- **`doc_revision_store::Reader` is a projection that re-implements reader plumbing.** It also
  clones each whole page (`entries.clone()`) to hold it as `pending_read`. It should be a
  `map` combinator.

**Disposition:** `okv`'s stateless `read` plus the shared `Tail` helper replace it. A source
implements only `latest_rev`, `read` and `watch`; nobody writes a reader again. Migrating the
13 implementors happens when their crates are ported, not now.

### `okv` spec (proposed 2026-11-18; awaiting operator approval)

Operator, 2026-11-18:
- yes to `sqlite_rs` `Tx::on_commit`;
- the SQLite implementation goes in a new **`big_sync_sqlite`** crate, not behind a feature;
- the read contract (a rev "stream") is being redesigned before any agents are dispatched.


Home: `big_automata/okv.rs` + `okv/` submodules, `strike!` style. The memory implementation
learns from `big_sync/keyed_frontier/memory.rs` (immutable roots, `surelock`). The SQLite
implementation sits behind an `sqlite` feature in the same crate.

**Nouns:**
- **`Rev`** (`u64`): one per committed atomic batch, strictly increasing per store.
- **`Entry<K, V> { key, rev, value: Option<V> }`:** `None` is a kept tombstone.
- **`Mutation<K, V> { Put { key, value }, Delete { key } }`.**
- **`Limits { max_entries: NonZeroUsize, max_bytes: Option<NonZeroUsize> }`.**
- **`Page<K, V> { entries, through }`:**
  - entries are ordered by `(rev, key)`;
  - a page **never splits a revision**;
  - `through` is at least the last entry's rev and at most the requested `through`;
  - every key whose *current* row has a rev in `(after, through]` appears in this page or an
    earlier one.

  Completeness is about the latest state per key: an overwrite during a read shows up later,
  at its own rev.
- **`Selector`:** `All`, `Keys(BTreeSet<K>)` or `Range(KeyRange)`. Part-store selectors stay
  in the part store.
- **`Watch`:** a wakeup handle that carries the latest committed rev, so a waiting `Tail` skips
  the extra `latest_rev` query.
- **`Tail`:** the replay-then-live helper.

**Verbs:**
- **`latest_rev()`.**
- **`read(&selector, after, through, limits) -> Page`:** stateless, and one SQL statement.
  The cutoff is `rev <= (subquery: rev of the max-th row, ORDER BY rev LIMIT 1 OFFSET max-1)`,
  bounded by `through`. A byte budget is a second bound in the same statement.
- **`apply(tx, mutations) -> Rev`:** inside a transaction the caller provides; the only write
  path. The rev counter lives in the same transaction, so a rollback discards it.
- **`watch() -> Watch`.**
- **`Tail::open(store, selector, after)`** captures `boundary = latest_rev`.
- **`Tail::next(limits)`** returns `Page`s until `through >= boundary`, then exactly one
  `ReplayComplete { through: boundary }`, then live pages. It waits on `Watch` only when
  `latest_rev <= after`.

**Wakeups:** in-process wakeups fire from a new `sqlite_rs` `Tx::on_commit(callback)`, which
runs only after a successful commit. The memory implementation fires them from its own commit.
Cross-process wakeups (`PRAGMA data_version` polling) are deferred, and listed in the
multi-process blockers.

**Errors:** `Res` for backend failures; invariant breaks panic.

**SQLite layout:**
- `okv_entries(namespace_id, key BLOB, rev INTEGER, value BLOB NULL, PRIMARY KEY(namespace_id, key))`;
- an index on `(namespace_id, rev, key)`;
- `okv_namespaces(namespace_id, name UNIQUE, rev)`, holding the counter.

Queries go through `cargo x sql`, with comma-first style.

**Testing plan (the Keyline pattern):**
1. **A naive oracle:** the full commit log as a `Vec` of batches. Current state is a fold, and a
   read is brute force over it. It's small enough to be obviously correct.
2. **Laws, run against the memory and SQLite implementations through one harness:**
   - L1 collapse: each key's entry is its last mutation, at that mutation's rev;
   - L2 page order and bounds: `(rev, key)` order, inside `(after, through]`, `through` within
     bounds;
   - L3 atomic revisions: a page holds all of a revision's matching rows or none;
   - L4 completeness: concatenated pages from `after` to `through` equal the oracle's current
     rows in that window, for the selector;
   - L5 soft limit: a page exceeds `max_entries` only when a single revision does, and then it
     is exactly that revision;
   - L6 tombstones: deletes are returned as `None`, and a put after a delete returns with a new
     rev;
   - L7 revs strictly increase; an empty mutation batch is rejected;
   - L8 rollback: nothing becomes visible, the rev isn't consumed, and no wakeup fires;
   - L9 wakeups: they fire after commit, never before;
   - Tail laws: exactly one `ReplayComplete`; nothing at or below the boundary after it; live
     delivers every later commit; resuming from any `after` loses nothing.
3. **A differential proptest:** random sequences of committed and rolled-back batches over a
   small key space, interleaved with reads at random `after`/`through`/limits/selectors,
   checked against the oracle. Memory runs on the laptop; SQLite runs on mothership.
4. **Concurrent-commit scenarios for `Tail`:** commits during replay, during a page, and an
   overwrite of a not-yet-read key, each checked against the oracle.
5. **Query-plan assertions:** `EXPLAIN QUERY PLAN` for every `read` shape uses the
   `(namespace_id, rev, key)` index, with no full scan and no temp b-tree for `ORDER BY`. Each
   read is exactly one statement.
6. Plus **one example test per verb**, inline where under 300 lines.

### Rev stream + `okv`: final design (approved 2026-11-18)

This supersedes the "Nouns/Verbs" of the `okv` spec above where they differ. The testing plan
above stands.

**`ReplayComplete` today** (production code):
- about 15 sites ignore it (`=> {}`);
- projections forward it;
- the delta walkers and `drawer/queries` use its `through` as progress only;
- only cabinet `doc/observe`, part-store replay pages (bulk to live), `prekey_janitor` and the
  dead `LiveRevisionWatch` use its "caught up to the snapshot" meaning.

It leaves the contract.

**`big_automata/rev.rs` (the read contract every source implements):**
- **`Rev = u64`.**
- **`Entry<K, V> { key, rev, value: Option<V> }`.**
- **`Page<K, V> { entries: Vec<Entry>, through: Rev }`.** It is reused across reads: the caller
  owns it, and a read clears it and fills it. `Page::revisions()` iterates atomic revisions as
  contiguous slices, without allocating. Entries are in `(rev, key)` order, a revision is never
  split, and completeness is latest-state-per-key.
- **`Limits { max_entries: NonZeroUsize, max_bytes: Option<NonZeroUsize> }`.**
- **`RevSource<K, V>`, the trait implementors write. It is inert:**
  - `type Selector`;
  - `latest_rev()`;
  - `read(&selector, after, until, limits, &mut Page)`: stateless, filling the caller's page;
  - `watch() -> RevWatch`.
- **`RevWatch`:** a runtime-agnostic `tokio::sync::watch`-style handle carrying the latest
  committed rev. A source sets it from its commit path only.
- **`RevStream<S>`, the single consumer implementation (the "informer"):**
  - built from `(source, selector, after)`;
  - `async fn next_into(&mut self, &mut Page, limits)`;
  - captures `snapshot_rev` lazily on its first read;
  - reads until `through >= snapshot_rev`, then live;
  - waits on `RevWatch` only when the watched rev is at most `after`;
  - **returns progress-only (empty) pages when `through` advances**, because walkers persist
    progress;
  - exposes `snapshot_rev()` and `caught_up()`;
  - is **not** a `futures::Stream`, so pages are reused rather than allocated per item.
- **Combinators: `RevSourceExt::{map, filter_map}`** build projections that adopt a source's
  payload type without hand-written plumbing. The concurrent walkers keep their own task and
  stack management on top of `RevStream`.

**`big_automata/okv.rs` (+ `okv/`):**
- `Mutation<K, V> { Put, Delete }`;
- **`OkvStore`:** a `RevSource` plus `apply(&mut tx, mutations) -> Rev`, the only write path.
  It wakes `RevWatch` from the transaction's commit, never by hand.
- **`okv/memory.rs`:** the memory implementation, with transactions, built from
  `big_sync/keyed_frontier/memory.rs` (immutable roots, `surelock`).
- **`okv/oracle.rs`:** the naive commit log.
- **`okv/laws.rs`:** a generic harness over a backend-supplied trait, exported behind a
  `test-support` feature so `big_sync_sqlite` can run the same laws.

**Allocation discipline:** reuse caller-provided pages, keep `_buf` scratch fields like
`watermark`, avoid per-read `BTreeMap`/`VecDeque` regrouping, and never clone a page to hold
it.

**Delivery:**
- **Agent 1:** `rev.rs`, `okv.rs` + memory, the oracle, the laws and `RevStream` scenarios,
  in big_automata.
- **Agent 2, after review:** `sqlite_rs` `Tx::on_commit`, the new `big_sync_sqlite` crate with
  the SQLite `okv` (single-statement paging), the query-plan assertions, and the shared laws
  run against SQLite.
- **The 13 `RevisionedStore` implementors migrate when their crates are ported.**

### Rev stream + `okv`: agent 1 delivered (2026-11-18; reviewed by the lane)

**Status:** `big_automata/rev.rs` (+ `rev/tests.rs`) and `okv.rs` with `okv/{memory, oracle,
laws, tests}.rs` are written. Lane-verified: clippy has 0 errors and no warnings in
`rev*`/`okv*`; **93/93 crate tests pass on mothership** (71 existing + 22 new). Every read in
the differential proptest is compared exactly with the oracle, and L5 is asserted
independently of the oracle as well.

**Correction to the `okv` spec (the lane's mistake, caught by the agent).** The paging hint
"`rev <=` the rev of the max-th row" breaks L5. With rows `[r1:a, r2:b, r2:c]` and `max = 2` it
returns 3 rows, though no single revision exceeds 2. The rule implemented:
- take whole revisions while both running totals stay within the bounds;
- the first revision is always taken;
- `through` is `until` when every matching row up to `until` is in the page, otherwise the
  last entry's rev.

The SQLite statement must match this: a cutoff on the rev of row `max + 1` (`rev <` cutoff),
plus taking the first revision alone when it exceeds the bound.

**Lane review notes:**
- `MemoryOkv` commit uses `Arc::make_mut`, which copies the whole root when a read holds the
  old one. That's O(rows) per contended commit: fine for a test backend, a footgun if the
  memory store ever serves production.
- `RevStream` snapshots from `latest_rev` and bounds live reads by the watch. That's correct
  even when the watch briefly lags.

**Agent decisions awaiting the operator:**
1. **Associated types** (`RevSource::{Key, Value, Selector}`) instead of `RevSource<K, V>`.
   This keeps `RevStream<S>` and lets `map`/`filter_map` return `impl RevSource`.
2. **Byte cost** = key bytes + value bytes; a tombstone's value costs 0. This puts an
   `AsRef<[u8]>` bound on the memory store's K and V.
3. **`read` preconditions** (`after < until`, and `until` was committed) are asserted.
4. **A stream opened with `after` at or past the snapshot** is caught up at once and waits
   (untested).
5. **`filter_map` always passes tombstones through.**
6. **Memory transactions are per store.** There's no memory transaction spanning several okv
   tables; the memory part store may need one.
7. **The laws fix K = V = `Vec<u8>`.** `big_sync_sqlite` must offer byte keys and values to
   reuse them.
8. **Tests hand-poll futures with a counting waker** and `futures::executor::block_on`; they
   haven't run under tokio.

### Memory okv redesign (operator, 2026-11-18)

Agent 1's first cut copied `keyed_frontier/memory.rs`: an `Arc` root snapshot, `make_mut`, and
a tokio `OwnedMutexGuard` held for the whole transaction. Snapshots existed only because the
old readers were stateful. A tokio mutex held across a caller's awaits is a futurelock hazard.

The follow-up instead takes the shape of `part_store/memory.rs`:
- **one surelock over the whole state;** tables own no lock;
- **a write is one synchronous critical section.**

The plan:
1. a plain `OkvTable<K, V>` (no lock, no `Arc`), embedded in a `MemoryDb` state behind one
   surelock;
2. an optimistic transaction across tables: it stages locally and commits in one synchronous
   section, with a conflict when the base rev moved. No tokio mutex;
3. stateless reads that copy one bounded page under the lock;
4. `get(&tx, key)` with read-your-writes;
5. stream laws generic over `RevSource`;
6. new laws: atomic visibility across tables, and a conflict leaves no trace and wakes nobody.

**Round 2 delivered (2026-11-18; lane-verified: clippy 0 errors and no warnings in
`rev*`/`okv*`, 101/101 on mothership):**
- `MemoryDb`: one surelock over a registry of type-erased `OkvTable`s;
- optimistic transactions across tables, returning `Conflict { base, current }`;
- validation of tables that were only read (so no lost update on read-modify-write);
- watches move only after the lock is released;
- `OkvStore::get(&mut tx, key)` with read-your-writes;
- stream laws generic over `RevSource` (`rev/laws.rs`, `StreamBackend`), also run over a
  `filter_map` projection;
- new laws: tables commit together; a conflict leaves no trace; reads are validated.

**Open, for the operator:**
1. **A `filter_map` that filters on the *value* breaks latest-state-per-key** for a consumer
   that folds pages. A key kept at rev 1 and dropped at rev 2 leaves a stale row; this is
   pinned by `filter_map_by_value_leaves_a_folding_consumer_a_stale_row`. Options: (a) turn a
   dropped value into a tombstone; (b) document that the filter must depend only on the key;
   (c) both.
2. **The final-view law now compares only rows that have a value,** because tombstones pass
   through `filter_map`.
3. **`get` takes `&mut Tx`,** because it records the base rev on first touch.
4. **No non-transactional point read.** Begin is lock-free, so `get` in a dropped transaction
   serves.
5. **`Conflict` is memory-only** (SQLite never conflicts), so the conflict laws are memory
   tests, not shared ones.
6. **A table registry, not a typed `MemoryDb<S>`.** With the registry, the memory part store's
   non-okv state must also become okv tables to share the lock.
**Operator, 2026-11-18:** (1) → (a). `filter_map` turns a dropped value into a tombstone, and
a separate key-only filter really drops entries. (2) accepted. (3) and (4) are clarified
(okv-level; (3) affects how the memory part store shares the lock) and still open.

7. **The stream laws assume one rev per committed batch, numbered as the okv oracle does.**
   Migrated sources with other numbering need their own model.


**From its contract tests (`host_contract.rs`, 32 `assert_*` scenarios, reviewed
2026-11-18):**
- **Visibility follows *current* access, and both grant orders are pinned**
  (`assert_page_outcome_contract`). The rev laws should do the same: generate "write, then
  filter change" and "filter change, then write".
- **Outcomes stay distinct:** unknown is not a denial, and a denial is not an empty page.
  Whole-route loss is a verdict (`TargetVerdict::Unauthorized`), not an empty page.
- **A hidden third value state:** a payload stripped to `null` means "exists, payload not
  advertised, fetch it". Consumers special-case it (`doc_revision_store` skips it; the cursor
  machine treats it as "fetch"). The part store should model it explicitly (for example
  `Payload | Withheld`), not as a magic `null`.
- **Example-scenario style only:** no generated grant/revoke/write sequences and no oracle.
  Revocation mid-stream is covered only as the `Unauthorized` verdict on later pages. The port
  should turn these into laws plus generated sequences checked against an oracle.

**Lessons from `part_store/memory.rs` to keep:**
- one sync lock over all tables;
- each operation is one critical section;
- embedded lock-free tables.

**Smells not to copy:**
- `AtomicU64` inside the mutex;
- `HashMap`s, whose iteration is nondeterministic;
- membership kept twice (`objs[obj].parts` and `parts[part].members`, synced by hand);
- the `bus` with its `dropped_keys` side channel for `remove_obj_payload`. In okv that is a
  `Delete`.

The part store builds on the keyed frontier, so **the keyed frontier (now `okv`) is ported
first.**

The operator's goals for the part store port:
- improve the data layout and the queries, and remove N+1 patterns;
- find the boundary between part-store semantics and what spilled into `HostPartStore` and
  the RPC layers;
- make it properly transactional (big_repo's single-transaction direction).

**Six pieces:**
- **A.** Objects and membership.
- **B.** Parts and access.
- **C.** Peer cursors.
- **D.** The bucket index.
- **E.** The revision log and replay.
- **F.** Maintenance. `part_store_stats`, `partless_objects` and `remove_part_member` have no
  callers; `remove_obj_payload` is used only in tests.

### Keyed frontier → `okv` (evaluated 2026-11-18)

**The native-wflow kv store is a different shape.** It is an ordered KV with a namespace-wide
revision used for compare-and-swap, plus key-range scans and a `watch`. It has no per-key
revision, no revision-ordered scan and no tombstones. `clay_store` (IndexedDB) is a third
ordered-KV shape. One shared substrate (ordered KV + atomic batch) is noted, not pursued.

**Unused or narrow verbs:**
- **Never called:** `isolation()` (`TransactionIsolation`), and frontier-side
  `NonAdvancingRevision`.
- **Only the memory part store:** `FrontierMutation` and `apply_at`.
- **Only part-store selectors:** the selector hooks `lower_bound`, `initial_after` and
  `emit_empty_progress`.
- **Writers bypass the trait's transaction:**
  - big_blobs projection, cabinet `datum_set` and daybook `facet_set` use the inherent
    `apply_in_context`, then call `notify_changed()` by hand;
  - the part store reaches raw SQL through `context_mut()`.

**Problems:**
1. **Wakeups are a process-local tokio `Notify`.** That breaks the multi-process goal and the
   web, and causes a thundering herd: every commit wakes every reader, costs 2 queries per
   reader, and hands consumers empty pages.
2. **Manual post-commit `notify_changed()`** is correctness by convention.
3. **Paging:** a full page needs a second query, and there are two separate fetch paths (the
   byte-budget path is part-store only).
4. **`decode_row` → `Option`** drops rows silently.
5. **Boundaries grew rather than being designed:** two reader contracts (`KeyedFrontier` and
   `RevisionedStore`), two SQLite implementations, part-store hooks in the generic frontier,
   and GAT `Context`/`Transaction` machinery whose real use is the `context_mut` escape hatch.

**Proposed `okv` direction (open):**
- a stateless `read(selector, after, through, limits) -> Page`;
- key-range or key-set selectors only, with part selectors moving into the part store;
- writes only through `apply(tx, mutations) -> Rev` inside a caller-provided transaction;
- wakeups fired from the transaction's commit hook;
- replay-then-live as a small helper.

**Open questions:**
- Cross-process wakeups: poll `PRAGMA data_version`, or a daemon?
- Should `RevisionedStore` and `okv` converge on one read contract?

### Part store spec, memory first (draft 2026-11-18; awaiting operator review)

**Decided with the operator:**
- **One stamped log is the global cursor.** That matches the old model, where everything is
  stamped by one counter.
- **Three traits, one per audience.**
- **Memory first, SQLite later,** against the same laws.

**What the old SQLite schema really is** (`big_sync/sqlite/migrations/001_init.sql`):
- **`big_sync_members` *is* the stamped log.**
  - It is keyed by `(obj_ref, maybe_part_ref)`, where `maybe_part_ref = 0` is the object lane.
  - Each row holds `event_type` (`Changed`/`Removed`), `txid` (the cursor) and `added_at`.
  - It is collapsed to one row per key, and indexed by `(part, txid)` and `(obj, txid)`.
- **`big_sync_syncable`** (part, principal) → `access_level` and `changed_at`: stamped with the
  same cursor, but stored outside the log.
- **`big_sync_buckets`** (part, `buck_id`) → `level`, `changed_at` (stamped), live/dead counts
  and fingerprints.
- **`big_sync_objs`** obj → `payload_json`, `buck_index`.
- **`big_sync_parts`** part → `latest_cursor`; the global cursor lives in `big_sync_meta`.
- **`big_sync_pending_members`:** membership assigned before the payload exists. It has no
  event and no bucket entry until the payload arrives; then it is promoted to live, gets its
  bucket transition, and emits `Changed` at the new cursor.
- **`big_sync_peer_cursors`.**
- **N+1 in the write paths:** `add_obj_to_parts` loops over parts calling `ensure_part_ref`
  and `load_member_state` once per part (`part_store/sqlite.rs:1061`).

**New model on `MemoryDb`.** All tables share one db and one transaction.
- **`log`, an okv table: the stamped log, whose rev *is* the cursor.**
  - Key: `LogKey { Object(obj), Member { obj, part }, Access { part, principal } }`.
  - Value: `LogEntry { Object(ObjLane), Member(Membership), Access(Level) }`. Tombstones for
    removals.
  - Access is now in the log, so access changes and membership changes share one cursor *by
    construction*, and `part_dirty_count` becomes a count of log rows in a rev window.
- **Plain KV tables** (a new non-revisioned table kind in `MemoryDb`: same lock and
  transaction, no rev, no rev index):
  - `objs`: obj → `{ payload | Withheld, payload_digest, buck_index }`. An explicit `Withheld`
    replaces the magic `null`; the digest is stored at write time for the bucket leaf
    fingerprint.
  - `parts`: part → `{ latest_cursor }`.
  - `members_by_part`: (part, obj) → () — a derived index for `member_count`, part listing and
    bucket ranges. Membership's truth is the log; this index is maintained by the same single
    write path.
  - `pending`: (obj, part) → ().
  - `buckets`: (part, `BuckId`) → `BucketSummaryState`, maintained incrementally with
    `apply_transition`.
  - `peer_cursors`: (peer, part) → cursor.
- **Piece F is dropped** (`part_store_stats`, `partless_objects`, `remove_part_member`,
  `remove_obj_payload` as a side channel).

**Three traits:**
1. **`PartStoreWrite` (embedders)**, inside a caller transaction:
   - set or withhold an object's payload;
   - add an object to parts or remove it;
   - set or revoke a principal's access to a part;
   - ensure a part exists.

   Every method takes batches (`&[…]`), so no N+1.
2. **`PartStoreRead` (client):**
   - batch reads `obj_states(objs)` (payload state, digest and membership) and
     `bucket_summaries(part, ids)`;
   - `member_count(part)`;
   - peer cursors: get, and `set_peer_part_cursors(batch)` (the batched, coalesced cursor
     writes from the tasks review).
3. **`PartStoreServe` (responder):**
   - the log as a `RevSource`, with a selector of part routes (each with a lower bound) and
     object routes;
   - authorization (`permitted_parts`, `read_denied`) against the log's current `Access` rows;
   - `part_dirty_count`;
   - bucket lists and leaves.

   Page assembly, sessions and budgets are out (the responder, later).

**Testing:**
- **A naive oracle for the whole store:** the full mutation history, with current state
  computed by fold.
- **Laws:**
  - replay reconstructs membership and access;
  - collapse and tombstones;
  - pending → live on the payload's arrival, with exactly one event;
  - authorization filtering of the log equals the oracle's filter, pinned in both
    grant/revoke orders;
  - `Withheld` is distinct from deleted;
  - the incremental bucket summary equals a recompute;
  - `part_dirty_count` equals a count over the oracle;
  - cursors only advance;
  - every write method is one transaction, with no partial visibility.
- **A differential proptest** over generated write/grant/revoke/read sequences.
- **The 32 `host_contract` scenarios** are triaged: keep the ones that are still meaningful
  as named laws or scenarios; drop the rest.
- **SQLite later:** the same laws, plus query-plan assertions.

**Operator decisions (2026-11-18), superseding the draft above where they differ:**
- **No part-store tables in `MemoryDb`, and no plain-KV table kind.** `MemoryPartStore` is its
  own store:
  - one surelock over its own relational state struct (`BTreeMap` tables in the relational
    spirit, so the SQLite port matches);
  - the log embedded in that state using okv's table type (`OkvTable`, made `pub(crate)` with
    its read and write helpers), so the log is still a `RevSource` that runs the rev laws.
- **No physical object lane.** The log keys are `Member { obj, part }` (object-first) and
  `Access { part, principal }`.
  - A payload change re-stamps the object's member rows at the new rev, so object routes are a
    key range and part replay reads that part's rows.
  - The cost is write amplification proportional to the number of parts the object is in.
- **Pending membership stays.**
- **The trait names stand:** `PartStoreWrite` / `PartStoreRead` / `PartStoreServe`.

**Previously open:**
1. **The `members_by_part` index vs ordering the log key part-first.** Part-first (`Member {
   part, obj }`) makes per-part replay a key range, but `obj_parts` then needs an index. I lean
   obj-first in the log, plus the part index.
2. **Does the object lane (`Object(obj)`) stay in the log?** It's what object routes replay,
   and payload changes stamp it.
3. **Should `pending` stay a feature?** The wiki calls membership-before-payload "more
   important"; I assume yes.
4. **The trait names:** `PartStoreWrite` / `PartStoreRead` / `PartStoreServe`.

### Memory part store delivered (2026-11-18; lane-verified)

**Status:**
- **Code:** `big_automata/part_store.rs` (the three traits and the types), plus
  `part_store/{memory, memory/{objects, access, buckets}, oracle, laws, tests}.rs`.
- **`OkvTable`** is `pub(crate)` with embedding helpers; the stub `store.rs` traits are gone.
- **`bucket.rs`** uses the batched reads, with no N+1.
- **Checks:** clippy 0 errors; the lane fixed 3 clippy needless-borrow warnings in
  `bucket.rs`/`ids.rs` left over from the verbatim port. **122/122 on mothership.**
- **Mutation check:** a planted bucket bug made the differential fail, so it isn't passing
  vacuously.
- **Mothership:** the scratch worktree's index is now reset to `refs/heads/mirror/automata`
  (pushed from the laptop). The agent's `git add -N` workaround is undone.

**Scenario triage of the old contracts** (full table in the agent's report, 2026-11-18):
- **Kept as laws:** most scenarios.
- **Moved to the future responder:** page limits, byte budgets, sessions, per-event filtering
  and verdicts on pages.
- **Dropped:** the aggregator, `summarize_parts`, and the `remove_obj_payload` half.
- **Open:** `tombstone_added_at` (Q7) and `object_lane_carries_no_membership` (Q6).

**Questions before the SQLite port (lane's recommendation in brackets):**
1. The traits use `async_trait` (`Send`), like `RevSource`, so `bucket.rs` dropped
   `FutureForm` [accept].
2. One rev per transaction, not per write method; `commit` returns `Option<Rev>` [accept].
3. Writes that change nothing stamp nothing [accept].
4. `PayloadDigest` is a fixed-seed SipHash u64 over `Hash` [**change to blake3 over
   canonical bytes**: the digest crosses peers through bucket fingerprints, and `Hash`-based
   SipHash isn't a stable cross-version or cross-platform encoding].
5. The bucket live fingerprint now hashes the digest (`-v2`), but the leaf fingerprint still
   hashes the payload because of the wire type, so a locally `Withheld` object counts as
   changed [**move the leaf fingerprint to `SipHash(seed, digest)`** as decided in the
   bucket review; a wire-type change in `rpc/bucket.rs`].
6. With no object lane, object routes also replay membership tombstones; the old contract
   said they carry no membership [operator call].
7. ADR 012's `added_at` tombstone rule (skip a removal for an object added and removed after
   the page cursor) can't be evaluated from an okv tombstone [**membership value
   `Live { added_at } | Removed { added_at }`** instead of a tombstone].
8. Part routes exclude `Access` rows; only `All` replays them [keep].
9. `part_dirty_count` counts revocations; the old store didn't [operator call; arguably more
   correct, since losing access is relevant to that principal].

**Lower priority:**
- Q12: `obj_states` clones payloads; split out a digest-only read.
- Q13: pending memberships are invisible to reads.
- Q14: `parts.latest_cursor` is maintained but unread.
- Q17: the `LogStreams` adapter uses filler writes to keep one rev per batch.
- Q20: the disabled tasks still name the removed traits.
- Q21: `add_members` and `grant_access` create parts implicitly.

### FutureForm, wasm, blake3 digest, `added_at` (2026-11-18; lane-verified)

**Status:** 126/126 on mothership. Native clippy 0 errors; the wasm32-unknown-unknown clippy
is clean apart from dead code.

**Operator decisions applied:**
- FutureForm now (`#[future_form(Sendable, Local)]`, as in subduction).
- No trap.rs.
- blake3 `PayloadDigest` over canonical JSON (keys sorted; streamed into the hasher).
- Leaf fingerprints over the digest (`big-sync-obj-fp-v2`).
- Access rows stay in the log, after reading the RPC: verdicts are recomputed every round from
  current state, and `RevWatch` already wakes on every commit.

**`added_at`:**
- The rule lives in `LogSelector::selects`: skip a removal when the membership's
  `first_added_at > max(after, route bound)`.
- It uses the *first* add, not the latest: under a collapsed log the latest-add rule drops a
  needed removal (live 5, removed 6, re-added 7, removed 8: a reader at 5 misses the removal).
- Cost: a reader whose cursor sits in a gap between incarnations gets a harmless extra
  removal.
- Possible refinement: also keep `prev_removed_at` and skip when
  `prev_removed_at <= cursor < latest_added_at`.

**Open:**
- `check-matrix` fails only on `-D warnings` over the disabled machine's dead code (97
  warnings).
- Six legacy findings are baselined in the new `guardrails.toml`.
- The canonical-JSON form is our own, not RFC 8785; `1` and `1.0` digest differently.
- `lib.rs` `Clock` is still `std::time::Instant`.
- The combinators are named types (`RevMap`/`RevFilterMap`/`RevFilterKeys`); their scratch
  page is `Box<dyn Any + Send>`, so projected keys and values must be `Send` even under
  `Local`.

## Cross-cutting items

- **Remove tokio mutex usages repo-wide (operator, 2026-11-18).** They are not cancel safe: a
  guard held by a future parked in `select!` or `FuturesUnordered` futurelocks every other
  waiter in the task. Fold this into the worker audit. Prefer one sync (surelock) critical
  section per operation, optimistic commits, or an actor where a task genuinely owns the state.

- **Compose the building blocks into reactive services (audit; operator, 2026-11-18).**
  `big_cabinet/datum_store.rs` and the workers named in the worker audit should be built from
  big_automata's blocks: the rev stream (okv), `Scheduler`/`KeyedScheduler`, watermark, the
  delta walkers. The goal is services that are reactive, poll-based (pull with wakeups),
  efficient and backpressured. Open: whether request/response layers should adopt tower's
  `Service`/`poll_ready` instead of growing our own (see the rev-stream design discussion).

- **Audit every worker and state machine in the repo** (later, after the port). The
  scheduling story has drifted between workers. Examples found while porting the scheduler:
  - big_blobs consumes `ConcurrentDeltaWalker`s serially (head-of-line blocking, no
    per-key coalescing);
  - projection uses durable parking instead of backoff;
  - the frontier worker abandoned `park`/`wake` after it stalled the walker's contiguous
    prefix, while other users may still park;
  - the scheduler is tokio-bound (`TokioKeyedScheduler`) in some places and sans-IO in
    others.

  For each worker, check: what it schedules on, how it retries, whether a deferral can stall
  a contiguous prefix, coalescing under bursts, determinism, and whether it can run on the
  injected clock. Converge on the big_automata scheduler (`Scheduler` + keyed layer).

- **Drawer-level replay dedup.** Optimize at the drawer level: don't run (or drop) a
  big_sync replay for a part when another ongoing replay with the same peer already covers
  it, for example a child drawer covered through its parent's sponsorship or mirror walk.
  This belongs to the cabinet/drawer layer, which knows the containment; big_sync only sees
  parts.

- **Event recording.** Every machine should be able to record the events it consumes and the
  transitions and commands it produces, behind one recorder hook shared by all machines (in
  `test_utils_rs` or a feature). Purpose: during realistic e2e runs, capture what each machine
  did, so we understand what goes over the wire (message counts and sizes per round, which
  transitions drove them). The sans-IO shape makes this cheap: record at the `handle_evt` /
  command-drain boundary. It is the same capture DST replay and quint trace conformance need.
