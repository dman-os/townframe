# Single-writer consumer watermarks, and the fence audit around them

## The bug

Per-consumer "reconciled through N" was stored three times, written by two transactions:

| consumer (delta-walker identity) | walker state | `cursors` row | admission-reader row |
| --- | --- | --- | --- |
| `big_repo.group_part` / `admission` | `delta_walker_progress` | `group_part:{scope}` | `group_part` |
| `big_repo.causal_checkpoint` / `admission` | same | `causal_checkpoint:{scope}` | `causal_checkpoint` |
| `big_repo.automerge_frontier` / `keyhive-admission` | same | `automerge_keyhive:{scope}` | `automerge_frontier` |
| `big_repo.prekey_janitor` / `admission` | same | — | `prekey_janitor` |

`acknowledge_source` commits the walker state (via `admission.ack`), then pushes the cursor
command into an **in-memory outbox** drained later:

```
ack(key, cursor)                  // walker tx commits: upstream_revision = N
outbox.push(AdvanceCursor(N))     // in memory only
drain_outbox()                    // second tx: cursors.seq = N, readers.seq = N
```

The workers are `Abortable`, so shutdown is a normal path. An abort between the two commits
leaves the walker's durable revision ahead of the watermark rows forever: the walker resumes at
its own revision, never re-reads, and every consumer of the watermark then waits for a number
that will never arrive.

Observed signature (big_repo restart test, 111s of identical reports, no batch reads after
restart):

```
network-rest fence stalled: group-part cursor behind admission head
  node=z9LbZUYMRAgo… admission_head=18 group_part_cursor=16
```

## The change

One durable writer per consumer: the delta walker's `upstream_revision` is the only
"reconciled through" value.

- `cursors` rows for these consumers are deleted; the table goes.
- `big_repo_keyhive_admission_readers` becomes a value-free registration: it names which
  consumers gate admission-log retention, and the value is read from `delta_walker_progress`.
- Prune floor = `MIN(COALESCE(p.upstream_revision, 0))` over registered readers LEFT JOINed to
  `delta_walker_progress` on `(p.namespace || '/' || p.consumer_id) = r.reader`. A registered
  consumer with no progress row pins the floor at 0 (conservative).
- Fences/observability read the walker progress row.

A store already in the gap state heals on reopen with no migration: the fence reads progress
(18), not the stale row (16).

Rule this establishes: **a consumer of the big_repo admission log keeps its walker progress row
in that log's database**, because the floor is derived from those rows. Daybook's blob-inventory
permission machine violated this — its walker state lived in
`ensure_sqlite_ctx(BLOB_INVENTORY_PERMISSION_STATE_ID)` (a per-id file) while it registered its
retention reader on the big_repo store's DB, copying a cursor across two files. Colocated.

Migration `004_single_consumer_watermark.sql` also drops `big_repo_sync_commits_watermark`,
which has no reader or writer anywhere in `src/`.

## Audit findings (other sites)

### Fixed in this work

1. The duplicate watermarks and the outbox-deferred cursor writes, in three workers.
2. Daybook blob-inventory permissions: walker state in a different file than the log it gates.
3. `hub.rs:643` quiescence clause was vacuous — `self.group_part_settled_seq <
   probe.group_part_settled_seq`, where the probe snapshotted that field from itself. The probe
   now captures the settle target from `admitted_head`, the same value
   `WaitForKeyhiveReconciliation` captures (`hub.rs:984`) and resolves on `GroupPartWorkerSettled`.
4. Dead watermark schema `big_repo_sync_commits_watermark`.

### Prevention (follow-up, same family)

Registration happens inside each worker's spawn, while pruning takes MIN over **registered**
readers only — an unregistered consumer is invisible. The maintenance loop
(`native.rs:2432`) sleeps one interval and then prunes, so the registry is complete only if
startup beats a wall-clock timer. Fix: register the four known consumers where the runtime opens
the store/scope. Then the prekey janitor's "cursor below `archived_through`" case is unreachable
by construction, and no clamp belongs in the janitor.

Do **not** add a heal there: `housekeep_after_add`'s replay guard only works while the `Add` is
still in the log, and the consumed-prekey record exists nowhere else, so a pruned window is
genuinely unrecoverable (rotating all published prekeys would break invites in flight).
Prevention is the only correct fix.

### Verified correct — do not churn

- The delta-walker protocol itself: `ack` returns `through` only when a contiguous prefix is
  settled, and callers ack only after their effect is durable, so walker progress never leads
  effects. The duplicate rows were the only thing that could advance independently.
- `big_sync`'s machine runs durable writes as commands and advances its in-memory cursor only on
  `handle_cmd_success` (`big_sync/worker.rs:669-681`) — write before advance.
- Part-store advertise path: every `big_sync_parts.latest_cursor` bump is inside the caller's
  transaction alongside the membership/content row (`TransactionIsolation::Serializable`). A part
  is advertised at a cursor only in the commit that made its content visible.
- Daybook walkers: effect → ack, "The command's effect is durable; only now may the walker cursor
  advance past it", panic on task failure, exactly one durable watermark, no `cursors` analogue.
  `begin_settlement` + `apply_projection_in_context` + `settle()` puts the projection write and
  the walker advance in one transaction.
- Keyhive dispatcher boots at the head with an in-memory cursor by design (subscribers pull their
  own initial state).
- Doc-sync receipt `Ready` on the live-doc path: `test2/access_matrix.rs:470-485` pins it as
  "this round's received content is applied to the live bundle", not "no blockers remain". Not a
  defect, though both meanings share one enum.

## Verification recipe

`.sqlx` is a CI gate (`SQLX_OFFLINE: true`; `x/sqlx-prepare.ts` → `cargo sqlx prepare --workspace
--check`). Only `big_sync` and `big_repo` own `query!` macros (147 cache entries). Regenerate
after any SQL change, per `.pre-commit-config.yaml`:

```sh
set -eu
database_dir=$(mktemp -d)
trap 'rm -rf "$database_dir"' EXIT
export DATABASE_URL="sqlite://$database_dir/sqlx-prepare.db"
cargo sqlx database create
cargo sqlx migrate run --source src/big_sync/migrations
cargo sqlx migrate run --source src/big_repo/migrations
cargo sqlx prepare --workspace -- --all-targets --all-features -p big_sync -p big_repo
```

Never edit migrations 001–003 (checksummed by `sqlx::migrate!`).

## Verification (this session)

| check | result |
| --- | --- |
| `cargo sqlx prepare` + `--check` (CI gate, `SQLX_OFFLINE`) | pass, 139 cache entries |
| `clippy --all-targets --all-features -p big_repo -p daybook_core -p big_sync -- --deny warnings` | clean |
| `fmt --check` (same crates) | clean |
| big_repo: store::sqlite + quiescence + keyhive_access_stream | 84/84 |
| big_repo: restart + offline + offline-transfer | 24/24 (incl. `tier5_restart_after_local_write_delivers_on_reconnect`, previously wedged 111s) |
| big_repo: full serial debug run (`NEXTEST_TEST_THREADS=1`) | 403/403, 520s |
| daybook_core: blob-pin + permission + sync | 45/45 |
| quiescence fence negative control | old capture semantics -> FAIL; fix -> PASS |

Blocking issues found by building, not by reading:

- `no such table: delta_walker_progress`: the store's own queries (`admission_consumer_progress`,
  the pruning floor) referenced a table only the walker created lazily, so `sqlx prepare --check`
  failed outright. Fixed by creating it in migration 004 with the walker's DDL verbatim; the store
  can now answer its own queries without assuming a walker exists. This also removed the runtime
  ordering hazard (prune before any walker repo exists) that the implementer had reported.
- `keyhive_access_stream` test module was missing the `DeltaWalkerStateTransaction` trait import.
- AFW `Worker.store` was dead after the cursor call went; `admission_consumer_progress` needed
  `#[cfg(test)]` (its only readers are the cfg(test) harness fences); daybook `permission_writer`
  teardown returned a double-boxed future after the local-state removal.

## Test-quality finding: `a_cancelled_read_re_serves_its_page`

A 10m stress run (`-p big_sync -p big_sync_core -p big_repo`, `NEXTEST_TEST_THREADS=1`,
`RUST_LOG_TEST=debug`) died after 2.1s on this test: `no drop landed on a parked page; the
cancellation window is untested`. It passes in isolation, and it passed 6x in the earlier green 30m
soak -- the failure is scheduling-dependent, not caused by the watermark change.

Instrumenting the miss path produced the measurement that explains it: the window is a *single*
poll count sitting just below the count at which the read delivers --
`polls=1,2,4,8,16 -> staged=0, not delivered`, `polls=32..4096 -> delivered`. The sweep was
linear `1..=32`, so it passed by landing in 17..31 and missed entirely once load pushed the
delivery threshold past 32. A geometric sweep misses it too (it steps 16 -> 32).

Fixed with a linear sweep to 512 (headroom for a threshold that moves with load), the failure
payload now reporting every attempt as `(polls, delivered, staged, ready)`, and a bounded
debug probe on the success path. Residual debt: this is still a schedule-sweep rather than a
deterministic park. A hold gate on the admission read (same shape as the hub's
`hold_events_for_test`) would make the window reachable by construction and remove the load
sensitivity entirely; the sweep is what keeps it honest until then.

## What the running soak is looking for

- `network-rest fence stalled` -- the split-watermark signature (22x in the wedge log). Expect 0.
- `group-part cursor lagged ... repairing` -- the deleted band-aid. Expect 0; non-zero means the
  delete did not take.
- `quiescence fence stalled` / `quiescence wait timed out` -- the *new* failure mode. The probe now
  requires `group_part_settled_seq >= admitted_head` captured at probe start, so a settle gap that
  was previously skipped now surfaces. That is learning, not noise: it names an admission the hub
  incorporated that the projection never settled.
- `TIMEOUT` / `FAIL` -- expect 0.

## Final state (post-verification)

- Soak: `-p big_sync -p big_sync_core -p big_repo`, `NEXTEST_TEST_THREADS=1`,
  `RUST_LOG_TEST=debug`, `--stress-duration 30m`, `--no-fail-fast` -> **3 full iterations, 1857
  tests, all passed**, 1 pre-existing skip. Every target signature zero (see above). The wedge did
  not reproduce across three serialized-debug iterations, and the strengthened quiescence fence
  reported nothing.
- `-p daybook_core` full suite: **174/174 passed**, 0 skipped, 0 timeouts (185s), including the
  four-node stress and the remote-restart/reconnect ladder.
- Gates: `fmt --check` clean; `clippy --all-targets --all-features` on all four crates clean under
  `--deny warnings`; `cargo sqlx prepare --check` (what CI runs under `SQLX_OFFLINE`) passes.

### Registration at runtime init (landed)

`SqliteBigRepoStore::register_admission_consumers()` registers the four consumer identities, and
`native.rs` awaits it before any worker or the maintenance loop is spawned, so a consumer can no
longer be invisible to the retention floor while pruning runs. The four workers no longer register
themselves (each has exactly one spawn site, in `native.rs`); they still read their own
`state.progress()` for the resume point. Registering from `SqliteBigRepoStore::new*` was rejected: a
store opened without a runtime would pin the floor at 0 forever.

### Residual items (not fixed, deliberately)

- `a_cancelled_read_re_serves_its_page` now reaches the parked state by construction, so it pins
  the *re-serve* half deterministically. The *fill* half (`next` staging the page before it awaits) is
  not deterministically observable on this harness -- measured: the resolution step completes inside a
  single poll, so no poll count lands on the window. A test-only gate between staging and delivering
  would need a poll-until-parked loop (the race back again), so it was not added. Documented in the
  test's doc comment.
- The hub's quiescence stall report still logs `admitted_head` and `group_part_settled_seq` but not
  the probe's captured target, so isolating a stall needs the probe-start `required_settled_seq`
  debug line. Worth adding to the report if that fence ever fires.
- Daybook's blob-inventory permission consumer registers itself (layering) and relies on its own
  archive-floor repair for the case where pruning outran it.
- `docs/adrs/002-automerge-heads-worker.md` still shows the dropped
  `big_repo_sync_commits_watermark` table (historical document, left alone).
