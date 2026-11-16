# Restate: retention and pruning of completed durable state

Research note for designing an equivalent policy in our own engine. Every non-obvious claim cites a
docs URL or a pinned source line; guesses are marked UNVERIFIED. **Provenance:**
`restatedev/restate@12adfb14c497b1577b1bba117f769e5eba941ee6` (`version = "1.8.2-dev"`, cloned
2026-11-16); source links read
`github.com/restatedev/restate/blob/12adfb14c497b1577b1bba117f769e5eba941ee6/<path>#L<n>`, shown as
`SRC:<path>#L<n>`. Docs are the live `docs.restate.dev` pages.

## The horizons

| Horizon | Governs | Default | Config scope | Clamp |
| --- | --- | --- | --- | --- |
| `idempotency_retention` | lifetime of the *completed invocation status* incl. the stored result, for calls carrying an idempotency key (and, with `controlled-idempotent-sharding`, for all Service/VO ingress calls, which get a random key injected) | 24h | per-handler → per-service → server | `max-idempotency-retention` |
| `workflow_completion_retention` | same status lifetime for a workflow **`run`**; on expiry also clears the workflow's K/V state and promises | 24h | per-workflow → server | `max-workflow-completion-retention` |
| `journal_retention` | lifetime of the invocation *journal* after completion (debug/audit/replay only) | 24h, may be `0` | per-handler → per-service → server | `max-journal-retention` |
| `worker.cleanup-interval` | how often a partition leader sweeps for expired invocations | `1h` | server | — |
| `worker.cleanup-max-in-flight-purges` | purge commands proposed but not yet applied, per partition | `32` | server | — |
| `worker.snapshots.snapshot-interval` / `-num-records` | partition-store snapshot cadence (bounds *log* trimming, not invocations) | `1h` and `100000` once `destination` is set | server | — |
| `worker.snapshots.num-retained` / `worker.trim-delay-interval` | snapshots kept; delay before log trim | `1` / `10m` | server | — |

[services/configuration](https://docs.restate.dev/services/configuration#retention-of-completed-invocations),
[references/server-config](https://docs.restate.dev/references/server-config),
SRC:`crates/types/src/config/invocation.rs#L33` (three `default_*` = 1d, three `max_*` = unset),
SRC:`crates/types/src/config/worker.rs#L69` and `#L83`.

**Defaults are server-wide; overrides are per-service and per-handler** (handler > service > server),
resolved once at ingestion: SRC:`crates/types/src/schema/metadata/mod.rs#L1005`, clamping at
`#L1438`. SDK surface: `#[service(...)]`/`#[handler(...)]` args `idempotency_retention`,
`journal_retention`, `workflow_completion_retention` (workflow attr only), or
`ServiceOptions`/`HandlerOptions` at bind time
([docs.rs restate_sdk::configuration](https://docs.rs/restate-sdk/latest/restate_sdk/configuration/index.html)).

## 1. The three clocks, and what `max-*` does

- **Completion retention** = `workflow_completion_retention` for a workflow `run`, else
  `idempotency_retention` ([configuration](https://docs.restate.dev/services/configuration#retention-of-completed-invocations)).
- **Journal retention is always capped**: `journal_retention = min(journal_retention, completion_retention)`,
  because "invocation status must be retained at least as long as the journal"
  (SRC:`crates/types/src/schema/invocation_target.rs#L107`).
- **Server defaults** fill in whatever neither service nor handler sets.

`compute_retention(has_idempotency_key)` (same file, `#L107`) is the exact truth table and is sharper
than the docs:

| request kind | completion retention | journal retention |
| --- | --- | --- |
| workflow `run` (with or without key) | resolved workflow retention | `min(journal, completion)` |
| any call with an idempotency key | resolved idempotency retention | `min(journal, completion)` |
| plain call, no key, `journal_retention > 0` | **= journal_retention** | `journal_retention` |
| plain call, no key, `journal_retention == 0` | `0` (nothing kept) | `0` |

So for a plain call, raising journal retention also raises how long the *status* lives ("to retain the
journal, we must retain the completion too. No way out of this." — same lines).

`max-*` knobs are **ceilings applied at ingestion**, not defaults: "values higher than this limit are
clamped down to it"; unset = no limit, `0` = force-disable
(SRC:`crates/types/src/config/invocation.rs#L33`, SRC:`crates/types/src/schema/metadata/mod.rs#L1438`).

## 2. Retained vs dropped; what the caller sees

A completed invocation owns an `InvocationStatus::Completed` row (target, timestamps,
`idempotency_key`, pinned deployment, `journal_metadata.length`, and `response_result` — the marshalled
success value or terminal error) plus journal rows.

**At journal expiry** (`PurgeJournal`): journal entries, journal events and (protocol <V4 only)
pending sleep timers are deleted; `journal_metadata.length/.commands` are zeroed so it is not purged
twice (SRC:`.../state_machine/lifecycle/purge_journal.rs#L90`). Status and result survive — "If the
journal retention is set to 0, you can see completion status but not journal entries".

**At completion expiry** (`PurgeInvocation`, SRC:`.../lifecycle/purge.rs#L124`):

- the status row is deleted (`do_free_invocation` → `delete_invocation_status`,
  SRC:`.../state_machine/mod.rs#L4775`) — and the status row *is* the idempotency record, so the key
  is freed;
- journal dropped if still present; vqueue entry deleted (asserts stage `Finished`);
- **workflow targets only**: all K/V state for the keyed service, and **all** promises of that
  service id, are cleared (purge.rs#L139-L144).

Never dropped by retention: **VirtualObject/service K/V state** (purge clears state only when the
target is `Workflow(Workflow)`, purge.rs#L138), **promises/signals of non-workflow services**, and any
**non-terminal** invocation (`Scheduled`/`Inboxed`/`Invoked`/`Suspended`/`Paused`) — no idle TTL.

Caller-visible outcomes:

- `output`/`attach` after expiry → status `Free` → `NotFound`
  (SRC:`.../partition/rpc/get_invocation_output.rs#L40`), rendered as `{"message":"not found"}`
  ([http](https://docs.restate.dev/services/invocation/http#attach-to-an-invocation)).
- A **new request with the same idempotency key** → `Free` → **re-executed** as a brand-new
  invocation under the *same* invocation id (SRC:`.../state_machine/mod.rs#L1098`). Same for a
  workflow `run`: "executes exactly once" is bounded by the retention window.
- A duplicate arriving *before* expiry gets `completed.response_result` replayed (mod.rs#L1161) — the
  stored result, not a journal read. (The [architecture page](https://docs.restate.dev/references/architecture)
  says "from the journal"; the code path is the status row.)
- No payload fingerprint is stored or compared in `handle_duplicated_requests` (mod.rs#L1076): a
  different body under a live key silently receives the original result.

## 3. When the countdown starts; failures, kills, cancels

Countdown starts at the **completed transition**, not at submission:
`completion_expiry_time = completed_transition_time + completion_retention`,
`journal_expiry_time = completed_transition_time + journal_retention`
(SRC:`crates/storage-api/src/invocation_status_table/mod.rs#L903`); the timestamp is stamped when the
terminal log record is applied (`record_completed_transition_time`, `#L106`).

- Queueing, scheduling and retry time do not consume retention.
- Failures, **kills and cancels** share the path: kill/cancel produce a terminal
  `ResponseResult::Failure(KILLED_INVOCATION_ERROR | ABORTED)` through `end_invocation` (mod.rs#L2151),
  so a cancelled/killed invocation is queryable and purgeable for `completion_retention` and then
  vanishes the same way. No shorter horizon for failures.
- `completion_retention == 0` short-circuits: no status written, journal dropped in the same apply
  (mod.rs#L2825, `#L2955`).
- Restart-as-new inherits the original's `completion_retention_duration`
  (SRC:`.../lifecycle/restart_as_new.rs#L315`).

## 4. How pruning is triggered and executed

Periodic scan → ordinary replicated commands, nothing more:

1. **Sweep.** `Cleaner` starts when a processor becomes leader
   (SRC:`crates/worker/src/partition/leadership/mod.rs#L790`) and ticks every `cleanup-interval`; the
   first tick waits 20% of the interval plus jitter "because the cleaner is currently quite an
   expensive scan" (SRC:`.../partition/cleaner.rs#L152`).
2. **Scan.** It walks the *invocation status* table in invocation-id order, decoding only the status
   discriminator (`filter_map_invocation_status_lazy`,
   SRC:`crates/partition-store/src/invocation_status_table/mod.rs#L244`), emitting an effect when
   `status == Completed` and `now >= completed_transition_time + retention`. **No expiry index, no
   watermark**: a full ordered scan paginated by invocation id, `CLEANER_EFFECT_BUFFER_SIZE = 4096`
   plus one element to detect "has more" (cleaner.rs#L37, `#L243`).
3. **One command per expired item.** `CleanerEffect::{PurgeInvocation,PurgeJournal}` `self_propose`s
   `commands::PurgeInvocationCommand` / `PurgeJournalCommand`
   (SRC:`.../leadership/leader_state.rs#L935`) — real WAL record kinds
   (`CommandKind::PurgeInvocation|PurgeJournal`, SRC:`crates/wal-protocol/src/v2/commands.rs#L440`),
   keyed by partition key, applied in log order by the same state machine. The Admin API endpoints
   (`PATCH /invocations/{id}/purge`, and purge-journal-only) use the same commands.
4. **Back-pressure.** At most `cleanup_max_in_flight_purges` (32) proposed-not-applied purges per
   partition; the stream stays pending until the leader applies one (`Action::CleanerPurgeApplied`,
   cleaner.rs#L60, leader_state.rs#L1120). Rationale: "Purges are cheap to propose but expensive to
   apply… keeps other commands from queueing behind a long backlog of purges in the log"
   (SRC:`crates/types/src/config/worker.rs#L83`).
5. **Idempotent apply**: replies `Ok` / `NotFound` (already `Free`) / `NotCompleted`, touching only
   `Completed` rows (purge.rs#L124).
6. Lag is metered: `restate.partition.cleaner_purge_delay.seconds` = expiry → purge applied
   (SRC:`crates/worker/src/metric_definitions.rs#L48`).

So: **no `settled_through`, no range truncation, no "purge everything older than T"** — one log record
per expired invocation per horizon, batched only by the appender (≤5000 envelopes per Bifrost append,
SRC:`.../leadership/self_proposer.rs#L33`).

## 5. Bounded dedup without the journal; tombstones

There is **no idempotency-key table and no tombstone.** The dedup index *is* the invocation status row,
addressed by a deterministic id:

- `InvocationId` = SHA-256 over (scope?, service, key?, handler, idempotency_key) for idempotent
  calls, or (scope?, service, workflow key) for a workflow `run`
  (SRC:`crates/types/src/identifiers.rs#L398`); non-idempotent non-workflow calls get a random ULID.
- Ingest computes that id and reads the status row: `Free` → new invocation, anything else →
  duplicate (mod.rs#L1096).
- Purge deletes the row, hence the dedup entry. Nothing else remains, so the structure is bounded by
  completion *rate × retention*, not by total history.
- A request with an already-pruned key arrives as `Free` → **re-executes** as a fresh invocation with
  the same invocation id and an empty journal. Any VirtualObject K/V the first run wrote is still
  there; workflow state/promises were cleared. The key is never stored separately, so the dedup window
  is exactly the status retention (`idempotency_retention` or workflow retention), never journal
  retention.

## 6. Cost profile, size limits

- **One O(#invocations in partition) full scan per partition per `cleanup-interval`**, described
  in-tree as "quite an expensive scan" (cleaner.rs#L148). Lazy decode keeps it to key iteration plus
  discriminator, but it does not shrink with history: every sweep touches every live status row.
- Per purge: O(1) rows + a journal range delete. The one O(journal) case is protocol <V4, where
  pending sleep timers require reading the whole journal — "a **heavy** process that requires reading
  the entire journal" (SRC:`.../state_machine/mod.rs#L5067`). V4+ skips it.
- **The result payload is stored twice** while both horizons are live: the journal `Output` entry and
  `CompletedInvocation::response_result` (`read_last_output_entry_result`, mod.rs#L2860). Each
  horizon reclaims one copy.
- Snapshotting is O(partition state): the whole RocksDB store is exported at most every 1h **and**
  100k records, `num-retained = 1` ([snapshots](https://docs.restate.dev/server/snapshots.md)). So
  retention drives snapshot size, restore time and the log trim point.
- Documented operational warnings, not hard caps: `rocksdb-total-memory-size` 2 GiB (min 256 MiB),
  `worker.data-service-memory-limit`, invoker `message-size-warning: 10 MiB`
  ([server-config](https://docs.restate.dev/references/server-config)); production advice is to
  disable default journal retention and opt in per service. No hard per-invocation journal cap or
  invocation count limit found: UNVERIFIED whether one exists.

## 7. Inactivity / abort timeout vs retention

Both are **attempt-scoped** and resolved per attempt handler → service → server
(`resolve_invocation_attempt_options`, SRC:`crates/types/src/schema/metadata/mod.rs#L1098`), unlike
retention which is baked into the invocation at submission.

- `inactivity_timeout` (1m): no new journal entry for that long → Restate asks the service to suspend.
  `abort_timeout` (10m): after that, the attempt is forcibly aborted
  ([configuration#timeouts](https://docs.restate.dev/services/configuration#timeouts)).
- Abort is **not terminal**: the attempt fails (`InvokerError::AbortTimeoutFired`,
  SRC:`crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs#L741`) and the
  invocation is retried with backoff. It stays non-terminal, so **no clock starts and it is never
  pruned**. The only interaction: retry policy `on_max_attempts = "kill"` converts an endlessly
  timing-out invocation into a terminal `Completed` one, which then enters the normal window;
  `"pause"` (the default) leaves it non-terminal forever.

## What is directly transferable to a SQLite-journal, snapshot-based authority

1. **Two deadlines, resolved at submit and persisted on the row** (`completed_at`,
   `journal_expires_at`, `status_expires_at`, journal ≤ status). Restate bakes the resolved durations
   into the invocation at ingestion so a later config edit cannot move an existing deadline — that is
   what makes pruning deterministic under live reconfiguration
   (SRC:`invocation_target.rs#L107`, `schema/metadata/mod.rs#L1005`).
2. **Deterministic ids instead of a dedup table.** Hash (scope, service, key, handler, idem-key) and
   let the completed row *be* the dedup record: no tombstone, window == row lifetime, bounded by
   rate × retention.
3. **Store the result in the completed row, not only the journal**, so journal pruning never affects
   `attach`/`output` and `journal_retention = 0` degrades to "status only".
4. **Prune as ordinary commands with a small in-flight window** (propose ≤N, wait for apply): off the
   critical path, replayable, idempotent (`Ok`/`NotFound`/`NotCompleted`). Meter expiry→purge lag; it
   is the only practical "pruning keeps up" signal.
5. **Keep log/snapshot reclamation separate** from invocation retention: trim to a durability point
   established by snapshots, plus a replication-lag delay.

## What is NOT transferable / Restate-specific

- **The full-table sweep.** Restate gets away with scanning every status row hourly because it is
  per-partition, lazy-decoding and file-ordered. In SQLite, use an index on `expires_at` with a
  bounded `DELETE … WHERE expires_at <= ?` loop, or a cursor. Don't copy the scan — copy "each item
  has its own deadline". A simple watermark is unsafe precisely because deadlines are *not*
  monotonic in submission order (retention is per-service, clamped, and journal/status differ).
- **One log record per purge** is affordable only because Bifrost batches appends and the log is
  trimmed behind snapshots. A SQLite authority should batch purges in one transaction with a row cap.
- **Workflow-only state/promise clearing.** Restate clears K/V + promises only for workflow targets
  and only at completion expiry; VO state and service promises leak by design. Its docs claim that
  after expiry "you cannot call any shared handlers anymore", but no code path was found that
  *rejects* such a call — enforcement appears to be "the state is gone". Do not copy the asymmetry.
- **Idle GC of entity state** is a separate feature Restate does not have; **`restart-from-prefix` /
  `restart-as-new`** need retained journals, so a keep-no-journals policy removes them silently.
- **Ingress auto-injected keys** (`controlled-idempotent-sharding`, default on for new clusters) make
  ingress retries safe but silently subject ordinary Service/VO calls to idempotency retention
  (SRC:`crates/ingress-http/src/handler/service_handler.rs#L193`). Only relevant if we auto-assign.

## Open / unverified

- Whether anything rejects a shared-handler call to a workflow whose `run` was purged (docs say you
  cannot call them; no such check found). UNVERIFIED.
- Status rows lacking `completed_transition_time` (pre-`v2` layout) are skipped by the cleaner —
  "will be cleaned up with the old timer" (cleaner.rs#L210) — but no such timer exists in this tree.
  UNVERIFIED whether a migration guarantees none remain.
- No documented hard cap on journal size per invocation or on invocation count. UNVERIFIED.
