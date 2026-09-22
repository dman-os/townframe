# big_sync retry/backoff pacing map

Read-only investigation, 2026-02. Question: do the low pacing constants cause churn that
stalls fences? Every claim carries a `file:line`. No builds were run.

## 1. The one formula, and the one cap

There is exactly one backoff formula in big_sync. Both `Scheduler::respawn_delayed`
(`src/big_sync_core/scheduler.rs:164`) and `Scheduler::spawn_delayed`
(`src/big_sync_core/scheduler.rs:207`) compute:

- first retry: `min(min_delay, max_backoff)` (`scheduler.rs:182`, `:220`)
- later retries: `max(prev_backoff * 2, min_delay)`, then `min(..., max_backoff)`
  (`scheduler.rs:186-189`, `:224-227`)
- a zero cap falls back to 60s (`scheduler.rs:178-180`, `:213-216`); the default cap is
  also 60s (`scheduler.rs:75`)
- `Scheduler::set_max_backoff` (`scheduler.rs:87`) is fed by
  `BigSyncMachine::set_max_task_backoff` (`src/big_sync_core/lib.rs:783`), which the
  worker wires from the embedder option
  (`src/big_sync/worker.rs:349-363` `spawn_big_sync_worker_with_options` → `:363`).

`lib.rs` holds one `tasks: Scheduler<TaskSeed>` (`lib.rs:771`) — there is no second
scheduler for the sync machine.

Key interaction, and the crux of the churn question: **when the cap is smaller than the
seed, the cap wins and the ladder is pinned at the cap forever.** `min_delay` = the seed
handed at each call site (2s nearly everywhere), so with a 500ms cap every retry computes
`max(2*prev, 2s)` = at least 2s, then clamps to 500ms. Doubling never becomes visible; a
persistently failing task retries every 500ms indefinitely. The cap was chosen
deliberately for this: "the daybook tests clamp it to 500ms so a route whose grant is
still in flight is picked up quickly" (`scheduler.rs:145-148` doc comment).

## 2. Every delayed-spawn site (seed → doubling → cap)

All sites pass the task's own `Retry` so the ladder doubles per attempt. `min_delay`
below is the seed passed at the site; it is only the floor, and the cap overrides it.

| # | Site | Task | Seed (min_delay) | First retry | Steady state |
|---|------|------|------------------|-------------|--------------|
| 1 | `lib.rs:1427` | `DecidePeerStrategy` (partial re-decide, `parts_retry.len() == response_len`) | 2s | see table below | see below |
| 2 | `lib.rs:1518` | `DecidePeerStrategy` (re-decide all failed parts) | 2s | 〃 | 〃 |
| 3 | `lib.rs:1814` | `ReplayPage` respawn (`spawn_replay_pages_inner`, `delayed: Some`) | seed handed in by caller | 〃 | 〃 |
| 4 | `lib.rs:2092` | `ReplayPage` respawn (second inner call, same pattern) | seed handed in by caller | 〃 | 〃 |
| 5 | `lib.rs:2380` | replay update after **refusals** (`schedule_replay_update`) | `REPLAY_RETRY_SEED` = 2s (`lib.rs:252`) | 〃 | 〃 |
| 6 | `lib.rs:2453` | replay update after RPC failure w/ new subscription | 2s (`lib.rs:2453`) | 〃 | 〃 |
| 7 | `lib.rs:2467` | replay pages rescheduled after failure below watermark (`schedule_replay_pages`, `lib.rs:1659`) | 2s | 〃 | 〃 |
| 8 | `lib.rs:3216` | `ListBuckets` error | 2s | 〃 | 〃 |
| 9 | `lib.rs:3250` | `LeafBuckets` error | 2s | 〃 | 〃 |
| 10 | `lib.rs:3370` | object-removal task failed (`RemoveFromParts`) | 2s | 〃 | 〃 |
| 11 | `lib.rs:3518` | "big sync object task failed; rescheduling" (`SyncKind::Sync`) — the AGENTS.md keyhive-race retry | 2s | 〃 | 〃 |
| 12 | `lib.rs:3566` | object task became **stale**; rescheduled | 2s | 〃 | 〃 |

Sites 3/4 take their seed from whoever called `schedule_replay_update` /
`schedule_replay_pages`; the only non-`None` callers are #5, #6, #7 (all 2s). Fresh
rounds call `spawn` immediately (`lib.rs:1819-1820`, `:2093-2094`, `lib.rs:1746`,
`:1944` pass `None`).

### Effective cadence per config

| Config | Cap set at | First retry (seed 2s) | Ladder | Steady state |
|--------|-----------|----------------------|--------|--------------|
| (a) daybook stress | 500ms: `src/daybook_core/sync/tests/stress.rs:266`, `:286`; wired via `src/daybook_core/sync.rs:267` → `src/daybook_core/repo.rs:25` (`sync_max_task_backoff`), `src/daybook_core/sync/bootstrap.rs:276` | `min(2s, 500ms)` = **500ms** | 2×500ms → 1s → `max(1s,2s)=2s` → clamped | **500ms per attempt, forever** |
| (b) big_repo harness | 5s: `src/big_repo/test2/harness/topo.rs:280` | **2s** | 2s → 4s → 8s → clamped | **5s per attempt** |
| (c) production default | none → 60s (`scheduler.rs:75`, `:178-180`) | **2s** | 2s → 4s → 8s → 16s → 32s → 60s | **60s per attempt** |

So under (a) a task that keeps failing retries 2×/s indefinitely — that is the churn the
operator is looking at. Under (c) the same task costs one attempt/minute. The cap is the
*only* lever on steady-state cadence; the 2s seed never binds when a cap below 2s is set.

Also paced but not by this ladder: the replay-page `Unauthorized`/`UnknownPart` refusal
is deliberately *not* given a faster pace than the ladder allows — the
`scheduler.rs:150-156` note says a caller that must not be paced at the embedder's cap
has to use something other than the task ladder. In current code the refusal retry rides
the ladder (#5), i.e. at 500ms under (a); the long-poll hold is the separate pacing
mechanism.

### Non-backoff pacing constants (for completeness)

- Replay page long-poll hold: `HOLD_MS = 15_000`
  (`src/big_sync_core/tasks/replay_page.rs:98`) — a client-side flow-control hold while
  caught up, not a retry. The big_repo harness shortens it to 200ms
  (`src/big_repo/test2/harness/topo.rs:26`, wired `:288`); daybook uses the 15s default.
- Worker idle sleep ceiling: 60s (`src/big_sync/worker.rs:600`), a safety-net wake.
- Stress mutation jitter: 1–15ms random sleeps (`src/big_sync/stress_support.rs:434`) —
  load shaping, not backoff.

## 3. Is there a genuine 50ms backoff?

**No.** Every 50ms constant found is a poll, a test long-poll hold, or load jitter:

| Site | Nature |
|------|--------|
| `src/big_sync/stress_support.rs:615` | settle-fence poll sleep (`wait_for_cluster_settled`) — poll |
| `src/big_sync/worker.rs:328` | `wait_for_idle` snapshot poll — poll |
| `src/big_sync/test.rs:1052`, `:1113`, `:1185`, `:1942`, `:1992`, `:2545` | test-harness polls/waits — poll |
| `src/big_sync/part_store/sqlite.rs:3153` (`PAGE_HOLD`) and the `:2431`/`:2486`/`:2569`/`:2600`/`:2848`/`:2876`/`:3235`/`:3283`/`:3296` usages | replay-page long-poll holds inside `#[cfg(test)]` (module at `sqlite.rs:1875`) — long-poll holds, not retries |

No retry path in big_sync or big_sync_core uses a 50ms constant. Retry pacing is
exclusively the scheduler ladder above.

## 4. Cost of raising the two candidate constants to 1s

### Candidate A: settle-fence poll 50ms → 1s (`stress_support.rs:615`)

The fence returns after `STRESS_SETTLE_STABLE_ROUNDS = 20` consecutive unchanged
observations (`stress_support.rs:16`, loop `:562-583`). Once the cluster is truly quiet,
each round costs one observation pass plus the sleep, so the fixed quiet window is
~20 × poll.

- At 50ms: quiet window ≈ **1.0s** after the last change.
- At 1s: quiet window ≈ **20s** → **+~19s per settle fence**.
- `run_randomized_stress` places **7** settle fences per run
  (`stress_support.rs:665, 686, 705, 726, 745, 766, 782`) → **+~2.2 min per stress run**,
  on top of the same 19s again whenever the fence's own change-detection latency
  matters during active churn.
- The daybook stress suite does *not* use this fence: its settlement is
  `wait_network_rest` → `wait_for_full_sync` + `wait_for_quiescence`
  (`src/daybook_core/sync/tests/stress.rs:394-431`, `:817`, `:844`), which is
  event-driven and pays nothing here. The cost lands entirely on
  `run_randomized_stress` consumers (`src/big_repo/test2/stress.rs:1240`, `:1261`).

Verdict: **not justified as a correctness change.** The poll is a poll, not a backoff;
raising it buys no reduction in retry churn because it does not touch the task ladder.
It only trades 20× slower fences for less observation IO. If poll cost is the concern,
lowering `STRESS_SETTLE_STABLE_ROUNDS` or scaling it with the poll keeps the *quiet
window* constant — the window (1s today) is the semantic, the round count is an
implementation detail. Measurement that would decide it: wall time of the 7 settle
phases and CPU time per run before/after (both already logged at
`stress_support.rs:494`/`:652` "completed stress phase" and settle timing logs).

### Candidate B: daybook stress cap 500ms → 1s

Effect (from the table in §2): the steady-state retry cadence of every failing task goes
from one attempt per 500ms to one per 1s — halving the worst-case "big sync object task
failed; rescheduling" storm rate, but doubling how long a part whose keyhive grant is
still in flight stays blocked (the exact reason the 500ms clamp exists,
`scheduler.rs:145-148`). Under the red-herring note in AGENTS.md, these retries are the
intentional big_sync↔keyhive membership race; the retry storm is the symptom, so the
only legitimate question is whether the *cadence* is wasteful, not whether the retries
should exist.

Verdict: defensible but not free; the 1s pickup penalty is paid on every grant-in-flight
route in every stress run. Measurement that would decide it: (1) count of
`big sync object task failed; rescheduling` lines per run under both caps, (2) settle
phase wall time under both caps. If halving the storm rate does not shorten settle wall
time (because settles are gated by grant arrival, not by retry CPU), the change is pure
cost and should be dropped.

## 5. Do retrying tasks actually block fences?

Four distinct fences touch the big_sync worker; they couple differently.

### 5.1 `wait_for_full_sync` — YES, structurally, via machine state

- The fence registers a waiter (`src/big_sync/worker.rs:240`) which is stored in the
  stat machine (`lib.rs:475-511`) and resolved only by
  `__check_peer_part_synced` (`lib.rs:644-700`).
- The gate predicate `peer_part_is_fully_synced` (`lib.rs:460-468`) requires
  `!pending && !multi_strat && replay_phase_done && !cursor_active`. An
  undecided part *must* block full sync (`lib.rs:464-466` comment).
- A failed `DecidePeerStrategy` re-seeds delayed (site #1/#2) and leaves its parts in
  `PeerPartStrategy::Pending(decide_task)` for the whole ladder
  (`lib.rs:1191`, `:1440`, `:1528`) → `mark_peer_part_pending(true)` (`lib.rs:610`)
  → part not synced → waiter unsatisfied. The fence is held until the decide succeeds.
- A refused replay route is `block_replay_route`d (`lib.rs:1837`, applied `:2378`) and
  marked unanswered (`:2384`); `update_peer_replay_done` (`lib.rs:2173-2195`) can then
  not mark the peer's replay done → `replay_phase_done` stays false → blocks full sync
  "until an update for it succeeds or the embedder drops it"
  (`lib.rs:2175-2176`, tests at `lib.rs:3590-3624`). The retried update (#5) is what
  resolves it, so the fence waits out the ladder — at 500ms/attempt under (a).
- Plain object-task retries (#10–#12) do **not** flip any of these flags; they only
  delay cursor/data arrival, lengthening fences indirectly.

### 5.2 `wait_for_idle` (test-support) — YES, directly, by `delayed > 0`

- `WorkerSnapshot::is_idle` (`src/big_sync/worker.rs:122-128`) requires
  `task_counts.delayed == 0` (plus spawn/stop queues and no active tasks);
  `TaskCounts::is_idle` mirrors it (`src/big_sync_core/tasks.rs:147-151`).
- A task sitting in the delayed queue is still `live` (`scheduler.rs:225-232`), so any
  task waiting out a backoff holds this fence for its entire remaining ladder. Under
  (c) production default, one mid-ladder task can hold `wait_for_idle` for ~60s per
  attempt; under (a) the same hold is ≤500ms/attempt. `wait_for_idle` is test-support
  only (`worker.rs:303-331`, `src/big_sync/test.rs:1071`), so this coupling shows up in
  test fences, not production.

### 5.3 `wait_for_cluster_settled` (stress settle fence) — NO structural block

- It polls `observed_state` only (`stress_support.rs:558-561`); the task table is not a
  term. A task parked in a 60s backoff does not hold this fence — in fact the fence is
  **blind** to delayed work and can pass with ladders outstanding (the inverse risk).
- Churn interacts only through data: each *successful* retry that lands data resets
  `stable_rounds` (`stress_support.rs:566-583`). A persistently *failing* task keeps the
  cluster observationally stable and lets the fence pass. So the AGENTS.md red-herring
  storms do not stall this fence by existing; they stall it (if at all) by succeeding
  intermittently.

### 5.4 runtime2 `wait_for_quiescence` — NO direct coupling to the task table

- The quiescence probe is a hub-side barrier: doc-worker fences, `tracked_in_flight`,
  keyhive syncs, and an activity-generation restart
  (`src/big_repo/runtime2/hub.rs:562-686`, predicate `:639-646`).
- big_sync's delayed queue is not a term. A retry that wakes and ingests into the repo
  can bump hub/doc-worker activity and restart the probe (`note_activity`,
  `hub.rs:555-559`; activity rules `:685-713`), but a task merely waiting out its
  backoff holds nothing. Daybook's fence composition
  (`daybook_core/sync.rs:1180` → `wait_for_network_rest`,
  `src/big_sync/test_support.rs:48-115`) couples the two: `wait_for_full_sync` (5.1)
  plus repeated `wait_for_quiescence` rounds.

## 6. Bottom line

- All retry pacing is the one scheduler ladder; seeds are uniformly 2s and the embedder
  cap is the only steady-state lever. Under the daybook stress cap the ladder is pinned
  at 500ms/attempt — that is the churn.
- Fences are blocked by *machine state* (pending decide, blocked replay route,
  `delayed > 0` for idle), and only until the ladder succeeds — never by a fixed
  timeout. The 500ms cap therefore shortens the very holds the low constants are blamed
  for; raising the cap to 1s makes every blocked fence wait longer per attempt.
- No 50ms constant is a backoff. The settle-poll raise to 1s would cost ~2.2min per
  stress run and changes nothing about retry churn; it should not be made on pacing
  grounds.