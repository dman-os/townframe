# Time attribution plan: big_sync / big_repo / daybook_core under full-parallelism stress

Goal: attribute the 51s → 234s stretch of the loaded 4-package tier10 run to layers and
work items. This is **attribution, not benchmarking**. Box facts: no `perf`, no rr,
`perf_event_paranoid=2` (user-space-only sampling of own processes is *allowed*), 16 cores,
PSI cpu avg10 ≈ 41–53 with full=0.00 → the run is **never CPU-saturated**; wall-clock is going
to waits, serialization points, or spawn/scheduling churn — this shapes every option below.

## 1. Ranked options

### Rank 1 — census records + offline aggregation (no install, answers the core question)

The census layer (`TASK_CENSUS_FILE`, one JSONL record per span close, carrying parent
link + the field vocabulary from `docs/scratch/task-attribution-conventions.md`) is the only
option that attributes wall-clock **by work item** across the whole 234s run, including tasks
that finish long before/after the interesting moment.

Aggregation script shape (`x/census-report.ts`, deno, no deps, reads JSONL):

- Build tree by `parent` anchor; keep each record's `name`, `fields`
  (`worker`, `task_id`, `peer_id`, `doc_id`, `obj_id`), `start_ns`, `dur_ns`.
- Emit two views:
  1. **Top-N table** by cumulative busy time, grouped by static span name (+ `worker`),
     with: count, sum/min/mean/p50/p99/max duration, fan-out (mean children per parent),
     and per-parent spread (max child sum / own duration — exposes wait-dominated spans).
  2. **Text icicle** (indented tree, sum durations, % of run) — the poor man's flamegraph.
- Attribution queries the census cannot fake: per-`doc_id` and per-`obj_id` wall-clock
  share; retry counts (same `worker`+`task_id` appearing >1×, or span with `err` status);
  **stalled-retry pinning** = tasks whose duration exceeds p99 or whose retries succeeded
  only after a delay (closes the big_sync ↔ keyhive race question with numbers);
  spawn-rate time series (tasks/sec, to see if the 51→234s delta is spawn-loop explosion).
- Cross-correlate with `INSTR`/`resp ingested` log lines by timestamp to pin the serving-side
  keyhive divergence hypothesis.

Can answer: which span name / work item class owns the wall-clock; which specific jobs stall;
whether time is fan-out (spawn loops) or depth (one slow link). Cannot answer: time inside a
single long poll or inside native/sqlite C code (that shows as one opaque long span); time
outside tokio tasks (sqlx SQLite connection threads, iroh) unless those get spans too.

Effort: script ~½ day (data format is already agreed); adding census to the 2–3 hot
non-tokio surfaces (sqlx acquire/wait) ~½ day each.

### Rank 2 — tokio-console / task tracing for wakeup churn (binary already installed)

`tokio-console` is present (`/nix/store/...-tokio-console-0.1.14`); `tokio_unstable` is already
on; `console-subscriber = "0.5"` is a commented-out dep and the registry hook is
pre-commented in `src/utils_rs/testing.rs:28,46-47,78-79`. Wiring it back is one dep + 3 lines.

Answers: live task counts, per-task poll durations, wakeups and wakes-per-poll — i.e. whether
the ~267 threads / thousands of tasks are waking constantly (churn) or parked (contention/
blocking). The console's "durations" column directly exposes tasks that poll-burn CPU vs tasks
that never get polled (scheduler starvation).

Cannot answer: wall-clock attribution by work item over a full 234s run (interactive/live, not
recorded history), and it sees only tokio tasks — not SQLite/iroh threads or sync code.
Use it as a *live* adjunct during the next hunt, not as the primary record.

Effort: 1–2 h to wire + one stress run with console attached.

### Rank 3 — sampling profiler: 'requires install', feasible here

- `samply` — **requires install** (`nix shell nixpkgs#samply`, network reachable). It uses
  `perf_event_open` directly, so **no `perf` binary is needed**, and `paranoid=2` still permits
  user-space sampling of our own processes. `nix shell` is transient (no system mutation).
  Gives real flamegraphs of user-space stacks. Kernel frames excluded under paranoid=2 —
  acceptable, our wait is unlikely to be kernel-side given io/mem PSI ≈ 0.
- `cargo-flamegraph`/`flamegraph` — would need the `perf` binary; skip in favor of samply.
- No-install userspace fallback: `strace -c -f` on one process for a coarse syscall-time
  profile (it exists on the box), plus per-thread `/proc/<pid>/task/*/stat` snapshots —
  coarse, heavy, last resort only.

Caveat: with cpu PSI full=0.00, a CPU sampler may show *nothing interesting* — if wall-clock
is in waits, sampling will be flat. Use samply only after census says a span is busy, to look
*inside* that span. That's the correct division of labor: census = where; samply = why.

Effort: 15 min install + run; interpretation 1–2 h.

## 2. Benchmarks — deprioritize; criterion is NOT in any Cargo.toml (checked), no benches/ dirs exist

Benchmarks answer *scaling* questions, not attribution. At most one per layer, each tied to a
specific open question, and only if the census leaves a specific doubt:

| Layer | Bench (1 max) | Question it answers |
|---|---|---|
| big_sync_core | `TokioKeyedScheduler`/`ConcurrentDeltaWalker` on a synthetic event set, parallelism 1/4/16 | Is walker throughput the knee, or does it scale — i.e. is spawn-per-item vs batched the 51→234s multiplier? |
| big_repo | SqlCtx fanout: 1 db × 1 conn vs 5 conn, plus 4-db loaded shape | Does immortal-conn/thread count (5 per db, 140 threads) itself cost wall-clock via contention, or is it idle overhead only? |
| daybook_core | tier10 at controlled parallelism sweep | Where is the parallelism knee — does raising/lowering worker concurrency move the 234s? |

Do not add criterion just to get numbers; `std::time::Instant` + the existing stress harness
(`src/big_repo/test2/stress.rs`, `src/big_sync/stress_support.rs`) is enough for these three
questions and keeps deps unchanged.

## 3. Recommended sequence (goal: pin spawn loops / stalled-retrying jobs next hunt)

1. **Census aggregation script** (`x/census-report.ts`) — ½ day. Write it against the agreed
   record format now, so it is ready when the census layer lands; dry-run on a recorded log.
2. **Census on SqlCtx/sqlx acquire + iroh-blob-store surfaces** — ½–1 day. The 140 idle
   connection threads and 99 blob-store tasks are unattributed today; one span each closes
   that hole. Verify the 51/85/234s baselines did not drift (conventions doc gate).
3. **Run the loaded tier10 hunt with census on; aggregate immediately** — 1 run. Produce the
   top-N table + icicle + retry/stall report. This alone should answer: which layer owns the
   183 extra seconds, and which specific `worker`+`task_id` jobs are the stalled retriers.
4. **tokio-console attached live during the same run** — 1–2 h (wiring + observation). Confirms
   or refutes wakeup-churn vs parking for the top census suspects.
5. **samply under the top hot span only, if census says a span is busy not waiting** — 15 min
   install + run. Skip entirely if the run is wait-dominated (likely, given PSI).
6. **Benchmarks: only as follow-up** (census → bench question mapping above), never as the
   first instrument.

**Start with step 1**: the aggregation script is install-free, immediately reusable every hunt,
and is the only option that attributes the *whole* run by work item — everything else either
needs installation, sees only tokio, or answers scaling rather than attribution.