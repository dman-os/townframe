---
name: perf-investigation
description: Find where an operation's latency, CPU, heap and IO actually go in a big_node / big_cabinet / big_repo workload - wall vs CPU per operation, critical-path phase timings, background load, SQLite transactions, fsyncs and thread hand-offs. Use when an operation is slower or heavier than its napkin cost (ledger D25 targets), when a cost grows with data size, or before proposing a performance fix.
---

# Perf investigation

Use this when a number is wrong: "creating a note takes 777 ms against a 5–20 ms target", "allocations
per note grow with the drawer". The goal is the physical execution: which phase, on which task, and
whether each cost is
- **a dumb thing**: a sleep, poll interval, debounce, timer tick, an actor round trip that batches on a
  timer, fsync, a lock held across awaits, or independent work run in series;
- **a design wrong turn**: report it, don't decide it;
- **genuine work**.

Read `AGENTS.md` first. Its Experimentation rules apply: every surprise gets instrumented in the same
turn, and instrumentation goes in its own commit.

## Suspect latency before CPU

An operation whose wall time is **flat with data size and suspiciously constant** across samples (for
example 761–771 ms for 30 notes in a row) is shaped by a timer, not by work. Work has variance and grows;
a poll interval does not.

Measure wall time and CPU time around the same operation first. Use `/proc/self/stat` fields 14 and 15
(utime and stime in clock ticks, normally 100 Hz). They sum over all threads, dead ones included. The
`costs.rs` harness prints both per sample.
- **CPU well below wall**: the operation is waiting. Look for timers and round trips.
- **CPU above wall**: other threads are busy, either the operation's own fan-out or background load.
  Separate the two before blaming the operation.

## The harness: `src/big_node/e2e/costs.rs`

- **Counting allocator.** A `#[global_allocator]` over `System`, off until a test sets `COUNTING`. It
  counts allocations, bytes allocated, live bytes and per-interval peak, process-wide, so the node's
  actors and workers are included. Counts are only meaningful with one test per process, which is how
  nextest runs.
- **`measure(fut)`.** Returns `Usage { wall, allocations, allocated, peak }` for one awaited future.
  Allocations are the *counted* work and can be asserted on. Wall time is only *reported* (the
  `BandRun` precedent).
- **`long_test_cost_table`.** Runs every user operation as a script and prints `COST` lines: p50, max,
  first tenth and last tenth of the drawer, storage per note, and heap and RSS per node.
  `BIG_NODE_COSTS_NOTES=N` sets the size; the default of 10 keeps the debug run short.
- **`a_note_costs_no_more_to_create_in_a_fuller_drawer`.** The pinned growth test: the median
  allocations of notes 6–10 against notes 26–30 must stay within 1.25×. Pin a new growth claim the
  same way, as a ratio of *counts* between two sizes, never as wall time.

To add an operation, drive it through the service clients like a user would, then wrap it in
`table.run("op", ...)`, or in `measure` for per-sample output.

## Release vs debug

- **Debug** is for the counted claims, which do not depend on the build, and for the default-run test.
- **Debug wall times are not findings.** Debug is 2–5× slower and the ratio is uneven between Rust,
  SQLite and crypto.
- **Take wall and CPU numbers in release, one release build at a time** (the host has a shared memory
  slice):

```bash
S=<scratchpad>
TASK_CENSUS_FILE=$S/census.tsv \
RUST_LOG_TEST="info,big_cabinet=debug,subduction_core=warn,keyhive_core=warn" \
./x/agent-build.ts -- cargo nextest run --release -p big_node -j 6 --no-capture \
  -E 'test(/costs::a_note_costs/)' > $S/run.log 2>&1
deno run --allow-read x/instr-summary.ts $S/run.log --skip 5
```

`x/instr-summary.ts` reads two kinds of `INSTR` line:
- per-sample lines (`INSTR create note 12: wall … cpu … allocations …`), reported as p50, first-tenth
  p50, last-tenth p50 and max, so growth shows as last > first;
- phase lines (`INSTR <name> timing k=<duration> …`), reported as count and p50.

`--skip N` drops warm-up samples.

If a remote box is offered (for example a different architecture), take the headline number there too,
and say which host each number came from.

## Phase timings: INSTR lines and spans

Existing `tracing` spans rarely cover the whole critical path. Add `// INSTR` timing lines where they
are missing:

```rust
let instr_start = std::time::Instant::now(); // INSTR
/* phase a */
let instr_a = instr_start.elapsed(); // INSTR
/* phase b */
tracing::info!(?instr_a, total = ?instr_start.elapsed(), "INSTR <name> timing"); // INSTR
```

- Name the line `INSTR <name> timing` so `x/instr-summary.ts` picks it up.
- Log at a level your `RUST_LOG_TEST` admits.
- Bracket each sample with `tracing::info!(ordinal, "INSTR <op> begin")` so one sample's lines can be
  cut out with `awk '/begin ordinal=20$/,/INSTR <op> 20:/'`.
- Check that the phases sum to the sample's wall time. If they don't, a phase is missing.

**Keep INSTR in its own commit, separate from fixes, so it can be dropped on its own.** Files another
agent is editing get INSTR lines only.

## Census: who else is running

`TASK_CENSUS_FILE=<path>` makes `utils_rs::census` append one TSV line per closed span (see
`src/utils_rs/census.rs`). Summarize it with:

```bash
deno run --allow-read x/census-report.ts <census.tsv> --top 30
```

The census key columns are `worker`, `task_id`, `peer_id`, `doc_id` and `obj_id`. **Service calls are
keyed by operation through `worker = nsid` on the `service.call` span** (`service_utils_rs/host.rs`).
Count calls and total time per operation with:

```bash
awk -F'\t' '$2=="service.call"{c[$5]++; d[$5]+=$3} END{for(k in c) printf "%7d %9.0f ms %s\n", c[k], d[k], k}' census.tsv | sort -rn
```

Divide the counts by the number of samples. An operation that issues one `createDoc` and three
`writeDatums` should not cost hundreds of `readDatums` per sample; when it does, a background worker is
reacting. Census durations run from span creation to close, so long polls (`pollDatums`) show as
seconds of time that is not busy.

## SQLite: transactions, leases and who takes them

`sqlite_rs` carries INSTR counters, added in the commit that pins the note-create investigation:
- `INSTR_SQL`: write leases, read leases, write-gate wait (ns), worker jobs, worker busy (ns), counted
  in `lease_inner` and `call_on`;
- `INSTR_SQL_CALLERS`: every 16th lease's caller, sampled with `Backtrace::force_capture` and keyed by
  read/write plus the first three frames from our crates.

Diff them around a sample, as `costs.rs` does, and dump the callers after the loop. A lease is one
transaction or one statement outside a transaction. Read the totals like this:
- **Write-gate wait** summed over a sample: contention on the single writer.
- **Job busy**: SQLite's share of the CPU.
- **Top callers**: who to blame. In the note-create case, the big_blobs worker and the index readers
  outnumbered the operation's own writes.

The same pattern works for any shared resource: a static atomic counter at the choke point, plus a
1-in-N backtrace sampler keyed by the first frames from our crates.

## IO, fsync and thread hand-offs: `strace -c`

`perf` may not be installed. Off-CPU waiting never shows in perf anyway; the census and INSTR timings
cover it. `strace -c` counts syscalls:

```bash
./x/agent-build.ts -- cargo nextest list --release -p big_node --message-format json  # binary-path
RUST_LOG_TEST=warn strace -f -c -o $S/strace.txt \
  -e trace=fsync,fdatasync,sync_file_range,pwrite64,pread64,write,futex \
  <binary> --exact e2e::costs::<test> --nocapture
```

- **fsync/fdatasync.** SQLite runs WAL with `synchronous=NORMAL`, so commits do not fsync; only
  checkpoints do. A few per operation is expected.
- **pwrite64.** Approximates WAL frames written.
- **futex.** Approximates cross-thread hand-offs: actor hops, the oneshot replies to SQLite worker jobs,
  notifies. Tens of thousands per operation means many small round trips.

strace slows the process, so take counts from it, not times.

## Separating critical-path waits from background load

1. **Phase sum.** The INSTR phases of one sample should add up to its wall time. A phase whose wall time
   is large and whose CPU is about 0 is a wait. Find what it waits on: a channel, a watermark, a
   `Notify`, a `sleep`.
2. **Change the suspect and re-run once, then revert.** For a poll interval, shrink it (250 ms → 5 ms)
   and watch both the wait and total wall time move. That is proof, not inference. Revert with no
   diff left, and report the experiment.
3. **Background load.** Process CPU, allocations and SQL leases per sample that grow with data size
   while wall time stays flat are background work hidden behind a wait. Attribute it with the census
   (operation counts) and the lease sampler. Then add one INSTR line per unit of background work, such
   as `INSTR blob worker processed document=…`, and count it per sample, and per document
   (`sort | uniq -c`), to see what is reprocessed and how often.
4. Note what the background work will do once the wait is gone: it will compete with the critical path
   for the writer and the cores.

## Pitfalls

- **A poll interval masquerading as work.** Wall time that is flat and constant across samples. In the
  note-create case, three waits on `keyhive_admission::IDLE_POLL` (250 ms) were 87% of the latency: the
  admission tailers only woke on the poll, and each grant waited for the group-part worker's watermark.
- **Background work hidden behind a wait.** CPU above wall, and allocations that grow while wall time is
  flat. Fixing the wait exposes it, so report both together.
- **A rearm that rescans everything.** A "policy changed → rearm" handler that reprocesses every row
  turns into O(n) per change, and quadratic overall when every operation triggers a change.
- **Experiments shift load.** A shorter poll also adds idle reads and writer contention. Compare SQL
  counters and write-gate wait between runs, not just wall time.
- **Other agents' half-landed edits.** A shared worktree can stop compiling between your runs. Retry the
  build in a loop with a pause between attempts rather than "fixing" their files. Expect files you only
  instrument to change under you.
- **Assuming the test binary's location.** The target directory comes from `.env`, and the binary may
  not be under `release/deps`. Ask nextest for it (`nextest list --message-format json`, field
  `binary-path`).
- **Clock-tick CPU.** `/proc/self/stat` has 10 ms resolution. Read CPU over many samples or medians,
  not one short operation.
- **Debug numbers reported as findings.** Re-take them in release.

## Report shape

- A table of phase → wall ms, CPU ms, allocations and class (dumb thing, design, or genuine), with the
  evidence for each row.
- Process-wide counters per sample: CPU, allocations, SQL leases, write-gate wait, fsync, futex.
- The top wins with a confidence for each, and the experiment behind it.
- The INSTR lines added, as file:line, and the commit they are in.
- The exact commands run.
