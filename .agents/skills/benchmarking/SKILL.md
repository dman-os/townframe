---
name: benchmarking
description: Measure and track performance across builds for any crate - which tier to use (instruction counts, counted allocations, simulated time, wall clock, browser), where benches live, how to add one, run it locally or on a mothership, read a trend report, and how the CI gate decides. Use when adding a bench, when a number moved, or when a PR trips the bench gate.
---

# Benchmarking

Deterministic numbers gate; real time only informs. A shared runner or a busy laptop cannot
tell a 5% slowdown from noise, but it counts instructions and allocations identically every
run. `x/bench.ts` is the one tool: every tier prints or is converted into the same JSON record
(`bench`, `metric`, `value`, `unit`, params, plus commit, dirty, machine, toolchain, profile,
target, ts; schema and logic in `x/bench-lib.ts`, tested by `x/bench-lib.test.ts`).

## Which tier

| Tier | What it measures | Gated | Where it lives | Use it for |
| --- | --- | --- | --- | --- |
| `instr` | callgrind instructions (Gungraun) | yes | `benches/instr.rs` | any CPU path, including ones that allocate nothing; a quadratic shows as 10x input costing 100x |
| `counts` | allocations and bytes (`utils_rs::alloc_count`), simulated time, rounds, wire bytes | yes | in-crate tests that print `BENCH_RECORD` lines (`scaling`, `driver/tests/bench.rs`, `durable_outbox/tests/bench.rs`) | scaling checks that need crate internals; throughput and latency sweeps on the paused tokio clock |
| `wall` | real time (divan, release) | never | `benches/wall.rs` | exploring where time goes on the machine in front of you |
| `web` | `performance.now()` and allocations in a browser | never | `src/big_automata_web` Playwright spec | the same workload as wasm; browsers have no instruction counter |

A crate opts in under `[package.metadata.bench]` in its Cargo.toml (`counts = {filter, features}`,
`instr = {bench}`, `wall = {bench, features}`, `web = {dir, spec}`); `x/bench.ts` finds every
crate that does, so nothing in the tool is specific to big_automata.

## Adding a bench

- **Scaling check (allocation counts):** a `#[test]` that calls `assert_linear(name, n, |n| ...)`
  with a closure that builds its input and returns `utils_rs::alloc_count::measure(op).1`. It runs
  n and 10n and fails above x14. Each run prints `BENCH_RECORD` lines. Install
  `#[global_allocator] static HEAP: utils_rs::alloc_count::CountingAllocator` once per test
  binary; counting is per thread, so tests may share a process.
- **Instruction bench:** put the setup and the measured body as public functions in
  `benches/workloads/mod.rs`, then wrap them with `#[library_benchmark]` and `#[bench::nXk(args =
  (n), setup = workloads::setup_fn)]` in `benches/instr.rs`, and list the function in the group.
  Setup is outside the measured region. Give every workload two sizes ten times apart.
  Gungraun's `setup` must be a path, not a closure, and benchmark functions take no doc comments.
- **Simulated-time sweep:** run on `#[tokio::test(start_paused = true)]`, inject latency with
  `n0_future::time::sleep`, print `big_automata::bench::record_line(...)`. The numbers are the
  same on every machine; assert the orderings that make the table trustworthy.
- **Wall bench:** a `#[divan::bench]` over the same workloads. Never add a gate to it.
- A bench file must still build on `wasm32-unknown-unknown`: gate the file with
  `#![cfg_attr(target_family = "wasm", no_main)]` and `#![cfg(not(target_family = "wasm"))]`, and
  keep native-only dev-dependencies under `[target.'cfg(not(target_family = "wasm"))']`.

## Running

```bash
x/bench.ts run big_automata:counts            # also :instr, :wall; no tier = counts and instr
x/bench.ts run big_automata --release         # profile is recorded with every record
x/bench.ts compare <rev-a> <rev-b>            # same machine, toolchain, profile, target only
x/bench.ts report --last 10 [--bench okv]     # per bench and metric, steps beyond noise flagged
```

History is append-only JSON lines at `$BENCH_HISTORY_DIR` (default
`~/.local/share/townframe-bench/<hostname>.jsonl`), outside the worktree, one file per machine.
Records of a dirty tree are kept but ignored by `compare` and `report`. `compare` refuses to
cross machines, toolchains or profiles; `--force-cross-machine` compares anyway under a loud
banner (counts can transfer between machines of one architecture, times cannot).

Noise thresholds (a step beyond one is flagged): 1% for instructions, allocations, bytes and
simulated time; 0.5% for calls, rounds and wire bytes; 15% for wall time.

**On a mothership** (the dev shell has valgrind and the Cargo shim; nothing builds on the laptop):

```bash
MS_HOST=mothership-alpha ~/.claude/skills/pi-lanes/ms-run <lane> 'x/bench.ts run big_automata --release'
```

The history lands in that machine's own file; compare commits there, never against the laptop.
Check out the two revisions in the lane worktree to build a pair.

## The CI gate

`.github/workflows/bench.yml`, on pull requests touching `src/big_automata*`, `src/utils_rs`,
`x/bench*` or the dev shells. One job, one runner, one checkout: `x/bench.ts gate --base <base
sha> big_automata` runs the gated tiers at the base commit, checks out the head, runs them again,
and calls the same `compare` as the command line. No baseline is committed. A tier the base does
not declare yet (the PR that introduces it) is skipped with a message. A regression beyond the
metric's noise threshold fails the job.

**Accepting an intentional regression:** put a line in the commit that causes it:

```
Bench-Accept: big_automata::scaling::okv* -- every row now carries a checksum
```

The glob matches bench names; the reason is mandatory and printed in the log. Nothing else
silences the gate; do not raise a threshold to make a PR pass.

## What does not work, and why

Recorded from runs on mothership-alpha (2026-11-25), not guessed:

- **Wall time in CI.** Shared runners are too noisy to gate on; wall is recorded, never compared
  by the gate (15% noise threshold, reported only).
- **Allocation counts miss non-allocating loops.** Replacing every replay target in one update
  allocates nothing at either size, so only the instruction bench (`replay_replace_every_target`)
  sees that path. Use both tiers for a hot path.
- **Setup is instrumented too.** Callgrind runs the setup under valgrind (about 50x slower), so
  a 100k-row setup costs minutes. Instruction benches use 1k/10k (replay: 1,638/16,380); the
  wall tier keeps 10k/100k.
- **Hardware counters (`perf_event_open`).** Not used: the gate counts instructions in software
  (callgrind). Whether GitHub's VMs expose them is unmeasured; `bench.yml` has a probe step for
  the first real run.
- **valgrind and the runner.** valgrind 3.27.1 comes from the repo's nixpkgs and is in the dev
  and `ci-rust` shells. `gungraun-runner` is not in nixpkgs; `x/bench.ts` installs the version
  `Cargo.lock` pins (0.20.0) with `cargo install --locked` into `target/gungraun-tools`.
  Gungraun's `summary.json` is version 7: `profiles[].data.total.metrics.Ir.values.new`.
- **divan has no JSON output**, so `fromDivanOutput` reads its tree table (median column). Its
  format is not a contract; the parser has a test against a captured sample.
- **wasm and dev-dependencies.** `cargo clippy --all-targets --target wasm32-unknown-unknown`
  already fails on the base commit (49 errors for `utils_rs`: tokio's `net`/mio and proptest's
  `wait-timeout` come in through dev-dependencies). Check wasm with the library builds:
  `cargo clippy -p big_automata --lib --all-features --target wasm32-unknown-unknown --config
  'resolver.feature-unification="selected"'` (clean, as are `utils_rs` and `big_automata_web`).
  The bench targets are empty on wasm by cfg but cannot be compiled there for the same reason.
- **Counting allocator in wasm.** `big_automata_web`'s `?mode=bench` installs it and reports
  allocations and `performance.now()` timings through `big_automata::bench::sync_once`; the
  host builds for wasm32. The Playwright run (`e2e/bench.spec.ts`) was **not executed**.
- **Whole sync stalls above about 3,000 objects.** With default `DriverConfig`, 3,000 objects
  sync in 2 rounds (8 calls) and 3,500 or more never finish, in under 5 s of real time and an
  hour of virtual time; `max_tracked = 1M` does not cure it. Pinned by the failing
  `driver::tests::bench::whole_sync_of_a_part_beyond_three_pages_completes`, not diagnosed or
  fixed. It also means the whole-sync bench cannot go to 10k/100k yet (the divan bench runs 1k
  and 3k).
- **`requeue_spawned`** (kit scheduler) rebuilds the whole spawn queue per call, so a budgeted
  driver using it is quadratic; nothing but tests calls it today.

**Unproven on GitHub.** The gate was run end to end on mothership-alpha, base against head, with
a planted regression, and not on a GitHub runner. Until a CI section goes green, unverified are:
the runner's valgrind (nix build or cache hit), cache behaviour (`rust-cache` and the
flakehub cache with a second checkout of the base commit in one job), the job's duration (the
gated tiers run twice, and callgrind is slow), whether `perf_event_open` exists there, and the
`Playwright` browser tier.
