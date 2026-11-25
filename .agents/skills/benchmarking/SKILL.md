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

FINDINGS-PLACEHOLDER
