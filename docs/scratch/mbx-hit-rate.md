# mbx hit rate across worktrees, and build-time tracking

Status: investigation, read-only.
Owner: claytxt lane; operator reviews.
Updated: 2026-11-19
Scope: mbx cache hit rate across worktrees and build-time tracking.

Investigation 2026-11-19, mbx 1.30.0. Read-only except two throwaway scratch
crates for experiments. Data: mbx session recordings
(`$(mbx cache dir)/sessions/v1/*.jsonl`) on both machines, which carry per
compilation the outcome, skip reason, duration and the hashed key components.
The recordings are retained for only 7d / 256 sessions, so the window is short:
laptop 139 sessions, mothership 272 sessions covering about the last 10 hours
(2026-11-18 18:00 to 2026-11-19 03:00 UTC). "s" below is compiler wall seconds
summed over compilations, not build wall time.

## Headline numbers

| | laptop | mothership |
| --- | --- | --- |
| lifetime hits / misses / not looked up / bypass | 15555 / 2054 / 10346 / 479 | 136806 / 209096 / 31795 / 4040 |
| hits / (hits + misses) | 88% | 39% |
| lifetime compiler time spent on misses / not looked up | 0.72 h / 5.4 h | 248 h / 61 h |
| uncached compiler time in the retained window | 22,048 s | 246,251 s |
| toolchain hashes seen | 1 | 1 |

The two machines fail differently. The laptop mostly runs cold units that no
prediction exists for ("not looked up"). The mothership looks up and misses.

## Hypotheses tested

- (a) Different Nix toolchain per branch: refuted. `flake.lock` is byte
  identical in every laptop worktree and in all 46 mothership worktrees
  (sha `a07c68`); nixpkgs 825e2028, rust-overlay fad80eb7. All recorded actions
  carry one `compiler toolchain` / `rustc version` hash per machine. The "Rust
  toolchain changed (31)" rows are a one-time transient from the 2026-11-10 mbx
  and flake bump (`ab93ace3`), when older baselines still predate it. Not
  steady state.
- (b) `--all-features` splitting clippy from nextest: real but small. Pairs of
  recordings differing only in `--cfg` (feature set) cost 11.9k s plus 5.4k s of
  cascade on the mothership (7%), and 2.5k s on the laptop. Most of it is
  nextest vs nextest (2.9k s), i.e. branches whose workspace features differ,
  not the clippy/nextest split (0.6k s).
- (c)+(d) `--out-dir`, `-Zembed-metadata`, `--emit` changing: real, and
  inherent. They are one signature: the same crate recorded once as a check
  or clippy unit (metadata only) and once as a build/test unit (codegen). Cargo
  keeps separate artifacts, hash and out-dir for each. mbx counts `--out-dir`
  as a changed flag but it is a consequence of the mode change. Cost: 25.6k s
  check<->build, 12.6k s clippy-driver<->rustc (also `CLIPPY_ARGS`,
  `CLIPPY_CONF_DIR`), about 16% of mothership uncached time, 22% on the laptop.
  Each (mode, features, profile, target) combination is paid once per source
  state; it only looks like a miss on "alternation". It cannot be removed, only
  made to happen once instead of once per worktree (see causes 1 and 2).
  `-Zembed-metadata` is set by Cargo itself and is not something a command opts
  into.
- (e) serde_derive: not an input of its own. serde_derive recompiles whenever
  `syn`/`quote`/`proc_macro2` artifacts differ, which follows the cause 4 and 5
  cascades. 57,000+ s on the mothership is "consequences only" cascade
  (`--extern`, `-C metadata`, `-C extra-filename`, `--out-dir`,
  `-L dependency` change, nothing else).

## Causes ranked by uncached time

### 1. Results evicted, then recompiled (mothership, about 69k s, 28%)

Evidence. Among 12,106 distinct action keys that were compiled uncached, 5,380
were compiled more than once. Every repeat is a recorded `miss` (a lookup that
found nothing), 12,539 of them more than an hour after the first, only 13
within 2 minutes, so it is not concurrency. The repeats are 69k s and continue
through the whole window (37% of the last hour). `~/.cache/mbx/actions/gc/v1/sweep.log`:
"the disk holding ... still has 24.4 GiB free after collection, under the
25.0 GiB minimum; collection removed everything it is allowed to remove". The
store holds only 10.9 GB (10,742 objects) although `gc.max_size` is 140 GiB;
`mbx stats` lifetime `pruned_store_bytes` is 1.26 TB, `pruned_target_bytes`
3.19 TB. The disk is 141 GB with 27 GB free, 75 live workspaces and 36.9 GB of
managed targets. `~/.config/mbx/config.toml` was edited at 02:36 today
(`min_free_size = "10GiB"`), after most of this window.

Fix (proposal, no setting changed here): the 10 GiB floor is right, but
`max_size = "140GiB"` and `target.max_size = "140GiB"` equal the whole disk, so
they cannot protect anything. Set `gc.max_size` to about 60 GiB and let
`target.evict_first` (already `.claude/worktrees` and `scratch`) do the target
trimming; prune stale `~/scratch/wt-*` (75 live workspaces counted as
independent caches totalling 171 GB). Verification: rerun
`x/build-timings.ts` and this analysis in 2 days, the repeat-key share should
go to about 0. Expected effect: removes most of the 69k s, the largest single
item; it also removes part of the 61k s in cause 2.

### 2. Unexplainable misses: recording cap (mothership, 61k s, 25%)

4,372 misses carry no key details because `events_max_size = 16MiB` truncates
big sessions (`mbx analyze`: "misses with no earlier recording ... stopped
recording at the per-session size limit"). This is the "misses with no earlier
recording" row. They cannot be classified at all, and they hide an unknown
share of causes 1 and 3-6. Fix: `mbx settings set events_max_size 256MiB` on
the mothership (and laptop, where 16 MiB sessions also truncate), plus
`gc.events_max_sessions 1024` if disk allows; hit-rate effect none directly, it
makes every later analysis trustworthy.

### 3. `ISLE_DIR` in the key: wasmtime stack (mothership 8.0k s direct, up to 40k s downstream)

Evidence. `environment ISLE_DIR` differs between consecutive recordings of
`cranelift_codegen` in 54 pairs, all across different worktrees (0 within one):
8,011 s direct, 98% of cranelift_codegen's pair-attributed time (the rest is
check<->build, 3,980 s). cranelift's `build.rs` line 186 emits
`cargo:rustc-env=ISLE_DIR=<isle_dir>`, and `isle_dir` defaults to `OUT_DIR` (lines
68-77), which is under the worktree's target path. mbx normalises `OUT_DIR`
but not a custom `rustc-env` value, so the worktree path lands in the key.
`cranelift_codegen` was uncached in 91 sessions; the wasmtime family
(cranelift*, wasmtime*, wash_runtime) is 43.5k s of uncached time, 40.4k s of it
in those 91 sessions (upper bound, partly overlaps cause 1). The laptop shows
the same shape in miniature (215 s direct, 2.7k s family). Related path
variables: `CARGO_MANIFEST_DIR` 2.9k s (big_node, day, big_cabinet, i.e. crates
using `env!("CARGO_MANIFEST_DIR")`, a value really compiled into the output),
`OUT_DIR` for `cranelift_assembler_x64` 2.2k s, `DEBUG_OUTPUT_DIR`
(`wasmtime_internal_component_macro`, `wit_bindgen_rust_macro`) 1.1k s,
`WCMP_BINDGEN_OUTPUT_DIR` 0.1k s.

Fix. In the dman-os wasmtime fork (rev a24366c, we have authority over it) change
the eight `include!(concat!(env!("ISLE_DIR"), ...))` sites
(`cranelift/codegen/src/lib.rs`, `opts/generated_code.rs`, and the
`isa/{x64,aarch64,riscv64,s390x,pulley_shared}/.../generated_code.rs`) to
`env!("OUT_DIR")` and drop the `rustc-env` line when `ISLE_SOURCE_DIR` is unset;
bump the rev in the root and `src/wasm_utils_rs` locks. `OUT_DIR` is already
normalised. Same treatment for our own `DEBUG_OUTPUT_DIR` / `WCMP_BINDGEN_OUTPUT_DIR`
(set from a build.rs or xtask: keep them out of the environment, or make the
value relative). Expected: 8-12k s direct, plausibly 20-40k s with the
downstream stack, 8-16% of mothership uncached time. Not verified by a rebuild:
that needs a heavy mothership build, to be done on the fork branch.

### 4. check<->build mode split (about 44k s mothership, 16%; 4.8k s laptop, 22%)

See (c)+(d). Inherent. The only lever is to make each mode pay once per source
state machine-wide, which is causes 1 and 3. Avoid gratuitous extra modes:
`--all-targets` and test profile (`debug-assertions` differs, 4.9k s) are
separate units from plain build.

### 5. Private incremental state, never shared (mothership 18k s, 7%)

`unconsulted: private-dependency / hot-reuse` is workspace crates mbx keeps
incremental for, plus every crate that links them (big_node 3.7k s, big_cabinet
3.7k s, day 2.9k s, big_nodule, big_blobs ...). They never look anything up and
never publish (documented in the incremental page). `learned_incremental = true`
on the mothership applies even to non-edit batch runs (nextest of unchanged
sources). Proposal: `mbx settings set learned_incremental false` on the
mothership only (it is a batch machine) and keep it on the laptop. Trade-off:
slower single-crate edit loops on the mothership. Try for a day and compare
`wall_s` in the timing log.

### 6. Feature-set differences (11.9k s + 5.4k s cascade mothership; 2.5k s laptop)

Includes the 12 laptop sessions run with `--config
resolver.feature-unification="selected"` (including native `check
--all-targets`, against the AGENTS.md rule); `selected` with different `-p`
lists gives each invocation its own feature sets, so the daytxt worktree alone
saw 769, 257, 129, 100 ... crates recompiled in successive sessions. Workspace
default is `workspace` in `.cargo/config.toml`. Fix: forbid `selected` on native
(wasm only), and prefer one `-p` set per lane. Expected 3-5%.

### 7. Bypasses that need dependency forks (mothership 8.2k s, 3%)

`unsupported-crate-type`: `iroh` 3.6k s, `iroh_relay` 1.3k s, `irpc_iroh`
1.2k s, `irpc` 1.1k s. All four declare `crate-type = ["lib", "cdylib"]` (irpc
`["cdylib","rlib"]`) in their published `Cargo.toml`; mbx does not cache
cdylib links. Fix: `[patch.crates-io]` forks with `crate-type = ["lib"]`, or
ask mbx upstream (not filed here). 8.2k s direct.

### 8. Cold units with no prediction (laptop 16.0k s, 72% of laptop uncached)

Laptop `unconsulted / no-prediction` is 7,386 compilations, 16.0k s. mbx groups
predictions by `Cargo.lock` digest and can borrow only from up to 8 earlier
lock states in Git history, or recent local store records. Lockfiles diverge:
laptop worktrees have 3 digests among 4 live checkouts (link-perf equals main;
design-prep equals daytxt; gui-loop differs, 459 to 462 changed lines vs main),
the mothership 46 worktrees have 24 distinct digests (1716 to 1757 packages).
Unmerged-branch digests are not in another branch's Git history. No key
was ever compiled twice in the laptop recordings (0 repeats of 9.5k keys), so
these are genuinely new keys, not lookup failures; it is cold cost for new
(lock, config) pairs. On the mothership this is only 3.7k s because the same 12
lock digests are shared by many worktrees. Fix: stop each lane from touching
`Cargo.lock` unless it adds a dependency, rebase onto main before building so
digests converge, and get one machine-wide warm tree (a `main` checkout built
by cron: `cargo clippy --all-targets --all-features` and `cargo nextest run
--no-run`) so predictions and results exist for main's digest. The remote cache
(`mbx remote-cache`, with `prefetch`) is the structural fix for the laptop
(not set up; per the docs it also publishes the prediction manifest).

### 9. Smaller items

- C objects embedding absolute paths (`aws-lc-sys`, `ring`, blake3 neon): `mbx
  explain --last` in design-prep lists 181+ `cc:*.c` "embeds absolute paths and
  is cached only for the same paths", 35.8 s for the build script plus about 5
  s per file, per worktree. Experiment: a small C file compiled in two
  directories gives identical objects with `-ffile-prefix-map=<root>=/ws`
  and different ones without; I could not make mbx cache the cc crate's real
  invocation in a scratch test (absolute out-of-root paths are bypassed), so
  the shell-hook idea is unverified: export `CFLAGS="-ffile-prefix-map=$PWD=/ws"`
  in the dev shell. Alternative: `mbx settings set cc_store_path_specific
  false` stops storing the useless objects.
- Laptop `unportable-native-link` (1.6k s): entirely from the link-perf lane's
  own linker experiments. A scratch crate with a build script and a proc macro
  under the exact dev-shell env (`-fuse-ld=/nix/store/...-mold-.../bin/mold`,
  clang linker) restores 4/4 from cache, so the dev shell's linker setup is
  cacheable.
- Linker versions: laptop recordings show two linker hashes (2858 vs 345
  actions), an older shell (`townframe-p3N-6`). Only matters for link units.
- `CARGO_TARGET_DIR`/shim: `mbx doctor` outside the dev shell warns
  "Cargo shim is not installed"; plain `cargo` there bypasses mbx.
- CI does not use mbx at all (`Swatinem/rust-cache`, `flakehub-cache-action`).

## Fix summary, by expected effect on mothership

1. Stop evicting: `gc.max_size` about 60 GiB, prune stale worktrees, keep the
   10 GiB floor. Up to 28%.
2. Raise `events_max_size` to 256MiB so the 25% unexplained misses can be
   diagnosed.
3. Fork patch: `ISLE_DIR` to `OUT_DIR`, same for our `*_OUTPUT_DIR`. 8-16%.
4. `learned_incremental false` on the mothership. up to 7%.
5. No `feature-unification=selected` natively; one canonical command per lane.
   3-5%.
6. cdylib forks for iroh*/irpc*. 3%.
7. Laptop: converge lockfiles, nightly warm main build, then a remote cache.

## Timing: what mbx already records and the proposal

Yes, per build and queryable, but not by `mbx stats --json`, which holds only
lifetime totals (builds, compilations by outcome, compiler_ns per outcome; no
wall time). `mbx analyze` and `explain --last` read only the last build. The
per-invocation record is the session file. Its `session_finished.stats`:
`session_duration_ns` (wall), `wrapper_phases_ns` with `permit_wait` (scheduler
admission wait), `flight_wait` (waiting on another build's identical
compilation), `compiler`, `key`, `lookup`, `restore`, `store`, plus
`compiler.{miss,unconsulted,bypass}` and `hits/misses/unconsulted` and
`estimated_compiler_duration_avoided_ns`, `slow_compilations`. `mbx cache trace
<session>` exports Perfetto. Sessions older than 7d / 256 are deleted, so
collection has to run at least daily. Observed on the laptop: scheduler wait is
negligible (a 148.7 s build spent 1.3 s in `permit_wait`); compiler time
dominates.

Prototype: `x/build-timings.ts` (deno, read-only against mbx and GitHub):

```
x/build-timings.ts mbx [--out FILE] [--host NAME]   # local cargo sessions
x/build-timings.ts gh  [--out FILE] [--limit N]     # CI job queue + run time, slowest steps
```

It appends idempotent JSONL rows (key `src` + `id`) to `$BUILD_TIMINGS` or
`~/.local/state/townframe/build-timings.jsonl`: per cargo invocation
`wall_s, scheduler_wait_s, flight_wait_s, compiler_cpu_s, hits, misses,
unconsulted, miss/unconsulted/bypass_compiler_s, avoided_compiler_s, workspace,
command, host`; per CI job `queue_s, run_s, conclusion, workflow, branch,
slowest_steps`. Tested: 152 laptop session rows and 5 CI job rows written, a
second run appended 0. Not wired anywhere. Proposal for every lane: run
`x/build-timings.ts mbx` from a user cron or systemd timer every 6 hours (well
inside the 7d retention) on each machine and `x/build-timings.ts gh` on one
machine, all pointing `BUILD_TIMINGS` at a shared synced file (or `rsync` of the
per-host files into `docs/scratch/build-timings/`), then trend with `jq` or
`x/instr-summary.ts`-style summaries. A follow-up would add a `report` mode
(p50/p95 wall per command, per host, per week; repeat-key rate from the
recordings as the eviction alarm).

## Method notes

Analysis scripts ran from the session scratchpad: per-pair key-component diffs
(most recent earlier recording of the same crate/unit-kind/crate-type, which
approximates `mbx analyze`'s baseline), consequence components
(`--extern`, `-C metadata`, `-C extra-filename`, `--out-dir`, `-L dependency`,
`-L native`) removed, and a repeat-key count over `diagnostic.action.hash`.
Caveats: 10 hours of mothership history; pair diffs can compare two valid
configurations of one crate; repeat-key detection excludes the 25% of
compilations with no recorded key details, so its 28% is a lower bound.
