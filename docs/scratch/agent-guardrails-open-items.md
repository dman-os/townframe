# Agent engineering guardrails — open items

Status: PR preparation (guardrail tooling and wasm/native split implemented; follow-ups below)
Owner: operator + agent engineering track
Updated: 2026-10-20 (tracker reconciliation; cross-worktree sccache adoption shelved)
Scope: Rust workspace only. The Kotlin/Compose app (`src/daybook_compose`) is being
deleted and replaced with Rust, so every Kotlin item (detekt thresholds,
`ForbiddenImport`, the generated `uniffi/*.kt`) is explicitly out of scope.

Framing agreed with the operator: this is not only code quality. It is **making
independent changes cheap and interference visible** so many agents can work on
many branches of one repo at once.

## 0. Status — what has landed (PR 1)

All in `src/xtask/` (`cargo x …`), no separate checker crate:

- `cargo x lint` — `syn`-based inventory of every workspace crate (~0.3s, no compile):
  authored definitions per module, methods per type/trait, longest fn, longest match arm,
  inline `#[cfg(test)]` size, plus pattern rules (`ERROR_SHARED_ASYNC_STATE`,
  `ERROR_ALLOW_ATTRIBUTE`, `ERROR_LINT_REASON`, `ERROR_GLOB_EXPORT`, `ERROR_SEND_DISCARDED`,
  `ERROR_IGNORED_TEST`, `ERROR_FIXED_TEMP_PATH`, `ERROR_FIXED_TEST_PORT`), dependency
  boundaries (`ERROR_DEPENDENCY_BOUNDARY`), migration identity (`ERROR_MIGRATION_HISTORY`,
  §6.4) and docs (`ERROR_DOC_LINK`, `ERROR_ADR_ID`, `ERROR_ADR_STATUS`, `ERROR_DOC_METADATA`,
  `ERROR_SKILL_DOC`). `@generated` files are skipped.
- Crate-local `guardrails.toml` next to each `Cargo.toml` (+ `docs/guardrails.toml`):
  role, allowed workspace deps, per-symbol exemptions with `class`/`reason`/`reference`,
  migration hashes. Exemptions are ceilings: an improved site is `ERROR_STALE_EXEMPTION`
  until `cargo x guardrails bless` lowers it. `bless` never raises or adds. `adopt` refuses
  to run when a policy already exists.
- `cargo x api --all --check|--bless` — `public-api.txt` per pilot crate (`big_sync_core`,
  `daybook_ffi`, `daybook_pdk`, `daybook_types`, `wflow_sdk`) with exposed-external-crate
  header; failure prints added/removed items, newly exposed crates and affected consumers.
  Syntactic (syn), not rustdoc-JSON: no auto-trait/bounds resolution yet (§10 item).
- `cargo x plan <files>`, `cargo x overlap --base <trunk>`, `cargo x record/verify`
  (source-fingerprint receipts), `cargo x doctor` (flag drift, `rustfmt.toml`, profile).
- `rustfmt.toml` `imports_granularity = "Item"` applied; `debug = "line-tables-only"`;
  `flake.nix` rustflags now carry `--cfg tokio_unstable` (§6.1 fixed); `sccache` in the
  dev shell; `x/agent-build.ts` (FIFO jobserver + `systemd-run` cgroup scope + sccache env).
- nextest `nightly` profile (`flaky-result = "fail"`), selected by CI on the cron trigger.
- Pre-commit: `cargo x lint`, `cargo x api --all --check`, `cargo x doctor`.
- `xtask` slimmed: demo commands and the dead `cargo x gen` type generator (2.4k lines
  writing into four crates that no longer exist) removed; dep tree 1114 → 439 nodes.
  `wflow_core/gen/mod.rs` is now hand-maintained (path unchanged, 10 external uses).

### 0.1 Wasm/native split (second PR)

- Two Cargo workspaces: root (native apps + portable libs) and `src/wasm_utils_rs`
  (guest components/apps, `opt-level = 3` in dev and release, separate lockfile seeded from
  the root one). Crate directories stay under `src/`; the guest workspace lists siblings as
  members and the root workspace `exclude`s them.
- `utils_rs` is portable-only: `native`/`downloader`/`wasm` features deleted. `downloader`
  and `file_exists` moved to `native_utils_rs` (consumers: `mltools`, `daybook_core`);
  `uuid/js` plus both `getrandom` `wasm_js` backends are browser-target dependencies.
- `daybook_types` no longer defaults to `uniffi`; `btress_sysadmin` no longer defaults to
  `ssr` + `hydrate` simultaneously (each side is built alone, which surfaced three dead-code
  and one unused-import break that the combined build hid).
- Plugs split into `plug_*_manifest` (portable types, manifest bin, native e2e) and
  `plug_*_routines` (WIT + workflows). `cargo x build-plug-oci --plug-root <manifest crate>`
  reads `[package.metadata.townframe] routines` and builds the sibling component; logical
  plug ids and bundle keys are unchanged, component filenames are `plug_*_routines.wasm`.
- `cargo x check-matrix` + `[builds.<target>]` contracts, with `ERROR_TARGET_BOUNDARY`,
  `ERROR_TARGET_CONTRACT`, `ERROR_FEATURE_CONTRACT`, `ERROR_PLATFORM_FEATURE` enforced by
  `cargo x lint` (no compilation).
- Target dirs: shells and `x/agent-build.ts` append the worktree name to `CARGO_TARGET_DIR`;
  `cargo x doctor` flags in-tree or shared target dirs. Found the hard way: with one shared
  dir, `target/debug/xtask` was another worktree's binary while `cargo build -p xtask`
  reported "fresh", so guardrail output came from foreign code.
- `btress_sysadmin` is a single-crate workspace, not a guest member: cargo-leptos assumes
  packages live under their workspace root and failed `cargo leptos build` with "Could not
  remove base …/src/wasm_utils_rs from …/src/btress_sysadmin/Cargo.toml" (E2E sysadmin job).
  A standalone closure also keeps root feature unification out of its hydrate build. It gets
  its own `Cargo.lock` (third lockfile) and the guest profiles are copied into its manifest.
  `Repository::load` discovers any `src/*/Cargo.toml` with a `[workspace]` table instead of
  hardcoding the guest manifest.

---

### 0.2 PR handoff and verification (2026-10-20)

Ship the implemented guardrails, workspace contracts, and opt-in build tooling described above;
keep architectural cleanup and new guardrail features separate. Documentation checks enforce a
ratchet over recorded debt, not a claim that historical docs have all been repaired. Cross-worktree
sccache adoption is shelved (Q7); no patched binary, normalization wrapper, or `trim-paths`
configuration is included.

Verified in the pinned `ci-rust` shell from the isolated checkout, using its configured external,
worktree-scoped target. Build/test commands ran through `x/agent-build.ts` with FIFO jobserver
and cgroup limits; `cargo x doctor` confirmed the inherited jobserver and no Rust compiler wrapper:

- [x] `cargo fmt --all -- --check` in both root and guest workspaces.
- [x] `cargo clippy --locked --all-targets --all-features -p xtask -- -D warnings`.
- [x] `cargo nextest run --locked -p xtask` — 18 passed, 0 skipped.
- [x] `cargo x lint` — 0 diagnostics, 608 source modules inventoried.
- [x] `cargo x api --all --check` — all five boundary snapshots match.
- [x] `cargo x sql-gen --check --all` — all 12 discovered SQLite units match.
- [x] `cargo x doctor` — pinned toolchain and build configuration accepted.
- [x] `deno fmt --check x/agent-build.ts`, `deno lint x/agent-build.ts`,
      `deno check x/agent-build.ts`, and `bash -n x/load-dotenv-safe.sh`.

Not rerun in this handoff: the full workspace nextest suite and complete `check-matrix` builds.
Those are not implied by the checks above; their broader CI results still need review before merge.

### 0.3 CI failure fixes (2026-10-20, PR #70 run 39366765119)

- **pre-commit `fmt` hook**: the hook runs `cargo fmt -- <each .rs file>`, which reaches files
  outside every crate module tree; `cargo fmt --all` never visits those. With the new
  `imports_granularity`/`group_imports` options, three such files failed:
  `src/daybook_core/gen/mod.rs` (PR-created, hand-maintained), `src/daybook_core/index/hydrate.rs`
  and `src/utils_rs/cheapstr.rs` (both pre-existing orphans; `cheapstr.rs` is a dead duplicate
  of the live inline `mod cheapstr` in `utils_rs/lib.rs:243`). Formatted all three with the
  hook-equivalent command; repo-wide hook-equivalent check is clean. Deleting the two dead
  orphans is left to the operator.
- **E2E (sysadmin / chromium)**: `cargo leptos build --release` failed in cargo-leptos
  (`Could not remove base …/src/wasm_utils_rs from …/src/btress_sysadmin/Cargo.toml`) because
  the crate had joined the guest workspace, whose root is a sibling directory. Moved
  `btress_sysadmin` to a single-crate workspace (§0.1): explicit dep versions in its manifest,
  own `Cargo.lock` seeded from the guest lock, `Repository::load` generalized to discover all
  `src/` workspace roots, and target-dir consumers repointed (`daybook_cli/cmds/server.rs`,
  `x/dev-btress-static.ts`). Verified: `cargo metadata` workspace_root is the crate dir;
  `cargo x check-matrix --package btress_sysadmin` passes both target contracts; and the exact
  failing CI step `cargo leptos build --release` completes, emitting
  `src/btress_sysadmin/target/site/pkg/btress_sysadmin.js`.
- Also verified after the fixes: `cargo x lint` (0 diagnostics, 608 modules), xtask clippy
  `-D warnings`, nextest 18/18, guest workspace `cargo check --locked`, `daybook_cli` check,
  `cargo x api --all --check`, `cargo x doctor`, matrix `--dry-run`, and fmt checks across all
  three workspaces plus the repo-wide per-file hook form.

## 1. Decisions locked by the operator

- [x] Ship the quality-control tooling first; cleanup is a separate later PR.
- [x] Today's offenders get exclusion markers with a stated reason. No rewrites in PR 1.
- [x] Raw file size is *not* the metric. Count symbols / things you must understand.
- [x] Ignore Kotlin entirely.
- [x] Do **not** touch `docs/DEVDOC/log.md` (no split, no churn). Dropped from plan.
- [x] Do `rustfmt` `imports_granularity` **now**, in the tooling PR.
- [x] `[profile.dev.package."*"] debug = "line-tables-only"`: do it. Agents that need a
      real debugger opt back into full debuginfo.
- [x] Machine-wide jobserver: wanted.
- [x] Per-worktree cgroup limits (`systemd-run --user --scope`): wanted.
- [x] Cargo `feature-unification` (workspace resolver): enabled after separating the native
      and guest workspaces. `cargo x check-matrix` overrides it with `selected` to validate
      each target/feature contract in isolation; see §7.5.
- [x] Violations must carry a reason, and reasons come from a **standardized class
      vocabulary** in the spirit of the existing `utils_rs::expect_tags` `ERROR_*` strings.
- [x] Documentation standardization + QC is in scope for this effort.
- [x] `/tmp`-hardcoded-path and fixed-port findings are accepted as real problems.
- [x] **Guardrails live in `src/xtask/`**, not `tools/`. Demo commands (`keyhive-demo`,
      `keyhive-visibility`, `play`) and undue dependencies (`keyhive_*`, `subduction_*`,
      `beekem`, `daybook_types`, `oci-wasm`, etc.) are removed from `xtask` so `xtask`
      compiles fast (~58% smaller `cargo tree`). Future demos must use test modules with
      `[dev-dependencies]`.

---

## 2. Open questions — need an operator decision

- [ ] **Q1 File metric set.** Confirm the four metrics in §4.1 (authored definitions per
      logical module, methods-per-type / items-per-trait, longest fn, longest match arm,
      inline `#[cfg(test)]` size) and that we gate **only new violations** of a calibrated
      budget, with coupling metrics reported but not gated.
- [x] **Q2 Exemption location.** Implemented as (b): per-crate `guardrails.toml` with
      per-symbol exemptions. In-file markers were dropped: a marker edits the file it
      exempts, which is exactly the file two branches are already fighting over.
- [x] **Q3 Which crates pilot public-API snapshots.** Done for `daybook_types`,
      `daybook_ffi`, `daybook_pdk`, `wflow_sdk`, `big_sync_core` (`[api]` in their policies).
- [ ] **Q4 Pedantic clippy subset.** Approve a *measurement-only* pass on
      `utils_rs`, `big_sync_core`, `daybook_types` so we choose by real counts.
- [ ] **Q5 `clippy::unwrap_used`** (forces `expect(ERROR_*)`) — in or out? 301 non-test
      `.unwrap()` sites today.
- [x] **Q6 Disk layout.** Historical measurement: ntfs3 small-file create was ~16x slower
      than ext4 (§6), while ext4 root had only ~2.9 GB free. Target dirs stay on the repo FS
      (`CARGO_TARGET_DIR=../tmp/townframe-target` in `.env`) and are now scoped per worktree
      by the dev shells and `x/agent-build.ts`: with one shared directory,
      `<target>/debug/xtask` is whichever sibling worktree linked last, and a no-op
      `cargo build -p xtask` leaves that foreign binary in place, so `cargo x` runs another
      tree's guardrails. `cargo x doctor` flags an in-tree or shared target dir. sccache
      remains a shared compiler cache; cross-worktree Rust reuse is unproven (Q7).
- [x] **Q7 sccache feasibility probes** (§7.2) — complete; cross-worktree adoption is
      **shelved by the operator**. The full acceptance protocol is not complete. Setup is opt-in via
      `TOWNFRAME_SCCACHE=1` in `.env` or `x/agent-build.ts --sccache`; Nix `ci-rust` includes
      the pinned `sccache` binary, `.env.example` shows `SCCACHE_DIR`/size/base dirs, and the
      wrapper sets `RUSTC_WRAPPER`/`CARGO_INCREMENTAL=0`.

      Benchmark 2026-10-19, `utils_rs` + `sqlite_rs`, separate target dirs, one shared cache:
      - Cold A: 176 Rust misses, 23s, successful.
      - Identical B from a different source path: 0/176 Rust hits, only 5 assembler/C++ hits,
        22s. `SCCACHE_BASEDIRS` did not normalize this Rust closure; `SCCACHE_DIRECT=false`
        showed the same result.
      - Rebuilding the same B path: 176/176 Rust hits, 12s.
      - Two warm copies concurrently: 350/352 Rust hits, 99.45% overall, 19–20s, two misses.

      Conclusion: keep sccache opt-in. It is excellent for repeated builds and warm concurrent
      jobs, but stock sccache 0.18.0 has no Rust `SCCACHE_BASEDIRS` support, so cross-worktree
      reuse needs Cargo-side artifact path normalization and eventually upstream key work. The
      first attempt also exhausted the filesystem before this smaller protocol, so disk budget
      remains part of the acceptance criterion.

      Follow-up research (2026-10-19): cross-worktree Rust hits are an active upstream problem,
      not a local misconfiguration. Stock sccache only applies basedirs to C/C++; Rust hashes
      `cwd`, absolute source paths, `--extern`/`-L` paths, linker paths, and path-valued Cargo
      env deps. Upstream work is split across mozilla/sccache#2652, #2813, #2814, #2882 and
      Cargo's stabilized 1.101 `trim-paths` profile. Reports:
      - a real 7-crate experiment went 0% → 98.6% cross-worktree hits with basedirs, per-token
        normalization, and `RUSTFLAGS=--remap-path-prefix=<root>=/proj` (johnkferguson/sccache#1);
      - a Bevy/Windows run with patched sccache got 365/365 after request-scoped basedirs fixed
        `env-dep:OUT_DIR` and `CARGO_MANIFEST_DIR` keys (issue #2870);
      - Firefox got 705/712 with `trim-paths = "all"` plus patched sccache (#2813/#2814/#2882)
        and linker-path normalization (issue #2652, 2026-10-19 comment);
      - `moriyoshi/sccache-wrapper` wraps sccache with workspace-path normalization, hardlink
        restore, singleflight, and output replay.

       Stock `trim-paths` probe: nightly 1.100, sccache 0.18.0, two disposable source copies,
       separate targets, and one fresh 4 GiB cache. `trim-paths = "all"` plus `-Ztrim-paths`
       produced 175 Rust misses in A and 0/175 Rust hits in B; rebuilding B produced 175/175
       hits, and two warmed copies produced 350/350 hits concurrently. The profile setting
       did not recover cross-worktree Rust reuse, so it is not enabled here. It can also remap
       compiler diagnostics.

       Patched probe: the `tcashel/sccache` `codex/request-scoped-basedirs` branch for upstream
       PR #2814 (stacked on #2813), built as sccache 0.18.0, produced 116/175 Rust hits in B
       (66.3%, 59 misses). Cold A took 33.54s; cross-copy B took 30.96s; a same-path repeat
       took 16.70s with 175/175 hits. Two warmed copies took 29.40s each with 350/350 Rust
       hits combined. The daemon reported `Base directories (none)`; request-scoped basedirs
       were present, but the remaining miss keys were not diagnosed. This is a feasibility
       result, not a correctness or throughput acceptance result. The probe used a branch
       archive, not an immutable source pin, and no patched binary was adopted.

       Shelved on 2026-10-20: retain the existing stock opt-in, but do not enable `trim-paths`,
       patch the Nix package, or add another wrapper for this PR. Before resuming adoption,
       pin the exact upstream source; explain remaining misses; complete implementation-edit,
       generated-code, compile-time-path, link-time, and resource checks in §7.2. The separate
       `moriyoshi/sccache-wrapper` has not been benchmarked.

- [x] **Q8 `Cargo.lock` / generated `gen.rs` conflict policy.** `.gitattributes` marks
      `src/**/sqlite/gen.rs` and `src/**/public-api.txt` `linguist-generated`; CONTRIBUTING
      documents regeneration commands and resolving dependency intent for both tracked
      lockfiles. Automated shared-dependency divergence checking remains open in §7.5.
- [ ] **Q9 Migration numbering.** Two branches each adding `005_` collide in both the
      `.sql` file *and* checked-in `gen.rs`. Switch to date-prefixed or UUID-suffixed
      migration names? (Never silently renumber applied history.)
- [ ] **Q10 ADR numbering.** `ERROR_ADR_ID` already rejects new duplicate identities;
      the two existing `007-*.md` files are tracked as legacy debt in `docs/guardrails.toml`.
      Naming policy and resolution of those historical identities still need a decision.
- [ ] **Q11 Branch-interference coordinator.** `cargo x overlap --base <trunk>` landed
      (symbol / migration-stream / `public-api.txt` overlap across sibling worktrees).
      The advisory-claims coordinator is deferred.
- [x] **Q12 Validation receipts** — `cargo x record --output target/… -- <cmd>` and
      `cargo x verify <receipt>` landed (source fingerprint before/after, toolchain identity,
      flag env). Not yet consumed by anything; CI integration is a later item.
- [ ] **Q13 `utils_rs::prelude` third-party re-exports.** Confirmed escape route
      (§6). Do we (a) stop re-exporting `tokio`/`dashmap`/`futures`/`regex`/`bs58`,
      (b) keep them but exempt `utils_rs` from the "no external types in public API"
      rule, or (c) leave as-is and only *report*?

---

## 3. Ideas parked / rejected so far

- Shelved by the operator (2026-10-20): cross-worktree sccache adoption beyond the existing
  stock opt-in. Q7 records completed feasibility probes and the remaining acceptance work.
- Rejected: raw line-count gate on files (operator).
- Rejected: splitting `docs/DEVDOC/log.md` into one file per entry (operator: don't touch).
- Rejected (for now): one shared workspace-wide baseline TOML — it becomes the conflict
  magnet it was meant to prevent. Prefer crate-local config.
- Deferred: Rewind VM as a CI dependency. Worth a *bounded experiment only*. Its documented
  constraints (KVM, 1 vCPU, loopback-only networking, CPU-vendor replay restrictions) make
  it unsuitable as a blanket CI gate.
- Noted, not yet acted on: Nix pins inputs; it does **not** make thread scheduling
  deterministic. A seed reproduces selected randomness, not an arbitrary Tokio schedule.
  A nextest recording is not a deterministic execution trace. Do not conflate these in docs.
- Noted: an AST matcher cannot reliably identify every *aliased* async mutex, and cannot
  prove cancellation safety. The async-mutex ban must steer ownership redesign, not
  mechanical swap to a blocking mutex.
- Noted: existing Loom tests model a *separate* `LoomQueue` implementation
  (`src/daybook_core/repos.rs:619`) rather than production transition logic — drift risk.
  Guardrail opportunity: test production logic where practical.

---

## 4. Measurement: "how much must you understand to change this"

### 4.1 Metrics to collect

Built on `syn` (already a workspace dep). Parses without compiling, so it runs in
milliseconds on changed files and works even when the branch does not build.

| Metric | Catches |
|---|---|
| Authored definitions per logical module | modules accumulating unrelated responsibilities |
| Methods per type / items per trait | moving a god-module into a god-object |
| Exported API items per crate | crate extraction that makes every internal detail public |
| Dependency fan-out and reverse fan-out | components that know too much / make every change expensive |
| Longest fn body | 464-line dispatch fns |
| Longest match-arm body | forces `Cmd::Foo(a) => self.on_foo(a)`, which is also a conflict fix |
| Inline `#[cfg(test)]` module size | test edits and prod edits colliding in one file |

Counting rules:
- Imports, comments and derive-generated boilerplate do **not** count as authored definitions.
- Tests, generated code, declarative schemas and implementation modules get **different policies**.
- An inline `mod` must not evade counting just because it has no file of its own.
- A single count invites gaming (shove everything into one giant `impl`, or invent tiny
  forwarding modules). Hence several *distinct* metrics.

### 4.2 Gating policy

- Initially gate **only new violations** of a calibrated symbol budget.
- **Report** coupling metrics without pretending they prove bad design.
- Exemptions must be: specific rule + specific module/symbol + a reason from the standard
  class vocabulary (§9.4).
- Distinguish *legitimate* exceptions (generated bindings) from *temporary* debt.
- **Detect stale exceptions** — an exemption whose condition no longer holds is an error.
  Same self-expiring idea as `#[expect]` / `unfulfilled_lint_expectations`.
- Record current offenders **without rewriting them**.
- Adding an exception must be an explicit policy change, not an automatic "bless everything".
  `--bless` may only *lower* ceilings, never raise them.
- Config/baselines stay **crate-local**, not one giant shared file every agent edits (Q2).

### 4.3 Measured today (evidence, not baseline)

Important caveat from the other agent's review: **earlier grep counts included inline test
code.** These are investigation leads, not a suitable baseline.

File-shape outliers:

| File | Lines | Top-level items | Notes |
|---|---|---|---|
| `src/big_sync_core/lib.rs` | 5828 | 32 | 118 fns; **2163-line inline `mod tests`**; 20 sibling modules it could move into |
| `src/big_sync/part_store/host_contract.rs` | 4852 | 64 | 6 async-mutex sites |
| `src/big_repo/runtime2/hub.rs` | 4341 | 36 | `handle_cmd` **464 lines**, `handle_evt` 435; `Runtime2Cmd` has **33 variants** |
| `src/big_repo/test.rs` | 3742 | 85 | |
| `src/big_repo/store/sqlite/tests.rs` | 3588 | 88 | |
| `src/daybook_core/rt.rs` | 3093 | — | |
| `src/big_repo/sqlite/gen.rs` | 839 | **246** | generated — exempt by `@generated`, and the count is harmless |

- 63 `.rs` files > 1000 lines; 34 > 1500 lines.
- 10 files have inline test modules > 700 lines: `big_sync_core/lib.rs` (2163),
  `big_sync/part_store/sqlite.rs` (1571), `big_sync/rpc.rs` (1536),
  `daybook_core/blobs/pin_worker.rs` (1141),   `big_repo/runtime2/keyhive_access_stream.rs` (889),
  `daybook_core/blobs/permission_writer.rs` (872), `am_utils_rs/codecs/json.rs` (791),
  `big_repo/runtime2/doc_worker.rs` (750), `daybook_core/index/doc_delta_store.rs` (716),
  `daybook_core/blobs/sync.rs` (716).
- Crate sizes: `daybook_core` 63k LOC, `big_repo` 59k, `big_sync` 26k, `big_sync_core` 16k.
  These are the atomization targets.
- `mod` declaration counts (files with >15): `daybook_core/lib.rs` 20,
  `big_repo/runtime2/mod.rs` 20, `big_sync_core/lib.rs` 19, `big_repo/test2/mod.rs` 18,
  `big_repo/lib.rs` 16.

---

## 5. Collaboration: many agents, many branches, one repo

### 5.1 Textual conflict magnets (measured)

- [ ] **ADR numbering already collided**: two `007-*.md` files exist (`docs/adrs/`).
- [ ] **Migration numbers** are contiguous `NNN_` per directory *and* embedded into the
      checked-in `gen.rs`, so two branches each adding `005_` conflict in two places.
- [ ] **Append-only shared files**: `docs/DEVDOC/log.md` (2050 lines — operator says do not
      touch), `docs/DEVDOC/todo.md`, `AGENTS.md`, `[workspace.dependencies]` (108 entries),
      `Cargo.lock`.
- [ ] **`AGENTS.md` → index + topic files** under `.agents/rules/`, so agents edit
      different files.
- [ ] **Dispatch hubs**: every new hub feature edits the same enum + the same 464-line
      match + `messages.rs`. The longest-match-arm cap turns "new command" into
      "new file + two one-line additions".
- [ ] **`.gitattributes`** currently only marks the Kotlin uniffi output as
      `linguist-generated`. Add merge policy for generated Rust (`gen.rs`,
      `keyed_frontier/sqlite_generic.rs`) and `Cargo.lock`: **regenerate, never hand-merge**.
      Ship a `cargo x resolve-generated` to do it.
- [ ] **`imports_granularity = "Item"` + `group_imports`** in `rustfmt.toml` (nightly
      options; we are on nightly) so new imports land on their own lines.
      Operator approved doing this **now**. One-time whole-repo diff; land at a quiet
      moment; in-flight branches resolve by re-running fmt.
- [ ] **TOML sorting** (`taplo` or `cargo sort`) so new deps land predictably.
- [ ] **Mergiraf** as the documented syntax-aware resolver. jj ships a config for
      `jj resolve --tool mergiraf` out of the box. Handles independent additions to the
      same block (imports, struct fields). *Unverified:* which Rust constructs it treats
      as order-independent. Do not use `mergiraf solve` interactively under jj (marker
      format differs); use `jj resolve --tool mergiraf`.
- [ ] **Re-runnable refactors**: a mechanical rename is committed as an ast-grep rule or
      xtask codemod. On conflict an agent drops that side and re-runs the codemod instead
      of hand-resolving 50 files.
- [ ] **Duplicate-number / duplicate-name lint** for ADRs and migrations.

### 5.2 Semantic conflicts (both branches pass; the merge does not build)

- [ ] **GitHub merge queue** so CI runs on the merged result before landing.
- [ ] **Public-API snapshot diff in review** surfaces the risky case: a PR that changes
      crate X's `public-api.txt` *and* consumes that change in crate Y. Soft check:
      suggest landing the interface change first.
- [ ] **Contract-first for cross-crate work**: agree the contract, then parallelize the
      implementations. Intentional breaking changes stay welcome — they become
      *coordinated* changes rather than surprises.

### 5.3 Detect interference *before* it becomes a conflict

Two agents can touch entirely different files and still collide:
one redefines when a completion means "durable"; both add the next migration number;
one changes a shared enum's serialization; both add slightly different abstractions for
the same responsibility; one flips a default feature and silently changes the other's build.

- [ ] **Change declaration per task** (initially advisory): components to modify, public
      contracts to change, shared resources affected (migration streams, wire formats,
      dependency pins), prerequisite changes. Compare against the actual diff and against
      other active tasks.
- [ ] **Local advisory coordinator** maintaining claims *outside the source tree*.
      Claims must expire; they must never lock files against legitimate work.
      Explicitly **not** a distributed orchestration project.
- [ ] **`cargo x overlap`**: all worktrees are local, so diff every live branch against
      trunk at the *symbol* level (via `syn`) and report which branches touch the same
      items. Agents run it at task start and before opening a PR. Avoids shared claim
      files, which would themselves conflict.
- [ ] **Speculative integration**: for overlapping changes, combine them in a disposable
      workspace and run the affected checks before either branch is declared finished.
- [ ] Note: public-API fingerprints identify interference; they do **not** prove semantic
      compatibility.

### 5.4 Crate extraction needs architecture checks, not just API snapshots

A crate boundary controls *visibility*, not dependency direction or responsibility.
An agent can preserve compilation while destroying the boundary by exporting internals,
depending on the implementation crate, or dumping another abstraction into `utils_rs`.

- [ ] **Kill the confirmed escape route**: `utils_rs::prelude` publicly re-exports `tokio`,
      `dashmap`, `futures`, `bs58`, `displaydoc`, `dotenv_flow`, `educe`, `regex`,
      `serde_json` (`src/utils_rs/lib.rs:9`). This conceals architectural coupling from a
      direct-dependency audit. See Q13.
- [ ] **Per-crate architectural metadata**: layer/role, allowed workspace deps,
      supported feature/target configs, whether it may do network/filesystem/DB/task-spawn,
      whether external library types may appear in its public interface.
- [ ] **Layer file** (`tools/lint/layers.toml`) enforced against `cargo metadata`.
      Stops upward or sideways deps. Use **guppy** for graph queries — it already provides
      package and feature graph queries; we need policy and reporting around it, not a
      homemade resolver.
- [ ] **`rust.unreachable_pub`** so `pub` actually means public.
- [ ] **Ban `pub use x::*` re-exports** (4 today). Without the ban, a snapshot changes
      when an upstream crate changes, hiding provenance.
- [ ] **Optional `*_api` crates** for the busiest boundaries: implementation churn then
      does not rebuild dependents, and the snapshot covers exactly the contract.
- [ ] Keep orchestration crates broad; keep domain crates narrow. **Do not split into tiny
      crates merely to improve a score.**
- [ ] **Dependency graph as an agent query**: "I'm changing crate X — who consumes it,
      what boundaries am I crossing, what must I validate?" Same graph powers the
      affected-check planner (§5.5).
- [ ] **Independent-crate builds under supported features**: workspace feature
      unification conceals missing feature declarations. An extracted crate must build alone.
- [ ] **Deterministic, component-local generators**: two agents regenerating unrelated
      interfaces must not rewrite the same aggregate output.

### 5.5 Affected-check planning + validation receipts

"Tests passed" in a handoff message is not enough between agents.

- [ ] **Machine-readable validation receipt** containing: source-tree fingerprint
      (including uncommitted inputs), compiler + dependency locks + feature set + target,
      exact checks and test selections, outcomes, artifact locations.
- [ ] A later edit **invalidates** the receipt. After integration, checks run against the
      combined tree — not merely against each original branch.
- [ ] **Two lanes**:
      - *Fast local feedback*: syntax/structure, dependency policy, changed-crate clippy,
        focused tests.
      - *Integration validation*: affected reverse dependents, contract tests, supported
        configurations.
- [ ] An unchanged API does **not** justify skipping consumer tests — behavior can change
      without signatures changing. Changes to shared build config, generators, or
      dependency resolution must **conservatively expand** the selection.
- [ ] **The fast checker must not sit behind xtask's dependency graph.** Confirmed:
      `src/xtask/Cargo.toml` pulls `keyhive_core`, `subduction_core`, `subduction_crypto`,
      `subduction_keyhive`, `beekem`, `sqlite_rs`, `rusqlite`, `daybook_types`.
      The tool that explains why your branch is broken must still run when your branch
      does not compile. Ship it as a **small independent crate, packaged through Nix**.

### 5.6 Shared state between parallel runs

- [ ] `src/pauperfuse/lib.rs` tests write to a fixed `/tmp/test-pauperfuse` (8+ sites).
- [ ] Fixed ports in dev/server code: 8071, 3000, 3001, 4433
      (`daybook_cli/cmds/server.rs`, `btress_sysadmin/server.rs`, `btress_sysadmin/app.rs`).
- [ ] `xtask/main.rs` reads fixed `/tmp/photo_2025-*.jpg` paths (3 sites).
- [ ] Lint: `"/tmp/…"` literals and non-zero port literals in tests.
- [ ] Per-worktree port base in the `x/` scripts.
- [ ] nextest `test-groups` for genuinely exclusive resources — but its own docs say the
      *correct* fix is bind-to-port-0 (or a Unix socket in a temp dir), and that groups
      make runs slower. Prefer port 0 / UDS; use groups only where impossible.
- [ ] nextest `threads-required` for heavy tests (`num-cpus`, `num-test-threads` weights)
      — relevant to the stress tests.
- [ ] **No "cleanup" that kills another agent's services** (AGENTS.md already warns about
      stale PIDs/ports; make it enforceable).

### 5.7 Make failures transferable between agents

The most useful idea taken from Zakaria's work: **a failure should be a portable artifact,
not a story about a run.**

- [ ] **Reproduction bundle** retaining: exact source and dependency inputs, test binaries
      + required runtime closure, generated WIT/plugin artifacts or their reproducible
      identities, explicit test inputs and resolved random seeds, allowlisted env/logs/
      debug info, and a replay command.
- [ ] **nextest archives** are a useful building block but do **not** include project
      source automatically. Nix supplies pinned tools and runtime deps.
- [ ] **Repro capsule line**: a failing test prints one line (commit, seed, flake.lock
      hash, exact command) — like `rewind show`. Agents pass that line, not prose.
      `TEST_SEED` + `NEXTEST_EXPERIMENTAL_RECORD` get us most of the way.
- [ ] **`sometimes!("label", cond)` reachability assertions** in stress tests: a stress
      test that never entered its race window proves nothing. Wire into the existing
      `TASK_CENSUS_FILE` stream (`src/utils_rs/census.rs`, `x/census-report.ts`).
      Fail when a declared window was never reached across the seeded runs.
- [ ] Pair with `tokio::runtime::Builder::rng_seed(TEST_SEED)` so `select!` ordering is
      replayable. (`TEST_SEED` already exists: `src/utils_rs/testing.rs:123`.)
- [ ] **Concurrency reachability assertions** generally: require evidence a test exercised
      overlapping operations or cancellation — not merely that several tasks were spawned.

### 5.8 Flake policy

- [ ] `.config/nextest.toml` has `flaky-result = "fail"` **commented out**, and the CI
      profile retries 3x. A flake is therefore invisible green.
- [ ] Nightly cron: retries off + `flaky-result = "fail"` + file the failing seed.
      PR runs keep retries.
- [ ] 47 `sleep(` in test files, 32 `tokio::time::sleep` in prod code, 24 `timeout(` in
      test files — timing waits are the flake source.
- [ ] For small algorithms, deterministic scheduling / model tests may be cheaper than
      Rewind. See the Loom drift note in §3.

---

## 6. Verified findings (established this session, with method)

### 6.1 `RUSTFLAGS` drift — a real, silent bug

Cargo's precedence (from the Cargo Book) is:
`CARGO_ENCODED_RUSTFLAGS` > `RUSTFLAGS` > `target.<triple>.rustflags` + `target.<cfg>.rustflags`
> `build.rustflags`.

- `.cargo/config.toml` sets `build.rustflags = ["--cfg", "tokio_unstable"]`.
- `flake.nix:103` sets `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS` (mold linker).
  That is `target.<triple>.rustflags`, so inside the devshell it **replaces**
  `tokio_unstable` entirely.
- `src/xtask/main.rs:432` (`append_tokio_unstable_rustflags`) builds a **third**
  combination via `CARGO_ENCODED_RUSTFLAGS`.
- pre-commit's `clippy --locked --all-features --all-targets -- --deny warnings` likely a
  **fourth** (*unverified*).
- Consequence: builds inside vs outside the devshell use different flags, so every switch
  rebuilds the world; and `tokio_unstable` (which the census/tracing instrumentation
  depends on) may be silently absent in some paths.
- [ ] **Fix**: one place, `target.<triple>.rustflags`, carrying both the linker flag and
      `--cfg tokio_unstable`. Replace `-- --deny warnings` with `build.warnings = "deny"`
      (a newer Cargo setting designed to avoid that rebuild). Add `cargo x doctor` that
      asserts the *effective* flags match across entry points.

### 6.2 ntfs3 vs ext4 — measured on this machine

`findmnt`: repo lives on `/dev/sdb2` = **ntfs3**, 44 GB avail of 882 GB.
Home/root `/dev/nvme0n1p5` = **ext4**, **2.9 GB free of 199 GB (99% used)**.

Benchmark: create 3000 small files, then read them all back.

| Filesystem | create 3000 | read all |
|---|---|---|
| ntfs3 (repo disk) | **3875 ms** | 79 ms |
| ext4 (home) | 236 ms | 56 ms |
| tmpfs | 261 ms | 58 ms |

**ntfs3 small-file creation is ~16x slower.** A Cargo target dir is hundreds of thousands
of small files — this is plausibly a large share of our build time, and likely why
`x/disk-watch.ts` and `x/clean-rust.ts` exist.

Answering "does NTFS block us?": **No, not for the primitives we need.** Verified on the
ntfs3 mount: `mkfifo` OK, Unix domain socket `bind` OK, symlink OK. So the jobserver FIFO,
sccache's IPC socket and cgroups all work there. cgroups are a kernel resource and are
filesystem-independent. The cost is **throughput**, not capability. See Q6 / §7.4.

Root-disk consumers: `/nix` 61 GB, `/var` 7 GB, `/tmp` 4.2 GB (of 186 GB used).

### 6.3 Repo hygiene counts (today's tree)

| Signal | Count |
|---|---|
| `tokio::sync::{Mutex,RwLock}` / `async_lock` sites | 76 (~65 non-test) |
| `dashmap` / `DHashMap` files | 8 |
| `#[expect]` / `#[allow]` | 81 / 13 |
| raw `tokio::spawn` | 59 |
| `.unwrap()` non-test | 301 |
| `.send(..).ok()` (ignores send errors) | 22 |
| `if let Err(e) = .. { warn!/error! }` (catch-and-log) | 26 |
| `timeout(` in test files | 24 |
| `#[ignore]` | 8 |
| `run_until_cancelled` (misuse per todo.md) | 7 |
| `println!/eprintln!` outside cli/xtask/tests | 18 |
| `XXX/FIXME/TODO` comments | 75 |
| `pub use ..::*` glob re-exports | 4 |
| `unsafe` | 4 |
| `mem::take` (some could be `drain()`, per todo.md) | 30 |
| zizmor findings (hook parked at `stages: [manual]`) | 122 |
| `tokio::time::sleep` prod / tests | 32 / 47 |

`utils_rs::expect_tags` already exists with **239 usages** — the precedent for the
standardized `ERROR_*` vocabulary (§9.4). It has two typos to fix while we're in there:
`ERROR_UNRECONIZED` and `"poisioned mutex"`.
`DHashMap` carries a `NOTE: only use these in actors or single writer scenarios`, so the
dashmap rule must **steer**, not blanket-ban.

### 6.4 Migration identity is path-derived — confirmed risk for atomization

`src/xtask/sql.rs:149` (`migration_table`) derives the persisted migration-history table
name from the source directory: `src/daybook_core/blobs` → `_daybook_core_blobs_migrations`.
**Moving a module into another crate changes a persisted identity without changing any
Rust API.** During the `daybook_core` atomization this silently orphans applied-migration
history.

- [ ] Guardrail: flag any change that alters a derived migration table name. Must be a
      hard error in the extraction PRs.
- [ ] Decide whether to pin explicit table names instead of deriving them.

### 6.5 Existing `expect_tags` classes

`ERROR_CANCELLED`, `ERROR_IMPOSSIBLE`, `ERROR_CHANNEL`, `ERROR_JSON`, `ERROR_UTF8`,
`ERROR_MUTEX`, `ERROR_PARSE`, `ERROR_ACTOR`, `ERROR_TOKIO`, `ERROR_CALLER`,
`ERROR_INVALID_PATCH`, `ERROR_UNRECONIZED`. Comments already distinguish `ERROR_ACTOR`
(actor/event-loop task) from `ERROR_CHANNEL` (oneshot / bespoke orchestration).

---

## 7. Build speed across worktrees

### 7.1 Answers to the operator's direct questions

**"Does sccache copy files into the target dir too?"** — Yes. sccache is a `RUSTC_WRAPPER`;
Cargo still owns the target dir and still requires the artifact there (`--out-dir` is
mandatory and `--emit link` must be present, per sccache's Rust docs). On a cache hit
sccache writes the cached artifact into Cargo's out-dir. Its own cache is a **separate**
store (`SCCACHE_DIR`, default `~/.cache/sccache`, default size 10 GB). **Disk cost is
additive**, not replacing.

**"Any way to have them hardlinked to save space?"** — **No.** Verified three ways:
the full `Configuration.md` (only `dir`, `size`, and preprocessor-cache-mode options under
`[cache.disk]`), `Local.md`, and `src/cache/disk.rs`, which stores blobs via
`write_all`. There is no hardlink/reflink option. Note also that reflink copies need
btrfs/xfs; neither ext4 nor ntfs3 would give them to us for free.
=> The lever is **size caps + placement**, not hardlinks.

**"Does nextest support jobservers?"** — Not found. Jobserver is a *build-time* protocol
(Cargo/rustc/make). nextest controls its own test parallelism via `-j`/`--test-threads`,
plus `threads-required` weights and `test-groups` semaphores. Treat test parallelism as a
separate knob from the build jobserver. (Searched nextest docs + repo tree; no jobserver
support surfaced. Low confidence it's absent by design vs. simply undocumented — but the
practical answer stands: configure `-j`/`threads-required`, don't expect jobserver reuse.)

**"Feature unification is a thing?"** — Yes: `resolver.feature-unification` (Cargo config),
plus nightly `-Z` variants. Stops dependencies rebuilding when an agent alternates between
`-p A` and `-p B`. Verify the exact accepted values/flags against our pinned toolchain
before enabling. Alternative/complement: `cargo-hakari` (workspace-hack crate) — but
**measure first**, especially across native vs WASI targets.

### 7.2 sccache: benchmark before adopting

Do **not** share one writable target dir among all worktrees as the primary solution —
Cargo locking and artifact churn turn supposed parallelism into waiting. Per-worktree
target dirs stay.

Known caveats (from sccache's Rust docs, verified):
- Incremental compilation **must be disabled** for cacheable invocations. Cross-worktree
  reuse improves; repeated edits in one worktree get slower. **Measure both.**
- Crates invoking the system linker are **not cached**: `bin`, `dylib`, `cdylib`,
  `proc-macro`. Do not expect every build stage to disappear.
- `env!` values need Rust >= 1.46 to be tracked; proc macros that read the filesystem may
  not cache properly.
- `SCCACHE_BASEDIRS` exists and is documented for exactly our case (hits across worktrees;
  longest-prefix match, absolute paths only, case-sensitive on Linux). But it is **not a
  proven turnkey Rust worktree solution** — Rust path normalization work is still open
  upstream (sccache PR #2794), and compile-time paths / generated output complicate
  correctness.
- Equal compiler *version strings* are insufficient: compiler/sysroot **paths**
  participate in hashing. Check the effective identities produced by the separate toolchain
  definitions at `flake.nix:124` and `flake.nix:156`.

Acceptance benchmark (Q7) — record wall time, cache misses **and miss reasons**, link time,
memory and disk, not just hit percentage:
1. Build in worktree A.
2. Build identical sources in B (expect hits).
3. Make one implementation-only edit in B (expect: only that crate recompiles).
4. Repeat edits in B (watch the incremental-disabled penalty).
5. Run A and B concurrently (watch contention).
6. Plus generated-code and compile-time-path correctness checks before adopting any
   normalization patch.

Arrangement to use if it passes: Nix-pinned sccache + consistent compiler identity;
shared host/user cache; each worktree keeps its own Cargo output dir; host-wide limits on
expensive builds/tests; explicit cache and disk budgets.

### 7.3 Machine-wide resource limits

16 logical cores, **14 GB RAM**. `flake.nix:101` gives every shell `CARGO_BUILD_JOBS=8`,
so eight agents can request 64 compiler jobs before tests. **A machine-wide build
admission controller may buy as much as caching.**

- [x] **FIFO jobserver wrapper**: `x/agent-build.ts` creates/reuses the host-wide FIFO and
      passes `MAKEFLAGS=--jobserver-auth=fifo:<path>` to its child. It defaults to
      `$XDG_RUNTIME_DIR/townframe-jobserver.fifo` (or a uid-scoped `/tmp` path).
      This is wrapper opt-in, not an automatic devshell-wide cap; builds that bypass the
      wrapper do not participate.
- [x] **Per-worktree cgroups**: the wrapper uses
      `systemd-run --user --scope` with `MemoryHigh=6G` and `MemoryMax=10G` by default when
      `systemd-run` is available; both limits are configurable. `--no-cgroup` opts out.
- [x] **`[profile.dev.package."*"] debug = "line-tables-only"`** — implemented in
      `.cargo/config.toml`. Agents needing full debuginfo opt back in.
- [ ] Reconcile with `x/disk-watch.ts` (5s loop, cleans below 4 GiB free, deletes
      `<target>/debug` if it can't recover to 10 GiB).

### 7.4 Disk placement (Q6)

- Historical measurement: ext4 root was **99% full (2.9 GB)** and is where sccache would
  default to (`~/.cache/sccache`, 10 GB default) — that alone would fill the disk.
- [x] Explicit cache location and budget: the stock opt-in configures `SCCACHE_DIR` and
      defaults `SCCACHE_CACHE_SIZE` to `8G`; `.env.example` documents overrides.
- ntfs3 has 44 GB but ~16x slower small-file create.
- Candidate splits:
  - (a) sccache cache on ntfs3 (fewer, larger blobs → the small-file penalty matters less);
        target dirs stay where they are.
  - (b) free the ext4 root first (61 GB `/nix`, 4.2 GB `/tmp`, 7 GB `/var`), then move
        target dirs + sccache there.
  - (c) accept ntfs3 for target dirs and reduce file *count* instead
        (`line-tables-only`, `split-debuginfo=unpacked` already set).
- Bends the AGENTS.md "never specify your own custom target dir" rule → operator call.

---

### 7.5 `feature-unification` — enabled after the wasm split

`resolver.feature-unification = "workspace"` resolves features as if the whole workspace were
being built, regardless of `-p`. It used to be blocked by the wasm members: with the plugs and
`test_wflows` in the root workspace, `cargo check -p plug_test --target wasm32-wasip2
--unit-graph` showed `utils_rs` gaining `native`/`downloader` and tokio gaining `full`, and
the build then failed in `utils_rs/downloader.rs` (`reqwest`'s wasm futures are not `Send`).

The workspace split removed the blocker, so it is now committed in `.cargo/config.toml`
(with the `[unstable] feature-unification = true` gate nightly needs; stable warns
`ignoring resolver.feature-unification without -Zfeature-unification` and keeps `selected`).
Evidence after enabling: `cargo check -p sqlite_rs --unit-graph` resolves `utils_rs` to
`default,hash` (no native-only features exist any more), and probes of `sqlite_rs`,
`daybook_pdk`, `wflow`, `big_sync_core` show no dev-only feature leaking into `kind=None`
units under unification versus `selected` (`big_sync_core/test-support` is on in both because
it is in that crate's own `default`). `cargo x check-matrix` pins
`--config resolver.feature-unification="selected"` per configuration so contracts are checked
in isolation rather than against the unified closure.

Cost found while enabling it: unification applies to every member of the workspace being
invoked, so per-crate wasm cross-checks from the root workspace now fail —
`daybook_types --target wasm32-wasip2 --no-default-features --features wit` dies in
`uniffi_core` and `utils_rs --target wasm32-unknown-unknown` dies in `mio`, because
`daybook_core` enables `daybook_types/uniffi` and tokio's `net`. Guest builds are unaffected
(they resolve inside `src/wasm_utils_rs`, whose members are all wasm; verified by rebuilding
`plug_test_routines` for wasip2 with unification active), and `cargo x check-matrix` passes
`--config resolver.feature-unification="selected"` so contracts are checked in isolation. If
that pin ever feels wrong, the alternative is dropping unification and accepting `-p` churn.

Still open:
- [ ] `cargo-hakari` measurement remains unrun; not needed while the guest workspace is small.
- [ ] The three lockfiles (root, `src/wasm_utils_rs`, `src/btress_sysadmin`) can drift.
      Seeded from the root lock, 40 of 341 shared crates resolve to
      different versions, and 6 of those are versions the root lock does not contain
      (`const-str`, `convert_case`, `derive-where`, `prettyplease`, `toml`, `tower-http` — all
      in the Leptos-only subtree). A `ERROR_LOCK_DIVERGENCE` check should require every crate
      reachable from a *shared* path dependency to resolve identically in both locks.

## 8. Documentation standardization and QC

New area raised by the operator. Docs are an agent's primary interface to the codebase, so
undisciplined docs cost more here than in a human-only repo: an agent that trusts a stale
doc burns a whole session.

### 8.1 Doc surface in the repo today

- `AGENTS.md` (root, monolithic) + `README.md` + `CONTRIBUTING.md`.
- `docs/adrs/` — 13 files, **two collide at `007`**.
- `docs/DEVDOC/` — `design.md`, `log.md` (2050 lines, do not touch), `prompts.md`,
  `todo.md` (324 lines).
- `docs/wiki/` — `big_sync.md`, `dict.md` (glossary), `README.md`.
- `docs/scratch/` — 13 handoff/audit notes (this file is the 14th).
- `.agents/skills/` — 4 playbooks (`stress-sync-investigation`,
  `fork-pinning-and-upstream-sync`, `rr-debugging`, `message-ordering-audit`).
- `.coderabbit.yaml` excludes `docs/DEVDOC/**` from review — i.e. our most-churned prose is
  the least reviewed.

### 8.2 Doc QC — implemented checks and remaining proposals

- [ ] **Every crate has a crate-level `//!` doc**; every `pub` item has a doc comment.
      `rust.missing_docs` at warn, ratcheted per crate (same exemption machinery as §4).
- [ ] `clippy::missing_errors_doc`, `missing_panics_doc`, `missing_safety_doc` — the panic
      doc one pairs directly with the `expect_tags` work (§9.4).
- [ ] `clippy::doc_markdown`, `doc_lazy_continuation`, `empty_docs`, `doc_link_with_quotes`.
- [ ] **Mechanism-at-the-site rule** (already in AGENTS.md): lint doc comments whose entire
      body is a link to an ADR. AGENTS.md explicitly says a pointer to an ADR is not a
      substitute for the sentence a reader needs.
- [ ] **Progress-comment ban** (AGENTS.md): ast-grep regex over comments for
      `// Now we…`, `// Step 2:`, `// First, …`, `// Finally, …`. Also covers todo.md's
      "Triple X comment detector" item (75 `XXX/FIXME/TODO` today).
- [ ] **Doctests are already run in CI** (`cargo llvm-cov --doc`). Keep; consider requiring
      a doctest for every new `pub fn` in the pilot snapshot crates.
- [x] **Relative Markdown link targets**: `ERROR_DOC_LINK` in `cargo x lint` checks links
      and images under `docs/**`, except `docs/DEVDOC/log.md`. Existing missing targets are
      explicit debt in `docs/guardrails.toml`, not silently ignored.
- [ ] Extend link checking to root docs, skill bodies, Rust doc comments, external URLs,
      and fragment anchors. The current check only verifies relative target existence.
- [ ] **Bare internal references**: validate `file_path:line_number` prose pointers;
      relative Markdown links into `src/` already receive the target-existence check.

### 8.3 Doc structure standards

- [x] **ADR identity and status presence**: `ERROR_ADR_ID` checks filename-prefix uniqueness;
      `ERROR_ADR_STATUS` requires a line starting with `Status:` or `**Status:**`.
      This checks presence, not the status vocabulary. The existing `007` collision is exempted.
- [ ] **ADR template and naming policy**: choose the identity scheme (Q10) and validate status
      values/lifecycle, without silently renaming accepted identities.
- [x] **Scratch-note metadata**: `ERROR_DOC_METADATA` requires `Status:`, `Owner:`, `Updated:`,
      and `Scope:` within the first 25 lines. These are plain Markdown fields, not YAML
      frontmatter. Existing omissions remain explicit legacy exemptions.
- [ ] Add scratch-note lifecycle checks: date/value validation, staleness/archival policy,
      related PR and supersession metadata. Do not treat metadata presence as freshness.
- [ ] **`AGENTS.md` → index + `.agents/rules/*.md` topic files.** Reduces the single-file
      conflict magnet and lets agents load only the relevant rule. Note: AGENTS.md is read
      automatically, so the index must stay small and point precisely.
- [ ] **Glossary-driven naming**: `docs/wiki/dict.md` exists, and todo.md flags
      "`KeyId` is a terrible name" / "`FacetKey` too, it's misused". Consider a lint that
      flags public type names absent from the glossary (advisory only — naming is a
      judgement call, not a mechanical one).
- [ ] **One doc owner per area**: `docs/wiki/` has a page for `big_sync` only, while
      `big_repo`, `daybook_core`, `wflow`, `plug_*` have none. Require a wiki stub when a
      crate crosses a size threshold, or when it becomes a snapshot pilot crate.
- [ ] **`.coderabbit.yaml`** currently excludes `docs/DEVDOC/**` — revisit once log.md
      churn settles (operator said don't touch log.md, so this stays parked).
- [x] **Skill discoverability**: `ERROR_SKILL_DOC` requires each directory under
      `.agents/skills/` to contain `SKILL.md` and its name to appear in `AGENTS.md`.
      This checks discoverability, not the skill body's correctness or links.

### 8.4 Why docs QC is an *agent-pace* item, not a nicety

- An agent's first move is reading. Stale or contradictory docs cause wrong hypotheses and
  wasted sessions — the exact failure mode AGENTS.md's "red herring on record" section
  documents for big_sync/keyhive.
- Machine-checkable doc rules are the only kind that survive compaction. Prose rules in
  AGENTS.md get summarized away; `missing_docs` does not.

---

## 9. Lint rules: turning AGENTS.md prose into checks

Today's tree already violates several AGENTS.md rules (counts in §6.3), which is the
evidence that prose alone is insufficient. **Every lint message must state why the rule
exists and what to do instead — the error message is effectively the agent's prompt.**

### 9.1 clippy config (cheapest, highest leverage)

- [ ] `clippy::allow_attributes = deny` + `allow_attributes_without_reason = deny`.
      Every suppression becomes `#[expect(..., reason = "…")]`, which the compiler
      auto-expires via `unfulfilled_lint_expectations`. **This is the baseline mechanism
      for everything below** — it converts today's 13 `#[allow]` + 81 `#[expect]` into a
      burn-down list that shrinks itself.
- [ ] `disallowed-types`: `tokio::sync::Mutex`, `tokio::sync::RwLock`, `async_lock::*`,
      `dashmap::DashMap`. Reason text names the replacement (std/parking_lot mutex never
      held across await, or mailbox/actor). Land now by `#[expect]`-ing the ~65 existing
      sites; burn-down is then `rg 'expect.*disallowed_types'`.
      Existing `clippy.toml` `await-holding-invalid-types` already covers 9 paths — extend,
      don't duplicate.
- [ ] `disallowed-methods`: `tokio::spawn` / `task::spawn` → one sanctioned `utils_rs`
      spawn helper that enforces the AGENTS.md panic-propagation contract
      (`AbortableJoinSet` exists at `src/utils_rs/lib.rs:823` but is not mandated).
      Also `futures::executor::block_on`, `std::env::var` outside config edges.
- [ ] `print_stdout` / `print_stderr` / `dbg_macro = deny`, with crate-level `#![expect]`
      in `daybook_cli` and `xtask`.
- [ ] `todo` / `unimplemented = warn` (9 sites), `unsafe_code = deny` (4 sites → `#[expect]`).
- [ ] `clippy::ignore_without_reason` — matches AGENTS.md "never add skip to tests".
- [ ] `clippy::wildcard_enum_match_arm` — a new variant in branch A then breaks every site
      that handles it. Directly serves the "unhandled branches should crash" rule.
- [ ] `clippy::unused_result_ok` + ast-grep for `.send(..)` results — 22 sites today ignore
      send errors. Broadcast channels (0..N listeners) get an explicit marker, per AGENTS.md.
- [ ] Determinism/perf pedantic subset (**measure first**, Q4): `iter_over_hash_type`,
      `large_futures`, `large_stack_arrays`, `large_types_passed_by_value`, `unused_async`,
      `needless_pass_by_value`, `unnecessary_to_owned`, `format_collect`, `manual_let_else`,
      `uninlined_format_args`, `semicolon_if_nothing_returned`.
- [ ] `mod_module_files` to lock the existing `foo.rs` + `foo/` convention.
- [ ] `too_many_lines` / `cognitive_complexity` with thresholds in `clippy.toml` —
      complements §4 for the 464-line `handle_cmd`.

### 9.2 ast-grep (syntactic, fast, YAML rules with built-in rule tests)

- [ ] catch-and-log: `if let Err(e) = .. { warn!/error! }` — 26 sites. AGENTS.md forbids
      swallowing errors; the fix is crash-or-toast, not log-and-continue.
- [ ] `timeout(` inside `#[cfg(test)]` modules — 24 sites. AGENTS.md: rely on nextest's
      hard timeout instead of internal ones.
- [x] `#[ignore]` without reason — implemented by the `syn` scanner as `ERROR_IGNORED_TEST`;
      new skips still require operator approval.
- [ ] progress comments (§8.2).
- [x] `/tmp/…` literals and non-zero socket-address string literals in tests (§5.6) —
      `ERROR_FIXED_TEMP_PATH` and `ERROR_FIXED_TEST_PORT` in the `syn` scanner.
- [ ] Detect fixed ports constructed from numeric arguments, not just socket-address strings.
- [ ] `crate/tests/*.rs` existence — AGENTS.md prefers in-crate `e2e` modules.

### 9.3 dylint (only where resolved types / compiler semantics are essential)

- [ ] Misused `run_until_cancelled` (todo.md: "`CancellationToken::run_until_cancelled`
      != cancel branch on select") — 7 sites. Needs types, so dylint.
- [ ] Aliased async mutexes that a path-based `disallowed-types` cannot see.
- [ ] Keep the count small: dylint runs on nightly (we already use nightly) but is the
      most expensive tier. **Use the cheapest sufficient tool**: clippy config → ast-grep →
      dylint.

### 9.4 Standardized violation classes (operator request)

Extend the existing `utils_rs::expect_tags` pattern in two directions.

**(a) Panic reasons.** Lint that `expect(..)` cites an `expect_tags::ERROR_*` constant
rather than a freeform string. Makes panic reasons greppable and classifiable, and turns
the 301 non-test `.unwrap()` sites into tagged classes (this is what makes Q5 tractable).
- [ ] Fix the two typos: `ERROR_UNRECONIZED`, `"poisioned mutex"`.
- [ ] Decide whether new classes need an ADR line or just a comment.

**(b) Lint diagnostic codes.**
- [x] `cargo x lint` emits named `ERROR_*` codes and supports `--format json` (or `--json`).
      JSON is an array of findings with a `code` field, not an object keyed by code.
- [ ] Stable numeric IDs and diagnostic-contract snapshots remain unimplemented.
- [ ] Changed-path filtering (`--changed`) remains unimplemented; use the full scan today.

**(c) Exemption reasons.**
- [x] Fixed reason classes are enforced by `guardrails::policy::Reason`: `LEGACY_DEBT`,
      `GENERATED`, `CONTRACT_TEST`, `SINGLE_WRITER_ACTOR`, `ARCHITECTURAL_EXCEPTION`.
      Every exemption also needs a nonempty reason and reference, an exact selector, and a
      positive ceiling. `INTENTIONAL_BREAK` is not an accepted class.
- [x] Improved exemptions become `ERROR_STALE_EXEMPTION`; `bless` only lowers/removes debt.
      This distinguishes legitimate exceptions from temporary debt.

Still needed for conspicuous policy-change review:
- [ ] Adding an exemption, dropping a test, or weakening a check must be **conspicuous in
      review** (CI posts a policy-change summary on the PR).

### 9.5 Other guardrails worth having

- [x] **`cargo x lint --format json`** — parseable full-scan findings for agents.
- [ ] **`cargo x lint --changed`** — changed-path filtering is a later optimization.
- [ ] **`cargo machete`** (already a todo.md item) + **`cargo deny`** (duplicate versions
      hurt compile pace). No `deny.toml` exists today.
- [ ] **zizmor ratchet**: 122 findings, hook parked at `stages: [manual]`. Ratchet the
      count down; un-park when low enough.
- [ ] **Targeted mutation testing** (`cargo-mutants`) on high-risk *changed* logic only,
      never the whole workspace. Exposes tests that pass despite broken behavior.
- [ ] **Failure-oriented performance tests**: assert bounded query counts, allocations or
      work-per-operation — not fragile wall-clock limits. (AGENTS.md's Performance section
      wants exactly this; shared CI runners make wall-clock assertions useless.)
- [ ] **Behavioral contracts** beyond Rust signatures: WIT interfaces, persisted
      serialization fixtures, DB migrations + migration identities, hash/domain-separation
      identifiers, and ordering/cancellation/durability semantics.
- [ ] **`cargo-hack`** for independent-crate builds under supported features.
- [ ] **Orphan `.rs` file lint**: flag `.rs` files not reachable from any crate module tree.
      The pre-commit fmt hook (per-file rustfmt) is the only check that visits them — it
      caught `src/utils_rs/cheapstr.rs` (dead duplicate of the inline `mod cheapstr` in
      `utils_rs/lib.rs`) and `src/daybook_core/index/hydrate.rs` (parked, unreferenced)
      when the import-granularity options changed (§0.3). A lint would make the debt
      explicit instead of leaving dead files to rot.

---

## 10. Nix, determinism and the fzakaria inspiration

- [ ] **The Nix store is already a cross-worktree cache**: sources are copied into the
      store, so identical inputs give identical derivations regardless of which worktree
      they came from.
- [ ] **`flake.checks` via crane or crate2nix** for lint / public-api / layers: a check on
      a crate unchanged versus trunk is a cache hit for every other agent. Use to
      *verify*, not in the edit loop (it gives up incremental compilation).
      Prerequisite: `x/wit-fetch.ts` and the plug OCI builds become fixed-output derivations.
- [ ] **Snapshot per crate *and* per supported configuration.** Do not hide auto-trait
      changes just to make a snapshot smaller.
- [ ] Pin `cargo-public-api` / `cargo-semver-checks` **versions together with the
      compiler** — rustdoc JSON format changes.
- [ ] Once a stress test is a derivation, trying **Rewind VM** on it is a cheap experiment
      (§3, §5.7).
- [ ] **CI review card** posted per PR: public-API diff, symbol-count changes, new
      `#[expect]`s, new `unsafe`, new dependencies, policy/exemption changes. With many
      agents, human review is the bottleneck; this makes the risky parts of a diff stand out.
- [ ] Nixify the fast checker itself (§5.5) so "the tool that explains why your branch is
      broken" is pinned and always runnable.

---

## 11. Sequencing

### PR 1 — guardrail tooling, no broad cleanup (plus the approved `imports_granularity`)
1. ~~Lightweight independent checker crate (not behind xtask)~~ → **lives in `xtask`**;
   instead `xtask` itself was slimmed (demo commands, dead generator and keyhive/subduction/
   daybook_types deps removed) so the gate compiles fast and does not depend on application
   crates. Readable + `--json` diagnostics with stable `ERROR_*` codes. Nix packaging: later.
2. Symbol/module inventory (§4.1) + narrow, reasoned legacy exemptions; gate new violations only.
3. Dependency-boundary / layer checks (§5.4) using the current graph as the starting baseline.
4. Public-interface snapshots for a small pilot set of boundary crates (Q3).
5. Affected-check planning + source-bound validation receipts (§5.5).
6. clippy config + `allow_attributes` → `expect(reason)` conversion (§9.1) — **deferred to
   the cleanup PR**. The AST rules (`ERROR_ALLOW_ATTRIBUTE`, `ERROR_LINT_REASON`,
   `ERROR_SHARED_ASYNC_STATE`) already gate new violations without compiling, and enabling
   the clippy equivalents under `-D warnings` requires touching the ~65 existing sites.
7. **Tests for the rules themselves**, including legitimate exceptions and ways a naive
   implementation could miss violations.
8. `rustfmt.toml` `imports_granularity = "Item"` + `group_imports` (approved, one-time diff).
9. `[profile.dev.package."*"] debug = "line-tables-only"` (approved).
10. Fix the `RUSTFLAGS` drift (§6.1) + `cargo x doctor`.

### PR 2 — build/tooling, independent of architectural cleanup
- Implemented: native/guest workspace split, target contracts and matrix runner, per-worktree
  targets, wrapper jobserver/cgroup limits, disk/cache budgets, and workspace feature unification.
- Stock sccache remains opt-in; normalization adoption is shelved (Q7). Hakari measurement,
  shared-dependency lockfile-divergence checking, and disk-watch coordination remain open.

### PR 3+ — collaboration and reproducibility
- Implemented: `cargo x overlap` reports sibling-worktree interference; validation receipts
  bind results to sources/toolchain; scheduled nextest runs use the nightly flake-fail policy.
- Still open: advisory claims, speculative integration, merge queue, receipt consumption in CI,
  reproduction bundles / repro capsules, and `sometimes!` reachability assertions.

### Later — cleanup, guided by the tooling
- Burn down exemptions; split `big_sync_core/lib.rs` (2163-line inline tests first),
  `hub.rs` dispatch arms; `daybook_core` atomization with the migration-identity guard
  (§6.4) active.

---

## 12. Success metrics

Measure these, **not** the number of enabled lints:
- Time to trustworthy feedback.
- Integration repair time (conflicts + semantic breakage discovered late).
- Duplicated compilation across worktrees.
- Reproducibility of failures handed between agents.
- Exemption count trending down (the burn-down actually happening).
- Number of PRs that changed a public contract without the snapshot diff showing it.
