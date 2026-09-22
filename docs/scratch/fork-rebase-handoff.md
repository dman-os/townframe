# Fork rebase onto upstream main — handoff index

Scratch doc (drop before merging PR #51 if unwanted). After any compaction, re-read
`AGENTS.md` and the `.agents/skills/` docs (`fork-pinning-and-upstream-sync`,
`stress-sync-investigation`) — they carry the rules this work follows.

Both forks were rebased **as clones**: the pre-existing fork lines were left exactly where they
were, so going back is a `jj` checkout away. No git command was run anywhere; nothing was pushed.

| repo | clone bookmark | head | old line (untouched) | gate |
|---|---|---|---|---|
| `/home/asdf/repos/rust/keyhive` | `upstream-rebased` | `14a523c3` (12 duplicated commits; the mislabelled head was renamed to what it does). The `pick_prekey` typed-error fix is `28a5399741f8` and is **not yet under the bookmark** | `townframe-changes` = `3afcd294`, `instrumentation` = `aa469ceb` | scoped clippy (`keyhive_core`+`keyhive_crypto`+`beekem`, `--all-features` and `--features=test_utils`) = **exit 0, zero warnings**; `nextest -p keyhive_core` → **245 passed / 4 skipped** |
| `/home/asdf/repos/rust/subduction` | `upstream-rebased` | `192e6b2d` (28 duplicated commits + 3 decision commits + 4 `chore: drop the out-of-scope …` commits = 35 over upstream) | `feat/observer-sketch` = `53055ca5`, `instrumentation` = `522f739b` | `nextest -p subduction_core -p sedimentree_core` → **634/634 pass (1 skipped)**; scoped clippy over the 8 consumed crates = **exit 0, zero warnings** |

Rebase destinations: keyhive `main@upstream` = `8c631cdc` ("Handle leaf conflicts (#237)");
subduction `main@upstream` = `472c5088` ("Switch to `typos` CI action (#303)").

## Going back / verifying the clone

```bash
cd ../keyhive      # or ../subduction
jj bookmark list                       # both lines are visible side by side
jj log -r '::upstream-rebased' --limit 5
jj diff -r 'main..townframe-changes' --stat   # the old line, unchanged
```

Nothing was rewritten on the old lines: `townframe-changes`/`feat/observer-sketch` and both
`instrumentation` bookmarks still point at the original shas. Neither `upstream-rebased` line
contains instrumentation.

## What the rebase did (details in the reports, not repeated here)

keyhive: dropped the hunks upstream superseded — `3afcd294`'s six #224-identical boxing edits,
`f53929d9`'s `register_group` hunk, `d4bda7c7`'s conflicted-leaf part, `e9bb4efb`'s
`has_pcs_key()` asserts — and kept the survivors (shared `state_generation`, `event_ingestion`
serialization, `static_membership_subject`, the typed `MissingPrekeys` fix). The **#216
`can.min(group_access)` hand-port into `add_member_with_manual_content` is DONE** (report §3) — the
one thing the rebase would not have flagged on its own.

subduction: dropped `b8868441`'s `covered_by_loose` half (upstream #290) and `c16da989`'s dedup
guard; kept `d4bb13df` (single lock hold), `023992fb` (per-sedimentree ingest membership),
`824c4a03` (minimization deltas) and all keyhive-side work. Three commits appended:
`0e21da0e` (a fragment's own head is never hidden from the frontier by its own checkpoints),
`58eb84f5` (spilling-residue regression compares frontiers as sets), `c231e77a` (rustfmt the
rewritten lines). Net fork delta vs upstream: 68 files, +5754/-998.

## Gate scope ruling (operator, this session)

Do not chase workspace-wide green — a previous pass did that and touched files we do not use. Our gates are
only: (a) lint/format debt introduced by our own diff, (b) warnings in files our diff already touches.

Consumed crates. keyhive: `keyhive_core`, `keyhive_crypto`, `beekem`. subduction: `subduction_core`,
`sedimentree_core`, `subduction_crypto`, `subduction_iroh`, `subduction_websocket`, `subduction_ephemeral`,
`subduction_redb_storage`, `subduction_keyhive`.

Out of scope — never lint-fixed for gate reasons: `subduction_hyper` (so the `axum.rs:174`
`unused_async_trait_impl` finding is moot; leave upstream's file alone), `sedimentree_hyper`,
`subduction_cli`, `sedimentree_wasm`, `subduction_wasm`, `keyhive_wasm`, `beelay`, examples/benches,
CI workflows.

Identified offenders — **subduction RESOLVED**. `2d130549`'s `sedimentree_wasm/src/{commit_id,storage}.rs` lint hunks were
dropped (`3372a68`, `4d0ed05`) along with `subduction_http_longpoll`'s (`1ce2548`, `192e6b2d`); both directories are now
byte-identical to `main@upstream` (`jj diff --stat` reports 0 files). `263b455ab8ee`'s `subduction_keyhive/src/protocol.rs`
(-28/+14) turned out to delete three function-local dead diagnostics, not API surface — kept. The only remaining
out-of-scope diff is **compile-forced, not lint**: `subduction_wasm/src/subduction.rs` (the fork's `store_commit` returns
`(Option<FragmentRequested>, Vec<CommitId>)`, so `.0` is required for that crate to build) and `subduction_cli/*` (from
`e60cf535`, `init_sendable_keyhive` now takes `NoListener`).

Ruling on the lane's two non-decisions: the four drop commits **stay separate** (visible audit trail) rather than squashed
into `2d130549`, which would rewrite descendants and re-conflict with `98920d0f`. The push shape (push `upstream-rebased`
as-is vs re-pointing `feat/observer-sketch`) is the operator's call — see the table row above and the decisions section.

Open risks recorded by the lane: the wasm crates cannot be compiled locally (no wasm32 target in the Nix toolchain), so
the `subduction_wasm` keep rests on the API argument rather than a build; and the four `#[expect]`s in `3a79ab6b9df3` are
only **locally** fulfilled (Nix clippy 1.100-nightly vs pinned 1.91.0), so a pinned-toolchain CI run is what settles them.
RESOLVED — the head was renamed before the crash to "fix: keep a generated principal's head stores bound to its hive's
state-generation counter" (`14a523c3`), and the audit found **nothing to drop**. The line's only out-of-scope hunks sit in
`keyhive_wasm/src/js/{event_handler,keyhive}.rs` (2 files, 4 hunks, +17/-4), and each is forced by one of our API
changes: `MembershipListener` gained `target: Identifier`, `PrekeyListener` gained
`secret: Option<&LocalPrekeySecret>`, and the CGKA-secret accessor now yields `ShareKey`/`ShareSecretKey` instead of a key
pair. Verified by reading each hunk at its call site.

Resumed after the parent-session crash killed the first pair — fresh workers (resume was impossible: the crash moved the
session's async root from `/tmp/nix-shell.vUlKZS` to `/tmp/nix-shell.SQJMuG`, so both the revived ids `0d7c7a6d`/`67d73c05`
and the originals `5b423e95`/`b276ef76` report "Async run not found"): keyhive `3f231e35-a66d-4919-9248-2b59616ed64e`,
subduction `6a8ad69e-2bdc-4efb-9007-ae712f2e834f`.

## Open items, in order

**Status at the end of session 2 — items 1 and 2 below are closed.** Keyhive, at head `14a523c3`: both scoped clippy
gates exit 0 with zero warnings (re-linted after touching sources, so not cache-assisted) and `nextest -p keyhive_core`
is 245 passed / 4 skipped. Subduction, at head `192e6b2d`: scoped clippy over the eight consumed crates exits 0 with zero
warnings and `nextest -p subduction_core -p sedimentree_core` is 634 passed / 1 skipped. The one remaining keyhive action
is putting the `pick_prekey` typed-error fix under the bookmark (`jj bookmark set upstream-rebased -r 28a5399741f8`) once
the operator decides the pin should be the tip.


1. **keyhive clippy gate** — not yet run: `cargo clippy --all-targets --features=test_utils -- -D warnings`
   (the fork's exact CI command; note the feature set, not `--all-features`).
2. **keyhive fix commit shape** — the `pick_prekey` typed-error fix is the working copy `4a15e0c1` and is
   *not* under `upstream-rebased`; put it on the bookmark (or `jj squash`) before anything points a pin at it.
3. **keyhive public API shape changed** — report §7 lists what the downstream `big_repo` consumer must
   follow. This is the input to the townframe migration the operator deferred.
4. **subduction pin bump + keyhive-API adaptation** — deliberately *not* done: subduction still pins keyhive
   `3afcd294`. Do it after the keyhive line is pushed (report §6).
5. **Push + repin** (operator holds credentials): push each `upstream-rebased`, set `rev = <sha>`, and remove
   the matching `[patch."https://github.com/dman-os/..."]` block in townframe's root `Cargo.toml`.
6. **Reviewer reconciliations** — subduction report §7 lists four re-derivations a reviewer must look at
   (`Ingested` vs `IngestSummary`, `add_commit`/`add_fragment` broadcast fallback, `head_assuming_minimal`,
   `sync_with_peer`'s fifth argument).

## Decisions owed by the operator


- Push shape for subduction: push `upstream-rebased` (`192e6b2d`) as a new branch and pin that, or re-point the existing
  `feat/observer-sketch` bookmark at it? The first keeps `53055ca5` reachable as the rollback, the second keeps one name.

- Fork CI lint scope: both lanes independently reached the same conclusion, so this is the one config decision left. The
  fork's CI job (`flake.nix:217-222`: `cargo check --workspace --all-targets` plus
  `cargo clippy --workspace --all-targets --features test_utils,debug_events -- -D warnings`) is workspace-wide and was
  deliberately not run under the ruling. If the fork's CI must be green, scope *that job* to the consumed crates in the CI
  config — never by editing upstream source we do not consume. Operator call, since it means touching our fork's CI.

- Ship keyhive's self-reference guard upstream? It is a 3-liner upstream would likely take
  (subduction report §3.2 has the reachability evidence).
- `subduction_hyper/src/axum.rs:174` `clippy::unused_async_trait_impl`: **verdict is toolchain drift** —
  the file is byte-identical to `main@upstream`, and the lint exists in the local Nix clippy 1.100.0 but not
  in the pinned/CI 1.91.0. Recommendation: leave upstream's file alone and record it.
- Open upstream PRs: pin any of them (`#241`/`#236`/`#238`/`#239` keyhive, `#299`/`#304` subduction)? That is a
  `rev =` change in townframe's `Cargo.toml`.

## Full reports (zero-context handoffs)

- keyhive: `/home/asdf/.pi/agent/sessions/--home-asdf-repos-rust-townframe-3--/subagent-artifacts/outputs/050d3ff0-c45b-4883-863c-d4902461e413/keyhive-rebase-report.md`
- subduction: `/home/asdf/.pi/agent/sessions/--home-asdf-repos-rust-townframe-3--/subagent-artifacts/outputs/050d3ff0-c45b-4883-863c-d4902461e413/subduction-rebase-report.md`
- upstream PR classification maps (per-PR and per-our-commit obsolescence):
  `.../outputs/ed13c3b5-0e32-416a-9f6e-0e9ba4c7e78c/{keyhive,subduction}-upstream-prs.md`

## Townframe migration input (the "easier Keyhive API" work, #222)

Upstream #222 (merged, now inside `upstream-rebased`) takes ids instead of `Arc<Mutex<T>>` handles for
`add_member`, `revoke_member`, `reachable_members`, `try_{en,de}crypt_content*`, `try_causal_decrypt_content`,
`force_pcs_update`, `try_pcs_key_hash`, `docs_reachable_by_agent`, `membered_reachable_by_agent`; returns ids
from `generate_group`/`generate_doc`/`receive_contact_card`; adds `access_for_doc`, `best_access_for_doc`,
`all_agent_events`, `event_digests_for_agent`; and renames `contact_card` → `generate_contact_card` (it
*rotates a prekey*). It deletes `register_group`, `get_membership_operation`, `receive_membership_op`,
`static_event_to_event`, `inject_pending_events`, `Cgka::has_pcs_key`. Its own motivation names our symptoms:
handle-passing leaves "weird state" for unknown entities, handles go stale when an individual becomes a group,
and "locking a handle is a common cause" of keyhive deadlocks.

Where it meets us: `src/big_repo/keyhive.rs` already hand-rolls the id-addressed surface
(`docs_for_agent:391`, `agents_for_membered:416`, `agent_access_on:500`, `membered_for_agent:528`, and a
comment calling `:441` "the `Identifier`-addressed twin of `get_group`"); ~42 handle-lock sites in
`big_repo/keyhive.rs` + `runtime2/{native,hub}.rs` lose the `get_document(...).await` → `.lock().await` dance;
`Individual::prekeys` and `can_decrypt_content` make our two open failure families (empty prekey set, missing
epoch key) observable instead of inferred; `contact_card`'s rename forces an audit of our six call sites
(`:157, :186, :192, :225, :230, :656-671`), `:225` being the path the class-3 panic came through. Revisit
`4d481a1d`'s `static_membership_subject` workaround once ids + `NotFound` land.
