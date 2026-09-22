---
name: fork-pinning-and-upstream-sync
description: Track, compare, and pin the dman-os Keyhive and Subduction forks that townframe consumes - jj-only inspection, upstream PR verification by patch, the [patch] <-> rev-pin lifecycle, and the CI jobs that depend on the pins. Use when a symptom points into ../keyhive or ../subduction, when picking up upstream work, or when repinning after a push.
---

# Fork pinning and upstream sync

`townframe-2` consumes two forks of our own. Upstream `main` is not ours; our work rides as an extra
commit on top of it on a fork bookmark.

| checkout | origin (ours) | upstream | pin today |
|---|---|---|---|
| `../keyhive` | `dman-os/keyhive` | `inkandswitch/keyhive` | `townframe-changes` at `d4bda7c7` |
| `../subduction` | `dman-os/subduction` | `inkandswitch/subduction` | `feat/observer-sketch` at `643c4820` |

Read `AGENTS.md` first. The VCS rules there apply to all three checkouts.

## Ground rules

- Never run git commands in these checkouts. They are jj colocated workspaces; use `jj`.
- Do not rebase or reshape the fork branch while instrumentation and fixes are interleaved. Split
  the instrumentation into its own commit first; see `stress-sync-investigation`.
- `gh` reads PRs from **upstream** (`inkandswitch/...`). The fork's own PR list is empty.
- Prefer naming a call site over naming a PR. A patch that changes a function which is not in our
  failing path is not the fix, however on-topic its title reads.

## Reconnaissance

```bash
cd ../keyhive              # or ../subduction
jj st                      # working copy state, unresolved conflicts
jj bookmark list           # upstream main vs the bookmark our pin points at
jj log -r 'main..@' --no-graph -T 'change_id.short() ++ " " ++ description.first_line() ++ "\n"'
jj diff -r 'main..@' --stat
```

Then evaluate upstream work by patch:

```bash
gh pr list --repo inkandswitch/keyhive --state open --limit 30
gh pr view <n> --repo inkandswitch/keyhive --json title,state,mergeCommit,files
gh pr diff <n> --repo inkandswitch/keyhive
```

Classify each candidate, with evidence:

1. already contained in our pin (its merge commit is an ancestor of the pin),
2. applies on top of the pin and touches our failing path,
3. changes code that is not on our path, or
4. wants a new pin.

State which function the PR changes and which function in our failing path that alters. A recap
from a previous session is only a starting point: re-validate its "upstream is irrelevant"
conclusions against the patch, because a workaround was once justified by #297 after the operator
had already ruled that subsystem (pubsub) out of scope.

## Patch lifecycle

Pins live in the root `Cargo.toml` as `rev = "<sha>"` - currently 8 `subduction_*`/`sedimentree_*`
crates and 3 `keyhive_*`/`beekem` crates. The same file carries commented-out patch blocks for
local work:

```toml
[patch."https://github.com/dman-os/keyhive"]
keyhive_core = { path = "../keyhive/keyhive_core" }
# one entry per crate
```

Both directions are deliberate:

- **Editing a fork locally**: uncomment the block you need; `Cargo.lock` drops the
  `git+...#<rev>` sources for those crates. Confirm with `cargo tree -p big_repo | rg 'keyhive|subduction'`
  that everything resolves to the local paths with no duplicate git copy, then run the fork's own
  tests in its checkout (`cargo nextest run -p keyhive_core`).
- **Landing a fork change**: push the fork bookmark, then set `rev` to the pushed commit and remove
  the patch block in the same change. A patch block left active resolves `../keyhive`, which does
  not exist on a CI runner.
- **Pushing needs credentials the agent does not hold** for these forks. Commit locally, ask the
  operator to push, then repin to the pushed sha. Repinning before the push leaves the workspace
  unresolvable for everyone else.

## After a repin

1. `cargo clean -p <fork crates>`; a changed rev with warm fingerprints hides resolution mistakes.
2. `cargo clippy --all-targets --all-features -p big_repo` (and `-p daybook_core`).
3. Run the fork's own suite where the change landed - keyhive is 144/144 as of `d4bda7c7`.
4. Relaunch a scoped hunt. Keep "pin upstream" and "fix our bug" as separate commits so the pin can
   be advanced again without dragging unrelated work along.

## CI that depends on the pins

- `cargo test --doc` compiles generated code, and `wit_bindgen::generate!` copies WIT doc comments
  into Rust. A fetched package that documents SQL in an *untyped* fence makes rustdoc compile that
  SQL as Rust and fails every crate that fetches it. Patch the fetcher (`x/wit-fetch.ts`,
  `tagBareDocFences`), never the gitignored fetched copy.
- The `rust tests` job builds coverage in three steps: `--no-report nextest` for unit/integration
  tests, `--no-report --doc` for doctests only, then `cargo llvm-cov report --doctests --lcov`.
  Omitting `--doctests` from the report silently writes an empty `lcov.info`; the job now fails if
  the file has no records.
- That job also enables nextest recording via `.config/nextest-user-ci.toml` and exports
  `nextest-run.zip` on failure. The artifact carries per-test stdout/stderr (middle-truncated) that
  the CI log would otherwise lose.

## Parking instrumentation before a push

The fix commit must sit directly on the pin, with instrumentation *off* the pushed branch (it is for
you, not for CI). Park it on its own bookmark rather than leaving it orphaned: an orphan can be lost
between sessions, and a bookmark makes the intent explicit.

```bash
cd ../subduction                        # or ../keyhive
jj bookmark set instrumentation -r <instrumentation commit>
jj describe -m "fix: <what the fix really does>"   # name the working-copy fix
jj log -r '@|@-|instrumentation'        # pin -> fix, instrumentation beside it
```

If the fix was written on top of the instrumentation, rebase the *fix* down onto the pin — never
advance the pin to reach the fix, because the pin is what townframe and CI compile:

```bash
jj new <pin rev> -m "fix: ..."
jj restore --from <old fix commit> -- <paths the fix touches>
jj abandon <old fix commit>
```

Verify the push shape with `jj log -r '::@' --limit 3`: first line is the fix, second is the pin. The
operator pushes the pin bookmark (`jj bookmark set <pin book> -r @`); pushing needs credentials the
agent does not hold.

## When the fork will not build under its own patch

A path patch compiles the fork's *working copy*, and that working copy can be ahead of the pin in ways
that demand a newer sibling fork. Real case: subduction's HEAD called
`keyhive_core::Keyhive::audit_state_generation()`, which the local `../keyhive` did not have, while
the *pin* was self-consistent. Before touching the other fork, check whether the call exists at the
pin:

```bash
jj file show -r <pin> path/to/file.rs | rg <method>
```

If it does not, the extra commits are the problem and basing the fix on the pin (above) resolves it
without advancing anything.

## Two ways a patch edit goes wrong

- **A partial `[patch]` block silently duplicates crates.** Deleting one entry (e.g. `beekem`) left
  the crate resolving from both the local path and the pinned git source; the symptom was
  `expected beekem::operation::CgkaOperation, found a different beekem::operation::CgkaOperation`,
  which reads like an API mismatch and is not. After editing a patch block, re-check resolution:
  `cargo tree -p big_repo --duplicates | rg -A3 'keyhive|beekem|subduction'` must show exactly one
  source per crate.
- **A patch edit is a resolution change, not a source change.** Adding or removing a block
  re-resolves the whole graph, so a warm build can pass while a cold one fails (or the reverse).
  Expect a rebuild, and re-check the duplicates above whenever you touch the file.

## The fork CI you are about to make red

Both forks gate on clippy with `-D warnings`, so "the fix compiles" is not enough to push:

- subduction: `cargo clippy --workspace --all-targets --all-features -- -D warnings`, plus a
  per-crate wasm32 pass. `--all-targets` must *build*: pre-existing broken integration tests (a
  changed `sync_with_peer` arity, for example) fail the job even though the library is fine.
- keyhive: `cargo clippy --all-targets --features=test_utils -- -D warnings`, workspace-level — note
  the feature set is `test_utils`, not `--all-features`.

Run the exact command from `.github/workflows/quality.yml` before asking for a push, and put the
pre-existing lint fixes in their own commit (`jj new -m "chore: ..."`) so the fix stays reviewable.
