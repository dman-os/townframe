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
