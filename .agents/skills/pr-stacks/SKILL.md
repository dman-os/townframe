---
name: pr-stacks
description: Run a lane's work as one stack of small draft PRs (gh stack): naming, bases, restacking, docs layers, CI on the tip only, review and merge, backups. Use when starting, extending, restacking or submitting a lane's PR stack, or when deciding where a commit belongs.
---

# One lane, one stack of small PRs

The operator's rules for every lane (nodules #101 is the reference stack). PRs exist so work stays small
and easy to review; that is the whole point, so minimize coordination effort around them.

**Operator, 2026-11-18:** don't spend effort making every PR clean; mechanical splits are fine. The
operator does not use GitHub stack merging (it has problems): later they pick N PRs and merge them
with their own tooling, which needs no rebasing of everything at once. So the restacking below is
optional hygiene, not a requirement.

## Shape

- **One stack per lane**, branches `stack/<lane>-NN-<slug>`. Never a separate stack for the same
  lane, never a push straight to an integration branch.
- **Strictly linear when submitted.** The bottom layer's base is `main`; every other PR's base is
  the previous layer's branch. `gh stack` builds and links them:
  - new stack: `gh stack init -b main <branches…>`, then `gh stack submit --auto`;
  - existing PRs: `gh stack link <pr#…>` (bottom to top).
  - `gh stack` needs `remote.pushDefault`; pass it for the one command only:
    `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=remote.pushDefault GIT_CONFIG_VALUE_0=origin gh stack …`.
- **NN is a name, not an order promise.** Take the next free number when work starts. Lanes may
  build several layers in parallel off one base; put them into a line (cherry-pick onto the tip,
  in order) when submitting, and renumber if the order changed.

## What a layer is

- **One concern per PR**, small enough to review in one sitting. Keep meaningful commits inside a
  layer; squash WIP and fixups. Instrumentation stays a commit of its own (AGENTS.md).
- **Docs and ledger:** a substantial batch of decisions or a new ADR is its own docs layer. A small
  ledger update that records the work of one layer rides in that layer. Never edit an existing
  layer just to park unrelated docs.
- **Fixing a lower layer** that isn't merged: amending it is nicest for its reviewer, but a fixup layer
  on top is fine when restacking would cost more than it's worth.

## Restacking (optional)

- Restack when you're about to push or submit, not on every move of the base.
- `gh stack rebase` rebases the whole stack onto trunk. Afterwards check that lower layers didn't
  change unexpectedly before pushing.
- Force-push only stack branches, never `main` or another lane's branches. No merge commits inside
  the stack.
- After a layer is squash-merged into `main`, rebase the remaining layers onto `main`.

## Other lanes' work

Don't base a PR on another lane's unmerged tip. If you need it, say so in the PR body and rebase
when theirs merges. For local testing on top of other lanes, keep a separate integration branch
that is never a PR base.

## Draft, CI, ready

- **Every PR opens as a draft.** Draft PRs skip CI (job-level
  `if: github.event.pull_request.draft != true`, plus a `ready_for_review` trigger).
- **Only the tip is greened.** Intermediate layers don't have to build alone ("tip green, layers
  topical"). Mark the tip ready (`gh pr ready`) to run CI; when the tip moves up, return the old tip
  to draft (`gh pr ready --undo`). Spend nothing greening layers until the operator asks.
- Watch CI with a background loop on `gh pr checks` for the named jobs, retrying network errors,
  with a 90-minute deadline.
- Full suites run on GitHub CI; local and mothership runs are narrow and incremental.

## Review and merge

The operator reviews and merges, choosing PRs with their own tooling; nothing merges before their
review, and lanes don't merge with GitHub's stack merging. Ask the operator before anything else.

## Conventions

- **PR titles are semantic** (`type(scope): …`). `gh stack submit` titles a new PR from its branch
  name, which fails the title check: fix it right away with `gh pr edit <n> --title …`.
- **PR body:** what and why, what was verified (exact commands), and dependencies on other lanes.
- **Commits** carry no `Co-Authored-By`, `Claude-Session` or "Generated with" lines.
- **Back up every commit at once** (the laptop's repo drive drops under load):
  `git push mothership <branch>:refs/heads/mirror/<branch>`.
- **Post on the swarm log** (`~/swarm/swarm post <lane> landed "…"`) when the tip moves, with the PR
  number, so lanes that build on you know.
