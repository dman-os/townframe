# intxt without a GUI: the terminal and agent experience

Status: Sketch, for the operator's review
Owner: intxt lane
Updated: 2026-11-18
Scope: How a person at a shell, and an agent with only a shell, use intxt end to end with no app: set up, record, package, publish to a PR, find and read someone's intxt, review, and pick up a review. It builds on FDR 008 (packaging), `intxt-review-surface.md` (review, GitHub) and this week's decisions (ledger: the PR description block, `github pull`, `summarizeIntxts`, the `post-rewrite` hook, D63, `doctor`). It marks what exists today and what is missing. It doesn't change a decision.

## Who

- **The operator at a shell:** reads, carves and reviews. Wants short commands, short ids, the next command suggested, and undo.
- **An agent with only a shell:** packages its own work, leaves a guide, reads review comments and answers them. Needs stable output (`--json`), commands that block until there is something to do (`inbox --wait`), and no prompts.
- **A teammate's machine:** has never seen this work. Finds it from a PR.

The same commands serve all three. The GUI (the review micro) reads the same datums and methods, so nothing here is CLI-only data.

## The loop, end to end

### T0. Set up a machine, once

```
$ day x intxt install --handler intxt_nodule_guest.wasm
intxt is installed
$ day x intxt doctor
node      6cc7…  running, reachable as endpoint…  (relay-alpha connected)
intxt     installed, enabled, cartridge v1, ready
worktree  ~/repos/townframe   recorded (Claude Code hooks: ok; post-rewrite: installed)
          on intxt m "install as a library call"
github    drawer zGaC…  1 pull request imported
fix:      none
```

`doctor` is read-only and prints any fix as a command (roadmap, 2026-11-18). **Missing:** `doctor`. Today `status` covers a worktree and `node status` covers the node, and the setup gaps found while building the mothership source node (no peers section, so no endpoint) were found by digging.

### T1. Record, without typing anything

Harness hooks (`capture`), the ACP proxy (`record`) and git's `post-rewrite` hook record into the capture drawer, locally. Nothing is shared (FDR 008 W1, D29). Secrets are the harness's to redact before capture (D63). **Exists.**

### T2. Package the work

```
$ day x intxt new "Datum refs move to dtxt" --description "…"
intxt m4 …   this worktree is on it now
$ day x intxt add --branch feat/refs
added to m4: 7 commits (thin pack), diff 23 files, 2 sessions
```

**Exists:** `new` (`--with` for a related intxt), `add --commits/--branch/--change`, `use`, `rm`, `drop`, `list`, `show`, `status`. The diff blob is on the stack (#177). **Missing from FDR 008:** `add --session` (W3), carving with `new --from … --paths/--turns` (W4), and `link --stacks-on/--follows-up/--supersedes`. These were outlined but never built.

### T3. Publish it to a PR

```
$ day x intxt github push --pr 179          # or: gh pr create … --body "$(day x intxt pr-block)"
pull request dman-os/townframe#179: intxt block written (m4), the rest of the description unchanged
```

**Exists:** `pr-block` prints the block, and `PrLinks::write_into` replaces only the intxt block, idempotently. **Missing:** a command that writes the block into the PR's description through `gh` (`github push`, or `pr-block --into <pr>`). Today the block is pasted by hand.

### T4. Find the intxts of someone's PR

```
$ day x intxt github pull dman-os/townframe#179
dman-os/townframe#179 "refactor(intxt): install the cartridge as a library call" (Open) imported
  intxt m "intxt: install the cartridge as a library call"  link: dtxt:doc:z4kn…?via=z8Kd…
$ day x intxt github ls
dman-os/townframe#179  Open  "refactor(intxt): install the cartridge as a library call"
$ day x intxt github show dman-os/townframe#179
…body…
intxts:
  dtxt:doc:z4kn…  "intxt: install the cartridge as a library call"  2 entries
```

**Exists:** `github pull`. **In progress:** `github ls` and `github show` (one `summarizeIntxts` call, the CLI twin of the micro's PR view). **Missing:** reaching an intxt that isn't on this node: today `show` needs the drawer synced here. With `via=`, `show <dtxt:doc URL>` could dial a named node and ask for membership. That is a design question (ledger: `via` details deferred).

### T5. Read an intxt

```
$ day x intxt show m4                 # what it holds, by time
$ day x intxt read m4                 # the guide in order, then each entry's diff, then turns
$ day x intxt read m4 s               # one entry: its commits and diff (from the diff blob)
$ day x intxt read m4 k7 --turns 20-45   # a session as turns
```

`read` pages through `$PAGER` and prints diffs with git's colouring (the diff blob is a unified diff, so `delta` or `diff-so-fancy` work as pagers). `--json` gives the same content as data for agents.

**Exists:** `show`. **Missing:** `read` (FDR 008 W5), which is the main gap for a terminal reader. The diff blob (#177) makes the diff part cheap: no pack parsing, and the same text the phone shows.

### T6. Review

```
$ day x intxt comment m4 4c1f:src/big_cabinet_core/reference.rs:120-124 -m "Why not reuse the fragment?"
comment kz   undo: day x intxt rm m4 kz
$ day x intxt reply kz -m "Done in 77b2" --resolve
$ day x intxt inbox m4
```

**Exists:** `comment`, `reply` (with resolve and reopen), and `inbox`, with comments carried across rebases (the e2e covers it). **Missing:** `guide` (sections, R1), `inbox --wait`, and `seen`.

### T7. The agent's side of review

The agent's skill runs the loop below: block until a comment arrives, read it with its target resolved on the current revision, fix, reply and resolve.

```
$ day x intxt inbox m4 --wait --json
```

**Missing:** `--wait`. It needs a node-side wait, and `pollDatums` with `hold_ms` already exists, so it is a client loop over the intxt's documents. Steering stays the harness's concern (D22): intxt doesn't inject anything into the agent. The harness decides when the agent looks.

### T8. History moves

A rebase or amend between rounds: the `post-rewrite` hook records old→new pairs, `add` carries them into the intxt, and comments follow (R4). `which <commit>` names the intxts on this node holding a commit, following rewrites. **Exists.** The cross-node version (which intxts anywhere hold this commit) waits for the cartridge index on processors.

## What to build, in order

1. **`github ls` / `github show`** (in progress, Luna lane): the CLI twin of the micro.
2. **`read`:** the terminal reader. It's the biggest gap: today a person can see what an intxt holds but not read it.
3. **`github push`** (or `pr-block --into`): no more pasting.
4. **`doctor`:** setup visibility.
5. **`inbox --wait` and `--json` on `show`/`read`/`inbox`:** the agent loop.
6. **`guide`, `add --session`, `link`:** FDR 008 and R1 leftovers.
7. **Carving (`new --from`):** larger; FDR 008's open question 2 (turn granularity) first.

## Open

1. Is `read` one command with a target (an intxt, an entry, a session), or `read` plus `diff`?
2. `github push`: only the description block, or also publishing comments that map to the PR's commits (R5)? Proposed: the block first, comments later.
3. A TUI (a ratatui reader over the same `read` model), or the pager only? Proposed: the pager first, and a TUI only if review in the terminal turns out to be the daily path.
4. `--json` shapes: the datums as stored, or a reading model (the same view-model as the micro)? Proposed: the micro's view-model, so the CLI, agents and the GUI agree.
