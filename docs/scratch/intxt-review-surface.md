# intxt: the review surface and GitHub (proposal for next week, FDR 008 "Review")

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-12
Scope: The commands for reviewing an intxt (comment, reply, resolve, inbox, guide) and how they map to GitHub pull requests. Datums are ADR 041's, with code targets per D59 (commit or op, `path`, `lines`, `quote`). Next week's target (ledger D52: guided review, marks, inbox/reply/resolve; GitHub, reviews, comments, threads).

## Walkthroughs

### R1. The agent leaves a guide

**Agent**, done with the work, knows which parts matter and in what order. It writes sections (ADR 041 §4 `structure`, `kind: section`, ordered):

```
$ day x intxt guide qx "Why the cabinet change" --commits 4c1f..9a0e --turns k7:3-19
$ day x intxt guide qx "The recorder" --paths 'src/intxt_core/client/**'
```

Each section targets commits, paths, session turns, or any mix. A section with no targets is prose only.

### R2. The operator reads and comments

`day x intxt read qx` walks the guide in order: each section's text, then its diffs, then the turns it names. Work no section covers comes last, under "not in the guide", so nothing is hidden by a guide.

```
$ day x intxt comment qx 4c1f:src/big_cabinet_core/reference.rs:120-124 -m "Why not reuse the fragment here?"
comment kz on 4c1f src/big_cabinet_core/reference.rs:120-124
  undo: day x intxt rm qx kz
```

- **Targets** are spelled `<commit>:<path>:<lines>` for committed code, `<op>:<path>:<lines>` for uncommitted states, `<session>:<turn>` for a turn, `<comment>` for a reply. The command fills `quote` from the content at the target.
- `--side old|new` picks which side of a diff a line number counts on, as GitHub does; the target is then the commit's parent or the commit.

### R3. The agent picks it up

```
$ day x intxt inbox qx            # comments it hasn't seen, and threads waiting on it
$ day x intxt inbox qx --wait     # blocks until there is something
$ day x intxt reply kz -m "Done in 77b2" --resolve
```

- "Unseen" is ADR 041 §5's `seen` datum of the reader; "waiting on me" is derived: the other side wrote last.
- `inbox` prints each comment with its target resolved against the intxt's **current** revision: carried by git's diff mapping and checked by `quote` (F25), or marked outdated.

### R4. A rewrite between rounds

The agent rebases. The intxt still holds the old commits (it names commits, not refs, D53), so comments stay attached to what they were written on. `read` shows them on the new revision where the mapping carries them, and on the old commit otherwise. Nothing is lost when a branch is force-pushed.

### R5. GitHub

The pull request stays the pull request: intxt adds to it and doesn't replace it.

```
$ day x intxt github link qx https://github.com/dman-os/townframe/pull/84
$ day x intxt github pull qx      # import review comments and threads
$ day x intxt github push qx      # publish intxt comments that map to the PR's commits
```

| GitHub | intxt |
| --- | --- |
| review comment (commit, path, line, side) | comment, target (commit, path, lines, quote) |
| reply in a review thread | comment with `replyTo` |
| resolved thread | `threadState.resolved` |
| PR description, review summary | comment targeting the intxt itself |

- **Import** keeps a comment on its commit even after GitHub marks it outdated or loses it in a force-push: the intxt never loses the commit.
- **Publish** only sends comments whose target is a commit in the PR. Comments on sessions, turns or uncommitted ops stay in the intxt: they are what GitHub has no place for (the "cut" from the 2026-11-12 thread).
- Each imported or published comment records its GitHub id, so the next sync updates rather than duplicates.

## What it needs

- A `githubPullEntry/<owner>/<repo>/<number>` entry, and a `githubId` field on comments (or a mapping datum, if comments should stay GitHub-free).
- `gh`'s token for API calls (`gh auth token`), not a token of our own.
- The target syntax parser and the quote filler (git for commits, the keep ref for ops).

## Open

1. Is the noun "comment" (W3C, GitHub) or "note"?
2. `guide` as its own command, or `comment --section`?
3. GitHub sync: both directions this week (proposed: pull first, push second), or pull only?
4. Comments on an op (uncommitted state): publish to GitHub when that content is later committed unchanged, or never?
