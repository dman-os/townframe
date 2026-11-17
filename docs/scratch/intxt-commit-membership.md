# intxt: which intxts hold a commit, and following commits through rewrites

Status: Proposal, revision 2, for the operator's review
Owner: intxt lane
Updated: 2026-11-17 (revision 2: commits live with the intxt; no #keyRef)
Scope: Where an intxt keeps the commits it holds, so "which intxts hold commit X" is one datum-set query, and how a rewritten commit (rebase, amend, force-push) is matched to the commit an intxt holds. This is the lookup this week's deliverable needs (ledger D62: open a GitHub PR on the phone and find its intxts).

Revision 1 proposed `#keyRef` lists in the entries and `x-content-hash` on `gitCommit`. The operator rejected that shape (2026-11-17, `keyref-critique.md`): the docs design shouldn't grow `x-` semantics for this, and the datum-set index already answers "datum with slot X". Revision 1 also assumed one shared repository document for all commits. The operator rejected that too: most commits never reach trunk, so commits are shared only between related intxts.

## The PR names its intxts (operator, 2026-11-17)

The primary link is the PR description. A PR and its intxts are many to many, so the description carries one line per intxt:

```
intxt: dtxt:datum:z5Kd…/org.example.intxt/main
intxt: dtxt:datum:z9Qa…/org.example.intxt/main
```

The text is an ADR 035 datum reference to the intxt's marker in its descriptor document. The drawer id is the descriptor document's id, the same on every node, so it is global and needs no new URL kind. `Intxt::reference` writes it, `day x intxt show` and `use` print it as the `link:` line, and `Intxt::named_in` reads every intxt a text names (intxt_core, so the phone micro can use it). Built in fc73b020 on feat/intxt-impl. The commit lookup below is the fallback, for PRs opened without the line and for "which other intxts hold these commits".

## Today

- Capture writes `gitCommit/<sha>` into a repository document whenever it observes a commit. Nothing looks for an existing repository document before creating one, so there is at least one per node, and possibly one per worktree.
- `gitCommitEntry/<tip>` in the intxt names a range by its tip and base, as plain strings, and carries the range's pack. A commit inside the range is "reachable from tip and not from base", which only a git walk answers.
- A rewritten commit is a stranger: the PR shows new shas, the intxt holds the old ones (D53), and re-running `add --branch` adds the new commits as a new range with no link from old to new.

## Proposal

1. **Commits live with the intxt.** `add` writes `gitCommit/<sha>` for every commit of the range into the intxt's commits document. The slot is the sha. A commit's presence there is the membership: no list in the entry, no membership datum, no reference type.
2. **The lookup is the datum-set index.** "Which intxts hold X" is the datum-set predicate `HasKey { gitCommit/<sha> }`, which already exists (`big_cabinet_core::query::Predicate`), over the documents this node holds and the caller can read. Each hit document is an intxt's commits document, and from there the lookup reaches the intxt in one more hop (or zero, if the commits live in the descriptor document).
3. **Related intxts share a commits document; unrelated ones don't.** `day x intxt new --with <intxt>` (a stack, a follow-up) shares that intxt's commits document, so overlapping commits are stored once. Otherwise each intxt has its own. Two unrelated intxts holding the same commit each have a copy. The sha is the same identity in both, so nothing needs to coordinate.
4. **`gitCommit` in an intxt carries no `raw`.** The entry's pack already carries the commit objects, and git verifies them against their shas when it imports the pack. The datum keeps the readable fields (parents, author, committer, message, changed files) for the phone and the review views.
5. **Capture stops writing `gitCommit`.** The op log already names shas, and the objects stay in the local repository until `add`. The repository document keeps identity only (`gitRepo`: root commits, remotes) and the clone's refs.
6. **Rewrites are `rewrite/<old sha>` datums** `{ successor: "<new sha>", how: hook | patchId | jjChange }`, in the same commits document as the old commit. They are written where the rewrite is seen:
   - `hook`: git's `post-rewrite` hook gives exact old → new pairs on amend and rebase.
   - `patchId`: when the rewrite wasn't seen (a teammate's force-push, a rebase on GitHub), `add` matches the new range's commits to held ones by `git patch-id --stable`, the mechanism `git range-diff` uses.
   - `jjChange`: from the change evolution intxt already records.

   The lookup follows rewrites: if no intxt holds the PR's sha, find `rewrite` datums whose successor is that sha. That is a value query, not a slot query, so the successor needs its own slot: write `rewrittenFrom/<new sha>` → `{ predecessor: "<old sha>" }` alongside. Then each hop is another `HasKey`.
7. **No `#keyRef`, no `x-content-hash`** for intxt. a5928798 (the `x-content-hash` on `gitCommit`) is reverted, and feat/intxt-keyref-entries is dropped. Whether the cabinet keeps them is for the operator and the nodules lane.

## Local state, not datums (operator, 2026-11-17)

Only what another machine needs is a datum. Everything else lives in the worktree's `<git dir>/intxt/state.json`, which capture already keeps:

- **Which intxt the worktree is on.** Built in 98fc2c82 (`new`, `use`, defaults for `add`/`show`/`inbox`, `status`).
- **Incremental cursors (to build):** the last reflog entry read per ref; commits already seen with their patch-ids, so `add` and rewrite matching only process new commits.
- **Datums:** the commits an intxt holds and the rewrite links. Reflogs are local and expire (git's default drops unreachable entries after 30 days), and the phone must follow rewrites, so neither can stay in git.

## Costs

- **Size:** one `gitCommit` without `raw` is a few hundred bytes, most of it the message and the file list. A 400-commit range (PR #84's size) adds about 400 datums and roughly 100–200 KB to the intxt's commits document, written once at `add`. The 500-datum wall is gone (e44932ce).
- **Duplication:** unrelated intxts over the same commits each hold a copy. That's the price of not sharing commits across unrelated work.
- **patch-id matching** is approximate. A rebase with conflict resolutions changes the patch-id, and that commit isn't matched. That's honest: it is a different change. The `hook` route has no such gap, but only the machine that did the rewrite sees it.

## Open

1. Where the commits document lives: its own document in the intxt's drawer (so `--with` can share it), or the descriptor document (one fewer hop, but not shareable). Proposed: its own document.
2. Should capture install git's `post-rewrite` in the clones it records, or only read the reflog after the fact? The hook is exact; the reflog gives only a ref's from/to, not per-commit pairs.
3. **The query needs an anchor** (nodules lane, node-22). *Resolved 2026-11-17 (operator): the lookup becomes a cartridge index built by processors over local-state SQL; no root drawer. The rest of this item is kept for the reasoning.* `HasKey` alone is not one, and only a node-wide authority may send an unanchored predicate. A micro's or agent's capability must send `And[InDrawer{drawer, transitive, at_least: Read}, HasKey{gitCommit/<sha>}]`, and `Or` over drawers doesn't anchor. Proposed: a per-user root drawer for intxts that holds every intxt drawer (created on first `new`), so the phone sends one transitive query per commit. The answer is `PollDatums` with `after: 0, hold_ms: 0`: its events carry doc id, key, heads and version, not values, so it's already a light membership check.
4. ~~Human edits~~ Deferred (operator, 2026-11-17) with the collaborative VCS work (D59's file documents).
