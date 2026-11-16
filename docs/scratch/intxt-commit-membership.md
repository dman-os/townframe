# intxt: which intxts hold a commit, and following commits through rewrites

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-17
Scope: How an intxt references the commits it holds, so "which intxts hold commit X" is one #keyRef query (ADR 035 §2.9), and how a rewritten commit (rebase, amend, force-push) is matched to the commit an intxt holds. This is the lookup this week's deliverable needs (ledger D62: open a GitHub PR on the phone and find its intxts).

## The constraint

A PR names commits by sha, and so do its review comments. The phone has to answer "which intxts hold these commits" with one query per commit. That query is `QueryDatumRefs { Key { "org.example.intxt.work.gitCommit/<sha>" } }`, which finds the datums that hold a `#keyRef` to that key (ADR 035 §2.9, landed on big-node-next as be159fca..24c3211c).

## What is wrong today

1. **Commits inside a range are not referenced at all.** `gitCommitEntry/<tip>` holds `tip` and `base` as plain strings (`intxt_core::work::GitCommitEntry`). The commits between them are only "reachable from tip and not from base", which takes a git walk to answer. A key query for any commit except the tip finds nothing, and the tip isn't found either, because it isn't a `#keyRef`.
2. **`gitCommit` doesn't declare its content hash.** Admission refuses a `#keyRef` whose target kind lacks an `x-content-hash` declaration (§2.9). 907091ec added `raw` (the commit object) but not the declaration `x-content-hash: { algorithm: gitObject, type: commit, content: raw }`.
3. **A rewritten commit is a stranger.** After a rebase or amend, the PR shows new shas. The intxt still holds the old ones (D53, deliberately), so the lookup misses. Today the only remedy is re-running `add --branch`, which adds the new commits as a new range and keeps no link from old to new. jj changes don't have this problem: `jjChange.commits` is the change's evolution.

## Proposal

1. **Each entry references every commit it names.** `GitCommitEntry` gets `commits: [#keyRef]`, which lists every commit in the range, resolved when the entry is written (the entry already says the range is "resolved when it was named and never re-evaluated"). `tip` and `base` become `#keyRef`s too. The same goes for `JjChangeEntry`, by way of `jjChange.commits`. One datum, one query, and nothing extra to keep in sync. The nodules lane agrees (2026-11-17), and noted that a separate membership datum only pays off if ranges get large.
2. **`gitCommit` declares `x-content-hash`** in intxt's cartridge, so admission verifies the slot against `raw` and accepts the references.
3. **Rewrites are recorded as `gitRewrite/<old sha>` datums**, `{ successor: #keyRef, how: hook | patchId | jjChange }`. They are written where the rewrite is seen:
   - `hook`: git's `post-rewrite` hook hands exact old → new pairs on amend and rebase. Morph does this; it's the one thing in morph's git model we lack.
   - `patchId`: when the rewrite wasn't seen (a teammate's force-push, a PR rebased on GitHub), `add` matches new commits to held ones by `git patch-id --stable`, the same mechanism `git range-diff` uses.
   - `jjChange`: from the evolution we already record.

   The lookup then also follows rewrites: query the PR's sha. If no intxt holds it, query for `gitRewrite` datums whose successor is that sha, and repeat with the old sha. It's the same key query, used once more per hop.
4. **`add --branch` after a rebase becomes optional for the lookup.** Re-running it still packs the new commits, so they're readable on another machine. But finding the intxt no longer depends on it.

## Costs

- **Entry size:** one key reference is about 80 bytes. A 400-commit range (PR #84's size) is about 32 KB in one datum. The 500-datum wall is gone (e44932ce). If ranges much larger than that show up, move the list into a membership datum per range (the nodules lane's threshold).
- **Write cost:** `add` already walks the range to build the pack, so listing the commits costs nothing extra.
- **patch-id matching** is approximate. A rebase with conflict resolutions changes the patch-id, and that commit isn't matched. That's honest: it is a different change. The `hook` route has no such gap, but it only works on the machine that did the rewrite.

## Open

1. Should `gitRewrite` live in the repo's document (with the `gitCommit` datums) or in the intxt? Proposed: the repo's document, because a rewrite is a fact about the repo, not about one intxt.
2. Should the capture hook install git's `post-rewrite` in the clones it records, or only read the reflog after the fact (what `GitObservation` sees today)? The hook is exact. The reflog gives only the ref's from/to, not per-commit pairs.
3. Human edits (from the morph comparison): an op with no `cause` is a change made outside any tool-call window. Should the PR view show these per op from the start, with line-level blame onto the commit coming later? Proposed: yes. It needs this proposal's rewrite map to survive amends.
