# intxt: which intxts hold a commit, and following commits through rewrites

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-17 (operator answers: PR descriptions, local state, human edits)
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

## The PR names its intxts (operator, 2026-11-17)

The primary link is the PR description, not the commit lookup. A PR and its intxts are many to many: an intxt holds any number of PRs' commits, and a PR can belong to several intxts. So the description carries one line per intxt:

```
intxt: dtxt:datum:z5Kd…/org.example.intxt/main
intxt: dtxt:datum:z9Qa…/org.example.intxt/main
```

The text is an ADR 035 datum reference to the intxt's marker in its descriptor document. The drawer id is the descriptor document's id, the same on every node, so it is global and needs no new URL kind. `Intxt::reference` writes it, `day x intxt show` and `use` print it as the `link:` line, and `Intxt::named_in` reads every intxt a text names (intxt_core, so the phone micro can use it). Built in fc73b020 on feat/intxt-impl.

The commit-key lookup in this proposal stays for what a description can't answer: PRs opened without the line, and "which other intxts hold these commits". A commit trailer would also travel with cherry-picks, but it rewrites history and clutters every commit, so it stays optional at most.

## Local state, not datums (operator, 2026-11-17)

Only what another machine needs is a datum. Everything else lives in the worktree's `<git dir>/intxt/state.json`, which capture already keeps (last snapshot, op sequence, open tool-call windows, the worktree's documents):

- **Which intxt the worktree is on.** Built in 98fc2c82: `new` puts the worktree on the intxt it creates, `use` shows or changes it, `add`/`show`/`inbox` default to it, `status` says it.
- **Incremental cursors (to build):** the last reflog entry read per ref, so a scan only reads new entries; commits already seen with their patch-ids, so `add` and rewrite matching only process new commits.
- **Datums:** the commits an entry holds (#keyRefs) and the rewrite links. Reflogs are local and expire (unreachable entries after 30 days by default), and the phone must follow rewrites, so the links can't stay in git.

## Costs

- **Entry size:** one key reference is about 80 bytes. A 400-commit range (PR #84's size) is about 32 KB in one datum. The 500-datum wall is gone (e44932ce). If ranges much larger than that show up, move the list into a membership datum per range (the nodules lane's threshold).
- **Write cost:** `add` already walks the range to build the pack, so listing the commits costs nothing extra.
- **patch-id matching** is approximate. A rebase with conflict resolutions changes the patch-id, and that commit isn't matched. That's honest: it is a different change. The `hook` route has no such gap, but it only works on the machine that did the rewrite.

## Open

1. Should `gitRewrite` live in the repo's document (with the `gitCommit` datums) or in the intxt? Proposed: the repo's document, because a rewrite is a fact about the repo, not about one intxt.
2. Should the capture hook install git's `post-rewrite` in the clones it records, or only read the reflog after the fact (what `GitObservation` sees today)? The hook is exact. The reflog gives only the ref's from/to, not per-commit pairs.
3. ~~Human edits~~ Deferred (operator, 2026-11-17) with the collaborative VCS work (D59's file documents): only that layer sees individual edits, so per-line authorship belongs there.
