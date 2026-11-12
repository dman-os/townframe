# intxt: do filesystem worktrees need file documents?

Status: Proposal, for the operator's review; experiment done (ledger F25, F26), recommendation below
Owner: intxt lane
Updated: 2026-11-12
Scope: What an intxt carries as code (ledger thread of 2026-11-12: options A and B), and whether capture of a git worktree on a filesystem should write Automerge file documents at all. Replaces the question in `intxt-granularity.md` if accepted.

## Why file documents exist

They were introduced for five reasons:

1. **Carry the code:** a phone has no git, so an intxt must hold file contents (S5).
2. **Anchor comments** with CRDT cursors that survive edits and rebases (S7).
3. **Record edits no tool reports:** `sed`, formatters, generated files, seen only by observing the filesystem (F7).
4. **Record uncommitted states:** the operator reviews work before it is committed (FDR 007).
5. **Collaborate:** later, a file document is the editing surface (dpaths, a virtual worktree).

Only 2 and 5 need a CRDT. 1, 3 and 4 need the content at each observed point, and content-addressed snapshots (git blobs, `git hash-object`) give exactly that.

Reason 2 is weaker than assumed. In the rebase anchor test (F6), cursors "never fail": an anchor on deleted text silently resolves to a neighbour, so the quote is required regardless. Quote matching alone recovered 99 of 103 moved anchors (3 ambiguous, 1 lost). The cursor adds little that the quote doesn't.

## Proposal C: two kinds of worktree

**A filesystem worktree (git or jj on disk) is recorded without file documents:**
- The **worktree log** (ADR 040 §3) stays: a linear, single-writer record of what happened. That covers edits (with their tool-call cause), checkouts ("at time X, HEAD was Y"), commits, amends and rebases.
- Each op records, for the paths it changed, the **content-addressed snapshot** (the git blob id) before and after. Uncommitted states are blobs too: `git hash-object` doesn't need a commit.
- **Commits and trees are git's own objects,** recorded as `gitCommit` datums plus the blobs they need.
- **Nothing is a CRDT,** because nothing has two writers. A checkout is just an op, so there's no "does checkout mutate the file document" question.

**A collaborative worktree, which doesn't exist yet (the dpaths/Delta design), is file documents:**
- It is created from a commit, and every edit happens in the document, whoever makes it.
- Checking out another commit makes a new worktree, not a CRDT operation.
- This is where file documents and cursors belong.

## What an intxt then carries

Content-addressed objects only: the commits it names, the blobs they and the included ops need, the sessions, and the worktree log segments for included work.

- **Granularity mostly disappears.** An intxt references exactly the blobs it needs, and identical content dedupes across machines and intxts (D54's rubric, applied all the way down).
- **No "10,000 edits but the intxt wants the last 50":** the intxt includes those 50 ops' snapshots, not a document's whole history.
- **Secrets:** a blob that captured a secret is simply not included, with no squash replay.
- **Email-patch independence:** a commit's patch is its blobs plus `gitCommit` metadata, so `git am` can reproduce it.
- **GitHub:** a review comment anchors to (commit, path, line, side), which maps to (commit, blob, line range, quote) with nothing in between.

This is A from the previous discussion, generalised to uncommitted work. B's file documents remain, but only for collaborative worktrees.

## What it costs

- **Comment anchors** on recorded work re-anchor across revisions by quote and diff mapping, the way GitHub and Gerrit do, instead of by cursor. F6 suggests this loses little; the experiment measures it.
- **A blob per changed file per op.** Git's own store has the same cost. Blobs are deduplicated, and large or binary files already go to the blob store (D43). The experiment measures the size against file-document history.
- **Migration:** ADR 040 §4 and the recorder's file-document writes (`intxt_core::client::record`) are replaced by blob writes. The recorder already handles blobs for large and binary files, so the path exists.

## The experiment: replay real work, walk the use cases

Material: this lane's own work on 2026-11-11 and 2026-11-12. That's the `feat/intxt-impl` history, with merges of -next and agents' branches, and the Claude Code transcripts on disk. We replay the transcripts and git reflog through capture, then build intxts under B and under C.

For each use case, we write down what the operator or agent does, what is recorded, what the intxt contains, and whether the use case works:

1. **Live capture** (U1): sessions, edits including `sed` and rustfmt, commits.
2. **Package after the fact** (W2): an intxt for "D50: recordObservations to client" out of a day that also did the rename and the ACP recorder.
3. **Carving** (U4): split the day into rename, D50 and ACP intxts.
4. **Review across a rewrite** (U6): comments on revision 1, then a merge or rebase. How many land correctly and how many are wrong-but-confident?
5. **Email patch:** `git am` from the intxt alone, on a clean clone. Same commit sha?
6. **GitHub mapping:** each comment as a GitHub review comment (commit, path, line, side), on paper.
7. **Handoff** (U14): what a fresh agent reads, and how long it takes.

Measured: the size of each intxt (B against C), how many documents it shares, how many anchors survive (correct, recovered, ambiguous, lost), and patch fidelity.

## Results (ledger F25, F26)

| Question | B: file docs | C: content-addressed snapshots |
| --- | --- | --- |
| Comments carried across a real rebase (1130 with ground truth) | 2 on the wrong line, confidently | all correct, with quote plus git's diff mapping |
| Rebuilding commits from the intxt alone | not applicable | 31/31 reproduce their sha, given the original committer and date |
| Size for a rebased branch (both revisions) | 341 KB plus 186 KB metadata | 1.28 MB packed, or 194 KB of gzipped patches per revision |
| Size for two days of capture (every HEAD move in 5 worktrees, including rebases and merges that bring in other lanes' work) | 2.65 MB, since Automerge stores each change as a diff; base content is repeated per worktree | 6.3 MB delta-packed, because the prototype stored a **whole file** per version as git does (Cargo.lock: 22 versions, 2.5 MB; the ledger: 57 versions, 1.4 MB). 98% of those blobs are already in git; our own uncommitted states add about 97 KB. Our tool edits alone: 275 KB |
| Packaging work from a long-lived worktree ("rename") | 96% of the shared history is unrelated to the intxt | only the intxt's own ops and blobs |
| Build and resolve time | seconds | milliseconds |

Size is not where C wins. C has two separable parts: the **identity** of each version, a content hash recorded per op, which gives dedup, verification and the link to git; and its **content**, which need not be a whole file. Storing each op's content as the patch from the previous version, with a whole file only the first time a path is seen (usually already in git), should cost about the size of the diffs, comparable to B. Not measured yet. C wins on anchors, commit fidelity and not carrying unrelated history.

Two other findings shape the design whichever option wins:
- **Most edits bypass the tools.** 455 Bash calls wrote files, against 53 Edit/Write calls. 32% of tool-touched paths reached their commit with content the tool reports don't explain. Capture has to observe the filesystem around every tool call (F7), not trust tool reports.
- **Attribution leans on the window, not the tool.** Tool reports attribute 12% of commit hunks to a session. A per-tool-call observation window (which tool call was running when the file changed) is what makes attribution work.

## Recommendation

C, for filesystem worktrees:
- **The worktree op log stays** (ADR 040 §3), with one op per observed tool-call window, recording per changed path the content hash after the op.
- **Content is addressed by git blob id, stored as patches.** Each op stores the patch from the previous version; a whole file is stored only on first sight, when git doesn't already have it.
- **Commits are git's own objects,** carried by the intxt (D58).
- **Comments anchor to (commit or op, path, line range, quote)** and carry across revisions with git's diff mapping.
- **File documents only for collaborative worktrees,** later, with dpaths.

What changes in code:
- ADR 040 §4 (file documents) and the granularity proposal are superseded.
- The recorder (`intxt_core::client::record`) writes blob ids instead of file-doc splices; it already writes blobs for large and binary files.
- Annotations target commit or op content (ADR 041).

## Open

1. Is "filesystem worktrees have no file documents; collaborative worktrees are file documents" the right line?
2. Uncommitted work: snapshot at every op (proposed), or only at turn ends?
3. For the experiment: our lane's work as the material (proposed), or a nodules-lane branch?
