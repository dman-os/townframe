# intxt: document granularity (proposal for an ADR 040 amendment)

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-12
Scope: How the recorded work (ADR 040) is split into documents, so that what an intxt includes is only the work it is about. Decisions go to the ledger and then into ADR 040; nothing here is decided.

## The constraint

An intxt is self-contained by its drawer: every document its items need is entered in it, and keyhive governs who reads them. That is only acceptable if those documents hold the work the intxt is about and little else. Automerge can't share part of a document's history: sharing a document shares everything it ever held. So **no document may grow with unrelated work over time**. A document is either about one piece of work, or small and identity-only.

## Where ADR 040 breaks it today

| Document | Holds | Grows with |
| --- | --- | --- |
| Repo | `gitRepo`, every `gitCommit` of the project (sharded past ~400) | every commit on every branch, forever |
| Clone | `gitClone`, every `gitRef` move, the jj op log | every branch and every jj operation |
| File | one per worktree per path, every edit | every branch the worktree ever had checked out |
| Op log | ~256 ops per document | bounded by count, but mixes whatever was done in that window |
| Session | one session | one session (fine) |

## Proposal

The organising unit is the **line of work**: one branch in one worktree, from the base it started on. Documents are cut along it.

1. **Revision documents carry commits and ref moves.** Each recorded move of a branch writes one revision document: the `refMove`, and the `gitCommit` datums newly reachable from the new position that this line of work hadn't recorded yet. Commits are content-addressed, so the same commit written into two revision documents is the same value (D13). Including a branch includes exactly its revision documents. The repo document keeps only the `gitRepo` identity, and the clone document only `gitClone`. Both are tiny and safe to share.
2. **A ref's history is its revision documents.** A small per-ref head datum in the capture drawer points at the latest one. It isn't shared, because readers get positions from the revisions they hold.
3. **File documents are per line of work:** one per (worktree, branch, path). When a branch starts, its file documents are seeded with the base content as a snapshot, with no history. Edits on the branch, including amends and rebases, accumulate in that document, so CRDT cursors and comment anchors track the code for the branch's whole life. Switching branches and back reuses that branch's documents. A comment carried to another line of work re-anchors by quote match, which S7 already defines as the fallback. A secret captured mid-branch is removed by ADR 040 §7's replacement document, as today.
4. **Op logs rotate per line of work**, then by count within it, so an op log never mixes branches.
5. **Sessions stay one document each.** Carving by turn range references the whole session document; splitting long sessions is a later optimisation if carving shows it's needed.
6. **jj:** the line of work is the jj change's lineage instead of a branch, keyed by change id. The jj op log moves out of the clone document into per-line documents, like the op logs.

## What it costs

- **More documents:** one per (branch, path) instead of one per path, and one per ref move. Each is smaller, and the alias-scan limit (F10, 500 datums) gets easier, not harder.
- **No cursor continuity across lines of work.** A comment made on `feat/a` and viewed on `feat/b` re-anchors by quote, never by CRDT position. That matches how reviews work: a comment belongs to the change it was made on.
- **Recording must know the line of work at every write.** Capture already observes the branch, but detached HEADs and mid-rebase states need a rule: a detached HEAD continues the line of work it left.

## Open

1. Is "one branch in one worktree, from its base" the right unit? An alternative is per intxt, but intxts are made after the fact, so recording can't key by them.
2. Does a rebase onto a new base start a new line of work, losing cursor continuity, or continue the old one (proposed: continue)?
3. Should the per-ref head live in the capture drawer only, or be shareable for readers that want "where is the branch now"?
