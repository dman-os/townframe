# intxt: how an intxt includes commits and tracks refs (proposal for ADRs 036 and 040)

Status: Accepted 2026-11-12 with its proposed defaults (ledger D61)
Owner: intxt lane
Updated: 2026-11-12
Scope: The datums behind ledger D53 (an intxt includes commits and tracks refs, separately), D54 (a ref versus a ref as seen on a clone), D57 (VCS-prefixed entries) and D59 (git objects as content). Behaviour is in FDR 008 (W2, W3, W6).

## Today

An intxt includes git history with `repoEntry { repo }`, a reference to a repository document. That says "this intxt is about this repository", not which commits or refs. So a reader can't tell what the intxt is about without guessing from its sessions and worktrees, and nothing records that a ref is tracked.

## Proposal

One datum per included thing, on the intxt's descriptor, keyed by the thing's own identity. Each one references the existing datum for that thing (`gitCommit`, `gitRef`) rather than copying it:

```jsonc
// commits named: everything reachable from tip and not from base
"org.example.intxt.work.gitCommitEntry/<tip sha>": { "tip": <ref gitCommit/<tip>>, "base": <ref gitCommit/<base>> }

// a jj change, followed through its rewrites by change id
"org.example.intxt.work.jjChangeEntry/<change id>": { }

// a ref tracked as one clone saw it
"org.example.intxt.work.gitRefEntry/<clone uuid>/<ref name>": { "ref": <ref gitRef/<clone>/<ref name>>, "base": <ref gitCommit/<base>> }
```

Why one datum per item rather than a list (ledger thread of 2026-11-12):
- **Set semantics:** the key is the content, so naming the same commits twice, or tracking the same ref on the same clone from two devices, converges on one slot instead of duplicating.
- **Authorship:** the cabinet attests who wrote each datum (ADR 036 rule 4), so each item records who included it.
- **Seen state** (ADR 041 §5) works per item.
- **Removal** deletes a slot; nothing is edited by index.
- **The reference index** answers "which intxts include this commit" (ADR 036 rule 1), because the entries are declared references, not bare shas.

Details:
- **A range is resolved when it is named.** `main..feat/refs` is stored as its tip and base at that moment, never as revset text, because a revset re-evaluated later gives a different answer.
- **jj changes** carry no fields: the key is the change id, and a reader follows the change's evolog. Every commit the change has had is in the intxt; the latest is its current form.
- **A tracked ref** is keyed like `gitRef` itself (clone and name), so one machine's view of `feat/x` is one entry. Tracking the same name on a second machine adds a second entry. Clones are never merged automatically, because shared root commits only suggest one project (ADR 040 §8). The intxt's revisions for that ref are the union of the entries' recorded moves; two clones that disagree show as diverging positions.
- **Sessions and worktrees keep their own entries.** `new --branch` and `add --commits` write the session entries they derive (the sessions whose observed edits landed in those commits), because deriving them needs the op log's causes and windows, which a reader shouldn't have to recompute.
- **`repoEntry`** stays as the repository's identity for the intxt.

## What it costs

- Three new datum kinds, one per kind of history an intxt can include.
- A reader resolving a tracked ref reads each listed clone's recorded moves.

## A dependency on the cabinet

A reference points into a document, but a content-addressed datum (`gitCommit/<sha>`) can sit in several documents with identical content. A rule is needed: a datum whose key is its content resolves by key, and any document holding that key is equivalent. That belongs in the reference ADR (035), in the nodules lane.

## Open

1. Should tracking default to only the clone where the command runs (proposed), or to every clone of the repository this node knows? The latter is automatic merging by root commit, which ADR 040 §8 rules out.
2. jj bookmarks: track them with `gitRefEntry`'s jj counterpart (`jjBookmarkEntry/<clone>/<bookmark>`), or is `jjChangeEntry` enough, given that jj doesn't need bookmarks? Proposed: `jjChangeEntry` only, until a use case needs bookmark tracking.
