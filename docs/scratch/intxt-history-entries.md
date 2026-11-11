# intxt: how an intxt names commits and tracks refs (proposal for ADRs 036 and 040)

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-12
Scope: The datum shape behind ledger D53 (an intxt names commits and tracks refs, separately) and D54 (a ref versus a ref as seen on a clone). Behaviour is in FDR 008 (W2, W3, W6); this is only the data.

## Today

An intxt includes git history with `repoEntry { repo }`, a reference to a repository document, and nothing else. That says "this intxt is about this repository", not which commits or refs. So a reader can't tell what the intxt is about without guessing from its sessions and worktrees, and nothing records that a ref is tracked.

## Proposal

Extend `repoEntry` instead of adding new entry kinds, so one entry per repository still says everything the intxt claims about it:

```jsonc
// org.example.intxt.work.repoEntry/<repo doc>
{
  "repo": "dtxt:datum:zRepo/org.example.intxt.work.gitRepo/<root sha>",
  "commits": [ { "base": "<sha>", "tips": ["<sha>"] } ],    // named, fixed: reachable from tips, not from base
  "changes": [ "<jj change id>" ],                        // named jj changes, followed through rewrites
  "tracks":  [ { "ref": "refs/heads/feat/refs", "base": "<sha>", "clones": ["<clone uuid>"] } ]
}
```

- **`commits`** holds the resolved revset, never the revset text. `main..feat/refs` is stored as its base and tips at the moment it was named, because re-evaluating a revset later gives a different answer.
- **`changes`** holds jj change ids. A reader follows each change's evolog: every commit that change has had is in the intxt, the latest is its current form, and earlier ones are its revisions.
- **`tracks`** names a ref, its base, and the clones whose observations count. A clone is added when someone runs `add --branch` on that machine; it defaults to the clone the command runs in. Clones are never merged automatically, because shared root commits only suggest one project (ADR 040 §8). The tracked ref's revisions are the union of the listed clones' recorded `refMove`s for that name. Two clones that disagree show as diverging positions.
- **Lists are Automerge lists**, so two people adding at once both land, as `body.order` does.

Sessions and worktrees keep their own entries. `new --branch` and `add --commits` write the session entries they derive (the sessions whose edits landed on those commits), because deriving them needs the ops' causes, which a reader shouldn't have to recompute.

## What it costs

- Readers resolve `tracks` against several clones' observations. Under the granularity proposal those are revision documents, so it's one document per recorded move.
- A clone that never runs `add --branch` contributes nothing, even if it moved the ref. That's deliberate: only what an intxt names is in it.

## Open

1. Should `tracks` default to every clone of the repository that this node knows, instead of only the clone where the command ran? That's more convenient, but it is automatic merging by root commit, which ADR 040 §8 rules out.
2. Is `changes` enough for jj, or does tracking a jj bookmark also need `tracks` (with `ref` naming a bookmark)? Proposed: yes, `tracks` with a bookmark name, so both VCSs work the same way.
