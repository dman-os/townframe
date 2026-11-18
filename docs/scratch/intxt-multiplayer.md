# intxt multiplayer audit

Status: Proposal (audit), for the operator
Owner: intxt lane
Updated: 2026-11-18
Scope: What intxt looks like with several people, their devices and their agents sharing intxts: what works, what is decided but unbuilt, and the gaps. Evidence: a sweep of ADRs 019, 022, 035–038, 040, 041, 043, 049, FDRs 007/008 and the ledgers, plus a live two-node run on 2026-11-18 (two scratch nodes on the laptop, loopback). Related: ledger D69 (named participants).

## Scenarios

| | Scenario | Stresses |
|---|---|---|
| A | A reviews B's intxt from A's node; B's agent picks up A's comments | sharing, sync, cross-node attribution, the agent loop across people |
| B | Two people and their agents add to and edit one intxt | concurrent `add`/`edit`, whose capture lives where |
| C | Someone never invited follows the intxt link in a PR description | `via=`, relays, public vs member drawers |
| D | One person on laptop, mothership and phone | one participant or several node actors; inbox and seen across devices |
| E | Many agents across several people (swarms) | long-lived identities, authority, scale |

## What the live run showed (scenario A)

1. **Sharing works.** `day drawers add-member --access write <drawer> <peer>` on the owner, then `day drawers remember <drawer>` on the reviewer. The reviewer's `show` lists the title, both commit ranges, the tracked ref and the agent session.
2. **Turning networking on means hand-editing `node.json`** (a `peers` section with `loopbackOnly`/`listenPort`); `day peers add` only appends a bootstrap ticket and needs a daemon restart. No command says "make this node reachable".
3. **A reviewer cannot comment without the code locally.** `comment` resolves its target in the git repository of the worktree it runs in; outside a repository it refuses, and in an unrelated one the commit "is not a commit". The intxt carries the commits as packs, but they are thin by default: they assume the reader has the base commit.
4. **The owner lost its own notes document after the reviewer commented.** After the reviewer's write, every read of the intxt's notes document on the owner failed with "document is not locally available", persistently. A restart alone does not do this (checked on a third, peerless node). **Cause (nodules, from both nodes' databases):** a duplicate meta-document creation race, not keyhive. `ensure_notes_doc` relies on its fixed command id `intxt-notes-<drawer>` to converge, but command ids dedupe per node: the reviewer had not yet synced the owner's notes entry, created its own document under the same id, and wrote the same roster slot. Sync kept the reviewer's, so the owner resolves the slot to a document it never had, and its own two comments are orphaned in the first one. The commits document (`intxt-commits-doc-<drawer>`) and the blob inventory (`intxt-inventory-<drawer>`) are created the same way and race on two people's first `add`. Nodules is making `CreateMetaDoc` get-or-create on the synced roster (retry-later while the root isn't synced, no silent repointing of an occupied slot); until then intxt cannot rely on command ids converging across nodes.
5. **`add --branch` on a pushed branch adds nothing.** The branch's commits are taken since the merge base with its upstream, and a pushed PR branch's upstream is itself.

## Per scenario

**A. Reviewing someone else's intxt.** Works: drawer membership as sharing; authorship attested per actor across nodes (ADR 036 rule 4), so a remote reviewer's comment is genuinely theirs. Gaps: review needs local git (above); what a reviewer must hold to comment is unspecified (today: `write` on the whole drawer, because comments go into a shared notes document; ADR 041 §6 hints at per-commenter notes documents "only for access"); B's agent has no way to wait for comments (`inbox --wait` is missing), and nothing covers it being offline.

**B. Two writers in one intxt.** Works: per-writer slots and content-derived keys converge (ADR 036 rule 2); `body.order` is an Automerge list; commits are write-once; `edit` refuses a stale base with exit 3 and is a no-op when the change already holds. Gaps: `edit` is all-or-nothing, so two people changing different entries both hit exit 3; title and description are last-writer-wins and clobber silently; each person's capture stays on their node, so B's ops, uncommitted work and rewrite pairs never reach A (ADR 040 §9.3); who may `add` or `edit` is not addressed; a document reachable through two drawers (`new --with`) is unverified.

**C. A stranger with a PR link.** Works: `via=` is parsed, printed and ignored for identity (ADR 035); the PR block reads and writes; the relay alpha stores ciphertext for drawers it is a member of (ADR 038). Decided, unbuilt: public reading through keyhive's app-public pattern (ADR 038, node requirements R15). Gaps: `via=` grants nothing and there is no request-access flow, so a stranger can only be invited by a member who is already connected to them; the line between a public intxt and its private layers (sessions, raw ops, diffs as child drawers) is only proposed; nothing validates what a PR block claims.

**D. One person, several devices.** Works: one's own drawers sync laptop ↔ mothership ↔ phone through relays (the D62 phone milestone). Gap, the largest: **there is no person above the node.** The operator is the node's key (ADR 035), and each remote node is its own actor (ADR 037). So reading on the laptop doesn't mark anything seen on the phone, and `inbox` treats a comment from your own other device as someone else's. ADR 041 §5 assumes one reader across devices; FDR 007 promises it. ADR 043's pairing enrolls a client with its node; it is not a device joining a person. Revoking a lost device is unaddressed.

**E. Swarms.** Decided: the plugin as a subsystem actor under intxt (D49, ADR 043, unbuilt); comments carry a claimed `creator`/`generator` (D69, in progress), with long-lived identities as a later layer. Gaps: a `creator` is a claim, so across people one agent can claim another's session; what another person's agent may do in a shared intxt is unaddressed; seen state and notes-document rotation are sized for humans; there is no presence or claiming, so agents would duplicate work on one thread.

## Gaps, ranked

1. **No person identity above the node** (D). Breaks inbox and seen across one's own devices today, and is the base the D69 identity layer will need.
2. **No join or authority path for collaborators** (A, C): request access, and what commenting requires short of `write` on everything.
3. **Review needs local git** (A): the intxt isn't self-contained for a reader.
4. **Shared documents created on first use race across nodes** (A, B): notes, commits and inventory documents. The cabinet fix is nodules'; per-participant notes documents (question 2) would also take notes out of the race.
5. **Concurrency beyond append** (B): `edit` merges nothing; title and description clobber.

## For the operator

1. **Person identity.** Should one person's devices be one participant (a person actor that device nodes act for, or a certificate chain from person to node), and should it come before or with the D69 identity layer?
2. **Commenting authority.** Per-participant notes documents (each commenter writes only their own; readers merge them by tag), so commenting needs no write on the intxt? This also removes the first-comment race.
3. **Self-contained review.** Should an intxt shared with someone outside the repo carry full packs (or name the repository's remote for the base), with a `day x intxt checkout` that materializes the code for reading and commenting?
4. **Joining by link.** Is a request-access flow (a stranger asks through `via=`, a member approves) in scope for the alpha, or does sharing stay invite-only?
5. **`edit` under concurrency.** Merge non-overlapping entry changes and refuse only real conflicts?
