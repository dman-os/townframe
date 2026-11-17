# ADR 036 restructure: proposal

Status: Superseded 2026-11-10 by ADR 036 (intxt core), ADR 040 (work capture) and ADR 041 (annotations), after operator review (ledger D31). Kept for the reasoning; §1's "discovery by tag instead of descriptor pointers" became layers. Decisions it builds on: ledger D27–D30.

## 1. Split

ADR 036 today is "datums for the review loop" and assumes the review loop is the point. Under D28 the review vocabulary is one citizen among many. Proposal:

- **ADR 036: intxt core.** What an intxt is, independent of any vocabulary: one drawer (D1); its documents are roster entries; identity and description through generic datums (§2); any datum of any tag may live in it or point into it with `dtxt:` locations (ADR 035); vocabularies are found by tag (`PollDatums` with `InDrawer` + `HasTag`, ADR 025), not through pointers in a descriptor; how readers render tags they don't know (ledger P4, shell renderer collection, closed placements); the multiplayer, hand-writable and signature-authorship rules that every vocabulary inside an intxt follows.
- **New ADR (040): agent work capture vocabulary.** Worktrees, git objects, agent sessions, and how they reference each other (§4, §5). The review loop uses it plus the generic comment datums; it adds no datums of its own beyond what §3 lists.

Consequence: the descriptor's `historyDoc` and `notesDoc` pointers go. They existed only so a reader could find the docs; discovery by tag across the drawer does that for every vocabulary, including ones added later. `repo` (a git remote URL) moves to the worktree/repo datums (§4), since an intxt may span repos.

## 2. Generic datums

Generic means: owned by a shared namespace, not intxt, so any micro that knows them works on intxt content and the other way round. An intxt slant is allowed where the generic one is underspecified, as fields added under `Extra`, not as a fork.

| Datum | Fields | Prior art |
|---|---|---|
| `title` | `title`, `description?` (markdown) | Patchwork stamps `@patchwork.title` on every doc type, and its datatypes implement `getTitle`/`setTitle`; Leaflet and Standard.site use `title`/`name` + `description`; schema.org `name`/`headline` |
| `text` | `text` (Automerge Text), `mediaType?` (`text/markdown`, `text/x-rust`, …), `path?` | a file and a note both are a text body; see below |
| `comment` | `targets: [location]`, `quote?` per target, `body` (markdown), `motivation` (open string), `replyTo?` | W3C Web Annotation (below), Patchwork comment threads, Leaflet `pub.leaflet.comment`, schema.org `Comment.parentItem` |
| `threadState` | `thread` (reference to the root comment), `resolved: bool` | Patchwork `isResolved`; W3C has no resolved state |

**File on top of note.** A file doc and a note doc both hold one text body. Proposal: the body is the generic `text` datum in both, so any text viewer or editor micro can open a file doc, and the intxt-specific part of a file doc is only a small provenance datum (`worktreeFile`: which worktree and path, §4). A note is a doc with `title` + `text` and nothing else. Whether file docs should rather be `text` with `path` set (no provenance datum) depends on §4's question of whether a file doc belongs to one worktree.

**Comments follow W3C Web Annotation's shape**, renamed to our conventions:
- `targets` ↔ W3C `target` (1+). Each target is a `dtxt:` location (path = the datum, fragment = position, e.g. a cursor range), plus the pin as the sibling `refHeads` (ADR 035 §2.8).
- `quote: { exact, prefix?, suffix? }` ↔ W3C `TextQuoteSelector`. This is exactly our re-attachment quote (F6: required on code anchors and checked on every resolution); the W3C model also allows a selector to carry both a position and a quote, which is our cursor + quote pair.
- `motivation` ↔ W3C motivation, an open string vocabulary: `commenting`, `replying`, `questioning`, `assessing`, `editing` (a suggested change), `tagging`, `linking`, … (W3C's list of 13). Review verdicts (D18) would be `assessing` later.
- Replies: W3C makes a reply an annotation whose target is the parent annotation with motivation `replying`. That needs no extra field, but threads then have to be rebuilt by following targets. Leaflet and schema.org use an explicit parent (`reply.parent`, `parentItem`). Proposal: `replyTo` (a datum reference to the parent comment) plus `motivation: replying`, so a thread is one reference index query (ADR 035 §2.6).
- Authorship is the signature (L5); W3C's `creator`/`created` are not fields.

**Where comments live.** Patchwork stores threads inside the commented document (`@comments`). We can't in general: the commenter may hold read-only access to the target (a recorded file doc, someone else's session), and a comment on two documents has no single home. Comments live in docs of the intxt drawer that commenters can write (a notes doc per intxt, or one per writer) and point at their targets. That makes the comment vocabulary usable outside intxt too: any drawer can hold comments on anything it can reference.

**Who owns the namespace** (title, text, comment, threadState) is open: big_cabinet_core's own vocabulary, or a shared "core datums" plug. Either way not `org.example.intxt`.

## 3. Sessions: ACP names

ACP's nouns (agentclientprotocol.com, read 2026-11-10): **session** (`sessionId`, with `cwd` plus `additionalDirectories`, i.e. several roots), **prompt turn** (a `session/prompt` and everything until its `stopReason`), **content blocks** and message chunks (`user_message_chunk`, `agent_message_chunk`, `agent_thought_chunk`), **tool call** (`toolCallId`, `title`, `kind`: read | edit | delete | move | search | execute | think | fetch | switch_mode | other; `status`; `locations: [{path, line?}]`; content including `diff {path, oldText, newText}`), **plan** (`entries` of `{content, priority, status}`, replaced whole on every update). ACP has no subagent concept.

Proposal: tags `agentSession`, `promptTurn`, `toolCall`, `plan`; field names follow ACP (`kind`, `status`, `locations`, `stopReason`). "Session" alone is ACP's word, but as a datum tag it is ambiguous among other vocabularies (login sessions, sync sessions), hence `agentSession`. Subagents: each its own `agentSession` doc with `parent` (D30), our extension since ACP has none. ACP's multiple roots are what makes one session span several worktrees (§4).

## 4. Worktrees (D29)

**What a worktree is in the datums.** A `worktree` datum per checkout directory: the machine (node actor), the absolute path, the repo (git remote URLs as fetch hints), and the git dir it shares with sibling worktrees. Recorded by that machine's recorder only, so it is a single-writer, linear history: a `worktreeOp/<uuid>` log with a sequence number, one entry per observed change, like jj's operation log.

**Ops** (open vocabulary): `edit` (a file changed; points at the tool call that did it when known), `commit`, `refMove` (with kind: commit | amend | rebase | reset), `checkout`, `reset`, `unknown` (a change observed without a cause). Checkout and reset need no revert semantics: the file docs follow the working tree, so a checkout is one op after which the touched files hold the new content. What the previous branch looked like stays reachable at earlier pins.

**Correlating sessions to worktrees.** A tool call's `cwd` and `locations` name paths; the recorder resolves them to the worktree containing them (the deepest worktree root). A session touching three worktrees in two repos produces ops in three logs, each pointing back at its tool call. Attribution (which turn produced which commit) is then a viewer-side reading of these links (D30), not a computed field.

**File docs.** One per worktree per modified path (a path that some op touched), holding the generic `text` body (§2). Git objects (`gitCommit` keyed by sha) are shared by worktrees of one repo; a commit pins the file doc states of the worktree it was made in.

**Questions for the operator:**

1. **Write granularity.** D29 makes file docs follow the working tree, which reverses D26 ("never on working-tree edits"). Are file docs written on every observed edit, or only at op boundaries that matter (commit, checkout, reset, end of a prompt turn), with per-edit detail staying in the session's tool-call diffs? Every edit is the faithful option. Boundaries are smaller and drop the states nobody pinned.
2. **What ships.** "Only what is referenced ships": an intxt referencing a file doc at a pin syncs that doc's whole history, including every earlier state (Automerge can't sync a prefix without its past, and can't drop a middle state). If a worktree's file doc saw a secret or junk before the referenced state, it ships with it. Options: (a) accept, and keep file docs per worktree per intxt so unrelated work doesn't share a doc (P1's cost); (b) the local recording stays outside intxts and is never synced, and referencing a state copies that state into an intxt-owned doc (a fresh doc per intxt, written at the referenced states only), losing per-edit Automerge provenance across the boundary but keeping secrets local; (c) ADR 007 branches (P2), which share genesis and so carry the history anyway. I lean (b): local, faithful, per-edit recording that never syncs, and intxt docs that receive only referenced states. That is P1 made concrete.
3. **Worktree identity across machines.** Two machines with checkouts of the same repo are two worktrees (D26's "two machines make two intxts" becomes "two worktrees"). Can one intxt include both? Under the vocabulary-neutral core nothing forbids it; the viewer shows them as separate lines of work.
4. **Refs.** With worktrees as the unit, is `gitRef` still a datum, or only an op (`refMove`) in the worktree log plus a derived "current ref" view? Keeping it makes "list revisions of branch X" one read; dropping it removes a multi-writer conflict case.
