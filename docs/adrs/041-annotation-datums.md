# ADR 041: Annotation datums: comments, threads, structure, seen state

**Changed 2026-11-19 (operator, ledger D70):** annotations are content, not drawer metadata, and no fixed slot names their document. Each writer (an attested actor, which today is a node's operator) keeps its own notes document, a content document of the intxt's drawer found by an `org.example.intxt.notes/main` marker naming the writer, so two nodes' first comments cannot race for one document and orphan one (§6). A thread's state is written in the document that holds its root comment (§3), and a reader's seen records in the reader's own notes document (§5). A reply points at its parent with a concrete reference, so a thread can span documents (§2). §2's `creator.id` example is a datum reference to the session's `agentSession/main`, as ledger D69 built it.

**Changed 2026-11-18 (operator, ledger D69):** comments carry `creator` and `generator` (W3C Web Annotation) as a claimed participant beside the cabinet's attested actor, so an agent and the operator on one node are two sides of a review. `creator.id` names a recorded session today and a long-lived identity later (§2).

**Changed 2026-11-12:** recorded work has no file documents (ledger D59, ADR 040 §4), so a comment on code targets a commit or a worktree op with a path, a line range and a quote, and is carried across revisions by git's diff mapping instead of a CRDT cursor (§2). Cursors stay for text held in documents.

**Status:** Proposed. Supersedes the notes, threads, structure and seen-state sections of the earlier ADR 036 ("intxt datums for the review loop").

**Depends on:** ADR 035 (references and locations), ADR 036 (intxt core, generic datums). Not intxt-specific: any drawer can hold annotations on anything it can reference. Evidence: ledger F6, F8, F10.

Tags are generic (`org.example.*`, ADR 036 §8 Q1 decides the namespace owner).

## 1. Prior art

- **W3C Web Annotation Data Model**: an annotation has 1+ `target`s and 0+ `body`s and a `motivation`. A target is a source plus selectors; `TextQuoteSelector { exact, prefix, suffix }` re-finds text when positions drift, and a target may carry both a position and a quote. Motivations are an open list (assessing, bookmarking, classifying, commenting, describing, editing, highlighting, identifying, linking, moderating, questioning, replying, tagging). A reply is an annotation whose target is another annotation. There is no resolved state.
- **Patchwork** (inkandswitch, `comments-view`): threads live inside the commented document (`@comments.threads`), each with `refs` (automerge sub-document URLs), `isResolved` and a list of comments; a comment's content may instead be a reference to another document, rendered inline by the tool registered for its type.
- **Leaflet** (`pub.leaflet.comment`, atproto): `subject`, `plaintext`, `reply.parent`, and an attachment quoting the commented text. schema.org `Comment` threads with `parentItem`.

We take W3C's shape (targets, quote selectors, motivation), an explicit parent for replies (Leaflet, schema.org), and Patchwork's thread-level resolved flag. Unlike Patchwork, comments do not live in the commented document: the commenter often has read-only access to it (a recorded file, someone else's session), and a comment on two documents has no single home.

## 2. Comments

```jsonc
// org.example.intxt.comment/<uuidv7>
{
  "targets": [
    { "source": { "ref": "dtxt:datum:zRepo/org.example.intxt.work.gitCommit/4c1f…" },   // a commit, or a worktreeOp for uncommitted content
      "path": "src/big_cabinet_core/reference.rs",                       // code targets only
      "lines": { "start": 120, "end": 124 },                              // 1-based, in that path's content at the source
      "quote": { "exact": "fn parse(", "prefix": "pub ", "suffix": "value: &str)" } }
  ],
  "body": { "mime": "text/markdown", "content": "Why not reuse the fragment here?" },   // the generic note shape
  "motivation": "commenting",                                             // open; W3C's list
  "replyTo": "dtxt:datum:zNotes/org.example.intxt.comment/0193…"                // replies only; the parent's own notes document, which may be another writer's
}
```

- **Targets** are any `dtxt:` locations: lines of a file at a commit or a worktree op, a text range in a document at a pinned state, a whole document or datum (no fragment), a commit, a session's tool call, another comment, a structure datum. A target's `source` is a `#datumPin` (ADR 035 §2.6): the location, and the heads it was read at when pinned (W3C names this member `source` too). A span of datums (turns 20–45) is a target whose `source` is the first datum and `to` (a `#datumLocation`) the last; `to` is in the same document and is read at `source`'s heads, so a span has one pin. Endpoints that need separate pins are separate targets.
- **Code** is targeted by its source, a `gitCommit` or a `worktreeOp` (ADR 040 §3), plus `path`. Both are written once, so the source needs no heads. The content is the path's blob in the commit's tree, or the path's content at that op (ADR 040 §3), and `lines` count in it. This maps to a GitHub review comment (commit, path, line, side) with nothing in between.
- **Text in a document** (a note, a collaborative worktree's file document) has positions given either as a line range at the pinned heads (`lines`) or as an Automerge cursor range in the fragment (`#content/%5B12@a3f-40@a3f%5D`, the brackets percent-encoded). Cursors can't be written by hand: an agent would need a tool to turn a line into a cursor. A line range at a pin is exact, because the pin fixes the text, so it is the hand-writable form; a tool may add the cursor, which carries the position to later revisions more cheaply than quote matching. Readers accept either.
- **Placing a comment in a diff** (claytxt, 2026-11-10). A viewer showing a diff between two commits or ops maps a target's source to a side by equality: the diff's base is the deletions side, its head the additions side, and `lines` are then that side's line numbers. A target at neither end of the diff on screen is carried to the head side through git's diff between its own content and the head's: lines outside changed hunks shift by the hunks above them, and lines inside one are re-found by quote within that hunk. If neither places it, it is shown as outdated at file level. On a real rebase, this placed all 1130 comments with known ground truth on the right lines, where CRDT cursors put 2 on wrong lines with no sign of it (F25). Text in documents is re-resolved through its cursor, else its quote. A range is drawn from start to end and its thread is placed after its last visible line; that placement is the viewer's policy, not the datum's.
- **File-level targets** (no fragment, no `lines`) are shown in the file's header. They follow a rename through git's rename detection (`git diff -M`) between the source and the revision on screen.
- **`quote` is required on text targets.** It checks every carried position: a reader compares the text found there with `quote`. A cursor never fails to resolve: one whose text was deleted resolves to a neighbouring position (F6), so a stale anchor looks valid. A reader compares the resolved text with `quote`; on a mismatch it re-attaches by quote matching or shows the comment as outdated. On a real rebase (F6), 785 of 888 anchors resolved onto identical text and quote matching recovered 99 of the other 103. Tools fill `quote`; people's clients fill it for them. A comment about deleted lines targets the earlier state.
- **Replies** carry `replyTo` and `motivation: replying`, so a thread is one reference-index query (ADR 035 §2.6) rather than a walk over every comment's targets.
- **Body** uses the generic note shape (ADR 036 §5). A body that is itself a datum to render (a suggested change, an embedded result) is a reference instead, rendered by the renderer registered for its tag (ADR 036 §4), as Patchwork renders typed comment content.
- **`creator` and `generator`** (W3C Web Annotation §3.3.1, Agents) name who wrote the comment and the software that produced it, as a claim. The cabinet's attestation (ADR 036 §1, rule 4) stays the verified layer: which actor signed the write. The two differ because one node's socket admits every caller from the node's uid as the operator (ADR 043, Context), so the operator and an agent running `day` in the same session are one attested actor. `creator` is what tells a reviewer's comment from the agent's. A reader shows `creator` as claimed until the attested actor is the creator's own: once a client pairs (ADR 043), the plugin's actor signs its own writes and the claim can be checked against it.

  ```jsonc
  "creator":   { "type": "Software", "id": "dtxt:datum:zSess…/org.example.intxt.work.agentSession/main" },   // or { "type": "Person" }: the operator, when no agent is detected
  "generator": { "type": "Software", "name": "claude-code", "version": "2.1.322" }
  ```

  - **Detection** follows the cross-tool convention: `AI_AGENT` (then `AGENT`) names the agent, and a tool-specific variable gives its session (Claude Code: `CLAUDE_CODE_SESSION_ID`). With none set, the creator is a `Person`. `generator` is the parsed `AI_AGENT` value.
  - **`creator.id` is a datum reference to whatever identifies the participant**: today the session's `agentSession/main` datum, in the session document capture recorded for that session id, and nothing when the session is not recorded. A long-lived identity (an agent lane that spans many sessions, a member of a swarm) is a later datum that `creator.id` can name instead, with no change to this shape (ledger D69).
  - **Time** stays the datum's change time; there is no `created`.

## 3. Thread state

```jsonc
// org.example.intxt.threadState/<root comment uuid>
{ "resolved": false }
```

One slot per thread, in the notes document that holds the thread's root comment, which exists before anyone resolves the thread, and written by anyone with Write on it. Resolving and reopening at the same time leaves one of the two values under Automerge's rules, and the DocService does not expose the overwritten one (`docs/wiki/docs.md`, Conflict model); the loser sees the thread in the other state and acts again. That is acceptable for a flag; if it proves not to be, the flag becomes a competing set (an array cleared and pushed on every write, so concurrent writes leave both values). Whose turn a thread is waiting on is derived (the side that didn't write last), not stored.

## 4. Structure

```jsonc
// org.example.intxt.structure/<uuidv7>         (D9: tasks, decisions, sections, groupings)
{ "kind": "task",                         // open: task | decision | section | …
  "title": "Move references to dtxt:", "body": { "mime": "text/markdown", "content": "…" },
  "status": "completed",                  // open, optional; ACP's spelling: pending | in_progress | completed
  "targets": [ { "source": { "ref": "dtxt:datum:zSess/org.example.intxt.work.promptTurn/t20" }, "to": "dtxt:datum:zSess/org.example.intxt.work.promptTurn/t45" } ],
  "parent": "dtxt:datum:zNotes/org.example.intxt.structure/0193…",
  "order": "15" }                         // fractional index: siblings sort by this string; between "1" and "2" write "15"
```

A guided review is a set of `section` structures with `order`. Adapters can map a harness's task lists here; ACP plans stay in the session (ADR 040 §6).

## 5. What a reader has seen

```jsonc
// org.example.intxt.seen/<actor uuid>/<range>  (one writer: that reader, in the reader's own notes document)
{ "datums": { "org.example.intxt.comment/0193…": ["zbd7…"] },   // per slot: the datum heads last read
  "reviewed": { "<ref or layer>": "<position>" } }       // vocabulary-defined: e.g. a ref target last reviewed
```

"New" is any slot whose current datum version differs from what is recorded here: one `ListDatums` at current heads (per-slot versions, no values) compared on the client. Datum heads are global, so the comparison is the same on every device. `PollDatums` cursors are node-local and can't be synced seen state. Seen state is split by slot range because one map datum hits the native read budget at about 5,400 entries (F10). A reader's seen records are in the reader's own notes document (§6), the one document they write that no one else does. How a reader picks ranges (by the first characters of a comment's slot, today) is open.

## 6. Where annotations live

In an intxt, annotations are content documents of the intxt's drawer (ADR 036 §2), not drawer metadata: a comment is something people said, and a drawer's meta documents describe the drawer. They are not a shared document with a fixed name either. A meta document's `(root, kind, name)` slot, or any pointer slot, holds one value, and two nodes that comment before they sync each create a document and the last writer's wins the slot, orphaning the other's comments (the live two-node run of 2026-11-18). Get-or-create on the synced roster cannot fix that for a writer that is offline.

So each writer has a notes document of its own:

```jsonc
// the notes document of one writer: a content document in the intxt's drawer
{
  "org.example.intxt.notes/main": { "writer": "<actor uuid>" },   // the marker: this is a notes document, and whose
  "org.example.intxt.comment/<uuidv7>": { … },                     // the writer's comments and replies
  "org.example.intxt.structure/<uuidv7>": { … },
  "org.example.intxt.threadState/<root comment uuid>": { … },      // for threads whose root is in this document (§3)
  "org.example.intxt.seen/<actor uuid>/<range>": { … }            // the writer's own seen records (§5)
}
```

- **A writer is an attested actor**, which today is a node's operator. Agent sessions on one node share their node's notes document (command ids already dedupe within a node, so the race is only between nodes), and each comment still carries its own `creator` (§2).
- **Discovery is by tag.** A reader lists the drawer's documents carrying `org.example.intxt.notes/main` and reads each marker. No slot names "the" notes document, so there is nothing to race for. A writer finds its own current document by the marker naming it, and makes one (marker included, in the same write that creates it) when it has none.
- **Threads span documents.** A reply is written in its writer's own notes document and names its parent with a concrete reference, so a thread is every comment whose `replyTo` chain reaches a root, whichever document each is in.
- **Rotation.** When a writer's notes document nears the ~500-datum update limit (F10), the writer starts a new one, as worktree op logs rotate (ADR 040 §7); the marker finds both. Not built yet.
- **Commits** follow the same pattern (ADR 040 §4): each writer's `add` writes its commits into a commits document of its own, marked `org.example.intxt.commits/main` with the writer, and the blob inventory of a drawer is named per writer (`blobInventory/intxt-<writer>`).

Outside intxts, any drawer can hold annotations the same way.

## 7. Open questions

1. **Suggested changes**: `motivation: editing` with a body that is a patch or a replacement text; shape deferred until the loop needs it.
2. **Verdicts** (D18): `motivation: assessing` later.
3. **Anchoring non-text targets** (images, structured datums): the fragment's automerge path covers structured positions; media fragments are deferred.
