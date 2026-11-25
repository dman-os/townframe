# ADR 041: Annotation datums: comments, threads, structure, seen state

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
  "replyTo": "dtxt:datum:zNotes/org.example.intxt.comment/0193…"                // replies only
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
- No `creator` or `created`: authorship is what the cabinet attests (ADR 036 §1, rule 4), and time is the datum's change time.

## 3. Thread state

```jsonc
// org.example.intxt.threadState/<root comment uuid>
{ "resolved": false }
```

One slot per thread, written by anyone. Resolving and reopening at the same time leaves one of the two values under Automerge's rules, and the DocService does not expose the overwritten one (`docs/wiki/docs.md`, Conflict model); the loser sees the thread in the other state and acts again. That is acceptable for a flag; if it proves not to be, the flag becomes a competing set (an array cleared and pushed on every write, so concurrent writes leave both values). Whose turn a thread is waiting on is derived (the side that didn't write last), not stored.

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
// org.example.intxt.seen/<actor uuid>/<range>  (one writer: that reader)
{ "datums": { "org.example.intxt.comment/0193…": ["zbd7…"] },   // per slot: the datum heads last read
  "reviewed": { "<ref or layer>": "<position>" } }       // vocabulary-defined: e.g. a ref target last reviewed
```

"New" is any slot whose current datum version differs from what is recorded here: one `ListDatums` at current heads (per-slot versions, no values) compared on the client. Datum heads are global, so the comparison is the same on every device. `PollDatums` cursors are node-local and can't be synced seen state. Seen state is split by slot range because one map datum hits the native read budget at about 5,400 entries (F10). How a reader picks ranges (by the target notes document, by the first characters of the slot) is open.

## 6. Where annotations live

In an intxt, annotations are one part (ADR 036 §2), entered by `annotationsEntry/main` and found by tag: any document in the drawer holding these tags. Annotations go into a shared notes document. Every comment is its own UUIDv7 slot, so writers sharing one document never conflict. When the current notes document nears the ~500-datum update limit (F10), the next writer starts a new one, as worktree op logs rotate (ADR 040 §7). A notes document per commenter would only be needed for access (a commenter with no write access to the shared one), not for conflicts. Outside intxts, any drawer can hold annotations the same way.

## 7. Open questions

1. **Suggested changes**: `motivation: editing` with a body that is a patch or a replacement text; shape deferred until the loop needs it.
2. **Verdicts** (D18): `motivation: assessing` later.
3. **Anchoring non-text targets** (images, structured datums): the fragment's automerge path covers structured positions; media fragments are deferred.
