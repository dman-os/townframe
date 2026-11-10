# ADR 041: Annotation datums: comments, threads, structure, seen state

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
    { "ref": "dtxt:datum:zFileA/org.example.intxt.note/main#content/[12@a3f-40@a3f]",
      "refHeads": ["zbd7…"],                                              // ADR 035 §2.8: the pin, a sibling
      "lines": { "start": 120, "end": 124 },                              // 1-based, at the pinned heads; or a cursor range in the fragment
      "quote": { "exact": "fn parse(", "prefix": "pub ", "suffix": "value: &str)" } }
  ],
  "body": { "mime": "text/markdown", "content": "Why not reuse the fragment here?" },   // the generic note shape
  "motivation": "commenting",                                             // open; W3C's list
  "replyTo": "dtxt:datum:zNotes/org.example.intxt.comment/0193…"                // replies only
}
```

- **Targets** are any `dtxt:` locations: a text range in a file document at a pinned state, a whole document or datum (no fragment), a commit, a session's tool call, another comment, a structure datum. A span of datums (turns 20–45) is a target whose `ref` is the first datum and `to` the last. A `refHeads` sibling pins every reference in its object, so a span's `to` shares its target's pin; a reference that needs a pin of its own goes in its own object.
- **Text positions** are given either as a line range at the pinned heads (`lines`) or as an Automerge cursor range in the fragment (`#content/[12@a3f-40@a3f]`). Cursors can't be written by hand: an agent would need a tool to turn a line into a cursor. A line range at a pin is exact, because the pin fixes the text, so it is the hand-writable form; a tool may add the cursor, which carries the position to later revisions more cheaply than quote matching. Readers accept either.
- **Placing a comment in a diff** (claytxt, 2026-11-10). A viewer showing a diff between two pins maps a target's `refHeads` to a side by equality: the diff's base pin is the deletions side, its head pin the additions side, and `lines` are then that side's line numbers. So `refHeads` must be exactly the heads of a pin the reader can name (an op's or a commit observation's), never a superset. A target pinned at neither end of the diff on screen is re-resolved through its cursor, else its quote, against the head side, and shown as outdated at file level if neither resolves. A range is drawn from start to end and its thread is placed after its last visible line; that placement is the viewer's policy, not the datum's.
- **File-level targets** (no fragment, no `lines`) are shown in the file's header. They survive renames because a renamed file keeps its document (ADR 040 §4).
- **`quote` is required on text targets.** A cursor never fails to resolve: one whose text was deleted resolves to a neighbouring position (F6), so a stale anchor looks valid. A reader compares the resolved text with `quote`; on a mismatch it re-attaches by quote matching or shows the comment as outdated. On a real rebase (F6), 785 of 888 anchors resolved onto identical text and quote matching recovered 99 of the other 103. Tools fill `quote`; people's clients fill it for them. A comment about deleted lines targets the earlier state.
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
  "targets": [ { "ref": "dtxt:datum:zSess/org.example.intxt.work.promptTurn/t20", "to": "dtxt:datum:zSess/org.example.intxt.work.promptTurn/t45" } ],
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
