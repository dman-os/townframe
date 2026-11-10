# ADR 036: intxt datums for the review loop

**Status:** Proposed.

**Depends on:** ADR 019 (drawers), ADR 020 (documents and datum writes), ADR 021 (datum schemas and codecs), ADR 026 (drawer-native blobs), ADR 035 (the `dtxt:` reference scheme; to be written). Functional requirements: FDR 006 (intxt) and FDR 007 (the review loop). Decisions are tracked in `docs/scratch/intxt-ledger.md` (D-numbers below).

Datum tags use the placeholder authority `org.example.intxt` until the naming lane settles NSIDs.

## 1. What this has to support

FDR 007's loop: a harness adapter records a session and the commits it makes; an intxt gathers them; the operator reads code, commits and session on any device and comments on any of them; the agent reads the comments and replies; a rewritten branch becomes a new revision without losing a comment.

Every shape below is held to four rules:

1. **Records of history** (D8). A datum records something that happened or was said. Live editing, when it comes, happens on branch documents (ADR 007) and enters the record when something pins it.
2. **Multiplayer** (D13). Anything several writers produce is keyed so that each writer writes its own slot (a UUIDv7 instance, or a key derived from content such as a commit sha, which makes concurrent writes identical). A slot several writers share holds one flag whose last-writer-wins outcome is acceptable, and a concurrent write there is a conflict Automerge already reports.
3. **Hand-writable** (D23). An agent can create or answer any of these by writing the JSON itself, given this document. No field requires a computation the agent can't do (it may need to read a value to copy, never to derive one).
4. **Authorship is the signature** (ADR 020 §5–§6). No datum carries an author field; the signing actor of a datum's current version is who said it.

References are `dtxt:` URLs (ADR 035): `dtxt:datum:<doc>/<tag>/<slot>[?heads=…][#<automerge path>]`. The path names a datum, the query pins a version, the fragment selects a position inside the datum using automerge-repo's path grammar (`@n` index, `{json}` match, `[cursor-cursor]` text range, map keys).

## 2. Layout: one drawer, a few kinds of document

One intxt is one drawer (D1). Its documents:

| Document | Holds | Written by | Why separate |
|---|---|---|---|
| **Descriptor** (the drawer's descriptor doc) | `intxt/main` | whoever creates the intxt; title edits by anyone | The drawer's identity and the intxt's own metadata (D-S1) |
| **History** (one per intxt) | `gitCommit/<sha>`, `gitRef/<name>`, `refMove/<uuid>` | the recorder | Small, always synced; everything a revision needs except file contents |
| **File** (one per touched path per intxt) | `file/main` | the recorder | Lazy loading per file (F1: 404 files as one doc loads at ~514 MB, as per-file docs at ~75 MB) |
| **Session** (one per session; may be in several intxts, D21) | `session/main`, `turn/<id>`, `message/<id>`, `edit/<id>` | the recorder | Large, often private, sometimes shared between intxts |
| **Notes** (one per intxt) | `note/<uuid>`, `thread/<rootUuid>`, `structure/<uuid>`, `seen/<actor>` | everyone | The discussion; every writer writes its own slots |

The descriptor names the history and notes documents; sessions and file docs are found through the history and session datums that reference them, and all of them are roster entries of the drawer so they sync with it. A private session lives in a different drawer and is referenced only (FDR 006, Sharing); readers without access see an unresolved reference.

A file doc belongs to one intxt. Sharing one file doc across intxts that touch the same path would give unrelated intxts a common history that can't be separated later (ledger P1); stacked intxts that need continuity across file docs are later work.

## 3. Descriptor

```jsonc
// org.example.intxt.intxt/main
{
  "title": "Datum references move to dtxt:",
  "description": "markdown, optional",
  "historyDoc": "zHistoryDoc",
  "notesDoc": "zNotesDoc",
  "repo": { "origins": ["git@github.com:dman-os/townframe.git"] }   // fetch hint only, never identity
}
```

Concurrent creators of the same intxt would each write `historyDoc`/`notesDoc`; creation is a single command by one writer, so the last-writer-wins case is a programming error the CLI prevents by creating the descriptor and both docs in one call.

## 4. Code: file documents

```jsonc
// org.example.intxt.file/main            (one per touched path)
{
  "path": "src/big_cabinet_core/reference.rs",   // current path; renames update it
  "text": "…",                                    // x-automerge Text
  "binary": null                                  // or { "blob": "dtxt:datum:<doc>/org.example.daybook.blob/<slot>" } when not text
}
```

The recorder writes the first content it sees for the path (normally the base of the earliest commit it records) in one change, and every later state as **line-level splices** from that previous state (F1: `update_text` is character-level and 25× slower). Every observed state is appended, including states from rewritten history; nothing is rewritten. A state is identified by the document heads after the change that produced it, and commits pin those heads (§5).

A deleted file keeps its doc; its pinned state at the deleting commit is absent (§5). Binary and very large files carry a blob reference instead of text and are shown as changed, not diffed.

## 5. History: commits, refs and revisions

```jsonc
// org.example.intxt.gitCommit/<sha>      (key = the sha: concurrent recorders write identical values)
{
  "sha": "4c1f…",                          // full git object id, as git spells it
  "parents": ["9a0e…"],
  "changeId": "kxqoltvm…",                 // jj change id when known, else absent
  "author":    { "name": "dman-os", "email": "…", "at": "2026-11-10T21:04:11Z" },
  "committer": { "name": "dman-os", "email": "…", "at": "2026-11-10T21:04:11Z" },
  "message": "refactor(reference): dtxt scheme",
  "files": [
    { "path": "src/big_cabinet_core/reference.rs",
      "status": "modified",                // open: added | modified | deleted | renamed | copied
      "oldPath": null,
      "base": "dtxt:datum:zFileA/org.example.intxt.file/main?heads=3Mbq",   // state in the first parent; absent for added
      "head": "dtxt:datum:zFileA/org.example.intxt.file/main?heads=5Nkx" }  // state in this commit; absent for deleted
  ]
}
```

Git hashes stay in git's own spelling inside datums (they are git's identifiers, and `sha` is also the slot key); only Automerge heads use the URL heads encoding.

`files` lists what the commit changed relative to its first parent, each with the two pinned states, so a commit's diff is the Automerge diff between `base` and `head` on each file doc. A merge commit records its first-parent diff; other parents are reachable through `parents`.

```jsonc
// org.example.intxt.gitRef/<name>        (name = the ref, e.g. refs/heads/feat/intxt, percent-encoded in URLs)
{ "target": "4c1f…", "base": "9a0e…" }     // base: the commit the review compares against (merge base with the upstream)

// org.example.intxt.refMove/<uuid>       (one per observed move; append-only)
{ "ref": "refs/heads/feat/intxt", "from": "77b2…", "to": "4c1f…", "base": "9a0e…", "at": "2026-11-10T21:04:12Z", "kind": "rebase" }
```

- **A revision** is a ref position: the commits reachable from `target` back to `base`. Walking `parents` from the target visits only reachable commits; unreachable commits stay recorded and cost nothing extra (D17).
- **Revision history** is the list of `refMove`s for a ref, ordered by `at` (and by causality where clocks disagree). D17 noted that the `gitRef` datum's own Automerge history already records its past targets; `refMove` makes that list an ordinary readable datum, so an agent or a phone can list revisions without walking document history. `kind` is an open string (`commit`, `amend`, `rebase`, `reset`, `unknown`) recorded when the adapter knows it.
- **"What changed between revision 1 and 2"** is, per file, the Automerge diff between the head-state pins of the two revisions' tips. It needs no matching of commits across the rewrite. When both commits carry a jj `changeId`, per-commit pairing is available too.

Only the recorder moves refs. Two recorders for the same ref (two machines) would race on `gitRef`; each move still lands as its own `refMove`, so the race is visible rather than lost.

## 6. Sessions

A session document is a normalized transcript (S8, D7): our own datums with ACP-compatible names, never the raw log, thinking excluded by default.

```jsonc
// org.example.intxt.session/main
{ "harness": "claude-code", "harnessSessionId": "eb26ecaa-…", "title": "intxt design",
  "startedAt": "…", "cwd": "/run/media/…/townframe-p3N-3",
  "parent": "dtxt:datum:zParentSession/org.example.intxt.session/main",   // subagent/fork parent, if any
  "git": { "branch": "feat/intxt" } }

// org.example.intxt.turn/<harness turn id>
{ "seq": 41, "role": "user", "at": "…", "parentTurn": "<id>" }          // parentTurn keeps rewinds/forks as a tree

// org.example.intxt.message/<harness message id>
{ "turn": "<turn id>", "kind": "agentMessage",                          // open: userMessage | agentMessage | toolCall | toolResult | …
  "text": "…",                                                          // for messages
  "tool": { "name": "Edit", "kind": "edit", "summary": "reference.rs: replace parse()" } }   // for tool calls; results summarised, not stored whole

// org.example.intxt.edit/<id>             (id derived from the tool call id and index, or a UUID for filesystem-observed edits)
{ "turn": "<turn id>", "toolCall": "<message id>",                      // toolCall absent when observed on disk (formatter, sed, codegen)
  "observedBy": "tool",                                                 // open: tool | filesystem
  "path": "src/big_cabinet_core/reference.rs",
  "patch": "@@ -120,7 +120,9 @@ …" }                                    // unified diff, always (F2: recoverable from every harness)
```

Keys reuse the harness's own ids so a re-import converges on the same slots instead of duplicating. Message text is a plain string: a transcript is a record, not something edited collaboratively.

## 7. Notes, threads and structure

```jsonc
// org.example.intxt.note/<uuidv7>
{
  "targets": [
    { "ref": "dtxt:datum:zFileA/org.example.intxt.file/main?heads=5Nkx#text/[12@a3f-40@a3f]",
      "quote": { "exact": "fn parse(", "prefix": "pub ", "suffix": "value: &str)" } }
  ],
  "body": { "$type": "org.example.intxt.note#markdown", "text": "Why not reuse the fragment here?" },
  "motivation": "commenting",           // open: commenting | replying | explaining | questioning | flagging | suggesting
  "inReplyTo": "dtxt:datum:zNotes/org.example.intxt.note/0193…",   // replies only
  "at": "2026-11-10T22:15:03Z"
}
```

- **Targets** are any `dtxt:` references, so a note can be about a line range (a text cursor range in a file doc pinned at a commit's state), a whole file (no fragment), a commit (`gitCommit/<sha>`), a session message or edit, a structure datum, or another note. `quote` is the fallback for re-attaching when a cursor can't follow (moved code, F1); tools fill it, humans' clients fill it for them.
- A **span of datums** (turns 20–45) is a target with `from` and `to` references instead of `ref`.
- **Threads** are the reply trees under root notes. Their shared state is one slot per root:

```jsonc
// org.example.intxt.thread/<root note uuid>
{ "resolved": false }
```

Resolve and reopen at the same time is a visible conflict; anything else is one writer at a time. Whose turn a thread is waiting on is derived (the side that didn't write last), not stored; a stored attention field can come later if derivation proves wrong.

```jsonc
// org.example.intxt.structure/<uuidv7>    (D9: tasks, decisions, groupings; adapters can map harness todo lists here)
{ "kind": "task",                         // open: task | decision | section | …
  "title": "Move references to dtxt:", "body": "markdown",
  "status": "done",                       // open, optional
  "targets": [ { "from": "dtxt:datum:zSess/org.example.intxt.turn/t20", "to": "dtxt:datum:zSess/org.example.intxt.turn/t45" },
               { "ref": "dtxt:datum:zHistory/org.example.intxt.gitCommit/4c1f…" } ],
  "parent": "dtxt:datum:zNotes/org.example.intxt.structure/0193…",
  "order": 2.5 }                          // position among siblings; fractional so writers can insert between
```

A guided review is a set of `section` structures with `order`. Structure is a record too: revising it writes a new version of the same slot, and its history stays readable.

## 8. What each reader has seen

```jsonc
// org.example.intxt.seen/<actor id>       (one slot per reader; only that reader writes it)
{ "notes": { "org.example.intxt.note/0193…": "5Nkx", "org.example.intxt.thread/0193…": "8Qrt" },  // per slot: the datum heads last read
  "reviewed": { "refs/heads/feat/intxt": "77b2…" } }          // the ref target they last reviewed
```

"New comments" are the slots whose current datum version differs from the heads recorded here: one `ListDatums` at current heads (per-slot `DatumVersion`s, no values) compared on the client. Datum heads are global, so the comparison is the same on every device. A single doc-heads watermark was rejected: it needs two listings, one at historical heads (which materializes that version), and `PollDatums` cursors are node-local, so they can't be synced seen state (node lane, 2026-11-10). "New since I last looked" (Jane Street Iron's per-reviewer brain, `docs/scratch/intxt-prior-art.md`) is the diff between the revision at `reviewed` and the current target, per file, across any rebase in between. An agent uses the same datum to find what it hasn't read.

## 9. What needs a nodule

None of the loop's writes. Creating the intxt, recording history and sessions, commenting, replying, resolving, structuring and marking seen are plain datum writes (D2, D23). Waiting for change is a `PollDatums` long-poll on the notes doc. A nodule would only be needed to build revisions from a checkout inside the node instead of in the adapter, which depends on git in wasm (the gitoxide check in progress).

## 10. Open questions

1. **Cost of seen state.** Per-slot heads grow with the number of notes, and `ListDatums` on a large notes doc is unmeasured. Measure before the alpha; a `since`-heads read mode is on the node lane's deferred list if it's needed.
2. **refMove vs the ref datum's own history.** Keep the explicit append-only `refMove` list, or read past targets from the `gitRef` datum's history (D17) and drop `refMove`?
3. **Session size.** One session doc holds every turn's datums; the largest normalized session is estimated at 1–3 MB. Confirm with a real import before deciding whether long sessions split.
4. **Line-splice recorder for rewrites.** A rebase appends the rebased contents as splices from the previous state of the same file doc. Confirm with a real rebase that cursors made on the old commit resolve sensibly at the new one.
5. **Hex in cursors.** Automerge's cursor string is `[-]<counter>@<actor hex>`, so a text-range fragment carries hex even though URLs otherwise avoid it (D24). Either accept Automerge's spelling inside the fragment (it is their grammar) or re-encode the actor in base58 and diverge. ADR 035 decides.
6. **Evidence** (check results) is out of the loop's scope (FDR 007) and gets its own datums later.
