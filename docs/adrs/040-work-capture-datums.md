# ADR 040: Work capture datums: repositories, worktrees, git, jj, agent sessions

**Status:** Proposed. Supersedes the code, history and session sections of the earlier ADR 036 ("intxt datums for the review loop"), including D26's ref-driven file documents (ledger D29).

**Depends on:** ADR 036 (intxt core: layers, generic datums), ADR 035 (references), ADR 041 (annotations, which target these datums). **Capture side:** ADR 039 (the adapter that writes them; to be revised against this ADR). Evidence: ledger F1, F5–F7, F10.

Datum tags use the placeholder authority `org.example.work`.

## 1. The model

Agent work happens in directories on machines. Git and jj describe some of those directories, but they are views over the same files, not the place work happens: one agent session can edit three checkouts of two repositories, a jj workspace and a git worktree can be the same directory, and nothing about a session is contained in one branch's linear history. So the capture vocabulary is arranged by where change physically happens, and version control is layered over it:

```text
agent session ──tool calls──▶ worktree op log ──pins──▶ file docs
                                   │
               git / jj layers ◀───┘  (commits, refs, jj operations and changes,
                                       observed in a worktree, keyed by their own ids)
```

- A **worktree** is one checkout directory on one machine. Exactly one recorder (the one on that machine) writes its datums, so its history is single-writer and linear.
- **Git and jj objects** are keyed by their own identifiers and are the same datums whichever worktree observed them. What links them to content is the worktree op that observed them, which pins that worktree's file docs.
- **Agent sessions** reference the worktree ops their tool calls caused. A session belongs to no worktree.

This is a data arrangement, not a product decision: an intxt picks which of these layers it includes (ADR 036 §2), and a review of "one branch" is one reading of them.

## 2. Repositories and clones

Git has no repository identity. Two clones of one project share nothing but history, and one clone can hold several unrelated histories. The vocabulary separates three things, which also line up with jj's (repo, workspace):

| Datum | Is | Identity | jj equivalent |
|---|---|---|---|
| `gitRepo/<uuid>` | a project, as far as we can tell | a UUID minted by the first recorder; `roots` (its root commit shas) let recorders on other machines match it; `remotes` are fetch hints | — |
| `clone/<uuid>` | one `.git` directory (or jj repo) on one machine; refs and the jj op log live here | a UUID stored in the clone (`.git/intxt-id`) | repo |
| `worktree/<uuid>` | one checkout directory of a clone; HEAD, index and working files live here | a UUID stored in the worktree's own git dir (`.git/worktrees/<name>/intxt-id`, or `.git/intxt-id` for the main worktree) | workspace |

Paths are recorded (`path`, the current absolute path) but are not identity: worktrees move, are deleted, and a new one may reuse an old path for unrelated work. A stored UUID survives the first two and distinguishes the third.

```jsonc
// org.example.work.gitRepo/<uuid>
{ "roots": ["a1b2…"], "remotes": ["git@github.com:dman-os/townframe.git"] }

// org.example.work.clone/<uuid>
{ "repo": "dtxt:datum:zRepo/org.example.work.gitRepo/<uuid>", "machine": "dtxt:actor:zNode", "path": "/run/media/…/townframe-p3N-3/.git",
  "vcs": ["git", "jj"] }

// org.example.work.worktree/main         (one per worktree document)
{ "clone": "dtxt:datum:zRepo/org.example.work.clone/<uuid>", "id": "<uuid>", "path": "/run/media/…/townframe-p3N-3",
  "files": { "src/big_cabinet_core/reference.rs": "zFileA" } }   // path → file doc, for every path an op touched
```

Two machines with checkouts of one project are two clones, each with its worktrees; one intxt may include any of them as separate layers, and a viewer shows them as separate lines of work.

## 3. The worktree op log

```jsonc
// org.example.work.worktreeOp/<seq>      (seq: 0, 1, 2 … written only by the worktree's recorder)
{ "at": "2026-11-10T21:04:11Z",
  "kind": "edit",                         // open: edit | checkout | reset | commit | refMove | jjSnapshot | unknown
  "cause": "dtxt:datum:zSess/org.example.work.promptTurn/<id>#toolCalls/{\"id\":\"toolu_01…\"}",   // when known
  "files": { "src/big_cabinet_core/reference.rs": "zbd7…" },      // path → file doc heads after this op
  "git": { "head": "4c1f…", "ref": "refs/heads/feat/intxt" },    // HEAD after the op, when it changed or was observed
  "jj": { "operation": "e1f0…", "change": "kxqoltvm…" } }         // when the op came from jj
```

- **Single writer, sequential keys.** The slot is the sequence number, so the log is linear by construction and readers page it by key. It is the worktree's equivalent of jj's operation log, at the granularity the recorder observes.
- **Every observed change is an op.** An edit by a tool call, a formatter run inside a Bash call (filesystem-observed, F7), a commit, an amend, a rebase, a `git checkout` or `reset`. Checkout and reset need no revert semantics: file docs follow the working tree, so a checkout is one op after which the touched files hold the checked-out content, and the earlier content stays readable at earlier pins.
- **`cause`** points at the tool call that made the change when the recorder knows it (tool-reported edits, a Bash call's window). Changes with no known cause are `unknown`; a background process writing during a Bash call is attributed to that call (F7's limit). Which turn produced which commit is then a reading of these links by the viewer (D30), not a stored field.

## 4. File documents

One document per worktree per path that some op touched: the generic `path` and `note` datums (ADR 036 §5) plus provenance.

```jsonc
// file doc
{ "org.example.path/main": { "path": "src/big_cabinet_core/reference.rs" },
  "org.example.note/main": { "mime": "text/x-rust", "content": "…" },          // Automerge Text
  "org.example.work.file/main": { "worktree": "dtxt:datum:zWt/org.example.work.worktree/main" } }
```

- **Single writer.** Only the worktree's recorder writes it, so the multi-writer machinery the ref-driven design needed (reading only at pins, repairing concurrent splices, divergent refs) is gone. Content is still always read at a pin: an op's or a commit observation's.
- **Line-level splices** (F1: Automerge's character-level `update_text` is 25× slower). The first content of a path is written in one change.
- Binary and very large files carry a reference to a `Blob` datum instead of text and are shown as changed, not diffed. A deleted file keeps its doc; its pinned state at the deleting op is absent (`files` maps the path to null).
- Per-file documents keep loads small (F1: 404 files in one doc load at ~514 MB, as per-file docs at ~75 MB).

## 5. Git and jj layers

```jsonc
// org.example.work.gitCommit/<sha>       (key = sha; identical for every recorder, in the repo's document)
{ "parents": ["9a0e…"], "changeId": "kxqoltvm…",                       // jj change id when known
  "author":    { "name": "dman-os", "email": "…", "at": "…" },
  "committer": { "name": "dman-os", "email": "…", "at": "…" },
  "message": "refactor(reference): dtxt scheme",
  "files": [ { "path": "src/big_cabinet_core/reference.rs", "status": "modified", "oldPath": null } ] }

// org.example.work.gitRef/<clone uuid>/<ref name>
{ "target": "4c1f…", "base": "9a0e…" }  // base: what a review compares against (merge base with upstream)
```

- **Commits hold no pins.** A commit is content-addressed and the same in every worktree; which file doc states it corresponds to depends on where it was observed. The `commit` op in a worktree log carries the sha and the pins (§3), so the diff of a commit is the Automerge diff between the file doc pins at the op that observed its first parent and the op that observed it. A commit observed in two worktrees has two such pairs, both valid.
- **Refs belong to a clone**, since two clones' `refs/heads/main` are different refs; the slot is keyed by clone. Ref moves are worktree ops (`refMove` with `kind`: commit, amend, rebase, reset), so a ref's revision history is a filter over its clone's worktree logs, and the `gitRef` datum's own Automerge history keeps past targets too (D17). A `gitRef` has one writer, its clone's recorder.
- **A revision** is still a ref position: the commits reachable from `target` back to `base`. "What changed between revision 1 and 2" is, per file, the diff between the pins at the two `refMove` ops.

**jj is first class**, as its own layer over the same worktree log rather than translated into git terms:

```jsonc
// org.example.work.jjOperation/<op id>   (the clone's jj op log; a DAG, since jj ops can diverge)
{ "parents": ["…"], "description": "snapshot working copy", "at": "…",
  "heads": ["<commit id>", …], "bookmarks": { "feat/intxt": "<commit id>" } }

// org.example.work.jjChange/<change id>  (a change's evolution: the commits it has been)
{ "commits": ["<commit id>", …] }       // appended in evolog order
```

jj's working copy is a commit that it snapshots on every command, so a jj worktree's ops can name the jj operation and change they observed (`jj` on the op), and rewrite tracking comes from change ids instead of guessing a `refMove` kind. jj commits are also git commits when jj is backed by git, so they share `gitCommit/<sha>` datums.

## 6. Agent sessions

Names follow the Agent Client Protocol (ACP: session, prompt turn, tool call, plan; read 2026-11-10). The tag is `agentSession` because "session" alone is ambiguous among other vocabularies.

```jsonc
// org.example.work.agentSession/main     (one document per session; D7, F5: largest 2.67 MB normalized)
{ "harness": "claude-code", "sessionId": "eb26ecaa-…", "title": "intxt design",
  "cwd": "/run/media/…/townframe-p3N-3", "additionalDirectories": [],   // ACP's root set
  "parent": "dtxt:datum:zParentSess/org.example.work.agentSession/main" }   // subagent or continuation, ours (ACP has none)

// org.example.work.promptTurn/<harness turn id>
{ "seq": 41, "at": "…", "parentTurn": "<id>", "stopReason": "end_turn",
  "messages": [ { "id": "<harness id>", "seq": 0, "at": "…", "kind": "agentMessage", "text": "…" } ],
  "toolCalls": [ { "id": "toolu_01…", "seq": 3, "title": "Edit reference.rs", "kind": "edit", "status": "completed",
                   "locations": [ { "path": "/run/…/reference.rs", "line": 120 } ] } ] }

// org.example.work.plan/main             (ACP plan: replaced whole on every update; its history is the plan's history)
{ "entries": [ { "content": "Rewrite reference.rs", "priority": "high", "status": "completed" } ] }
```

- A session is a normalized transcript, never the raw log; thinking is excluded by default.
- Keys reuse the harness's ids, so a re-import converges on the same slots.
- Messages and tool calls live inside their turn: a long session has thousands of messages (3,483 in one, F5) but around a hundred turns, and a document refuses updates at about 500 datums (F10). Updating a datum replaces its signature rather than accumulating one (F10), so appending to the open turn on every hook is cheap. A session past the limit continues in a new document whose `parent` is the previous one.
- **Subagents** are imported, each as its own session document with `parent` (D30).
- **Which worktrees a session touched** is derived: each tool call's `locations` and `cwd` resolve to the deepest worktree containing them, and the recorder writes the link once, on the op (`worktreeOp.cause`); a tool call's changes are found through the reference index (ADR 035 §2.6).

## 7. Documents

How the datums above split into documents. Each split has a reason: one writer per document where possible, the ~500-datum update limit (F10), loading only what a view shows (F1), and access boundaries.

| Document | Holds | Writers | How many |
|---|---|---|---|
| Repo | `gitRepo/<uuid>`, `gitCommit/<sha>` | every recorder of the project (commits are content-addressed, so concurrent writes are identical) | one per project; commits shard into more repo docs past ~400 |
| Clone | `clone/<uuid>`, `gitRef/<clone>/<name>`, `jjOperation`, `jjChange` | that clone's recorder | one per clone |
| Worktree | `worktree/main` (lists op-log and file docs) | that worktree's recorder | one per worktree |
| Op log | `worktreeOp/<seq>` for one seq range | that worktree's recorder | one per ~256 ops; the worktree datum lists them |
| File | `path`, `note`, `file` provenance | that worktree's recorder | one per path an op touched |
| Session | `agentSession/main`, `promptTurn/<id>`, `plan/main` | the recorder of that harness | one per session, subagent and continuation |

For example, one day of this lane's work (one repo, one clone, one worktree, 30 changed files, a main session and five subagents) is about 40 documents: 1 repo, 1 clone, 1 worktree, 1–2 op-log, 30 file and 6 session documents, plus the intxt's descriptor and a notes document per commenter (ADR 041).

**Readers must expect gaps.** An intxt spread over many documents, some in other drawers, is never guaranteed to arrive whole or at once: a file doc may be missing, a session may be private, a pin may name heads not yet synced. Readers render what resolves and mark the rest, as a browser renders a page with a broken image (ADR 036 §1, ADR 035 §2.1).

**Removing a document.** One document per file means a file doc that captured a secret can be removed from the drawer's roster. Relays and honest peers then stop holding and serving it, and the recorder writes a replacement doc from the cleaned state. Removal does not retract what peers already synced (ADR 019: a stable id is not a promise that an audience change erases earlier disclosure).

## 8. Settled (operator, 2026-11-10)

- **What ships:** the worktree's documents themselves, at per-document granularity; unwanted documents are removed and replaced, not filtered (§7).
- **Granularity:** per observed change. The single writer per file doc puts this back in scope (D26's commit-only rule existed because ref-driven docs had several writers). Interactive editing over an intxt would edit a fork (ADR 007) live and fold back into the worktree's docs after a debounce, writing one op. The per-write dmeta and signature overhead for such fine-grained writes is not yet measured; the node lane is asked for a measurement and for whether intermediate changes can skip signing (ledger).
- **Repository matching:** shared root commits only *suggest* that two clones are one project; they never merge repos automatically. Forks and clones of a starter repo share roots and aren't the same project.
- **Directories outside any checkout:** an opt-in layer later (a worktree without a clone); nothing here blocks it.
- **jj:** record jj operations' and changes' identities and structure (ids, parents, descriptions, which changes and bookmarks moved), not the full content of jj's op store. A jj backend on top of intxt is a future VCS conversation.

## 9. Open questions

1. **Op cost.** Every observed change is an op datum plus file-doc writes. Op-log chunking keeps documents under the limit, but the per-write dmeta cost (§8) decides whether per-change granularity is affordable on large sessions.
