# ADR 040: Work capture datums: repositories, worktrees, git, jj, agent sessions

**Changed 2026-11-12:** §4 states which datums are content-identified and which are location state (ledger D54), and when a file document may span machines (D55).

**Changed 2026-11-12 (later):** filesystem worktrees are recorded without file documents (ledger D59, `docs/scratch/intxt-file-docs.md`, F25–F27). Ops record git object ids (§3), content is git's own objects (§4), commit diffs are git's (§5), and file documents remain only for collaborative worktrees, later. This supersedes the file-document design of §4 and the granularity proposal (`docs/scratch/intxt-granularity.md`).

**Status:** Proposed. Supersedes the code, history and session sections of the earlier ADR 036 ("intxt datums for the review loop"), including D26's ref-driven file documents (ledger D29).

**Depends on:** ADR 036 (intxt core: layers, generic datums), ADR 035 (references), ADR 041 (annotations, which target these datums). **Capture side:** ADR 039 (the adapter that writes them; to be revised against this ADR). Evidence: ledger F1, F5–F7, F10.

Datum tags use the placeholder authority `org.example.intxt.work`.

## 1. The model

Agent work happens in directories on machines. Git and jj describe some of those directories, but they are views over the same files, not the place work happens: one agent session can edit three checkouts of two repositories, a jj workspace and a git worktree can be the same directory, and nothing about a session is contained in one branch's linear history. So the capture vocabulary is arranged by where change physically happens, and version control is layered over it:

```text
agent session ──tool calls──▶ worktree op log ──object ids──▶ git objects
                                   │
               git / jj layers ◀───┘  (commits, refs, jj operations and changes,
                                       observed in a worktree, keyed by their own ids)
```

- A **worktree** is one checkout directory on one machine. Exactly one recorder (the one on that machine) writes its datums, so its history is single-writer and linear.
- **Git and jj objects** are keyed by their own identifiers and are the same datums whichever worktree observed them. What links them to a session is the worktree op that observed them; their content is git's own objects (§4).
- **Agent sessions** reference the worktree ops their tool calls caused. A session belongs to no worktree.

This is a data arrangement, not a product decision: an intxt picks which of these parts it includes through entry datums (`repoEntry`, `worktreeEntry`, `sessionEntry`; ADR 036 §2), and a review of "one branch" is one reading of them.

## 2. Repositories and clones

Git has no repository identity. Two clones of one project share nothing but history, and one clone can hold several unrelated histories. The vocabulary separates three things, which also line up with jj's (repo, workspace):

| Datum | Is | Identity | jj equivalent |
|---|---|---|---|
| `gitRepo/<root sha>` | a project, as far as we can tell | keyed by the repository's root commit (the smallest, when there are several), so every recorder's datum for it has the same key and identical content (D13); each clone's recorder still writes it in its own document, created at the first commit; `roots` and `remotes` are hints | — |
| `gitClone/<uuid>` | one `.git` directory (or jj repo) on one machine; refs and the jj op log live here | a UUID stored in the clone (`.git/intxt-id`) | repo |
| `worktree/<uuid>` | one checkout directory of a clone; HEAD, index and working files live here | a UUID stored in the worktree's own git dir (`.git/worktrees/<name>/intxt-id`, or `.git/intxt-id` for the main worktree) | workspace |

Paths are recorded (`path`, the current absolute path) but are not identity: worktrees move, are deleted, and a new one may reuse an old path for unrelated work. A stored UUID survives the first two and distinguishes the third.

```jsonc
// org.example.intxt.work.gitRepo/<root sha>
{ "roots": ["a1b2…"], "remotes": ["git@github.com:dman-os/townframe.git"] }

// org.example.intxt.work.gitClone/<uuid>
{ "repo": "dtxt:datum:zRepo/org.example.intxt.work.gitRepo/<root sha>",   // absent until the repository has a commit
  "machine": "dtxt:actor:zNode", "path": "/run/media/…/townframe-p3N-3/.git",
  "vcs": ["git", "jj"] }

// org.example.intxt.work.worktree/main         (one per worktree document)
{ "clone": "dtxt:datum:zRepo/org.example.intxt.work.gitClone/<uuid>", "id": "<uuid>", "path": "/run/media/…/townframe-p3N-3",
  "opLogs": [ { "doc": "zOps0", "from": 0 }, { "doc": "zOps1", "from": 256 } ] }   // op-log documents in sequence order
```

Two machines with checkouts of one project are two clones, each with its worktrees; one intxt may include any of them as separate layers, and a viewer shows them as separate lines of work.

## 3. The worktree op log

```jsonc
// org.example.intxt.work.worktreeOp/<seq>      (seq: 0, 1, 2 … written only by the worktree's recorder)
{ "at": "2026-11-10T21:04:11Z",
  "kind": "edit",                         // open: edit | checkout | reset | commit | amend | rebase | merge | cherryPick | revert | refMove | jjSnapshot
  "cause": "dtxt:datum:zSess/org.example.intxt.work.promptTurn/<id>#toolCalls/%7B%22id%22:%22toolu_01…%22%7D",   // when known
  "files": { "src/big_cabinet_core/reference.rs": "e69d…" },     // path → git blob id after this op, for every path it changed
  "deleted": [ "src/old.rs" ],                                    // paths this op deleted
  "git": { "head": "4c1f…", "ref": "refs/heads/feat/intxt", "from": "77b2…", "to": "4c1f…",
           "reflog": "commit (amend): …" },                       // HEAD after the op; for ref moves, the move and git's reflog message
  "jj": { "operation": "e1f0…", "change": "kxqoltvm…" } }         // when the op came from jj
```

- **Single writer, sequential keys.** The slot is the sequence number, so the log is linear by construction and readers page it by key. It is the worktree's equivalent of jj's operation log, at the granularity the recorder observes.
- **Every observed change is an op**, one per observed tool-call window: the recorder compares the working tree before and after each tool call, so edits a tool doesn't report (a formatter or `sed` inside a Bash call, F7) are recorded too. Tool reports alone miss most of them: in our own sessions 455 Bash calls wrote files against 53 Edit/Write calls, and 32% of tool-touched paths reached their commit with content the reports don't explain (F26). Commits, amends, rebases, `git checkout` and `reset` are ops too. A checkout needs no revert semantics: it is one op listing the paths it changed with their new blob ids, and earlier content stays readable through earlier ops.
- **Content at an op.** A path's content at op N is the blob of the latest op at or before N that lists it; a path no op has listed has its content in the tree of the worktree's HEAD at the first op. The first op of a worktree records HEAD and the blob ids of the paths already differing from it. Renames are not recorded: a reader pairs deletions and additions with git's rename detection (`git diff -M`), as git and GitHub do.
- **`cause`** points at the tool call that made the change when the recorder knows it (tool-reported edits, a Bash call's window). A change with no known cause is an `edit` with no `cause`; a background process writing during a Bash call is attributed to that call (F7's limit). Which turn produced which commit is then a reading of these links by the viewer (D30), not a stored field.

## 4. Content

Content is git's own objects: blobs, trees and commits, named by their git ids. Committed content is already in the clone's object store. An uncommitted state is a blob too: the recorder writes it with `git hash-object -w` and records its id on the op (§3). Nothing here is a CRDT, because nothing has two writers.

What is content-identified and what is location state (D54):
- **Content-identified:** commits, trees and blobs are the same wherever they are observed, so any recorder writes the same value and they can be shared across machines.
- **Location state:** ref positions, the worktree op log and jj op logs belong to one clone or worktree and have one writer, its recorder.

The same path edited in worktrees on two machines therefore has two op logs that share ground only through git objects. A comment made in one reaches the other by git's diff mapping and quote matching (ADR 041 §2).

**An intxt carries objects as a pack.** It holds the objects its commits and ops need, as a thin pack (deltas against a base commit the recipient already has, as `git push`, a bundle with prerequisites and `format-patch` assume) or as a full pack when the recipient has no base. On our lane's work, a thin pack is 53–82 KB for 5–12 commits, against 0.6–0.9 MB full (F27). A commit carried this way rebuilds with its original sha (31 of 31, F25), so `git am` from an intxt alone reproduces it.

- **Large files:** a file over 8 MiB is recorded by its blob id and size and not packed (D45).
- **Secrets:** an object that captured a secret is left out of the pack. Ops still name its id, and a reader shows that content as unavailable.

**File documents** (`path`, `note`, provenance; ADR 036 §5) are only for **collaborative worktrees**, which don't exist yet (dpaths). A collaborative worktree is created from a commit, and every edit happens in its documents, whoever makes it; checking out another commit makes a new worktree, not a CRDT operation. A file document spans machines only when both edit it through an editor that writes it directly. The other machine's working copy must then come from that document, never from `git checkout`, or the two would diverge as separate writers (D55).

## 5. Git and jj layers

```jsonc
// org.example.intxt.work.gitCommit/<sha>       (key = sha; identical for every recorder, in the repo's document)
{ "parents": ["9a0e…"], "changeId": "kxqoltvm…",                       // jj change id when known
  "author":    { "name": "dman-os", "email": "…", "at": "…" },
  "committer": { "name": "dman-os", "email": "…", "at": "…" },
  "message": "refactor(reference): dtxt scheme",
  "files": [ { "path": "src/big_cabinet_core/reference.rs", "status": "modified", "oldPath": null } ] }

// org.example.intxt.work.gitRef/<clone uuid>/<ref name>
{ "target": "4c1f…", "base": "9a0e…" }  // base: what a review compares against (merge base with upstream)
```

- **Commits are written once.** Every recorder that sees a commit writes the same value to the same slot, so concurrent creation converges. A recorder never rewrites an existing commit datum: a recorder that lacks a field another wrote (a jj `changeId`) would remove it (whole-value writes, ADR 036 §1). A later fact about a commit goes into its own datum.
- **A commit's diff is git's:** its tree against its first parent's tree. The `commit` op in a worktree log carries the sha and links the commit to the session window that made it (§3); a commit observed in two worktrees has two such ops, both valid.
- **Refs belong to a clone**, since two clones' `refs/heads/main` are different refs; the slot is keyed by clone. Ref moves are worktree ops (`refMove` with `kind`: commit, amend, rebase, reset), so a ref's revision history is a filter over its clone's worktree logs, and the `gitRef` datum's own Automerge history keeps past targets too (D17). A `gitRef` has one writer, its clone's recorder.
- **A revision** is still a ref position: the commits reachable from `target` back to `base`. "What changed between revision 1 and 2" is git's diff between the two targets' trees, which git computes across a rebase too (`git range-diff` for commit by commit).

**jj is first class**, as its own layer over the same worktree log rather than translated into git terms:

```jsonc
// org.example.intxt.work.jjOperation/<op id>   (the clone's jj op log; a DAG, since jj ops can diverge)
{ "parents": ["…"], "description": "snapshot working copy", "at": "…",
  "heads": ["<commit id>", …], "bookmarks": { "feat/intxt": "<commit id>" } }

// org.example.intxt.work.jjChange/<change id>  (a change's evolution: the commits it has been)
{ "commits": ["<commit id>", …] }       // appended in evolog order
```

jj's working copy is a commit that it snapshots on every command, so a jj worktree's ops can name the jj operation and change they observed (`jj` on the op), and rewrite tracking comes from change ids instead of guessing a `refMove` kind. jj commits are also git commits when jj is backed by git, so they share `gitCommit/<sha>` datums.

## 6. Agent sessions

Names follow the Agent Client Protocol (ACP: session, prompt turn, tool call, plan; read 2026-11-10). The tag is `agentSession` because "session" alone is ambiguous among other vocabularies.

```jsonc
// org.example.intxt.work.agentSession/main     (one document per session; D7, F5: largest 2.67 MB normalized)
{ "harness": "claude-code", "sessionId": "eb26ecaa-…", "title": "intxt design",
  "cwd": "/run/media/…/townframe-p3N-3", "additionalDirectories": [],   // ACP's root set
  "parent": "dtxt:datum:zParentSess/org.example.intxt.work.agentSession/main" }   // subagent or continuation, ours (ACP has none)

// org.example.intxt.work.promptTurn/<harness turn id>
{ "seq": 41, "at": "…", "parentTurn": "<id>", "stopReason": "end_turn",
  "messages": [ { "id": "<harness id>", "seq": 0, "at": "…", "kind": "agentMessage", "text": "…" } ],   // open: userMessage | agentMessage | agentThought | meta | crossSession
  "toolCalls": [ { "id": "toolu_01…", "seq": 3, "title": "Edit reference.rs", "kind": "edit", "status": "completed",
                   "cwd": "/run/…/townframe-p3N-3/src",                                // when it differs from the session's
                   "locations": [ { "path": "/run/…/reference.rs", "line": 120 } ],
                   "diffs": [ { "path": "/run/…/reference.rs", "patch": "@@ -120 +120 @@ …" } ],   // what the harness reported, when it did
                   "output": "…" } ] }                                                  // what the harness showed the model, whole (D37)

// org.example.intxt.work.plan/main             (ACP plan: replaced whole on every update; its history is the plan's history)
{ "entries": [ { "content": "Rewrite reference.rs", "priority": "high", "status": "completed" } ] }
```

- A session is a normalized transcript, never the raw log. Thinking is excluded by default (FDR 007) and recorded as `agentThought` messages only when the operator opts in.
- **Message kinds** are ACP's (`userMessage`, `agentMessage`, `agentThought`) plus two of ours: `meta`, text the harness injected that the model saw (reminders, compaction summaries), and `crossSession`, a message another agent session sent. This settles ADR 039 open question 2's marking half.
- **Tool output** is stored whole in `output`, as the harness showed it to the model (D37); a tool call's `diffs` and `locations` use absolute paths, as ACP does.
- Keys reuse the harness's ids, so a re-import converges on the same slots.
- Messages and tool calls live inside their turn: a long session has thousands of messages (3,483 in one, F5) but around a hundred turns, and a document refuses updates at about 500 datums (F10). Updating a datum replaces its signature rather than accumulating one (F10), so appending to the open turn on every hook is cheap. A session past the limit continues in a new document whose `parent` is the previous one.
- **Subagents** are imported, each as its own session document with `parent` (D30).
- **Which worktrees a session touched** is derived: each tool call's `locations` and `cwd` resolve to the deepest worktree containing them, and the recorder writes the link once, on the op (`worktreeOp.cause`); a tool call's changes are found through the reference index (ADR 035 §2.6).

## 7. Documents

How the datums above split into documents. Each split has a reason: one writer per document where possible, the ~500-datum update limit (F10), loading only what a view shows (F1), and access boundaries.

| Document | Holds | Writers | How many |
|---|---|---|---|
| Repo | `gitRepo/<root sha>`, `gitCommit/<sha>` | every recorder of the project (commits are content-addressed, so concurrent writes are identical) | one per project; commits shard into more repo docs past ~400 |
| Clone | `gitClone/<uuid>`, `gitRef/<clone>/<name>`, `jjOperation`, `jjChange` | that clone's recorder | one per clone |
| Worktree | `worktree/main` (lists op-log docs) | that worktree's recorder | one per worktree |
| Op log | `worktreeOp/<seq>` for one seq range | that worktree's recorder | one per ~256 ops; the worktree datum lists them |
| Session | `agentSession/main`, `promptTurn/<id>`, `plan/main` | the recorder of that harness | one per session, subagent and continuation |
| Pack | a `Blob` (ADR 026) holding a git pack (§4) | the command that includes commits or ops in an intxt | one per inclusion |

For example, one day of this lane's work (one repo, one clone, one worktree, 30 changed files, a main session and five subagents) is about 10 documents: 1 repo, 1 clone, 1 worktree, 1–2 op-log and 6 session documents, plus the intxt's descriptor, a shared notes document (ADR 041) and the pack (§4).

**Readers must expect gaps.** An intxt spread over many documents, some in other drawers, is never guaranteed to arrive whole or at once: an object may be missing from the pack, a session may be private, a pin may name heads not yet synced. Readers render what resolves and mark the rest, as a browser renders a page with a broken image (ADR 036 §1, ADR 035 §2.1).

**Removing content.** A secret is removed by leaving its object out of the pack (§4), or, once shared, by replacing the pack and removing the old one from the drawer's roster. Relays and honest peers then stop holding and serving it. Removal does not retract what peers already synced (ADR 019: a stable id is not a promise that an audience change erases earlier disclosure).

## 8. Settled (operator, 2026-11-10)

- **What ships:** the worktree's documents themselves, at per-document granularity; unwanted documents are removed and replaced, not filtered (§7). Since D59, content ships as packed git objects, and unwanted objects are left out (§4).
- **Granularity:** per observed change, now one op per tool-call window (D59). The single writer per file doc put this back in scope (D26's commit-only rule existed because ref-driven docs had several writers). Interactive editing over an intxt would edit a fork (ADR 007) live and fold back into the worktree's docs after a debounce, writing one op. The per-write dmeta and signature overhead for such fine-grained writes is not yet measured; the node lane is asked for a measurement and for whether intermediate changes can skip signing (ledger).
- **Repository matching:** shared root commits only *suggest* that two clones are one project; they never merge repos automatically: two clones' repo documents stay separate even though their `gitRepo` datums share a key, and an intxt's `repoEntry` is keyed by the repo document, not the root. Forks and clones of a starter repo share roots and aren't the same project.
- **Directories outside any checkout:** an opt-in layer later (a worktree without a clone); nothing here blocks it.
- **jj:** record jj operations' and changes' identities and structure (ids, parents, descriptions, which changes and bookmarks moved), not the full content of jj's op store. A jj backend on top of intxt is a future VCS conversation.

## 9. Open questions

1. **Op cost.** Every observed change is an op datum carrying object ids; the content stays in git. Op-log chunking keeps documents under the limit, but the per-write dmeta cost (§8) decides whether per-change granularity is affordable on large sessions.
2. **Keeping uncommitted objects.** Settled (operator, 2026-11-12, ledger D60): `git hash-object -w` writes loose objects that nothing references, and `git gc` prunes those after two weeks (`gc.pruneExpire`), so the recorder keeps them reachable under a ref of its own, as jj does with `refs/jj/keep/*`, until an intxt has packed them or the operator drops the capture.
3. **Reading capture from another machine before packaging.** Ops name objects that only the recording machine's clone holds, so a live view from another device (an agent on a VM watched from a laptop) shows ids without content. Deferred (operator, 2026-11-12): a possible approach, file documents that start from a commit and match the git/jj view, is in the ledger's open thread on uncommitted work on another device.
