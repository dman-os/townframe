# ADR 039: The capture adapter

**Status:** Proposed. Rewritten 2026-11-10 for worktrees as the capture unit (ledger D29); the earlier ref-driven design (D26) is superseded. §4 revised 2026-11-11 for D41 (observe on the host, record in the nodule).

**Depends on:** ADR 040 (the datums it writes), ADR 036 (intxt entries and `body`), ADR 019 (drawers), ADR 037 (acting as a descendant actor, its open question 2). Functional requirements: FDR 007, "Harness adapter". Evidence: `docs/scratch/intxt-ledger.md` F5 (session import), F7 (capture loop prototype), F10 and F12 (document limits, write cost); decisions D29–D35.

## 1. Context

FDR 007 asks the harness adapter to record what an agent did: its session, the changes it made to files, and what happened in version control. It must cost the agent no tokens, never steer it, and keep observations local until something refers to them.

**The baseline agent runs shell commands and nothing else** (D35), like mini-SWE-agent: no edit tools, no structured diffs. Capture therefore observes the filesystem, and harness-specific information (an Edit tool's patch, a transcript's tool-call structure) only adds a cause to what the filesystem already shows. A harness that gives more, as Claude Code does, makes the record richer, never different in kind.

A prototype (`/run/media/asdf/p3N/tmp/intxt-proto/session-import`, ledger F7) recorded Claude Code from hooks alone:

- **Transcript import is incremental.** Each hook reads the transcript from the last byte offset, and the result equals a one-shot import of the same 42 MB transcript: about 73 ms per hook after the first (395 ms).
- **Command-made edits are observable.** Around each shell call, hashing only the files that differ from the git index, plus the index listing, takes about 25 ms on this repository. The difference between the two snapshots is what the call changed (sed, formatters, generated files, deletions, checkouts).
- **Normalized sessions are small** (F5): 4.2% of the raw transcripts; the largest is 2.67 MB.

## 2. Where captures live

**A local capture drawer per machine.** Worktree, clone, repo and session documents need a drawer from their first write, long before anyone decides which intxt they belong to. The node keeps one capture drawer, owned by the operator and synced to no one, and capture writes every document there. Node services (the `day` socket, datum writes, drawers) are assumed available wherever capture runs.

**An intxt references captures.** `intxt new` creates the intxt's drawer and writes entry datums (`sessionEntry`, `worktreeEntry`, `repoEntry`; ADR 036 §2) pointing at capture documents, and admits those documents to the intxt drawer's roster. A document then belongs to both drawers, which Cabinet supports today (ADR 019: a document can be entered in several drawers; every document is already in the node's two required local drawers; `AddDoc` takes several drawers, and removal from one leaves it readable through the others): the capture drawer keeps recording into it, and the intxt drawer shares it. Adding a document shares its whole history (ADR 040 §7). Documents nothing references stay in the capture drawer and never leave the machine.

**Removing and redacting.** Removing a document from an intxt's roster stops relays and honest peers from holding and serving it; it does not recall copies already pulled (ADR 019). For a secret, that is the best available, so capture makes it cheap:

- a file doc that captured a secret is removed, and a replacement doc is written from the cleaned state;
- a session doc whose tool output or diffs carry the secret is replaced by a redacted copy, a new document with the offending content removed;
- the references are re-pointed: the intxt's entry datum and any comment targeting the old documents are rewritten to the new ids. Because every link is a declared reference (ADR 035 §2.6), the reference index lists exactly what to rewrite.

## 3. What capture records

| Record | Source | Goes to (ADR 040) |
|---|---|---|
| Worktree, clone and repository identity | `git rev-parse` at first sight; UUIDs stored in the git dir | `worktree`, `clone`, `gitRepo` |
| File changes | Snapshots of changed files around each shell call, and at every hook | `worktreeOp` (`kind: edit`) and file docs |
| What caused a change | The tool call whose window the change fell in, when the harness reports one | `worktreeOp.cause` |
| HEAD and ref moves, commits | `HEAD`, the clone's refs and the reflog, read at every hook | `worktreeOp` (`commit`, `refMove`, `checkout`, `reset`), `gitCommit`, `gitRef` |
| jj operations and changes | `jj op log` / `jj evolog` ids and structure, when the clone is a jj repo | `jjOperation`, `jjChange`, the `jj` field of ops |
| Session, prompt turns, tool calls, plan | The harness transcript, imported incrementally; subagents as their own sessions (D30) | `agentSession`, `promptTurn`, `plan` |

**Resolving a change to a worktree.** A path belongs to the deepest worktree root containing it. A shell call's window covers every worktree its session has touched or its working directory is in, so one session can produce ops in several worktrees of several repositories (ADR 040 §1). Paths outside any checkout are not recorded (opt-in later, D32).

**Checkout and reset.** A window in which `HEAD` moved records its file changes as one op whose kind comes from the reflog entry (`checkout`, `reset`, `commit (amend)`, `rebase`), not as an edit. The file docs follow the working tree; the earlier content stays at earlier pins.

**Batching.** A signed write costs about 60 ms and grows with the document's history (F12), and parallel tool calls produce bursts. Each hook invocation writes once per document: all ops it observed in one op-log write, and each file doc once, however many changes it saw.

## 4. How it runs

Capture has two halves (ledger D41). **Observing** happens on the agent's machine: reading the hook's JSON on stdin, snapshotting the worktree, reading git and jj state, and importing the transcript need the filesystem, which a nodule cannot reach. It is a `day` command, `day intxt capture <hook event>`. **Recording** is datum work: sequencing a worktree's ops, writing file docs, the session document and commit datums. It is an operation of intxt's nodule, `recordObservations`, which the `day` command calls with everything one hook invocation observed, so the single-writer order of each worktree's op log is kept in the node rather than by file locks between hook processes. Until that operation exists the `day` command writes the datums itself as the operator, and when nodules can be granted a worktree's files the observing half moves into the nodule too. For Claude Code the command is installed as hooks:

| Hook | What capture does |
|---|---|
| `SessionStart` | Record the session; resolve its working directory's worktree |
| `PreToolUse` (any) | Snapshot files that differ from the index in the session's worktrees |
| `PostToolUse`, `PostToolUseFailure` (any) | Snapshot again; record differences as ops caused by this tool call; check `HEAD`, refs and jj |
| `UserPromptSubmit`, `Stop`, `SubagentStop`, `PreCompact`, `SessionEnd` | Import the transcript up to now; snapshot; check `HEAD`, refs and jj |

Every invocation first imports the transcript from its stored offset, so any hook brings the session up to date. Hooks of parallel tool calls run concurrently: each invocation takes a per-worktree file lock around its snapshot and its local state, and `recordObservations` orders the ops it receives per worktree. Snapshotting around every tool call, not only shell calls, is what makes the shell-only baseline and edit tools look the same: an edit tool's write is a change in its window like any other, and its patch, when the transcript has one, is only a cross-check. A harness without hooks is recorded by snapshots at session boundaries and by importing its transcript afterwards; changes then have no cause finer than the session.

**Errors.** Capture never blocks or feeds the agent. A failure exits non-zero with a message (Claude Code shows it to the operator, not the model), and the next hook retries from the stored state. Exit code 2, which Claude Code feeds back to the model, is never used.

**Attribution.** Writes are meant to act as the agent's actor, `<operator>/agents/<harness>/<session id>`, through ADR 037's act-as mechanism (an environment variable the hook sets, read by every `day` call in that invocation). Act-as is still a proposal in ADR 037 §2. Until it lands, captures are signed by the operator, with no stand-in identity. Which tool call, turn or commit a change belongs to is read from the links (`cause`, op order) by the viewer, not stored as a separate attribution (D30).

## 5. Local state

- **Identity files.** A UUID per clone in `.git/intxt-id`, and per worktree in its own git dir (`.git/worktrees/<name>/intxt-id`; the main worktree uses the clone's directory, under `intxt-worktree-id`). They are created at first sight and never change.
- **Capture state**, under each worktree's git dir in `intxt/`, append-only: per session, the transcript offset and the latest snapshot; per worktree, the next op sequence number and the last observed `HEAD`, refs and jj operation.
- **The capture drawer's id** and the document id of every worktree, clone, repo and session it holds, in the node's local configuration, since there is no `day drawers ls` yet.

## 6. Consequences

- Recording costs the agent no tokens; each hook costs about 100 ms plus one signed write per touched document.
- Every change an agent makes in a worktree is recorded, whether it came from an edit tool, a shell command or a checkout. A change made outside any session (the operator in an editor) is recorded at the next hook as an op with no cause.
- Several machines, or several worktrees on one machine, are separate single-writer histories. One intxt can include any of them.

## 7. Open questions

1. **Concurrent changes in one window.** A background process, or a parallel tool call, changing files during a tool call is attributed to that call; hook data can't tell them apart. Parallel tool calls in one assistant message share overlapping windows: attribute to the whole message, or to the call whose window closed first?
2. **Failed edits and synthetic messages.** Whether to keep tool calls whose edits failed, and how to mark harness-generated messages (session-limit notices, compaction summaries).
3. **Snapshot cost on large worktrees.** About 25 ms on this repository per snapshot, two per tool call. Large monorepos or many untracked files need measuring; a file-watcher in the node could replace snapshots later.
