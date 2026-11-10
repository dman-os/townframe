# ADR 039: The intxt capture adapter

**Status:** Proposed; to be revised against ADR 040. Worktrees, not refs, are now the capture unit (ledger D29), so §1–§3 below describe the superseded ref-driven design (D26), and the datums it names moved from the earlier ADR 036 to ADR 040.

**Depends on:** ADR 040 (the datums it writes), ADR 036 (intxt layers), ADR 037 (acting as a descendant actor, its open question 2). Functional requirements: FDR 007, "Harness adapter". Evidence: `docs/scratch/intxt-ledger.md` F5 (session import), F7 (capture loop prototype), F10 (document size limits); decisions D26 and P3.

## 1. Context

FDR 007 asks the harness adapter to record the session, the edits made in it (including those made by commands), and the commits and branch moves, at no token cost to the agent, without steering it, and keeping observations nothing refers to local.

Git commits are the base layer (D26). intxt does not shadow the working tree: file docs receive only the content of commits on tracked refs (ADR 036 §4). Everything between commits is a record in the session.

A prototype (`/run/media/asdf/p3N/tmp/intxt-proto/session-import`, ledger F7) recorded Claude Code from hooks alone:

- **Transcript import is incremental.** Each hook reads the transcript from the last byte offset; the result equals a one-shot import of the same 42 MB transcript. About 73 ms per hook after the first (395 ms).
- **Command-made edits are observable.** Around each Bash call, hashing only the files that differ from the git index, plus the index listing, takes about 25 ms on this repository. The difference between the two snapshots is the call's edits (sed, formatters, generated files, deletions).
- **Ref moves are observable** by comparing each tracked ref's target with the last recorded one at each hook.
- **Normalized sessions are small** (F5): 4.2% of the raw transcripts; the largest is 2.67 MB.

## 2. What capture records

| Record | Source | Goes to |
|---|---|---|
| Session, turns, messages | The harness transcript, imported incrementally | Session doc (ADR 036 §6) |
| Edits by edit tools | The transcript (`structuredPatch`, or the content of a Write-create) | `edit` datums, `observedBy: tool` |
| Edits by commands | Snapshots of changed files around each Bash call | `edit` datums, `observedBy: filesystem`, with the call's `toolCall` |
| Commits and ref moves | Each tracked ref's target, read at every hook | `gitCommit`, `refMove`, `gitRef` in the history doc; commit content into file docs (ADR 036 §4–§5) |

Edits are patches attributed to the turn that made them. They show what the agent did between commits; they never write file docs.

**Ref moves.** The adapter reads `refs/heads/<name>` for each ref the intxt tracks, not `HEAD`, so checking out another branch, staging, stashing and working-tree edits record nothing on the history side. When a tracked ref's target changed, it writes a `gitCommit` for each commit between the old and new targets not yet recorded, ingests their content into the file docs in first-parent order, then writes the `refMove` and the `gitRef`'s new target. `kind` is taken from the ref's reflog entry (`commit`, `commit (amend)`, `rebase (finish)`, `reset`), git's own account of the move, and is `unknown` when the reflog has none.

## 3. How it runs

The adapter is a subcommand of the intxt CLI, `capture <hook event>`, reading the hook's JSON on stdin. It is installed as Claude Code hooks:

| Hook | What capture does |
|---|---|
| `SessionStart` | Record the session, check tracked refs |
| `PreToolUse` (Bash) | Snapshot files that differ from the index |
| `PostToolUse`, `PostToolUseFailure` (Bash) | Snapshot again and record the difference as edits; check tracked refs |
| `PostToolUse` (Edit, Write, MultiEdit) | Note the written file's hash, so a concurrent Bash window doesn't record the same write twice |
| `UserPromptSubmit`, `Stop`, `SubagentStop`, `PreCompact`, `SessionEnd` | Import the transcript up to now; check tracked refs |

Every invocation first imports the transcript from its stored offset, so any hook brings the session up to date. Hooks of parallel tool calls run concurrently, so each invocation takes a per-session file lock.

Claude Code also writes a `bashEditDiff` on Bash results (F7, undocumented). The snapshot stays the source for command edits because it is harness-independent and also sees deletions.

**Errors.** Capture never blocks or feeds the agent. A failure exits non-zero with a message (Claude Code shows it to the operator, not to the model), and the next hook retries from the stored state. Exit code 2, which Claude Code feeds back to the model, is never used.

**Attribution.** Every write is meant to act as the agent's actor, `<operator>/agents/<harness>/<session id>`, through ADR 037's act-as mechanism (an environment variable the hook sets, read by every `day` call in that invocation). Act-as is still a proposal in ADR 037 §2, waiting on the operator's decision there and on 037's earlier steps. Until it lands, captures are signed by the operator; the adapter uses no stand-in identity in the meantime.

## 4. Before an intxt exists

Code needs no working set: nothing reaches a file doc until a ref is tracked, and `intxt new <ref> [--base <ref>]` backfills the commits from the base to the tip. Only committed content enters an intxt, so a secret gets in only if it was committed, and then it is in git too.

Sessions are recorded before anyone decides which intxt they belong to. They stay in the adapter's local log (§5) as normalized datums, not Automerge changes, until attached: `intxt new` attaches the current session, and `intxt attach <session>` adds another. Attaching writes the session's datums into a session doc; from then on, capture writes that session straight into it. The log is datum-shaped because Automerge changes can't move between documents that don't share history.

## 5. Local state

Kept under the repository's git directory (`git rev-parse --git-dir`, so each worktree has its own), in `intxt/`, as an append-only log per session (the prototype rewrote 4 MB per hook on a long session):

- per session: the transcript offset, the latest snapshot, hashes of recent tool writes, and the normalized datums of a session not yet attached;
- per repository: the drawer id of every intxt created here (there is no `day drawers ls` yet), the refs each one tracks, and the last recorded target of each.

## 6. Consequences

- Recording costs the agent no tokens; each hook costs about 100 ms of wall time.
- Two machines recording the same branch independently make two intxts (D26). Two machines recording into the same intxt is handled by ADR 036 §4–§5 (pins, divergent refs).
- Line provenance for edits between commits is not in the file docs. A per-edit live file doc is a separate future kind, tied to editor integration (D26).

## 7. Open questions

1. **Attribution of concurrent changes.** A background process, or a parallel tool call, writing during a Bash call is attributed to that call; hook data can't tell them apart.
2. **Checkout and reset during a Bash call** change files without the agent editing them, and are recorded as filesystem edits. Suppress edits from a window in which `HEAD` moved, or keep them marked.
3. **Subagents.** Bash calls inside subagents never reach the main transcript, so their edits have no turn. Import subagent transcripts as linked sessions (D7) and attribute there.
4. **Which refs.** `intxt new` tracks the given ref. Whether capture should offer to track new branches the session creates is open.
5. **Failed edits and synthetic messages.** Whether to record edits whose tool call failed, and how to mark harness-generated messages (session-limit notices, compaction summaries).
