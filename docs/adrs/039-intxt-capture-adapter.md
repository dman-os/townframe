# ADR 039: The intxt capture adapter

**Status:** Proposed. §4 (where captured work lives before it is in an intxt) needs the operator's decision.

**Depends on:** ADR 036 (the datums it writes), ADR 037 (acting as a descendant actor, open question 2 there). Functional requirements: FDR 007, "Harness adapter". Evidence: `docs/scratch/intxt-ledger.md` F5 (session import), F7 (capture loop prototype), F8/F10 (document size limits).

## 1. Context

FDR 007 asks the harness adapter to record the session, the edits made in it (including those made by commands), and the commits and branch moves, at no token cost to the agent, without steering it, and keeping observations nothing refers to local.

A prototype (`/run/media/asdf/p3N/tmp/intxt-proto/session-import`, ledger F7) did this for Claude Code from hooks alone:

- **Transcript import is incremental.** Each hook reads the transcript from the last byte offset; the result equals a one-shot import of the same 42 MB transcript. About 73 ms per hook after the first (395 ms).
- **Command-made edits are observable.** Around each Bash call, hashing only the files that differ from the git index, plus the index listing, takes about 25 ms on this repository. The difference between the two snapshots is the call's edits (sed, formatters, generated files, deletions).
- **Commits are observable** by comparing `HEAD` and the current branch's target with the last recorded position at each hook.
- **Normalized sessions are small** (F5): 4.2% of the raw transcripts; the largest is 2.67 MB with 125 turns and 3,483 messages.

Two cabinet limits apply (F10): until the alias-scan fix lands, a document refuses updates to existing datums once it holds about 500 datums; and one datum's value is read within a 16,384-node budget.

## 2. Decision: what runs, and when

The adapter is a subcommand of the intxt CLI, `capture <hook event>`, reading the hook's JSON on stdin. It is installed as Claude Code hooks:

| Hook | What capture does |
|---|---|
| `SessionStart` | Record the session (`session/main`), check the branch position |
| `PreToolUse` (Bash) | Snapshot files that differ from the index; check the branch position |
| `PostToolUse`, `PostToolUseFailure` (Bash) | Snapshot again; record the difference as `edit` datums with `observedBy: filesystem` and the call's `toolCall`; check the branch position |
| `PostToolUse` (Edit, Write, MultiEdit) | Note the written file's hash, so a concurrent Bash window doesn't record the same write twice |
| `UserPromptSubmit`, `Stop`, `SubagentStop`, `PreCompact`, `SessionEnd` | Import the transcript up to now; check the branch position |

Every invocation first imports the transcript from its stored offset, so any hook brings the session up to date. Hooks of parallel tool calls run concurrently, so each invocation takes a per-session file lock.

**Edits made by the edit tools** come from the transcript (`structuredPatch` for Edit, the content for a Write-create), as in F5. **Edits made by commands** come from the snapshots. Claude Code also writes a `bashEditDiff` on Bash results (F7, undocumented); the snapshot stays the source because it is harness-independent and also sees deletions and index changes.

**Commits and branch moves.** At each check, if the current branch's target differs from the recorded one, the adapter writes a `gitCommit` datum for every commit between the old and new targets not already recorded, then a `refMove` from the old target to the new. `kind` is taken from the branch's reflog entry (`commit`, `commit (amend)`, `rebase (finish)`, `reset`), which is git's own account of the move, and is `unknown` when the reflog has none. The new revision's file contents go into the file docs as line-level splices (ADR 036 §4).

**Attribution.** Every write acts as the agent's actor, `<operator>/agents/<harness>/<session id>`, through ADR 037's act-as mechanism (an environment variable the hook sets, read by every `day` call in that invocation). The signing actor of each datum is then the session that produced it, not the operator.

**Errors.** Capture never blocks or feeds the agent. A failure exits non-zero with a message (Claude Code shows it to the operator, not to the model), and the next hook retries from the stored offset. Exit code 2, which Claude Code feeds back to the model, is never used.

## 3. Decision: local capture state

The adapter keeps its state under the repository's git directory (`git rev-parse --git-dir`, so each worktree has its own), in `intxt/`:

- per session: the transcript offset, the latest snapshot, the hashes of recent tool writes, and the last recorded branch position;
- per repository: the drawer id of every intxt created here (there is no `day drawers ls` yet).

State is an append-only log per session, not a file rewritten on every hook (the prototype rewrote 4 MB per hook on a long session).

## 4. Proposal, for the operator: where captured work lives before an intxt

Ledger P1. Recorded work has to exist before anyone decides which intxt it belongs in, and Automerge can't unsync a change, so anything written into a shared document can only be superseded, never removed.

**Proposed: captured work stays in the local log until it is promoted.** The log in §3 holds the normalized datums and the snapshot blobs, not only offsets. Nothing is written to any Automerge document until the operator or the agent runs `intxt new` or `intxt add <session|commits>`, which writes the chosen datums into the intxt's documents. From then on, a session that belongs to an intxt is captured straight into it.

- What this buys: nothing leaves the machine, or enters a document with history, until it is chosen. The footgun disappears for unpromoted work.
- What it costs: unpromoted work can't be read from the phone. That is the point of keeping it private, but it means "start an intxt" is the step that makes remote review possible, and the CLI has to make that one command.
- The alternative, a private local drawer as the working set, keeps everything in documents from the start and so inherits the footgun the moment one of those documents is shared.

## 5. Consequences

- ADR 036 §6 changes. One message datum per transcript record exceeds the ~500-datum limit on long sessions (3,483 messages in one real session). A turn datum holds its messages as a list instead (125 datums for that session); updates don't accumulate signatures, so appending to the open turn on every hook is cheap (F10). Edits stay separate datums so that notes can target them.
- Sessions longer than about 500 turns still exceed the limit until the cabinet fix lands; the adapter starts a continuation session document (`parent` pointing at the previous one) when one fills.
- The agent's work costs it no tokens, and each hook costs about 100 ms of wall time.

## 6. Open questions

1. **Attribution of concurrent changes.** A background process, or a parallel tool call, writing during a Bash call is attributed to that call; hook data can't tell them apart.
2. **Checkout and reset.** `git checkout` and `git reset` change files without the agent editing them. Record them as edits with a `kind`, or suppress edits when the branch position moved in the same window.
3. **Subagents.** Bash calls inside subagents never reach the main transcript, so their edits have no turn. Import subagent transcripts as linked sessions (D7) and attribute there.
4. **Which refs.** The current branch only (proposed for the alpha), or every branch the session touches.
5. **Failed edits and synthetic messages.** Whether to record edits whose tool call failed, and how to mark harness-generated messages (session-limit notices, compaction summaries).
