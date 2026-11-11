# FDR 008: The intxt surface, nouns and verbs

**Status:** Proposed (2026-11-11). This covers how people and agents use intxt (FDR 006): the things they name and the actions they take. It also fixes the use cases delivered this week. FDR 007 describes the review loop these verbs serve. Datum shapes are in ADRs 036, 039 and 040; working notes are in `docs/scratch/intxt-ledger.md`.

## One surface for people and agents

People and agents use the same commands. An agent is a reader and writer like the operator, not a special client with its own interface (ledger D23). Three rules follow:

- **Every verb is a command.** It runs under `day x intxt <verb>` today, the same way from a shell, a hook, an agent's tool call or a script. The plugin, the CLI and the node layer on each other: a plugin's hooks call the CLI, and the CLI calls the node. Nothing reaches the node by a path the CLI doesn't have.
- **The output reads well as text, and `--json` gives the same content as data.** The default is short, plain text with stable ids that an agent can paste back into the next command. Nothing ever needs scraping.
- **Things are named the same way everywhere.** An id printed by one verb is accepted by every other verb, and the same goes for a `file:line` or a turn number.

Recording costs an agent nothing (FDR 007). Agents spend tokens only on the verbs they choose: leaving a mark, writing a guide, replying.

## This week's use cases

These are the use cases we deliver this week, and they don't depend on work in other lanes. Phone review is the claytxt lane's work; these use cases supply the data it shows.

1. **Session recording.** A session is recorded as it runs, whichever harness it uses:
   - **Claude Code**, through hooks. This works today.
   - **Any harness that speaks ACP** (Agent Client Protocol), through a recorder that sits between the client and the agent and records the session from the protocol itself. This covers every ACP agent at once, without one adapter per harness.

   Tool outputs are recorded in their own document, apart from the conversation (ledger D47).
2. **Guided review.** The agent leaves a reading path through its work, and the operator follows it:
   - The path is ordered sections. Each says what changed and why, and points at the files, line ranges, commits and session turns that make it up.
   - The agent can leave cheap marks while it works ("turns 20–45 implemented X because of Y") and turn them into the guide at the end. Nothing has to be finished in one pass.
   - The operator responds per section or per line. The agent finds the responses it hasn't seen, replies and resolves.
3. **Association with git.** An intxt knows which branches and commits its work went through:
   - Each time a branch moves (commit, amend, rebase), that position becomes a new revision. Earlier revisions stay.
   - A reader lists the revisions, sees what changed between two of them, and sees the commits each one is made of, without git.
   - Existing work can be included by naming a branch or a commit range, not only by recording it live.

## Nouns

| Noun | What it is |
| --- | --- |
| **intxt** | A package of development context with a title and a description (FDR 006). |
| **part** | Something an intxt includes: a session, a worktree or a repository. One part can be in several intxts. |
| **session** | An agent session as a readable record: turns, messages, tool calls and plans. Subagents are sessions of their own. |
| **turn** | One prompt and everything the agent did in answer to it. Numbered within its session. |
| **tool call** | One action an agent took, with its kind, its status and where it acted. |
| **tool output** | What a tool call brought back from the environment, kept in a separate document (D47). |
| **worktree** | A checkout the work happened in, with every change to its files. |
| **repo** | A git repository, identified by its root commit, wherever it is cloned. |
| **file** | A file in a worktree at some point in time: text that merges line by line, or a blob. |
| **commit** / **branch** | Git's own commits and refs, as observed. jj changes are recorded alongside. |
| **revision** | One position of a branch the intxt follows, with the commits on it and the files as they stood. |
| **mark** | A short note on a stretch of work, left while working: a run of turns, an edit, a commit. |
| **guide** | An ordered set of sections forming the reading path through an intxt. |
| **section** | One step of a guide: a title, why it matters, and what it points at. |
| **comment** | A note about anything an intxt holds, at a specific version. Replies form a **thread**, which can be resolved. |
| **seen** | What one reader has already seen, so "new to me" is answerable. |

Every noun that can be pointed at can be the target of a comment or a section: a line range, a file, a commit, a turn, a stretch of turns, a tool call, a section, another comment.

## Verbs

### Recording
- `capture`: what a harness hook calls with its event. It records the session and the worktrees it touches. It runs in the hook and never in the agent's context.
- `record -- <agent command>`: runs an ACP agent behind the recorder. The ACP client launches this in place of the agent.
- `import <branch | range>`: records existing git history as a worktree's revisions, for work that wasn't captured live.

### Making an intxt
- `new <title> [--description]`: creates an intxt and prints its id.
- `add <intxt> <part>` and `remove <intxt> <part>`: include or drop a session, a worktree or a repo. Removing stops further sharing but can't recall copies already pulled (FDR 007).
- `list` and `show <intxt>`: what exists, and what one intxt holds.

### Guiding
- `mark <target> <text>`: leaves a mark on a stretch of turns, an edit or a commit. It's meant to cost one short tool call.
- `guide <intxt>`: prints the guide. With `--edit` it reads a guide in plain text (sections, each with a title, a why and its targets) and writes it. A guide can be revised any number of times; earlier comments stay attached to the sections they were made on.

### Reviewing
- `revisions <intxt>`: lists the revisions, with their commits.
- `diff <intxt> [<rev> [<rev>]]`: what changed in a revision, or between two.
- `read <target>`: a file at a revision, a turn, a tool call, a section.
- `comment <target> <text>` and `reply <comment> <text>`: start a thread or answer one.
- `resolve <comment>` and `reopen <comment>`: close or reopen a thread.
- `inbox <intxt>`: the comments this reader hasn't seen, each with what it points at. Reading it marks them seen. `--wait` blocks until something new arrives, which is how an agent waits for feedback without being interrupted.

## Behaviour that must hold

- An agent can do everything in this FDR with shell commands alone.
- Every target an output prints can be passed back unchanged as the target of another verb.
- `inbox` never loses a comment: one written while a reader was reading appears on that reader's next call.
- The ACP recorder is transparent. The client and the agent behave the same with or without it, and if recording fails the session continues, with the failure reported.

## Not this week

- Phone and desktop UI (claytxt).
- Approve / request changes, GitHub publishing, and check results (FDR 007's exclusions).
- `why <file:line>` (U8). It needs an index from code to edits, which is a later ADR.
- Pairing the plugin with the node as its own identity. The node lane is designing that (ledger D49), and capture keeps its current identity until it lands.

## Open questions

1. **What does `--wait` wait on?** A reader needs to know when a document it can read changes. If the cabinet can't report that yet, `--wait` polls the intxt's heads at a modest interval until it can, and the verb's behaviour doesn't change.
2. **Who writes the guide by default?** The agent, prompted by a skill when it finishes a piece of work, or the operator asking for one? Both use the same verb; the question is what the plugin nudges.
