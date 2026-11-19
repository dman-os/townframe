# intxt: fencing experiments

**Status:** scratch, 2026-11-19 (ledger D75). Candidate mechanisms for structuring a session without spending the agent's tokens, each with the default it competes with and the measure that decides it. The best one becomes the default; the losers go in the ledger as tried and dropped, with their numbers. Builds on FDR 010 (intents, interpretations, tasks; its second draft's principle: record and structure always, use and delivery later) and FDR 008 (capture, short ids).

## Two layers, one mechanism

A session carries two kinds of structure, and they are separate:

- **Stages:** what kind of work a stretch of the session is. Turns already trend this way: research, design, review, feedback, implementation, chore. The vocabulary is open: those are suggested values, not an enum. Stages descend from plan mode, but they are recorded over the work as it happens, not produced as gated documents up front (Spec Kit's Spec → Plan → Tasks → Implement, Kiro's requirements → design → tasks).
- **Tasks:** what a stretch of work is for (FDR 010's intent tracking).

Both are **fences**. A fence is a kind (`stage` or `task`), an open value, and the span of turns and tool calls it covers. Everything recorded inside a fence belongs to it, with no explanation from the agent. The two kinds nest independently: a task can run through research, implementation and chore stages, and one stage can serve several tasks. Using one mechanism for both layers means one way to record, correct, query and show them.

**Menial work asks for no why.** Fixing typos or iterating until cargo passes is a span under a fence like any other, and the agent explains none of it. The churn inside is folded away and only the final diff is attributed. A `chore` stage collapses in review.

## Ways a fence is set

Cheapest first:

1. **Inferred, zero tokens.** Capture already records every tool call. The tool mix suggests the stage:
   - reads and searches: research;
   - edits: implementation;
   - a loop of build and test runs with small edits: chore;
   - a turn answering a comment: feedback.
   
   The in-progress task of the harness's todo list, where there is one, is the task fence.
2. **Inline tags.** Text the agent emits in its own message, such as `⟨stage: design⟩` or `⟨task: t2⟩`, which capture parses afterwards. A tag has no effects: nothing acts on it, so a wrong tag is only a wrong label and is corrected by a comment. This is in-band signalling, as ReAct parses `Thought:` and `Action:` out of model text. It costs a few tokens and no tool call or round trip:
   - Claude Code: capture parses the assistant's text from the transcript at `Stop`;
   - ACP: the recording proxy sees the agent's message chunks as they stream.
   
   Tags stay terse, because they show in the operator's terminal and in transcripts.
3. **Explicit, through `day`.** A command sets a fence, for harnesses or moments where a tag won't do. It costs a tool call.

## The stage step at commit

Fences attribute tool calls, and tool calls don't always map cleanly to hunks: a formatter or `sed` has no recorded cause, and one tool call can serve two tasks. Before a commit, intxt shows the staged hunks grouped by the fence their tool calls ran in, each with a short id. The agent links only what the fences didn't settle:

```
$ day x intxt stage
staged: 9 hunks
  t2 "reply resolves within one intxt"   h1 h2 h3 h4      (fenced)
  t3 "drop the #[path] attribute"         h5               (fenced)
  ?  no cause (formatter)                 h6 h7
  ?  two tasks touched it                 h8 h9 (t2, t3)
  link: day x intxt link h6 h7 --task t2
```

- It is prefilled from the fences, so a fully fenced commit needs nothing typed.
- A hunk can link to two tasks: the combined case.
- It runs as a git pre-commit hook that prints and never blocks; the agent sees the output in its tool result. The bare `day x intxt` shows unlinked hunks that remain when work is handed over.
- Links attach to the commit's hunks. A commit is immutable, so they are stable, and a rebase carries them through the rewrite links capture records.
- Each hunk also inherits the stage of the tool calls that made it.

## Experiments

Ground truth for stages is the operator's own labels on a week of pground sessions.

| Idea | Competes with | Measure | Drop it if |
| --- | --- | --- | --- |
| Keyword classification of the prompt (Morph's `TaskPhase`) | nothing: the baseline | Agreement with the operator's labels | Kept only as the floor others must beat |
| Stage inferred from the tool mix | the keyword baseline | Agreement with the operator's labels | Under ~80%, or no better than the baseline |
| Inline tags | inference | Tags per turn without prompting; tokens spent; agreement | Agents skip them, or they cost more than a `day` call |
| `chore` stage, no why asked | agents explaining menial work | Tokens spent explaining menial work, before and after | No drop |
| Commit stage step | fences alone | Ambiguous hunks per commit; tokens to resolve them | Most commits are fully fenced anyway |
| Agent Trace export | nothing | One consumer (git-ai) reads our hunk links | Never consumed |

The first experiment needs no agent changes: stage inference runs over capture data already recorded, against the keyword baseline and the operator's labels.

## Prior art

- **Morph** (`../morph`) records each agent session as a Run with a Trace (prompts, tool calls, reads, edits) through the same Claude Code hooks (`UserPromptSubmit`, `Stop`). It treats the prompt as the intent, verbatim, and attaches immutable annotations (feedback, bookmark, tag) to any object or to one trace event. Its `inspect task` infers a fixed `TaskPhase` (create, modify, fix-bug, diagnose, verify, explain) by keyword matching on the prompt (`morph-core/src/structured.rs`). It has no interpretation, task, decision or hunk-level link.
- **Cursor's Agent Trace** (draft spec) links line ranges to the conversation and contributor that produced them (human, AI, mixed, unknown), stored as files, git notes or a database. It records who wrote a line, not why: a candidate export format for our hunk links.
- **GitHub Copilot's coding agent** puts an `Agent-Logs-Url` trailer on each commit, linking it to the session log. That links commit to session, with no structure inside the session.
- **Spec Kit and Kiro** stage work as documents with approval gates. Kiro's quick mode, which skips the gates, suggests the gates are heavy.
