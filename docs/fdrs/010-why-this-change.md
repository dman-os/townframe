# FDR 010: Why this change: intents, interpretations and tasks

**Status:** Accepted 2026-11-19 (operator; ledger D73), second draft. Its first four open questions are settled below. Builds on FDR 006 (intxt), FDR 008 (the intxt surface), ledger D9 (work structure is its own datums; adapters map harness task systems into them) and D16 (grouping is a datum with references).

**Changed in the second draft** (operator, 2026-11-19):
- The principle that governs the rest: record and structure always, and work out use and delivery later.
- An intent keeps its source verbatim, in the source's own medium, and review shows it first.
- Structure can be added after the fact by naming spans, without recounting tool calls.
- Work that serves several concerns at once has a way to say so.
- Tasks don't depend on the harness having a task tool.
- The kinds form one graph, so comments and review work on any node.
- Turn ids are known when the turn arrives; the id scheme belongs to an ADR.

## The problem

A reviewer opens a pull request with a thousand changed lines. Weeks later, among a hundred such pull requests, they can't recall:

- why it was asked for;
- what the agent understood it to be;
- which decisions shaped it;
- which part of the diff serves which purpose.

When the result is wrong, nobody can tell whether the request was unclear, the agent misread it, or the agent read it right and built it wrong. Those are three different failures with three different fixes.

## The principle: record and structure always

An agent spends much of its work understanding the task and placing itself within it: what was asked, what it means here, what has to change and in what order. A text-only record (a transcript, a summary at the end) keeps the words and loses that work, because nothing in it says which part of the work served which part of the request. intxt keeps the structure as the agent produces it, always, whether or not anyone yet knows how it will be read. What it is used for (review, handoff, the next intxt, repo rules) and how it is delivered are worked out later, from what was recorded.

So the structure has to be the shape the agent already works in, not a form it fills in afterwards. It is not the agent's thinking traces: those are the agent's, some harnesses hide them, and they aren't written for a reader. Labs could train agents to use the vocabulary well, but intxt has to work without that.

## What intxt records

Each kind below is something a person or an agent says about the work. They point at each other, and at the work, with plain references.

- **Intent:** what someone wanted, and where it came from:
  - a turn of the operator's in a session;
  - an issue ticket;
  - another agent's task (a subagent's brief);
  - the agent's own initiative ("the test was flaky, so I fixed it").
  
  It keeps its source **verbatim, in the source's own medium**, and doesn't restate it. A person who wrote a request recognises their own words weeks later; an agent's rephrasing of them doesn't bring back the state of mind they were in. For a voice note, the intent keeps the audio and its transcript and points at the span it came from (0:42–1:05). Review shows the operator's own words first and the interpretation under them.
- **Interpretation:** how an agent understood an intent. It gives a restatement, the assumptions made, and any correction, for example: "the message says the `drawers` command; from the code it means `drawers ls`". It is separate from the intent, so a misreading is visible as a misreading, and two agents can read one intent differently.
- **Task:** a unit of work toward an interpretation. These are the items agents already keep in their todo lists and plans. An agent is asked to break an intent down so that each task maps to a reviewable part of the diff. In a harness with no task tool, the agent writes tasks through `day` directly.
- **Decision:** a choice and its reason. Divergence is a decision: "this can't be done as asked, because …, so I did … instead".
- **Action:** anything the agent did while a task was in progress that changed something:
  - an edit;
  - a commit;
  - a shell command that changed a system.
  
  Reading and searching aren't actions.
- **Guide:** an ordered way through the result for a reviewer ("read this first"). Its sections point at tasks, decisions and parts of the diff.

**One graph.** References between the kinds are many-to-many, so the kinds form a graph:
- an intent has interpretations;
- an interpretation has tasks;
- a task has actions;
- a decision points at whatever it decided about.

A comment is one more node, and the comment machinery (threads, resolving, `comment list`) works on any node: disputing an interpretation is a comment on it. Review is a walk over the graph.

**Combined concerns.** One action or hunk can serve several tasks: it references each of them. When why they had to go together is worth saying, that is a decision that references them all ("one migration serves both, because …").

**Precedent.** A decision can cite earlier decisions, including other intxts' decisions. A decision cited again and again is a candidate for the repo's own rules (AGENTS.md, skills), so reviewed work informs the agents' next work. Promoting one comes after the alpha; citing is in the shape from the start.

## Walkthroughs

**1. The operator asks; the agent breaks it down.** The operator writes a turn asking for a feature. The agent records an intent that points at that turn, then its interpretation, which corrects a typo in the request. It lays out tasks in its own todo list, as it already does. While each task is in progress, intxt attributes the agent's edits and effectful commands to that task, without the agent doing anything more. A subagent spawned during a task inherits it: the subagent's brief is an intent, and its source is the parent's task.

**2. The agent can't do what was asked.** Halfway through, the agent finds the request conflicts with an existing constraint. It records a decision: what was asked, why it can't be done as asked, and what it did instead. The decision points at the intent and at the task that carries the alternative.

**3. Work from an issue ticket.** The agent starts from a GitHub issue rather than a turn. The intent's source is the issue. Everything else is the same.

**4. The reviewer.** A person or a reviewing agent opens the pull request's intxt. They see:
- the intents, each with its interpretation and the decisions taken;
- the diff split by task;
- the guide's order.

When something looks wrong, they can say which layer is wrong: "this interpretation is wrong" is a comment on the interpretation, and "this doesn't do what you said you'd do" is a comment on the task's part of the diff.

**5. Work that is not text** (after the alpha, D73). An agent changes a running system with a shell command, such as a deployment. It shows up as an action under the task it served, next to the diffs. Only that it happened, and why, is recorded here; recording the system's state is out of scope.

**6. Requirements from a voice note.** The operator records three minutes of talk about what they want and hands it to the agent. The agent carves it into intents, each pointing at its span of the audio, records its interpretations, and starts work at once. The operator corrects the carving later, by commenting on an intent or an interpretation, not by restating the request. At review weeks later, each part of the diff leads back to the stretch of audio that asked for it.

**7. Structure after the fact.** An agent worked through a task without recording much. Before handing over, it names spans instead of recounting tool calls:
- "turns k7:12–15 serve this intent";
- "the ops since this commit are this task";
- "this commit's hunks under `src/foo/` are that task".

Capture already holds the ops with their causes (FDR 008), so the attribution follows from the spans.

## How agents take part

- **Automatically, through the harness's task tools.** Capture already sees every tool call through the harness's hooks. That includes the agent's todo or plan updates (Claude Code's task tools, ACP plan updates) and their in-progress item, so tasks and the attribution of actions to them come at no cost to the agent. No harness lets us add a parameter to its edit tool; the in-progress task is the fence.
- **Explicitly, through commands for what a todo list can't say.** These are recording an intent and its interpretation, recording a decision, and relating tasks, plus starting and finishing a task in harnesses with no task tool. The same things can be written through `edit` (FDR 008, D64). The exact commands are settled with this FDR's review, from these walkthroughs.
- **By instruction.** A skill tells agents when to record:
  - at the start of a piece of work: the intent and the interpretation;
  - on divergence: a decision;
  - before handing work over for review: a guide.
- **MCP is deferred** (D19: hooks and CLI first). It is for agents without a shell, such as chat and desktop apps or sandboxed agents, and offers the same operations.

## Out of scope for now

- Attribution finer than "this tool call served this task". Mapping a tool call to the hunks it produced is the tool-call-to-hunk tracking already planned; this feature consumes it when it lands.
- Recording the state of non-file systems an action changed.
- The review GUI. Everything here works from the CLI first, and the CLI is the forcing function for the commands.

## Open questions

1. ~~**Names**~~ Settled (D73): intent, interpretation, task, decision, action and guide. ADR 041's generic `structure` datum goes away in favour of these specific kinds, related by references.
2. ~~**Does an interpretation live inside its intent, or apart from it?**~~ Settled (D73): **apart**, its own datum referencing the intent, so two agents can interpret one intent and a misreading stays visible beside what was asked.
3. ~~**How an agent names the source turn**~~ Settled (D73): **recording tells the agent.** On each operator turn, the recorder injects the turn's short id (`<session>:<turn>`, FDR 008) into what the agent sees:
   - Claude Code: the capture hook answers `UserPromptSubmit` with `additionalContext`, which the harness adds to the turn;
   - ACP: the recording proxy (`record acp`) prepends a text block to the prompt it forwards.
   
   The id has to be known when the hook fires, before the transcript catches up, and stay the same on every node that records the session. The id scheme is the capture ADR's (ADR 040) to settle; the candidate is a hash chain over the session, the parent turn and the prompt, as git hashes a commit with its parent. An intent then quotes its source by the id it was handed, with no lookup.
4. ~~**Do actions other than edits belong in the alpha?**~~ Settled (D73): **no.** The alpha records edits and commits; shell commands that change a system come after.
5. **Is structuring a burden without training?** The measure is dogfooding: how often agents record unprompted, and what a `Stop`-hook reminder adds when a turn changed code and recorded nothing.
6. **Where a voice note's audio lives:** as a blob in the session's document, or in the intent's own?
