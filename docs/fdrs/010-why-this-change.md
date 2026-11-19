# FDR 010: Why this change: intents, interpretations and tasks

**Status:** Proposed (2026-11-18, first draft). Names are proposals (see Open questions). Builds on FDR 006 (intxt), FDR 008 (the intxt surface), ledger D9 (work structure is its own datums; adapters map harness task systems into them) and D16 (grouping is a datum with references).

## The problem

A reviewer opens a pull request with a thousand changed lines. Weeks later, among a hundred such pull requests, they can't recall:

- why it was asked for;
- what the agent understood it to be;
- which decisions shaped it;
- which part of the diff serves which purpose.

When the result is wrong, nobody can tell whether the request was unclear, the agent misread it, or the agent read it right and built it wrong. Those are three different failures with three different fixes.

## What intxt records

Each kind below is something a person or an agent says about the work. They point at each other, and at the work, with plain references.

- **Intent:** what someone wanted, and where it came from:
  - a turn of the operator's in a session;
  - an issue ticket;
  - another agent's task (a subagent's brief);
  - the agent's own initiative ("the test was flaky, so I fixed it").
  
  It quotes its source; it doesn't restate it.
- **Interpretation:** how an agent understood an intent. It gives a restatement, the assumptions made, and any correction, for example: "the message says the `drawers` command; from the code it means `drawers ls`". It is separate from the intent, so a misreading is visible as a misreading, and two agents can read one intent differently.
- **Task:** a unit of work toward an interpretation. These are the items agents already keep in their todo lists and plans. An agent is asked to break an intent down so that each task maps to a reviewable part of the diff.
- **Decision:** a choice and its reason. Divergence is a decision: "this can't be done as asked, because …, so I did … instead".
- **Action:** anything the agent did while a task was in progress that changed something:
  - an edit;
  - a commit;
  - a shell command that changed a system.
  
  Reading and searching aren't actions.
- **Guide:** an ordered way through the result for a reviewer ("read this first"). Its sections point at tasks, decisions and parts of the diff.

## Walkthroughs

**1. The operator asks; the agent breaks it down.** The operator writes a turn asking for a feature. The agent records an intent that points at that turn, then its interpretation, which corrects a typo in the request. It lays out tasks in its own todo list, as it already does. While each task is in progress, intxt attributes the agent's edits and effectful commands to that task, without the agent doing anything more. A subagent spawned during a task inherits it: the subagent's brief is an intent, and its source is the parent's task.

**2. The agent can't do what was asked.** Halfway through, the agent finds the request conflicts with an existing constraint. It records a decision: what was asked, why it can't be done as asked, and what it did instead. The decision points at the intent and at the task that carries the alternative.

**3. Work from an issue ticket.** The agent starts from a GitHub issue rather than a turn. The intent's source is the issue. Everything else is the same.

**4. The reviewer.** A person or a reviewing agent opens the pull request's intxt. They see:
- the intents, each with its interpretation and the decisions taken;
- the diff split by task;
- the guide's order.

When something looks wrong, they can say which layer is wrong: "this interpretation is wrong" is a comment on the interpretation, and "this doesn't do what you said you'd do" is a comment on the task's part of the diff.

**5. Work that is not text.** An agent changes a running system with a shell command, such as a deployment. It shows up as an action under the task it served, next to the diffs. Only that it happened, and why, is recorded here; recording the system's state is out of scope.

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

1. **Names.** Intent, interpretation, task, decision, action and guide are proposals. ADR 041's generic `structure` datum goes away in favour of these specific kinds, related by references.
2. **Does an interpretation live inside its intent, or apart from it?** Apart lets two agents interpret one intent; inside is simpler.
3. **How an agent names the source turn.** Today `current` names a session and `<session>:20-45` names turns (FDR 008). An agent may need "the operator's last message" without knowing turn numbers.
4. **Do actions other than edits belong in the alpha** (walkthrough 5), or only after edits and commits work?
