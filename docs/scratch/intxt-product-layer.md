# intxt: an opinionated product layer (product notes)

Status: Notes, operator's idea of 2026-11-12, not a proposal yet
Owner: intxt lane
Updated: 2026-11-12
Scope: A product built on top of intxt's data layer. It keeps the operator's idea as stated, maps it to what exists, and lists what it would ask of the data layer. It changes no decision.

## The idea

The data layer is deliberately unopinionated: it works with whatever harnesses, VCSs and vocabularies already exist, and it's built for interoperability. A product can sit on top of it with opinions, its own nouns and its own way of presenting work.

1. **Human nouns.** The data layer's nouns (sessions, worktrees, clones, machines, harnesses) are too many to follow how an intxt came to be. The product names things the way people talk about work: **threads** (as in Delta), and names that agents give to their group, their session or their piece of work ("the D50 lane", "the ACP recorder"), not "a Claude Code session on a Linux VM in worktree X".
2. **A live intxt.** An intxt is incremental. Opening one while the work is still going on shows a live stream of what's happening: which agents are working, on what, and which goals have been met. intxt doubles as an observability surface for agents, including a swarm of agents working together across several intxts.
3. **Structure recorded as the work happens.** While working, an agent knows far more about what it's doing than a diff shows: "done in response to the operator's request at this turn", "decided here, amended there". Today that lives in documents like this lane's ledger, its ADRs and its scratch notes. The product records it as structured metadata, as the work goes on, so it becomes part of the intxt.
4. **Review against what was asked.** With requests and deliverables recorded, review at the end can answer whether everything that was asked for was delivered, and lets the operator choose: of 20 things asked for, this intxt carries 7.
5. **The intxt is the record, not the workplace.** It isn't where a person types in tasks; working practice is personal and belongs elsewhere. At the end of the work, it should show why it was produced, how, and why each decision was made.

## How it maps to what exists

- **Threads, work names:** a naming layer over sessions and the agents' own labels. It needs a place for agent-given names (a title on a session, a group datum). Nothing exists yet.
- **The live stream:** intxt documents already sync as they're written. A live view is a reader that watches them; nothing in the data layer blocks it. A swarm view needs sessions to say which group they belong to.
- **Structure as you go:** U2 (structuring work while it happens), D9 (tasks and decisions as datums) and `Structure` with `task` and `decision` kinds in ADR 041 cover the shape. The ledger is a working example of what agents would record: requests, decisions with their sources, findings, amendments.
- **Requested versus delivered:** requests are turns or messages, and deliverables are commits, documents and decisions. This needs a link from a deliverable to the request it answers, and a way to pick a subset into an intxt (a cut, D57).

## What it asks of the data layer (to check later)

1. A datum for an agent-given name or group, so threads and swarms are data, not guesses.
2. A link from work to the request it answers: a `Structure` targeting a turn, or a field on it.
3. Readers that can follow an intxt live: the cabinet's change notifications (FDR 008's `inbox --wait` needs the same).

## Open

1. Is "thread" the product's word for a session, a line of work, or a request and everything done for it?
2. Should the ledger practice itself be what agents record into an intxt? Its decisions, findings and sources are already structured by hand.
