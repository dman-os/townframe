# intxt: use cases and settled decisions (pre-FDR)

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: The concrete use cases the intxt FDRs must cover, and the design decisions that are settled enough to stop discussing. It is the input for the intxt FDRs (functional) and then ADRs (technical). Data-model sketches live in `intxt-design.md` (draft v0, partly superseded below); decisions and findings are tracked in `intxt-ledger.md`.

## What intxt is for

Development now happens mostly inside one person's context with their agents: design, implementation and review interleave in the same sessions. The PR model is built around delivery between distant, untrusted parties: a reviewable unit, its CI gate, its merge. That makes it the wrong unit for this way of working. It pushes agents and people towards either mega-PRs or costly up-front slicing, loses context at every rebase, and keeps none of the reasoning that produced the code.

An intxt is a **package of development context**: the changes, the sessions that produced them, the structure people and agents give to that work, the discussion about it, and the evidence that it works. Many systems write into it (harness adapters, CI, IDEs, the intxt micro, other agents), and the format stays usable for whatever vocabulary they bring. Delivery (a PR on GitHub, a patch email) is one thing you can do with an intxt, not what it is.

It is not: an agent harness, an editor, a CI system, a package/OCI distribution format, or (yet) a VCS.

## Actors

- **Author agent**: the coding agent doing the work (Claude Code, Codex, OpenCode, Pi, …), through a harness adapter it doesn't have to think about.
- **Operator**: the human directing the agents. Reviews mostly on the phone, sometimes on desktop.
- **Peer agent**: another agent lane reviewing or consuming the work.
- **Verifier**: whatever runs checks: the author agent locally, a hook, a CI runner.
- **Reader**: someone given access later (a collaborator, the public, a fresh agent session).

## Use cases

Each use case names what has to be true for it to work. The FDRs should be able to point at these.

### Capturing the work

**U1. Live session capture.** An author agent works on a task. Its session (messages, decisions, the edits it made) lands in an intxt as the session runs, via a harness adapter (hooks/plugin), not via instructions to the agent. The agent spends no tokens on capture.
- Needs: a normalized session shape that every harness can map into; per-edit records; capture that tolerates rewinds, forks, subagents and compaction.

**U2. Structuring work while it happens.** While exploring, the agent (or operator) leaves cheap markers: "turns 20–45 were about X because the operator asked Y", "decided Z here", "this supersedes the approach in turn 12". No finished guide is required at any point; structure accretes and can be revised later.
- Needs: annotations that target ranges of turns and edits, written as the work happens; revisable without rewriting history; token cost of one short tool call, rarely.

**U3. Many-to-many sessions and intxts.** One session produces work that ends up in two intxts (p3N-4's session produced #80 and #81). One intxt gathers several sessions (subagents, three lanes collaborating, a review agent).
- Needs: sessions as their own documents that several intxts include or reference; no copying.

### Shaping the work

**U4. Carving and remixing.** The operator or agent splits an intxt's work into logical changes, moves a change into another intxt, stacks one intxt on another, or marks a follow-up, without git history surgery and without forcing every intermediate state to be green.
- Needs: logical changes as first-class groupings over edits/hunks/files that can span commits and be regrouped; intxt-to-intxt relationships (stacks on, follows up, supersedes, includes).

### Reviewing the work

**U5. Phone review loop.** The operator opens an intxt on the phone: sees the change by logical section or by file, the agent's annotations inline, can reply, annotate, resolve and give a verdict. The author agent, still running, picks up the feedback and acts on it.
- Needs: the code carried in the intxt (the phone has no git); anchors from annotations into code; a way for the agent to wait for and receive responses.

**U6. Review across revisions.** The agent pushes revision 2, including a rebase or jj rewrite. The operator sees what changed between revisions; earlier comments stay attached to their code, follow it to the new revision, or show as outdated. Nothing is silently lost.
- Needs: every pushed revision retained; anchors that track across revisions (CRDT cursors where history is shared, content matching otherwise); revision identity stable across rewrites.

**U7. Agents reviewing with context.** A peer agent reviews the intxt with the session included, so its review is informed by what was asked and decided, not only by the diff. Its review is ordinary annotations.
- Needs: the whole package readable by an agent (MCP/CLI); reviewer agents writing the same datums humans do.

**U8. "Why does this line exist?"** From a line of code, find the edit that introduced it, the turn that made the edit, and the request or decision behind it.
- Needs: edit→turn links recorded at capture; an index from code positions to edits maintained incrementally, not reconstructed on demand.

### Verifying the work

**U9. Local evidence.** The agent or a hook runs checks on an exact commit in a described environment (e.g. a Nix closure / flake lock hash) and attaches the result. The operator sees what was actually run, against what, versus what the agent claims.
- Needs: evidence separate from annotations; its subject is an exact version; environment recorded; trust in the recorder is explicit (local runs are trusted by default in a single-operator world).

**U10. Evidence across a stack.** One check run at the tip of a stack covers the intxts below it; checks that a later revision doesn't affect needn't be rerun. The operator sees which intxts a run covers and why.
- Needs: evidence attached to versions, not to intxts; intxt ordering/ancestry to reason about coverage; coverage shown as derived, never silently assumed.

### Interop and sharing

**U11. Git and GitHub.** Import a PR, branch or commit range into an intxt; publish an intxt as a branch/PR. Later: sync GitHub comments both ways.
- Needs: git commit/tree identities recorded with revisions; content carried, not just referenced.

**U12. jj first-party.** jj change ids are recorded, so a change rewritten by jj stays the same logical change across revisions.

**U13. Sharing selectively.** Make an intxt (code, guide, discussion) public or shared with a collaborator while keeping the raw session private; the relay holds what it is given without reading private parts.
- Needs: private parts in separately governed documents/drawers that the shared intxt references.

**U14. Context handoff.** A fresh agent session (or a teammate) loads an intxt to get what happened, what was decided and what's open, instead of recapping raw logs.
- Needs: the structured parts (U2, U4) to be readable without the full transcript.

**U15. Beyond code.** A design discussion is a document plus the chat that produced it plus annotations; daytxt chat is a member like any session.
- Needs: nothing code-specific in the package core; code is one vocabulary.

### Later (VCS stage; shapes the model, not built now)

**U16. Native edits.** An agent works in a Daybook checkout (`feat/dpaths` FDRs 001–004). Each edit is ingested as a change on the file's document as it happens, and the session records which turn produced which heads. Provenance is exact, not matched.

## Settled decisions

Proposed as settled; strike any you disagree with.

| # | Decision | Source |
|---|---|---|
| S1 | One intxt is one drawer; its descriptor doc holds the intxt's own metadata | operator |
| S2 | Clients write intxt datums directly; a nodule only does what clients can't | operator |
| S3 | An intxt composes documents it doesn't own; any document (session, file, evidence) can be in several intxts via drawer membership or references | operator, drawer model |
| S4 | A VCS stays separate from the intxt; the intxt references whatever holds the code (git today, Daybook docs later) | design discussion |
| S5 | The code an intxt discusses is carried in it: one Automerge Text document per touched file, starting at base content and edited to each revision with line-level splices. A revision is the set of file-doc heads plus the git (and jj) identities it came from. Grouping several files per doc is a later optimisation | operator, F1 |
| S6 | Every pushed revision is retained, including rebases and rewrites | operator |
| S7 | References are target (with version) + selector + optional quote; code anchors are Automerge cursors with quote matching as the fallback for moved code; a stored reference is never silently rewritten | design discussion, F1 |
| S8 | Sessions are stored normalized (turns, messages, edits as patches, markers), not as raw logs; raw logs are optional and separate; thinking is not stored by default | operator, F2 |
| S9 | Capture is a harness adapter's job (hooks/plugins); the agent adds structure only when it is cheap, as it works | operator, Delta |
| S10 | Evidence is its own datum about an exact version and environment, never an annotation's claim | design discussion |
| S11 | Authorship is the datum signature, not a field | ADR 020 |
| S12 | Private parts (raw or full sessions) live in separately governed docs/drawers; a reference grants nothing; a relay can hold them via a Relay-level child entry | drawer model |
| S13 | No "layers": the graph is references; which doc something lives in is only about sync and access | operator |
| S14 | Docs: FDRs first (functional, dpaths-style), then ADRs | operator |

## Open decisions the FDRs must settle

- **O1. Session schema vs ACP.** Adopt ACP's session-update vocabulary as the normalized shape, or our own datums informed by it (with import adapters per harness)? Lean: our own datums, ACP-compatible naming, so the format isn't hostage to a protocol revision.
- **O2. Session document granularity.** One doc per session, or split long sessions? Normalized conversation for our largest session is ~1–3 MB, so one doc per session looks fine; subagents as their own session docs linked to the parent.
- **O3. Revision identity across rewrites.** What makes revision 2 "the same change" as revision 1: the intxt, a logical change, a jj change id, patch identity? How is the inter-revision diff computed when the base moved?
- **O4. Where work structure lives.** U2's task/decision markers: in the session doc (the agent writes them as it goes) or in the intxt (they span sessions)? Lean: intxt-level datums that target turn ranges in any session.
- **O5. What "logical change" is.** U4's unit: a named grouping of edits/hunks (annotation-like), or something with its own revisions that can move between intxts?
- **O6. Evidence coverage semantics.** U10: what lets a run at version X count for intxt Y (ancestry, unchanged inputs, declared dependency)? Shown as derived; what's the rule?
- **O7. Agent-facing interface.** How the agent reads feedback and writes structure: MCP tools generated from datum Lexicons (`day mcp`), a CLI, or harness hooks injecting feedback. Probably all three; which is first.
- **O8. FDR split.** One FDR for the package, or several (package & references; sessions & capture; review & revisions; evidence)?
