# FDR 006: intxt, packages of development context

**Status:** Proposed. This document describes what intxt is for and how people and software use it. Data shapes, reference encoding and storage are technical design for later ADRs. The first feature built on it is FDR 007 (the review loop). Working notes: `docs/scratch/intxt-use-cases.md`, `intxt-ledger.md`.

## Why

Development increasingly happens inside one person's context with their agents: design, implementation and review interleave in the same sessions, and the reasoning lives in those sessions. The pull request is a delivery mechanism designed for distant, untrusted contributors: a reviewable unit with its CI gate and merge. Used as the unit of agent work it serves this poorly:

- It keeps the diff and drops how the change came about: the requests, the decisions, the dead ends, the agent's own account.
- It forces a choice between mega-PRs and expensive up-front slicing, and makes every intermediate state answer to CI.
- Rebases and rewrites orphan review comments, because comments hang off commits that no longer exist.
- Its review tools are slow and poor on phones, and remote agent tools show the chat but not the files.

An **intxt** is a package of development context: the changes, the sessions that produced them, the structure people and agents give the work, the discussion about it, and evidence about it. Delivering a change (a branch on GitHub, a patch) is one thing you can do with an intxt, not what it is.

intxt is not an agent harness, an editor, a CI system, a distribution format, or a version-control system. It works alongside all of them.

## Who uses it

intxt's users are people and software alike:

- **The operator**: the person directing the work. Reads, annotates and decides, often from a phone.
- **Author agents**: coding agents doing the work, in any harness. They should not need to spend effort to be recorded.
- **Harness adapters**: plugins or hooks for Claude Code, Codex, OpenCode, Pi and others that record what a session does into intxts.
- **Peer agents**: agents that review or build on another agent's work, reading the same package a human reads.
- **Verifiers**: anything that runs checks (the agent itself, a hook, CI) and records what it ran and what happened.
- **Bridges**: git and jj import; later, GitHub publishing and comment sync.
- **Other apps**: daytxt chat and other Daybook software whose items can be members of an intxt or be talked about in one.

## What an intxt contains

An intxt is a collection with its own identity, title and description. Its members are items that different users contribute:

- **Working copies**: each checkout the work happened in, possibly several, in several repositories, with every change to its files as it happened, so the work can be read without git and without the machine it ran on.
- **Git and jj history**: the commits and branch positions the work went through, including ones later rewritten. Version control is a layer over the working copies, not the thing intxt records.
- **Sessions**: what happened in agent sessions, in a readable form rather than raw logs.
- **Notes**: annotations, replies, explanations, guides through the change.
- **Structure**: tasks, decisions and groupings that say which parts of the work belong together and why.
- **Evidence**: results of checks, about the exact version they ran against.

New kinds of items can be added by any producer, and they have the same standing as intxt's own: the review loop (FDR 007) is one set of kinds among others. A reader that doesn't understand a kind still keeps it intact and shows it generically; it never drops or rewrites it. Where an existing general kind fits (a title, a note, a comment), intxt uses it rather than its own, so other apps understand intxt content and intxt understands theirs.

An item can belong to several intxts. A session that produced two changes is in both; a check run can be relevant to a stack of intxts; an intxt can include or refer to another. Nothing is copied to make this work.

## Talking about anything

Any user can point at any item, or part of an item, at a specific version, and attach something to it:

- a line range in a file, a whole file, a commit;
- a message, a stretch of turns, or one edit in a session;
- a section of a guide, a check result, another note.

Code is one kind of thing to talk about, not the frame everything must fit into. A comment on a session message or on a check result is as ordinary as a comment on a line.

A pointer always records exactly what it was made about. When the item later changes, the reader shows whether the pointer follows to the new version, or that it refers to an older one. It is never silently moved to a guess.

## History is kept

intxt records the intermediate states of work, not only the result: earlier revisions, rewritten commits, what an agent did before changing approach. Rewriting git history doesn't remove what an intxt has already recorded, so comments made on an earlier revision stay readable and can be followed forward.

Recording observes a great deal locally (every change, every command), and it stays on the machine until an intxt includes it. What enters a shared intxt is what someone included or what something in it refers to, a whole session or a whole file's history at a time. Content that should not have been shared (a leaked secret in an intermediate state) can be taken out by replacing what holds it with a cleaned copy; that stops it spreading further, but anyone who already pulled it keeps it, because shared history is not rewritten.

Readers expect gaps. An intxt is spread over many pieces, some private or not yet synced, so a reader shows what it can reach and marks what it can't, the way a web page shows a missing image.

## Many participants at once

Several people and agents work in the same intxt at the same time, on different devices, sometimes offline. Their contributions merge without anyone losing work. Who wrote each contribution is attested by the system, not just a name typed into a field; content that names an author of its own (a commit's author line, a comment imported from GitHub) is shown as such, alongside who brought it in.

## Sharing

An intxt is the unit of sharing and syncing: a person shares an intxt with a collaborator, mirrors it to a relay so their other devices can reach it, or (later) makes it public. Parts that must stay private, typically full session transcripts, can be kept out of what is shared while the shared intxt still refers to them; someone without access sees that something is there, not its contents.

## Working with existing tools

- **git and jj** remain where code is versioned today. intxt imports what it needs from them (commits, branch positions, file contents) and keeps it, so a reader needs neither git nor network access to the original repository.
- **GitHub**: publishing an intxt as a branch or PR and syncing comments are planned bridges, not prerequisites.
- **Harnesses** are not replaced. Adapters record from them; agents keep working as they do.
- **A future Daybook version-control system** would be another source of code states. intxt would refer to it as it refers to git; neither depends on the other's change units.

## Out of scope for now

- Managing trunks, merges and approval vocabulary.
- Deciding when a check run covers other versions or other intxts in a stack.
- Being a version-control system.

## Open questions

1. When does a session's activity become an intxt: one per task, one per branch, one per session, or whatever the agent or operator decides? FDR 007 picks a default for the review loop.
2. Who decides what an intxt includes (the agent, the operator, adapter defaults), and how the CLI makes that choice easy and hard to get wrong.
3. How a reader is told that a pointer follows to a newer version versus refers only to an older one, for kinds other than code.
