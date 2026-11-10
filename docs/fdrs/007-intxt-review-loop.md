# FDR 007: The intxt review loop

**Status:** Proposed. The first feature built on intxt (FDR 006). It describes the loop between an author agent and the operator. UI structure is in the claytxt lane's `docs/scratch/intxt-review-loop.md` (branch `docs/intxt-review-loop`); this FDR states the behaviour both sides depend on.

## The need

An operator runs agents on their machine and steps away. Today:

- Remote control of an agent session shows the conversation but not the files or the diff, so the operator can't actually review what was done.
- Reviewing on GitHub means pushing a PR first, works badly on a phone, and shows the diff without the session behind it.
- When the agent reworks the change (rebase, amend, squash), comments attached to the old commits stop lining up.
- Getting feedback back to the agent means copy-pasting it into the chat.

The review loop makes this one continuous flow: the agent's work shows up as an intxt the operator can read anywhere, the operator responds in place, and the agent receives the response without anyone relaying it.

## The loop

1. **The agent works.** A harness adapter records the session as it runs: messages, the edits made, and the commits created. The agent does nothing extra for this.
2. **The work becomes an intxt.** The agent or the operator makes an intxt from recorded work with a CLI command, and can later add more work to it. Its title and description come from the agent and can be edited by anyone. Once work is in a shared intxt it can't be taken back out, only superseded, so the choice of what goes in is deliberate.
3. **A revision is recorded.** Each time the branch moves (a new commit, an amend, a rebase), the intxt records a new revision: the commits now on the branch and the contents of every file they touch. Earlier revisions remain.
4. **The operator reviews, anywhere.** The intxt syncs to the operator's other devices through their relay. On a phone or desktop they see:
   - the change file by file, with the code itself, not only the patch;
   - the commits it is made of;
   - the session that produced it, with the agent's notes and any structure it left;
   - every note, at the place it refers to.
5. **The operator responds.** They comment on a line range, a file, a commit, a session message or another note; reply in threads; and resolve threads. Several devices can be used at once; nothing they write overwrites another.
6. **The agent picks it up.** The agent reads the intxt like any other document, when it chooses to or by waiting for changes, and finds the comments it hasn't seen and what each points at. Everything in an intxt is plain data an agent can read and write directly; no special feedback interface is needed. intxt never injects into or interrupts a running agent: steering belongs to the harness, and an agent that wants to be woken by new feedback can set that up itself. The agent replies and resolves threads by writing that data, and acts on the feedback by changing code.
7. **The next revision.** The agent's new commits produce revision 2. The operator sees what changed since the revision they reviewed. Each earlier comment is shown either attached to the matching code in the new revision or as referring to the earlier revision; none disappear.

The loop repeats until the operator is satisfied. Finishing (merging, pushing to GitHub) happens outside intxt for now.

## What each user does

**Operator**
- Lists intxts with new activity and unseen comments.
- Reads a revision by file or by commit, and compares any two revisions.
- Sees what is new since they last looked, even if the agent rebased in between (Jane Street Iron's per-reviewer "brain"; see `docs/scratch/intxt-prior-art.md`). What each reader has seen is their own, so two readers never disturb each other.
- Reads the session alongside the code and jumps from a note to what it refers to.
- Writes comments, replies and resolutions from any device.
- Sees for every comment who wrote it, and whether it is current or refers to an earlier revision.

**Author agent**
- Optionally opens an intxt and leaves notes or structure (for example "these turns implemented X because of Y") when that is cheap.
- Reads the intxt when it chooses, or waits for it to change; it is never interrupted by intxt.
- Replies to and resolves threads.
- Otherwise works as it already does; commits drive revisions.

**Harness adapter** (Claude Code first)
- Records the session in readable form: messages, edits, and changes made by commands, attributed to the turn that made them. Thinking is not recorded by default.
- Records commits and branch movements as they happen.
- Keeps observations that nothing refers to local, and ignores files the repository ignores.
- Only records. It does not steer the agent or put feedback into its context.

**Peer agent**
- Reads an intxt (code, session, notes) through the CLI and adds comments like any other participant.

## Behaviour that must hold

- The operator can review without git, without network access to the source repository, and without the author's machine being online.
- Rewriting history never loses a comment. Comments made against a rewritten commit remain readable at the revision they were made on.
- Edits made by commands (formatters, code generators, `sed`) are recorded as changes like any other, not missed because no edit tool was used.
- Recording costs the agent no extra tokens; only the notes it chooses to write cost anything.
- Two people or agents commenting at once never lose each other's comments.

## Not in this feature

- Approve / request changes. The loop ends when the operator says so; intxt doesn't manage trunks yet.
- GitHub publishing and comment sync.
- Check results and whether they cover other revisions.
- Public sharing. Sharing is between the operator's own devices and their relay.
- MCP access (CLI and hooks come first).

## Open questions

1. **Deciding what goes into an intxt:** recorded work may sit somewhere private first (a proposed working set, not settled); which commits, sessions and states the agent or operator promotes into an intxt, and how the CLI makes that choice easy and hard to get wrong, given that it can't be undone.

Settled while drafting (ledger D21–D23): an agent reads and writes an intxt as plain data, with no special feedback interface; a session spanning two intxts is one session in both and each viewer decides how much to show; intxt never injects into or interrupts an agent.
