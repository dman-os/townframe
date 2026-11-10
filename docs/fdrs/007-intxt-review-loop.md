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
2. **The work becomes an intxt.** The agent or the operator opens an intxt for the task with a CLI command, or the adapter opens one when the agent first commits on a branch that has none. Its title and description come from the agent and can be edited by anyone.
3. **A revision is recorded.** Each time the branch moves (a new commit, an amend, a rebase), the intxt records a new revision: the commits now on the branch and the contents of every file they touch. Earlier revisions remain.
4. **The operator reviews, anywhere.** The intxt syncs to the operator's other devices through their relay. On a phone or desktop they see:
   - the change file by file, with the code itself, not only the patch;
   - the commits it is made of;
   - the session that produced it, with the agent's notes and any structure it left;
   - every note, at the place it refers to.
5. **The operator responds.** They comment on a line range, a file, a commit, a session message or another note; reply in threads; and resolve threads. Several devices can be used at once; nothing they write overwrites another.
6. **The agent picks it up.** At its next turn, the adapter gives the agent the feedback it hasn't seen yet, with where each comment points. An agent that is idle can wait for feedback with a CLI command. It replies and resolves threads through the CLI, and acts on the feedback by changing code.
7. **The next revision.** The agent's new commits produce revision 2. The operator sees what changed since the revision they reviewed. Each earlier comment is shown either attached to the matching code in the new revision or as referring to the earlier revision; none disappear.

The loop repeats until the operator is satisfied. Finishing (merging, pushing to GitHub) happens outside intxt for now.

## What each user does

**Operator**
- Lists intxts with new activity and unseen comments.
- Reads a revision by file or by commit, and compares any two revisions.
- Reads the session alongside the code and jumps from a note to what it refers to.
- Writes comments, replies and resolutions from any device.
- Sees for every comment who wrote it, and whether it is current or refers to an earlier revision.

**Author agent**
- Optionally opens an intxt and leaves notes or structure (for example "these turns implemented X because of Y") when that is cheap.
- Receives new feedback automatically at its next turn; can also wait for it explicitly.
- Replies to and resolves threads.
- Otherwise works as it already does; commits drive revisions.

**Harness adapter** (Claude Code first)
- Records the session in readable form: messages, edits, and changes made by commands, attributed to the turn that made them. Thinking is not recorded by default.
- Records commits and branch movements as they happen.
- Keeps observations that nothing refers to local, and ignores files the repository ignores.
- Delivers unseen feedback into the agent's next turn.

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

1. **When an intxt opens automatically:** first commit on a branch without one (proposed), first push, or only when asked.
2. **Session scope:** a session that spans two branches contributes to two intxts; does each intxt show the whole session or only the parts relevant to it?
3. **What the agent is given at its next turn:** every unseen comment in full, a summary with references, or only comments on what it is working on now. This trades context-window cost against completeness.
4. **Comments that arrive mid-turn:** delivered at the next turn only, or able to interrupt.
