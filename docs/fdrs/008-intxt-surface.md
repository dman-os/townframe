# FDR 008: The intxt surface

**Status:** Accepted 2026-11-19 (operator; ledger D71), third draft, written after the operator's first round of dogfooding. Its open questions stay open. How people and agents use intxt (FDR 006): which commands exist, what each one means, and how someone finds them. It is derived from the use cases in `docs/scratch/intxt-use-cases.md` (U1–U16). Datum shapes are in ADRs 036, 040 and 041; working notes are in `docs/scratch/intxt-ledger.md`.

**Changed in the third draft:**
- Commands are grouped into families that mirror the data's layers, replacing the loaded `add` and `rm` verbs.
- The bare command is the status view, and it brings in what it can (as jj snapshots on every command).
- `show` becomes a family.
- Comments get their own family.
- Recording is the `record` family.
- Unstable commands sit behind a gate instead of an `exp` prefix.
- Comments and recording are designed as vocabularies wider than intxt.

## Commands mirror the layers

intxt holds several layers of data: git and jj history, recorded sessions, comments, and later intents and tasks (FDR 010). Each layer has its own family of commands, named after the layer. A person who knows the data model can guess the command; a person who knows the command learns the data model. No verb is overloaded across layers.

```
day x intxt                                  # what's going on here, and bring in what can be brought in
day x intxt new | list | use | drop          # intxts themselves
day x intxt git add <range|commit>...        # git history, several at once
day x intxt git track <ref>...               # follow a ref from its current tip
day x intxt git untrack <ref>
day x intxt jj add <change>...
day x intxt session add <session>...
day x intxt comment add <target> -m …        # comments, on anything (below)
day x intxt comment reply <id> -m … [--resolve]
day x intxt comment resolve | reopen <id>
day x intxt comment list [--unanswered] [--for <participant>]
day x intxt show [<id>]                      # an intxt, or any object in it, in detail
day x intxt show session <id> [--turns 20-45]
day x intxt show file <commit>:<path>
day x intxt edit                             # one editable document per intxt (D64)
day x intxt record …                         # recording: hooks, ACP, git's side (below)
```

Removing something is the family's own verb (`git rm <id>`, `session rm <id>`, `comment rm <id>`), so what a removal touches is as clear as what an addition brings.

## The bare command

`day x intxt`, run in a worktree, answers "what is going on here?", as `jj` does. On the way it brings in whatever can be brought in without a decision:
- new commits on refs the intxt tracks;
- the sessions those commits came from.

```
$ day x intxt
worktree ~/repos/townframe (feat/refs)   on intxt m4 "Datum refs move to dtxt"
recorded here: 2 sessions (k7 running, q2 ended), 41 ops
brought in: feat/refs moved 3f2a9c1 → 8be01d4 (1 commit), with session k7
comments: 2 unanswered (day x intxt comment list)
```

This is how a tracked ref gets new commits: they come in when someone looks, not silently on every move. Sharing stays something that happens when a person or an agent runs intxt.

## Tracking a ref

`git track <ref>` starts at the ref's current tip, so it works for trunk, for a fresh branch with no commits yet, and for a branch already pushed for review. The bare command brings in what the ref moves onto after that. `git add <range>` is the snapshot form: those commits, fixed. A rebase or an amend of a tracked ref comes in as a new revision, linked to the one it rewrote.

## Comments are on anything

A comment targets any object an intxt holds, by its short id:
- an entry;
- a session, or a turn or tool call in it;
- another comment;
- later, FDR 010's intents and tasks.

Code keeps its own form, `<commit>:<path>:<lines>`. Every object `show` prints has a short id, so whatever can be seen can be commented on.

`comment list` is what was called `inbox`: threads that are unanswered or unresolved. "Unanswered" is relative to the participant asking:
- the operator;
- a particular agent session, found from the cross-tool `AI_AGENT` convention and the harness's session variable (D69).

`--for <participant>` lists what is addressed to someone. Addressing a thread to a participant (a mention, or an assignment) is the open question 2 below.

**Comments are not intxt's alone.** The comment vocabulary is generic (D28): a comment targets any datum location (ADR 041), so a daytxt document or a micro can carry the same comments. Before beta it moves out of intxt with the other generic datums (D39). The commands follow it, from `day x intxt comment` to a shared family.

## Recording

Recording moves raw data into the node: harness hooks, the ACP proxy, git's side of rewrites. Nothing in it calls for judgement, and a person meets it twice: when setting it up, and in the bare command's summary.

```
day x intxt record                            # what's recorded in this worktree, in detail
day x intxt record install claude-code        # writes the hook configuration
day x intxt record hook                       # the harness hooks' entry point
day x intxt record acp -- <agent command>     # an ACP client launches the agent through this
day x intxt record git-rewrite <how>          # git's post-rewrite hook
```

**Recording is not intxt's alone either.** An end user vibe-coding a micro in daytxt benefits from the same record of what an agent did. The capture vocabulary is already its own (ADR 040), and recording moves out with it, from `day x intxt record` to `day x record`.

## How things are named

People and agents name things the way they already know them, never by internal ids:

- **Git's own names:** a branch (`feat/refs`), a range (`main..feat/refs`), a commit (`a1b2c3d`).
- **Paths** in the worktree, with globs (`src/big_repo/**`).
- **Sessions:** `current` is the session being recorded in this worktree right now, and `last` is the most recently ended one. Any session can also be named by its short id. If `current` is ambiguous (two agents running in one worktree), the command fails and lists the candidates with their short ids.
- **Turns** within a session: `<session>:20-45`.

Things intxt creates have **short ids, as in jj**. Each object has a stable random id, rendered in an alphabet that can't be mistaken for a git hash. A short id is the shortest prefix that is unique among what is currently visible, and is always printed with the rest of the id dimmed. The same scheme serves dpaths, so it lives in a shared crate (`short_ids`). Petnames are an open question (3 below).

## Unstable commands

A command that isn't settled lives at its final path but runs only behind a gate:

- `-Z unstable` on the command line, or `DAY_UNSTABLE=1`;
- a one-line notice on stderr;
- hidden from help until the gate is given.

Stabilizing a command removes the gate, so no path changes and nothing breaks. cargo's `-Z unstable-options` is the precedent. A command becomes stable once the operator has signed it off by using it (D66). The gate is a `day`-wide convention, agreed with the node lane.

## Walkthroughs

Each one names who acts, what they know at that moment, what they type, and what they see.

### W1. Recording from the start (U1)

**Operator, once per machine:** `day x intxt install` installs intxt in the node, and `day x intxt record install claude-code` writes the harness's hook configuration. ACP clients launch agents through `day x intxt record acp -- <agent>`.

**Agent:** types nothing. Its sessions, the edits it makes in its worktrees (by any means, including `sed` and formatters), and its commits and branch moves are recorded as they happen. Nothing is shared and no intxt exists yet; recording only fills the node's capture drawer.

### W2. Packaging a piece of work (U1, U11)

```
$ day x intxt new "Datum refs move to dtxt"
intxt m4 "Datum refs move to dtxt"   this worktree is on it now
$ day x intxt git track feat/refs
m4 tracks feat/refs from 3f2a9c1: 7 commits since main, with sessions k7, q2
  undo: day x intxt git untrack feat/refs
```

**Agent, at the end of a task** (its skill asks it to): the same, plus a description through `edit`. It doesn't need its session id: the sessions come with the commits.

### W3. Adding something the defaults missed (U3)

A session explored the design on `main` before the branch existed, so tracking didn't pick it up.

```
$ day x intxt session add q9
added to m4: session q9 (claude-code, 11 turns, ended yesterday 18:40)
  undo: day x intxt session rm q9
```

### W4. Carving a large change (U4)

A 30,000-line change on one branch is really four changes. The operator, or an agent asked to do it, makes four intxts from the one:

```
$ day x intxt new "big_repo snapshot loading" --from m4 --paths 'src/big_repo/**'
intxt r8 "big_repo snapshot loading"
  from m4: 2 revisions, 4 commits, 9 files under src/big_repo, sessions k7 (turns 3-19)
```

Nothing is copied or rewritten: the new intxt references the same recorded work, so comments on either land on the same code. Relations between intxts (stacks on, follows up, supersedes) are written through `edit`.

### W5. Handing off to a fresh agent (U14)

A new session, or a teammate's agent, has to pick up m4: `day x intxt show m4` lists its items by time (`--by file`, `--by kind`, `--by session` sort differently). `show <item>` goes deeper:
- a session as turns;
- a revision as its commits and diff;
- a file at a revision.

`--json` on any of these gives the same content as data.

### W6. A branch that wasn't recorded (U11)

Work done before intxt was installed, or on another machine. `git add` and `git track` import the git history they need, as recording does. There are no sessions, since none were recorded, and `show` says so.

### W7. Reviewing with an agent on the same machine

The operator comments on a commit with `comment add`. The agent, running `day` inside its own session, is a different participant (D69). So its `comment list` shows the thread as unanswered. It fixes the code, commits, and replies with `comment reply <id> --resolve`. The operator's next bare `day x intxt` shows the reply and brings in the new commit.

## Finding commands and flags

- `day x intxt --help` lists the families in the order people meet them: intxts, then the bare command, then `git`/`jj`/`session`, then `comment`, `show`, `edit`, and `record` last, so recording doesn't crowd what people type.
- Every command's help ends with two or three real examples.
- Completion covers short ids, branch names and session names.
- The bare command and every error suggest the next command, with the arguments filled in where they are known.
- Every command that changes something prints the change and the command that undoes it.

## Nouns

| Noun | Meaning |
| --- | --- |
| **intxt** | A package of work, with a title and description. It names commits or jj changes, and may track refs. |
| **item** | One thing an intxt includes: a session, a revision, commits. Items come from recording and are shared, not copied. |
| **session** | An agent session as recorded: turns, messages, tool calls, plans. |
| **revision** | One recorded position of a tracked ref: its commits since the base. |
| **comment** | A note on any object, in a thread of replies; generic, not intxt's alone. |
| **participant** | Who wrote something: a person, or an agent session (D69); later, a long-lived identity. |

Reading order is no longer only a choice of sort: FDR 010's guides give an authored order where one is wanted.

## Behaviour that must hold

- A person can package work with commands they can guess from the bare command's output.
- No command needs an internal id. Short ids are only for things intxt itself prints.
- Every change can be undone with the command it prints.
- Including work in an intxt shares only the documents that work lives in, not the rest of the node's recordings. Content is carried as git objects in a pack (ADR 040 §4, D59).
- Nothing is shared without a person or an agent running intxt.

## Open questions

1. ~~**Which sessions a branch brings**~~ Settled 2026-11-12 (ledger D61): the sessions whose observed ops produced content in the branch's commits.
2. **Addressing a thread** to a participant: a mention in the body, or an assignment datum (ADR 041 reserves "whose turn a thread is waiting on")?
3. **Petnames:** for participants (`comment list --for dman-os`), for intxts instead of short ids, or both? The operator floated `day x intxt dman-os`.
4. **Carving by turns:** when `--from … --turns` takes part of a session, does the new intxt reference the whole session document, or does that need a finer document layout?
5. **The noun:** is it "item", or another word?
6. **The capture keep ref:** `refs/intxt/keep/*` shows in `git log --all`. Does an alternates object store, which would hide it, come before or after the alpha?
