# FDR 008: The intxt surface

**Status:** Proposed (2026-11-12, second draft). How people and agents use intxt (FDR 006): which commands exist, what each one means, and how someone finds them. It is derived from the use cases in `docs/scratch/intxt-use-cases.md` (U1–U16), walked through as the operator and as an agent. This week covers agent work and git. Review (comments, threads, verdicts) and GitHub are next week's, so their commands are only outlined here. Datum shapes are in ADRs 036, 040 and 041; working notes are in `docs/scratch/intxt-ledger.md`.

## Two kinds of commands

intxt has two kinds of commands, and they are designed differently.

- **Recording** moves raw data into the node: harness hooks, the ACP proxy, importing git history. Nothing in it calls for judgement. It runs unattended, writes datums directly (cabinet writes through the client library), and is designed for correctness and cost, not for typing. A person meets it twice: when setting it up, and in `status`.
- **Packaging** (and, next week, **reviewing**) is where a person or an agent decides something: what a package of work is, what belongs in it, what to say about it. These commands are designed for people at a shell and for agents with only a shell, and they are the subject of this FDR.

## How things are named

People and agents name things the way they already know them, never by internal ids:

- **Git's own names:** a branch (`feat/refs`), a range (`main..feat/refs`), a commit (`a1b2c3d`).
- **Paths** in the worktree, with globs (`src/big_repo/**`).
- **Sessions:** `current` is the session being recorded in this worktree right now, and `last` is the most recently ended one. Any session can also be named by its short id. If `current` is ambiguous (two agents running in one worktree), the command fails and lists the candidates with their short ids.
- **Turns** within a session: `<session>:20-45`.

Things intxt creates (intxts, the items in them) have **short ids, as in jj**. Each object has a stable random id, rendered in an alphabet that can't be mistaken for a git hash. A short id is the shortest prefix that is unique among what is currently visible: the live intxts, or the items of the intxt being shown. Short ids grow only as needed and are always printed with the rest of the id dimmed. The same scheme serves dpaths, so it lives in a shared crate.

## Walkthroughs

Each one names who acts, what they know at that moment, what they type, and what they see.

### W1. Recording from the start (U1)

**Operator, once per machine:** `day x intxt install` installs intxt in the node. Recording is then set up per harness: Claude Code's hooks call `day x intxt capture`, and ACP clients launch agents through `day x intxt record -- <agent>`. Install prints the exact hook configuration to paste in.

**Agent:** types nothing. Its sessions, the edits it makes in its worktrees (by any means, including `sed` and formatters) and its commits and branch moves are recorded as they happen. Nothing is shared and no intxt exists yet; recording only fills the node's capture drawer.

**Operator, any time, in a worktree:** `day x intxt status` answers "what has been recorded here, and what is packaged?":

```
worktree ~/repos/townframe (feat/refs, 3 revisions since main)
sessions recorded here:
  k7  claude-code  running   started 14:02   42 turns
  q2  claude-code  ended     11:30           18 turns
in intxts: none
  package it:  day x intxt new "<title>" --branch feat/refs
```

### W2. Packaging a piece of work (U1, U11)

The work is done, or done enough to look at. The packager knows the branch, and perhaps that this session did it.

**Operator:**
```
$ day x intxt new "Datum refs move to dtxt" --branch feat/refs
intxt m4 "Datum refs move to dtxt"
  branch feat/refs   3 revisions, 7 commits since main, 23 files
  sessions           k7, q2 (the sessions that changed files on feat/refs)
  undo: day x intxt drop m4
```

**Agent, at the end of a task** (its skill asks it to): the same command, plus `--description` written by the agent. The agent knows its branch from git; it doesn't need its session id, because the sessions come with the branch.

An intxt says two separate things about history (ledger D53):

- **It names commits:** `--commits <range>` (`main..feat/refs`, a single commit) or `--change <jj change>`. Named commits are fixed. Named jj changes include their rewrites, since jj keeps a change's id when it is amended or rebased.
- **It tracks refs:** `--branch feat/refs` names the commits from the branch's base to where it points now, and also tracks the branch, so commits it moves onto later (new commits, amends, rebases) come in as new revisions. Only positions a recorder observed count. A ref is tracked by name, and each machine's recorder contributes the positions it saw (D54).

`--branch` is the common path. Whichever way the work is named, the command decides what comes with it: the commits, the files they and the uncommitted work touched, and the sessions whose edits landed on them. If the commits were never recorded live (W6), their history is imported first. That is recording, so it needs no confirmation.

### W3. Adding something the defaults missed (U3)

A session explored the design on `main` before the branch existed, so `--branch` didn't pick it up.

```
$ day x intxt add m4 --session q9
added to m4: session q9 (claude-code, 11 turns, ended yesterday 18:40)
  undo: day x intxt rm m4 q9
```

`add` takes exactly one kind of thing per flag: `--session`, `--branch`, `--commits <range>`, `--change <jj change>`. Each flag's help says what it brings with it (`--session` brings the session; `--commits` brings those commits and the files they touch). Adding something already in the intxt changes nothing and says so.

`rm m4 <item>` removes an item by the short id `show` printed. It stops further sharing; copies already synced to others stay with them (FDR 007).

### W4. Carving a large change (U4)

A 30,000-line change on one branch is really four changes. The operator, or an agent asked to do it, makes four intxts from the one:

```
$ day x intxt new "big_repo snapshot loading" --from m4 --paths 'src/big_repo/**'
intxt r8 "big_repo snapshot loading"
  from m4: 2 revisions, 4 commits, 9 files under src/big_repo, sessions k7 (turns 3-19)
  undo: day x intxt drop r8
$ day x intxt link r8 --stacks-on m4
```

`--from` with `--paths`, `--commits` or `--turns` selects part of an existing intxt. Nothing is copied or rewritten: the new intxt references the same recorded work, so comments on either land on the same code. The source intxt stays as it was, and dropping it afterwards is a separate decision.

Relations between intxts are `link` with one of `--stacks-on`, `--follows-up`, `--supersedes`. An intxt never contains another.

### W5. Handing off to a fresh agent (U14)

A new session, or a teammate's agent, has to pick up m4.

```
$ day x intxt show m4
m4 "Datum refs move to dtxt"   tracks feat/refs   stacks on: none
  description …
items (by time):
  q9  session    claude-code   yesterday 18:40   11 turns   "explore dtxt refs"
  k7  session    claude-code   today 14:02       42 turns   running
  v1  revision   feat/refs @ 3f2a9c1   today 15:10   4 commits
  v2  revision   feat/refs @ 8be01d4   today 16:44   amend of v1
  …
```

`show` sorts items by time by default; `--by file`, `--by kind` and `--by session` sort differently. An item that came from another intxt shows where it came from. To go deeper: `day x intxt read <item>` prints a session as turns (`--turns 20-45`), a revision as its commits and diff, a file at a revision. `--json` on any of these gives the same content as data.

### W6. A branch that wasn't recorded (U11)

Work done before intxt was installed, or on another machine. `--branch` and `--commits` import the git history they need (commits, the branch's current position, file contents) as recording does. There are no sessions, since none were recorded, and `show` says so. Live branch moves after that are recorded as usual.

## Finding commands and flags

- `day x intxt --help` groups commands as **Packaging** (`new`, `add`, `rm`, `drop`, `link`, `show`, `read`, `status`, `list`), **Review** (next week) and **Recording, run by hooks** (`install`, `capture`, `record`). Recording comes last, so it doesn't crowd what people type.
- Every command's help ends with two or three real examples.
- Completion covers short ids, branch names and session names.
- `status` and every error suggest the next command, with the arguments filled in where they are known.
- Every command that changes something prints the change and the command that undoes it.

## Nouns

| Noun | Meaning |
| --- | --- |
| **intxt** | A package of work, with a title and description. It names commits or jj changes, and may track refs. |
| **item** | One thing an intxt includes: a session, a revision, commits, files. Items come from recording and are shared, not copied. |
| **session** | An agent session as recorded: turns, messages, tool calls, plans. |
| **revision** | One recorded position of a branch: its commits since the base, and the files as they stood. |
| **link** | A relation from one intxt to another: stacks on, follows up, supersedes. |

Items are sorted, not authored into an order. There is no story or guide layer: reading order is a choice of sort.

## Behaviour that must hold

- A person can package work with one command they can guess from `status`.
- No command needs an internal id. Short ids are only for things intxt itself prints.
- Every change can be undone with the command it prints.
- Including work in an intxt shares only the documents that work lives in, not the rest of the node's recordings. This constrains document granularity (ADR 040 amendment, to come): a long-lived document that every intxt would reference, such as one holding a project's whole commit history or a file's every edit, is not allowed.

## Open questions

1. **Which sessions a branch brings:** the sessions whose edits landed on the branch (proposed), or also sessions that only read it?
2. **Carving by turns:** when `--from … --turns` takes part of a session, does the new intxt reference the whole session document (simple, more shared), or does that need a finer document layout (see the granularity constraint)?
3. **The noun:** is it "item", or another word?
