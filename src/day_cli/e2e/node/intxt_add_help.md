# `day x intxt add --help`

```console
$ day x intxt add --help
Add commits, a branch or a jj change to an intxt, with the sessions that produced them

Run it in the worktree that holds the work. What is named is resolved now, to commits: a range is stored as its tip and merge base, never as the text you typed. The objects the commits need are carried as a git pack (thin against the base unless `--full`), in the node's blob store, and the sessions whose recorded edits produced content in those commits come with them. Adding something already in the intxt changes nothing and says so. Names the intxt by the short id `show` prints, or by its full drawer id.

Usage: day exec intxt add [OPTIONS] <--commits <RANGE>|--branch <NAME>|--change <CHANGE>> <INTXT>

Arguments:
  <INTXT>
          The intxt: its short id, or its full drawer id

Options:
      --commits <RANGE>
          Commits: a range `main..feat/x` (its tip and its merge base with `main`), or one commit (against its first parent). Brings those commits and the files they touch

      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

      --branch <NAME>
          A local branch: its commits since its merge base with its upstream, or with `main` (`master`) when it has none, and the branch itself, tracked as this clone sees it, so commits it moves onto later come in as new revisions. Brings what `--commits` brings

      --change <CHANGE>
          A jj change, by its change id, followed through its rewrites: every commit the change has had comes in. Needs `jj`

      --full
          Pack every object, not only what the base does not have
          
          The default pack is thin: it assumes whoever receives the intxt already has the base commit. A full pack stands alone and is several times larger.

  -h, --help
          Print help (see a summary with '-h')

Examples:
  day x intxt add qx --branch feat/refs
  day x intxt add qx --commits main..feat/refs
  day x intxt add qx --change kxqoltvm

```

# `day x intxt list --help`

```console
$ day x intxt list --help
List every intxt remembered by this node, with counts of its commits, refs and sessions

Usage: day exec intxt list [OPTIONS]

Options:
      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]]

  -h, --help
          Print help (see a summary with '-h')

Examples:
  day x intxt list
  day x intxt list --node ~/.local/share/day/node

```

# `day x intxt drop --help`

```console
$ day x intxt drop --help
Forget an intxt on this node

The drawer and anything already shared stay; it only stops being listed here, and naming it by its full id brings it back.

Usage: day exec intxt drop [OPTIONS] <INTXT>

Arguments:
  <INTXT>
          The intxt: its short id, or its full drawer id

Options:
      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]]

  -h, --help
          Print help (see a summary with '-h')

Examples:
  day x intxt drop qx
  day x intxt drop qy

```

# `day x intxt show --help`

```console
$ day x intxt show --help
Show what an intxt holds: its commits, jj changes, tracked refs and sessions

Each entry has a short id, the shortest prefix that is unique among the intxt's entries.

Usage: day exec intxt show [OPTIONS] <INTXT>

Arguments:
  <INTXT>
          The intxt: its short id, or its full drawer id

Options:
      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

  -h, --help
          Print help (see a summary with '-h')

Examples:
  day x intxt show qx

```

# `day x intxt status --help`

```console
$ day x intxt status --help
Say what is recorded in this worktree and which intxts include it

Run it in a worktree: it lists the ops and sessions capture recorded here, and the intxts that include this worktree's commits, its clone's tracked refs or its sessions.

Usage: day exec intxt status [OPTIONS]

Options:
      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

  -h, --help
          Print help (see a summary with '-h')

Examples:
  day x intxt status

```
