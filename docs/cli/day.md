# Command-Line Help for `day`

This document contains the help content for the `day` command-line program.

**Command Overview:**

* [`day`↴](#day)
* [`day node`↴](#day-node)
* [`day node init`↴](#day-node-init)
* [`day node start`↴](#day-node-start)
* [`day node stop`↴](#day-node-stop)
* [`day node status`↴](#day-node-status)
* [`day carts`↴](#day-carts)
* [`day carts dev-takeover`↴](#day-carts-dev-takeover)
* [`day peers`↴](#day-peers)
* [`day peers add`↴](#day-peers-add)
* [`day peers ls`↴](#day-peers-ls)
* [`day drawers`↴](#day-drawers)
* [`day drawers ls`↴](#day-drawers-ls)
* [`day drawers remember`↴](#day-drawers-remember)
* [`day drawers forget`↴](#day-drawers-forget)
* [`day drawers add-member`↴](#day-drawers-add-member)
* [`day drawers revoke-member`↴](#day-drawers-revoke-member)
* [`day grants`↴](#day-grants)
* [`day grants pending`↴](#day-grants-pending)
* [`day grants accept`↴](#day-grants-accept)
* [`day grants decline`↴](#day-grants-decline)
* [`day grants list`↴](#day-grants-list)
* [`day grants revoke`↴](#day-grants-revoke)
* [`day dump`↴](#day-dump)
* [`day dump node`↴](#day-dump-node)
* [`day dump drawer`↴](#day-dump-drawer)
* [`day dump doc`↴](#day-dump-doc)
* [`day dump grants`↴](#day-dump-grants)
* [`day dump carts`↴](#day-dump-carts)
* [`day exec`↴](#day-exec)
* [`day exec intxt`↴](#day-exec-intxt)
* [`day exec intxt install`↴](#day-exec-intxt-install)
* [`day exec intxt add`↴](#day-exec-intxt-add)
* [`day exec intxt edit`↴](#day-exec-intxt-edit)
* [`day exec intxt new`↴](#day-exec-intxt-new)
* [`day exec intxt rm`↴](#day-exec-intxt-rm)
* [`day exec intxt list`↴](#day-exec-intxt-list)
* [`day exec intxt drop`↴](#day-exec-intxt-drop)
* [`day exec intxt which`↴](#day-exec-intxt-which)
* [`day exec intxt show`↴](#day-exec-intxt-show)
* [`day exec intxt comment`↴](#day-exec-intxt-comment)
* [`day exec intxt reply`↴](#day-exec-intxt-reply)
* [`day exec intxt inbox`↴](#day-exec-intxt-inbox)
* [`day exec intxt use`↴](#day-exec-intxt-use)
* [`day exec intxt pr-block`↴](#day-exec-intxt-pr-block)
* [`day exec intxt status`↴](#day-exec-intxt-status)
* [`day exec intxt doctor`↴](#day-exec-intxt-doctor)
* [`day exec intxt github`↴](#day-exec-intxt-github)
* [`day exec intxt github pull`↴](#day-exec-intxt-github-pull)
* [`day exec intxt github ls`↴](#day-exec-intxt-github-ls)
* [`day exec intxt github show`↴](#day-exec-intxt-github-show)
* [`day exec intxt record`↴](#day-exec-intxt-record)
* [`day exec intxt record install`↴](#day-exec-intxt-record-install)
* [`day exec intxt record install claude-code`↴](#day-exec-intxt-record-install-claude-code)
* [`day exec intxt record hook`↴](#day-exec-intxt-record-hook)
* [`day exec intxt record acp`↴](#day-exec-intxt-record-acp)
* [`day exec intxt record git-rewrite`↴](#day-exec-intxt-record-git-rewrite)
* [`day completions`↴](#day-completions)

## `day`

**Usage:** `day [OPTIONS] <COMMAND>`

Exit codes:
- 0: success
- 1: failure, the command could not do what was asked (the error is on stderr)
- 2: usage error, the arguments were not understood
- 3: no server running (`day node status`, which still reports what it found)

Output goes to stdout and diagnostics to stderr. Only `day node start` logs as it serves;
the other commands show warnings, and RUST_LOG overrides the level. Colour is used on a
terminal unless NO_COLOR is set.

###### **Subcommands:**

* `node` — Set up, run, stop and inspect the node
* `carts` — Installed cartridges
* `peers` — Manage the peers the node's server dials at start
* `drawers` — Share drawers with other nodes
* `grants` — What cartridges may reach, and the requests they make for more
* `dump` — Raw debug view of what the node holds, as JSON
* `exec` — Run a method or a cartridge's command, through the node's dispatch router
* `completions` — Generate shell completions

###### **Options:**

* `--node <DIR>` — The node's directory



## `day node`

Set up, run, stop and inspect the node

**Usage:** `day node <COMMAND>`

###### **Subcommands:**

* `init` — Set up a new node directory with a fresh identity
* `start` — Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
* `stop` — Stop the background server and return once it has exited
* `status` — Show the node's id, how it was reached, and what it runs (exit status 3 when no server is serving it)



## `day node init`

Set up a new node directory with a fresh identity

**Usage:** `day node init [OPTIONS] [DIR]`

###### **Arguments:**

* `<DIR>` — The directory to create the node in (default: the --node directory)

###### **Options:**

* `--relay` — Run the node as a relay: it accepts any peer and mirrors the drawers it is made a member of
* `--json` — Print JSON instead of text



## `day node start`

Serve the node on its socket in the foreground until SIGTERM or Ctrl-C

With `--bg` the node is served by a background server instead, and the command returns once it is serving; a server that is already running is left alone.

**Usage:** `day node start [OPTIONS]`

###### **Options:**

* `--bg` — Serve from a background server and return once it is serving
* `--idle-exit <DURATION>` — Stop by itself once the node has been idle this long, such as `10m` or `1h 30m`: no client connected and no work in progress (ADR 052). Without it the server runs until `day node stop`



## `day node stop`

Stop the background server and return once it has exited

**Usage:** `day node stop`



## `day node status`

Show the node's id, how it was reached, and what it runs (exit status 3 when no server is serving it)

**Usage:** `day node status [OPTIONS]`

###### **Options:**

* `--json` — Print JSON instead of text



## `day carts`

Installed cartridges

**Usage:** `day carts <COMMAND>`

**Command Alias:** `cart`

###### **Subcommands:**

* `dev-takeover` — Dev hook: run an installed dev build AS another installed cartridge, to test it against real data



## `day carts dev-takeover`

Dev hook: run an installed dev build AS another installed cartridge, to test it against real data

The dev build takes over the slot: it runs under the original's actor with its data, its drawer grants in full and the method grants it asks for that the original's own code asked for too. Nothing else does this: routing a method to another provider (`org.example.daybook.nodules.setMethodRoute`) leaves each provider its own data and grants. The node refuses unless its node.json sets `dev.allow_slot_takeover`, and the dev build must not be enabled on its own.

**Usage:** `day carts dev-takeover [OPTIONS] <SLOT>`

###### **Arguments:**

* `<SLOT>` — The cartridge whose slot is taken over, as `<did>/<name>`

###### **Options:**

* `--by <BY>` — The installed cartridge that runs as the slot
* `--clear` — End the takeover: the slot runs its own install again



## `day peers`

Manage the peers the node's server dials at start

**Usage:** `day peers <COMMAND>`

**Command Alias:** `peer`

###### **Subcommands:**

* `add` — Add a peer to the server's bootstrap list: it is dialed as soon as the server starts
* `ls` — List the bootstrap peers and, when a server is serving, who it is connected to



## `day peers add`

Add a peer to the server's bootstrap list: it is dialed as soon as the server starts

**Usage:** `day peers add <TICKET>`

###### **Arguments:**

* `<TICKET>` — The peer's endpoint ticket, as `day node status` prints it under `address`



## `day peers ls`

List the bootstrap peers and, when a server is serving, who it is connected to

**Usage:** `day peers ls [OPTIONS]`

###### **Options:**

* `--json` — Print JSON instead of text



## `day drawers`

Share drawers with other nodes

**Usage:** `day drawers <COMMAND>`

**Command Alias:** `drawer`

###### **Subcommands:**

* `ls` — List the drawers this node knows
* `remember` — Remember a drawer, by default mirroring it
* `forget` — Forget a drawer: drop it from the known-drawers list and stop mirroring it
* `add-member` — Add another node as a member of a drawer, with keyhive access
* `revoke-member` — Remove another node's membership of a drawer



## `day drawers ls`

List the drawers this node knows

One drawer per line: its id, and `mirror` when the node mirrors it. The list is the node's known-drawers list (`listKnownDrawers`), the drawers it was asked to remember.

**Usage:** `day drawers ls [OPTIONS]`

###### **Options:**

* `--json` — Print JSON instead of text



## `day drawers remember`

Remember a drawer, by default mirroring it

A node syncs only the drawers it mirrors: being made a member of a drawer on another node lets this node read it, but nothing is fetched until this node mirrors it (a relay is the exception: it mirrors every drawer it is made a member of). Remembering an already remembered drawer updates its mirror setting.

**Usage:** `day drawers remember [OPTIONS] <DRAWER>`

###### **Arguments:**

* `<DRAWER>` — The drawer's id

###### **Options:**

* `--no-mirror` — Keep it in the list without syncing its documents



## `day drawers forget`

Forget a drawer: drop it from the known-drawers list and stop mirroring it

The documents already held stay; this node just stops asking peers for the drawer.

**Usage:** `day drawers forget <DRAWER>`

###### **Arguments:**

* `<DRAWER>` — The drawer's id



## `day drawers add-member`

Add another node as a member of a drawer, with keyhive access

The peer must be known to this node, which it is once the two have connected (see `day peers add`): its keyhive identity arrives with the connection.

**Usage:** `day drawers add-member --access <ACCESS> <DRAWER> <PEER>`

###### **Arguments:**

* `<DRAWER>` — The drawer's id
* `<PEER>` — The peer's node id, as `day node status` prints it

###### **Options:**

* `--access <ACCESS>` — What the peer may do: relay stores and forwards ciphertext, read decrypts, write also changes documents, admin also manages members

  Possible values: `relay`, `read`, `write`, `admin`




## `day drawers revoke-member`

Remove another node's membership of a drawer

**Usage:** `day drawers revoke-member <DRAWER> <PEER>`

###### **Arguments:**

* `<DRAWER>` — The drawer's id
* `<PEER>` — The peer's node id, as `day node status` prints it



## `day grants`

What cartridges may reach, and the requests they make for more

**Usage:** `day grants <COMMAND>`

**Command Alias:** `grant`

###### **Subcommands:**

* `pending` — List the requests waiting for an answer
* `accept` — Say yes to a request
* `decline` — Say no to a request
* `list` — List what cartridges may do, all of them or one cartridge's
* `revoke` — Take back one answer



## `day grants pending`

List the requests waiting for an answer

One per line: its id, the cartridge, what it asks and when. Requests live in the running node's memory, so this needs the node running (`day node start`).

**Usage:** `day grants pending [OPTIONS]`

###### **Options:**

* `--json` — Print JSON instead of text



## `day grants accept`

Say yes to a request

Without `--once` the answer is kept, follows you to your other devices, and can be taken back with `day grants revoke`.

**Usage:** `day grants accept [OPTIONS] <REQUEST>`

###### **Arguments:**

* `<REQUEST>` — The request's id, as `day grants pending` prints it

###### **Options:**

* `--once` — Only this time: until the node is next started, and written nowhere
* `--drawer <DRAWER>` — For a request about a drawer that names none: the drawer to allow



## `day grants decline`

Say no to a request

The cartridge is not asked about the same thing again until the node is next started.

**Usage:** `day grants decline <REQUEST>`

###### **Arguments:**

* `<REQUEST>` — The request's id, as `day grants pending` prints it



## `day grants list`

List what cartridges may do, all of them or one cartridge's

**Usage:** `day grants list [OPTIONS] [CARTRIDGE]`

###### **Arguments:**

* `<CARTRIDGE>` — Only this cartridge's answers, as `<did>/<name>`

###### **Options:**

* `--json` — Print JSON instead of text



## `day grants revoke`

Take back one answer

Takes effect on the cartridge's next action.

**Usage:** `day grants revoke <CARTRIDGE> <GRANT>`

###### **Arguments:**

* `<CARTRIDGE>` — The cartridge, as `<did>/<name>`
* `<GRANT>` — The answer to take back, as `day grants list` prints it: `drawer:<id>`, `cartridge:<id>`, `net:<host[:port]>`, `method:<nsid>` or `authority:<prefix>`



## `day dump`

Raw debug view of what the node holds, as JSON

**Usage:** `day dump <COMMAND>`

###### **Subcommands:**

* `node` — Everything: node info, installed cartridges and their grants, and every known drawer with its documents and their datums
* `drawer` — One drawer: its descriptor, members, documents and their datums
* `doc` — One document's datums, or one datum by key
* `grants` — What cartridges may do: every grant of every cartridge
* `carts` — Installed cartridges and their nodules



## `day dump node`

Everything: node info, installed cartridges and their grants, and every known drawer with its documents and their datums

**Usage:** `day dump node`



## `day dump drawer`

One drawer: its descriptor, members, documents and their datums

**Usage:** `day dump drawer <DRAWER>`

###### **Arguments:**

* `<DRAWER>` — The drawer's id



## `day dump doc`

One document's datums, or one datum by key

**Usage:** `day dump doc <DOC> [KEY]`

###### **Arguments:**

* `<DOC>` — The document's id
* `<KEY>` — A datum key, such as `note/main`



## `day dump grants`

What cartridges may do: every grant of every cartridge

**Usage:** `day dump grants`



## `day dump carts`

Installed cartridges and their nodules

**Usage:** `day dump carts`



## `day exec`

Run a method or a cartridge's command, through the node's dispatch router

`day x <nsid> [input]` calls the method `nsid` and prints the JSON answer. A cartridge's commands are the subcommands of `day x` (`day x intxt install ..`) and have no other spelling; they are hard-coded until they are generated from the installed cartridges' command bindings. An NSID always contains dots, so it cannot be a command's name.

**Usage:** `day exec [OPTIONS] [NSID] [INPUT]
       exec <COMMAND>`

**Command Alias:** `x`

###### **Subcommands:**

* `intxt` — Record agent work and its git history, and package it (`day x intxt --help`)

###### **Arguments:**

* `<NSID>` — The method's NSID, such as org.example.daybook.nodules.listNodules
* `<INPUT>` — The method's input as JSON (default: {})

###### **Options:**

* `--get` — Call a query: send the input, an object of primitives, as the query string of a GET instead of the JSON body of a POST
* `--body <FILE>` — Stream this file as the raw request body of an operation whose input is bytes, such as org.example.daybook.blobs.put; the input is then its parameters, as a query string
* `--range <RANGE>` — An HTTP Range, such as bytes=0-99, for an operation that answers part of its bytes
* `--output <FILE>` — Write an answer that is bytes, such as org.example.daybook.blobs.read, to this file



## `day exec intxt`

Record agent work and its git history, and package it (`day x intxt --help`)

**Usage:** `day exec intxt <COMMAND>`

###### **Subcommands:**

* `install` — Admit intxt's cartridge to this node, host its handler, and enable it
* `add` — Add commits, a branch or a jj change to an intxt, with the sessions that produced them
* `edit` — Change an intxt through one document: its title, description and entries
* `new` — Create an intxt and remember it, so it has a short id
* `rm` — Remove entries and comments from an intxt, by the short ids `show` and `comment` print
* `list` — List every intxt remembered by this node, with counts of its commits, refs and sessions
* `drop` — Forget an intxt on this node
* `which` — Say which intxts on this node hold a commit
* `show` — Show what an intxt holds: its commits, jj changes, tracked refs and sessions
* `comment` — Comment on lines of a commit or an op the intxt holds
* `reply` — Reply to a comment, and resolve or reopen its thread
* `inbox` — List the threads waiting for you and where their comments are on the current revision
* `use` — Say which intxt this worktree is on, or put it on another
* `pr-block` — Print the block that names intxts in a PR description
* `status` — Say what is recorded in this worktree and which intxts include it
* `doctor` — Report how intxt is set up on this machine and in this worktree
* `github` — Work with GitHub pull requests
* `record` — Recording: the hooks that record sessions, the ACP proxy, git's side of rewrites



## `day exec intxt install`

Admit intxt's cartridge to this node, host its handler, and enable it

Stores the built handler component in the node's blob store, admits the cartridge that names it, grants the nodule the methods it calls, and enables it, which registers intxt's records and serves `createIntxt` and `addToIntxt`.

**Usage:** `day exec intxt install --handler <HANDLER>`

###### **Options:**

* `--handler <HANDLER>` — The handler component: `src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm`, built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release`



## `day exec intxt add`

Add commits, a branch or a jj change to an intxt, with the sessions that produced them

Run it in the worktree that holds the work. What is named is resolved now, to commits: a range is stored as its tip and merge base, never as the text you typed. The objects the commits need are carried as a git pack (thin against the base unless `--full`), in the node's blob store, and the sessions whose recorded edits produced content in those commits come with them. Adding something already in the intxt changes nothing and says so. Names the intxt by the short id `show` prints, or by its full drawer id, and with neither, adds to the intxt this worktree is on.

**Usage:** `day exec intxt add [OPTIONS] <--commits <RANGE>|--branch <NAME>|--change <CHANGE>> [INTXT]`

Examples:
  day x intxt add qx --branch feat/refs
  day x intxt add qx --commits main..feat/refs
  day x intxt add qx --change kxqoltvm

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id. Default: the one this worktree is on (`day x intxt use`)

###### **Options:**

* `--commits <RANGE>` — Commits: a range `main..feat/x` (its tip and its merge base with `main`), or one commit (against its first parent). Brings those commits and the files they touch
* `--branch <NAME>` — A local branch: its commits since its merge base with its upstream, or with `main` (`master`) when it has none, and the branch itself, tracked as this clone sees it, so commits it moves onto later come in as new revisions. Brings what `--commits` brings
* `--change <CHANGE>` — A jj change, by its change id, followed through its rewrites: every commit the change has had comes in. Needs `jj`
* `--full` — Pack every object, not only what the base does not have

   The default pack is thin: it assumes whoever receives the intxt already has the base commit. A full pack stands alone and is several times larger.



## `day exec intxt edit`

Change an intxt through one document: its title, description and entries

The document is JSON (`org.example.intxt.edit`, with a JSON Schema: `--schema`). With no flag, `edit` writes it to a temporary file, runs `$VISUAL`, `$EDITOR` or `vi` on it (as git does, so `EDITOR="code --wait"` works) and applies the difference when the editor exits: an entry that is gone is removed (as `rm` does), an `{"add": {...}}` entry is resolved in this worktree and added (as `add` does), and a changed title or description is written. If it cannot be applied the file reopens with an `errors` member until it applies, or is saved unchanged or emptied. Without a terminal this form refuses at once.

The non-interactive form never opens an editor. `--print` writes the document; `--apply` reads one from a file or `-` for stdin. An applied document may be partial: `base` (the heads it was printed at, required) and only the members to change. An omitted member is unchanged and `"description": null` clears the description. `entries` is the whole list, as printed: the existing entries it leaves out are removed. `entryChanges` is a delta that leaves the other entries alone: `{"remove": "<id>"}` and `{"add": {...}}`. A document has one of the two, never both. Entries are named by their full id (32 characters); any prefix that is unambiguous when the document is applied (every short id `show` prints) is accepted. Entries are added or removed, never edited in place. Applying prints each change and the commands that undo them, in the order to run them. `--dry-run` prints the plan and writes nothing.

`--json` prints the result as `{"changes": [{"op", "text"}], "undo": [...]}`, or on failure `{"errors": [{"path": "<JSON pointer into the document>", "message"}]}`.

Exit status: 0 applied (or nothing to change); 1 the document is wrong; 2 bad command line; 3 the intxt changed since the document was printed (stale `base`) and the document still has something to change: the current document is printed (with `--json`, as `"current"` beside the errors) to edit again. A stale document that asks for nothing the intxt does not already have (a retry of one that applied) is "nothing to change", exit 0.

**Usage:** `day exec intxt edit [OPTIONS] [INTXT]`

Example, for an agent or a script:
  day x intxt edit qx --print > qx.json    # the document, with its base heads
  # edit qx.json: remove an entry by deleting its line, or add one with
  #   { "add": { "branch": "feat/x" } }
  day x intxt edit qx --apply qx.json --dry-run
  day x intxt edit qx --apply qx.json      # prints the changes and their undo commands

A partial document works as well:
  echo '{"base": ["z.."], "title": "New title", "entryChanges": [{"remove": "zkq"}]}' | day x intxt edit qx --apply -

Interactively:
  day x intxt edit qx


###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id. Default: the one this worktree is on (`day x intxt use`), or with `--apply` the one the document names

###### **Options:**

* `--print` — Write the document to stdout
* `--apply <FILE>` — Apply the document in FILE (`-` for stdin)
* `--schema` — Write the document's JSON Schema to stdout
* `--dry-run` — Print what would change and write nothing
* `--json` — Print the result of `--apply` as JSON



## `day exec intxt new`

Create an intxt and remember it, so it has a short id

Prints the new intxt's short id, which names it in `add`, `show` and `rm`.

With `--with`, the new intxt is lent the named intxt's commits documents, for a stack or a follow-up: commits they have in common are stored once, and each holds the commits the other added.

**Usage:** `day exec intxt new [OPTIONS] <TITLE>`

Examples:
  day x intxt new "Datum refs move to dtxt"
  day x intxt new "Follow-up" --with qx

###### **Arguments:**

* `<TITLE>` — The intxt's title

###### **Options:**

* `--description <DESCRIPTION>` — The intxt's description, in Markdown
* `--with <WITH>` — A related intxt (its short id, or its full drawer id) whose commits documents the new intxt is lent



## `day exec intxt rm`

Remove entries and comments from an intxt, by the short ids `show` and `comment` print

Deletes the entries (commits, jj changes, tracked refs, sessions) and the packs only they carried, and comments (a thread's first comment takes its replies with it). The documents already shared with the intxt stay shared, and copies synced to others stay with them.

**Usage:** `day exec intxt rm <INTXT> <ITEMS>...`

Examples:
  day x intxt rm qx kq vx

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id
* `<ITEMS>` — The entries to remove, by short id



## `day exec intxt list`

List every intxt remembered by this node, with counts of its commits, refs and sessions

**Usage:** `day exec intxt list`

Examples:
  day x intxt list
  day x intxt list --node ~/.local/share/day/node



## `day exec intxt drop`

Forget an intxt on this node

The drawer and anything already shared stay; it only stops being listed here, and naming it by its full id brings it back.

**Usage:** `day exec intxt drop <INTXT>`

Examples:
  day x intxt drop qx
  day x intxt drop qy

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id



## `day exec intxt which`

Say which intxts on this node hold a commit

Names the commit by a full sha, a unique prefix or any name git resolves in this worktree. Prints one line per intxt that holds it: its short id, its title and its `link:`. Prints nothing when no intxt does.

**Usage:** `day exec intxt which <COMMIT>`

Examples:
  day x intxt which 4c1f9a0

###### **Arguments:**

* `<COMMIT>` — The commit, as this worktree's git resolves it



## `day exec intxt show`

Show what an intxt holds: its commits, jj changes, tracked refs and sessions

Each entry has a short id, the shortest prefix that is unique among the intxt's entries.

**Usage:** `day exec intxt show [INTXT]`

Examples:
  day x intxt show qx

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id. Default: the one this worktree is on (`day x intxt use`)



## `day exec intxt comment`

Comment on lines of a commit or an op the intxt holds

The target is `<commit>:<path>:<lines>` (a commit the intxt holds, `4c1f9a0` or any name git resolves) or `op<seq>:<path>:<lines>` (an uncommitted state of this worktree); lines are `a-b` or `a`, 1-based in the path's content at that commit or op. Run it in the worktree whose repository has the commit. The comment records the quoted text, so it can be found again after a rebase.

**Usage:** `day exec intxt comment [OPTIONS] --message <MESSAGE> <INTXT> <TARGET>`

Examples:
  day x intxt comment qx 4c1f9a0:src/lib.rs:120-124 -m "Why not reuse the fragment here?"
  day x intxt comment qx 4c1f9a0:src/lib.rs:12 --side old -m "This line was fine."

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id
* `<TARGET>` — What to comment on: `<commit>:<path>:<lines>` or `op<seq>:<path>:<lines>`

###### **Options:**

* `-m`, `--message <MESSAGE>` — The comment, in Markdown
* `--side <SIDE>` — Count the lines in the commit's parent, as the old side of its diff

  Default value: `new`

  Possible values:
  - `old`:
    The commit's parent
  - `new`:
    The commit itself

* `--motivation <MOTIVATION>` — Why: one of W3C Web Annotation's motivations

  Default value: `commenting`

  Possible values: `assessing`, `bookmarking`, `classifying`, `commenting`, `describing`, `editing`, `highlighting`, `identifying`, `linking`, `moderating`, `questioning`, `tagging`




## `day exec intxt reply`

Reply to a comment, and resolve or reopen its thread

Names the comment by the short id `comment` and `inbox` print, which is unique among one intxt's entries and comments: name that intxt with `--intxt` or run in the worktree that is on it. With `--resolve` or `--reopen` and no message, only the thread's state changes.

**Usage:** `day exec intxt reply [OPTIONS] <COMMENT>`

Examples:
  day x intxt reply kz -m "Done in 77b2" --resolve
  day x intxt reply kz --intxt qx --reopen

###### **Arguments:**

* `<COMMENT>` — The comment to answer, by the short id printed for it

###### **Options:**

* `--intxt <INTXT>` — The intxt the comment is in: its short id, or its full drawer id. Default: the one this worktree is on (`day x intxt use`)
* `-m`, `--message <MESSAGE>` — The reply, in Markdown
* `--resolve` — Mark the thread resolved
* `--reopen` — Mark the thread unresolved



## `day exec intxt inbox`

List the threads waiting for you and where their comments are on the current revision

A thread is listed when it is unresolved and you have not seen its latest comments, or someone else wrote last. Each comment is carried to the newest revision the intxt holds (same path only): its placed lines, `(context changed)` when the text is there but its surroundings changed, or `outdated`. Run it in the worktree whose repository has the commits. Listing marks the threads seen.

**Usage:** `day exec intxt inbox [INTXT]`

Examples:
  day x intxt inbox qx

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id. Default: the one this worktree is on (`day x intxt use`)



## `day exec intxt use`

Say which intxt this worktree is on, or put it on another

`add`, `show` and `inbox` act on that intxt when none is named, and `new` puts the worktree it runs in on the intxt it creates. Kept in the worktree's local state (`<git dir>/intxt/state.json`), never synced: each checkout is on its own intxt.

**Usage:** `day exec intxt use [OPTIONS] [INTXT]`

Examples:
  day x intxt use qx
  day x intxt use
  day x intxt use --none

###### **Arguments:**

* `<INTXT>` — The intxt: its short id, or its full drawer id

###### **Options:**

* `--none` — Put the worktree on no intxt



## `day exec intxt pr-block`

Print the block that names intxts in a PR description

Prints a collapsed `<details>` holding a JSON code block (`org.example.intxt.prLinks`) with each intxt's `dtxt:doc:` reference (hinting at this node and the peers it syncs with), title and tracked refs, to paste at the end of the description. Default: the intxt this worktree is on. Reading a PR with `day x intxt github pull` finds the intxts it names there.

**Usage:** `day exec intxt pr-block [INTXTS]...`

Examples:
  day x intxt pr-block
  day x intxt pr-block qx qy

###### **Arguments:**

* `<INTXTS>` — The intxts: short ids, or full drawer ids. Default: the one this worktree is on



## `day exec intxt status`

Say what is recorded in this worktree and which intxts include it

Run it in a worktree: it lists the ops and sessions capture recorded here, and the intxts that include this worktree's commits, its clone's tracked refs or its sessions.

**Usage:** `day exec intxt status`

Examples:
  day x intxt status



## `day exec intxt doctor`

Report how intxt is set up on this machine and in this worktree

Checks, in order: `day` and `intxt` on the PATH, the node and its server, intxt's cartridge on the node, the intxts the node remembers, the Claude Code hooks that record sessions, git's `post-rewrite` hook, and the intxt this worktree is on. Each prints one line, `ok`, `warn` or `fail`, with the command that fixes anything not ok. It changes nothing and does not start the node: with no server running, the checks that read the node are skipped (`day node start --bg` runs them). Exits 1 when a check fails.

**Usage:** `day exec intxt doctor [OPTIONS]`

Examples:
  intxt doctor
  intxt doctor --json

###### **Options:**

* `--json` — Print the checks as JSON: `[{check, status, detail, fix}]`



## `day exec intxt github`

Work with GitHub pull requests

**Usage:** `day exec intxt github <COMMAND>`

###### **Subcommands:**

* `pull` — Import a pull request onto the node, with your `gh` credentials
* `ls` — List the pull requests imported onto this node
* `show` — Show an imported pull request and the intxts named by its description



## `day exec intxt github pull`

Import a pull request onto the node, with your `gh` credentials

Fetches the pull request with `gh api` (so `gh` must be installed and logged in) and writes its title, description, state, branches, shas and update time into the node's GitHub document for the repository, which a micro can read. Pulling again overwrites it only when GitHub says the pull request changed. Prints the title and the intxts the description's intxt block names (`day x intxt pr-block` prints one); a malformed block is an error.

**Usage:** `day exec intxt github pull <TARGET>`

Examples:
  day x intxt github pull dman-os/townframe#140
  day x intxt github pull https://github.com/dman-os/townframe/pull/140

###### **Arguments:**

* `<TARGET>` — `<owner>/<repo>#<number>`, or the pull request's URL



## `day exec intxt github ls`

List the pull requests imported onto this node

**Usage:** `day exec intxt github ls`



## `day exec intxt github show`

Show an imported pull request and the intxts named by its description

**Usage:** `day exec intxt github show <TARGET>`

###### **Arguments:**

* `<TARGET>` — `<owner>/<repo>#<number>`, or the pull request's URL



## `day exec intxt record`

Recording: the hooks that record sessions, the ACP proxy, git's side of rewrites

Recording moves raw data into the node's capture drawer. Nothing in it calls for judgement.

**Usage:** `day exec intxt record [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `install` — Write a harness's hook configuration
* `hook` — Record one Claude Code hook invocation: the hook's JSON on stdin
* `acp` — Record an ACP session: run the agent command, passing everything through
* `git-rewrite` — Record the commits git's `post-rewrite` hook reports as replaced (stdin: `<old> <new>` per line) in this worktree's capture

###### **Options:**

* `--verbose` — Say on stderr what was recorded (`hook`, `git-rewrite`)



## `day exec intxt record install`

Write a harness's hook configuration

**Usage:** `day exec intxt record install <COMMAND>`

###### **Subcommands:**

* `claude-code` — Write the Claude Code hooks into `./.claude/settings.json`



## `day exec intxt record install claude-code`

Write the Claude Code hooks into `./.claude/settings.json`

Adds a hook running `intxt record hook` to every event that is recorded, merging into the file: other keys and hooks stay, running it twice changes nothing, and an older `day x intxt capture` entry becomes the new command. Malformed JSON is an error and the file is left alone.

**Usage:** `day exec intxt record install claude-code [OPTIONS]`

###### **Options:**

* `--user` — Write `~/.claude/settings.json` instead of the project's
* `--dry-run` — Print the resulting JSON and write nothing



## `day exec intxt record hook`

Record one Claude Code hook invocation: the hook's JSON on stdin

The command of the hooks `install claude-code` writes. Observes the session's transcript and worktrees, puts changed binary files in the node's blob store, and records everything in the node's capture drawer. It prints nothing unless it has something to report or `--verbose` is given, and then only to stderr; it never exits with status 2, which Claude Code feeds back to the model.

**Usage:** `day exec intxt record hook`



## `day exec intxt record acp`

Record an ACP session: run the agent command, passing everything through

An ACP client (an editor) launches this in place of the agent, e.g. `intxt record acp -- claude-code-acp`. It spawns the agent, forwards the client's stdin to it and its stdout to the client unchanged, and records the session's messages, tool calls and plan, and the edits each prompt turn made in the session's working directory, in the node's capture drawer. Needs the node's server; without one the session runs unrecorded. A recording that fails says why on stderr and stops, and the session goes on. Exits with the agent's status.

**Usage:** `day exec intxt record acp [OPTIONS] <AGENT>...`

###### **Arguments:**

* `<AGENT>` — The agent's command and arguments

###### **Options:**

* `--thinking` — Record the agent's thinking (`agent_thought_chunk`) as well



## `day exec intxt record git-rewrite`

Record the commits git's `post-rewrite` hook reports as replaced (stdin: `<old> <new>` per line) in this worktree's capture

What the `post-rewrite` hook capture installs in a clone runs; git passes `amend` or `rebase`. A failure is one line on stderr and status 1; git ignores it, since `post-rewrite` cannot block.

**Usage:** `day exec intxt record git-rewrite <KIND>`

###### **Arguments:**

* `<KIND>` — What was rewritten, git's argument to the hook

  Possible values: `amend`, `rebase`




## `day completions`

Generate shell completions

**Usage:** `day completions <SHELL>`

###### **Arguments:**

* `<SHELL>`

  Possible values: `bash`, `elvish`, `fish`, `powershell`, `zsh`




