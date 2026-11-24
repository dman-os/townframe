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
* `exec` — Execute an operation through the node's dispatch router and print the JSON answer
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

* `node` — Everything: node info, installed cartridges and their grants, and every drawer the node holds, uses or knows (with its role) with its documents and their datums
* `drawer` — One drawer: its descriptor, members, documents and their datums
* `doc` — One document's datums, or one datum by key
* `grants` — What cartridges may do: every grant of every cartridge
* `carts` — Installed cartridges and their nodules



## `day dump node`

Everything: node info, installed cartridges and their grants, and every drawer the node holds, uses or knows (with its role) with its documents and their datums

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

Execute an operation through the node's dispatch router and print the JSON answer

**Usage:** `day exec [OPTIONS] <NSID> [INPUT]`

**Command Alias:** `x`

###### **Arguments:**

* `<NSID>` — The operation's NSID, such as org.example.daybook.nodules.listNodules
* `<INPUT>` — The operation's input as JSON (default: {})

###### **Options:**

* `--get` — Call a query: send the input, an object of primitives, as the query string of a GET instead of the JSON body of a POST
* `--body <FILE>` — Stream this file as the raw request body of an operation whose input is bytes, such as org.example.daybook.blobs.put; the input is then its parameters, as a query string
* `--range <RANGE>` — An HTTP Range, such as bytes=0-99, for an operation that answers part of its bytes
* `--output <FILE>` — Write an answer that is bytes, such as org.example.daybook.blobs.read, to this file



## `day completions`

Generate shell completions

**Usage:** `day completions <SHELL>`

###### **Arguments:**

* `<SHELL>`

  Possible values: `bash`, `elvish`, `fish`, `powershell`, `zsh`




