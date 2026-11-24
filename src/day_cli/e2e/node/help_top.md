# `day --help` documents the exit codes

The long help carries the exit code table every command follows; `-h` is the summary.

```console
$ day --help
Usage: day [OPTIONS] <COMMAND>

Commands:
  node         Set up, run, stop and inspect the node
  carts        Installed cartridges [alias: cart]
  peers        Manage the peers the node's server dials at start [alias: peer]
  drawers      Share drawers with other nodes [alias: drawer]
  grants       What cartridges may reach, and the requests they make for more [alias: grant]
  dump         Raw debug view of what the node holds, as JSON
  exec         Run a method or a cartridge's command, through the node's dispatch router [alias: x]
  completions  Generate shell completions
  help         Print this message or the help of the given subcommand(s)

Options:
      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version

Exit codes:
- 0: success
- 1: failure, the command could not do what was asked (the error is on stderr)
- 2: usage error, the arguments were not understood
- 3: no server running (`day node status`, which still reports what it found)

Output goes to stdout and diagnostics to stderr. Only `day node start` logs as it serves;
the other commands show warnings, and RUST_LOG overrides the level. Colour is used on a
terminal unless NO_COLOR is set.

```
