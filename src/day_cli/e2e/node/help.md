# `day node --help`

```console
$ day node --help
Set up, run, stop and inspect the node

Usage: day node [OPTIONS] <COMMAND>

Commands:
  init    Set up a new node directory with a fresh identity
  run     Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
  start   Start a background daemon and return once it is serving
  stop    Stop the background daemon and return once it has exited
  status  Show the node's id, how it was reached, and what it runs (exit status 3 when no daemon is serving it)
  help    Print this message or the help of the given subcommand(s)

Options:
      --node <DIR>  The node's directory [env: DAY_NODE=[..]/node]
  -h, --help        Print help

```
