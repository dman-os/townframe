# `day daemon --help`

```console
$ day daemon --help
Run, start, stop and inspect the node's background daemon

Usage: day daemon [OPTIONS] <COMMAND>

Commands:
  run     Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
  start   Start a background daemon and return once it is serving
  stop    Stop the background daemon and return once it has exited
  status  Report whether a daemon is running (exit status 3 when it is not)
  help    Print this message or the help of the given subcommand(s)

Options:
      --node <DIR>  The node's directory [env: DAY_NODE=[..]/node]
  -h, --help        Print help

```
