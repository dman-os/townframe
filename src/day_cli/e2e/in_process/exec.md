# `exec` with no daemon

The raw dispatch porcelain: the answer is the operation's JSON output. `x` is the short
spelling of `exec`.

```console
$ day init
initialized node [..] at [..]/node

$ day exec org.example.daybook.nodules.listNodules
{
  "nodules": []
}

$ day x org.example.daybook.nodules.listNodules
{
  "nodules": []
}

$ day exec --help
Execute an operation through the node's dispatch router and print the JSON answer

Usage: day exec [OPTIONS] <NSID> [INPUT]

Arguments:
  <NSID>   The operation's NSID, such as org.example.daybook.nodules.listNodules
  [INPUT]  The operation's input as JSON (default: {})

Options:
      --node <DIR>  The node's directory [env: DAY_NODE=[..]/node]
  -h, --help        Print help

```
