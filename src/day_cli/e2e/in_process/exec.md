# `exec` with no daemon

The raw dispatch porcelain: the answer is the operation's JSON output. `x` is the short
spelling of `exec`.

```console
$ day node init
initialized node [..] at [..]/node

$ day exec --get org.example.daybook.nodules.listNodules
{
  "nodules": []
}

$ day x --get org.example.daybook.nodules.listNodules
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
      --get            Call a query: send the input, an object of primitives, as the query string of a GET instead of the JSON body of a POST
      --node <DIR>     The node's directory [env: DAY_NODE=[..]/node]
      --body <FILE>    Stream this file as the raw request body of an operation whose input is bytes, such as org.example.daybook.blobs.put; the input is then its parameters, as a query string
      --range <RANGE>  An HTTP Range, such as bytes=0-99, for an operation that answers part of its bytes
      --output <FILE>  Write an answer that is bytes, such as org.example.daybook.blobs.read, to this file
  -h, --help           Print help

```
