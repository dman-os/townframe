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
Run a method or a cartridge's command, through the node's dispatch router

`day x <nsid> [input]` calls the method `nsid` and prints the JSON answer. A cartridge's commands are the subcommands of `day x` (`day x intxt install ..`) and have no other spelling; they are hard-coded until they are generated from the installed cartridges' command bindings. An NSID always contains dots, so it cannot be a command's name.

Usage: day exec [OPTIONS] [NSID] [INPUT]
       day exec <COMMAND>

Commands:
  intxt  Record agent work and its git history, and package it (`day x intxt --help`)
  help   Print this message or the help of the given subcommand(s)

Arguments:
  [NSID]
          The method's NSID, such as org.example.daybook.nodules.listNodules

  [INPUT]
          The method's input as JSON (default: {})

Options:
      --get
          Call a query: send the input, an object of primitives, as the query string of a GET instead of the JSON body of a POST

      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

  -h, --help
          Print help (see a summary with '-h')

```
