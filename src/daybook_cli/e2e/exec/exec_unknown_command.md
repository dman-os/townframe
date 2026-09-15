# `exec` with an unknown plug command

The `exec` (alias `x`) subcommand tree is built dynamically from plug
manifests at startup. A command that no plug declared is a clap
`InvalidSubcommand`: the parse fails before any repo work, with exit code 2.

```console
$ daybook_cli init
$ daybook_cli exec not-a-real-command
? 2
error: unexpected argument 'not-a-real-command' found

Usage: daybook_cli exec

For more information, try '--help'.

```
