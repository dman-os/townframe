# `exec` on an uninitialized repo

The dynamic CLI (which registers the plug commands under `exec`) terminates
early when the repo is not initialized: a tracing ERROR line on stderr, then
the deferred static clap error for the unknown `exec` subcommand (the static
command tree does not contain `exec`). Exit code 2.

The ERROR line carries a volatile timestamp and resolved repo path; the clap
error block is clap boilerplate.

<!-- NOTE (resolved by snapshot regeneration): exact trailing clap error block (Usage/suggestion lines)
after "error: unrecognized subcommand 'exec'" -->

```console
$ daybook_cli exec test-label
? 2
...
error: unrecognized subcommand 'exec'

Usage: daybook_cli <COMMAND>

For more information, try '--help'.

```
