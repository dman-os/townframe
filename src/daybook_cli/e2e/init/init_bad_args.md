# `daybook_cli init` rejects unknown arguments

Unknown flags must fail fast with clap's usage error (exit code 2).

```console
$ daybook_cli init --bogus
? 2
error: unexpected argument '--bogus' found

Usage: daybook_cli init

For more information, try '--help'.

```
