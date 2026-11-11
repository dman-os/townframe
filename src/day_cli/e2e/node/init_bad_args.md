# `day node init` rejects unknown arguments

Unknown flags must fail fast with clap's usage error (exit code 2).

```console
$ day node init --bogus
? 2
error: unexpected argument '--bogus' found

  tip: to pass '--bogus' as a value, use '-- --bogus'

Usage: day node init [OPTIONS] [DIR]

For more information, try '--help'.

```
