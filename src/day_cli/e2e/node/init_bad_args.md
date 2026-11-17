# `day init` rejects unknown arguments

Unknown flags must fail fast with clap's usage error (exit code 2).

```console
$ day init --bogus
? 2
error: unexpected argument '--bogus' found

  tip: to pass '--bogus' as a value, use '-- --bogus'

Usage: day init [OPTIONS] [DIR]

For more information, try '--help'.

```
