# `exec` with no plug subcommand

Bare `exec` prints the exec long help (the dynamically registered plug
commands) and exits with FAILURE. Exit code 1.

The exact help layout comes from clap's long-help renderer; the parent
regenerates the precise text, keeping the volatility notes: subcommand list
below is stable (plug command names + one-line descriptions), the rest is
clap boilerplate.

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ daybook_cli plugs import ./plug-oci
imported @daybook/test v0.0.1 (doc: [..])

$ daybook_cli exec
? 1
Usage: exec [COMMAND]
...

```
