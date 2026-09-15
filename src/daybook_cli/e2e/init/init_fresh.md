# `daybook_cli init` on a fresh directory

Bootstraps a daybook repo at the current directory. Success is silent on the
merged stream (the CLI reports via tracing logs, suppressed suite-wide by
`RUST_LOG=error`).

```console
$ daybook_cli init
```
