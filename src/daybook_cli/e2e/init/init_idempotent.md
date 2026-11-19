# `daybook_cli init` is idempotent

A second `init` on an already-initialized repo must not fail (it warns and
exits 0). The first `init` creates the repo, so the second runs against a
live one.

```console
$ daybook_cli init
$ daybook_cli init
```
