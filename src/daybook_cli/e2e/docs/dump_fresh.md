# `dump` on a fresh repo

`dump` serializes the repo's drawer and app automerge documents as pretty
JSON. The content is build-random (doc ids, change heads, byte arrays folded
to length markers), so the body is elided; the assertion is that `dump` runs
and exits 0 on a freshly initialized repo.

```console
$ daybook_cli init
$ daybook_cli dump
...
```
