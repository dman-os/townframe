# `touch` then `dump`: the new doc lands in the drawer

The drawer doc is the registry of documents; a `touch` adds an entry to it,
so a `dump` after should reflect the new doc. Content is build-random
(doc id, heads), so the body is elided; the assertion is that the
touch-then-dump sequence succeeds (exit 0) — i.e. writing a doc does not
corrupt the drawable automerge state.

```console
$ daybook_cli init
$ daybook_cli touch
[..]

$ daybook_cli dump
...
```
