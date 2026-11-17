# `completions bash` works without a node

`completions` is deferred from the static CLI to the dynamic one, which needs no node.

```console
$ day completions bash
_day() {
...
```
