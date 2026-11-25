# `peers ls` and `peers add` without a server

`peers add` validates the ticket before it writes anything; a node reached in process has no
peers service, which `peers ls` says.

```console
$ day node init
initialized node [..] at [..]/node

$ day peers add nonsense
? 1
Error: `nonsense` is not an endpoint ticket[..]
...

$ day peers ls
bootstrap 0
connected none attached (only a server attaches peers)

```
