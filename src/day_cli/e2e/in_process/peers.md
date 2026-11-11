# `peers ls` and `peers add` without a daemon

`peers add` validates the ticket before it writes anything; a node reached in process has no
peers service, which `peers ls` says.

```console
$ day node init
initialized node [..] at [..]/node

$ day peers add nonsense
? 1
Error:[..]
   0: `nonsense` is not an endpoint ticket
...

$ day peers ls
bootstrap 0
connected none attached (only a daemon attaches peers)

```
