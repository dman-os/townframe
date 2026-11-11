# `day node init` on an existing node is idempotent

A second `init` reports the node that is there and changes nothing, exit status 0, so a script
can run it unconditionally. The file is compared byte for byte, so the identity is the
same before and after.

```console
$ day node init
initialized node [..] at [..]/node

$ sh -c 'cp "$DAY_NODE/node.json" before.json'

$ day node init
node [..] already initialized at [..]/node

$ sh -c 'cmp before.json "$DAY_NODE/node.json" && echo node.json unchanged'
node.json unchanged

```
