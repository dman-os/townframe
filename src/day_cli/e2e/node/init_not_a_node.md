# `day init` refuses a directory that holds something that is not a node

A `node.json` that does not parse is not a node. `init` fails (exit 1) and leaves the file as
it found it: it neither overwrites it nor guesses.

```console
$ sh -c 'mkdir -p "$DAY_NODE" && echo nope > "$DAY_NODE/node.json"'

$ day init
? 1
Error:[..]
   0: [..]/node exists but is not a valid node; refusing to touch it
...

$ sh -c 'cat "$DAY_NODE/node.json"'
nope

```
