# `day init` refuses to touch an existing node

A second `init` fails and changes nothing, so a node's identity cannot be replaced by
running it twice. The first fence's node id is not compared; the point is that the failure
leaves the directory as it was.

```console
$ day init
initialized node [..] at [..]/node

$ day init
? 1
[..]a node is already initialized here[..]

```
