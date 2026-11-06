# `status` with no daemon boots the node in process

The node id, its directory, how it was reached, and what the node can say about itself.
A fresh node has no nodules; services and peers are not exposed by any operation.

```console
$ day init
initialized node [..] at [..]/node

$ day status
node      [..]
path      [..]/node
mode      in-process
socket    [..]/node/run/node.sock (absent)
nodules   0 enabled of 0
services  not exposed by the node
peers     not exposed by the node

```
