# `status` with no daemon boots the node in process

The node id, its directory, how it was reached, and what the node can say about itself.
A fresh node has no nodules and, reached in process, no peers service; the services are not
exposed by any operation.

```console
$ day node init
initialized node [..] at [..]/node

$ day node status
? 3
node      [..]
path      [..]/node
mode      in-process
socket    [..]/run/day/[..].sock (absent)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (only a daemon attaches peers)

```
