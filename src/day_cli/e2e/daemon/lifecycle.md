# The daemon lifecycle: start, status, call over the socket, stop

`daemon start` returns once the daemon prints its readiness line. While it runs, `status`
and `call` reach the node over its socket instead of booting their own. `daemon stop`
returns once the process has exited, and the socket is gone.

```console
$ day init
initialized node [..] at [..]/node

$ day daemon status
? 3
stopped

$ day daemon start
daemon started (pid [..]) listening on [..]/node/run/node.sock

$ day daemon start
daemon already running on [..]/node/run/node.sock

$ day daemon status
running (process [..]) on [..]/node/run/node.sock

$ day status
node      [..]
path      [..]/node
mode      daemon
socket    [..]/node/run/node.sock (live)
nodules   0 enabled of 0
services  not exposed by the node
peers     not exposed by the node

$ day call org.example.daybook.nodules.listNodules
[]

$ day daemon stop
daemon stopped (pid [..])

$ day daemon status
? 3
stopped

$ day status
node      [..]
path      [..]/node
mode      in-process
socket    [..]/node/run/node.sock (absent)
nodules   0 enabled of 0
services  not exposed by the node
peers     not exposed by the node

```
