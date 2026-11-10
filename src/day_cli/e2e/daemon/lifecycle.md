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
daemon started (pid [..]) listening on [..]/run/day/[..].sock

$ day daemon start
daemon already running on [..]/run/day/[..].sock

$ day daemon status
running (process [..]) on [..]/run/day/[..].sock

$ day status
node      [..]
path      [..]/node
mode      daemon
socket    [..]/run/day/[..].sock (live)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (the daemon's node.json has no peers section)

$ day call org.example.daybook.nodules.listNodules
{
  "nodules": []
}

$ day daemon stop
daemon stopped (pid [..])

$ day daemon status
? 3
stopped

$ day status
node      [..]
path      [..]/node
mode      in-process
socket    [..]/run/day/[..].sock (absent)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (only a daemon attaches peers)

```
