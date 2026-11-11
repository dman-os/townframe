# The daemon lifecycle: start, status, exec over the socket, stop

`node start` returns once the daemon prints its readiness line. While it runs, `node status`
and `exec` reach the node over its socket instead of booting their own. `node stop`
returns once the process has exited, and the socket is gone.

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

$ day node start
daemon started (pid [..]) listening on [..]/run/day/[..].sock

$ day node start
daemon already running on [..]/run/day/[..].sock

$ day node status
node      [..]
path      [..]/node
mode      daemon
socket    [..]/run/day/[..].sock (live)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (the daemon's node.json has no peers section)

$ day exec --get org.example.daybook.nodules.listNodules
{
  "nodules": []
}

$ day node stop
daemon stopped (pid [..])

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
