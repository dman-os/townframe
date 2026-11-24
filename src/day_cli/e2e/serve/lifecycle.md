# The server lifecycle: start, status, exec over the socket, stop

`node start --bg` returns once the server prints its readiness line. While it runs, `node status`
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
peers     none attached (only a server attaches peers)

$ day node start --bg
node serving in the background (pid [..]) on [..]/run/day/[..].sock

$ day node start --bg
a server is already running on [..]/run/day/[..].sock

$ day node status
node      [..]
path      [..]/node
mode      serving
socket    [..]/run/day/[..].sock (live)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (the server's node.json has no peers section)

$ day exec --get org.example.daybook.cartridges.listCartridgeGrantRequests
{
  "requests": []
}

$ day node stop
server stopped (pid [..])

$ day node status
? 3
node      [..]
path      [..]/node
mode      in-process
socket    [..]/run/day/[..].sock (absent)
nodules   0 enabled of 0
services  not exposed by the node
peers     none attached (only a server attaches peers)

```
