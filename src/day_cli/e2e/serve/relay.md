# A relay node

`day node init --relay` records the relay flag and the network attachment a relay is reached over.
The server runs the relay policy, and `node status` says so. The test binds the loopback interface
only, so nothing leaves the machine.

```console
$ day node init --relay
initialized node [..] at [..]/node

$ sh -c 'sed -i "s/\"loopbackOnly\": false/\"loopbackOnly\": true/" "$DAY_NODE/node.json"'

$ day node start --bg
node serving in the background (pid [..]) on [..]/run/day/[..].sock

$ day node status
node      [..]
path      [..]/node
mode      serving
socket    [..]/run/day/[..].sock (live)
nodules   1 enabled of 1
  did:key:[..]/core  enabled  ready
relay     yes
services  not exposed by the node
peers     0 connected
  address [..]

$ day node stop
server stopped (pid [..])

```
