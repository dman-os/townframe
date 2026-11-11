# A relay node

`day init --relay` records the relay flag and the network attachment a relay is reached over.
The daemon runs the relay policy, and `status` says so. The test binds the loopback interface
only, so nothing leaves the machine.

```console
$ day init --relay
initialized node [..] at [..]/node

$ sh -c 'sed -i "s/\"loopbackOnly\": false/\"loopbackOnly\": true/" "$DAY_NODE/node.json"'

$ day daemon start
daemon started (pid [..]) listening on [..]/run/day/[..].sock

$ day status
node      [..]
path      [..]/node
mode      daemon
socket    [..]/run/day/[..].sock (live)
nodules   0 enabled of 0
relay     yes
services  not exposed by the node
peers     0 connected
  address [..]

$ day daemon stop
daemon stopped (pid [..])

```
