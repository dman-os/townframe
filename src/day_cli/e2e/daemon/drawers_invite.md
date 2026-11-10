# `drawers invite` names what is wrong

A drawer this node does not know is refused by the node. A peer it has not met is refused with
the commands that fix it: its keyhive identity arrives with a live connection. Arguments that
are not ids are refused before the node is asked.

```console
$ day init
initialized node [..] at [..]/node

$ day daemon start
daemon started (pid [..]) listening on [..]/run/day/[..].sock

$ sh -c '"$DAY" drawers invite z11111111111111111111111111111111 5866666666666666666666666666666666666666666666666666666666666666 --access read 2>&1 | grep -o "the node refused the invite: UnknownDrawer" | head -1'
the node refused the invite: UnknownDrawer

$ sh -c 'cap=$("$DAY" exec org.example.daybook.drawers.mintDrawerCapability "{\"scope\":{\"drawers\":{},\"create\":true},\"rights\":[\"use\"]}" | sed -n "s/.*\"capabilityId\": \"\(.*\)\".*/\1/p"); drawer=$("$DAY" exec org.example.daybook.drawers.createDrawer "{\"capability\":\"$cap\",\"command\":\"c1\",\"layout\":\"singleGroup\"}" | sed -n "s/.*\"drawerId\": \"\(.*\)\".*/\1/p"); "$DAY" drawers invite "$drawer" 5866666666666666666666666666666666666666666666666666666666666666 --access relay 2>&1 | grep -o "has not met [0-9a-f]*: connect to it first.*"'
has not met 5866666666666666666666666666666666666666666666666666666666666666: connect to it first (`day peers add <ticket>` and a daemon restart), then retry

$ day drawers invite nonsense 5866666666666666666666666666666666666666666666666666666666666666 --access read
? 1
Error:[..]
   0: `nonsense` is not a drawer id[..]
...

$ day drawers invite z11111111111111111111111111111111 nonsense --access read
? 1
Error:[..]
   0: `nonsense` is not a node id[..]
...

$ day daemon stop
daemon stopped (pid [..])

```
