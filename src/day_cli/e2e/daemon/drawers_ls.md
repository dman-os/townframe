# `drawers ls` lists the known drawers

The node keeps a list of drawers it was asked to remember. `day drawers ls` finds the drawer
that holds the list through `getNodeInfo`, mints a handle on just that drawer, and prints one
drawer per line, so a user can find drawer ids without being handed a capability first.

```console
$ day init
initialized node [..] at [..]/node

$ day daemon start
daemon started (pid [..]) listening on [..]/run/day/[..].sock

$ day drawers ls

$ sh -c 'kd=$("$DAY" exec org.example.daybook.node.getNodeInfo "{}" | sed -n "s/.*\"knownDrawersId\": \"\(.*\)\".*/\1/p"); known=$("$DAY" exec org.example.daybook.drawers.mintDrawerCapability "{\"scope\":{\"drawers\":{\"$kd\":\"Admin\"},\"create\":false},\"rights\":[\"use\"]}" | sed -n "s/.*\"capabilityId\": \"\(.*\)\".*/\1/p"); cap=$("$DAY" exec org.example.daybook.drawers.mintDrawerCapability "{\"scope\":{\"drawers\":{},\"create\":true},\"rights\":[\"use\"]}" | sed -n "s/.*\"capabilityId\": \"\(.*\)\".*/\1/p"); drawer=$("$DAY" exec org.example.daybook.drawers.createDrawer "{\"capability\":\"$cap\",\"command\":\"c1\",\"layout\":\"singleGroup\"}" | sed -n "s/.*\"drawerId\": \"\(.*\)\".*/\1/p"); "$DAY" exec org.example.daybook.drawers.rememberDrawer "{\"capability\":\"$known\",\"drawerId\":\"$drawer\",\"mirror\":true}" >/dev/null; [ "$("$DAY" drawers ls)" = "$drawer  mirror" ] && echo listed'
listed

$ sh -c '"$DAY" drawers ls --json | grep -c "\"mirror\": true"'
1

$ day daemon stop
daemon stopped (pid [..])

```
