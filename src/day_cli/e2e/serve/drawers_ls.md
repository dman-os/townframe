# `drawers remember`, `ls` and `forget` keep the known drawers

The node keeps a list of drawers it was asked to remember, and syncs the ones it mirrors: a
read grant on another node makes a drawer readable here, but nothing is fetched until this node
mirrors it. `day drawers ls` finds the drawer that holds the list through `getNodeInfo`, mints a
handle on just that drawer, and prints one drawer per line, so a user can find drawer ids without
being handed a capability first. `remember` and `forget` go through the same handle.

```console
$ day node init
initialized node [..] at [..]/node

$ day node start --bg
node serving in the background (pid [..]) on [..]/run/day/[..].sock

$ day drawers ls

$ sh -c 'cap=$("$DAY" exec org.example.daybook.drawers.mintDrawerCapability "{\"scope\":{\"drawers\":{},\"create\":true},\"rights\":[\"use\"]}" | sed -n "s/.*\"capabilityId\": \"\(.*\)\".*/\1/p"); "$DAY" exec org.example.daybook.drawers.createDrawer "{\"capability\":\"$cap\",\"command\":\"c1\",\"layout\":\"singleGroup\"}" | sed -n "s/.*\"drawerId\": \"\(.*\)\".*/\1/p" > "$DAY_NODE/drawer-id"; drawer=$(cat "$DAY_NODE/drawer-id"); [ "$("$DAY" drawers remember "$drawer")" = "remembered $drawer, mirrored" ] && [ "$("$DAY" drawers ls)" = "$drawer  mirror" ] && echo mirrored'
mirrored

$ sh -c '"$DAY" drawers ls --json | grep -c "\"mirror\": true"'
1

$ sh -c 'drawer=$(cat "$DAY_NODE/drawer-id"); "$DAY" drawers remember --no-mirror "$drawer" >/dev/null; [ "$("$DAY" drawers ls)" = "$drawer" ] && echo kept'
kept

$ sh -c 'drawer=$(cat "$DAY_NODE/drawer-id"); [ "$("$DAY" drawers forget "$drawer")" = "forgot $drawer" ] && [ -z "$("$DAY" drawers ls)" ] && echo forgotten'
forgotten

$ day drawers remember nonsense
? 1
Error: `nonsense` is not a drawer id[..]
...

$ day node stop
server stopped (pid [..])

```
