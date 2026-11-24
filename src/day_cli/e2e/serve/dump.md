# `dump` prints the raw data the node holds

`day dump` is a debug view: every subcommand reads through existing node methods as the operator
and prints pretty JSON. A drawer is itself a document, so `dump doc <drawer>` shows its
descriptor datum, and `dump drawer` shows its view, its roster and the datums of the documents in
it. `dump node` lists the node's own drawers (present, originated and the config drawers, each
with its role) next to the known ones.

```console
$ day node init
initialized node [..] at [..]/node

$ day node start --bg
node serving in the background (pid [..]) on [..]/run/day/[..].sock

$ sh -c 'cap=$("$DAY" exec org.example.daybook.drawers.mintDrawerCapability "{\"scope\":{\"drawers\":{},\"create\":true},\"rights\":[\"use\"]}" | sed -n "s/.*\"capabilityId\": \"\(.*\)\".*/\1/p"); "$DAY" exec org.example.daybook.drawers.createDrawer "{\"capability\":\"$cap\",\"command\":\"c1\",\"layout\":\"singleGroup\"}" | sed -n "s/.*\"drawerId\": \"\(.*\)\".*/\1/p" > "$DAY_NODE/drawer-id"; "$DAY" drawers remember "$(cat "$DAY_NODE/drawer-id")" >/dev/null'

$ sh -c 'drawer=$(cat "$DAY_NODE/drawer-id"); "$DAY" dump drawer "$drawer" | grep -c "^  \"drawerId\": \"$drawer\""'
1

$ sh -c 'drawer=$(cat "$DAY_NODE/drawer-id"); "$DAY" dump drawer "$drawer" | grep -o "\"status\": \"accepted\"" | head -1'
"status": "accepted"

$ sh -c 'drawer=$(cat "$DAY_NODE/drawer-id"); "$DAY" dump doc "$drawer" | grep -o "\"status\": \"accepted\"" | head -1'
"status": "accepted"

$ sh -c '"$DAY" dump node | grep -c "^      \"mirror\": true"'
1

$ sh -c '"$DAY" dump node | grep -o "^ *\"\(present\|originated\|known drawers\|node config\|cartridge registry\|cartridge config\)\"$" | tr -d " " | sort'
"cartridgeconfig"
"cartridgeregistry"
"knowndrawers"
"nodeconfig"
"originated"
"present"

$ sh -c '"$DAY" dump carts | grep -c "^ *\"cartridgeId\": \"did:key:[^\"]*/core\",$"'
2

$ day dump grants
{
  "grants": [
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.authority.attenuateCapability",
      "thisBoot": true,
      "value": {}
    },
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.authority.openActor",
      "thisBoot": true,
      "value": {}
    },
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.authority.retractCapability",
      "thisBoot": true,
      "value": {}
    },
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.cartridges.awaitCartridgeGrants",
      "thisBoot": true,
      "value": {
        "privileged": true
      }
    },
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.cartridges.listCartridgeGrants",
      "thisBoot": true,
      "value": {
        "privileged": true
      }
    },
    {
      "cartridgeId": "did:key:[..]/core",
      "grant": "method:org.example.daybook.cartridges.listCartridgeInstalls",
      "thisBoot": true,
      "value": {
        "privileged": true
      }
    }
  ]
}

$ day dump doc nonsense
? 1
Error: `nonsense` is not a document or drawer id[..]

$ day node stop
server stopped (pid [..])

```
