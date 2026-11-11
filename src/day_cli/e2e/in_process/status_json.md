# `status --json`

```console
$ day node init
initialized node [..] at [..]/node

$ day node status --json
? 3
{
  "mode": "inProcess",
  "nodeId": "[..]",
  "nodules": [],
  "path": "[..]/node",
  "peers": null,
  "relay": false,
  "services": null,
  "socket": {
    "path": "[..]/run/day/[..].sock",
    "state": "absent"
  }
}

```
