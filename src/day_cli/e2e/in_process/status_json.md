# `status --json`

A fresh node's one nodule is its own core cartridge's (ADR 053); its actor, heads and revision
differ per node.

```console
$ day node init
initialized node [..] at [..]/node

$ day node status --json
? 3
{
  "mode": "inProcess",
  "nodeId": "[..]",
  "nodules": [
    {
...
      "cartridgeId": "did:key:[..]/core",
      "enabled": true,
...
      "readiness": "ready",
...
      "version": "0.1.0"
    }
  ],
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
