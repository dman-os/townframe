# `status --json`

A fresh node's one nodule is its own core cartridge's (ADR 053); its actor, heads and revision
differ per node. Its requests are all resolved, including the three for the nodules service's
reads, whose provider registers after the nodule service has booted.

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
      "requests": {
        "org.example.daybook.authority.attenuateCapability": {
          "contract": "org.example.daybook.authority.attenuateCapability",
          "locked": {
            "contract": "org.example.daybook.authority.attenuateCapability",
            "provider": "builtin:authority"
          }
        },
        "org.example.daybook.authority.openActor": {
          "contract": "org.example.daybook.authority.openActor",
          "locked": {
            "contract": "org.example.daybook.authority.openActor",
            "provider": "builtin:authority"
          }
        },
        "org.example.daybook.authority.retractCapability": {
          "contract": "org.example.daybook.authority.retractCapability",
          "locked": {
            "contract": "org.example.daybook.authority.retractCapability",
            "provider": "builtin:authority"
          }
        },
        "org.example.daybook.cartridges.awaitCartridgeGrants": {
          "contract": "org.example.daybook.cartridges.awaitCartridgeGrants",
          "locked": {
            "contract": "org.example.daybook.cartridges.awaitCartridgeGrants",
            "provider": "builtin:nodules"
          }
        },
        "org.example.daybook.cartridges.listCartridgeGrants": {
          "contract": "org.example.daybook.cartridges.listCartridgeGrants",
          "locked": {
            "contract": "org.example.daybook.cartridges.listCartridgeGrants",
            "provider": "builtin:nodules"
          }
        },
        "org.example.daybook.cartridges.listCartridgeInstalls": {
          "contract": "org.example.daybook.cartridges.listCartridgeInstalls",
          "locked": {
            "contract": "org.example.daybook.cartridges.listCartridgeInstalls",
            "provider": "builtin:nodules"
          }
        }
      },
...
      "version": "0.2.0"
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
