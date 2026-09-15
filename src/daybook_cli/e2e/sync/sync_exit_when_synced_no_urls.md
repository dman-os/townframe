# `sync --exit-when-synced` with no sync URLs

The sync machinery (iroh peer exchange) is known-broken, so this case asserts
the deterministic surfaces only. Plain `daybook_cli sync` loops forever; the
`--exit-when-synced` form without any sync URLs must fail fast instead.

The QR block depends on the random local ticket (volatile lines, `...`), the
the ticket itself is random (`[..]`), and the command reports the deterministic
`--exit-when-synced requires at least one sync URL` validation error before
attempting to wait for peers.

```console
$ daybook_cli init
$ daybook_cli sync --exit-when-synced
? 1
Scan the following QR code to clone this repo
...
Or copy the following ticket:


[..]


[..]s ERROR daybook_cli::cmds::sync: --exit-when-synced requires at least one sync URL

```
