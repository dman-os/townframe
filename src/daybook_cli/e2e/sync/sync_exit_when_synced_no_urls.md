# `sync --exit-when-synced` with no sync URLs

The sync machinery (iroh peer exchange) is known-broken, so this case asserts
the deterministic surfaces only. Plain `daybook_cli sync` loops forever; the
`--exit-when-synced` form without any sync URLs must fail fast instead.

The QR block depends on the random local ticket (volatile lines, `...`), the
ticket itself is random (`[..]`), and the process currently dies at teardown
with an "error shutting down iroh blob store" eyre report (the expected
`--exit-when-synced requires at least one sync URL` error is swallowed by the
teardown failure — this is the known-broken part; this snapshot locks the
current surface and should be updated together with the sync fix).

```console
$ daybook_cli init
$ daybook_cli sync --exit-when-synced
? 1
Scan the following QR code to clone this repo
...
Or copy the following ticket:


[..]


Error: 
   0: error shutting down iroh blob store: Oneshot recv error
      Caused by:
          Sender closed

Location:
   src/daybook_core/blobs.rs:352

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
