# `devices add` with a malformed ticket

`devices add` reaches the network for valid tickets (clone-provision RPC), so
only the fast local parse failure is testable: the endpoint-ticket parser runs
before any endpoint is bound. `? 1`, eyre report on stderr.

```console
$ daybook_cli init
$ daybook_cli devices add garbage
? 1
Error: 
   0: invalid endpoint ticket payload in clone url
   1: wrong prefix, expected endpoint

Location:
   src/daybook_core/sync/bootstrap.rs:728

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
<!-- NOTE (resolved by snapshot regeneration): the Location file:line (bootstrap.rs parse_clone_endpoint_addr wrap_err
site) is elided with [..]; a possible additional `   1: <source parse error>` line is not
included here — fix via overwrite pass. -->
