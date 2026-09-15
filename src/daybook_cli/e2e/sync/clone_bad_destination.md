# `clone` rejects a non-empty destination

The destination guard runs before any network or keyring work (a real clone
would need a live peer and would write a per-device identity to the system
keyring — deliberately not exercised). Pointing at the initialized sandbox
`repo/` directory (non-empty) must fail fast with `? 1`.

The absolute destination path is sandbox-specific (random temp dir), so it is
elided with `[..]`.

```console
$ daybook_cli init
$ daybook_cli clone garbage ./repo
? 1
Error: 
   0: clone destination must be empty or non-existent: [..]

Location:
   src/daybook_core/sync/bootstrap.rs:[..]

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
<!-- NOTE (resolved by snapshot regeneration): Location file:line elided with [..]; if the report renders the
destination path differently (quoted/expanded), fix via overwrite pass. -->
