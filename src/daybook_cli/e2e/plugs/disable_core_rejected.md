# `plugs disable` is rejected for `@daybook/core`

The core plug is the system bootstrap; disabling it must fail with a clear
error and a non-zero exit code.

```console
$ daybook_cli init
$ daybook_cli plugs disable @daybook/core
? failed
Error: 
   0: @daybook/core cannot be disabled

Location:
   src/daybook_core/plugs/mutations.rs:261

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
