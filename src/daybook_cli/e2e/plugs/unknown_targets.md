# `plugs` with an unknown target fails cleanly

`show`/`enable`/`disable` on ids that are neither known nor readable must fail
with an error and a non-zero exit code, leaving the repo untouched.

```console
$ daybook_cli init
$ daybook_cli plugs show @nope/nothing
? failed
Error: 
   0: no manifest for @nope/nothing (not known, or not readable at pinned heads)

Location:
   [..]

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

$ daybook_cli plugs enable @nope/nothing
? failed
Error: 
   0: manifest doc missing main branch

Location:
   [..]

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

$ daybook_cli plugs disable @nope/nothing
disabled @nope/nothing (config heads: [..])

```
