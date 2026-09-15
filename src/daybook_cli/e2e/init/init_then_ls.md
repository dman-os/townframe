# init then boot the drawer: `ls` works

Regression for the `lazy.rs` boot bug: `plugs_repo()` was resolved before the
drawer attached to the plugs repo, which broke every post-init command, not
just `plugs`. `ls` on a fresh repo must succeed and print the doc table.

Doc ids are random per repo, so the id column is elided with `[..]` and the
rows with `...`.

```console
$ daybook_cli init
$ daybook_cli ls
[..]Title       Branches 
...
```
