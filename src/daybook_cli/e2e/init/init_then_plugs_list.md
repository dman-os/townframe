# init then `plugs list`: core plug present after a fresh boot

The boot path must ensure the `@daybook/core` plug and materialize the plugs
cache, so a fresh repo lists it as enabled.

```console
$ daybook_cli init
$ daybook_cli plugs list
 ID             Version  Status   Config Doc[..]
 @daybook/core  0.0.1    enabled  [..]

```
