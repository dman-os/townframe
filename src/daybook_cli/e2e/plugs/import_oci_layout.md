# `plugs import` from a local OCI layout, then `plugs list`

The canonical plug installation flow: import the test plug artifact and see it
enabled with a config doc.

The artifact is copied into the sandbox from `$PLUG_OCI` (the xtask-built
layout at target/oci/@daybook/test) so cases reference a stable path.

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ daybook_cli plugs import ./plug-oci
imported @daybook/test v0.0.1 (doc: [..])

$ daybook_cli plugs list
 ID             Version  Status   Config Doc[..]
 @daybook/core  0.0.1    enabled  [..]
 @daybook/test  0.0.1    enabled  [..]

```
