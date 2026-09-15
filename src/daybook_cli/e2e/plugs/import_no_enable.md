# `plugs import --no-enable` leaves the plug known but disabled

Importing without enabling must register the plug and its manifest while the
status stays `disabled` — and `pending` must not list it (it was never
enabled).

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ daybook_cli plugs import --no-enable ./plug-oci
imported @daybook/test v0.0.1 (doc: [..]) [known only]

$ daybook_cli plugs list
 ID             Version  Status    Config Doc[..]
 @daybook/core  0.0.1    enabled   [..]
 @daybook/test  0.0.1    disabled  -                                            

$ daybook_cli plugs pending
no pending plugs

```
