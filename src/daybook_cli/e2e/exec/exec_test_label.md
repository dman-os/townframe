# `exec test-label` runs a plug routine against a doc

The `test-label` command is a DocCommand of the `@daybook/test` plug: it runs
a wasm routine that writes a `LabelGeneric` facet with the value `test_label`
into the given document. We create a doc with `touch`, capture its id, run
the command, then assert the label landed via `cat`.

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ daybook_cli plugs import ./plug-oci
imported @daybook/test v0.0.1 (doc: [..])

$ sh -c 'set -e; ID=$("$DAYBOOK_CLI" touch); echo "$ID" > .id; "$DAYBOOK_CLI" exec test-label "$ID"'

$ sh -c 'ID=$(cat .id); "$DAYBOOK_CLI" cat "$ID" | grep -q "test_label" && echo "label written"'
label written

```
