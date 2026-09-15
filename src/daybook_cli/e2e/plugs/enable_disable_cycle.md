# enable/disable cycle: import known-only, enable, disable, re-enable

The doc id is random per run, so the first step captures it into a sandbox
file and later steps reuse it (re-enabling a *different* doc at the same
version is rejected as a republish without a version bump).

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ sh -c 'DOC=$("$DAYBOOK_CLI" plugs import ./plug-oci --no-enable | sed -n "s/.*(doc: \([A-Za-z0-9]*\)).*/\1/p"); echo "$DOC" > .docid; "$DAYBOOK_CLI" plugs enable "$DOC"'
enabled [..] at db+facet:///[..]/org.example.daybook.plugManifest/main?branch=main&at=[..]

$ daybook_cli plugs list
 ID             Version  Status   Config Doc[..]
 @daybook/core  0.0.1    enabled  [..]
 @daybook/test  0.0.1    enabled  [..]

$ daybook_cli plugs disable @daybook/test
disabled @daybook/test (config heads: [..])

$ daybook_cli plugs list
 ID             Version  Status    Config Doc[..]
 @daybook/core  0.0.1    enabled   [..]
 @daybook/test  0.0.1    disabled  [..]

$ sh -c 'DOC=$(cat .docid); "$DAYBOOK_CLI" plugs enable "$DOC"'
enabled [..] at db+facet:///[..]/org.example.daybook.plugManifest/main?branch=main&at=[..]

$ daybook_cli plugs list
 ID             Version  Status   Config Doc[..]
 @daybook/core  0.0.1    enabled  [..]
 @daybook/test  0.0.1    enabled  [..]

```
