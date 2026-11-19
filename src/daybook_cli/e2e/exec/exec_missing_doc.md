# `exec test-label` on a nonexistent doc

The `test-label` command is registered from the imported plug's manifest. A
doc id that does not exist fails in the exec dispatch itself (doc lookup),
before any WASM routine runs — this asserts command registration plus the
dispatch plumbing without executing the plug's routines.

The error report is the plain (NO_COLOR) color-eyre format. The doc id is a
fixed fake id so the message is deterministic.

```console
$ daybook_cli init
$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'
$ daybook_cli plugs import ./plug-oci
imported @daybook/test v0.0.1 (doc: [..])

$ daybook_cli exec test-label deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
? failed
Error: 
   0: document not found: deadbeefdeadbeefdeadbeefdeadbeefdeadbeef

Location:
   [..]

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
