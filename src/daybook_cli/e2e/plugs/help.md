# `plugs` subcommand help surface

`--help` for the plugs group and its subcommands — this is the contract for
the CLI surface (names, argument shapes, defaults).

```console
$ daybook_cli plugs --help
...
Manage plugs (ADR 007)

Usage: daybook_cli plugs <COMMAND>

Commands:
  list     List known plugs: id, version, status, config doc id
  show     Show a plug's manifest summary, facets, and refs
  enable   Enable a plug by facet ref or doc id
  disable  Disable a plug (rejected for @daybook/core)
  update   Re-pin a plug to the latest main-branch heads
  pending  Enabled entries whose doc/heads are not locally readable
  import   Import a plug from a doc id, facet ref, OCI registry ref, or local OCI layout
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help

$ daybook_cli plugs import --help
...
Import a plug from a doc id, facet ref, OCI registry ref, or local OCI layout

Usage: daybook_cli plugs import [OPTIONS] <TARGET>

Arguments:
  <TARGET>  db+facet:// ref, bare doc id, oci://<registry-ref>, or a local OCI layout path

Options:
      --no-enable      Leave the plug known but not enabled
      --heads <HEADS>  Pin to explicit heads (pipe-separated) for doc-id targets
  -h, --help           Print help

$ daybook_cli plugs enable --help
...
Enable a plug by facet ref or doc id

Usage: daybook_cli plugs enable [OPTIONS] <TARGET>

Arguments:
  <TARGET>  db+facet:// ref or bare manifest doc id

Options:
      --heads <HEADS>  Pin to explicit heads (pipe-separated) instead of current main
  -h, --help           Print help

```
