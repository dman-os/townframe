# `cat` prints a document's content

`cat <id>` renders the doc as a `{:#?}` debug block followed by pretty JSON.
Both blocks are build-random (doc id, automerge change heads, facet values),
so the whole output is elided with `...`. The assertion is the round trip:
capture the id from `touch`, then `cat` that exact id inside the same sandbox
step succeeds (exit 0).

```console
$ daybook_cli init
$ sh -c 'set -e; ID=$("$DAYBOOK_CLI" touch); "$DAYBOOK_CLI" cat "$ID"'
...
```
