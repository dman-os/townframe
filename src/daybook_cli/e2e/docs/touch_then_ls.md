# `touch` creates a document; `ls` lists it

`touch` prints the new doc id (random per run) on stdout. The `sh` step
captures it (`set -e` so a failed `touch` aborts the step loudly instead of
being masked by the trailing `echo`) into a sandbox file and echoes it so the
test asserts the id came back. `ls` renders the doc table; ids, the doc set,
and column padding are build-random, so rows are elided — the stable surface
is the header.

```console
$ daybook_cli init
$ sh -c 'set -e; ID=$("$DAYBOOK_CLI" touch); echo "$ID" > .id; echo "id: $ID"'
id: [..]

$ daybook_cli ls
[..]Title       Branches 
...
```
