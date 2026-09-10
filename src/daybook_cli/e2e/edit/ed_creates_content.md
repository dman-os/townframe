# `ed` edits a document through the configured EDITOR

The doc id is random, so the first step captures it into a sandbox file. A
fake EDITOR script rewrites the doc's title facet from "Untitled" to
"Edited via ed" (a no-op `sed`, which is valid JSON round-tripping). Running
the same editor again exercises the no-change path.

```console
$ daybook_cli init
$ sh -c 'ID=$("$DAYBOOK_CLI" touch); echo "$ID" > .id'

$ sh -c 'printf '"'"'#!/bin/sh\nsed -i "s/\\\"Untitled\\\"/\\\"Edited via ed\\\"/" "$1"\n'"'"' > editor.sh && chmod +x editor.sh'
$ sh -c 'ID=$(cat .id); EDITOR=./editor.sh "$DAYBOOK_CLI" ed "$ID"'
Updated document: [..]

$ sh -c 'ID=$(cat .id); "$DAYBOOK_CLI" cat "$ID" | grep -q "Edited via ed"'

$ sh -c 'ID=$(cat .id); OUT=$("$DAYBOOK_CLI" cat "$ID") && ! printf "%s\n" "$OUT" | grep -q "Untitled"'

$ sh -c 'ID=$(cat .id); EDITOR=./editor.sh "$DAYBOOK_CLI" ed "$ID"'
No changes detected.

```
