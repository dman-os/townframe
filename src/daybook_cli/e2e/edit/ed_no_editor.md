# `ed` fails cleanly when EDITOR cannot be spawned

The editor spawn happens after the doc is fetched, so a valid doc id is
required first. The spawn error propagates as an eyre report.

<!-- The eyre report keeps the stable error message while eliding source location details. -->

```console
$ daybook_cli init
$ sh -c 'ID=$("$DAYBOOK_CLI" touch); echo "$ID" > .id'

$ sh -c 'ID=$(cat .id); EDITOR=/nonexistent/editor "$DAYBOOK_CLI" ed "$ID"'
? failed
Error: 
   0: No such file or directory (os error 2)

Location:
   [..]

Backtrace omitted. Run with RUST_BACKTRACE=1 environment variable to display it.
Run with RUST_BACKTRACE=full to include source snippets.

```
