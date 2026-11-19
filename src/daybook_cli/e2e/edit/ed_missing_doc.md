# `ed` on a nonexistent document fails cleanly

```console
$ daybook_cli init
$ daybook_cli ed does-not-exist
? 1
   [..]s ERROR daybook_cli::cmds::ed: document not found: does-not-exist

```
