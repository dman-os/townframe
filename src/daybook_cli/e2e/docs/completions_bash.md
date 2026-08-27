# `completions bash` works without an initialized repo

`completions` is handled on the static-CLI path *before* the dynamic CLI is
built. On an uninitialized repo the dynamic bootstrap still logs two
timestamped "repo not initialized" errors on stderr (a known wart: the
command works, the boot just complains), then the bash completion script goes
to stdout and the process exits 0.

The volatile error lines and the bulk of the script are elided; the first
script line (clap_complete's `_daybook_cli()` function) is asserted literally.

```console
$ daybook_cli completions bash
...
_daybook_cli() {
...
```
