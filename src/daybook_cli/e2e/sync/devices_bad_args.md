# `devices` rejects unknown subcommands

Clap usage error, exit 2 (mirrors the `init --bogus` contract).

```console
$ daybook_cli init
$ daybook_cli devices bogus
? 2
error: unrecognized subcommand 'bogus'

Usage: daybook_cli devices <COMMAND>

For more information, try '--help'.

```
<!-- NOTE (resolved by snapshot regeneration): exact clap 4 wording for an unknown subcommand (may be "unrecognized
subcommand" vs "unexpected argument", and may include a tip line) — fix via overwrite pass. -->
