# `devices ls` on a fresh repo

No devices known yet: the comfy-table (NOTHING preset) renders its header with
zero data rows.

```console
$ daybook_cli init
$ daybook_cli devices ls
 Endpoint  Name  Added At 

```
<!-- NOTE (resolved by snapshot regeneration): exact empty-table layout (header-only comfy_table). The header row
`Endpoint|Name|Added At` is certain; whether a trailing blank line or extra separator line
prints is not, fix via overwrite pass. -->
