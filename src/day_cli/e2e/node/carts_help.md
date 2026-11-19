# `day carts dev-takeover --help`

```console
$ day carts dev-takeover --help
Dev hook: run an installed dev build AS another installed cartridge, to test it against real data

The dev build takes over the slot: it runs under the original's actor with its data, its drawer grants in full and the method grants it asks for that the original's own code asked for too. Nothing else does this: routing a method to another provider (`org.example.daybook.nodules.setMethodRoute`) leaves each provider its own data and grants. The node refuses unless its node.json sets `dev.allow_slot_takeover`, and the dev build must not be enabled on its own.

Usage: day carts dev-takeover [OPTIONS] <SLOT>

Arguments:
  <SLOT>
          The cartridge whose slot is taken over, as `<did>/<name>`

Options:
      --by <BY>
          The installed cartridge that runs as the slot

      --node <DIR>
          The node's directory
          
          [env: DAY_NODE=[..]/node]

      --clear
          End the takeover: the slot runs its own install again

  -h, --help
          Print help (see a summary with '-h')

```
