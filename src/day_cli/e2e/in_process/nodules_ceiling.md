# `nodules ceiling add | rm | ls` without a daemon

The ceiling starts empty. `add` and `rm` are idempotent and say whether they changed anything,
validate every NSID before writing, and `ls` shows what `node.json` holds.

```console
$ day init
initialized node [..] at [..]/node

$ day nodules ceiling ls
authorities 0
operations 0

$ day nodules ceiling add --authority org.example.daybook.docs --operation org.example.daybook.drawers.listKnownDrawers
authority org.example.daybook.docs: added to the ceiling
operation org.example.daybook.drawers.listKnownDrawers: added to the ceiling

$ day nodules ceiling add --authority org.example.daybook.docs
authority org.example.daybook.docs: already in the ceiling

$ day nodules ceiling ls
authorities 1
  org.example.daybook.docs
operations 1
  org.example.daybook.drawers.listKnownDrawers

$ day nodules ceiling add --authority 'not/an/nsid'
? 1
Error:[..]
   0: `not/an/nsid` is not an NSID (dot-separated letters, digits and hyphens)
...

$ day nodules ceiling add
? 1
Error:[..]
   0: name at least one --authority or --operation
...

$ day nodules ceiling rm --authority org.example.daybook.docs --authority org.example.other
authority org.example.daybook.docs: removed from the ceiling
authority org.example.other: not in the ceiling

$ day nodules ceiling ls
authorities 0
operations 1
  org.example.daybook.drawers.listKnownDrawers

```
