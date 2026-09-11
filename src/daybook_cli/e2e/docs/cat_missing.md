# `cat` on a missing document fails

`cat` with an unknown id reports the failure through the tracing `error!`
path (timestamped, volatile — elided with `...`) and exits failure (code 1).

```console
$ daybook_cli init
$ daybook_cli cat doesnotexist123
? failed
...
```
