# Commands on a directory with no node say so

`status`, `exec` and `daemon` need a node; without one they fail with a pointer to
`day init` instead of creating anything.

```console
$ day status
? 1
[..]node not initialized at resolved path; run `day init`[..]

$ day exec org.example.nothing.here
? 1
[..]node not initialized at resolved path; run `day init`[..]

$ day daemon start
? 1
[..]node not initialized at resolved path; run `day init`[..]

```
