# `call` prints the node's error envelope and fails

An NSID nothing claims is the router's `UnknownMethod`. Input that is not JSON is refused
before the node is booted.

```console
$ day init
initialized node [..] at [..]/node

$ day call org.example.nothing.here
? 1
{
  "error": "UnknownMethod",
  "message": "no provider implements the operation"
}
[..]the node refused the call[..]

$ day call org.example.daybook.nodules.listNodules '{nope'
? 1
...
[..]the input is not valid JSON[..]
...

$ day call 'not/an/nsid'
? 1
...
[..]is not an NSID[..]
...

```
