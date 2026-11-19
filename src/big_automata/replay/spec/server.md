# Server machine: Quint model and full-graph conformance

`server.qnt` is the `ReplayServer` machine alone (`update`, `close`, `sweep`; laws 1-5 and the
request validation of `update`) for one client principal, with every input a free event. Where
`session.qnt` asks whether the protocol is safe over a network, this asks whether the Rust
machine *is* the model: `server/tests/conformance.rs` checks every (state, event) pair of the
model's whole reachable graph against the Rust code, so `update` accepts exactly the transitions
the model allows. Trace replay samples paths; this cannot miss an edge of the alphabet.

## What is modelled

* Events: `update(session, generation, additions, removals)` with an admission bit per route
  (`admitted`, the routes the `admits` closure says yes to), `close(session)`, and `sweep`.
* Answers: `updated(generation, refused)`, `unknown_session`, `invalid`.
* Domains: generations 0..2, target ids 0..2, routes 0..1, updates carry at most one removal and
  either at most one addition over any id or two over ids 0 and 1 (so the same id twice, which
  is `invalid`, is in the alphabet).
* **One session id.** Nothing couples two sessions, so the graph over two ids is the product of
  the per-session graph with itself (141 nodes becomes 19881, with 1106 events each: 22M edges).
  The conformance test instead checks isolation directly: every edge, run beside a busy second
  session, on either id, leaves the other session untouched (`sessions_are_isolated`).
* **No `Limit`** (law 14): the caps are 16384 targets and 8 sessions, unreachable in a graph
  this small, and scaling them down would mean making the constants configurable in `server.rs`.
* No `resolve_round` (it depends on a part store) and no TTL beyond `sweep`.

## Regenerating `server.graph.json`

TLC enumerates the graph; `server.py` runs it and writes the fixture. From the repository root,
in `nix develop`:

```sh
cd src/big_automata/replay/spec
python3 server.py regen            # rewrites server.graph.json (about 20 s, mostly JVM start)
python3 server.py regen --check    # fails if the checked-in fixture is stale
```

On the build machine: `~/.claude/skills/pi-lanes/ms-run tlc 'cd src/big_automata/replay/spec &&
python3 server.py regen'`, then copy `server.graph.json` back.

How it works, since TLC's `-dump dot` cannot do it (it merges edges between the same two states
and carries neither the event nor the answer):

1. `quint compile --target=tlaplus --main=server server.qnt` gives TLA+; Apalache's `:=` is
   rewritten to `=` for TLC.
2. A wrapper module defines `View == srv` (a node is the server state alone; the model also
   keeps `lastEvent` and `lastAnswer` as variables so the transition can be printed) and
   `EdgeNext == q_step /\ PrintT(<<"EDGE", srv, lastEvent', lastAnswer', srv'>>)`.
3. TLC runs with `VIEW`, from the jar bundled with Apalache (`tlc2.TLC`), expanding each node once
   and printing each (state, event, answer, next state) it evaluates. `server.py` parses those
   values and checks the graph is a function: every (node, event) has exactly one edge.

The fixture is `nodes` (node 0 is the initial state), `events`, `answers`, and `next[node]`, a flat
array holding, for event index `e`, the answer index at `2e` and the next node at `2e + 1`.

Safety sanity checks of the model itself: `quint run --invariant=sane server.qnt`.
