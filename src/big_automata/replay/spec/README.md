# Replay session protocol: Quint model

`session.qnt` models the replay session protocol of
[`src/big_automata/replay.rs`](../../replay.rs) (its 14 laws) and
[`wire.rs`](../wire.rs): one client and one server, over a network that
drops, duplicates, reorders and delays messages. The server can forget a
session (`CloseSession`, a TTL sweep and a restart are one step), and the
client can restart onto a fresh session id. TLC explores each configuration's
whole state graph.

## Running

The dev shell (`nix develop`) provides `quint` (`src/nix/formal-tools.nix`).
TLC comes with it: Apalache's jar bundles TLC, and
`quint verify --backend=tlc` runs it from there, so no separate
`tla2tools.jar` is pinned. Run from this directory:

```sh
# Safety: invariants are state predicates of the module, comma separated.
quint verify --backend=tlc --main=final --invariant=safety,boundsHeld session.qnt

# Liveness: one temporal property per run gives one counterexample per property.
quint verify --backend=tlc --main=final --temporal=liveCatchesUp session.qnt
```

`--main` picks the configuration. TLC stops at the first violation and prints
the trace (breadth first, so it is a shortest one). A quick random search
before a TLC run: `quint run --main=laws --invariant=agreement session.qnt`.

Configurations (all two routes, one drop, bounds sized so `boundsHeld` holds):

| module    | variant                                       | routes | budget | revisions |
|-----------|-----------------------------------------------|--------|--------|-----------|
| `laws`    | the laws as written                           | 2      | 4      | 1         |
| `amended` | client opens a new session on `UnknownTarget` | 2      | 4      | 1         |
| `final`   | server never opens a session id it forgot     | 2      | 4      | 1         |
| `rows`    | `final`, with room for rows to arrive late    | 1      | 5      | 2         |

`amended` and `final` are candidate amendments for the hole below, modelled to
see what each buys. Neither is a law.

## What the model abstracts

* A route's log is a counter `head`. A round answers `Events` with the rows
  `(cursor, resume]` for a `resume` between the round's cursor and the head,
  at least one row past the cursor when there is one (a page `limit` is at
  least one revision). Rows travel as an explicit set, so the client's checks
  do not trust `resume`.
* A round names one target. Verdicts are per target (law 7), so a round of
  several targets is several of these.
* Requests stay in the network until a drop removes them, so any request can
  arrive again, late and out of order. An answer is consumed when read: a
  second copy of an answer is what re-delivering its request produces. The
  server leaves out answers the client would ignore forever (an older session,
  an older update, a refused round): session ids, generations and cursors only
  grow.
* Unknown and unreadable routes answer alike (law 4), so all routes exist and
  only readability changes.
* Environment steps (readability flips, appends, wanted-set changes,
  forgetting, client restarts) spend a budget, and drops spend their own. Every
  behaviour therefore ends with the environment still, which is the premise of
  the liveness properties.
* A refused route is retried only while it is readable (`unblock`). A retry
  while unreadable is refused again and ends where it began with a larger id,
  so leaving it out keeps the graph finite. The retry that is taken can still
  race a revocation.
* Fairness: the newest update and the newest round per route are eventually
  answered, answers that change the client are eventually read, and the
  client's own steps (send, retry, unblock, round) are weakly fair. Stale
  requests carry no fairness, since the server may answer them forever.
* The TTL sweep (`sweepRevived`) is weakly fair and spends no budget. It takes a
  revived session (one whose id was forgotten) that holds ids no live session of
  the client carries: any such session the client has left, or removed ids in the
  client's own. `forget` (the environment's choice, budgeted) is unchanged.
* A late duplicate that revives a forgotten session is spent: the network is a
  set that never consumes, so without this the same copy could revive the session
  after every sweep for ever, and no fairness rules that out. A real network
  holds finitely many copies. The cost: one copy revives at most once, so
  `amended` no longer explores a second revival by the same copy.

## Properties

Safety (state invariants; history kept in ghost variables):

* `law2StaleNoop` (law 2): an update the server has applied before never
  changes server state when delivered again. "Applied before" is a session held
  at that generation or newer, or a session the server forgot (a session exists
  only once its generation 0 applied).
* `law3IdsUnique` (law 3): an id names one route within a session.
* `noOrphans` (laws 2, 3): the server never holds, in the client's session, an
  id the client stopped tracking; a stale addition cannot resurrect a removed
  id.
* `law5NoRevival` (law 5): a session the server forgot is never held again
  while the client still uses its id.
* `agreement`: with no update in flight and an acknowledgement in hand, the
  client's targets equal the server's, minus removals the client queued and has
  not sent.
* `law7NoLeak` (law 7): every row the client holds existed at some moment the
  route was readable, and is filed under the route the server read.
* `law8NoSkip` (laws 8, 9): the client's cursor never passes a row it did not
  receive. The cursor never decreases by construction: the client keeps the
  larger of its cursor and `resume`.
* `law9InOrder` (law 9): no `Events` answer reaches the client with a `resume`
  below its cursor.
* `blockedStayWanted` (law 4): refused routes stay wanted; every wanted route is
  held, pending or blocked, and in exactly one of them.
* `law6NoUnknownTarget` (law 6): no round is answered `UnknownTarget` for a
  target the client holds and is not removing.
* `noAhead`: the answer to the client's newest update names that update's
  generation.
* `boundsHeld`: the client never needed a session id, target id or generation
  the bounds did not have. It holds in every configuration, so no liveness
  failure is an artifact of the bounds.
* `safety` is all of the above but `law9InOrder` and `boundsHeld`;
  `safetyCore` leaves out the ones the hole below breaks.

Liveness (each `fairness implies ...`):

* `liveConverges`: eventually and forever, the server holds every wanted
  readable route and nothing unwanted, and the client's held set is the
  server's.
* `liveCatchesUp`: eventually and forever, every wanted readable route's
  cursor is at its head.
* `liveConvergesExactly`: the server's routes eventually equal the wanted
  readable set. This is false by design: law 4 admits at update time, so a
  route revoked after admission stays held and law 7 refuses its rounds.
  `liveConverges` is the property the laws promise.

## Results

TLC on mothership, 2026-11-18 (logs were `~/scratch/spec-*.log` there).

**`laws` (the laws as written).** Every safety invariant the hole doesn't touch holds:
`safetyCore`, 4,701,485 distinct states, exhaustive, 9m30s. `law9InOrder` and
`liveCatchesUp` hold too.

These fail, all on the same hole: a duplicate of a session's opening update (generation 0)
delivered after the server forgot the session opens it again under the same id, and the
client is never told.
- `law2StaleNoop`: the late opening changes server state.
- `law5NoRevival`.
- `noOrphans`: ids the client already removed come back.
- `agreement`.
- `law6NoUnknownTarget`: targets added since the opening are missing.
- `liveConverges`: the orphans are never removed.

The Rust simulation found the same hole (`replay::tests::a_late_opening_update_revives_a_forgotten_session`).

`liveConvergesExactly` fails by design (see above).

**`final` (the server never opens a session id it forgot).** The whole `safety` set holds:
4,325,224 distinct states, exhaustive, 8m12s. The `liveCatchesUp` run did not finish: it had
3.9M distinct states and 189k left on its queue when it stopped. Re-run it before relying on
liveness for `final`.

**Not run to completion:** `amended`, and the `rows` configuration.

**Decision (operator, 2026-11-18):** law 5 is amended to allow the revival (see `replay.rs`):
the client resets on an `UnknownTarget` for a target it holds, and orphans expire with the
revived session. The `amended` configuration models this. Its safety run (with the invariants
the revival breaks by design, `law5NoRevival`, `noOrphans` and `agreement`, left out or
restated) and its `liveCatchesUp` run are still to do.

**`amended` results (2026-11-18, TLC):**
- `safetyCore,law9InOrder,law6NoUnknownTarget,boundsHeld` hold: 4,447,966 distinct states,
  exhaustive, 8m29s.
- `liveCatchesUp` holds: exhaustive, 35m29s. Every wanted, readable route's cursor reaches its
  head.
- `liveConverges` as first run failed (1m07s). The trace: the client wants `a`, restarts
  (session 1), sends the opening, unwants `a` and sends the removal; the server forgets session
  1, and a late duplicate of the opening revives it with the orphan (`0 -> "b"`); the client,
  now at generation 2 with nothing wanted and nothing in flight, has no update due and never
  touches the session. Nothing removes the orphan, because the model had no TTL sweep. Class:
  a missing fairness assumption, not a protocol hole (the amended law says orphans expire with
  the revived session at its TTL). Adding the weakly fair `sweepRevived` exposed a second
  lasso: sweep, then the server re-takes the same stale opening from the set network, for ever.
  That is the network model's fault (see the spent-duplicate rule above).
- After both changes (`sweepRevived`, fair; revival spends the duplicate):
  - `safetyCore,law9InOrder,law6NoUnknownTarget,boundsHeld` hold: 5,033,138 distinct states,
    exhaustive, 8m59s.
  - `liveConverges`: rerun pending (the run was cut by a mothership reboot).

## Not checked

* Law 10 (`drained`): a pacing hint with no correctness content; the model has
  no pacing.
* Law 11 (every removal after the cursor is delivered): the model's log has no
  membership, so there are no removals to deliver or omit.
* Law 12 (object targets): every route is a part; an object route's
  readability "live in some readable part" is one more readability bit, which
  the model already varies freely.
* Law 13 (hold): timing only, and "waiting never changes what a round may
  carry" is a statement about real time that a state graph does not see.
* Law 14 (caps): no caps are modelled. The `UpdateSession` that opens and the
  orphan sessions below would count against the sessions-per-client cap.
* Rounds over several targets in one snapshot (law 7's "one snapshot"): rounds
  name one target.
* `CloseSession` as a message: forgetting is a server step, which law 5 says
  is the same thing.
