// -*- mode: Bluespec; -*-
//
// The server machine of `src/big_automata/replay/server.rs` (`ReplayServer::update`, `close`,
// `sweep`) alone, for ONE client principal, with every input a free event. Where `session.qnt`
// asks whether the protocol is safe over a network, this asks whether the Rust machine IS this
// transition system: TLC enumerates the whole reachable graph, `server.py` exports it as
// `server.graph.json`, and `replay/server/tests/conformance.rs` checks every (state, event)
// edge against the Rust code. Run instructions are in `server.md`.
//
// Events (laws 1-5 and the request validation of `update`):
//   update(session, generation, additions, removals) with an admission bit per route,
//   close(session), sweep (forgets every session).
// Each edge is (state, event, answer, next state). Answers: `updated` (generation, refused),
// `unknown_session` (law 1) and `invalid` (a request that names an id twice, or adds an id the
// session holds and does not remove).
//
// Left out on purpose:
// * `Limit` (law 14): the caps are 16384 targets and 8 sessions, unreachable in a graph this
//   small, and scaling a `const` down would mean editing `server.rs`.
// * `resolve_round`: its answers depend on a part store, not on this machine alone.
// * TTL: `sweep` is the only way time matters, and it forgets everything (`now` is past the
//   TTL of every session, which the conformance test arranges).
//
// Encoding notes (chosen so TLC and the converter stay simple):
// * Total maps. A session slot is always present; `held` says whether the server holds it, and
//   an absent target is route -1.
// * No sum types: events and answers are flat records with a string `kind`.
module server {
  // One session id, targets 0..2, routes 0..1, generations 0..2. The graph is the product of
  // the sessions' graphs (nothing couples two sessions), so every session id multiplies the
  // node count by the per-session count and is left to the Rust test's isolation check; see
  // `server.md`.
  pure val SESSIONS = 0.to(0)
  pure val TARGETS = 0.to(2)
  pure val ROUTES = 0.to(1)
  pure val GENS = 0.to(2)
  pure val NO_ROUTE = -1

  type Session = { held: bool, generation: int, targets: int -> int, refused: List[int] }
  type Event = {
    kind: str,
    session: int,
    generation: int,
    additions: List[(int, int)],
    removals: List[int],
    // The routes the `admits` closure answers true for.
    admitted: Set[int],
  }
  type Answer = { kind: str, generation: int, refused: List[int] }

  pure val NO_SESSION: Session = {
    held: false,
    generation: 0,
    targets: TARGETS.mapBy(_ => NO_ROUTE),
    refused: List(),
  }

  pure def hasDuplicate(ids: List[int]): bool =
    ids.indices().exists(i => ids.indices().exists(j => i < j and ids[i] == ids[j]))

  // Law 4: an addition is held when its route is admitted, otherwise named in `refused` (in
  // addition order).
  pure def admitInto(s: Session, additions: List[(int, int)], admitted: Set[int]): Session =
    additions.foldl(s, (acc, a) =>
      if (admitted.contains(a._2)) { ...acc, targets: acc.targets.put(a._1, a._2) }
      else { ...acc, refused: acc.refused.append(a._1) }
    )

  pure val ANSWER_INVALID: Answer = { kind: "invalid", generation: 0, refused: List() }
  pure val ANSWER_UNKNOWN: Answer = { kind: "unknown_session", generation: 0, refused: List() }

  pure def answerOf(s: Session): Answer =
    { kind: "updated", generation: s.generation, refused: s.refused }

  // `ReplayServer::update` for one principal: the answer, and the server's session `e.session`
  // afterwards (the other session is untouched).
  pure def update(s: Session, e: Event): (Answer, Session) = {
    val ids = e.additions.foldl(List(), (acc, a) => acc.append(a._1))
    if (hasDuplicate(ids) or hasDuplicate(e.removals)) {
      (ANSWER_INVALID, s)
    } else if (not(s.held)) {
      // Law 1: only generation 0 opens, with the additions as the whole set (removals have
      // nothing to remove).
      if (e.generation != 0) (ANSWER_UNKNOWN, s)
      else {
        val opened = admitInto({ ...NO_SESSION, held: true }, e.additions, e.admitted)
        (answerOf(opened), opened)
      }
    } else if (e.generation <= s.generation) {
      // Law 2: a repeat or a stale copy applies nothing and reads back the held outcome.
      (answerOf(s), s)
    } else {
      // Law 3: removals first; removing an unheld id is a no-op.
      val removed = e.removals.foldl(s.targets, (m, id) => m.put(id, NO_ROUTE))
      val reused = ids.indices().exists(i =>
        s.targets.get(ids[i]) != NO_ROUTE
          and not(e.removals.indices().exists(j => e.removals[j] == ids[i])))
      if (reused) (ANSWER_INVALID, s)
      else {
        val applied = admitInto({ ...s, targets: removed, generation: e.generation, refused: List() },
                             e.additions, e.admitted)
        (answerOf(applied), applied)
      }
    }
  }

  // ---- the event alphabet ----
  pure val PAIRS = TARGETS.map(id => ROUTES.map(r => (id, r))).flatten()
  // At most one addition over any target, or two over targets 0 and 1 (the same id twice is an
  // `invalid` request).
  pure val LOW_PAIRS = 0.to(1).map(id => ROUTES.map(r => (id, r))).flatten()
  pure val ADDITIONS: Set[List[(int, int)]] =
    Set(List())
      .union(PAIRS.map(p => List(p)))
      .union(LOW_PAIRS.map(p => LOW_PAIRS.map(q => List(p, q))).flatten())
  // At most one removal.
  pure val REMOVALS: Set[List[int]] = Set(List()).union(TARGETS.map(i => List(i)))
  pure val ADMITTED: Set[Set[int]] = ROUTES.powerset()

  pure val NO_EVENT: Event =
    { kind: "init", session: 0, generation: 0, additions: List(), removals: List(), admitted: Set() }

  pure val UPDATES: Set[Event] =
    tuples(SESSIONS, GENS, ADDITIONS, REMOVALS, ADMITTED).map(t =>
      { kind: "update", session: t._1, generation: t._2, additions: t._3, removals: t._4,
        admitted: t._5 })
  pure val CLOSES: Set[Event] =
    SESSIONS.map(s => { ...NO_EVENT, kind: "close", session: s })
  pure val SWEEP: Event = { ...NO_EVENT, kind: "sweep" }
  pure val EVENTS: Set[Event] = UPDATES.union(CLOSES).union(Set(SWEEP))

  // The server state, plus the last edge taken (`lastEvent`, `lastAnswer`), which the TLC run
  // prints for the converter. The state graph's nodes are `srv` alone (see `server.md`).
  var srv: int -> Session
  var lastEvent: Event
  var lastAnswer: Answer

  action init = all {
    srv' = SESSIONS.mapBy(_ => NO_SESSION),
    lastEvent' = NO_EVENT,
    lastAnswer' = ANSWER_INVALID,
  }

  action step = {
    nondet e = EVENTS.oneOf()
    if (e.kind == "update") {
      val r = update(srv.get(e.session), e)
      all {
        srv' = srv.put(e.session, r._2),
        lastEvent' = e,
        lastAnswer' = r._1,
      }
    } else if (e.kind == "close") {
      all {
        srv' = srv.put(e.session, NO_SESSION),
        lastEvent' = e,
        lastAnswer' = { kind: "none", generation: 0, refused: List() },
      }
    } else {
      all {
        srv' = SESSIONS.mapBy(_ => NO_SESSION),
        lastEvent' = e,
        lastAnswer' = { kind: "none", generation: 0, refused: List() },
      }
    }
  }

  // Invariants (cheap sanity checks of the model itself).
  val refusedNotHeld = SESSIONS.forall(s =>
    srv.get(s).refused.indices().forall(i => srv.get(s).targets.get(srv.get(s).refused[i]) == NO_ROUTE))
  val forgottenIsBlank = SESSIONS.forall(s => srv.get(s).held or srv.get(s) == NO_SESSION)
  val sane = refusedNotHeld and forgottenIsBlank
}
