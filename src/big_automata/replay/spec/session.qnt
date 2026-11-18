// -*- mode: Bluespec; -*-
//
// The replay session protocol of `src/big_automata/replay.rs`, as one client and one server
// over a network that drops, duplicates, reorders and delays messages.
//
// The laws this checks are numbered as in the module docs of `replay.rs`. How to run it, and
// what each property means, is in the README beside this file.
//
// Abstractions:
//
// * A route's log is a counter `head`: revisions 1..head exist. A round answers `Events` with
//   the rows `(cursor, resume]` for some `resume` between the round's cursor and the head (the
//   page `limit` picks it), or `Refused` when the route is unreadable at the round. Rows are
//   carried as an explicit set so the client's "no skip" check does not trust `resume`.
// * A round names one target. The laws decide each target's verdict on its own, so a round of
//   several targets is several of these interleaved.
// * The network is a set that keeps every message sent until a drop removes it: a delivery
//   leaves the message in place, so any message can arrive again, late, in any order.
// * Forgetting (CloseSession, TTL, restart) is one server step that drops a session.
// * Unknown and unreadable routes answer alike (law 4), so every route is known here and only
//   readability varies.
// * Every step that is not the protocol's own (readability flips, appends, wanted-set changes,
//   forgetting, client restarts) spends one unit of `ENV_BUDGET`, and drops spend `DROP_BUDGET`.
//   That keeps the graph finite and is what "the environment stops changing" means for the
//   liveness properties: every behaviour has a suffix with no environment steps.
module session {
  // Routes the client may want.
  const ROUTES: Set[str]
  // Session ids are 0..MAX_SID. A reset that would need a larger one is disabled.
  const MAX_SID: int
  // Target ids of one session are 0..MAX_TID-1.
  const MAX_TID: int
  // Generations of one session are 0..MAX_GEN-1.
  const MAX_GEN: int
  // The largest revision of a route.
  const MAX_HEAD: int
  const ENV_BUDGET: int
  const DROP_BUDGET: int
  // Off: the laws as written. A round answered `UnknownTarget` for a target the client holds
  // is a client bug (law 6), recorded in `ghost.bug`, and the client carries on.
  // On: the client treats that answer like `UnknownSession` and opens a new session. This is
  // a candidate amendment, not a law; see the README.
  const RESET_ON_UNKNOWN_TARGET: bool
  // Off: the laws as written. A forgotten session can be opened again by any generation-0
  // update naming it, including a late copy of the one that first opened it (law 1).
  // On: the server remembers every id it forgot and answers `UnknownSession` to it for good.
  // A candidate amendment (it does not survive a restart unless persisted); see the README.
  const FORGET_IS_FINAL: bool

  type Route = str
  type Tid = int

  type UpdateMsg = { sid: int, gen: int, adds: Set[(Tid, Route)], rems: Set[Tid] }

  type Msg =
    // Client to server.
    | Update(UpdateMsg)
    | Page({ sid: int, tid: Tid, route: Route, cursor: int })
    // Server to client. `reqGen`, and a page answer's `route`, are what the transport's
    // request/response pairing tells the client, not wire fields. `srvRoute` is a ghost: the
    // route the server read.
    | Updated({ sid: int, reqGen: int, gen: int, refused: Set[Tid] })
    | UpdateUnknownSession({ sid: int, reqGen: int })
    | Events({ sid: int, route: Route, srvRoute: Route, resume: int, rows: Set[int] })
    | Refused({ sid: int, tid: Tid })
    | UnknownTarget({ sid: int, tid: Tid })
    | PageUnknownSession({ sid: int })

  type Sent = NoUpdate | InFlight(UpdateMsg)

  type Env = {
    readable: Route -> bool,
    head: Route -> int,
    // Ghost: the newest revision of the route that existed at some moment the route was
    // readable. A row past it was never readable by the client.
    exposed: Route -> int,
    budget: int,
    drops: int,
  }

  type Client = {
    wanted: Set[Route],
    sid: int,
    // The generation the next update carries.
    gen: int,
    nextTid: Tid,
    // Targets the client believes the server holds: acknowledged additions.
    believed: Route -> Tid,
    // Additions not acknowledged yet. They stay until an answer to an update carrying them.
    pendingAdd: Route -> Tid,
    // Removals not sent yet, and the removals of the update in flight. A newer update carries
    // both, since the one in flight may never arrive.
    pendingRem: Set[Tid],
    inflightRem: Set[Tid],
    // Wanted routes whose addition was refused, waiting to be re-added under a fresh id.
    blocked: Set[Route],
    lastSent: Sent,
    // An update of this session has been acknowledged.
    acked: bool,
    cursor: Route -> int,
    // Every row the client received, per route.
    got: Route -> Set[int],
  }

  type Session = { gen: int, targets: Tid -> Route, refused: Set[Tid] }

  type Ghost = {
    // Session ids the server has forgotten.
    forgotten: Set[int],
    // A round answered `UnknownTarget` for a target the client holds (law 6's "client bug").
    bug: bool,
    // The client filed rows under a route other than the one the server read.
    misroute: bool,
    // The answer to the client's newest update named a newer generation.
    ahead: bool,
    // An `Events` answer reached the client with a resume below its cursor.
    backwards: bool,
    // The client needed a fresh session id and the model had none left.
    exhausted: bool,
    // The server changed state on an update it had applied before.
    staleChanged: bool,
  }

  var env: Env
  var cl: Client
  var srv: int -> Session
  var net: Set[Msg]
  var ghost: Ghost

  // ---------------------------------------------------------------------------------------
  // Helpers

  pure def without(m: a -> b, k: a): a -> b = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

  pure def tids(m: Route -> Tid): Set[Tid] = m.keys().map(r => m.get(r))

  pure def entries(m: Route -> Tid): Set[(Tid, Route)] = m.keys().map(r => (m.get(r), r))

  pure def heldPairs(m: Tid -> Route): Set[(Tid, Route)] = m.keys().map(t => (t, m.get(t)))

  pure def heldRoutes(m: Tid -> Route): Set[Route] = m.keys().map(t => m.get(t))

  pure def max(a: int, b: int): int = if (a > b) a else b

  // A fresh session with the whole wanted set queued for its opening update (law 5).
  pure def reopened(c: Client): Client = {
    val queued = c.wanted.fold(
      (Map(), 0),
      (acc, r) => (acc._1.put(r, acc._2), acc._2 + 1)
    )
    {
      ...c,
      sid: c.sid + 1,
      gen: 0,
      nextTid: queued._2,
      believed: Map(),
      pendingAdd: queued._1,
      pendingRem: Set(),
      inflightRem: Set(),
      blocked: Set(),
      lastSent: NoUpdate,
      acked: false,
    }
  }

  // Stop tracking a route the client no longer wants. Whatever id it had is removed
  // server-side, including an addition whose answer never came back: it may have landed.
  pure def untracked(c: Client, r: Route): Client = {
    val heldId = if (c.believed.keys().contains(r)) Set(c.believed.get(r)) else Set()
    val pendingId = if (c.pendingAdd.keys().contains(r)) Set(c.pendingAdd.get(r)) else Set()
    {
      ...c,
      wanted: c.wanted.exclude(Set(r)),
      believed: c.believed.without(r),
      pendingAdd: c.pendingAdd.without(r),
      pendingRem: c.pendingRem.union(heldId).union(pendingId),
      blocked: c.blocked.exclude(Set(r)),
    }
  }

  // The client's handling of the answer to its newest update: additions it carried that are
  // still pending under the same id land, or block when refused.
  pure def acknowledged(c: Client, u: UpdateMsg, refused: Set[Tid]): Client = {
    val settled = u.adds.filter(p => c.pendingAdd.keys().contains(p._2) and c.pendingAdd.get(p._2) == p._1)
    val landed = settled.filter(p => not(refused.contains(p._1)))
    val refusedHere = settled.filter(p => refused.contains(p._1))
    val routesSettled = settled.map(p => p._2)
    {
      ...c,
      believed: setToMap(entries(c.believed).union(landed).map(p => (p._2, p._1))),
      pendingAdd: c.pendingAdd.keys().exclude(routesSettled).mapBy(r => c.pendingAdd.get(r)),
      // The refused id goes in a removal: harmless when the server holds nothing under it.
      pendingRem: c.pendingRem.union(refusedHere.map(p => p._1)),
      blocked: c.blocked.union(refusedHere.map(p => p._2)),
      inflightRem: Set(),
      lastSent: NoUpdate,
      acked: true,
    }
  }

  // ---------------------------------------------------------------------------------------
  // Init

  action init = all {
    env' = {
      readable: ROUTES.mapBy(_ => true),
      head: ROUTES.mapBy(_ => 0),
      exposed: ROUTES.mapBy(_ => 0),
      budget: ENV_BUDGET,
      drops: DROP_BUDGET,
    },
    cl' = {
      wanted: Set(),
      sid: 0,
      gen: 0,
      nextTid: 0,
      believed: Map(),
      pendingAdd: Map(),
      pendingRem: Set(),
      inflightRem: Set(),
      blocked: Set(),
      lastSent: NoUpdate,
      acked: false,
      cursor: ROUTES.mapBy(_ => 0),
      got: ROUTES.mapBy(_ => Set()),
    },
    srv' = Map(),
    net' = Set(),
    ghost' = { forgotten: Set(), bug: false, misroute: false, ahead: false, backwards: false, exhausted: false, staleChanged: false },
  }

  // A step that is never taken. A `match` arm that cannot act names it, since every arm of
  // an action must assign the same variables.
  action disabled: bool = all {
    false,
    env' = env, cl' = cl, srv' = srv, net' = net, ghost' = ghost,
  }

  // ---------------------------------------------------------------------------------------
  // Environment

  action flipReadable(r: Route): bool = all {
    env.budget > 0,
    val now = not(env.readable.get(r))
    env' = {
      ...env,
      readable: env.readable.set(r, now),
      exposed: if (now) env.exposed.set(r, max(env.exposed.get(r), env.head.get(r))) else env.exposed,
      budget: env.budget - 1,
    },
    cl' = cl, srv' = srv, net' = net, ghost' = ghost,
  }

  action appendRow(r: Route): bool = all {
    env.budget > 0,
    env.head.get(r) < MAX_HEAD,
    val h = env.head.get(r) + 1
    env' = {
      ...env,
      head: env.head.set(r, h),
      exposed: if (env.readable.get(r)) env.exposed.set(r, h) else env.exposed,
      budget: env.budget - 1,
    },
    cl' = cl, srv' = srv, net' = net, ghost' = ghost,
  }

  action want(r: Route): bool = all {
    env.budget > 0,
    not(cl.wanted.contains(r)),
    cl.nextTid < MAX_TID,
    env' = { ...env, budget: env.budget - 1 },
    cl' = {
      ...cl,
      wanted: cl.wanted.union(Set(r)),
      pendingAdd: cl.pendingAdd.put(r, cl.nextTid),
      nextTid: cl.nextTid + 1,
    },
    srv' = srv, net' = net, ghost' = ghost,
  }

  action unwant(r: Route): bool = all {
    env.budget > 0,
    cl.wanted.contains(r),
    env' = { ...env, budget: env.budget - 1 },
    cl' = untracked(cl, r),
    srv' = srv, net' = net, ghost' = ghost,
  }

  // CloseSession, a TTL sweep and a restart are one step (law 5).
  action forget(sid: int): bool = all {
    env.budget > 0,
    srv.keys().contains(sid),
    env' = { ...env, budget: env.budget - 1 },
    srv' = srv.without(sid),
    ghost' = { ...ghost, forgotten: ghost.forgotten.union(Set(sid)) },
    cl' = cl, net' = net,
  }

  // The client restarts, or abandons its session for its own reasons.
  action clientRestart: bool = all {
    env.budget > 0,
    cl.sid < MAX_SID,
    env' = { ...env, budget: env.budget - 1 },
    cl' = reopened(cl),
    srv' = srv, net' = net, ghost' = ghost,
  }

  action drop(m: Msg): bool = all {
    env.drops > 0,
    env' = { ...env, drops: env.drops - 1 },
    net' = net.exclude(Set(m)),
    cl' = cl, srv' = srv, ghost' = ghost,
  }

  action envStep = any {
    nondet r = ROUTES.oneOf()
    any { flipReadable(r), appendRow(r), want(r), unwant(r) },
    nondet sid = srv.keys().oneOf()
    forget(sid),
    clientRestart,
    nondet m = net.oneOf()
    drop(m),
  }

  // ---------------------------------------------------------------------------------------
  // Client

  // Send the pending changes as the next generation. Enabled when there is something to send
  // that differs from the update already in flight; an identical re-send is `retryUpdate`.
  pure def nextUpdate(c: Client): UpdateMsg =
    { sid: c.sid, gen: c.gen, adds: entries(c.pendingAdd), rems: c.pendingRem.union(c.inflightRem) }

  pure def updateDue(c: Client): bool = {
    val u = nextUpdate(c)
    val same = match c.lastSent {
      | NoUpdate => false
      | InFlight(f) => f.adds == u.adds and f.rems == u.rems
    }
    (u.adds != Set() or u.rems != Set()) and not(same)
  }

  action sendUpdate: bool = {
    val u = nextUpdate(cl)
    all {
      updateDue(cl),
      cl.gen < MAX_GEN,
      cl' = {
        ...cl,
        gen: cl.gen + 1,
        pendingRem: Set(),
        inflightRem: u.rems,
        lastSent: InFlight(u),
      },
      net' = net.union(Set(Update(u))),
      env' = env, srv' = srv, ghost' = ghost,
    }
  }

  // Re-send the update in flight unchanged, after the network lost it.
  action retryUpdate: bool = all {
    cl.lastSent != NoUpdate,
    not(net.contains(Update(inFlight(cl.lastSent)))),
    net' = net.union(Set(Update(inFlight(cl.lastSent)))),
    env' = env, cl' = cl, srv' = srv, ghost' = ghost,
  }

  // Re-add a refused route under a fresh id (law 4). Only retries that can succeed are taken:
  // a retry while the route is still unreadable is refused again and ends where it started,
  // with a larger id. Dropping those keeps the graph finite and loses no interleaving that
  // matters, since the retry taken can still race a revocation.
  action unblock(r: Route): bool = all {
    cl.blocked.contains(r),
    env.readable.get(r),
    cl.nextTid < MAX_TID,
    cl' = {
      ...cl,
      blocked: cl.blocked.exclude(Set(r)),
      pendingAdd: cl.pendingAdd.put(r, cl.nextTid),
      nextTid: cl.nextTid + 1,
    },
    env' = env, srv' = srv, net' = net, ghost' = ghost,
  }

  // One round over one acknowledged target, from the client's cursor.
  action sendPage(r: Route): bool = all {
    cl.believed.keys().contains(r),
    net' = net.union(Set(Page({ sid: cl.sid, tid: cl.believed.get(r), route: r, cursor: cl.cursor.get(r) }))),
    env' = env, cl' = cl, srv' = srv, ghost' = ghost,
  }

  pure val noUpdate: UpdateMsg = { sid: -1, gen: -1, adds: Set(), rems: Set() }

  pure def inFlight(s: Sent): UpdateMsg = match s {
    | NoUpdate => noUpdate
    | InFlight(u) => u
  }

  pure def isReply(m: Msg): bool = match m {
    | Update(_) => false
    | Page(_) => false
    | _ => true
  }

  // The client lost its session: open a new one, unless the model has no session id left.
  pure def sessionLost(c: Client, g: Ghost): (Client, Ghost) =
    if (c.sid < MAX_SID) (reopened(c), g) else (c, { ...g, exhausted: true })

  // What the client makes of one answer. An answer it has no use for (an older session, an
  // older update, a refused round) changes nothing.
  pure def received(c: Client, g: Ghost, m: Msg): (Client, Ghost) = match m {
    | Updated(a) =>
      val u = inFlight(c.lastSent)
      if (a.sid == c.sid and c.lastSent != NoUpdate and u.gen == a.reqGen)
        (acknowledged(c, u, a.refused), { ...g, ahead: g.ahead or a.gen != a.reqGen })
      else (c, g)
    | UpdateUnknownSession(a) => if (a.sid == c.sid) sessionLost(c, g) else (c, g)
    | PageUnknownSession(p) => if (p.sid == c.sid) sessionLost(c, g) else (c, g)
    | UnknownTarget(t) =>
      // A round can race the removal of its own target; only an id the client still holds
      // and is not removing is law 6's client bug.
      val held = t.sid == c.sid
        and tids(c.believed).contains(t.tid)
        and not(c.pendingRem.union(c.inflightRem).contains(t.tid))
      if (not(held)) (c, g)
      else if (RESET_ON_UNKNOWN_TARGET) sessionLost(c, g)
      else (c, { ...g, bug: true })
    | Events(e) =>
      if (e.sid != c.sid) (c, g)
      else (
        {
          ...c,
          got: c.got.set(e.route, c.got.get(e.route).union(e.rows)),
          // Law 8: resume never goes below the round's cursor, but the answer to an earlier
          // round can arrive after a later one, so the cursor only moves forward.
          cursor: c.cursor.set(e.route, max(c.cursor.get(e.route), e.resume)),
        },
        {
          ...g,
          misroute: g.misroute or e.route != e.srvRoute,
          backwards: g.backwards or e.resume < c.cursor.get(e.route),
        },
      )
    | _ => (c, g)
  }

  // Answers are consumed on delivery. A second copy of an answer is what re-delivering its
  // request produces, and requests stay in the network, so nothing is lost by it.
  action clientRecv(m: Msg): bool = {
    val r = received(cl, ghost, m)
    all {
      isReply(m),
      cl' = r._1,
      ghost' = r._2,
      net' = net.exclude(Set(m)),
      env' = env, srv' = srv,
    }
  }

  // ---------------------------------------------------------------------------------------
  // Server

  // Put an answer on the network. An answer the client would ignore (an older session, an
  // older update, a refused round, a round racing its target's removal) is left out: the
  // client's session id, generation and cursors only grow, so it would be ignored forever,
  // and carrying it would only multiply states.
  action reply(m: Msg): bool = all {
    net' = if (received(cl, ghost, m) != (cl, ghost)) net.union(Set(m)) else net,
    env' = env, cl' = cl,
  }

  action serverUpdate(u: UpdateMsg): bool = {
    // Law 4: an addition is held only if readable at update time; every other one is refused
    // and not held, even when an earlier generation took the same id.
    val admitted = u.adds.filter(p => env.readable.get(p._2))
    val refused = u.adds.filter(p => not(env.readable.get(p._2))).map(p => p._1)
    val held = srv.keys().contains(u.sid)
    val s = if (held) srv.get(u.sid) else { gen: -1, targets: Map(), refused: Set() }
    // Law 3: removals first, then additions.
    val kept = s.targets.keys().exclude(u.rems).exclude(refused)
    val applied = {
      gen: u.gen,
      targets: setToMap(kept.map(t => (t, s.targets.get(t))).union(admitted)),
      refused: refused,
    }
    // Law 1: an unknown session opens at generation 0 only, with the additions as its whole
    // target set (`s` is empty then). Law 2: a known one applies only a newer generation.
    val reopenable = not(FORGET_IS_FINAL and ghost.forgotten.contains(u.sid))
    val applies = if (held) u.gen > s.gen else u.gen == 0 and reopenable
    val srvNext = if (applies) srv.put(u.sid, applied) else srv
    val answer =
      if (applies) Updated({ sid: u.sid, reqGen: u.gen, gen: u.gen, refused: refused })
      else if (held) Updated({ sid: u.sid, reqGen: u.gen, gen: s.gen, refused: s.refused })
      else UpdateUnknownSession({ sid: u.sid, reqGen: u.gen })
    // History check for law 2: an update the server has applied before is one whose session
    // it holds at that generation or newer, or one whose session it has forgotten (a session
    // exists only once its generation 0 applied). Re-delivering it must change nothing.
    val stale = ghost.forgotten.contains(u.sid) or (held and u.gen <= s.gen)
    all {
      srv' = srvNext,
      ghost' = { ...ghost, staleChanged: ghost.staleChanged or (stale and srvNext != srv) },
      reply(answer),
    }
  }

  action serverPage(p: { sid: int, tid: Tid, route: Route, cursor: int }): bool = {
    if (not(srv.keys().contains(p.sid))) all {
      srv' = srv,
      ghost' = ghost,
      reply(PageUnknownSession({ sid: p.sid })),
    } else {
      val s = srv.get(p.sid)
      if (not(s.targets.keys().contains(p.tid))) all {
        srv' = srv,
        ghost' = ghost,
        reply(UnknownTarget({ sid: p.sid, tid: p.tid })),
      } else {
        val r = s.targets.get(p.tid)
        // Law 7: the verdict is the route's readability now, with the rows of this snapshot.
        // The page `limit` picks `resume`, and a limit is at least one revision, so a round
        // with rows to give gives at least one (law 8 and `PageSession::limit`).
        val h = env.head.get(r)
        if (env.readable.get(r)) {
          nondet resume = (if (p.cursor < h) p.cursor + 1 else h).to(h).oneOf()
          all {
            srv' = srv,
            ghost' = ghost,
            reply(Events({
              sid: p.sid,
              route: p.route,
              srvRoute: r,
              resume: resume,
              rows: (p.cursor + 1).to(resume),
            })),
          }
        } else all {
          srv' = srv,
          ghost' = ghost,
          reply(Refused({ sid: p.sid, tid: p.tid })),
        }
      }
    }
  }

  action serverRecv(m: Msg): bool = match m {
    | Update(u) => serverUpdate(u)
    | Page(p) => serverPage(p)
    | _ => disabled
  }

  // ---------------------------------------------------------------------------------------
  // Steps

  action serverStep = { nondet m = net.oneOf() serverRecv(m) }
  action clientRecvStep = { nondet m = net.oneOf() clientRecv(m) }

  // The steps fairness is stated over. Stale requests stay in the network and the server may
  // answer them again and again, so plain fairness over "the server takes a request" could be
  // spent on those forever. Liveness only needs the client's newest update and newest round
  // per route to be answered, and the answers that change the client to be read.
  action deliverLatestUpdate = all {
    cl.lastSent != NoUpdate,
    net.contains(Update(inFlight(cl.lastSent))),
    serverUpdate(inFlight(cl.lastSent)),
  }

  action deliverLatestPage(r: Route): bool = {
    val p = { sid: cl.sid, tid: cl.believed.get(r), route: r, cursor: cl.cursor.get(r) }
    all {
      cl.believed.keys().contains(r),
      net.contains(Page(p)),
      serverPage(p),
    }
  }

  // Reading an answer that changes the client. These are finitely many, since every one of
  // them moves a counter (generation, session id, cursor) or sets a ghost.
  action recvRelevant = {
    nondet m = net.oneOf()
    all {
      received(cl, ghost, m) != (cl, ghost),
      clientRecv(m),
    }
  }
  action unblockStep = { nondet r = ROUTES.oneOf() unblock(r) }
  action sendPageStep = { nondet r = ROUTES.oneOf() sendPage(r) }

  action step = any {
    envStep,
    sendUpdate,
    retryUpdate,
    unblockStep,
    sendPageStep,
    serverStep,
    clientRecvStep,
  }

  // ---------------------------------------------------------------------------------------
  // Safety

  // Law 2: a re-delivered or stale update never changes server state.
  val law2StaleNoop = not(ghost.staleChanged)

  // Law 3: an id names one route within a session.
  val law3IdsUnique = net.forall(m1 => net.forall(m2 => match m1 {
    | Update(a) => match m2 {
      | Update(b) => a.sid == b.sid implies a.adds.forall(p => b.adds.forall(q => p._1 == q._1 implies p._2 == q._2))
      | _ => true
    }
    | _ => true
  }))

  // Laws 2 and 3: the server never holds, in the client's session, an id the client has
  // stopped tracking. A stale addition resurrecting a removed id would break this.
  val noOrphans = srv.keys().contains(cl.sid) implies
    srv.get(cl.sid).targets.keys().subseteq(
      tids(cl.believed).union(tids(cl.pendingAdd)).union(cl.pendingRem).union(cl.inflightRem)
    )

  // Law 5: a session the server forgot is never held again while the client still uses it.
  val law5NoRevival = not(ghost.forgotten.contains(cl.sid) and srv.keys().contains(cl.sid))

  // With no update in flight and an acknowledgement in hand, the client's targets are the
  // server's, except for removals the client has queued and not sent.
  val agreement = (cl.lastSent == NoUpdate and cl.acked and srv.keys().contains(cl.sid)) implies
    heldPairs(srv.get(cl.sid).targets).filter(p => not(cl.pendingRem.contains(p._1))) == entries(cl.believed)

  // Law 7: the client never holds a row of a route that did not exist while it was readable,
  // and rows are filed under the route the server read.
  val law7NoLeak = and {
    ROUTES.forall(r => cl.got.get(r).subseteq(1.to(env.exposed.get(r)))),
    not(ghost.misroute),
  }

  // Laws 8 and 9: the cursor never passes a row the client did not receive.
  val law8NoSkip = ROUTES.forall(r => 1.to(cl.cursor.get(r)).subseteq(cl.got.get(r)))

  // Law 4: refused routes stay wanted, and every wanted route is held, pending or blocked.
  val blockedStayWanted = and {
    cl.blocked.subseteq(cl.wanted),
    cl.wanted == cl.believed.keys().union(cl.pendingAdd.keys()).union(cl.blocked),
    cl.believed.keys().intersect(cl.pendingAdd.keys()) == Set(),
    cl.believed.keys().intersect(cl.blocked) == Set(),
    cl.pendingAdd.keys().intersect(cl.blocked) == Set(),
  }

  // The bounds were large enough: the client never needed a session id, target id or
  // generation the model did not have. A liveness failure in a state breaking this is an
  // artifact of the bounds, not of the protocol.
  val boundsHeld = and {
    not(ghost.exhausted),
    cl.blocked.forall(r => env.readable.get(r) implies cl.nextTid < MAX_TID),
    updateDue(cl) implies cl.gen < MAX_GEN,
  }

  // Law 6: a round never names an id the session does not hold, unless the client is
  // removing it.
  val law6NoUnknownTarget = not(ghost.bug)

  // The answer to the client's newest update names that update's generation.
  val noAhead = not(ghost.ahead)

  val safety = and {
    law2StaleNoop,
    law3IdsUnique,
    noOrphans,
    law5NoRevival,
    agreement,
    law7NoLeak,
    law8NoSkip,
    blockedStayWanted,
    law6NoUnknownTarget,
    noAhead,
  }

  // Everything but the properties a known counterexample breaks (see the README).
  val safetyCore = and {
    law3IdsUnique,
    law7NoLeak,
    law8NoSkip,
    blockedStayWanted,
    noAhead,
  }

  // Law 9's "never out of order within a target", across answers: no answer reaches the
  // client with a resume below the cursor it already has. Expected to fail when the client
  // reads the answer of a round it has already repeated; see the README.
  val law9InOrder = not(ghost.backwards)

  // ---------------------------------------------------------------------------------------
  // Liveness

  val vars = (env, cl, srv, net, ghost)

  temporal fairness = and {
    weakFair(deliverLatestUpdate, vars),
    ROUTES.forall(r => weakFair(deliverLatestPage(r), vars)),
    weakFair(recvRelevant, vars),
    weakFair(sendUpdate, vars),
    weakFair(retryUpdate, vars),
    weakFair(unblockStep, vars),
    weakFair(sendPageStep, vars),
  }

  val serverRoutes = if (srv.keys().contains(cl.sid)) heldRoutes(srv.get(cl.sid).targets) else Set()
  val wantedReadable = cl.wanted.filter(r => env.readable.get(r))

  // The server holds every wanted readable route and nothing unwanted, and the client agrees.
  // A route revoked after it was admitted stays held (law 4 admits at update time; law 7
  // refuses its rounds), so this is not an equality with the wanted readable set.
  val converged = and {
    wantedReadable.subseteq(serverRoutes),
    serverRoutes.subseteq(cl.wanted),
    cl.believed.keys() == serverRoutes,
  }

  // The equality the task statement named. Expected to fail: see the README.
  val convergedExactly = serverRoutes == wantedReadable

  val caughtUp = wantedReadable.forall(r => cl.cursor.get(r) == env.head.get(r))

  temporal liveConverges = fairness implies eventually(always(converged))
  temporal liveConvergesExactly = fairness implies eventually(always(convergedExactly))
  temporal liveCatchesUp = fairness implies eventually(always(caughtUp))
}

// Configurations. Each instance is a closed model TLC explores in full.

// Laws as written.
module laws {
  import session(
    ROUTES = Set("a", "b"),
    MAX_SID = 4,
    MAX_TID = 5,
    MAX_GEN = 6,
    MAX_HEAD = 1,
    ENV_BUDGET = 4,
    DROP_BUDGET = 1,
    RESET_ON_UNKNOWN_TARGET = false,
    FORGET_IS_FINAL = false,
  ).*
}

// The same, with the client resetting on `UnknownTarget` for a held target.
module amended {
  import session(
    ROUTES = Set("a", "b"),
    MAX_SID = 4,
    MAX_TID = 5,
    MAX_GEN = 6,
    MAX_HEAD = 1,
    ENV_BUDGET = 4,
    DROP_BUDGET = 1,
    RESET_ON_UNKNOWN_TARGET = true,
    FORGET_IS_FINAL = false,
  ).*
}

// The same, with the server never opening a session id it has forgotten.
module final {
  import session(
    ROUTES = Set("a", "b"),
    MAX_SID = 4,
    MAX_TID = 5,
    MAX_GEN = 6,
    MAX_HEAD = 1,
    ENV_BUDGET = 4,
    DROP_BUDGET = 1,
    RESET_ON_UNKNOWN_TARGET = false,
    FORGET_IS_FINAL = true,
  ).*
}

// `final` over one route, with room for rows to arrive out of order (two revisions) and one
// more environment step to append them. Laws 7 to 9 are per target, so one route is enough.
module rows {
  import session(
    ROUTES = Set("a"),
    MAX_SID = 4,
    MAX_TID = 5,
    MAX_GEN = 6,
    MAX_HEAD = 2,
    ENV_BUDGET = 5,
    DROP_BUDGET = 1,
    RESET_ON_UNKNOWN_TARGET = false,
    FORGET_IS_FINAL = true,
  ).*
}
