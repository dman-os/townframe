// Source owner: tasks/router.rs. Integer identities are injective classes.
module router_machine {
  import task_ticket as ticket from "./task_ticket"
  import router_slot as slot from "./router_slot"
  type Session = { node: int, incarnation: int, session: int }
  type Active = { task: int, attempt: int, allocation: int }
  type Registered = { key: Session, capabilities: Set[int], capacity: int, protocol: int, active: Set[Active] }
  type Allocation = { id: int, session: Session, task: int, attempt: int, cancel: bool }
  type Block = { session: Session, task: int, reason: int }
  type Proof = { request: int, ticket: ticket::Ticket }
  type State = { pool: int, node: int, incarnation: int, protocols: Set[int],
    lanes: slot::Slot, live: Set[(int,int,int)], own: slot::Claim, persisted: bool,
    routing: bool, edit: Set[int], sessions: Set[Registered], allocations: Set[Allocation],
    pending: Set[ticket::Ticket], blocked: Set[Block], proofSequence: int, proofs: Set[Proof], nextAllocation: int }
  // Actions carry complete evidence. reason:1 NotReady,2 Obsolete,3 Incomplete,
  // 4 Invalid,5 Unsupported,6 StaleSession,7 Unavailable.
  type Action = { kind: int, session: Session, allocation: int, attempt: int,
    task: int, reason: int, claim: slot::Claim, ticket: ticket::Ticket, request: int }
  type Result = { state: State, actions: List[Action] }
  pure val noSession = { node: 0, incarnation: 0, session: 0 }
  pure val noDeclaration = { task: 0, pool: 0, opaque: 0, producer: 0, capabilities: Set(),
    placement: 0, preferred: 0, effect: 0, before: -1, after: -1, retention: 0, retainUntil: -1 }
  pure val noTicket = ticket::fresh(noDeclaration,{ publisher: 0, envelope: 0, witness: 0 })
  pure val noAction = { kind: 0, session: noSession, allocation: 0, attempt: 0, task: 0,
    reason: 0, claim: slot::absent, ticket: noTicket, request: 0 }
  pure def fresh(capacity: int): State = { pool: 1, node: 1, incarnation: 1, protocols: Set(1),
    lanes: slot::empty, live: Set(), own: slot::absent, persisted: false, routing: false,
    edit: Set(), sessions: Set(), allocations: Set(), pending: Set(), blocked: Set(),
    proofSequence: 0, proofs: Set(), nextAllocation: 1 }
  pure def winner(s: State): slot::Claim = {
    val eligible = (slot::candidates).map(n => s.lanes.get(n)).filter(c => c.sequence >= 0 and s.edit.contains(c.candidate))
    val newest = eligible.filter(c => eligible.forall(o => c.generation >= o.generation))
    val live = newest.filter(c => s.live.contains(slot::liveKey(c)) or (s.own.sequence >= 0 and slot::liveKey(c) == slot::liveKey(s.own)))
    val candidates = if (live.size() != 0) live.filter(c => live.forall(o => c.protocol >= o.protocol)) else newest
    candidates.fold(slot::absent,(a,c) => if (a.sequence == -1 or c.rank < a.rank) c else a)
  }
  pure def reconsider(s: State): State = { ...s, routing: s.own.sequence >= 0 and s.edit.contains(s.node) and s.persisted and winner(s) == s.own }
  pure def owed(s: State, session: Registered): int = session.active.size() +
    s.allocations.filter(a => a.session == session.key and not(session.active.exists(x =>
      x.task == a.task and (if (x.allocation != 0) x.allocation == a.id else a.attempt != 0 and x.attempt == a.attempt)))).size()
  pure def candidateBefore(a: Registered,b: Registered,d: ticket::Declaration): bool = {
    val ap = d.placement == 1 and a.key.node == d.preferred
    val bp = d.placement == 1 and b.key.node == d.preferred
    if (ap != bp) ap else if (a.key.node != b.key.node) a.key.node < b.key.node
    else if (a.key.session != b.key.session) a.key.session < b.key.session
    else a.key.incarnation < b.key.incarnation
  }
  pure def allocate(r: Result): Result = {
    val tasks = List(1,2).foldl(List(),(ids,id) => if (r.state.pending.exists(t => t.task == id)) ids.append(id) else ids)
    tasks.foldl(r,(acc,id) => {
      val s = acc.state
      val t = s.pending.fold(noTicket,(a,t) => if (t.task == id) t else a)
      if (not(s.routing) or s.allocations.exists(a => a.task == id)) acc
      else if (ticket::terminal(t)) { ...acc, state: { ...s, pending: s.pending.exclude(Set(t)) } }
      else {
        val candidates = s.sessions.filter(x => (t.declaration.placement != 2 or x.key.node == t.declaration.preferred)
          and t.declaration.capabilities.subseteq(x.capabilities) and owed(s,x) < x.capacity
          and not(s.blocked.exists(b => b.session == x.key and b.task == id)))
        val selected = candidates.filter(x => candidates.forall(y => x == y or candidateBefore(x,y,t.declaration)))
        if (selected.size() == 0 or s.sessions.exists(x => x.active.exists(a => a.task == id))) acc
        else {
          val session = selected.fold(noSession,(k,x) => x.key)
          val a = { id: s.nextAllocation, session: session, task: id, attempt: 0, cancel: false }
          { state: { ...s, allocations: s.allocations.union(Set(a)), nextAllocation: s.nextAllocation + 1 },
            actions: acc.actions.append({ ...noAction, kind: 5, session: session, allocation: a.id, task: id, ticket: t }) }
        }
      }
    })
  }
  pure def cancel(r: Result, task: int): Result = {
    val s = r.state
    val allocated = s.allocations.filter(a => a.task == task)
    val origins = s.sessions.filter(x => x.active.exists(a => a.task == task)
      and not(allocated.exists(a => a.session == x.key)))
    { state: { ...s, allocations: s.allocations.map(a => if (a.task == task) { ...a, cancel: true } else a) },
      actions: allocated.fold(r.actions,(as,a) => as.append({ ...noAction, kind: 7, session: a.session, allocation: a.id, task: task }))
        .concat(origins.fold(List(),(as,x) => as.append({ ...noAction, kind: 7, session: x.key, task: task }))) }
  }
  pure def observe(r: Result,t: ticket::Ticket, obsolete: bool): Result =
    if (not(ticket::valid(t.declaration))) r
    else if (ticket::terminal(t) or obsolete) cancel({ ...r, state: { ...r.state, pending: r.state.pending.filter(x => x.task != t.task) } },t.task)
    else allocate({ ...r, state: { ...r.state, pending: r.state.pending.filter(x => x.task != t.task).union(Set(t)) } })
  pure def startPending(s: State,key: Session,id: int,task: int): bool =
    s.routing and s.allocations.exists(a => a.id == id and a.session == key and a.task == task and a.attempt != 0 and not(a.cancel))
  pure def proofPending(s: State,p: Proof): bool = s.proofs.contains(p)
  pure def offerEvidence(s: State,key: Session,id: int,task: int,d: ticket::Declaration): Set[ticket::Ticket] =
    if (s.routing and s.allocations.exists(a => a.id == id and a.session == key and a.task == task and a.attempt == 0 and not(a.cancel)))
      s.pending.filter(t => t.task == task and t.declaration == d) else Set()
  type Event = { kind: int, edit: Set[int], proof: Proof, obsolete: bool,
    claims: List[slot::Claim], key: (int,int,int), identity: slot::Claim,
    registration: Registered, session: Session, ticket: ticket::Ticket,
    task: int, allocation: int, attempt: int, reason: int, running: bool }
  // Event tags follow RouterEvent declaration order, 1..18. Claims/rank
  // identities are minted externally; counters are unbounded in this reducer.
  pure def handle(s: State,e: Event): Result = {
    val r = { state: s, actions: List() }
    if (e.kind == 1) {
      val allowed = e.edit.contains(s.node)
      val sessions = s.sessions.filter(x => allowed and e.edit.contains(x.key.node))
      { state: reconsider({ ...s, edit: e.edit, live: s.live.filter(k => e.edit.contains(k._1)),
        own: if (allowed) s.own else slot::absent, persisted: allowed and s.persisted,
        sessions: sessions, allocations: s.allocations.filter(a => sessions.exists(x => x.key == a.session)),
        blocked: s.blocked.filter(b => sessions.exists(x => x.key == b.session)),
        pending: if (allowed) s.pending else s.pending.union(s.proofs.map(p => p.ticket)),
        proofs: if (allowed) s.proofs else Set() }), actions: List() }
    } else if (e.kind == 2) {
      if (not(s.proofs.contains(e.proof))) r else
        observe({ ...r, state: { ...s, proofs: s.proofs.exclude(Set(e.proof)) } },e.proof.ticket,e.obsolete)
    } else if (e.kind == 3) {
      val merged = e.claims.foldl(r,(acc,c) => {
        val m = if (c.pool == s.pool) slot::merge(acc.state.lanes,c)
          else { slot: acc.state.lanes, error: 3, changed: false }
        if (m.error != 0) { ...acc, actions: acc.actions.append({ ...noAction, kind: 8, claim: c, reason: m.error }) }
        else { ...acc, state: { ...acc.state, lanes: m.slot } }
      })
      { ...merged, state: reconsider(merged.state) }
    } else if (e.kind == 4) {
      if (not(s.edit.contains(e.key._1))) r else
        { ...r, state: reconsider({ ...s, live: s.live.union(Set(e.key)) }) }
    } else if (e.kind == 5) { ...r, state: reconsider({ ...s, live: s.live.exclude(Set(e.key)) }) }
    else if (e.kind == 6) {
      val w = winner(s)
      val live = w.sequence >= 0 and (s.live.contains(slot::liveKey(w))
        or (s.own.sequence >= 0 and slot::liveKey(w) == slot::liveKey(s.own)))
      if (s.routing or not(s.edit.contains(s.node)) or live) r else {
        val seq = if (s.lanes.get(s.node).sequence < 0) 0 else s.lanes.get(s.node).sequence
        val c = { ...e.identity, pool: s.pool, candidate: s.node, incarnation: s.incarnation,
          sequence: (if (seq > s.own.sequence) seq else s.own.sequence) + 1,
          generation: slot::maxGeneration(s.lanes) + 1,
          protocol: s.protocols.fold(0,(a,p) => if (p > a) p else a),
          observed: (slot::candidates).mapBy(n => s.lanes.get(n).generation) }
        val m = slot::merge(s.lanes,c)
        { state: { ...s, own: c, persisted: false, lanes: m.slot },
          actions: List({ ...noAction, kind: if (m.error == 0) 2 else 8, claim: c, reason: m.error }) }
      }
    } else if (e.kind == 7) {
      val stored = s.lanes.get(s.node)
      if (s.own.sequence == -1 or s.own.generation != e.key._1 or s.own.claim != e.key._2
        or { ...stored, sequence: s.own.sequence } != s.own) r
      else { ...r, state: reconsider({ ...s, own: stored, persisted: true }) }
    } else if (e.kind == 8) {
      if (s.persisted or s.own.generation != e.key._1 or s.own.claim != e.key._2) r
      else { ...r, state: { ...s, own: slot::absent, routing: false,
        live: s.live.exclude(Set(slot::liveKey(s.own))) } }
    } else if (e.kind == 9) {
      if (s.routing and s.own.sequence >= 0) { ...r, actions: List({ ...noAction, kind: 3, claim: s.own }) } else r
    } else if (e.kind == 10) {
      val x = e.registration
      val reason = if (not(s.edit.contains(s.node)) or not(s.edit.contains(x.key.node))) 7
        else if (not(s.protocols.contains(x.protocol))) 5 else 0
      if (reason != 0) { ...r, actions: List({ ...noAction, kind: 4, session: x.key, reason: reason }) }
      else {
        val changed = s.sessions.exists(y => y.key == x.key and (y.capacity != x.capacity or y.active != x.active))
        allocate({ ...r, state: { ...s, sessions: s.sessions.filter(y => y.key != x.key).union(Set(x)),
          blocked: s.blocked.filter(b => not(changed and b.session == x.key and b.reason == 7)) } })
      }
    } else if (e.kind == 11) allocate({ ...r, state: { ...s,
      sessions: s.sessions.filter(x => x.key != e.session), allocations: s.allocations.filter(a => a.session != e.session),
      blocked: s.blocked.filter(b => b.session != e.session) } })
    else if (e.kind == 12) {
      val cleared = { ...s, proofs: s.proofs.filter(p => p.ticket.task != e.ticket.task) }
      if (ticket::terminal(e.ticket)) observe({ ...r, state: cleared },e.ticket,false) else {
        val p = { request: s.proofSequence + 1, ticket: e.ticket }
        { state: { ...cleared, proofSequence: p.request, pending: s.pending.filter(t => t.task != e.ticket.task),
          proofs: cleared.proofs.union(Set(p)) },
          actions: List({ ...noAction, kind: 1, task: e.ticket.task, ticket: e.ticket, request: p.request }) }
      }
    } else if (e.kind == 13) cancel({ ...r, state: { ...s, proofs: s.proofs.filter(p => p.ticket.task != e.task),
      pending: s.pending.filter(t => t.task != e.task) } },e.task)
    else if (e.kind == 14) {
      val found = s.allocations.filter(a => a.id == e.allocation and a.session == e.session and (a.attempt == 0 or a.attempt == e.attempt))
      if (not(s.routing) or found.size() == 0) r else {
        val a = found.fold({ id: 0, session: noSession, task: 0, attempt: 0, cancel: false },(a,x) => x)
        { state: { ...s, allocations: s.allocations.exclude(found).union(Set({ ...a, attempt: e.attempt })) },
          actions: List({ ...noAction, kind: if (a.cancel) 7 else 6, session: a.session, allocation: a.id, task: a.task, attempt: e.attempt }) }
      }
    } else if (e.kind == 15) {
      val found = s.allocations.filter(a => a.id == e.allocation and a.session == e.session and a.attempt == 0)
      if (found.size() == 0) r else {
        val blocks = if (Set(1,2,3,4,7).contains(e.reason)) found.map(a => { session: a.session, task: a.task, reason: e.reason }) else Set()
        allocate({ ...r, state: { ...s, allocations: s.allocations.exclude(found),
          blocked: s.blocked.filter(b => not(blocks.exists(x => x.session == b.session and x.task == b.task))).union(blocks) } })
      }
    } else r
  }
}
