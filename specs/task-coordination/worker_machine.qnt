// Source owner: tasks/worker.rs. Emission and native admission are distinct.
module worker_machine {
  import task_ticket as ticket from "./task_ticket"
  import coordination_identity as identity from "./coordination_identity"
  type Token = { task: int, attempt: int, declaration: ticket::Declaration,
    allocation: int, session: identity::Session }
  type Attempt = { task: int, attempt: int, declaration: ticket::Declaration,
    invocation: int, allocation: int, persistEmitted: bool, persisted: bool,
    startRequested: bool, startEmitted: Set[Token], running: bool,
    cancel: int, finishing: Set[ticket::Fact] }
  type Request = { request: int, task: int, ticket: ticket::Ticket,
    session: identity::Session, allocation: int, original: int,
    originalTask: int, declaration: ticket::Declaration }
  type Wait = { task: int, watch: int }
  type Handed = { task: int, fact: ticket::Fact }
  type State = { node: int, incarnation: int, pool: int, capabilities: Set[int],
    capacity: int, protocols: Set[int], current: identity::Session, participating: bool,
    attempts: Set[Attempt], blocked: Set[Wait], settled: Set[int], handed: Set[Handed],
    classSequence: int, classifications: Set[Request], nextAttempt: int }
  type Action = { kind: int, task: int, attempt: int, allocation: int,
    session: identity::Session, reason: int, declaration: ticket::Declaration,
    invocation: int, token: Token, request: Request, fact: ticket::Fact,
    active: Set[identity::Active], watch: int }
  type Result = { state: State, actions: List[Action], panic: bool }
  // kind:1 ParticipationChanged,2 RouterDiscovered,3 RouterLost,4 OfferReceived,
  // 5 StartReceived,6 CancelReceived,7 AttemptPersisted,8 PersistFailed,
  // 9 DispatchStarted,10 AttemptFinished,11 AttemptFailed,12 TerminalFactAccepted,
  // 13 TicketObserved,14 ReadinessWakeup,15 OriginStart,16 Classified.
  type Event = { kind: int, allowed: bool, session: identity::Session, protocol: int,
    task: int, allocation: int, attempt: int, ticket: ticket::Ticket, declaration: ticket::Declaration,
    hasTicket: bool, reason: int, token: Token, request: Request,
    classification: int, invocation: int, watch: int, fact: ticket::Fact }
  // classification:1 Runnable,2 NotReady,3 Obsolete,4 Incomplete,5 Invalid.
  // invocation is complete captured argument equality class; 0 means absent.
  pure val noToken = { task: 0, attempt: 0, declaration: identity::noDeclaration,
    allocation: 0, session: identity::noSession }
  pure val noRequest = { request: 0, task: 0, ticket: identity::noTicket,
    session: identity::noSession, allocation: 0, original: 0, originalTask: 0, declaration: identity::noDeclaration }
  pure val absent = { task: 0, attempt: 0, declaration: identity::noDeclaration,
    invocation: 0, allocation: 0, persistEmitted: false, persisted: false,
    startRequested: false, startEmitted: Set(), running: false, cancel: 0, finishing: Set() }
  pure val noAction = { kind: 0, task: 0, attempt: 0, allocation: 0, session: identity::noSession,
    reason: 0, declaration: identity::noDeclaration, invocation: 0, token: noToken,
    request: noRequest, fact: ticket::noFact, active: Set(), watch: 0 }
  pure def fresh(capacity: int): State = { node: 1, incarnation: 1, pool: 1,
    capabilities: Set(1), capacity: capacity, protocols: Set(1), current: identity::noSession,
    participating: true, attempts: Set(), blocked: Set(), settled: Set(), handed: Set(),
    classSequence: 0, classifications: Set(), nextAttempt: 1 }
  pure def find(s: State,task: int): Attempt = s.attempts.fold(absent,(a,x) => if (x.task == task) x else a)
  pure def replace(s: State,a: Attempt): State = { ...s, attempts: s.attempts.filter(x => x.task != a.task).union(Set(a)) }
  pure def remove(s: State,task: int): State = { ...s, attempts: s.attempts.filter(x => x.task != task) }
  pure def dispatchPending(s: State,t: Token): bool = s.attempts.exists(a => a.task == t.task
    and a.startEmitted.contains(t) and a.cancel == 0 and a.finishing.size() == 0)
  pure def sameRequest(a: Request,b: Request): bool =
    (a.request,a.task,a.ticket,a.session,a.allocation) == (b.request,b.task,b.ticket,b.session,b.allocation)
  pure def classificationPending(s: State,p: Request): bool = s.classifications.exists(a => sameRequest(a,p))
  pure def emit(r: Result,a: Action): Result = { ...r, actions: r.actions.append(a) }
  pure def report(r: Result,a: Attempt,status: int): Result = if (r.state.current == identity::noSession) r
    else emit(r,{ ...noAction, kind: 7, task: a.task, attempt: a.attempt, session: r.state.current, reason: status })
  pure def cancel(r: Result,task: int,reason: int): Result = {
    val a = find(r.state,task)
    if (a.attempt == 0) r else {
      val stopped = { ...a, cancel: reason, startEmitted: if (a.persisted) Set() else a.startEmitted }
      val changed = { ...r, state: replace(r.state,stopped) }
      if (a.persisted) emit(changed,{ ...noAction, kind: 6, task: task, attempt: a.attempt, reason: reason }) else changed
    }
  }
  pure def start(r: Result,task: int): Result = {
    val a = find(r.state,task)
    if (a.attempt == 0 or a.running or a.startEmitted.size() != 0 or not(a.persisted) or not(a.startRequested)) r
    else {
      val t = { task: a.task, attempt: a.attempt, declaration: a.declaration, allocation: a.allocation, session: r.state.current }
      // Source helpers do not test cancellation/finishing. Native admission does.
      emit({ ...r, state: replace(r.state,{ ...a, startEmitted: Set(t), invocation: 0 }), panic: r.panic or a.invocation == 0 },
        { ...noAction, kind: 5, task: a.task, attempt: a.attempt, allocation: a.allocation,
          declaration: a.declaration, invocation: a.invocation, token: t })
    }
  }
  pure def persist(r: Result,task: int): Result = {
    val a = find(r.state,task)
    if (a.attempt == 0) r else if (a.persistEmitted) start(r,task) else
      emit({ ...r, state: replace(r.state,{ ...a, persistEmitted: true }), panic: r.panic or a.invocation == 0 },
        { ...noAction, kind: 4, task: task, attempt: a.attempt, allocation: a.allocation,
          declaration: a.declaration, invocation: a.invocation })
  }
  pure def retire(r: Result,reason: int): Result = r.state.attempts.filter(a => a.allocation != 0).fold(r,(acc,a) => {
    val origin = { ...a, allocation: 0 }
    if (not(a.startRequested) and not(a.running)) {
      if (a.persisted) emit({ ...acc, state: remove(acc.state,a.task) },{ ...noAction, kind: 6, task: a.task, attempt: a.attempt, reason: reason })
      else if (not(a.persistEmitted)) { ...acc, state: remove(acc.state,a.task) }
      else { ...acc, state: replace(acc.state,{ ...origin, cancel: reason }) }
    } else { ...acc, state: replace(acc.state,origin) }
  })
  pure def wait(r: Result,task: int,watch: int): Result = {
    val w = { task: task, watch: watch }
    if (r.state.blocked.contains(w)) r else {
      val changed = { ...r, state: { ...r.state, blocked: r.state.blocked.filter(x => x.task != task).union(Set(w)) } }
      if (watch == 0) changed else emit(changed,{ ...noAction, kind: 10, task: task, watch: watch })
    }
  }
  pure def wake(r: Result,task: int): Result = {
    val had = r.state.blocked.exists(x => x.task == task)
    val changed = { ...r, state: { ...r.state, blocked: r.state.blocked.filter(x => x.task != task) } }
    if (had and r.state.current != identity::noSession) emit(changed,{ ...noAction, kind: 9, task: task, session: r.state.current }) else changed
  }
  pure def decline(r: Result,e: Event,reason: int): Result = emit(r,{ ...noAction,
    kind: 3, task: e.task, allocation: e.allocation, session: e.session, reason: reason })
  pure def classifyResult(r: Result,e: Event,origin: bool): Result = {
    val s = r.state
    val t = e.ticket
    val d = if (origin) t.declaration else e.declaration
    val existing = find(s,e.task).attempt != 0
    val invalid = d.task != e.task or d.pool != s.pool or not(ticket::valid(d)) or (e.hasTicket and (t.task != e.task or t.declaration != d))
    if (origin and not(ticket::valid(d))) { ...r, panic: true }
    else if (not(origin) and e.session != s.current) decline(r,e,6)
    else if (existing or s.attempts.size() >= s.capacity) { if (origin) r else decline(r,e,7) }
    else if (not(origin) and invalid) decline(r,e,4)
    else if (origin and (s.settled.contains(e.task) or (d.placement == 2 and d.preferred != s.node)
      or not(d.capabilities.subseteq(s.capabilities)))) r
    else {
      val c = if (not(origin) and not(e.hasTicket)) 4 else if (s.settled.contains(e.task)) 3 else e.classification
      if (c == 1) {
        val a = { ...absent, task: e.task, attempt: s.nextAttempt, declaration: d, invocation: e.invocation,
          allocation: if (origin) 0 else e.allocation, startRequested: origin }
        val inserted = { ...r, state: replace({ ...s, nextAttempt: s.nextAttempt + 1 },a) }
        val accepted = if (origin) inserted else emit(inserted,{ ...noAction, kind: 2,
          task: e.task, attempt: a.attempt, allocation: e.allocation, session: e.session })
        persist(accepted,e.task)
      } else if (c == 2) { val blocked = wait(r,e.task,e.watch); if (origin) blocked else decline(blocked,e,1) }
      else if (c == 4) { val blocked = wait(r,e.task,0); if (origin) blocked else decline(blocked,e,3) }
      else if (c == 3) {
        val settled = { ...r, state: { ...s, settled: s.settled.union(Set(e.task)) } }
        if (origin) settled else decline(settled,e,2)
      } else if (origin) r else decline(r,e,4)
    }
  }
  pure def observe(r: Result,e: Event): Result = {
    val t = e.ticket
    if (not(ticket::valid(t.declaration))) r else if (ticket::terminal(t)) {
      val fact = ticket::summary(t)
      val old = r.state.handed.filter(x => x.task == t.task)
      val hand = not(old.exists(x => x.fact == fact or (x.fact.success and not(fact.success))))
      val settled = { ...r, state: { ...r.state, settled: r.state.settled.union(Set(t.task)),
        handed: if (hand) r.state.handed.filter(x => x.task != t.task).union(Set({ task: t.task, fact: fact })) else r.state.handed } }
      val handed = if (hand) emit(settled,{ ...noAction, kind: 8, task: t.task, fact: fact }) else settled
      cancel(handed,t.task,1)
    } else if (r.state.settled.contains(t.task)) cancel(r,t.task,1)
    else if (e.classification == 3) cancel({ ...r, state: { ...r.state, settled: r.state.settled.union(Set(t.task)) } },t.task,2)
    else if (e.classification == 2) wait(r,t.task,e.watch)
    else if (e.classification == 1) wake(r,t.task)
    else if (e.classification == 4) wait(r,t.task,0) else r
  }
  pure def apply(s: State,e: Event): Result = {
    val r = { state: s, actions: List(), panic: false }
    val a = find(s,if (e.kind == 9) e.token.task else e.task)
    val exact = a.attempt != 0 and a.attempt == e.attempt
    if (e.kind == 2) {
      if (e.session.node != s.node or e.session.incarnation != s.incarnation) { ...r, panic: true }
      else if (not(s.protocols.contains(e.protocol))) emit(retire({ ...r, state: { ...s, current: identity::noSession } },3),
        { ...noAction, kind: 12, session: e.session, reason: e.protocol })
      else {
        val retired = if (s.current != e.session) retire(r,3) else r
        val changed = { ...retired, state: { ...retired.state, current: e.session } }
        emit(changed,{ ...noAction, kind: 1, session: e.session,
          active: changed.state.attempts.filter(x => x.persistEmitted).map(x => { task: x.task, attempt: x.attempt, allocation: x.allocation }) })
      }
    } else if (e.kind == 3) {
      if (s.current != e.session) r else retire({ ...r, state: { ...s, current: identity::noSession } },3)
    } else if (e.kind == 4) classifyResult(r,e,false)
    else if (e.kind == 5) {
      if (s.current != e.session or a.attempt == 0 or a.allocation != e.allocation) r
      else persist({ ...r, state: replace(s,{ ...a, startRequested: true }) },e.task)
    } else if (e.kind == 6) { if (s.current == e.session) cancel(r,e.task,e.reason) else r }
    else if (e.kind == 7) {
      if (not(exact)) r else {
        val acked = { ...r, state: replace(s,{ ...a, persisted: true }) }
        if (a.cancel != 0) emit(acked,{ ...noAction, kind: 6, task: a.task, attempt: a.attempt, reason: a.cancel })
        else if (a.allocation == 0) start(acked,e.task) else persist(acked,e.task)
      }
    } else if (e.kind == 8) {
      if (not(exact)) r else emit(report({ ...r, state: remove(s,e.task) },a,2),{ ...noAction, kind: 6, task: a.task, attempt: a.attempt, reason: e.reason })
    } else if (e.kind == 9) {
      if (not(dispatchPending(s,e.token)) or a.running) r else report({ ...r, state: replace(s,{ ...a, running: true }) },a,1)
    } else if (e.kind == 10) {
      if (not(exact) or a.finishing.size() != 0) r else {
        val previous = s.handed.filter(x => x.task == e.task)
        val already = s.settled.contains(e.task) and previous.exists(x => x.fact == e.fact or (x.fact.success and not(e.fact.success)))
        if (already) report({ ...r, state: remove(s,e.task) },a,if (e.fact.success) 3 else 4)
        else emit({ ...r, state: { ...replace(s,{ ...a, finishing: Set(e.fact) }),
          handed: s.handed.filter(x => x.task != e.task).union(Set({ task: e.task, fact: e.fact })) } },
          { ...noAction, kind: 8, task: e.task, attempt: e.attempt, fact: e.fact })
      }
    } else if (e.kind == 11) {
      if (not(exact) or a.finishing.size() != 0) r else report({ ...r, state: remove(s,e.task) },a,2)
    } else if (e.kind == 12) {
      if (not(exact)) r else if (a.finishing.size() == 0) { ...r, panic: true }
      else {
        val success = a.finishing.exists(f => f.success)
        report({ ...r, state: { ...remove(s,e.task), settled: if (success) s.settled.union(Set(e.task)) else s.settled } },a,if (success) 3 else 4)
      }
    } else if (e.kind == 13) observe(r,e)
    else if (e.kind == 14) wake(r,e.task)
    else if (e.kind == 15) classifyResult(r,e,true) else r
  }
  pure def handle(s: State,e: Event): Result = {
    val r = { state: s, actions: List(), panic: false }
    if (e.kind == 1) {
      if (e.allowed) { ...r, state: { ...s, participating: true } }
      else {
        val retired = retire({ ...r, state: { ...s, participating: false, current: identity::noSession, classifications: Set() } },4)
        retired.state.attempts.filter(a => a.finishing.size() == 0).fold(retired,(acc,a) => cancel(acc,a.task,4))
      }
    } else if (not(s.participating) and not(Set(7,8,10,11,12).contains(e.kind))) r
    else if (e.kind == 16) {
      if (not(classificationPending(s,e.request))) r else {
        val expected = s.classifications.fold(noRequest,(a,p) => if (sameRequest(p,e.request)) p else a)
        val cleared = { ...s, classifications: s.classifications.exclude(Set(expected)) }
        if (expected.session != identity::noSession and expected.session != s.current) { ...r, state: cleared }
        else apply(cleared,{ ...e, kind: expected.original, task: expected.originalTask, ticket: expected.ticket,
          declaration: expected.declaration, session: expected.session, allocation: expected.allocation, hasTicket: true })
      }
    } else {
      val normalized = if (e.kind == 15 and ticket::terminal(e.ticket)) { ...e, kind: 13 } else e
      val passivePreserved = normalized.kind == 13 and not(ticket::terminal(e.ticket))
        and s.classifications.exists(p => p.task == e.ticket.task and p.ticket == e.ticket and p.allocation != 0)
      val cleared = if (normalized.kind == 3) { ...s, classifications: s.classifications.filter(p => p.session != e.session) }
        else if ((normalized.kind == 6 and e.session == s.current) or (normalized.kind == 13 and ticket::terminal(e.ticket)))
          { ...s, classifications: s.classifications.filter(p => p.task != (if (normalized.kind == 13) e.ticket.task else e.task)) } else s
      val request = (normalized.kind == 4 and e.session == s.current and e.hasTicket)
        or (normalized.kind == 13 and not(ticket::terminal(e.ticket))) or normalized.kind == 15
      if (passivePreserved) r else if (request) {
        val p = { request: cleared.classSequence + 1, task: e.ticket.task, ticket: e.ticket,
          session: if (normalized.kind == 4) e.session else identity::noSession,
          allocation: if (normalized.kind == 4) e.allocation else 0, original: normalized.kind,
          originalTask: if (normalized.kind == 4) e.task else e.ticket.task, declaration: e.declaration }
        val superseded = cleared.classifications.filter(x => x.task == p.task and x.session != identity::noSession and x.allocation != 0)
        val changed = { ...r, state: { ...cleared, classSequence: p.request,
          classifications: cleared.classifications.filter(x => x.task != p.task).union(Set(p)) } }
        val declined = superseded.fold(changed,(acc,x) => emit(acc,{ ...noAction, kind: 3,
          task: x.task, session: x.session, allocation: x.allocation, reason: 7 }))
        emit(declined,{ ...noAction, kind: 11, task: p.task, request: p })
      } else apply(cleared,normalized)
    }
  }
  pure def restore(s: State,d: ticket::Declaration,id: int,invocation: int,running: bool): Result = {
    val invalid = s.current != identity::noSession or d.pool != s.pool or not(ticket::valid(d))
      or (d.placement == 2 and d.preferred != s.node) or s.settled.contains(d.task)
      or find(s,d.task).attempt != 0 or s.attempts.exists(a => a.allocation == 0 and a.attempt == id)
    val a = { ...absent, task: d.task, attempt: id, declaration: d, invocation: invocation,
      persistEmitted: true, persisted: true, startRequested: true, running: running }
    { state: if (invalid) s else replace(s,a), actions: List(), panic: invalid }
  }
}
