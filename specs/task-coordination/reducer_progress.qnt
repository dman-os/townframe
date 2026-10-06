module worker_progress {
  import worker_machine as worker from "./worker_machine"
  import coordination_fixtures as fixtures from "./coordination_fixtures"
  var capacity: int
  var state: worker::State
  var stages: int -> int
  var durable: Set[(int,int)]
  var admitted: Set[(int,int)]
  var completed: Set[(int,int)]
  var violation: bool
  pure def taskIds(cap: int): Set[int] = if (cap == 1) Set(1) else Set(1,2)
  val tasks = taskIds(capacity)
  pure val event = { kind: 0, allowed: true, session: { node: 1, incarnation: 1, session: 1 },
    protocol: 1, task: 1, allocation: 1, attempt: 1,
    ticket: fixtures::base, declaration: fixtures::declaration, hasTicket: true,
    reason: 1, token: worker::noToken, request: worker::noRequest,
    classification: 1, invocation: 1, watch: 1, fact: fixtures::fact(true,1) }
  pure def incoming(t: int,kind: int): worker::Event = { ...event, kind: kind,
    task: t, allocation: t, attempt: t,
    declaration: { ...event.declaration, task: t },
    ticket: { ...event.ticket, task: t, declaration: { ...event.ticket.declaration, task: t } } }
  pure def initial(cap: int): worker::State = taskIds(cap).fold(worker::handle(worker::fresh(cap),{ ...event, kind: 2 }).state,(s,t) => {
    val requested = worker::handle(s,incoming(t,4)).state
    val request = requested.classifications.fold(worker::noRequest,(a,p) => if (p.task == t) p else a)
    worker::handle(requested,{ ...incoming(t,16), request: request }).state
  })
  action init = {
    nondet cap = Set(1,2).oneOf()
    all { capacity' = cap, state' = initial(cap), stages' = taskIds(cap).mapBy(_ => 0), durable' = Set(),
      admitted' = Set(), completed' = Set(), violation' = false }
  }
  action advance(t: int): bool = if (not(tasks.contains(t))) all { false, capacity' = capacity, state' = state, stages' = stages, durable' = durable, admitted' = admitted, completed' = completed, violation' = violation } else {
    val stage = stages.get(t)
    val a = worker::find(state,t)
    val token = a.startEmitted.fold(worker::noToken,(a,t) => t)
    val kind = if (stage == 0) 5 else if (stage == 1) 7 else if (stage == 2) 9 else if (stage == 3) 10 else 12
    val outcome = worker::handle(state,{ ...incoming(t,kind), attempt: a.attempt, token: token, fact: { ...event.fact, attempt: a.attempt, result: t } })
    val identity = (t,a.attempt)
    all { stage < 5,
      capacity' = capacity,
      state' = outcome.state,
      stages' = stages.set(t,stage + 1),
      durable' = if (stage == 1) durable.union(Set(identity)) else durable,
      admitted' = if (stage == 2 and worker::dispatchPending(state,token)) admitted.union(Set(identity)) else admitted,
      completed' = if (stage == 3) completed.union(Set(identity)) else completed,
      violation' = (violation or outcome.panic
        or (stage == 2 and not(durable.contains(identity)))
        or (stage == 3 and not(admitted.contains(identity)))
        or (stage == 4 and not(completed.contains(identity)))) }
  }
  action step = { nondet task = tasks.oneOf(); any { advance(task), all {
    capacity' = capacity, state' = state, stages' = stages, durable' = durable, admitted' = admitted, completed' = completed, violation' = violation } } }
  val safety = not(violation) and admitted.subseteq(durable) and completed.subseteq(admitted)
  val vars = (capacity,state,stages,durable,admitted,completed,violation)
  temporal fairness = if (capacity == 1) weakFair(advance(1),vars) else weakFair(advance(1),vars) and weakFair(advance(2),vars)
  temporal progress = fairness implies eventually(state.attempts.size() == 0 and state.settled == tasks)
  temporal unfairProgress = eventually(state.attempts.size() == 0 and state.settled == tasks)
}
