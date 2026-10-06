// Additional depth8 arbitrary repeated-event/old-attempt profile, not a replacement for original full finite one-occurrence graph.
module worker_repeated_checks {
  import worker_machine as worker from "./worker_machine"
  import coordination_fixtures as joins from "./coordination_fixtures"
  pure val session = { node: 1, incarnation: 1, session: 1 }
  pure val event = { kind: 0, allowed: true, session: session, protocol: 1, task: 1,
    allocation: 1, attempt: 1, ticket: joins::base, declaration: joins::declaration,
    hasTicket: true, reason: 1, token: worker::noToken, request: worker::noRequest,
    classification: 1, invocation: 1, watch: 1, fact: joins::fact(true,1) }
  pure def connected(cap: int): worker::State = worker::handle(worker::fresh(cap),{ ...event, kind: 2 }).state
  pure def accepted(cap: int): worker::State = {
    val requested = worker::handle(connected(cap),{ ...event, kind: 4 }).state
    val request = requested.classifications.fold(worker::noRequest,(a,p) => p)
    worker::handle(requested,{ ...event, kind: 16, request: request }).state
  }
  pure def dispatching(cap: int): worker::State = {
    val started = worker::handle(accepted(cap),{ ...event, kind: 5 }).state
    worker::handle(started,{ ...event, kind: 7 }).state
  }
  var state: worker::State
  var remaining: int
  var panicObserved: bool
  action init = all { state' = connected(2), remaining' = 8, panicObserved' = false }
  action step = if (remaining == 0) all { state' = state, remaining' = remaining, panicObserved' = panicObserved } else {
    nondet kind = 0.to(16).oneOf()
    nondet identity = Set(1,2).oneOf()
    val request = state.classifications.fold(worker::noRequest,(a,p) => p)
    val token = worker::find(state,1).startEmitted.fold(worker::noToken,(a,t) => t)
    val result = if (kind == 0) { state: state, actions: List(), panic: false }
      else worker::handle(state,{ ...event, kind: kind, attempt: identity, request: request, token: token })
    all { remaining > 0, state' = result.state, remaining' = remaining - 1, panicObserved' = (panicObserved or result.panic) }
  }
  val safety = state.attempts.forall(a => a.startEmitted.size() == 0 or a.persisted)
    and state.attempts.forall(a => state.attempts.forall(b => a == b or a.task != b.task))
    and state.classifications.forall(a => state.classifications.forall(b => a == b or a.task != b.task))
}
