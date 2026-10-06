module worker_checks {
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
  run persistenceAcknowledgementBeforeStartTest = {
    val requested = worker::handle(accepted(1),{ ...event, kind: 5 })
    assert(requested.actions == List() and worker::find(requested.state,1).startEmitted.size() == 0)
  }
  run cancellationBetweenEmissionAndAdmissionTest = {
    val emitted = dispatching(1)
    val token = worker::find(emitted,1).startEmitted.fold(worker::noToken,(a,t) => t)
    val cancelled = worker::handle(emitted,{ ...event, kind: 6 }).state
    assert(not(worker::dispatchPending(cancelled,token)))
  }
  run finishingRetainsCapacityUntilDomainAckTest = {
    val finishing = worker::handle(dispatching(1),{ ...event, kind: 10 }).state
    val remote = worker::handle(finishing,{ ...event, kind: 12, attempt: 2 }).state
    val acknowledged = worker::handle(finishing,{ ...event, kind: 12 }).state
    assert(finishing.attempts == remote.attempts and finishing.attempts.size() == 1
      and acknowledged.attempts.size() == 0 and acknowledged.settled.contains(1))
  }
  run stalePersistenceAckCannotStartReplacementTest = {
    val result = worker::handle(accepted(1),{ ...event, kind: 7, attempt: 2 })
    assert(result.state == accepted(1) and result.actions == List())
  }
  run staleClassifiedCannotAcquireNewSessionTest = {
    val pending = worker::handle(connected(1),{ ...event, kind: 4 }).state
    val request = pending.classifications.fold(worker::noRequest,(a,p) => p)
    val reconnected = worker::handle(pending,{ ...event, kind: 2, session: { ...session, session: 2 } }).state
    val result = worker::handle(reconnected,{ ...event, kind: 16, request: request })
    assert(result.state.attempts.size() == 0 and result.state.classifications.size() == 0)
  }
  run passiveSameVersionKeepsAllocationClassificationTest = {
    val pending = worker::handle(connected(1),{ ...event, kind: 4 }).state
    assert(worker::handle(pending,{ ...event, kind: 13 }).state == pending)
  }
  run deniedParticipationRetainsProducedOutcomeTest = {
    val finishing = worker::handle(dispatching(1),{ ...event, kind: 10 }).state
    val denied = worker::handle(finishing,{ ...event, kind: 1, allowed: false }).state
    assert(denied.attempts.size() == 1 and worker::handle(denied,{ ...event, kind: 12 }).state.attempts.size() == 0)
  }
  run restoreMayExceedNewCapacityHintTest = {
    val first = worker::restore(worker::fresh(1),joins::declaration,1,0,true).state
    val second = worker::restore(first,{ ...joins::declaration, task: 2 },2,1,false)
    assert(not(second.panic) and second.state.attempts.size() == 2)
  }
  run originChecksCapabilitiesAndPlacementTest = {
    val result = worker::classifyResult({ state: connected(2), actions: List(), panic: false },
      { ...event, ticket: { ...joins::base, declaration: { ...joins::declaration, capabilities: Set(2) } } },true)
    assert(result.state.attempts.size() == 0)
  }
  run localFailureIsNotGlobalTerminalTest = {
    val result = worker::handle(dispatching(1),{ ...event, kind: 11 })
    assert(result.state.attempts.size() == 0 and result.state.settled == Set()
      and result.actions.foldl(true,(ok,a) => ok and a.kind != 8))
  }
  run repeatedStartRequestDoesNotReemitDispatchTest = {
    val s = dispatching(2)
    assert(worker::handle(s,{ ...event, kind: 5 }).actions == List())
  }
  run prematureTerminalAckIsExercisableTest = {
    assert(worker::handle(accepted(1),{ ...event, kind: 12 }).panic)
  }
  var state: worker::State
  var remaining: Set[int]
  var panicObserved: bool
  action init = all { state' = connected(2), remaining' = 1.to(16), panicObserved' = false }
  action step = {
    nondet kind = remaining.union(Set(0)).oneOf()
    nondet identity = Set(1,2).oneOf()
    val request = state.classifications.fold(worker::noRequest,(a,p) => p)
    val token = worker::find(state,1).startEmitted.fold(worker::noToken,(a,t) => t)
    val result = if (kind == 0) { state: state, actions: List(), panic: false }
      else worker::handle(state,{ ...event, kind: kind, attempt: identity, request: request, token: token })
    all { state' = result.state, remaining' = remaining.exclude(Set(kind)), panicObserved' = (panicObserved or result.panic) }
  }
  val safety = state.attempts.forall(a => state.attempts.forall(b => a == b or a.task != b.task))
    and state.classifications.forall(a => state.classifications.forall(b => a == b or a.task != b.task))
}
