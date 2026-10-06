// Actual ExecutionGate lacks deadline check. guarded=true is required-contract
// diagnostic, not an implemented Rust gate. Reducer wake paths share RunJob.
module continuation_admission {
  type State = { guarded: bool, now: int, active: bool, held: bool,
    pending: bool, pendingKind: int, cancelled: bool, cancellationRequested: bool, crashed: bool,
    parentDeadline: int, taskDeadline: int, executions: Set[(int,int)] }
  var s: State
  pure def initial(guarded: bool): State = { guarded: guarded, now: 10, active: true,
    held: false, pending: false, pendingKind: 0, cancelled: false, cancellationRequested: false,
    crashed: false, parentDeadline: 20, taskDeadline: 30, executions: Set() }
  action init = s' = initial(true)
  action initActual = s' = initial(false)
  pure def deadline(state: State): int = if (state.parentDeadline < state.taskDeadline)
    state.parentDeadline else state.taskDeadline
  // Classes: timer, message, transient retry, step-effect continuation, restore.
  action wake(kind: int): bool = all { not(s.cancelled), not(s.pending),
    s' = { ...s, pending: true, pendingKind: kind } }
  action execute(kind: int): bool = all { s.pending, s.pendingKind == kind, s.active, not(s.held), not(s.cancelled),
    not(s.guarded) or s.now <= deadline(s),
    s' = { ...s, pending: false, executions: s.executions.union(Set((kind,s.now))) } }
  action requestCancellation = all { not(s.cancellationRequested),
    s' = { ...s, cancellationRequested: true } }
  // Cancellation request/abort effect and durable reducer incorporation are not atomic.
  action incorporateCancellation = all { s.cancellationRequested, not(s.cancelled),
    s' = { ...s, cancelled: true, pending: false } }
  action tick = all { s.now == 10, s' = { ...s, now: 21 } }
  action hold = all { not(s.held), s' = { ...s, held: true } }
  action release = all { s.held, s' = { ...s, held: false } }
  action crash = all { not(s.crashed), s' = { ...s, crashed: true, active: false } }
  action activate = all { not(s.active), s' = { ...s, active: true } }
  action idle = s' = s
  action step = { nondet kind = Set(1,2,3,4,5).oneOf()
    any { wake(kind), execute(kind), requestCancellation, incorporateCancellation, tick,
      hold, release, crash, activate, idle } }
  val preActionAuthorization = s.executions.forall(x => x._2 <= 20)
  run cancellationLagActualCounterexampleTest = initActual.then(tick).then(requestCancellation)
    .then(wake(1)).then(execute(1)).then(all { assert(not(preActionAuthorization)), idle })
  run restoredDescendantActualCounterexampleTest = initActual.then(crash).then(tick).then(activate)
    .then(wake(5)).then(execute(5)).then(all { assert(not(preActionAuthorization)), idle })
  run releasedHeldJobActualCounterexampleTest = initActual.then(hold).then(tick).then(release)
    .then(wake(3)).then(execute(3)).then(all { assert(not(preActionAuthorization)), idle })
}
