// Current source: immutable ExecutionGate bindings and pre-service expiry check.
// Unknown binding is intentionally fail-open in generic wflow, not authorized-job proof.
module continuation_gate_repair {
  type State = { now: int, active: bool, held: bool, bound: bool, binding: int,
    crashed: bool, pending: int, context: bool, cache: bool, aborted: bool,
    calls: Set[(int,int)], contextAt: int }
  var s: State
  pure val initial = { now: 10, active: false, held: false, bound: false, binding: 0,
    crashed: false, pending: 0, context: false, cache: false, aborted: false,
    calls: Set(), contextAt: 10 }
  action init = s' = initial
  // Profile names the host obligation to bind effective min(direct30,inherited20).
  action bind = all { not(s.active), not(s.bound), s' = { ...s, bound: true, binding: 20 } }
  action activate = all { s.bound, not(s.active), s' = { ...s, active: true } }
  action hold = all { not(s.held), s' = { ...s, held: true } }
  action release = all { s.held, s' = { ...s, held: false } }
  action wake(kind: int): bool = all { s.pending == 0, s' = { ...s, pending: kind, aborted: false } }
  // Gate permission/state/context awaits precede expiry sampling.
  action loadContext = all { s.active, not(s.held), s.pending != 0, not(s.context),
    s' = { ...s, context: true, contextAt: s.now } }
  action invoke = {
    val expired = s.bound and s.now > s.binding
    all { s.context, s.pending != 0,
      s' = { ...s, context: false, pending: 0, aborted: expired,
        cache: if (expired) false else true,
        calls: if (expired) s.calls else s.calls.union(Set((s.pending,s.now))) } }
  }
  action tick = all { s.now < 21, s' = { ...s, now: if (s.now == 10) 20 else 21 } }
  action crash = all { not(s.crashed), s' = { ...s, crashed: true, active: false,
    bound: false, binding: 0, context: false, cache: false } }
  action idle = s' = s
  action step = { nondet kind = Set(1,2,3,4,5).oneOf()
    any { bind, activate, hold, release, wake(kind), loadContext, invoke, tick, crash, idle } }
  val preActionAuthorization = s.calls.forall(x => x._2 <= 20)
  val immutableBinding = s.bound implies s.binding == 20
  val safety = preActionAuthorization and immutableBinding
  run expiryDuringContextAwaitTest = init.then(bind).then(activate).then(wake(1)).then(loadContext)
    .then(tick).then(tick).then(invoke)
    .then(all { assert(safety and s.aborted and s.calls == Set() and not(s.cache)), idle })
  run cacheResumeExpiryTest = init.then(bind).then(activate).then(wake(4)).then(loadContext).then(invoke)
    .then(wake(3)).then(loadContext).then(tick).then(tick).then(invoke)
    .then(all { assert(safety and s.aborted and not(s.cache) and s.calls == Set((4,10))), idle })
  run restoreReleaseRetainsBindingTest = init.then(bind).then(activate).then(crash).then(bind).then(hold)
    .then(tick).then(tick).then(release).then(activate).then(wake(5)).then(loadContext).then(invoke)
    .then(all { assert(safety and s.binding == 20 and s.aborted and s.calls == Set()), idle })
  run deadlineBoundaryPermitsTest = init.then(bind).then(activate).then(tick).then(wake(2)).then(loadContext).then(invoke)
    .then(all { assert(safety and not(s.aborted) and s.calls == Set((2,20))), idle })
}
