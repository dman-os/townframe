// Collision-free equality classes, NOT a cryptographic hash/serialization proof.
// Source: rt/triage/origins.rs and rt/task_adapter.rs processor classification.
module successor_authorization {
  type Identity = (int,int,int,int,int)
  type Authorization = { task: Identity, nonce: int, origin: int, deadline: int }
  pure def identity(slot: int, capture: int, a: Authorization): Identity =
    (slot,capture,a.nonce,a.origin,a.deadline)
  pure val original: Authorization = { task: (1,1,1,10,20), nonce: 1, origin: 10, deadline: 20 }
  // Trusted is the conjunction of authenticated canonical-input equality,
  // proven capture/provenance/publisher rights/pool and historical config witness.
  // It is NOT established by decoding bytes or finding a local origin DB row.
  pure def accepted(slot: int, capture: int, a: Authorization,
      declarationTask: Identity, declarationDeadline: int, lifetime: int, trusted: bool): bool =
    trusted and identity(slot,capture,a) == declarationTask
      and a.task == declarationTask and a.deadline == declarationDeadline
      and a.deadline - a.origin == lifetime
  pure def reuseAt(a: Authorization, now: int, lifetime: int): bool = now <= a.deadline and a.deadline - a.origin == lifetime
  pure def check(a: Authorization): bool = accepted(1,1,a,original.task,20,10,true)
  run slotBindingTest = assert(not(accepted(2,1,original,original.task,20,10,true)))
  run captureBindingTest = assert(not(accepted(1,2,original,original.task,20,10,true)))
  run nonceBindingTest = assert(not(check({ ...original, nonce: 2 })))
  run originBindingTest = assert(not(accepted(1,1,{ ...original, origin: 11 },original.task,20,9,true)))
  run deadlineBindingTest = assert(not(accepted(1,1,{ ...original, deadline: 21 },original.task,21,11,true)))
  run authorizationTaskBindingTest = assert(not(check({ ...original, task: (1,1,2,10,20) })))
  run declarationTaskBindingTest = assert(not(accepted(1,1,{ ...original, task: (1,1,2,10,20) },(1,1,2,10,20),20,10,true)))
  run declarationDeadlineBindingTest = assert(not(accepted(1,1,original,original.task,21,10,true)))
  run historicalLifetimeBindingTest = assert(not(accepted(1,1,original,original.task,20,11,true)))
  run unverifiedBytesAreNotAuthorizationTest = assert(not(accepted(1,1,original,original.task,20,10,false)))
  run lastAuthorizedSecondReplayTest = assert(reuseAt(original,20,10) and not(reuseAt(original,21,10)))
  run changedLifetimeRenewsBeforeExpiryTest = assert(reuseAt(original,15,10) and not(reuseAt(original,15,11)))
  run validSuccessorAndFutureOriginBoundaryTest = {
    val successor = { task: (1,1,2,21,31), nonce: 2, origin: 21, deadline: 31 }
    assert(check(original))
      .then(assert(successor.task != original.task))
      .then(assert(accepted(1,1,successor,successor.task,31,10,true)))
    // No now/origin bound is checked here in source. Fresh authenticated input
    // can choose a future origin; finite duration is not a clock-skew proof.
  }
  var durable: List[Authorization]
  var current: Authorization
  var lifetime: int
  var stage: int
  action init = all { durable' = List(original), current' = original, lifetime' = 10, stage' = 0 }
  action replayReopen = all { stage == 0, reuseAt(current,15,10), durable' = durable, current' = durable.nth(0), lifetime' = lifetime, stage' = 1 }
  // Caller already proved renewed unmet-capture decision and now > old deadline.
  // Fresh nonce/collision-free ID is a randomness contract; SQLite transaction
  // serializes selection+insertion. This model does not prove SQLite concurrency.
  action renew = all { stage == 1, not(reuseAt(current,21,10)), lifetime' = lifetime,
    current' = { task: (1,1,2,21,31), nonce: 2, origin: 21, deadline: 31 },
    durable' = durable.append({ task: (1,1,2,21,31), nonce: 2, origin: 21, deadline: 31 }), stage' = 2 }
  // Changed policy duration renews the same unmet capture even before expiry.
  action changedLifetime = all { stage == 1, not(reuseAt(current,15,11)),
    lifetime' = 11, current' = { task: (1,1,2,15,26), nonce: 2, origin: 15, deadline: 26 },
    durable' = durable.append({ task: (1,1,2,15,26), nonce: 2, origin: 15, deadline: 26 }), stage' = 2 }
  action successorReopen = all { stage == 2, durable' = durable, current' = durable.nth(1), lifetime' = lifetime, stage' = 3 }
  action done = all { stage == 3, durable' = durable, current' = current, lifetime' = lifetime, stage' = stage }
  action step = any { replayReopen, renew, changedLifetime, successorReopen, done }
  val safety = durable.nth(0) == original
    and accepted(1,1,current,current.task,current.deadline,lifetime,true)
    and (stage < 2 implies current == original)
    and (stage >= 2 implies current.task != original.task)
}
