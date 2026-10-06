// Source owner: rt/dispatch.rs descendant witness + rt.rs host expiry fences.
module descendant_authorization {
  pure val ids = Set(1,2,3)
  type State = { now: int, online: bool, crashed: bool, caller: bool,
    queued: Set[int], admitted: Set[int], parents: int -> int, deadlines: int -> int,
    pending: Set[int], expiryMarked: Set[int], cancellation: Set[int], publishing: Set[int], running: Set[int],
    status: int -> int, starts: Set[(int,int)], successes: Set[(int,int)], effects: Set[(int,int)] }
  var s: State
  pure val initial = { now: 10, online: true, crashed: false, caller: true,
    queued: Set(), admitted: Set(1), parents: ids.mapBy(_ => 0), deadlines: ids.mapBy(_ => 20),
    pending: Set(1), expiryMarked: Set(), cancellation: Set(), publishing: Set(), running: Set(),
    status: ids.mapBy(_ => 0), starts: Set(), successes: Set(), effects: Set() }
  action init = s' = initial
  action enqueue(child: int): bool = all { s.online, s.caller, child != 1,
    not(s.admitted.contains(child)), not(s.queued.contains(child)),
    s' = { ...s, queued: s.queued.union(Set(child)) } }
  // Repository worker owns caller-independent SQL admission. Same transaction
  // rolls back dispatch insertion when parent is terminal or deadline expired.
  action admit(child: int): bool = {
    val parent = child - 1
    all { s.online, s.queued.contains(child), s.admitted.contains(parent), s.status.get(parent) == 0,
      s.now <= s.deadlines.get(parent),
      s' = { ...s, queued: s.queued.exclude(Set(child)), admitted: s.admitted.union(Set(child)),
        parents: s.parents.set(child,parent), deadlines: s.deadlines.set(child,s.deadlines.get(parent)),
        pending: s.pending.union(Set(child)) } }
  }
  action callerCancel = all { s.caller, s' = { ...s, caller: false } }
  action start(id: int): bool = all { s.online, s.admitted.contains(id), s.status.get(id) == 0,
    not(s.running.contains(id)), not(s.cancellation.contains(id)), s.now <= s.deadlines.get(id),
    s' = { ...s, running: s.running.union(Set(id)), starts: s.starts.union(Set((id,s.now))) } }
  // Exact publication CAS: requested cancellation after a claim cannot own it.
  action claim(id: int): bool = all { s.online, s.running.contains(id), not(s.publishing.contains(id)),
    not(s.cancellation.contains(id)), s' = { ...s, publishing: s.publishing.union(Set(id)) } }
  action cancel(id: int): bool = all { s.online, s.admitted.contains(id), s.status.get(id) == 0,
    not(s.cancellation.contains(id)),
    s' = { ...s, expiryMarked: s.expiryMarked.union(Set(id)), cancellation: if (s.publishing.contains(id)) s.cancellation else s.cancellation.union(Set(id)) } }
  // Managed effect fence independent of an earlier publication claim.
  action publish(id: int): bool = all { s.online, s.publishing.contains(id), s.status.get(id) == 0,
    s.now <= s.deadlines.get(id), not(s.effects.contains((id,s.now))),
    s' = { ...s, effects: s.effects.union(Set((id,s.now))) } }
  action complete(id: int): bool = {
    val success = not(s.cancellation.contains(id)) and s.now <= s.deadlines.get(id)
    all { s.online, s.admitted.contains(id), s.status.get(id) == 0,
      s' = { ...s, status: s.status.set(id,if (success) 2 else 3), pending: s.pending.exclude(Set(id)),
        running: s.running.exclude(Set(id)), publishing: s.publishing.exclude(Set(id)),
        successes: if (success) s.successes.union(Set((id,s.now))) else s.successes } }
  }
  // LIMIT 1 finite representative of independent bounded expiry enumeration.
  action expireOne = {
    val due = s.pending.filter(id => not(s.expiryMarked.contains(id)) and s.now > s.deadlines.get(id))
    nondet id = due.oneOf()
    all { s.online, s' = { ...s, expiryMarked: s.expiryMarked.union(Set(id)), cancellation: if (s.publishing.contains(id)) s.cancellation else s.cancellation.union(Set(id)) } }
  }
  action tick = all { s.now == 10, s' = { ...s, now: 21 } }
  action crash = all { s.online, not(s.crashed), s' = { ...s, online: false, crashed: true,
    queued: Set(), running: Set(), publishing: Set() } }
  action restart = all { not(s.online), s' = { ...s, online: true } }
  action idle = s' = s
  action step = { nondet id = ids.oneOf()
    any { enqueue(id), admit(id), start(id), claim(id), cancel(id), publish(id), complete(id),
      expireOne, tick, callerCancel, crash, restart, idle } }
  val immutableInheritance = s.admitted.forall(id => s.deadlines.get(id) == 20
    and (id == 1 or s.parents.get(id) == id - 1 and s.admitted.contains(id - 1)))
  val finiteExecution = s.starts.forall(x => x._2 <= 20) and s.successes.forall(x => x._2 <= 20)
    and s.effects.forall(x => x._2 <= 20)
  val retainedWitness = s.admitted.forall(id => s.deadlines.get(id) == 20)
    and s.pending == s.admitted.filter(id => s.status.get(id) == 0)
  val safety = immutableInheritance and finiteExecution and retainedWitness
  run nestedCallerCancellationTest = init.then(enqueue(2)).then(callerCancel).then(admit(2))
    .then(all { assert(s.parents.get(2) == 1 and s.deadlines.get(2) == 20), idle })
  run nestedParentFinishingTest = init.then(enqueue(2)).then(admit(2)).then(enqueue(3)).then(admit(3))
    .then(start(1)).then(claim(1)).then(complete(1))
    .then(all { assert(safety and s.parents.get(3) == 2 and s.deadlines.get(3) == 20 and s.pending.contains(3)), idle })
  run retainedExpiredLateSuccessTest = init.then(enqueue(2)).then(admit(2)).then(crash).then(restart).then(tick)
    .then(complete(2)).then(all { assert(safety and s.status.get(2) == 3 and s.deadlines.get(2) == 20), idle })
  run publicationDoesNotSealExpiryTest = init.then(start(1)).then(claim(1)).then(tick).then(complete(1))
    .then(all { assert(s.status.get(1) == 3 and safety), idle })
  run publicationCancellationTooLateTest = init.then(start(1)).then(claim(1)).then(cancel(1))
    .then(all { assert(not(s.cancellation.contains(1)) and s.publishing.contains(1)), idle })
    .then(complete(1)).then(all { assert(s.status.get(1) == 2), idle })

}
