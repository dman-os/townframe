module register_delivery {
  import register.* from "./register"
  const genuine: Set[Rep]
  pure val peers = Set(1,2)
  var replicas: int -> Register
  var received: int -> Set[Rep]
  var pending: int -> Set[Rep]
  var online: int -> bool
  var connected: bool
  action init = all {
    replicas' = peers.mapBy(_ => empty), received' = peers.mapBy(_ => Set()),
    pending' = peers.mapBy(_ => genuine), online' = peers.mapBy(_ => true), connected' = true
  }
  action deliver(p: int): bool = {
    nondet r = pending.get(p).oneOf()
    all { connected, online.get(p), pending.get(p).size() > 0,
      replicas' = replicas.set(p,merge(replicas.get(p),batch(singleton(r)))),
      received' = received.set(p,received.get(p).union(Set(r))),
      pending' = pending.set(p,pending.get(p).exclude(Set(r))), online' = online, connected' = connected }
  }
  action duplicate = {
    nondet p = peers.oneOf()
    nondet r = genuine.oneOf()
    all { connected, online.get(p), received.get(p).contains(r),
      replicas' = replicas.set(p,merge(replicas.get(p),batch(singleton(r)))),
      received' = received, pending' = pending, online' = online, connected' = connected }
  }
  action partition = all { replicas' = replicas, received' = received, pending' = pending, online' = online, connected' = false }
  action heal = all { replicas' = replicas, received' = received, pending' = pending, online' = online, connected' = true }
  // A completed reducer snapshot is retained. Crash may interrupt the NEXT
  // delivery before merge; pending evidence is retained by the environment.
  // This is not a model of host SQL commit/carry-forward, which is still pending.
  action crash = {
    nondet p = peers.oneOf()
    all { replicas' = replicas, received' = received, pending' = pending, online' = online.set(p,false), connected' = connected }
  }
  action restart = {
    nondet p = peers.oneOf()
    all { not(online.get(p)), replicas' = replicas.set(p,restore(batch(replicas.get(p)))),
      received' = received, pending' = pending, online' = online.set(p,true), connected' = connected }
  }
  action delay = all { replicas' = replicas, received' = received, pending' = pending, online' = online, connected' = connected }
  action step = any { deliver(1), deliver(2), duplicate, partition, heal, crash, restart, delay }
  val vars = (replicas,received,pending,online,connected)
  val available = connected and peers.forall(p => online.get(p))
  val converged = peers.forall(p => pending.get(p).size() == 0 and replicas.get(p) == expected(genuine))
  val safety = peers.forall(p => replicas.get(p) == expected(received.get(p)) and wellFormed(replicas.get(p)))
  temporal convergence = (eventually(always(available)) and weakFair(deliver(1),vars) and weakFair(deliver(2),vars)) implies eventually(always(converged))
  temporal unfairConvergence = eventually(always(available)) implies eventually(converged)

  // Nonvacuity witness includes a partition and crash; the caller supplies the
  // genuine input cardinality. Delivery order remains nondeterministic.
  run reachableConvergenceTest = init.then(partition).then(crash).then(heal).then(restart)
    .then(genuine.size().reps(_ => deliver(1)))
    .then(genuine.size().reps(_ => deliver(2)))
    .expect(available and converged and safety)
}

module register_convergence {
  import register.* from "./register"
  // Positive experimental profile includes overwritten writes and poison
  // recovery, but does not discharge the signature-variant conflict profile.
  pure val inputSet = Set(rep(1,1,30,2,2,1,1,1),rep(1,1,20,1,1,1,1,1),
    rep(1,3,50,1,2,1,1,1),rep(1,3,50,1,1,2,2,2),
    rep(2,2,40,1,1,1,1,1),rep(3,2,41,1,1,1,1,1))
  import register_delivery(genuine=inputSet) as D
  action init = D::init
  action step = D::step
  val safety = D::safety
  temporal convergence = D::convergence
  temporal unfairConvergence = D::unfairConvergence
}

module register_signature_convergence {
  import register.* from "./register"
  // Reachable conflicting header-signature variants MUST NOT be constrained away.
  pure val inputSet = Set(rep(1,1,10,2,1,1,1,1),rep(1,1,10,1,2,1,1,1),
    rep(1,1,20,2,1,1,1,1),rep(2,2,40,1,1,1,1,1),rep(3,2,41,1,1,1,1,1))
  import register_delivery(genuine=inputSet) as D
  action init = D::init
  action step = D::step
  val safety = D::safety
  temporal convergence = D::convergence
}

module register_zero_convergence {
  import register.* from "./register"
  pure val inputSet = Set(rep(1,0,10,1,1,1,1,1),rep(1,0,20,1,1,1,1,1),rep(1,1,30,1,1,1,1,1))
  import register_delivery(genuine=inputSet) as D
  action init = D::init
  action step = D::step
  temporal convergence = D::convergence
}
