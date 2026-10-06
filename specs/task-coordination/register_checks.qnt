module register_checks {
  import register.* from "./register"

  pure val a = rep(1, 1, 30, 2, 2, 1, 1, 1)
  pure val aw = rep(1, 1, 30, 1, 1, 2, 2, 2)
  pure val b = rep(1, 1, 20, 2, 1, 1, 1, 1)
  pure val c = rep(1, 1, 10, 2, 1, 1, 1, 1)
  pure val cs = rep(1, 1, 10, 1, 2, 1, 1, 1)
  pure val recovered = rep(1, 3, 50, 1, 1, 1, 1, 1)
  pure val causal = rep(2, 2, 40, 1, 1, 1, 1, 1)
  pure val foreign = rep(3, 2, 41, 1, 1, 1, 1, 1)
  pure val middle = rep(1, 2, 60, 1, 1, 1, 1, 1)
  pure val d = rep(1, 1, 5, 1, 1, 1, 1, 1)
  pure val inputs = Set(a, aw, b, c, cs, d, recovered, causal, foreign, middle)

  var state: Register
  var history: Set[Rep]
  var availableKeys: Set[int]
  action init = all { state' = empty, history' = Set(), availableKeys' = Set() }
  action receive = {
    nondet r = inputs.oneOf()
    all { state' = merge(state, batch(singleton(r))), history' = history.union(Set(r)), availableKeys' = availableKeys }
  }
  action invalidBatch = {
    nondet reason = Set(1, 2, 3, 4).oneOf()
    val goodAndBad = join(singleton(recovered), singleton(causal))
    val invalid = { schema: reason != 1, binding: reason != 2, uniqueWriters: reason != 3, valid: reason != 4, lanes: goodAndBad }
    all { state' = merge(state, invalid), history' = history, availableKeys' = availableKeys }
  }
  action keyAvailability = {
    nondet nextKeys = powerset(Set(1, 2)).oneOf()
    all { state' = state, history' = history, availableKeys' = nextKeys }
  }
  // Serialization/restart contract only: there is no persistence implementation
  // in the pure Rust reducer. This round trip does not claim SQL durability.
  action snapshotRestore = all { state' = restore(batch(state)), history' = history, availableKeys' = availableKeys }
  action step = any { receive, invalidBatch, keyAvailability, snapshotRestore }
  val oracleAgreement = state == expected(history)
  val structuralInvariant = wellFormed(state)
  val causality = heads(state) == expectedHeads(history)
  val replayFences = writers.forall(w => history.filter(r => r.header.writer == w).forall(r => state.get(w).sequence >= r.header.sequence))
  val exactIdentity = writers.forall(w => state.get(w).representations.forall(r => history.contains(r)))
  val poisonExcluded = writers.forall(w => if (state.get(w).representations.size() == 0) not(heads(state).exists(h => h.writer == w)) else true)
  val behavioralSafety = structuralInvariant and causality and replayFences and exactIdentity and poisonExcluded
  val safety = oracleAgreement and behavioralSafety

  run poisonReplacementTest = init
    .then(all { state' = join(singleton(a), singleton(b)), history' = Set(a,b), availableKeys' = availableKeys })
    .then(all { state' = join(state, singleton(c)), history' = history.union(Set(c)), availableKeys' = availableKeys })
    .then(all { state' = join(state, singleton(cs)), history' = history.union(Set(cs)), availableKeys' = availableKeys })
    .expect(safety and state.get(1).headers == Set(b.header,cs.header) and state.get(1).representations.size() == 0)
  run poisonTransferAndRecoveryTest = init
    .then(all { state' = restore(batch(join(singleton(a),singleton(b)))), history' = Set(a,b), availableKeys' = availableKeys })
    .then(all { state' = join(state,singleton(recovered)), history' = history.union(Set(recovered)), availableKeys' = availableKeys })
    .then(all { state' = join(state,singleton(middle)), history' = history.union(Set(middle)), availableKeys' = availableKeys })
    .expect(safety and state.get(1) == current(recovered))
  run sameOriginalHistoricalWrapperTest = init
    .then(all { state' = join(singleton(a),singleton(aw)), history' = Set(a,aw), availableKeys' = Set(2) })
    .expect(safety and state.get(1) == current(aw) and readerAccepts(aw,1,1,30,true) and not(readerAccepts(aw,2,1,30,true)))
  run hiddenReplayFenceTest = init
    .then(all { state' = join(singleton(a),singleton(causal)), history' = Set(a,causal), availableKeys' = availableKeys })
    .expect(safety and not(heads(state).contains(a.header)) and state.get(1).sequence == 1)
    .then(all { state' = join(state,singleton(a)), history' = history, availableKeys' = availableKeys })
    .expect(safety and heads(state) == Set(causal.header))
  run foreignRecordDoesNotDominateTest = init
    .then(all { state' = join(singleton(a),singleton(foreign)), history' = Set(a,foreign), availableKeys' = availableKeys })
    .expect(safety and heads(state) == Set(a.header,foreign.header))
  run rejectedMultiLaneBatchTest = init
    .then(all { state' = merge(state,{ schema: true, binding: true, uniqueWriters: true, valid: false, lanes: join(singleton(a),singleton(causal)) }), history' = history, availableKeys' = availableKeys })
    .expect(state == empty and safety)
  run localProducerAndReaderBoundaryTest = {
    assert(not(localAllowed(singleton(recovered),a)))
      .then(assert(not(localAllowed(singleton(a),b))))
      .then(assert(localAllowed(singleton(a),aw)))
      .then(assert(localAllowed(singleton(a),middle)))
      .then(assert(not(readerAccepts(a,1,1,30,false))))
      .then(assert(not(readerAccepts(a,1,2,30,true))))
      .then(assert(not(readerAccepts(a,1,1,20,true))))
  }
  run canonicalOrderingAndOutcomeTest = {
    val variants = Set(rep(1,1,30,1,1,1,1,1),rep(1,1,30,1,1,1,1,2),
      rep(1,1,30,2,1,1,1,1),rep(1,1,30,1,1,1,2,1),
      rep(1,1,30,1,1,2,1,1),rep(1,1,30,1,2,1,1,1))
    assert(variants.forall(x => variants.forall(y => before(x,y) == (rank(x) < rank(y)))))
      .then(assert(canonical(variants) == rep(1,1,30,1,1,1,1,1)))
      .then(assert(outcome(empty,singleton(a)) == 1))
      .then(assert(outcome(singleton(a),singleton(a)) == 0))
      .then(assert(outcome(singleton(a),singleton(b)) == 2))
      .then(assert(outcome(join(singleton(a),singleton(b)),singleton(recovered)) == 1))
  }
  run fourHeaderPoisonJoinTest = {
    val first = join(singleton(a),singleton(b))
    val second = join(singleton(c),singleton(d))
    val combined = join(first,second)
    assert(combined.get(1).headers == Set(d.header,c.header))
      .then(assert(combined.get(1).representations.size() == 0))
      .then(assert(join(combined,first) == combined))
      .then(assert(outcome(combined,first) == 0))
  }
  run greaterDependencyAndPoisonedObserverTest = {
    val observer = rep(3,2,42,1,1,1,1,1)
    val observed = join(singleton(a),singleton(observer))
    assert(heads(observed) == Set(observer.header))
      .then(assert(observed.get(1) == current(a)))
      .then(assert(heads(join(observed,singleton(foreign))) == Set(a.header)))
  }
  run signatureVariantFutureConflictTest = {
    val first = join(join(singleton(cs),singleton(c)),singleton(b))
    val second = join(singleton(cs),join(singleton(c),singleton(b)))
    assert(first == second and first == expected(Set(cs,c,b)))
      .then(assert(join(singleton(c),singleton(cs)) == singleton(cs)))
  }
}

module register_algebra {
  import register.* from "./register"
  // Three writers, four semantic conflicts, two signature/wrapper classes,
  // empty and transferred poison, and sequences 1/2/3.
  pure val samples = Set(rep(1,1,30,2,2,1,1,1),rep(1,1,30,1,1,2,2,2),
    rep(1,1,20,2,1,1,1,1),rep(1,1,10,2,1,1,1,1),rep(1,1,10,1,2,1,1,1),rep(1,2,60,1,1,1,1,1),
    rep(1,1,5,1,1,1,1,1),rep(1,3,50,1,1,1,1,1),rep(2,2,40,1,1,1,1,1),rep(3,2,41,1,1,1,1,1))
  pure val lanes = Set(empty).union(samples.map(singleton)).union(Set(
    join(singleton(rep(1,1,30,2,2,1,1,1)),singleton(rep(1,1,20,2,1,1,1,1))),
    join(singleton(rep(1,1,20,2,1,1,1,1)),singleton(rep(1,1,10,1,1,1,1,1))),
    join(singleton(rep(1,1,10,2,1,1,1,1)),singleton(rep(1,1,5,1,1,1,1,1)))))
  var left: Register
  var center: Register
  var right: Register
  // Quantify all triples in init once. An all-to-all transition graph would
  // repeat 14^3 successors from every triple without checking additional inputs.
  action init = {
    nondet a = lanes.oneOf()
    nondet b = lanes.oneOf()
    nondet c = lanes.oneOf()
    all { left' = a, center' = b, right' = c }
  }
  action step = all { left' = left, center' = center, right' = right }
  val algebra = join(left,left) == left
    and join(left,center) == join(center,left)
    and join(join(left,center),right) == join(left,join(center,right))
    and restore(batch(left)) == left and join(left,empty) == left
}


// Dedicated profile leaves the existing positive input domains unchanged.
module register_zero_checks {
  import register.* from "./register"
  pure val zero = rep(1,0,10,1,1,1,1,1)
  pure val conflict = rep(1,0,20,1,1,1,1,1)
  pure val later = rep(1,1,30,1,1,1,1,1)
  pure val inputs = Set(zero,conflict,later)
  var state: Register
  var history: Set[Rep]
  action init = all { state' = empty, history' = Set() }
  action receive = {
    nondet r = inputs.oneOf()
    all { state' = merge(state,batch(singleton(r))), history' = history.union(Set(r)) }
  }
  action restoreSnapshot = all { state' = restore(batch(state)), history' = history }
  action step = any { receive, restoreSnapshot }
  val safety = state == expected(history) and wellFormed(state)
    and heads(state) == expectedHeads(history)
    and (history.size() == 0 implies state == empty)
    and (history.size() > 0 implies state.get(1).sequence >= 0)
  run zeroInsertionReplayConflictRecoveryTest = init
    .then(all { state' = merge(state,batch(singleton(zero))), history' = Set(zero) })
    .expect(safety and state != empty and state.get(1).representations == Set(zero)
      and outcome(empty,singleton(zero)) == 1 and localAllowed(empty,zero))
    .then(all { state' = merge(state,batch(singleton(zero))), history' = history })
    .expect(safety and outcome(state,singleton(zero)) == 0)
    .then(all { state' = merge(state,batch(singleton(conflict))), history' = history.union(Set(conflict)) })
    .expect(safety and state.get(1).headers == Set(zero.header,conflict.header)
      and heads(state).size() == 0 and not(localAllowed(state,zero)))
    .then(restoreSnapshot).expect(safety)
    .then(all { state' = merge(state,batch(singleton(later))), history' = history.union(Set(later)) })
    .expect(safety and state == singleton(later))
    .then(all { state' = merge(state,batch(singleton(zero))), history' = history })
    .expect(safety and state == singleton(later))
}

module register_zero_algebra {
  import register.* from "./register"
  pure val samples = Set(rep(1,0,10,1,1,1,1,1),rep(1,0,20,1,1,1,1,1),rep(1,1,30,1,1,1,1,1))
  pure val lanes = powerset(samples).map(expected)
  var left: Register
  var center: Register
  var right: Register
  // Include absent, each zero Current, zero Poison, and later recovery.
  action init = {
    nondet a = lanes.oneOf()
    nondet b = lanes.oneOf()
    nondet c = lanes.oneOf()
    all { left' = a, center' = b, right' = c }
  }
  action step = all { left' = left, center' = center, right' = right }
  val algebra = join(left,left) == left
    and join(left,center) == join(center,left)
    and join(join(left,center),right) == join(left,join(center,right))
    and restore(batch(left)) == left and join(left,empty) == left
}
