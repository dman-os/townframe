// Finite receipt-boundary composition, not native effect execution or GC.
// Each summary class names a genuine immutable original attempt/result pair.
// Caller proofs and authenticated declaration construction remain contracts.
module domain_ack_ledger {
  pure def commit(acks: Set[int], requested: int): Set[int] = acks.union(Set(requested))
  pure def admissible(owner: int, digest: int): bool = owner == 0 or owner == digest
  pure val originalEffects = Set(1,2)
  var ascending: bool
  var replyLost: bool
  var owner: int
  var durable: Set[int]
  var delivered: Set[int]
  var stage: int
  val first = if (ascending) 1 else 2
  val second = if (ascending) 2 else 1
  action init = {
    nondet order = Set(false,true).oneOf()
    nondet lost = Set(false,true).oneOf()
    all { ascending' = order, replyLost' = lost, owner' = 0,
      durable' = Set(), delivered' = Set(), stage' = 0 }
  }
  // Owner and exact row commit atomically. Canonical ticket winner remains2,
  // but neither local original outcome is substituted with that winner.
  action firstCommit = all { stage == 0, ascending' = ascending, replyLost' = replyLost,
    owner' = 1, durable' = commit(durable,first), delivered' = delivered, stage' = 1 }
  // Both response-before-crash and commit-before-response cuts survive reopen.
  action firstReopen = all { stage == 1, ascending' = ascending, replyLost' = replyLost,
    owner' = owner, durable' = durable,
    delivered' = if (replyLost) delivered else delivered.union(Set(first)), stage' = 2 }
  action firstRetry = all { stage == 2, durable.contains(first), ascending' = ascending,
    replyLost' = replyLost, owner' = owner, durable' = commit(durable,first),
    delivered' = delivered.union(Set(first)), stage' = 3 }
  action secondCommit = all { stage == 3, ascending' = ascending, replyLost' = replyLost,
    owner' = owner, durable' = commit(durable,second), delivered' = delivered, stage' = 4 }
  action secondReopen = all { stage == 4, ascending' = ascending, replyLost' = replyLost,
    owner' = owner, durable' = durable, delivered' = delivered, stage' = 5 }
  action secondRetry = all { stage == 5, durable.contains(second), ascending' = ascending,
    replyLost' = replyLost, owner' = owner, durable' = commit(durable,second),
    delivered' = delivered.union(Set(second)), stage' = 6 }
  action done = all { stage == 6, ascending' = ascending, replyLost' = replyLost,
    owner' = owner, durable' = durable, delivered' = delivered, stage' = stage }
  action step = any { firstCommit, firstReopen, firstRetry, secondCommit, secondReopen, secondRetry, done }
  val vars = (ascending,replyLost,owner,durable,delivered,stage)
  val safety = delivered.subseteq(durable) and durable.subseteq(originalEffects)
    and ((owner == 0) == (durable.size() == 0))
    and (owner == 0 or owner == 1)
  temporal progress = weakFair(step,vars) implies eventually(delivered == originalEffects)
  run arrivalOrderAndReplayTest = {
    assert(commit(commit(Set(),1),2) == originalEffects)
      .then(assert(commit(commit(Set(),2),1) == originalEffects))
      .then(assert(commit(commit(originalEffects,1),1) == originalEffects))
  }
  run ownerDigestAndFullSummaryIdentityTest = {
    assert(admissible(0,1) and admissible(1,1) and not(admissible(1,2)))
      .then(assert(not(commit(Set(),1).contains(2))))
  }
}
