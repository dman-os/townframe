// Historical diagnostic snapshot, prior to the approved acknowledgement ledger.
// Policy1 is the former first-success retention;
// policy2 is the proposed-but-insufficient max-summary-only repair, NOT source.
// All requested summaries are genuine original outcomes of one immutable task.
module domain_receipt_arrival_orders {
  pure def incorporate(existing: int,requested: int,policy: int): int =
    if (existing == 0) requested else if (policy == 1) existing
    else if (requested > existing) requested else existing
  var policy: int
  var ascending: bool
  var stored: int
  var stage: int
  var acknowledgements: Set[int]
  pure val effects = Set(1,2)
  val first = if (ascending) 1 else 2
  val second = if (ascending) 2 else 1
  action init = {
    nondet p = Set(1,2).oneOf()
    nondet order = Set(false,true).oneOf()
    all { policy' = p, ascending' = order, stored' = 0, stage' = 0, acknowledgements' = Set() }
  }
  // Durable receipt commit may happen before any volatile driver acknowledgement.
  action firstCommit = all { stage == 0, policy' = policy, ascending' = ascending,
    stored' = incorporate(stored,first,policy), stage' = 1, acknowledgements' = acknowledgements }
  // Crash/reopen forgets volatile delivery. Original effects and receipt survive.
  action reopen = all { stage == 1, policy' = policy, ascending' = ascending,
    stored' = stored, stage' = 2, acknowledgements' = acknowledgements }
  action firstRetry = all { stage == 2, policy' = policy, ascending' = ascending,
    stored' = incorporate(stored,first,policy),
    stage' = if (incorporate(stored,first,policy) == first) 3 else stage,
    acknowledgements' = if (incorporate(stored,first,policy) == first) acknowledgements.union(Set(first)) else acknowledgements }
  action secondRetry = all { stage == 3, policy' = policy, ascending' = ascending,
    stored' = incorporate(stored,second,policy),
    stage' = if (incorporate(stored,second,policy) == second) 4 else stage,
    acknowledgements' = if (incorporate(stored,second,policy) == second) acknowledgements.union(Set(second)) else acknowledgements }
  action done = all { stage == 4, policy' = policy, ascending' = ascending,
    stored' = stored, stage' = stage, acknowledgements' = acknowledgements }
  action step = any { firstCommit, reopen, firstRetry, secondRetry, done }
  val vars = (policy,ascending,stored,stage,acknowledgements)
  val safety = acknowledgements.subseteq(effects) and (stored == 0 or effects.contains(stored))
  temporal progress = (weakFair(firstCommit,vars) and weakFair(reopen,vars)
    and weakFair(firstRetry,vars) and weakFair(secondRetry,vars)) implies eventually(acknowledgements == effects)
  run bothCurrentArrivalOrdersFailTest = {
    assert(incorporate(incorporate(0,1,1),2,1) != 2)
      .then(assert(incorporate(incorporate(0,2,1),1,1) != 1))
  }
  run canonicalAdvanceStillRejectsOlderGenuineOutcomeTest = {
    assert(incorporate(incorporate(0,1,2),2,2) == 2)
      .then(assert(incorporate(incorporate(0,2,2),1,2) != 1))
  }
}
