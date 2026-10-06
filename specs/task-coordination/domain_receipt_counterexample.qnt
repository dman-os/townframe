// Historical receipt-policy snapshot: former incorporate_task_receipt first-success rule.
// Imported TaskTicket fixtures now use current schema3. This is a counterexample,
// not a proposed receipt policy or an accepted liveness proof.
module domain_receipt_counterexample {
  import task_ticket as ticket from "./task_ticket"
  import coordination_fixtures as fixtures from "./coordination_fixtures"
  pure val success1 = { success: true, attempt: 1, result: 1, reason: 0 }
  pure val success2 = { success: true, attempt: 2, result: 2, reason: 0 }
  pure val first = { ...fixtures::base, terminal: fixtures::base.terminal.set(1,
    { writer: 1, sequence: 1, fact: success1 }) }
  pure val second = { ...fixtures::base, terminal: fixtures::base.terminal.set(2,
    { writer: 2, sequence: 1, fact: success2 }) }
  pure val target = ticket::summary(ticket::merge(first,second).ticket)
  pure def incorporate(existing: ticket::Fact,incoming: ticket::Fact): ticket::Fact =
    if (existing.success or existing == incoming) existing else incoming
  var retained: ticket::Fact
  var acknowledged: bool
  action init = all { retained' = ticket::summary(first), acknowledged' = false }
  // SQL receipt persists across crash/reopen. Retrying cannot change a retained
  // success; actual driver requeues the action after its equality check fails.
  action retry = all { retained' = incorporate(retained,target),
    acknowledged' = (incorporate(retained,target) == target) }
  action step = retry
  val vars = (retained,acknowledged)
  temporal progress = weakFair(retry,vars) implies eventually(acknowledged)
  run exactCanonicalSummaryAckTest = {
    assert(target == success2)
      .then(assert(incorporate(ticket::summary(first),target) == target))
  }
}
