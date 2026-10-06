// Repository-owned accepted request; cancellation before successful send is outside
// this contract. No pool observer. Queue capacity/multi-request ordering abstracted.
module host_admission_ownership {
  type State = { stage: int, caller: bool, stopping: bool, db: bool, cache: bool,
    hint: bool, fence: bool, owner: bool, proof: bool, observed: bool, deadline: int, reopened: bool }
  var s: State
  action init = s' = { stage: 1, caller: true, stopping: false, db: false,
    cache: false, hint: false, fence: false, owner: false, proof: false,
    observed: false, deadline: 10, reopened: false }
  action abandon = all { s.caller, s' = { ...s, caller: false } }
  // RepoStop closes intake and drains accepted requests while repo remains alive.
  action stop = all { not(s.stopping), s' = { ...s, stopping: true } }
  action commit = all { s.stage == 1,
    s' = { ...s, stage: 2, db: true, owner: true, proof: true, fence: true } }
  // Crucially NOT gated on caller or stop. Writer retains transition fence
  // across committed-row/cache gap through cache and notification publication.
  action publish = all { s.stage == 2,
    s' = { ...s, stage: 3, cache: true, hint: true, fence: false } }
  action dueRead = all { s.owner and not(s.fence),
    s' = { ...s, observed: true } }
  action complete = all { s.stage == 3 and s.observed,
    s' = { ...s, stage: 4, owner: false } }
  // Process crash only after SQL ownership exists: boot loads cache/proof and
  // rereads the index. hint denotes discoverability (signal OR boot read).
  action reopen = all { s.stage >= 2 and not(s.reopened),
    s' = { ...s, stage: if (s.stage == 2) 3 else s.stage,
      cache: true, hint: true, fence: false, caller: false, reopened: true } }
  action idle = s' = s
  action step = any { abandon, stop, commit, publish, dueRead, complete, reopen, idle }
  val vars = (s)
  val safety = (s.observed implies s.cache and s.proof)
    and (s.stage >= 2 implies s.proof and s.deadline == 10)
    and (s.stage >= 3 implies s.cache and s.hint)
    and (s.stage == 4 implies not(s.owner))
  temporal progress = (weakFair(commit,vars) and weakFair(publish,vars)
    and weakFair(dueRead,vars) and weakFair(complete,vars)) implies eventually(s.stage == 4)
}

module waiting_terminal_cleanup {
  // Status classes: Waiting1, Active2, Succeeded3, Failed4, Cancelled5.
  pure def verdict(status: int, owner: bool): (int,bool) =
    if (status == 1) (4,false) else (status,owner)
  run waitingFailureRemovesOwnerTest = assert(verdict(1,true) == (4,false))
  run staleWaitingVerdictPreservesActiveAndTerminalTest = {
    assert(verdict(2,true) == (2,true))
      .then(assert(verdict(3,false) == (3,false)))
      .then(assert(verdict(4,false) == (4,false)))
      .then(assert(verdict(5,false) == (5,false)))
  }
  var status: int
  var owner: bool
  var proof: bool
  var reopened: bool
  action init = {
    nondet current = Set(1,2,3,4,5).oneOf()
    all { status' = current, owner' = current <= 2, proof' = true, reopened' = false }
  }
  // Verdict and owner deletion share the SQL transaction. Restart retains both.
  action waitingFailure = all { status' = verdict(status,owner)._1,
    owner' = verdict(status,owner)._2, proof' = proof, reopened' = reopened }
  action reopen = all { status' = status, owner' = owner, proof' = proof, reopened' = true }
  action step = any { waitingFailure, reopen }
  val safety = proof and (status >= 3 implies not(owner))
}
