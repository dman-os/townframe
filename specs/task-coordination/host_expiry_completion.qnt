// Finite owner profile: boot read already discovered one due task; no pool observer.
// Durable control reconciliation/finalization eventually returns (environment contract).
module host_expiry {
  pure val observer = false
  var owner: bool
  var marked: bool
  var cancelled: bool
  var terminal: bool
  var cached: bool
  var crashed: bool
  action init = all { owner' = true, marked' = false, cancelled' = false,
    terminal' = false, cached' = true, crashed' = false }
  // Lost/coalesced hints are irrelevant once pending deadline is cached.
  action tick = all { owner' = owner, marked' = marked, cancelled' = cancelled,
    terminal' = terminal, cached' = cached, crashed' = crashed }
  action cancel = all { owner and cached and not(marked),
    owner' = owner, marked' = marked, cancelled' = true,
    terminal' = terminal, cached' = cached, crashed' = crashed }
  // Profile excludes finalization-already-owned no-op; marked does NOT globally
  // imply actual abort in Rust. Mark comes after fail_local returns.
  action mark = all { owner and cancelled, owner' = owner, marked' = true,
    cancelled' = cancelled, terminal' = terminal, cached' = false, crashed' = crashed }
  action complete = all { cancelled and not(terminal), owner' = false, marked' = false,
    cancelled' = cancelled, terminal' = true, cached' = false, crashed' = crashed }
  // Cuts before/after control or mark retain durable ownership/control. Boot
  // rereads only unrequested owner; attempt evidence is never removed here.
  action reopen = all { not(crashed), owner' = owner, marked' = marked,
    cancelled' = cancelled, terminal' = terminal, cached' = (owner and not(marked)), crashed' = true }
  action step = any { tick, cancel, mark, complete, reopen }
  val vars = (owner,marked,cancelled,terminal,cached,crashed)
  val safety = (terminal implies not(owner)) and (marked implies owner and cancelled)
  temporal progress = (weakFair(cancel,vars) and weakFair(complete,vars)) implies eventually(terminal)
}

// Actual completion versus requested status and volatile callback delivery.
module completion_order {
  pure def winner(expired: bool): int = if (expired) 2 else 1
  pure def callbackStatus(persisted: int): int = persisted
  run requestedSuccessIsNotDurableWinnerTest = assert(winner(true) == 2 and callbackStatus(winner(true)) == 2)
  var expired: bool
  var durable: int
  var owner: bool
  var proof: bool
  var response: int
  var callback: int
  var crashed: bool
  action init = {
    nondet late = Set(false,true).oneOf()
    all { expired' = late, durable' = 0, owner' = true, proof' = true,
      response' = 0, callback' = 0, crashed' = false }
  }
  // Requested success is fenced by SQLite's clock at the write. Owner removal
  // shares the transaction; evidence survives. This is not physical fsync timing.
  action commit = all { durable == 0, expired' = expired, durable' = winner(expired),
    owner' = false, proof' = proof, response' = response, callback' = callback, crashed' = crashed }
  action returned = all { durable != 0 and response == 0 and not(crashed),
    expired' = expired, durable' = durable, owner' = owner, proof' = proof,
    response' = durable, callback' = callback, crashed' = crashed }
  action notify = all { response != 0 and not(crashed), expired' = expired,
    durable' = durable, owner' = owner, proof' = proof, response' = response,
    callback' = callbackStatus(response), crashed' = crashed }
  action reopen = all { durable != 0 and not(crashed), expired' = expired, durable' = durable,
    owner' = owner, proof' = proof, response' = 0, callback' = callback, crashed' = true }
  action idle = all { expired' = expired, durable' = durable, owner' = owner,
    proof' = proof, response' = response, callback' = callback, crashed' = crashed }
  action step = any { commit, returned, notify, reopen, idle }
  val vars = (expired,durable,owner,proof,response,callback,crashed)
  val safety = proof and ((durable == 0) == owner)
    and (expired and durable != 0 implies durable == 2)
    and (response != 0 implies response == durable)
    and (callback != 0 implies durable != 0 and callback == durable)
  // Diagnostic expected failure: no durable callback outbox/replay is modeled.
  temporal callbackDelivery = (weakFair(commit,vars) and weakFair(returned,vars)
    and weakFair(notify,vars)) implies eventually(callback != 0)
}

// Historical pre-repository-writer admission-hint gap, NOT current-source claim.
module host_silent_hint_gap {
  var cancelled: bool
  action init = cancelled' = false
  // New owner exists durably, but cached next=None. No signal/restart arrives;
  // pending-owner clock sampler is disabled by the cached absence.
  action step = cancelled' = cancelled
  val vars = (cancelled)
  temporal progress = weakFair(step,vars) implies eventually(cancelled)
}

// Historical pre-cleanup set_waiting_failed terminal path; fixed in current Rust.
module waiting_failure_owner_gap {
  var owner: bool
  var terminal: bool
  action init = all { owner' = true, terminal' = false }
  action step = all { owner' = owner, terminal' = true }
  val safety = terminal implies not(owner)
}
