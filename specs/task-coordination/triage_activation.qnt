// Source: rt/triage.rs install_activation_snapshot/query_activation.
// Single plug; target equality includes exact config/manifest identities.
module triage_activation {
  type Ack = { target: int, boundary: int, processors: int }
  type Installed = { ack: Ack, pending: bool }
  pure val absent = { target: 0, boundary: 0, processors: 0 }
  // Manifest generation and enabled-ref comparison are explicit inputs sampled
  // from the snapshot; publication/SQL/refresh awaits are not atomic here in Rust.
  pure def install(previous: Ack,target: int,available: bool,rejected: bool,
    boundary: int,sameEnabledRef: bool,generation: int): Installed = {
    if (previous.target != 0 and target == previous.target and not(available) and not(rejected))
      { ack: previous, pending: true }
    else if (available) { ack: if (previous.target == target) previous else
      { target: target, boundary: boundary, processors: if (previous.target != 0 and sameEnabledRef) previous.processors else generation }, pending: false }
    else { ack: absent, pending: false }
  }
  // query statuses:0 Pending,1 Active,2 Rejected,3 Disabled,4 Superseded.
  pure def query(ack: Ack,wanted: int,target: int,available: bool,rejected: bool): int =
    if (rejected) 2 else if (target == 0) 3 else if (target != wanted) 4
    else if (not(available)) 0 else if (ack.target == wanted) 1 else 0
  pure def eligible(ack: Ack,revision: int,pending: bool): bool =
    ack.target != 0 and not(pending) and revision > ack.boundary
}

module triage_activation_checks {
  import triage_activation.* from "./triage_activation"
  var ack: Ack
  var pending: bool
  var violation: bool
  action init = all { ack' = absent, pending' = false, violation' = false }
  action step = {
    nondet target = Set(0,1,2).oneOf()
    nondet available = Set(false,true).oneOf()
    nondet rejected = Set(false,true).oneOf()
    nondet boundary = Set(0,1,2).oneOf()
    nondet sameRef = Set(false,true).oneOf()
    val consistent = target != 0 and available and not(rejected)
    val installed = install(ack,target,consistent,rejected,boundary,sameRef,target)
    all { ack' = installed.ack, pending' = installed.pending,
      violation' = (violation or (ack.target == target and consistent and installed.ack != ack)
        or (ack.target != target and consistent and installed.ack.boundary != boundary)
        or (installed.pending and installed.ack != ack)) }
  }
  val safety = not(violation) and (pending implies ack.target != 0)
  run sameTargetUnavailablePreservesBoundaryTest = {
    val before = { target: 1, boundary: 1, processors: 1 }
    val unavailable = install(before,1,false,false,2,true,1)
    assert(unavailable.ack == before and unavailable.pending)
      .then(assert(not(eligible(before,2,unavailable.pending))))
      .then(assert(install(before,1,true,false,2,true,1).ack == before))
  }
  run changedTargetSamplesBoundaryAndDisableFencesTest = {
    val before = { target: 1, boundary: 0, processors: 1 }
    val changed = install(before,2,true,false,1,true,2)
    assert(changed.ack.boundary == 1 and changed.ack.processors == 1)
      .then(assert(not(eligible(changed.ack,1,false)) and eligible(changed.ack,2,false)))
      .then(assert(install(changed.ack,0,false,false,2,false,0).ack == absent))
      .then(assert(query(changed.ack,1,2,true,false) == 4))
  }
  run queryCurrentTargetStatusTest = {
    val old = { target: 1, boundary: 1, processors: 1 }
    assert(query(old,1,1,false,false) == 0)
      .then(assert(query(old,1,1,true,true) == 2))
      .then(assert(query(old,1,0,false,false) == 3))
      .then(assert(query(old,1,2,true,false) == 4))
      .then(assert(query(old,1,1,true,false) == 1))
      .then(assert(not(eligible(old,1,false)) and eligible(old,2,false)))
  }

}

