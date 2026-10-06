// Diagnostic isolation only: not the full router acceptance profile.
module offer_declined_core {
  type Session = { node: int, incarnation: int, session: int }
  type Allocation = { id: int, session: Session, task: int, attempt: int, cancel: bool }
  type Block = { session: Session, task: int, reason: int }
  type State = { allocations: Set[Allocation], blocked: Set[Block] }
  pure val session = { node: 1, incarnation: 1, session: 1 }
  pure def decline(s: State, allocation: int, key: Session, reason: int): State = {
    val found = s.allocations.filter(a => a.id == allocation and a.session == key and a.attempt == 0)
    if (found.size() == 0) s else {
      val blocks = if (Set(1,2,3,4,7).contains(reason)) found.map(a => { session: a.session, task: a.task, reason: reason }) else Set()
      { ...s, allocations: s.allocations.exclude(found),
        blocked: s.blocked.filter(b => not(blocks.exists(x => x.session == b.session and x.task == b.task))).union(blocks) }
    }
  }
  var state: State
  action init = state' = { allocations: Set({ id: 1, session: session, task: 1, attempt: 0, cancel: false }), blocked: Set() }
  action step = { nondet reason = 1.to(7).oneOf(); state' = decline(state,1,session,reason) }
  val safety = state.allocations.size() <= 1
}
