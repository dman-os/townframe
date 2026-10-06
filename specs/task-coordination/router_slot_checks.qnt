// Source: tasks/router.rs RouterSlotState::merge_claim. Compatible history
// profile has monotone generations and one claim per writer/sequence.
module router_slot_checks {
  import router_slot.* from "./router_slot"
  pure def claim(n: int,q: int): Claim = { ...absent, pool: 1, candidate: n,
    incarnation: 1, sequence: q, generation: q, claim: q + 1, protocol: 1, rank: n }
  pure val inputs = Set(claim(1,0),claim(1,1),claim(2,0),claim(2,1))
  pure def expected(history: Set[Claim]): Slot = candidates.mapBy(n =>
    history.filter(c => c.candidate == n).fold(absent,(a,c) => if (c.sequence > a.sequence) c else a))
  var state: Slot
  var history: Set[Claim]
  action init = all { state' = empty, history' = Set() }
  action step = { nondet incoming = inputs.oneOf()
    all { state' = merge(state,incoming).slot, history' = history.union(Set(incoming)) } }
  val safety = state == expected(history)
  run zeroReplayAndGenerationBoundaryTest = {
    val zero = merge(empty,claim(1,0))
    val conflict = merge(zero.slot,{ ...claim(1,0), incarnation: 2 })
    val latest = merge(zero.slot,claim(1,1))
    assert(zero.changed and merge(zero.slot,claim(1,0)).changed == false)
      .then(assert(conflict.error == 2 and conflict.slot == zero.slot))
      .then(assert(merge(latest.slot,{ ...claim(1,1), sequence: 2, generation: 0 }).error == 1))
      .then(assert(merge(latest.slot,claim(1,0)).slot == latest.slot))
  }
}
