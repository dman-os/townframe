// Source: rt/triage/slots.rs ProcessorSlot::evaluate/settle/desired.
// One already-materialized slot; independent network cell merge is a host contract.
module processor_slots {
  type Cell = { evaluation: int, observed: int -> int, desired: int, matches: bool,
    settlement: (int,int), retained: Set[(int,int)] }
  type Slot = int -> Cell
  pure val writers = Set(1,2)
  pure val emptyCell = { evaluation: 0, observed: writers.mapBy(_ => 0), desired: 0,
    matches: false, settlement: (0,0), retained: Set() }
  pure val empty = writers.mapBy(_ => emptyCell)
  pure def visible(s: Slot): Set[int] = writers.filter(w => s.get(w).desired != 0
    and not(writers.exists(o => s.get(o).observed.get(w) >= s.get(w).evaluation and s.get(w).evaluation != 0)))
  pure def desires(s: Slot): Set[int] = visible(s).map(w => s.get(w).desired)
  pure def settled(s: Slot,g: int): bool = writers.exists(w => s.get(w).settlement._1 == g or s.get(w).retained.exists(t => t._1 == g))
  pure def baseline(s: Slot): Set[int] = writers.map(w => s.get(w).settlement._1).exclude(Set(0))
  pure def hasBaseline(s: Slot): bool = writers.exists(w => s.get(w).settlement._1 != 0)
  pure def evaluate(s: Slot,w: int,g: int,m: bool): Slot = {
    val c = s.get(w)
    if (c.desired == g and c.matches == m and visible(s).forall(o => s.get(o).desired == g and s.get(o).matches == m)) s
    else s.set(w,{ ...c, evaluation: c.evaluation + 1,
      observed: writers.mapBy(o => if (o != w) s.get(o).evaluation else 0), desired: g, matches: m,
      retained: c.retained.filter(t => t._1 == g) })
  }
  pure def settle(s: Slot,w: int,t: (int,int)): Slot = {
    val c = s.get(w)
    if (c.settlement == t) s else {
      val kept = c.retained.filter(old => desires(s).contains(old._1))
      val previous = if (c.settlement._1 != 0 and desires(s).contains(c.settlement._1)
        and not(kept.exists(old => old._1 == c.settlement._1))) Set(c.settlement) else Set()
      s.set(w,{ ...c, settlement: t, retained: kept.union(previous) })
    }
  }
}

module processor_slot_checks {
  import processor_slots.* from "./processor_slots"
  var state: Slot
  var violation: bool
  var receipts: Set[(int,int,int)]
  action init = all { state' = empty, violation' = false, receipts' = Set() }
  action evaluation = {
    nondet w = writers.oneOf()
    nondet g = Set(1,2).oneOf()
    nondet m = Set(false,true).oneOf()
    val updated = evaluate(state,w,g,m)
    all { state.get(w).evaluation < 2, state' = updated, receipts' = receipts,
      violation' = (violation or updated.get(w).settlement != state.get(w).settlement
        or writers.exists(o => updated.get(w).observed.get(o) > state.get(o).evaluation)) }
  }
  action settlement = {
    nondet w = writers.oneOf()
    nondet g = Set(1,2).oneOf()
    nondet a = Set(1,2).oneOf()
    val updated = settle(state,w,(g,a))
    all { state' = updated, receipts' = receipts.union(Set((w,g,a))), violation' = (violation or updated.get(w).evaluation != state.get(w).evaluation
      or updated.get(w).observed != state.get(w).observed or updated.get(w).desired != state.get(w).desired) }
  }
  action step = any { evaluation, settlement }
  val safety = not(violation) and writers.forall(w =>
    state.get(w).retained.map(t => t._1).size() == state.get(w).retained.size()
    and state.get(w).retained.size() <= 2
    and state.get(w).retained.forall(t => receipts.contains((w,t._1,t._2)))
    and (state.get(w).settlement._1 != 0 implies receipts.contains((w,state.get(w).settlement._1,state.get(w).settlement._2))))
  run lateSettlementKeepsDesiredProofTest = {
    val wanted = evaluate(empty,1,1,true)
    val success = settle(wanted,1,(1,1))
    val late = settle(success,1,(2,2))
    assert(settled(late,1) and late.get(1).evaluation == wanted.get(1).evaluation and baseline(late) == Set(2) and hasBaseline(late))
      .then(assert(evaluate(late,1,1,true) == late))
      .then(assert(not(settled(evaluate(late,1,2,true),1))))
  }
  run observedDesireSuppressionTest = {
    val first = evaluate(empty,1,1,true)
    val updated = evaluate(first,2,2,true)
    assert(visible(updated) == Set(2) and updated.get(2).observed.get(1) == 1)
  }
}
