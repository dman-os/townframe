module coordination_join_checks {
  import task_ticket as ticket from "./task_ticket"
  import router_slot as slot from "./router_slot"
  pure val declaration = { task: 1, pool: 1, opaque: 1, producer: 2,
    capabilities: Set(1), placement: 1, preferred: 1, effect: 0,
    before: -1, after: 2, retention: 1, retainUntil: 3 }
  pure val evidence = { publisher: 1, envelope: 1, witness: 1 }
  pure val base = ticket::fresh(declaration,evidence)
  pure def fact(success: bool, attempt: int): ticket::Fact = { success: success, attempt: attempt, result: attempt, reason: 1 }
  pure def withLane(w: int, q: int, success: bool, attempt: int): ticket::Ticket =
    { ...base, terminal: base.terminal.set(w,{ writer: w, sequence: q, fact: fact(success,attempt) }) }
  pure val inputs = Set(withLane(1,1,false,0),withLane(1,2,true,1),withLane(1,3,false,0),withLane(2,1,true,2))
  // History oracle selects highest success sequence if any success exists,
  // otherwise highest cancellation sequence. It does not call merge/lane.
  pure def oracle(history: Set[ticket::Ticket]): ticket::Ticket = { ...base,
    publishers: history.fold(base.publishers,(es,t) => es.union(t.publishers)),
    terminal: (ticket::writers).mapBy(w => {
      val observed = history.map(t => t.terminal.get(w)).filter(x => x.sequence >= 0)
      val successes = observed.filter(x => x.fact.success)
      val candidates = if (successes.size() != 0) successes else observed
      candidates.fold(ticket::absentTerminal,(a,x) => if (x.sequence > a.sequence) x else a)
    }) }
  var state: ticket::Ticket
  var history: Set[ticket::Ticket]
  action init = all { state' = base, history' = Set() }
  action step = {
    nondet incoming = inputs.oneOf()
    all { state' = ticket::merge(state,incoming).ticket, history' = history.union(Set(incoming)) }
  }
  val safety = state == oracle(history)
  run successSurvivesNewerCancelTest = {
    val success = withLane(1,2,true,1)
    val cancel = withLane(1,3,false,0)
    assert(ticket::merge(success,cancel).ticket == success)
  }
  run olderSuccessDefeatsCancelTest = {
    val success = withLane(1,2,true,1)
    val cancel = withLane(1,3,false,0)
    assert(ticket::merge(cancel,success).ticket == success)
  }
  run equivocationRejectsWholeBatchTest = {
    val local = withLane(1,2,true,1)
    val conflicting = { ...withLane(1,2,false,0), publishers: Set({ ...evidence, envelope: 2 }) }
    val result = ticket::merge(local,conflicting)
    assert(result.error == 6 and result.ticket == local and not(result.changed))
  }
  run samePublisherDistinctContentRetainedTest = {
    val other = { ...base, publishers: Set({ ...evidence, envelope: 2 }) }
    assert(ticket::merge(base,other).ticket.publishers == Set(evidence,{ ...evidence, envelope: 2 }))
  }
  run completeVersionIncludesEvidenceTest = {
    val other = { ...base, publishers: Set({ ...evidence, witness: 2 }) }
    assert(ticket::version(base) != ticket::version(other))
  }
  run successSummaryIndependentOfWriterSequenceTest = {
    val combined = ticket::merge(withLane(1,3,true,1),withLane(2,1,true,2)).ticket
    assert(ticket::summary(combined) == fact(true,2))
  }
  run validationPrecedesDeclarationEqualityTest = {
    val remote = { ...base, declaration: { ...declaration, after: 4 } }
    assert(ticket::merge(base,remote).error == 4)
  }
  run authoritativePlacementValidationTest = {
    assert(not(ticket::valid({ ...declaration, effect: 3 })))
  }
  pure val claim = { ...slot::absent, pool: 1, candidate: 1, incarnation: 1,
    sequence: 2, generation: 2, claim: 1, protocol: 1, rank: 1 }
  run newerGenerationRegressionRejectedTest = {
    val local = slot::merge(slot::empty,claim).slot
    val result = slot::merge(local,{ ...claim, sequence: 3, generation: 1 })
    assert(result.error == 1 and result.slot == local)
  }
  run staleGenerationRegressionIgnoredTest = {
    val local = slot::merge(slot::empty,claim).slot
    val result = slot::merge(local,{ ...claim, sequence: 1, generation: 1 })
    assert(result.error == 0 and result.slot == local)
  }
  run equalSequenceDifferentIncarnationRejectedTest = {
    val local = slot::merge(slot::empty,claim).slot
    val result = slot::merge(local,{ ...claim, incarnation: 2 })
    assert(result.error == 2 and result.slot == local)
  }
  run livenessDoesNotIncludeIncarnationTest = {
    assert(slot::liveKey(claim) == slot::liveKey({ ...claim, incarnation: 2, sequence: 3 }))
  }
  run zeroSequenceTerminalIsPresentTest = {
    val zero = withLane(1,0,true,1)
    assert(ticket::merge(base,zero).ticket == zero and ticket::terminal(zero))
  }
  run zeroSequenceClaimIsInsertedTest = {
    val zero = { ...claim, sequence: 0 }
    assert(slot::merge(slot::empty,zero).slot.get(1) == zero)
  }
}
