module task_ticket_checks {
  import task_ticket as ticket from "./task_ticket"
  pure val declaration = { task: 1, pool: 1, opaque: 1, producer: 2,
    capabilities: Set(1), placement: 1, preferred: 1, effect: 0,
    before: -1, after: 2, retention: 1, retainUntil: 3 }
  pure val evidence = { publisher: 1, envelope: 1, witness: 1 }
  pure val base = ticket::fresh(declaration,evidence)
  pure def fact(success: bool, attempt: int): ticket::Fact = { success: success, attempt: attempt, result: attempt, reason: 1 }
  pure def withLane(w: int, q: int, success: bool, attempt: int): ticket::Ticket =
    { ...base, terminal: base.terminal.set(w,{ writer: w, sequence: q, fact: fact(success,attempt) }) }
  pure val inputs = Set(withLane(1,0,false,0),withLane(1,1,false,0),withLane(1,2,true,1),withLane(1,3,false,0),withLane(2,0,true,2),
    { ...base, publishers: Set({ ...evidence, envelope: 2 }) })
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
    and ticket::terminal(state) == history.exists(t => ticket::writers.exists(w => t.terminal.get(w).sequence >= 0))
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
  run zeroSequenceTerminalIsPresentTest = {
    val zero = withLane(1,0,true,1)
    assert(ticket::merge(base,zero).ticket == zero and ticket::terminal(zero))
  }
  run zeroReplayConflictRecoveryTest = {
    val zero = withLane(1,0,false,0)
    val conflict = withLane(1,0,true,1)
    val first = ticket::merge(base,zero)
    val replay = ticket::merge(first.ticket,zero)
    val rejected = ticket::merge(first.ticket,conflict)
    val recovery = ticket::merge(first.ticket,withLane(1,1,true,1))
    assert(first.changed and replay.error == 0 and not(replay.changed)
      and rejected.error == 6 and rejected.ticket == first.ticket
      and recovery.error == 0 and ticket::summary(recovery.ticket) == fact(true,1))
  }
  run optionalZeroAndSummaryOrderingTest = {
    val zeroTime = { ...declaration, before: 0, after: 0, retainUntil: 0 }
    val zeroSuccess = { ...withLane(1,0,true,0), terminal: base.terminal.set(1,
      { writer: 1, sequence: 0, fact: { success: true, attempt: 0, result: -1, reason: 0 } }) }
    val equalAttempt = { ...withLane(2,1,true,0), terminal: base.terminal.set(2,
      { writer: 2, sequence: 1, fact: { success: true, attempt: 0, result: 0, reason: 0 } }) }
    assert(ticket::valid(zeroTime) and not(ticket::valid({ ...zeroTime, after: -1 })))
      .then(assert(ticket::summary(zeroSuccess) == zeroSuccess.terminal.get(1).fact))
      .then(assert(ticket::summary(ticket::merge(zeroSuccess,equalAttempt).ticket) == equalAttempt.terminal.get(2).fact))
      .then(assert(ticket::summary(ticket::merge(withLane(1,0,false,1),withLane(2,1,false,2)).ticket) == fact(false,1)))
  }
  run malformedWriterBindingCurrentBehaviorTest = {
    val malformed = { ...withLane(1,0,false,1), terminal: base.terminal
      .set(1,{ writer: 2, sequence: 0, fact: fact(false,1) })
      .set(2,{ writer: 1, sequence: 0, fact: fact(false,2) }) }
    assert(ticket::merge(base,malformed).error == 0)
      .then(assert(ticket::summary(malformed) == fact(false,1)))
    // With retained writer2, incoming key2 is not less; writer99 would replace.
      .then(assert(ticket::summary({ ...malformed, terminal: malformed.terminal.set(1,
        { writer: 99, sequence: 0, fact: fact(false,1) }) }) == fact(false,2)))
  }
  run validationErrorPrecedenceTest = {
    assert(ticket::merge(base,{ ...base, schema: 4, task: 2 }).error == 1)
      .then(assert(ticket::merge({ ...base, schema: 4 },{ ...base, schema: 4 }).error == 2))
      .then(assert(ticket::merge(base,{ ...base, task: 2 }).error == 3))
      .then(assert(ticket::merge(base,{ ...base, declaration: { ...declaration, opaque: 2 } }).error == 5))
      .then(assert(not(ticket::valid({ ...declaration, before: 3, after: 2 }))))
      .then(assert(ticket::valid({ ...declaration, after: -1 })))
  }
  run guardInvalidBatchPrecedesMutationTest = {
    val remote = { ...withLane(1,2,true,2), publishers: Set({ publisher: 2, envelope: 2, witness: 2 }),
      declaration: { ...declaration, before: 3, after: 2 } }
    val result = ticket::merge(base,remote)
    assert(result.error == 4 and result.ticket == base and not(result.changed))
  }
  run guardSameWriterSuccessSequenceTest = {
    val old = withLane(1,2,true,2)
    val older = withLane(1,1,true,1)
    val newerCancel = withLane(1,3,false,1)
    assert(ticket::merge(old,older).ticket == old and not(ticket::merge(old,older).changed))
      .then(assert(ticket::merge(old,newerCancel).ticket == old))
      .then(assert(ticket::summary(ticket::merge(older,old).ticket) == fact(true,2)))
  }

}
