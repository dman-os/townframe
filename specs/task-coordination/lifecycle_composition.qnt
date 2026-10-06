// Vertical composition, NOT all-event Router/Worker or native transport proof.
// Reuses source-owned semantic reducers; effects/commit/response are separate.
module lifecycle_composition {
  import task_ticket as ticket from "./task_ticket"
  import coordination_fixtures as fixture from "./coordination_fixtures"
  import router_slot as slot from "./router_slot"
  import router_machine as router from "./router_machine"
  import worker_machine as worker from "./worker_machine"
  import successor_authorization as auth from "./successor_authorization"
  import processor_slots as slots from "./processor_slots"
  import domain_ack_ledger as ledger from "./domain_ack_ledger"

  pure val ids = Set(1,2)
  type State = {
    competing: bool, now: int, online: bool, observer: bool, crashes: int,
    lanes: slot::Slot, takeover: bool, slot: slots::Slot, advanced: bool,
    issued: Set[int], published: Set[int], queued: Set[int], prepared: Set[int],
    running: Set[int], durable: int -> int, results: int -> int,
    outbox: Set[int], hooks: Set[int], sent: Set[int],
    received: Set[int], pending: Set[int], consumed: int -> int,
    acks: Set[int], accepted: Set[int], released: Set[int], tickets: int -> ticket::Ticket,
    starts: Set[(int,int)], commits: Set[(int,int,int)], settlements: Set[(int,int)]
  }
  var s: State
  var depth: int
  pure def authorization(competing: bool, id: int): auth::Authorization =
    if (id == 1) auth::original
    else if (competing) { task: (1,1,2,10,20), nonce: 2, origin: 10, deadline: 20 }
    else { task: (1,1,2,21,31), nonce: 2, origin: 21, deadline: 31 }
  pure def declaration(competing: bool,id: int): ticket::Declaration =
    { ...fixture::declaration, task: id, before: authorization(competing,id).origin,
      after: authorization(competing,id).deadline, retention: 0, retainUntil: -1 }
  pure def claim(node: int,generation: int): slot::Claim =
    { ...slot::absent, pool: 1, candidate: node, incarnation: 1, sequence: generation,
      generation: generation, claim: generation, protocol: 1,
      observed: ids.mapBy(_ => 0), rank: node }
  pure def elected(lanes: slot::Slot): int = router::winner({ ...router::fresh(2),
    lanes: lanes, edit: ids, live: ids.map(n => slot::liveKey(lanes.get(n))) }).candidate
  pure def originalFact(id: int,status: int): ticket::Fact =
    { success: status == 2, attempt: id, result: id, reason: 1 }
  pure def summaryClass(id: int,status: int): int = id * 10 + status
  pure def workerPermit(competing: bool,id: int): bool = {
    val token = { ...worker::noToken, task: id, attempt: id, declaration: declaration(competing,id) }
    val attempt = { ...worker::absent, task: id, attempt: id, persisted: true,
      declaration: declaration(competing,id), startEmitted: Set(token) }
    worker::dispatchPending({ ...worker::fresh(2), attempts: Set(attempt) },token)
  }
  pure def initial(competing: bool): State = {
    val tickets = ids.mapBy(id => ticket::fresh(declaration(competing,id),
      { publisher: id, envelope: id, witness: id }))
    { competing: competing, now: 10, online: true, observer: false, crashes: 0,
      lanes: slot::merge(slot::empty,claim(1,1)).slot, takeover: false,
      slot: slots::evaluate(slots::empty,1,1,true), advanced: false,
      issued: Set(1), published: Set(), queued: Set(), prepared: Set(), running: Set(),
      durable: ids.mapBy(_ => 0), results: ids.mapBy(_ => 0),
      outbox: Set(), hooks: Set(), sent: Set(), received: Set(), pending: Set(),
      consumed: ids.mapBy(_ => 0), acks: Set(), accepted: Set(), released: Set(), tickets: tickets,
      starts: Set(), commits: Set(), settlements: Set() }
  }
  action initProfile(competing: bool): bool = s' = initial(competing)
  action init = { nondet competing = Set(false,true).oneOf()
    all { initProfile(competing), depth' = 0 } }
  // Concurrent independent authorization or renewed unmet capture. No deadline mutation.
  action issue = all { s.online, not(s.issued.contains(2)),
    s.competing or (s.now >= 21 and not(slots::settled(s.slot,1))),
    s' = { ...s, issued: s.issued.union(Set(2)) } }
  action publish(id: int): bool = all { s.online, s.issued.contains(id), not(s.published.contains(id)),
    s' = { ...s, published: s.published.union(Set(id)) } }
  action enqueue(id: int): bool = all { s.online, s.published.contains(id), not(s.queued.contains(id)),
    s.now >= authorization(s.competing,id).origin, s.now <= authorization(s.competing,id).deadline,
    not(slots::settled(s.slot,1)), slots::desires(s.slot).contains(1), elected(s.lanes) != 0,
    auth::accepted(1,1,authorization(s.competing,id),authorization(s.competing,id).task,
      declaration(s.competing,id).after,10,true),
    s' = { ...s, queued: s.queued.union(Set(id)) } }
  // Repository-owned accepted request survives loss of the submitting pool caller.
  action persist(id: int): bool = all { s.online, s.queued.contains(id), not(s.prepared.contains(id)),
    s' = { ...s, prepared: s.prepared.union(Set(id)) } }
  action start(id: int): bool = all { s.online, s.prepared.contains(id), s.durable.get(id) == 0,
    not(s.running.contains(id)), s.now <= authorization(s.competing,id).deadline,
    not(slots::settled(s.slot,1)), slots::desires(s.slot).contains(1), workerPermit(s.competing,id),
    s' = { ...s, running: s.running.union(Set(id)), starts: s.starts.union(Set((id,s.now))) } }
  // SQL write is the success fence; actual winner, result and outbox commit together.
  action completeAs(id: int,requested: int): bool = {
    val status = if (requested == 2 and s.now > authorization(s.competing,id).deadline) 3 else requested
    all { s.online, s.running.contains(id), s.durable.get(id) == 0,
      s' = { ...s, durable: s.durable.set(id,status), results: s.results.set(id,id),
        outbox: s.outbox.union(Set(id)), running: s.running.exclude(Set(id)),
        commits: s.commits.union(Set((id,s.now,status))) } }
  }
  action complete(id: int): bool = { nondet requested = Set(2,3,4).oneOf()
    completeAs(id,requested) }
  // Independent host expiry/control or obsolescence winner; no pool observer needed.
  action expire(id: int): bool = all { s.online, s.prepared.contains(id), s.durable.get(id) == 0,
    s.now > authorization(s.competing,id).deadline or slots::settled(s.slot,1) or not(slots::desires(s.slot).contains(1)),
    s' = { ...s, durable: s.durable.set(id,3), outbox: s.outbox.union(Set(id)),
      running: s.running.exclude(Set(id)), commits: s.commits.union(Set((id,s.now,3))) } }
  // Local success writes exact capture settlement, never current desired capture.
  action hook(id: int): bool = all { s.online, s.outbox.contains(id), not(s.hooks.contains(id)),
    s' = { ...s, hooks: s.hooks.union(Set(id)),
      slot: if (s.durable.get(id) == 2) slots::settle(s.slot,1,(1,id)) else s.slot,
      settlements: if (s.durable.get(id) == 2) s.settlements.union(Set((1,id))) else s.settlements } }
  // Stable (parent job, request-id) message identity; snapshot retains consumed IDs too.
  action reply(id: int): bool = all { s.online, s.outbox.contains(id), s.hooks.contains(id), not(s.sent.contains(id)),
    s' = { ...s, sent: s.sent.union(Set(id)), received: s.received.union(Set(id)),
      pending: if (s.received.contains(id)) s.pending else s.pending.union(Set(id)) } }
  action consume(id: int): bool = all { s.online, s.pending.contains(id),
    s' = { ...s, pending: s.pending.exclude(Set(id)), consumed: s.consumed.set(id,s.consumed.get(id)+1) } }
  // Progress/dependency hooks are abstracted idempotent completed effects here.
  action finishDelivery(id: int): bool = all { s.online, s.outbox.contains(id), s.hooks.contains(id), s.sent.contains(id),
    s' = { ...s, outbox: s.outbox.exclude(Set(id)) } }
  // Adapter waits for delivery before receipt; response/publication/worker release split.
  action acknowledge(id: int): bool = all { s.online, s.observer, s.durable.get(id) != 0, s.durable.get(id) != 4,
    not(s.outbox.contains(id)), not(s.acks.contains(summaryClass(id,s.durable.get(id)))),
    s' = { ...s, acks: ledger::commit(s.acks,summaryClass(id,s.durable.get(id))) } }
  action terminal(id: int): bool = {
    val fact = originalFact(id,s.durable.get(id))
    val old = s.tickets.get(id)
    val update = { ...old, terminal: old.terminal.set(1,{ writer: 1, sequence: 1, fact: fact }) }
    all { s.online, s.observer, s.acks.contains(summaryClass(id,s.durable.get(id))), not(s.accepted.contains(id)),
      s' = { ...s, accepted: s.accepted.union(Set(id)), tickets: s.tickets.set(id,ticket::merge(old,update).ticket) } }
  }
  action release(id: int): bool = all { s.online, s.accepted.contains(id), not(s.released.contains(id)),
    s' = { ...s, released: s.released.union(Set(id)) } }
  action advanceSource = all { s.online, not(s.advanced),
    s' = { ...s, advanced: true, slot: slots::evaluate(s.slot,2,2,true) } }
  action takeover = all { s.online, not(s.takeover),
    s' = { ...s, takeover: true, lanes: slot::merge(s.lanes,claim(2,2)).slot } }
  action tickTo(later: int): bool = all { later > s.now, s' = { ...s, now: later } }
  action tick = { nondet later = Set(20,21,31,32).oneOf()
    tickTo(later) }
  action observe = all { not(s.observer), s' = { ...s, observer: true } }
  action crash = all { s.online, s.crashes == 0,
    s' = { ...s, online: false, observer: false, crashes: 1, queued: s.queued.intersect(s.prepared), hooks: Set(), sent: Set() } }
  action restart = all { not(s.online), s' = { ...s, online: true } }
  action idle = s' = s
  action taskStep(id: int): bool = any { publish(id), enqueue(id), persist(id), start(id), complete(id), expire(id),
    hook(id), reply(id), consume(id), finishDelivery(id), acknowledge(id), terminal(id), release(id) }
  action baseStep = { nondet id = ids.oneOf()
    any { taskStep(id), issue, advanceSource, takeover, tick, observe, crash, restart, idle } }

  action step = all { baseStep, depth' = depth }
  // Depth cutoff retains every transition kind. Only bounded safety uses it.
  action boundedStep = any { all { depth < 12, baseStep, depth' = depth + 1 },
    all { depth == 12, idle, depth' = depth } }

  // Independent ghost evidence, not merely rechecking enabled action guards.
  val persistenceBeforeStart = s.starts.forall(x => s.prepared.contains(x._1))
  val finiteAuthorization = s.starts.forall(x => x._2 >= authorization(s.competing,x._1).origin
    and x._2 <= authorization(s.competing,x._1).deadline)
    and s.commits.forall(x => x._3 != 2 or x._2 <= authorization(s.competing,x._1).deadline)
  val exactSettlement = s.settlements.forall(x => x._1 == 1 and s.durable.get(x._2) == 2)
    and not(slots::settled(s.slot,2))
  val receiptAfterDelivery = ids.forall(id => s.acks.contains(summaryClass(id,s.durable.get(id))) implies
    not(s.outbox.contains(id)) and s.received.contains(id)
      and (s.durable.get(id) != 2 or s.settlements.contains((1,id))))
  val duplicateMessageSafety = ids.forall(id => s.consumed.get(id) <= 1
    and (s.pending.contains(id) or s.consumed.get(id) != 0 implies s.received.contains(id)))
  val originalFactBeforeRelease = s.released.subseteq(s.accepted) and s.accepted.forall(id =>
    s.acks.contains(summaryClass(id,s.durable.get(id)))
      and ticket::summary(s.tickets.get(id)) == originalFact(id,s.durable.get(id)))
  val retainedResult = ids.forall(id => s.durable.get(id) == 2 implies s.results.get(id) == id)
  val durableDeliveryOwner = ids.forall(id => s.durable.get(id) == 0 or
    s.outbox.contains(id) or s.received.contains(id))
  val safety = persistenceBeforeStart and finiteAuthorization and exactSettlement and receiptAfterDelivery
    and duplicateMessageSafety and originalFactBeforeRelease and durableDeliveryOwner and retainedResult
  // Stable live repository, eventual clock progress & observer; per-effect fairness.
  temporal fairDelivery = ids.forall(id => weakFair(hook(id),s) and weakFair(reply(id),s)
    and weakFair(consume(id),s) and weakFair(finishDelivery(id),s))
  temporal fairAttempts = ids.forall(id => weakFair(persist(id),s) and weakFair(expire(id),s)
    and weakFair(acknowledge(id),s) and weakFair(terminal(id),s) and weakFair(release(id),s))
  temporal environment = eventually(always(s.online and s.observer and s.now == 32))
  temporal progress = (environment and fairDelivery and fairAttempts) implies
    ids.forall(id => always(s.prepared.contains(id) implies eventually(s.released.contains(id) or (s.durable.get(id) == 4 and not(s.outbox.contains(id))))))
  temporal observerIndependentDelivery = (eventually(always(s.online)) and fairDelivery) implies
    ids.forall(id => always(s.durable.get(id) != 0 implies eventually(not(s.outbox.contains(id)))))
  run crashAfterConsumedReplyTest = initProfile(true).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(start(1)).then(completeAs(1,2)).then(hook(1)).then(reply(1)).then(consume(1))
    .then(crash).then(restart).then(hook(1)).then(reply(1)).then(finishDelivery(1))
    .then(all { assert(safety and s.consumed.get(1) == 1 and not(s.pending.contains(1))), idle })
  run competingAndTakeoverTest = initProfile(true).then(issue).then(publish(1)).then(publish(2))
    .then(enqueue(1)).then(enqueue(2)).then(takeover)
    .then(all { assert(s.queued == ids and elected(s.lanes) == 2 and authorization(true,1).task != authorization(true,2).task), idle })
  run successorPreservesOriginalTest = initProfile(false).then(tickTo(21)).then(issue)
    .then(all { assert(authorization(false,1) == auth::original and authorization(false,2).deadline == 31 and s.issued == ids), idle })
  run lateSettlementExactCaptureTest = initProfile(true).then(publish(1)).then(enqueue(1))
    .then(persist(1)).then(start(1)).then(completeAs(1,2)).then(advanceSource).then(hook(1))
    .then(all { assert(exactSettlement and slots::settled(s.slot,1) and not(slots::settled(s.slot,2))), idle })
  run sqlExpiryWinnerTest = initProfile(true).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(start(1)).then(tickTo(21)).then(completeAs(1,2))
    .then(all { assert(finiteAuthorization and s.durable.get(1) == 3 and s.outbox.contains(1)), idle })
  run receiptRequiresIntegrationTest = initProfile(true).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(start(1)).then(completeAs(1,2)).then(hook(1)).then(reply(1)).then(finishDelivery(1))
    .then(observe).then(acknowledge(1)).then(terminal(1)).then(release(1))
    .then(all { assert(safety and s.released.contains(1)), idle })
  run crashAfterAckBeforePublicationTest = initProfile(true).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(start(1)).then(completeAs(1,2)).then(hook(1)).then(reply(1)).then(finishDelivery(1))
    .then(observe).then(acknowledge(1)).then(crash).then(restart).then(observe)
    .then(terminal(1)).then(release(1))
    .then(all { assert(safety and s.released.contains(1) and s.acks == Set(12)), idle })
  run failureDeliveryWithoutObserverTest = initProfile(false).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(tickTo(21)).then(expire(1)).then(crash).then(restart).then(hook(1)).then(reply(1))
    .then(finishDelivery(1))
    .then(all { assert(safety and not(s.observer) and s.received.contains(1) and s.acks == Set()), idle })
  // Repository queue ownership outlives caller cancellation, not process crash.
  run crashDropsUncommittedAdmissionTest = initProfile(true).then(publish(1)).then(enqueue(1))
    .then(crash).then(restart)
    .then(all { assert(not(s.queued.contains(1)) and not(s.prepared.contains(1))), idle })
  run crashRetainsAcknowledgedAdmissionTest = initProfile(true).then(publish(1)).then(enqueue(1)).then(persist(1))
    .then(crash).then(restart)
    .then(all { assert(s.queued.contains(1) and s.prepared.contains(1)), idle })
  // Exhaustive recovered-terminal profile: same transition alphabet/properties,
  // starts at a retained SQL commit, not a claim about reaching this cut fairly.
  action initReplay = {
    nondet firstStatus = Set(2,3,4).oneOf()
    nondet secondStatus = Set(2,3,4).oneOf()
    val base = initial(true)
    all { depth' = 0, s' = { ...base, now: 32, issued: ids, published: ids, queued: ids, prepared: ids,
      durable: Map(1 -> firstStatus,2 -> secondStatus), results: ids.mapBy(id => id), outbox: ids,
      commits: Set((1,20,firstStatus),(2,20,secondStatus)) } }
  }
  temporal unfairDelivery = eventually(always(s.online)) implies
    ids.forall(id => always(s.durable.get(id) != 0 implies eventually(not(s.outbox.contains(id)))))
}

