module router_checks {
  import router_machine as router from "./router_machine"
  import router_slot as slot from "./router_slot"
  import coordination_fixtures as joins from "./coordination_fixtures"
  pure val session = { node: 1, incarnation: 1, session: 1 }
  pure val registration = { key: session, capabilities: Set(1), capacity: 1, protocol: 1, active: Set() }
  pure val event = { kind: 0, edit: Set(1,2), proof: { request: 0, ticket: joins::base }, obsolete: false,
    claims: List(), key: (1,1,1), identity: { ...slot::absent, claim: 1, rank: 1 }, registration: { key: session, capabilities: Set(1), capacity: 1, protocol: 1, active: List() },
    session: session, ticket: joins::base, task: 1, allocation: 1, attempt: 1, reason: 1, running: true }
  pure val leader: router::State = {
    val allowed = router::handle(router::fresh(1),{ ...event, kind: 1 }).state
    val claiming = router::handle(allowed,{ ...event, kind: 6 }).state
    router::handle(claiming,{ ...event, kind: 7 }).state
  }
  pure def offered(cap: int): router::State = {
    val registered = router::handle(leader,{ ...event, kind: 10, registration: { ...event.registration, capacity: cap, active: List() } }).state
    val observed = router::handle(registered,{ ...event, kind: 12 }).state
    val proof = observed.proofs.fold({ request: 0, ticket: joins::base },(a,p) => p)
    router::handle(observed,{ ...event, kind: 2, proof: proof }).state
  }
  run unpersistedClaimCannotRouteTest = {
    val permitted = router::handle(router::fresh(1),{ ...event, kind: 1 }).state
    val result = router::handle(permitted,{ ...event, kind: 6 })
    assert(not(result.state.routing) and result.actions.nth(0).kind == 2 and leader.routing)
  }
  run durableSequenceGapCanConfirmClaimTest = {
    val allowed = router::handle(router::fresh(1),{ ...event, kind: 1 }).state
    val claiming = router::handle(allowed,{ ...event, kind: 6 }).state
    val durable = { ...claiming.own, sequence: 3 }
    val merged = router::handle(claiming,{ ...event, kind: 3, claims: List(durable) }).state
    assert(router::handle(merged,{ ...event, kind: 7 }).state.routing)
  }
  run newerUnsupportedLiveClaimDefeatsLeaderTest = {
    val foreign = { ...leader.own, candidate: 2, generation: 2, claim: 2, protocol: 2, rank: 2 }
    val merged = router::handle(leader,{ ...event, kind: 3, claims: List(foreign) }).state
    val live = router::handle(merged,{ ...event, kind: 4, key: (2,2,2) }).state
    assert(not(live.routing))
  }
  run authorityLossFencesPendingStartsTest = {
    val accepted = router::handle(offered(1),{ ...event, kind: 14 }).state
    val denied = router::handle(accepted,{ ...event, kind: 1, edit: Set(2) }).state
    assert(not(router::startPending(denied,session,1,1)) and denied.allocations == Set())
  }
  run duplicateAcceptanceMayRepeatRawStartTest = {
    val accepted = router::handle(offered(1),{ ...event, kind: 14 }).state
    assert(router::handle(accepted,{ ...event, kind: 14 }).actions.nth(0).kind == 6)
  }
  run wrongSessionAcceptanceCannotOwnAllocationTest = {
    val s = offered(1)
    assert(router::handle(s,{ ...event, kind: 14, session: { ...session, session: 2 } }).state == s)
  }
  run acceptedDeclineDoesNotReleaseOwnershipTest = {
    val s = router::handle(offered(1),{ ...event, kind: 14 }).state
    assert(router::handle(s,{ ...event, kind: 15 }).state == s)
  }
  run cancellationRetainsOwedWorkTest = {
    val s = offered(1)
    val cancelled = router::handle(s,{ ...event, kind: 13 }).state
    assert(cancelled.allocations.size() == 1 and cancelled.allocations.forall(a => a.cancel))
  }
  run lowerCapacityHintDoesNotEraseActiveWorkTest = {
    val active = List({ task: 1, attempt: 1, allocation: 0 },{ task: 2, attempt: 2, allocation: 0 })
    val r = router::handle(leader,{ ...event, kind: 10, registration: { ...event.registration, active: active } })
    assert(r.state.sessions.exists(s => router::owed(r.state,s) == 2) and r.actions == List())
  }
  run onlyUnavailableClearedByCapacityChangeTest = {
    val s = { ...leader, sessions: Set(registration), blocked: Set(
      { session: session, task: 1, reason: 7 },{ session: session, task: 2, reason: 2 }) }
    val r = router::handle(s,{ ...event, kind: 10, registration: { ...event.registration, capacity: 2, active: List() } }).state
    assert(r.blocked == Set({ session: session, task: 2, reason: 2 }))
  }
  run staleObsolescenceCannotDeleteNewTicketTest = {
    val first = router::handle(leader,{ ...event, kind: 12 }).state
    val old = first.proofs.fold({ request: 0, ticket: joins::base },(a,p) => p)
    val second = router::handle(first,{ ...event, kind: 12, ticket: { ...joins::base, publishers: Set({ publisher: 1, envelope: 2, witness: 1 }) } }).state
    assert(router::handle(second,{ ...event, kind: 2, proof: old, obsolete: true }).state == second)
  }
  run guardClaimFailureIdentityTest = {
    val allowed = router::handle(router::fresh(1),{ ...event, kind: 1 }).state
    val proposed = router::handle(allowed,{ ...event, kind: 6 }).state
    val stale = router::handle(proposed,{ ...event, kind: 8, key: (2,1,1) })
    val failed = router::handle(proposed,{ ...event, kind: 8 })
    assert(stale.state == proposed and stale.actions == List()
      and failed.state.own == slot::absent and failed.state.lanes == proposed.lanes
      and not(failed.state.routing))
      .then(assert(router::handle(leader,{ ...event, kind: 8 }).state == leader))
  }
  run guardHeartbeatAuthorityAndExpiryTest = {
    val denied = router::handle(leader,{ ...event, kind: 4, key: (3,1,1) })
    val foreign = { ...leader.own, candidate: 2, generation: 2, claim: 2, rank: 2 }
    val merged = router::handle(leader,{ ...event, kind: 3, claims: List(foreign) }).state
    val live = router::handle(merged,{ ...event, kind: 4, key: (2,2,2) }).state
    val expired = router::handle(live,{ ...event, kind: 5, key: (2,2,2) }).state
    assert(denied.state == leader and denied.actions == List() and not(live.routing)
      and not(expired.live.contains((2,2,2))))
      .then(assert(router::handle(leader,{ ...event, kind: 9 }).actions.nth(0).kind == 3))
  }
  run guardRegistrationReasonsTest = {
    val denied = router::handle(leader,{ ...event, kind: 10,
      registration: { ...event.registration, key: { ...session, node: 3 }, active: List() } })
    val unsupported = router::handle(leader,{ ...event, kind: 10,
      registration: { ...event.registration, protocol: 2, active: List() } })
    assert(denied.state == leader and denied.actions.nth(0).reason == 7
      and unsupported.state == leader and unsupported.actions.nth(0).reason == 5)
  }
  run guardCancelledAcceptanceTest = {
    val removed = router::handle(offered(1),{ ...event, kind: 13 }).state
    val accepted = router::handle(removed,{ ...event, kind: 14 })
    assert(accepted.actions.nth(0).kind == 7 and accepted.state.allocations.size() == 1
      and not(router::startPending(accepted.state,session,1,1)))
  }
  run guardStaleAttemptOutcomeTest = {
    val accepted = router::handle(offered(1),{ ...event, kind: 14 }).state
    val stale = router::handle(accepted,{ ...event, kind: 16, attempt: 2, running: false })
    val exact = router::handle(accepted,{ ...event, kind: 16, running: false })
    assert(stale.state == accepted and stale.actions == List()
      and exact.actions.nth(0).kind == 5 and exact.actions.nth(0).allocation == 2)
  }
  run guardRunningReportDoesNotOverwriteTest = {
    val registered = { ...leader, sessions: Set({ ...registration,
      active: Set({ task: 1, attempt: 1, allocation: 0 }) }) }
    assert(router::handle(registered,{ ...event, kind: 16, attempt: 2 }).state == registered)
      .then(assert(router::handle(registered,{ ...event, kind: 16, session: { ...session, session: 2 } }).state == registered))
  }
  run guardReadinessReasonFenceTest = {
    val blocked = { ...leader, sessions: Set(registration), blocked: Set(
      { session: session, task: 1, reason: 1 },{ session: session, task: 2, reason: 7 }) }
    val unknown = router::handle(blocked,{ ...event, kind: 17, session: { ...session, session: 2 } })
    val ready = router::handle(blocked,{ ...event, kind: 17 })
    assert(unknown.state == blocked and ready.state.blocked == Set({ session: session, task: 2, reason: 7 })
      and router::handle(blocked,{ ...event, kind: 17, task: 2 }).state == blocked)
  }
  run guardSessionCloseReoffersTest = {
    val second = { ...registration, key: { ...session, session: 2 } }
    val registered = router::handle(offered(1),{ ...event, kind: 10, registration: { ...event.registration, key: second.key } }).state
    val closed = router::handle(registered,{ ...event, kind: 11 })
    assert(closed.actions.nth(0).kind == 5 and closed.actions.nth(0).session == second.key
      and closed.actions.nth(0).allocation == 2 and closed.state.sessions == Set(second))
  }
  run guardAllocationConstraintsTest = {
    val t = { ...joins::base, declaration: { ...joins::base.declaration, capabilities: Set(2) } }
    val s = { ...leader, sessions: Set(registration), pending: Set(t) }
    assert(router::handle(s,{ ...event, kind: 18 }).actions == List())
      .then(assert(router::handle({ ...s, pending: Set({ ...t,
        declaration: { ...t.declaration, capabilities: Set(), placement: 2, preferred: 2 } }) },
        { ...event, kind: 18 }).actions == List()))
  }
  run guardWrongAcceptedAttemptTest = {
    val accepted = router::handle(offered(1),{ ...event, kind: 14 }).state
    val stale = router::handle(accepted,{ ...event, kind: 14, attempt: 2 })
    assert(stale.state == accepted and stale.actions == List())
  }
  run guardOrderedDuplicateRegistrationTest = {
    val first = { task: 1, attempt: 1, allocation: 1 }
    val last = { task: 1, attempt: 2, allocation: 0 }
    val forward = router::handle(leader,{ ...event, kind: 10,
      registration: { ...event.registration, active: List(first,last) } }).state
    val reverse = router::handle(leader,{ ...event, kind: 10,
      registration: { ...event.registration, active: List(last,first) } }).state
    assert(forward.sessions == Set({ ...registration, active: Set(last) })
      and reverse.sessions == Set({ ...registration, active: Set(first) }))
      .then(assert(router::handle(forward,{ ...event, kind: 16 }).state == forward))
      .then(assert(router::owed(forward,{ ...registration, active: Set(last) }) == 1))
  }
  run guardCapacityZeroAndAllocationDedupTest = {
    val accepted = router::handle(offered(1),{ ...event, kind: 14 }).state
    val explicit = { ...registration, active: Set({ task: 1, attempt: 2, allocation: 1 }) }
    val fallback = { ...registration, active: Set({ task: 1, attempt: 1, allocation: 0 }) }
    val different = { ...registration, active: Set({ task: 1, attempt: 2, allocation: 0 }) }
    assert(router::owed(accepted,explicit) == 1 and router::owed(accepted,fallback) == 1
      and router::owed(accepted,different) == 2 and offered(0).allocations == Set())
  }
  run guardAllocationTieOrderingTest = {
    val newer = { ...registration, key: { ...session, session: 2 } }
    val r = router::handle({ ...leader, sessions: Set(registration,newer), pending: Set(joins::base) },
      { ...event, kind: 18 })
    assert(r.actions.nth(0).session == session)
  }
  var state: router::State
  var remaining: Set[int]
  action init = all { state' = offered(2), remaining' = 1.to(18) }
  action step = {
    nondet kind = remaining.union(Set(0)).oneOf()
    nondet stale = Set(false,true).oneOf()
    val proof = state.proofs.fold({ request: 0, ticket: joins::base },(a,p) => p)
    val r = if (kind == 0) { state: state, actions: List() }
      else router::handle(state,{ ...event, kind: kind, proof: proof,
        session: if (stale) { ...session, session: 2 } else session })
    all { state' = r.state, remaining' = remaining.exclude(Set(kind)) }
  }
  val safety = state.allocations.forall(a => state.allocations.forall(b => a == b or a.task != b.task))
    and (state.routing implies (state.persisted and state.edit.contains(state.node) and router::winner(state) == state.own))
}
