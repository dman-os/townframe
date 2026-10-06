module router_compiler_repro {
  import router_machine as router from "./router_machine"
  import router_slot as slot from "./router_slot"
  import coordination_fixtures as joins from "./coordination_fixtures"
  pure val session = { node: 1, incarnation: 1, session: 1 }
  pure val registration = { key: session, capabilities: Set(1), capacity: 1, protocol: 1, active: Set() }
  pure val event = { kind: 0, edit: Set(1,2), proof: { request: 0, ticket: joins::base }, obsolete: false,
    claims: List(), key: (1,1,1), identity: { ...slot::absent, claim: 1, rank: 1 }, registration: registration,
    session: session, ticket: joins::base, task: 1, allocation: 1, attempt: 1, reason: 1, running: true }
  pure val leader: router::State = {
    val allowed = router::handle(router::fresh(1),{ ...event, kind: 1 }).state
    val claiming = router::handle(allowed,{ ...event, kind: 6 }).state
    router::handle(claiming,{ ...event, kind: 7 }).state
  }
  pure def offered(cap: int): router::State = {
    val registered = router::handle(leader,{ ...event, kind: 10, registration: { ...registration, capacity: cap } }).state
    val observed = router::handle(registered,{ ...event, kind: 12 }).state
    val proof = observed.proofs.fold({ request: 0, ticket: joins::base },(a,p) => p)
    router::handle(observed,{ ...event, kind: 2, proof: proof }).state
  }
  var state: router::State
  action init = state' = leader
  action step = state' = state
  val safety = state.pool == 1
}
