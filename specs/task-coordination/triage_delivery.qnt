// refresh_processors retains an enabled actor across either manifest/domain Pending.
module triage_delivery {
  import triage_activation as activation from "./triage_activation"
  import processor_slots as slots from "./processor_slots"
  type State = { target: int, manifest: bool, rejected: bool, domain: bool,
    actor: bool, ack: activation::Ack, pending: bool, revision: int,
    evaluated: bool, terminal: bool, outbox: bool, observed: bool,
    settled: bool, receipt: bool, online: bool, crashed: bool, slot: slots::Slot }
  var s: State
  pure val initial = { target: 1, manifest: true, rejected: false, domain: false,
    actor: true, ack: { target: 1, boundary: 1, processors: 1 }, pending: false,
    revision: 1, evaluated: false, terminal: false, outbox: false, observed: false,
    settled: false, receipt: false, online: true, crashed: false,
    slot: slots::evaluate(slots::empty,1,1,true) }
  action init = s' = initial
  action manifestPending = all { s.manifest, s' = { ...s, manifest: false } }
  action materializeManifest = all { not(s.manifest), s' = { ...s, manifest: true } }
  action materializeDomain = all { not(s.domain), s' = { ...s, domain: true } }
  action refresh = {
    val enabledPending = s.target != 0 and not(s.rejected) and (not(s.manifest) or not(s.domain))
    val installed = activation::install(s.ack,s.target,s.manifest and s.target != 0 and not(s.rejected),
      s.rejected,s.revision,true,1)
    all { s.online, s' = { ...s, ack: installed.ack, pending: enabledPending,
      actor: if (enabledPending) s.actor else s.target != 0 and not(s.rejected) } }
  }
  action disable = all { s.target != 0, s' = { ...s, target: 0 } }
  action reject = all { not(s.rejected), s' = { ...s, rejected: true } }
  action source = all { s.revision == 1, s' = { ...s, revision: 2 } }
  action evaluate = all { s.online, s.actor, s.manifest, s.domain, s.target == 1, not(s.rejected),
    activation::eligible(s.ack,s.revision,s.pending), not(s.evaluated),
    s' = { ...s, evaluated: true } }
  action complete = all { s.online, s.evaluated, not(s.terminal),
    s' = { ...s, terminal: true, outbox: true } }
  action producerObserve = all { s.online, s.terminal, not(s.observed), s' = { ...s, observed: true } }
  // Captured domain readiness is independent of producer observation/activation.
  action settle = all { s.online, s.terminal, s.domain, not(s.settled),
    s' = { ...s, settled: true, slot: slots::settle(s.slot,1,(1,1)) } }
  action delivered = all { s.online, s.outbox, s.settled,
    s' = { ...s, outbox: false } }
  action receipt = all { s.online, s.observed, s.terminal, not(s.outbox), not(s.receipt),
    s' = { ...s, receipt: true } }
  action domainPending = all { s.domain, s' = { ...s, domain: false } }
  action crash = all { s.online, not(s.crashed), s' = { ...s, online: false, crashed: true, observed: false } }
  action restart = all { not(s.online), s' = { ...s, online: true } }
  action idle = s' = s
  action step = any { manifestPending, materializeManifest, materializeDomain, refresh, disable, reject,
    source, evaluate, complete, producerObserve, settle, delivered, receipt, domainPending, crash, restart, idle }
  val safety = (s.receipt implies s.settled and not(s.outbox) and slots::settled(s.slot,1))
    and not(slots::settled(s.slot,2)) and (s.settled implies s.terminal)
  temporal progress = (eventually(always(s.online and s.domain))
    and weakFair(settle,s) and weakFair(delivered,s) and weakFair(producerObserve,s) and weakFair(receipt,s))
    implies always(s.terminal implies eventually(s.receipt))
  run enabledManifestPendingRetainsActorTest = init.then(manifestPending).then(refresh)
    .then(all { assert(s.actor and s.pending and s.ack == initial.ack), idle })
  run rejectionAndDisableWithdrawTest = init.then(reject).then(refresh)
    .then(all { assert(not(s.actor) and not(s.pending)), idle })
    .then(init).then(disable).then(refresh)
    .then(all { assert(not(s.actor) and s.ack == activation::absent), idle })
  run terminalWaitsForCapturedDomainTest = init.then(materializeDomain).then(refresh).then(source).then(evaluate)
    .then(complete).then(domainPending).then(producerObserve).then(crash).then(restart)
    .then(producerObserve).then(materializeDomain).then(settle).then(delivered).then(receipt)
    .then(all { assert(safety and s.receipt and s.settled), idle })
}
