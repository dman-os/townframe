// RegisterStore local handoff: retained source0 directly to selected0/3.
// No deletion/retirement transition; each node knows only its local sources.
module register_handoff {
  import register as reg from "./register"
  pure val nodes = Set(1,2)
  pure val generations = Set(0,3)
  pure val records = Set(1,2)
  pure def representation(id: int): reg::Rep = reg::rep(id,1,id,1,0,id,id,1)
  pure def projected(history: Set[int]): reg::Register = history.fold(reg::empty,
    (state,id) => reg::join(state,reg::singleton(representation(id))))
  type State = { selected: int -> int, online: Set[int], crashed: Set[int],
    data: int -> (int -> Set[int]), registers: int -> (int -> reg::Register),
    checkpoint: int -> (int -> Set[int]), batch: int -> Set[int], destination: int -> int,
    opened: Set[int], writes: Set[(int,int,int)], copied: Set[(int,int,int)] }
  var s: State
  pure val initial = { selected: nodes.mapBy(_ => 0), online: nodes, crashed: Set(),
    data: nodes.mapBy(n => generations.mapBy(g => if (g == 0) Set(n) else Set())),
    registers: nodes.mapBy(n => generations.mapBy(g => if (g == 0) projected(Set(n)) else reg::empty)),
    checkpoint: nodes.mapBy(_ => generations.mapBy(_ => Set())), batch: nodes.mapBy(_ => Set()),
    destination: nodes.mapBy(_ => 0), opened: Set(), writes: Set(), copied: Set() }
  action init = s' = initial
  action selectCurrent(n: int): bool = all { s.online.contains(n), s.selected.get(n) == 0,
    s' = { ...s, selected: s.selected.set(n,3) } }
  // Old writer0 is valid only under the transaction's selected binding.
  action oldWrite(n: int,id: int): bool = all { s.online.contains(n), s.selected.get(n) == 0,
    not(s.data.get(n).get(0).contains(id)),
    s' = { ...s, data: s.data.set(n,s.data.get(n).set(0,s.data.get(n).get(0).union(Set(id)))),
      registers: s.registers.set(n,s.registers.get(n).set(0,reg::join(s.registers.get(n).get(0),projected(Set(id))))),
      writes: s.writes.union(Set((n,s.selected.get(n),id))) } }
  action readSource(n: int): bool = all { s.online.contains(n), not(s.opened.contains(n)), s.selected.get(n) == 3,
    s' = { ...s, opened: s.opened.union(Set(n)), destination: s.destination.set(n,3),
      batch: s.batch.set(n,s.data.get(n).get(0).exclude(s.checkpoint.get(n).get(3))) } }
  action publish(n: int,id: int): bool = all { s.online.contains(n), s.opened.contains(n), s.batch.get(n).contains(id),
    s.destination.get(n) == s.selected.get(n),
    s' = { ...s, data: s.data.set(n,s.data.get(n).set(3,s.data.get(n).get(3).union(Set(id)))),
      registers: s.registers.set(n,s.registers.get(n).set(3,reg::join(s.registers.get(n).get(3),projected(Set(id))))),
      copied: s.copied.union(Set((n,3,id))) } }
  action checkpoint(n: int): bool = all { s.online.contains(n), s.opened.contains(n),
    s.destination.get(n) == s.selected.get(n), s.batch.get(n).subseteq(s.data.get(n).get(3)),
    s' = { ...s, checkpoint: s.checkpoint.set(n,s.checkpoint.get(n).set(3,s.checkpoint.get(n).get(3).union(s.batch.get(n)))),
      opened: s.opened.exclude(Set(n)), batch: s.batch.set(n,Set()) } }
  action crash(n: int): bool = all { s.online.contains(n), not(s.crashed.contains(n)),
    s' = { ...s, online: s.online.exclude(Set(n)), crashed: s.crashed.union(Set(n)),
      opened: s.opened.exclude(Set(n)), batch: s.batch.set(n,Set()) } }
  action restart(n: int): bool = all { not(s.online.contains(n)), s' = { ...s, online: s.online.union(Set(n)) } }
  action idle = s' = s
  action step = { nondet n = nodes.oneOf(); nondet id = records.oneOf()
    any { selectCurrent(n), oldWrite(n,id), readSource(n), publish(n,id), checkpoint(n), crash(n), restart(n), idle } }
  val checkpointAfterPublication = nodes.forall(n => s.checkpoint.get(n).get(3).subseteq(s.data.get(n).get(3)))
  val oldWriterFenced = s.writes.forall(w => w._2 == 0)
  val exactCiphertext = nodes.forall(n => generations.forall(g => s.registers.get(n).get(g) == projected(s.data.get(n).get(g))))
  val localKnowledge = nodes.forall(n => s.data.get(n).get(3).subseteq(s.data.get(n).get(0)))
  val safety = checkpointAfterPublication and oldWriterFenced and exactCiphertext and localKnowledge
  temporal localTransfers = (eventually(always(nodes.forall(n => s.online.contains(n) and s.selected.get(n) == 3)))
    and nodes.forall(n => weakFair(readSource(n),s) and weakFair(checkpoint(n),s)
      and records.forall(id => weakFair(publish(n,id),s)))) implies
    nodes.forall(n => eventually(s.data.get(n).get(0).subseteq(s.checkpoint.get(n).get(3))))
  run preFenceWriteOfflineSkipTest = init.then(oldWrite(1,2)).then(crash(1)).then(selectCurrent(2)).then(restart(1))
    .then(selectCurrent(1)).then(readSource(1)).then(publish(1,1)).then(publish(1,2)).then(checkpoint(1))
    .then(all { assert(safety and s.data.get(1).get(3) == records and s.data.get(2).get(3) == Set()), idle })
  run publishCrashReplayRelayTest = init.then(selectCurrent(1)).then(readSource(1)).then(publish(1,1))
    .then(crash(1)).then(restart(1)).then(readSource(1)).then(publish(1,1)).then(checkpoint(1))
    .then(all { assert(safety and s.checkpoint.get(1).get(3) == Set(1)), idle })
  run checkpointRestartTest = init.then(selectCurrent(1)).then(readSource(1)).then(publish(1,1)).then(checkpoint(1))
    .then(crash(1)).then(restart(1)).then(readSource(1))
    .then(all { assert(safety and s.batch.get(1) == Set()), idle })
}
