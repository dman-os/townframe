// Source owner: tasks/model.rs. Opaque is the complete equality class of
// handler/input/domain/coordination/effect/retention payloads, not a lossy hash.
module task_ticket {
  type Declaration = { task: int, pool: int, opaque: int, producer: int,
    capabilities: Set[int], placement: int, preferred: int, effect: int,
    before: int, after: int, retention: int, retainUntil: int }
  // -1 denotes an absent optional timestamp/result, outside Rust u64. Zero is present.
  // placement:0 AnyNode,1 PreferOrigin,2 Only; effect:3 AuthoritativePlacement;
  // retention:0 ExternalSettlement,1 TicketAuthoritative.
  type Evidence = { publisher: int, envelope: int, witness: int }
  type Fact = { success: bool, attempt: int, result: int, reason: int }
  type Terminal = { writer: int, sequence: int, fact: Fact }
  type Ticket = { schema: int, task: int, declaration: Declaration,
    publishers: Set[Evidence], terminal: int -> Terminal }
  type Merge = { ticket: Ticket, error: int, changed: bool }
  pure val writers = Set(1,2)
  pure val noFact = { success: false, attempt: 0, result: 0, reason: 0 }
  pure val absentTerminal = { writer: 0, sequence: -1, fact: noFact }
  pure def valid(d: Declaration): bool =
    not (d.before != -1 and d.after != -1 and d.after < d.before)
    and not (d.retention == 1 and d.retainUntil != -1 and d.after == -1)
    and not (d.retention == 1 and d.retainUntil != -1 and d.after > d.retainUntil)
    and not (d.effect == 3 and d.placement != 2)
  pure def fresh(d: Declaration, e: Evidence): Ticket =
    { schema: 2, task: d.task, declaration: d, publishers: Set(e),
      terminal: Set(1,2).mapBy(_ => { writer: 0, sequence: -1,
        fact: { success: false, attempt: 0, result: 0, reason: 0 } }) }
  pure def terminal(t: Ticket): bool = writers.exists(w => t.terminal.get(w).sequence >= 0)
  pure def summary(t: Ticket): Fact = {
    val successes = writers.map(w => t.terminal.get(w)).filter(x => x.sequence >= 0 and x.fact.success)
    if (successes.size() != 0) successes.fold(absentTerminal, (a,x) =>
      if (a.sequence == -1 or x.fact.attempt > a.fact.attempt or (x.fact.attempt == a.fact.attempt and x.fact.result > a.fact.result)) x else a).fact
    else {
      // BTreeMap iteration compares incoming map key with retained lane.writer,
      // not retained map key. Preserve that behavior even for malformed bindings.
      val selected = List(1,2).foldl(absentTerminal,(a,w) =>
        if (t.terminal.get(w).sequence >= 0 and (a.sequence == -1 or w < a.writer)) t.terminal.get(w) else a)
      selected.fact
    }
  }
  pure def lane(a: Terminal, b: Terminal): Terminal =
    if (b.sequence == -1) a else if (a.sequence == -1) b
    else if (a.fact.success and not(b.fact.success)) a
    else if (not(a.fact.success) and b.fact.success) b
    else if (b.sequence > a.sequence) b else a
  // Errors retain source validation precedence. Authentication is upstream;
  // merge does not independently validate terminal map-key/writer binding.
  pure def merge(a: Ticket, b: Ticket): Merge = {
    val error = if (b.schema != a.schema) 1
      else if (a.schema != 2) 2
      else if (b.task != a.task) 3
      else if (not(valid(b.declaration))) 4
      else if (b.declaration != a.declaration) 5
      else if (writers.exists(w => {
        val x = a.terminal.get(w)
        val y = b.terminal.get(w)
        x.sequence >= 0 and y.sequence >= 0 and x.sequence == y.sequence and x.fact != y.fact
      })) 6 else 0
    val joined = if (error != 0) a else { ...a,
      publishers: a.publishers.union(b.publishers),
      terminal: writers.mapBy(w => lane(a.terminal.get(w), b.terminal.get(w))) }
    { ticket: joined, error: error, changed: joined != a }
  }
  // Complete ticket value stands for the collision-free digest equality class;
  // publisher evidence and every terminal lane participate in freshness.
  pure def version(t: Ticket): Ticket = t
}
