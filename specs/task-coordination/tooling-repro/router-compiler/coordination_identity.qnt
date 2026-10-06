// Dependency-free value/type leaf: avoids Quint0.33 diamond-alias flattening.
module coordination_identity {
  type Session = { node: int, incarnation: int, session: int }
  type Active = { task: int, attempt: int, allocation: int }
  pure val noSession = { node: 0, incarnation: 0, session: 0 }
  pure val noDeclaration = { task: 0, pool: 0, opaque: 0, producer: 0, capabilities: Set(),
    placement: 0, preferred: 0, effect: 0, before: -1, after: -1, retention: 0, retainUntil: -1 }
  pure val noTicket = { schema: 2, task: 0,
    declaration: { task: 0, pool: 0, opaque: 0, producer: 0, capabilities: Set(),
      placement: 0, preferred: 0, effect: 0, before: -1, after: -1, retention: 0, retainUntil: -1 },
    publishers: Set({ publisher: 0, envelope: 0, witness: 0 }),
    terminal: Set(1,2).mapBy(_ => { writer: 0, sequence: -1,
      fact: { success: false, attempt: 0, result: 0, reason: 0 } }) }
}
