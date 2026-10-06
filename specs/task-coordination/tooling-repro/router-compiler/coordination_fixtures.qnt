// Complete finite meaning/evidence values shared by reducer scenarios.
module coordination_fixtures {
  pure val declaration = { task: 1, pool: 1, opaque: 1, producer: 2,
    capabilities: Set(1), placement: 1, preferred: 1, effect: 0,
    before: -1, after: 2, retention: 1, retainUntil: 3 }
  pure val base = { schema: 2, task: 1, declaration: declaration,
    publishers: Set({ publisher: 1, envelope: 1, witness: 1 }),
    terminal: Set(1,2).mapBy(_ => { writer: 0, sequence: -1,
      fact: { success: false, attempt: 0, result: 0, reason: 0 } }) }
  pure def fact(success: bool,attempt: int): { success: bool, attempt: int, result: int, reason: int } =
    { success: success, attempt: attempt, result: attempt, reason: 1 }
}
