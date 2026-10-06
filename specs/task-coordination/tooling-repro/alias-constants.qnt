// Quint 0.33.0 compiler repro candidate: constructors reference module constants.
module leaf {
  pure val domain = Set(1,2)
  pure val sentinel = { value: 0 }
  pure def construct(n: int): int -> { value: int } = domain.mapBy(_ => { ...sentinel, value: n })
}
module middle {
  import leaf as L
  pure val initial = L::construct(0)
}
module alias_constants {
  import middle as M
  var state: int -> { value: int }
  action init = state' = M::initial
  action step = state' = state
  val safety = state.get(1).value == 0
}
