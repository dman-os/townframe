module leaf_export {
  pure val domain = Set(1,2)
  pure val sentinel = { value: 0 }
  pure def construct(n: int): int -> { value: int } = domain.mapBy(_ => { ...sentinel, value: n })
}
module middle_export {
  import leaf_export as L
  export L.*
  pure val initial = L::construct(0)
}
module alias_export {
  import middle_export as M
  var state: int -> { value: int }
  action init = state' = M::initial
  action step = state' = state
  val safety = state.get(1).value == 0
}
