module cross_file {
  import middle as M from "./middle"
  import leaf.* from "./leaf"
  var state: int -> { value: int }
  action init = state' = M::initial
  action step = state' = state
  val safety = state == construct(0)
}
