module leaf {
  pure val domain = Set(1,2)
  pure val sentinel = { value: 0 }
  pure def construct(n: int): int -> { value: int } = domain.mapBy(_ => { ...sentinel, value: n })
}
