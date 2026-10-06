module middle {
  import leaf as L from "./leaf"
  pure val initial = L::construct(0)
}
