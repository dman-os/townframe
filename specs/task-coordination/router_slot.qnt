// Source owner: tasks/router.rs RouterSlot::merge_claim. Pool validation
// belongs to RouterMachine::SlotMerged, not this standalone join.
module router_slot {
  type Claim = { pool: int, candidate: int, incarnation: int, sequence: int,
    generation: int, claim: int, protocol: int, observed: int -> int, rank: int }
  type Slot = int -> Claim
  type Merge = { slot: Slot, error: int, changed: bool }
  pure val candidates = Set(1,2)
  pure val absent = { pool: 0, candidate: 0, incarnation: 0, sequence: -1,
    generation: 0, claim: 0, protocol: 0, observed: candidates.mapBy(_ => 0), rank: 0 }
  pure val empty = candidates.mapBy(_ => absent)
  pure def merge(s: Slot, c: Claim): Merge = {
    val old = s.get(c.candidate)
    val error = if (old.sequence >= 0 and c.sequence > old.sequence and c.generation < old.generation) 1
      else if (c.sequence == old.sequence and c != old) 2 else 0
    val joined = if (error == 0 and c.sequence > old.sequence) s.set(c.candidate,c) else s
    { slot: joined, error: error, changed: joined != s }
  }
  pure def maxGeneration(s: Slot): int = candidates.fold(0,(g,n) => if (s.get(n).generation > g) s.get(n).generation else g)
  // Liveness deliberately excludes incarnation and writer sequence.
  pure def liveKey(c: Claim): (int,int,int) = (c.candidate,c.generation,c.claim)
}
