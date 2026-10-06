// Source owner: src/big_sync_core/encrypted_register.rs. See coverage.md.
module register {
  type Header = { writer: int, sequence: int, semantic: int, signature: int }
  // semantic is a collision-free identity class for the COMPLETE original
  // signed statement plus public frontier; it is not a naked payload hash.
  type Rep = { header: Header, binding: int, publisher: int, ciphertext: int, signature: int }
  type Lane = { sequence: int, headers: Set[Header], representations: Set[Rep] }
  type Register = int -> Lane
  type Batch = { schema: bool, binding: bool, uniqueWriters: bool, valid: bool, lanes: int -> Lane }

  pure val writers = Set(1, 2, 3)
  // -1 represents map absence, outside Rust u64; sequence zero is real evidence.
  pure val emptyLane = { sequence: -1, headers: Set(), representations: Set() }
  pure val empty = writers.mapBy(_ => emptyLane)
  pure def rep(w: int, q: int, s: int, hs: int, b: int, p: int, c: int, rs: int): Rep =
    { header: { writer: w, sequence: q, semantic: s, signature: hs }, binding: b, publisher: p, ciphertext: c, signature: rs }
  pure def current(r: Rep): Lane = { sequence: r.header.sequence, headers: Set(r.header), representations: Set(r) }
  pure def before(a: Rep, b: Rep): bool =
    if (a.header.signature != b.header.signature) a.header.signature < b.header.signature
    else if (a.binding != b.binding) a.binding < b.binding
    else if (a.publisher != b.publisher) a.publisher < b.publisher
    else if (a.ciphertext != b.ciphertext) a.ciphertext < b.ciphertext
    else a.signature < b.signature
  // Fold selectors are deterministic on singleton/minimum classes and supported
  // by both the TypeScript simulator and TLC (chooseSome is not in the former).
  pure def pickHeader(hs: Set[Header]): Header = hs.fold({ writer: 0, sequence: 0, semantic: 999, signature: 999 },
    (a,h) => if (h.semantic < a.semantic or (h.semantic == a.semantic and h.signature < a.signature)) h else a)
  pure def pickRep(rs: Set[Rep]): Rep = rs.fold(rep(0,0,999,999,999,999,999,999),
    (a,r) => if (before(r,a)) r else a)
  pure def canonical(rs: Set[Rep]): Rep = pickRep(rs)
  // Independent mixed-radix order oracle; profile coordinates are in 0..2.
  pure def rank(r: Rep): int = r.header.signature*10000 + r.binding*1000 + r.publisher*100 + r.ciphertext*10 + r.signature
  pure def witnesses(hs: Set[Header]): Set[Header] = {
    val ids = hs.map(h => h.semantic)
    val least = ids.fold(999,(a,s) => if (s < a) s else a)
    val rest = ids.exclude(Set(least))
    val second = rest.fold(999,(a,s) => if (s < a) s else a)
    hs.filter(h => Set(least, second).contains(h.semantic)
      and hs.filter(o => o.semantic == h.semantic).forall(o => h.signature <= o.signature))
  }
  pure def joinLane(a: Lane, b: Lane): Lane =
    if (b.sequence < a.sequence) a
    else if (b.sequence > a.sequence) b
    else if (a.sequence == -1) emptyLane
    else if (a.representations.size() == 1 and b.representations.size() == 1
      and pickHeader(a.headers).semantic == pickHeader(b.headers).semantic) {
      current(canonical(a.representations.union(b.representations)))
    } else { sequence: a.sequence, headers: witnesses(a.headers.union(b.headers)), representations: Set() }
  pure def join(a: Register, b: Register): Register = writers.mapBy(w => joinLane(a.get(w), b.get(w)))
  pure def singleton(r: Rep): Register = empty.set(r.header.writer, current(r))
  pure def accepted(b: Batch): bool = b.schema and b.binding and b.uniqueWriters and b.valid
  pure def merge(a: Register, b: Batch): Register = if (accepted(b)) join(a, b.lanes) else a
  pure def batch(a: Register): Batch = { schema: true, binding: true, uniqueWriters: true, valid: true, lanes: a }
  pure def restore(b: Batch): Register = merge(empty, b)
  // Outcome tags: 0 Unchanged, 1 Changed, 2 EquivocationRetained; invalid
  // envelopes reject before join rather than returning one of these tags.
  pure def outcome(a: Register, b: Register): int = {
    val joined = join(a,b)
    if (joined == a) 0 else if (writers.exists(w => joined.get(w) != a.get(w)
      and joined.get(w).headers.size() == 2 and joined.get(w).representations.size() == 0)) 2 else 1
  }

  // Independent specification oracle: select maximal writer version from ALL
  // received originals, then determine semantic conflict and canonical evidence.
  // This does not call joinLane, join, canonical or witnesses.
  pure def expectedLane(history: Set[Rep], w: int): Lane = {
    val own = history.filter(r => r.header.writer == w)
    if (own.size() == 0) emptyLane else {
      val latest = own.filter(r => own.forall(o => o.header.sequence <= r.header.sequence))
      val ids = latest.map(r => r.header.semantic)
      if (ids.size() == 1) {
        val selected = pickRep(latest.filter(r => latest.forall(o => rank(r) <= rank(o))))
        { sequence: selected.header.sequence, headers: Set(selected.header), representations: Set(selected) }
      } else {
        val selectedHeaders = latest.map(r => r.header).filter(h =>
          ids.filter(s => s < h.semantic).size() < 2
          and latest.filter(r => r.header.semantic == h.semantic).forall(r => h.signature <= r.header.signature))
        { sequence: own.fold(0,(q,r) => if (r.header.sequence > q) r.header.sequence else q), headers: selectedHeaders, representations: Set() }
      }
    }
  }
  pure def expected(history: Set[Rep]): Register = writers.mapBy(w => expectedLane(history, w))
  pure def wellFormed(a: Register): bool = writers.forall(w => {
    val l = a.get(w)
    l.headers.forall(h => h.writer == w and h.sequence == l.sequence)
      and (if (l.sequence == -1) l.headers.size() == 0 and l.representations.size() == 0
        else if (l.representations.size() == 1) l.headers == l.representations.map(r => r.header)
        else l.representations.size() == 0 and l.headers.size() == 2 and l.headers.map(h => h.semantic).size() == 2)
  })

  // Finite direct causal classes:40 observes this record/version1;41 names
  // another record/version3;42 observes this record/version3 (>= projection).
  pure def dependencies(s: int): Set[(int, int, int)] =
    if (s == 40) Set((1, 1, 1)) else if (s == 41) Set((2, 1, 3)) else if (s == 42) Set((1, 1, 3)) else Set()
  pure def observes(a: Header, b: Header): bool = dependencies(a.semantic).exists(d => d._1 == 1 and d._2 == b.writer and d._3 >= b.sequence)
  pure def heads(a: Register): Set[Header] = {
    val candidates = writers.map(w => a.get(w)).filter(l => l.representations.size() == 1).map(l => pickHeader(l.headers))
    candidates.filter(h => not(candidates.exists(o => o.writer != h.writer and observes(o, h))))
  }
  pure def expectedHeads(history: Set[Rep]): Set[Header] = {
    val live = writers.map(w => expectedLane(history, w)).filter(l => l.representations.size() == 1).map(l => pickHeader(l.headers))
    live.filter(h => not(live.exists(o => o.writer != h.writer and dependencies(o.semantic).exists(d => d._1 == 1 and d._2 == h.writer and d._3 >= h.sequence))))
  }
  pure def localAllowed(a: Register, r: Rep): bool = {
    val l = a.get(r.header.writer)
    r.header.sequence > l.sequence or (r.header.sequence == l.sequence and l.representations.size() == 1 and pickHeader(l.headers).semantic == r.header.semantic)
  }
  // OriginalReader is distinct from opaque merge. Finite equality classes replace
  // cryptographic arithmetic; tampered exact originals are rejected after opening.
  pure def readerAccepts(r: Rep, originalWriter: int, originalSequence: int, originalSemantic: int, validSignature: bool): bool =
    validSignature and r.header.writer == originalWriter and r.header.sequence == originalSequence and r.header.semantic == originalSemantic
}
