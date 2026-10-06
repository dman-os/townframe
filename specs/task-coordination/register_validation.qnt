module register_validation {
  import register.* from "./register"
  // Finite descriptors denote concrete boundary/equality/signature classes;
  // no Ed25519/BLAKE3/serde implementation is asserted verified.
  type Descriptor = { schema: int, key: int, writer: int, mapWriter: int,
    keyBytes: int, dependencyKeyBytes: int, frontierEntries: int, cipherBytes: int, metadataBytes: int,
    canonicalFrontier: bool, selfPast: bool, canonicalKeyHeads: bool, headerSignature: bool, outerSignature: bool }
  pure val valid = { schema: 1, key: 1, writer: 1, mapWriter: 1, keyBytes: 2, dependencyKeyBytes: 2,
    frontierEntries: 2, cipherBytes: 2, metadataBytes: 2, canonicalFrontier: true, selfPast: true,
    canonicalKeyHeads: true, headerSignature: true, outerSignature: true }
  pure def validate(d: Descriptor): bool = d.schema == 1 and d.key == 1 and d.writer == d.mapWriter
    and d.keyBytes <= 2 and d.dependencyKeyBytes <= 2 and d.frontierEntries <= 2
    and d.cipherBytes <= 2 and d.metadataBytes <= 2 and d.canonicalFrontier and d.selfPast
    and d.canonicalKeyHeads and d.headerSignature and d.outerSignature
  pure val invalids = Set({ ...valid, schema: 2 },{ ...valid, key: 2 },{ ...valid, mapWriter: 2 },
    { ...valid, keyBytes: 3 },{ ...valid, dependencyKeyBytes: 3 },{ ...valid, frontierEntries: 3 },
    { ...valid, cipherBytes: 3 },{ ...valid, metadataBytes: 3 },{ ...valid, canonicalFrontier: false },
    { ...valid, selfPast: false },{ ...valid, canonicalKeyHeads: false },
    { ...valid, headerSignature: false },{ ...valid, outerSignature: false })
  pure def validPoison(w: int, first: Header, second: Header): bool =
    first.writer == w and second.writer == w and first.sequence == second.sequence and first.semantic < second.semantic
  var descriptor: Descriptor
  action init = descriptor' = valid
  action step = { nondet d = invalids.union(Set(valid)).oneOf(); descriptor' = d }
  val boundaryClasses = validate(descriptor) == (descriptor == valid)
  run exactLimitAndBatchAtomicityTest = {
    assert(validate(valid))
      .then(assert(invalids.forall(d => not(validate(d)))))
      .then(assert(merge(empty,{ ...batch(singleton(rep(1,1,10,1,1,1,1,1))), valid: validate({ ...valid, outerSignature: false }) }) == empty))
  }
  run poisonedWitnessValidityTest = {
    val a = { writer: 1, sequence: 1, semantic: 10, signature: 1 }
    val b = { writer: 1, sequence: 1, semantic: 20, signature: 1 }
    assert(validPoison(1,a,b))
      .then(assert(not(validPoison(1,b,a))))
      .then(assert(not(validPoison(1,a,a))))
      .then(assert(not(validPoison(2,a,b))))
      .then(assert(not(validPoison(1,a,{ ...b, sequence: 2 }))))
  }
  run opaqueRecordKeyAndSelfFrontierClassesTest = {
    // Equal byte prefixes with different component boundaries are distinct key
    // classes; concrete framing and duplicate-map decoding remain codec contracts.
    assert(not(validate({ ...valid, key: 2 })))
      .then(assert(validate({ ...valid, frontierEntries: 0 })))
      .then(assert(not(validate({ ...valid, selfPast: false }))))
  }
}
