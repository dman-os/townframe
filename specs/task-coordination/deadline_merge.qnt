// rt.rs finalization + drawer/mutations.rs synchronous document-lock boundary.
// Clock is nondecreasing; this is NOT host interruption or persistence timing.
module deadline_merge {
  pure def authorized(now: int, bounded: bool, claimed: bool): bool = not(bounded) or now <= 10
  pure def merge(start: int, finish: int, bounded: bool, claimed: bool): (int,bool) =
    if (not(authorized(start,bounded,claimed))) (0,true)
    else if (not(authorized(finish,bounded,claimed))) (0,true) else (1,false)
  run crossingDeadlineLeavesTargetUnchangedTest = assert(merge(10,11,true,true) == (0,true))
  run expiredClaimDoesNotAuthorizePublicationTest = assert(merge(11,11,true,true) == (0,true))
  run lastAuthorizedSecondAndUnboundedTest = {
    assert(merge(10,10,true,true) == (1,false))
      .then(assert(merge(11,11,false,true) == (1,false)))
  }
  var bounded: bool
  var now: int
  var stage: int
  var privateCopy: int
  var visible: int
  var issuedAt: int
  var refused: bool
  var persisted: bool
  var cancelled: bool
  var hooks: bool
  action init = {
    nondet b = Set(false,true).oneOf()
    nondet clock = Set(10,11).oneOf()
    all { bounded' = b, now' = clock, stage' = 0, privateCopy' = 0,
      visible' = 0, issuedAt' = -1, refused' = false, persisted' = false, cancelled' = false, hooks' = false }
  }
  // Finalization claim already exists. It never overrides expiry.
  action begin = all { stage == 0, bounded' = bounded, now' = now,
    stage' = if (authorized(now,bounded,true)) 1 else 3,
    refused' = not(authorized(now,bounded,true)), privateCopy' = privateCopy,
    visible' = visible, issuedAt' = issuedAt, persisted' = persisted, cancelled' = cancelled, hooks' = hooks }
  action compute = {
    nondet clock = Set(10,11).filter(t => t >= now).oneOf()
    all { stage == 1, bounded' = bounded, now' = clock, stage' = 2,
      privateCopy' = 1, visible' = visible, issuedAt' = issuedAt,
      refused' = refused, persisted' = persisted, cancelled' = cancelled, hooks' = hooks }
  }
  // Atomic check+replacement under the synchronous document lock. The finite
  // profile treats this as one issued mutation; physical time between CPU
  // instructions is not independently proved by this abstraction.
  action publish = {
    val permit = authorized(now,bounded,true)
    all { stage == 2, bounded' = bounded, now' = now, stage' = 3,
      privateCopy' = privateCopy, visible' = if (permit) privateCopy else visible,
      issuedAt' = if (permit) now else issuedAt, refused' = not(permit),
      persisted' = persisted, cancelled' = cancelled, hooks' = hooks }
  }
  // Later persistence/notification is not a fresh execution-effect issuance.
  // Root's post-merge check can cancel and suppress hooks without undoing an
  // earlier authorized visible mutation.
  action persistAndRootRecheck = {
    nondet clock = Set(10,11).filter(t => t >= now).oneOf()
    all { stage == 3, bounded' = bounded, now' = clock, stage' = 4,
      privateCopy' = privateCopy, visible' = visible, issuedAt' = issuedAt, refused' = refused,
      persisted' = visible == 1, cancelled' = (refused or not(authorized(clock,bounded,true))),
      hooks' = not(refused or not(authorized(clock,bounded,true))) }
  }
  action done = all { stage == 4, bounded' = bounded, now' = now, stage' = stage,
    privateCopy' = privateCopy, visible' = visible, issuedAt' = issuedAt,
    refused' = refused, persisted' = persisted, cancelled' = cancelled, hooks' = hooks }
  action step = any { begin, compute, publish, persistAndRootRecheck, done }
  val safety = (bounded and visible == 1 implies issuedAt >= 0 and issuedAt <= 10)
    and (refused implies visible == 0)
    and (stage == 4 and bounded and now > 10 implies cancelled and not(hooks))
    and (persisted implies visible == 1)
  run issuedMutationCanPersistAfterDeadlineTest = {
    assert(merge(10,10,true,true) == (1,false))
      .then(assert(not(authorized(11,true,true))))
  }
}
