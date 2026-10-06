# RouterMachine / TaskTicket guard correspondence continuation

Source read: `src/daybook_core/tasks/router.rs` RouterEvent (18 variants), RouterAction (8), on_event and helpers; `tasks/model.rs` TaskTicket merge/summary. This item closes missing executable guard witnesses, not full native correspondence. Existing model alphabet, properties, domains and bounds remain unchanged. Parent's active descendant-authorization implementation is excluded and unvalidated here.

## All RouterEvent kinds and relevant guard classes

Tags correspond to existing `router_machine::handle`. M = modeled semantic branch; W = new executable guard witness; existing = earlier named witness; missing = unclosed representation/environment coverage. All eighteen tags remain in original safety profile.

| Tag / Rust variant | Guards / outcomes | Coverage |
|---|---|---|
| 1 AuthorityChanged | local allowed/denied; retain authorized live/session; filter allocations/blocks; deny clears own/persisted and returns proofs to pending | M; existing authorityLossFencesPendingStartsTest; proof restoration independently unwitnessed |
| 2 ObsolescenceChecked | missing proof, mismatched exact token, matching proof; invalid/terminal/obsolete/runnable ticket | M; existing staleObsolescenceCannotDeleteNewTicketTest; digest-byte correspondence missing |
| 3 SlotMerged | foreign pool; absent/newer/same/stale lane; regressed generation/equivocation; continue after reject; reconsider | M imported router_slot; earlier router-slot witnesses; multi-claim mixed reject/accept not newly witnessed |
| 4 HeartbeatObserved | unauthorized candidate ignored; authorized exact liveness key; reconsider | M+W guardHeartbeatAuthorityAndExpiryTest |
| 5 HeartbeatExpired | remove exact live key, including absent; reconsider | M+W same witness; no actual timer theorem |
| 6 ConsiderTakeover | routing/denied/live winner prevent; otherwise sequence/generation advance, observed lanes, highest protocol; persist or collision reject | M; existing unpersistedClaimCannotRouteTest; arithmetic exhaustion and generated hash-rank collision outside finite model |
| 7 ClaimPersisted | no own/stale generation/claim; exact stored pool/live/incarnation/protocol/observed; adopt stored sequence gaps; reconsider | M; existing durableSequenceGapCanConfirmClaimTest; stored binding mismatch branches modeled but not individually witnessed |
| 8 ClaimPersistenceFailed | ignore persisted or stale generation/claim; matching unpersisted clears own/live, retains lane | M+W guardClaimFailureIdentityTest |
| 9 HeartbeatDue | only routing with own claim emits heartbeat | M+W guardHeartbeatAuthorityAndExpiryTest; denied witness remains missing |
| 10 RegisterExecutor | local/executor Edit denial -> Unavailable; unsupported -> UnsupportedProtocol; capacity/active changed clears only Unavailable; replacement and allocation | M+W guardRegistrationReasonsTest; existing capacity/block witnesses; duplicate task entries differ (see below) |
| 11 SessionClosed | remove exact session, its allocations and blocks; reallocate remaining pending work | M+W guardSessionCloseReoffersTest; absent closure modeled |
| 12 TaskObserved | terminal removes proof and observe; nonterminal replaces pending/proof, increments request and emits check | M; existing stale-obsolescence witness; digest-byte/canonical publisher ordering correspondence missing |
| 13 TaskRemoved | remove proof/pending; cancel offered/accepted and origin-reported work without releasing ownership | M; existing cancellationRetainsOwedWorkTest and W guardCancelledAcceptanceTest; origin-only cancellation unwitnessed |
| 14 OfferAccepted | not routing/missing allocation/wrong session/wrong accepted attempt ignored; bind exact attempt; cancelled emits Cancel otherwise Start; duplicate accepted may repeat Start | M+W guardWrongAcceptedAttemptTest/guardCancelledAcceptanceTest; existing session/duplicate witnesses |
| 15 OfferDeclined | missing/wrong session/already accepted ignored; remove offered; block five local decline reasons; StaleSession/Unsupported do not block; allocate | M; existing acceptedDeclineDoesNotReleaseOwnershipTest; all seven reasons modeled but not individually exercised |
| 16 AttemptChanged | Running: conflicting tracked attempt ignored, absent session ignored, exact inserts with no allocation; every non-Running state: only exact tracked or accepted owner releases, clears Unavailable, reallocates | M+W guardStaleAttemptOutcomeTest/guardRunningReportDoesNotOverwriteTest; all non-Running enum values collapsed to one shared Rust match branch |
| 17 ReadinessWakeup | unknown session ignored; only exact task NotReady/CoordinationIncomplete unblocked; retain other reasons; allocate | M+W guardReadinessReasonFenceTest, including Unavailable non-release |
| 18 RoutingTick | allocate | M+W guardAllocationConstraintsTest |

Shared allocation helper: routing required, existing allocation prevents duplicate, terminal removes pending; Only placement, capabilities, capacity/owed, blocked task/session, active task anywhere prevent offer; PreferOrigin then node/session/incarnation order selects; new allocation counter advances. M; new constraints witness checks capabilities and Only. Tie ordering, capacity zero, active/allocation dedup variants and multiple candidate fairness still need independent guard witnesses. Owed compares explicit allocation when present, otherwise accepted attempt; canceled allocation retains capacity. No global exclusivity across partitioned routers is claimed.

All eight action variants mapped: CheckObsolescence=1, PersistClaim=2, PublishHeartbeat=3, RejectSession=4, SendOffer=5, StartAttempt=6, CancelAttempt=7, RejectClaim=8. Quint carries extra sentinel/evidence fields; bridge compares only semantic Rust fields. Cancel reason text and RejectClaim typed payload/error numbers require explicit bridge mapping, not direct serialized equality.

## TaskTicket operation/branch ledger

There is no TaskTicket event enum: reducer operations are new/declare_local, merge, terminal_summary/is_terminal/is_success/version_digest. Do not manufacture an event alphabet.

- `new`: schema3, declaration task ID, singleton publisher, empty terminal map -> fresh; constructor validation intentionally absent.
- `declare_local`: three declaration invariant checks and panic for invalid -> valid precondition; panic mechanism is not modeled.
- `merge` validation precedence: remote schema mismatch, local unsupported schema, task mismatch, invalid declaration, unequal declaration, equal-writer-sequence unequal-fact equivocation. All modeled; existing validationErrorPrecedenceTest/equivocationRejectsWholeBatchTest; W guardInvalidBatchPrecedesMutationTest proves invalid input cannot retain new evidence/lane.
- Declaration guards: optional not-before/deadline order; authoritative retention deadline order; authoritative effect requires Only placement. Modeled finite equality classes; Rust wire/required-field decode, arbitrary signed values and optional Some(-1) outside profile.
- Publisher union by full content, canonical sorted vector in Rust -> set abstraction; existing samePublisherDistinctContentRetainedTest. Vector duplicates already present in malformed local snapshot/order are not modeled.
- Lane absent inserts; existing success vs cancellation retains success regardless sequence; incoming success replaces cancellation regardless sequence; same kind only strictly greater sequence replaces. Equal sequence unequal fact rejected in validation before any mutation. All modeled; W guardSameWriterSuccessSequenceTest checks stale genuine success and later cancellation cannot regress; prior zero-sequence witnesses cover map absence vs zero.
- Summary: greatest success (attempt,result), None before Some; absent success -> cancellation chosen by incoming map key versus retained lane.writer. Model deliberately preserves this malformed-binding behavior; existing malformedWriterBindingCurrentBehaviorTest. Empty returns absent sentinel; byte ordering contracts not verified.
- Version freshness: model uses full-ticket equality as collision-free digest class. Current Rust version_digest hashes both terminal map key and lane.writer, sequences and facts. Publisher vector order/multiplicity is hashed in Rust but abstracted as set in Quint; canonical input ordering is necessary for equality-class correspondence. Digest byte algorithm remains unverified.

## Source/model discrepancies reported to parent

1. Rust RegisterExecutor collects `active_attempts: Vec` into task-keyed map (last duplicate task wins). Quint active Set has no list arrival order, can retain two attempts for one task and overcount owed. Existing well-formed fixtures do not expose that branch. No new input validation policy chosen here. Needs explicit unique-task ingress contract or ordered normalization model and Rust bridge cases.
2. Publisher vector ordering/multiplicity vs model set requires canonicalized ingress; full raw snapshot equality is not modeled. An initial progress note incorrectly reported lane.writer omitted from the digest; independent reread found it hashed and the note was explicitly withdrawn to parent. No source bug asserted.
3. Router rank integer is an external ordering class, not BLAKE3 reproduction. Hash bytes/collisions, checked counter exhaustion and arbitrary identities remain outside model.

## Executed evidence and negative controls

`cd specs/task-coordination && node runner.mjs guards`: final exit0 `/tmp/quint-reducer-guards-accepted.log`; ten Router guard witnesses plus two TaskTicket guard witnesses. Six actual QNT508 failures: wrong accepted attempt, cancelled Start, failed-claim token bypass, readiness clearing Unavailable, stale same-writer success replacing newer, invalid declaration acceptance. No safety exploration or full-suite run in this item; original lifecycle and full Router safety artifacts preserved unchanged. Tests are source-grounded schedules, not implementation execution proof.

Historical experiment `/tmp/quint-reducer-guards-complete.log`: removing stale-outcome guard alone passed because remaining exact removal/allocation guards preserved output/state. That redundant guard mutant is not claimed detected. Replaced with concrete wrong-accepted-attempt control. `/tmp/quint-reducer-guards-v2.log`: readiness mutant initially survived because witness woke task1 while Unavailable belonged to task2; fixed witness includes wake task2 and detects mutation. Both failures retained, not hidden.

`reducer-conformance-vectors.json` provides six concrete semantic event/output schedules for parent Rust bridge, using emitted IDs/tokens rather than assuming minted numeric identities. Bridge remains unimplemented/unrun. Independent parent review and rerun required; remaining witnesses and source discrepancy decisions block complete reducer correspondence acceptance.

Source snapshot SHA256: router.rs `5ef5dac83ed11f02ed48447e1e0f7659ab56843d6caea48ea34fc555907235d4`; model.rs `5da572af8c56ecc12cc1f14b4acf83f033e25a7355efaaa0d08dc6dacc5644c1`. Additional checks: `node --check specs/task-coordination/runner.mjs` and `python3 -m json.tool specs/task-coordination/reducer-conformance-vectors.json` passed.
