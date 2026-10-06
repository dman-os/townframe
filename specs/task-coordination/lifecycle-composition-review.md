# Composed lifecycle continuation (independent recovery review)

Recovered the stopped worker's `lifecycle_composition.qnt`; inspected its actions and properties independently rather than accepting its temporal state count. Added executable runner mode and two crash/delivery witnesses. No Rust, ADR, Cargo, Git, or full-suite work. Historical counterexamples remain intact.

## Requirement / source / verification coverage matrix

Paths below are relative to `src/daybook_core` unless stated otherwise. Rows explicitly distinguish a composed abstraction from all-event implementation coverage.

| ADR requirement | Source seam | Model/actions/properties | Evidence and remaining scope |
|---|---|---|---|
| 011 §5 election and takeover | tasks/model.rs RouterMachine; tasks/driver.rs | imported router_machine winner, router_slot merge; takeover/enqueue | competingAndTakeoverTest; only two eligible claims, not live RPC, registration, capability/session/capacity branches |
| 011 §8 immutable ticket and original terminal | tasks/model.rs TaskTicket::merge/terminal_summary; tasks/store.rs | imported task_ticket; terminal/release; originalFactBeforeRelease | bounded safety; store IO, ciphertext and arbitrary lane conflicts are component obligations |
| 011 §10a accepted submissions survive caller loss | rt/dispatch.rs repository-owned worker | enqueue/persist/start; persistenceBeforeStart | composed durable preparation abstraction; admission-ownership-repair-review.md separately covers publication fence, cache and hints |
| 011 §10a finite admission/start/success | model.rs TaskDeclaration::expired_at; rt/task_adapter.rs; dispatch.rs::complete | enqueue/start/expire/completeAs; finiteAuthorization | boundary witness and actual-winner mutant; managed effects covered separately by deadline_merge, not all descendants/continuations |
| 011 §9/10a successors do not renew predecessor | rt/triage/origins.rs authorization; rt/triage/domain.rs | imported successor_authorization; issue/declaration | competing and expired successor profiles, immutable original witness; no durable logical checkpoint/idempotency protocol across many successors |
| 010 §4–6/9 exact captured settlement | rt/triage/slots.rs; domain.rs; rt.rs completion hook | imported processor_slots; advanceSource/hook; exactSettlement | late settlement witness and wrong-capture mutant; one slot/two captures, not arbitrary concurrent lane/source ordering |
| 010 §1/10 activation/source order | rt/triage.rs; triage/domain.rs | triage_activation component, not composed here | standalone evidence in verification-continuation.md; materialization, enabled generations, pending domain availability not discharged |
| 011 §10 exact domain acknowledgement | tasks/driver.rs incorporation/publication; rt/task_adapter.rs; dispatch.rs::incorporate_domain_ack | imported domain_ack_ledger; acknowledge/terminal/release | crash after ack before publication witness; exact task/status classes abstract digest/proof validation, covered by component ledger/successor controls |
| 011 §10a durable outcome delivery/replay | dispatch.rs completion outbox; rt.rs::deliver_dispatch_completion; task_adapter.rs::outcome | complete/expire/hook/reply/finishDelivery/crash/restart | receiptAfterDelivery, durableDeliveryOwner, retainedResult; hook/notification dependencies remain assumptions |
| 011 §10a stable reply identity through snapshot/consumption | src/wflow_core/partition/reduce.rs received_message_ids | reply/consume/crash; duplicateMessageSafety | consumed-reply crash witness and forget-ID mutant; parent native receipt regression separately exercises runtime ordering |
| 011 §4/11/14 transport/rotation/retention | tasks/storage.rs, transport.rs, driver.rs reader/checkpoint | existing register models; not composed | cryptography, native authorization, key provenance, cursor coverage and physical retirement remain outside this model |
| 011 §10a clock/GC/descendants | runtime/workflow and collectors | no invented defaults or transitions | explicit missing contract: skew/rollback, descendant inheritance, continuation fences, notification collection/backpressure and Relay deadline evidence |

## Profile and properties actually checked

Two task IDs; two router candidates; one slot; two capture classes; one crash/restart; time classes 10,20,21,31,32; terminal classes success=2, cancellation/expiry=3, failure=4. Competing independent authorizations and later successor authorizations are both safety initial profiles. Every action kind remains in `baseStep`. No event alphabet or existing property was removed.

Safety uses `boundedStep` depth12, then stutters: 1,269,438 generated /177,918 distinct /depth13. This is bounded prefix safety, not complete system safety or liveness. Eight conjuncts: persistenceBeforeStart, finiteAuthorization, exactSettlement, receiptAfterDelivery, duplicateMessageSafety, originalFactBeforeRelease, durableDeliveryOwner, retainedResult.

Temporal `progress` and `observerIndependentDelivery` each exhaust 322,085 generated /46,824 distinct /depth21 from `initReplay` (nine pairs of retained terminal outcomes). No depth cutoff there. This initializer starts **after** durable commits at now32: enqueue/start/issue are consequently unavailable, so these counts establish conditional replay/delivery progress only, NOT election-to-execution liveness. Progress assumes eventual stable online observer/time32 and separate weak fairness of persistence, expiry, each delivery and receipt/publication/release effect. Observer-independent delivery assumes eventual stable online repository and fair delivery effects. These premises do not manufacture domain readiness or native permission.

`hook` abstracts callbacks as enabled completed idempotent effects. Pending captured settlement domains can disable actual callbacks; blanket callback liveness is not established. PerBoot init callbacks are not represented. Parent now guards `InitRepo::mark_done` with the currently selected dispatch under the running-dispatch read lock; old boot callbacks cannot satisfy a different boot, while PerNode/PerInstall remain durable replay. Parent regression `/tmp/parent-boot-replay-test-v2.log` is reported evidence, not a worker-executed gate.

## Commands and negative controls

From `specs/task-coordination`: `node runner.mjs lifecycle` (final exit0; `/tmp/quint-lifecycle-low-complete.log`). Runs typecheck, TypeScript seed1 tests (23, including imported component tests; eight own witnesses), TLC bounded safety, both temporal properties, unfairDelivery and four mutants. Runner uses existing pinned Quint0.33.0, TLC config 2GiB/two workers and supported `-Xss16m -javaagent:.verification/quint-json-limit.jar` environment. Existing tooling setup must supply that jar.

- No delivery fairness: actual temporal counterexample, not parser failure.
- Receipt before outbox drained: bounded safety violation.
- Lose consumed message identities on crash: QNT508 crashAfterConsumedReplyTest.
- Trust requested success after deadline instead of SQL winner: QNT508 sqlExpiryWinnerTest.
- Settle current capture instead of original: QNT508 lateSettlementExactCaptureTest.

Preserved failed attempts: `/tmp/quint-lifecycle-low.log` translation error without the existing JSON-limit agent; `/tmp/quint-lifecycle-low-v2.log` outer180s timeout after positive checks and two negative controls; `/tmp/quint-lifecycle-low-final.log` incorrectly targeted wrong **writer**, not capture, mutant passed. Inspection of `processor_slots::settle(s,w,t)` pinned that test-design error; corrected mutation changes tuple capture and fails. No production bug inferred from that invalid mutant. Final complete run passes all expected controls.

Hashes: lifecycle_composition.qnt `7c408ac777e2d5ad50c1471b680c24699ee8156c28d4b6e87ce01774b1b13c76`; runner.mjs `0b94d6b6b3aabd7d54eaaf514999c9331b27fcde3b79da3aad1a39d43d2d35c4`.

Independent acceptance still requires parent diff review and rerun. This is a substantive vertical lifecycle verification artifact, not completion of WHOLE tasks/triage verification. Full reducer event/guard correspondence, native runtime trace bridge, pending domains, arbitrary successor reconciliation, descendants, continuations, clock and GC remain required.
