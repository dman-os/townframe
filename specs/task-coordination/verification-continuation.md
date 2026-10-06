# Verification continuation and concrete blockers

Snapshot: 2026-10-10; later receipt/successor/router continuation sections supersede historical blockers. Parent has not independently accepted these results. This is **not completion of the original full verification scope**. Current-source models remain separate from the newly discussed finite-TTL/successor/domain-ack policy; ADR changes do not establish implementation.

## Original expected deliverables and status

| Expected surface | Current evidence | Remaining scope |
|---|---|---|
| Encrypted register | Prior positive/zero safety, algebra, fair convergence and mutants | Arbitrary inputs/crypto/host composition are contracts |
| TaskTicket | Optional None(-1)/Some(0), summary result tie-break, malformed binding; thirteen tests; finite safety/algebra and four mutants | Arbitrary descriptors, decoder/canonical vector classes, complete runtime projection |
| RouterSlot | Zero/replay/conflict/generation witness, independent history graph16 states | Arbitrary claims and full algebra/error composition |
| RouterMachine | Eleven named cases; original full18-event safety passes2,457,600 distinct states with larger JVM stack | Complete guard ledger, repeated profiles, fair routing; default-stack translation still fails |
| PoolWorkerMachine | Twelve named cases; original full one-occurrence graph1,320,096 states; additional depth8 repeated-event graph5,087 states; staged fair progress42 states and unfair control | Complete semantic guard coverage, multi-session/capture/capacity profiles, driver composition |
| ProcessorSlot | Source-faithful local evaluate/settle/visible desire, receipt ghost-history safety1,416,545 states and mutants | Exact capture heads/baseline identity, replicated-cell merge, host durability and conditional progress |
| Triage activation | Single-plug snapshot projection25 states, unavailable/boundary/disable witnesses and retrospective mutant | Ordered finite-prefix notifications, async refresh/publication/ack crash cuts, full planner/predicate/capture/source checkpoint composition |
| Durable domain/task acknowledgement | Concrete Rust reopen regression plus executable fair-retry counterexample | Approved exact-ack ledger implemented; finite boundary composition passes. Native triage and authenticated successor-ID proof remain blocked |
| Native storage/transport/driver/system | Source seams read, explicit contracts in architecture ledger | Full durable/effect/recovery composition remains undelivered |

No passing finite structural invariant above implies completed semantic coverage. Original approved scope is retained, not silently narrowed.

## Correspondence and mechanical corrections

- `task_ticket.qnt` now uses -1 for absent optional timestamps; shared fixture declarations retain their original semantic values. Summary folding chooses an actual success, including attempt0/resultNone, rather than a sentinel fact. Cancellation selection preserves Rust's comparison of incoming map key with retained lane.writer, even for malformed bindings.
- `tasks/store.rs::TaskStore::ticket` opens authenticated register originals, validates schema/task/pool/declaration, and constructs terminal map key **and** `SignedTerminalFact.writer` from `representation.original.writer`. Valid binding is enforced by construction on this ingress, not by public `TaskTicket::merge`. The pure malformed-input profile therefore remains separate and executable; no universal upstream-validation claim is made.
- `reducer_progress.qnt::advance` previously evaluated task2 against capacity1's stage map during temporal compilation. It now returns a disabled, fully assigned action for a task outside the profile domain. Both capacities and all existing stage transitions remain; out-of-domain runtime exception is not a property counterexample.
- `processor_slots.qnt` maps `ProcessorSlot::{desired,evaluate,settle,execution_baseline}` in `rt/triage/slots.rs`. Finite generations stand for exact capture equality classes; settlement tuples include attempt classes. Settlement never advances evaluation/observation clocks. Receipt ghost history independently forbids invented incorporation evidence. Latest settlement supplies the baseline head class; retained proof is not incorrectly folded into baseline.
- `triage_activation.qnt` maps the single-plug semantic projection of `install_activation_snapshot`, `query_activation`, and the strict `source_revision > source_boundary` gate. Snapshot consistency (rejected/disabled targets have no available manifest) is an explicit environment contract. This projection deliberately does not model Rust awaits as an atomic implementation theorem.

## Executed commands and evidence

All Quint commands below use scoped Quint0.33.0, Apalache0.62.1, Java21 and unchanged `tlc.json`. Logs are bounded/redirected. Verification processes must be serialized: overlapping compiler-server attempts produced a bare compile error and are not acceptance evidence.

```sh
# cwd specs/task-coordination
node runner.mjs ticket
node runner.mjs reducers
JAVA_TOOL_OPTIONS="-javaagent:$PWD/.verification/quint-json-limit.jar" timeout 1800 node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main worker_checks --invariant safety worker_checks.qnt
JAVA_TOOL_OPTIONS="-javaagent:$PWD/.verification/quint-json-limit.jar" timeout 1800 node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main worker_repeated_checks --invariant safety worker_repeated_checks.qnt
node_modules/.bin/quint test --backend typescript --main router_slot_checks --seed 1 router_slot_checks.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main router_slot_checks --invariant safety router_slot_checks.qnt
```

Final `reducers` exit0: `/tmp/quint-reducers-final-receipts.log`. Includes router11, worker12, processor2, activation2 named tests; activation1801 generated/25 distinct; processor16,173,617 generated/1,416,545 distinct/depth15; worker staged progress145 generated/42 distinct/depth11; unfair progress produces a real temporal violation. Mutants: bypass dispatch admission, increment settlement evaluation clock, forget retained settlement proof, allow boundary-equal retrospective work. Each actual assertion fails; parser failures do not qualify. An initial cancel-only mutant was not detected because cancellation also clears emitted tokens; that insufficient mutant was replaced with actual admission bypass, not counted as passed.

Worker original full graph exit0: `/tmp/quint-worker-long-serial.log`,17,920,673 generated/1,320,096 distinct/depth17,117.275s checker. Additional depth8 repeated profile exit0: `/tmp/quint-worker-repeated8.log`,95,706 generated/5,087 distinct/depth9. This additionally checks emitted dispatch implies persisted; it does not assume arbitrary callbacks represent real completed effects. RouterSlot exit0: `/tmp/quint-router-slot-safety.log`,65 generated/16 distinct/depth5.

Earlier worker Apalache4-step command timed out in Snowcat; no verdict. Original120s TLC attempt stopped midgraph; no verdict. One supported wildcard-import diagnostic passed TS but introduced a compiler operator cycle, so it was reverted. Source hashes pinned in `/tmp/quint-worker-blocked-source-hashes.txt` and `/tmp/quint-worker-long-source-hashes.txt`.

## Historical default-stack router compiler blocker and diagnostic isolation

Full acceptance target remains `router_checks.safety`, original18-event profile. It reproducibly exits1 after SanyParser with no checker verdict. Scoped `tooling/rpc-diagnostic.cjs` only observes child-process returned failures/callbacks, never requests/IR, and preserves exit status. It reveals `compileToTlaplus` Left `{explanation:"",errors:[]}`; server detailed log has no exception. No installed package, checker/domain/property or parser data was modified. Tool hashes pinned in `/tmp/quint-diagnostic-tool-hashes.txt`.

`tooling-repro/router-compiler/` contains diagnostic-only copies. `checks.qnt` uses a leader initialization plus stutter, **not original router verification**. Truncating handle after event14 compiles/checks; including event15 OfferDeclined fails with the empty error. Removing allocate/cancel helpers alone did not cure failure; replacing conditional blocks with equivalent filter/map did not cure the full copy. Passing `control.qnt` and failing `checks.qnt` preserve this boundary. Repro source omissions are solely diagnostic and must never replace the original acceptance target. Logs `/tmp/quint-router-minimal-first14.log`, `/tmp/quint-router-minimal-first15.log`, `/tmp/quint-router-diagnostic-value.log`; full baseline `/tmp/quint-router-full.log`. Focused minimization did not identify a supported full-model remedy.

## Confirmed durable receipt counterexample

Source: `rt/dispatch.rs::incorporate_task_receipt` retains and returns any existing success; `tasks/driver.rs` DomainTerminalFact requires exact returned task/summary, publishes local terminal and acknowledges only afterward, requeues on mismatch. `TaskTicket::terminal_summary` legitimately chooses greatest `(attempt,result)` after concurrent successes merge. Thus first-success receipt and later canonical success can disagree permanently under fair retry.

Test-only Rust edits approved by parent: `rt/dispatch.rs::tests::durable_receipt_acknowledges_later_canonical_success_after_reopen`; `tasks.rs` exposes existing cfg(test) fixtures crate-locally. Real ticket merge chooses attempt2/result-b, SQLite reopen retains attempt1/result-a, and the assertion mirroring driver's equality condition fails. This invokes actual receipt persistence/reopen and real ticket merge, **not native execution or live driver**.

```sh
OLLAMA_URL=http://localhost:11434 cargo nextest run -p daybook_core -E 'test(durable_receipt_acknowledges_later_canonical_success_after_reopen)'
OLLAMA_URL=http://localhost:11434 cargo clippy --all-targets --all-features -p daybook_core
```

Regression exit100,0.315s test: `/tmp/quint-receipt-rust-env.log`. Clippy exit0,5m02s: `/tmp/quint-receipt-clippy.log`. Initial build attempt reached tool timeout, subsequent compile failed missing compile-time OLLAMA_URL; neither is bug evidence. The configured environment rerun supplies actual failure. No ignored test or production fix added.

`domain_receipt_counterexample.qnt` reproduces real ticket-summary merge plus receipt retention and retry acknowledgement. Named exact-ack assertion returns QNT508; TLC `progress` fails under weakFair(retry),2 generated/1 distinct/depth1, infinite retry stutter. Logs `/tmp/quint-receipt-counterexample-{test,temporal}.log`. These are retained failures, not successful verification or negative mutants.

Parent owns the fix decision. Alternatives to inspect: (1) domain receipt is durable incorporation evidence independent of current canonical success, requiring a separately specified compatibility/ack relation and exact original-task/digest/effect fences; (2) receipt advances to canonical summary with monotonic deterministic ordering, requiring proof it neither fabricates new effects nor loses original durable result evidence. Neither is selected here. New TTL policy does not cure this existing acknowledgement mismatch.


## Approved receipt implementation and mandatory-deadline continuation

The approved production batch now uses explicit `LocalOutcome` versus `ObservedTerminal` requests. The adapter checks a local fact against the actual durable outcome, including `result_ref`, and retains supersession cancellation. Driver acknowledgement verifies task, immutable declaration digest, and exact incorporated summary before publishing the **original** finishing fact. Canonical result selection remains exclusively `TaskTicket::terminal_summary`.

`DispatchRepo` stores an atomic task/digest settlement owner and exact summary acknowledgement rows. Replay returns the matching row, not the current winner. Existing singleton receipts migrate transactionally to only their one known acknowledgement, using bounded256-row pages; conflicting digests are rejected. Cleanup/retention is still parent-owned: this adds neither GC nor a perpetual-retention promise.

The Rust reopen regression now exercises ascending **and descending** genuine successes, replay, second reopen, and digest-conflict rejection. Legacy migration checks one known acknowledgement without inventing another. Native command restart passed with the forged-original-result rejection assertion; this is from the earlier binary, not validation of the final mandatory-deadline batch.

Mandatory `TaskDeclaration.not_after_secs: i64`, schema3 and digest v3 are now reflected in `task_ticket.qnt`: `after` is always present, including negative signed values. Only bounded optional `before`/`retainUntil`/result classes use absence-1. Optional Some(-1), arbitrary signed ranges, actual JSON decoding and digest bytes remain outside the Quint profile. Rust tests cover required-field decoding/identity. Adapter classification/start use `expired_at`; readiness wakes at `Some(deadline.saturating_add(1))`. Command fixtures authorize an explicit finite now+3600 interval; processor fixtures carry an explicit nonzero one-hour lifetime. This does **not** establish running-effect expiry or GC.

### Executed post-resume gates

- `OLLAMA_URL=http://localhost:11434 cargo nextest run -p daybook_core -E 'test(durable_receipt_acknowledges_later_canonical_success_after_reopen) | test(legacy_domain_receipt_migrates_only_its_exact_ack) | test(processor_task_lifetime_is_explicit_and_origin_bound) | test(replay_reopen_and_concurrent_origins_preserve_deadline_until_successor) | test(result_retention_never_removes_the_execution_deadline) | test(missing_or_null_deadlines_cannot_decode_as_task_declarations) | test(execution_deadline_is_part_of_the_immutable_task_identity) | test(execution_expiry_and_collection_account_for_admission_clock_lag)'`: exit0,8/8 passed; `/tmp/quint-ack-required-deadline-gate.log`.
- `OLLAMA_URL=http://localhost:11434 cargo clippy --all-targets --all-features -p daybook_core`: exit0; `/tmp/quint-ack-required-deadline-clippy.log`. Redis future-incompatibility warning remains visible.
- From `specs/task-coordination`, `node runner.mjs ticket`: exit0,13 named cases,64 safety states,8,000 algebra states and four assertion-failing mutants; `/tmp/quint-required-deadline-ticket.log`.
- `node runner.mjs ack`: exit0,2 named cases,28 safety/progress states/depth7, three assertion-failing controls (first-success retention, canonical-only selection, digest overwrite); `/tmp/quint-ack-model-complete.log`. Direct TS/safety/progress commands also passed in `/tmp/quint-ack-model-{tests,safety,progress}.log`.

`domain_ack_ledger.qnt` composes exact ledger commit/replay and driver delivery for both arrival orders, response-before-crash and commit-before-response. Weak fairness of the staged retry action yields both acknowledgements. Original genuine outcomes/authenticated caller proofs are explicit inputs, not execution theorems. It does not model arbitrary interleavings, live native driver execution, remote proof availability, time expiry, successor publication, ledger collection or result retention.

### Preserved failures and concrete remaining blockers

`/tmp/quint-ack-native-gate.log`: exit100; captured archived recovery passed43.050s, native command restart passed30.771s, native distributed triage timed out120.020s awaiting durable preparation; uninterrupted native command was unrun after fail-fast. The timeout is pinned to `ladder.rs`'s task-attempt/event observation loop, **not** dismissed by the later8-test gate. Parent owns diagnosis/rerun; no concurrent retry was performed.

Adapter classification still requires the deterministic `blake3(slot,capture)` processor task ID. Parent's new durable origins can mint fresh successor IDs. This is a concrete authorization mismatch, not complete attribution of the earlier timeout. Parent explicitly reserved replacing the proof with authenticated slot/capture/immutable-deadline binding; the old check is preserved, not silently removed or replaced with trust in a node-local origin table.

Original full router safety remains blocked by the pinned compiler failure. Complete native durable/effect/driver composition, running-effect expiry, successor proof and lifecycle/cleanup coverage remain unfinished. Historical receipt-policy counterexamples continue to represent the former bug, not the new ledger implementation; their imported ticket fixtures now use schema3.

Additional post-resume checks: TS router11 and worker12 cases passed after shared schema/deadline fixture updates (`/tmp/quint-required-deadline-{router,worker}-tests.log`). Historical receipt `progress` still exits1 with an actual temporal counterexample, and the expected-negative wrapper passes (`/tmp/quint-ack-historical-counterexample.log`). Source hashes: `/tmp/quint-ack-resume-source-hashes.txt`. No Git/staging commands or full Rust suites were used; unrelated working changes remain preserved.

## Successor authorization proof review supersedes the deterministic-ID blocker

Parent replaced the old deterministic-only check with authenticated authorization recomputation over proven slot/capture, nonce, origin and deadline; both claimed task IDs, declaration deadline and historically witnessed lifetime are checked. See `successor-authorization-review.md` for the independently read source mapping and explicit trust contracts. This worker edited only specifications/evidence, not Rust, and ran no Cargo/native gates.

`node runner.mjs successor` passes12 named cases,4-state replay/successor safety and11 assertion-failing controls (`/tmp/quint-successor-final.log`). Parent logs inspected separately show3 successor/local-native tests passing and8 receipt/lifetime tests passing. The earlier mixed-snapshot triage timeout remains unattributed, not retroactively accepted. Future-origin clock bounds, lifetime-changing replay witness retention and earlier prototype-schema upgrade need parent policy/compatibility review. Full router safety remains compiler-blocked; remote/native composition, running-effect expiry and GC are not established by this item.


## Full-router operational compiler repair and safety verdict

The unchanged original full `router_checks.safety` now passes with supported JVM `-Xss16m`:43,120,641 generated /2,457,600 distinct /depth19, repeated by `node runner.mjs router`. Original default-stack failure and minimized reproducer remain preserved. See `router-compiler-resolution.md` for exact commands, source hashes, warnings, failed helper extraction and compiler-seam limits. This is stack-sensitive translation, not an instrumented exception attribution or a model/property refactor. Complete repeated-event/guard/liveness/native coverage remains unfinished.

Parent evidence inspected separately: remote/ack5tests pass and scoped Clippy exit0 in `/tmp/parent-successor-remote-ack.log` and `/tmp/parent-successor-clippy.log`. No Cargo was run for this item; older mixed-snapshot native timeout remains unattributed. Future-origin bound, lifetime-witness policy and prototype schema migration remain parent-owned/pending.

## Deadline staging publication and chosen replay policy

`deadline-merge-review.md` maps original durable attempt deadline lookup, private bounded merge under the document lock, immediate pre-replacement expiry refusal and root post-merge recheck. `node runner.mjs deadline` passes4 cases/23 safety states and detects all3 required mutants. It distinguishes an authorized issued mutation from later persistence/notification; this is not complete execution expiry.

The implemented lifetime-change policy now creates a fresh authorization for the same unmet capture before expiry, retaining predecessor ID/deadline. Identical-lifetime replay reuses. Updated successor gate passes13 cases/6 states/12 controls. Earlier lifetime-only replay mismatch objection is superseded by this chosen policy; future-origin bound and prototype migration remain unresolved.

Parent logs inspected independently as evidence only:6 effect/native tests and Clippy pass; original full router safety independently passes2,457,600 states/depth19. Worker ran no Cargo/native gates. Host cancellation, continuations, success/domain-publication fences, full lifecycle and GC remain outside this item.

## Host expiry ownership and durable completion ordering

See `host-expiry-completion-review.md`. `node runner.mjs host` passes the finite boot-discovered/no-observer owner safety and conditional progress (8 states), completion status/order safety (12 states), six mutation controls and three expected diagnostic failures. Actual SQLite winner—not requested success—drives post-completion callbacks; attempt evidence survives expiry-row removal in `complete`.

Do not generalize these results: mapped Waiting failure uses `set_waiting_failed`, which does not remove the expiry row; silent post-commit admission hint loss with cached absence needs lifecycle validation; terminal completion before hook/reply delivery lacks an established replay obligation. Diagnostics remain executable rather than excluded. Descendant/continuation inheritance, durable domain expiry/replan notification, future-origin offset/skew contract and GC remain open. Processor origin now gates not-before, but is not a maximum future-offset theorem.

Parent4-test v2 log and earlier Clippy inspected as parent evidence only. Duplicate-dispatch fixture baseline preserved; no worker Rust/Cargo/native runs. No full execution-expiry or original-scope acceptance.

## Repaired admission cancellation and Waiting terminal ownership

`admission-ownership-repair-review.md` supersedes the historical silent-hint and Waiting-row gaps. Repository worker—not cancellable pool caller—owns accepted frozen request commit/cache/notify; due reads share the publication fence. Waiting failure checks current Waiting status and deletes expiry ownership in the terminal transaction. `node runner.mjs admission` passes2 cases,26 safety/progress admission states,10 cleanup states and6 negative controls; all5 status classes remain represented.

Original silent-hint/Waiting diagnostic modules remain historical executable counterexamples. Terminal hook/direct-reply replay remains unfixed. Parent3-test/Clippy seam evidence and separate actor-manifest TIMEOUT/fix are recorded only as parent evidence; no worker Cargo/native gates or actor modeling. Strong live-repository/fair IO premises, descendant/continuation and domain expiry/replan/clock/GC gaps remain explicit.

## Composed lifecycle recovery continuation

See `lifecycle-composition-review.md` and `node runner.mjs lifecycle`: independently recovered vertical composition, bounded safety177,918 states/depth13, retained-terminal conditional temporal replay46,824 states/depth21,23 named/imported tests and five meaningful negative controls. The temporal initializer begins after terminal commit, not election; no whole-system liveness claim. Captured settlement pending-domain callbacks, PerBoot selection, descendants/continuations/clock/GC and full event/native correspondence remain explicitly open. Historical failed translation, timeout and incorrectly targeted mutant logs are preserved and explained there.

## Router/TaskTicket guard correspondence continuation

`reducer-guard-correspondence.md` inventories all18 Router events/eight actions and TaskTicket operation guards. `node runner.mjs guards` passes12 targeted source-grounded witnesses and detects six assertion-failing controls; `/tmp/quint-reducer-guards-accepted.log`. Six manual semantic schedules in `reducer-conformance-vectors.json` are suitable for parent Rust bridge but not runtime-tested. Duplicate-task registration Vec-to-map versus model Set and canonical publisher vector/set correspondence remain gaps. Full semantic/event/native correspondence is not accepted. Parent's active descendant authorization seam remains excluded. Earlier redundant-control and insufficient-readiness-witness failures are retained and explained.

## Ordered registration discrepancy closed

`ordered-registration-review.md` supersedes the duplicate-active-task model gap: incoming List normalizes to retained task-keyed Set with existing Rust last-entry-wins semantics. Three new witnesses and first-wins mutation control; narrow guards15 pass/seven controls detected. Original Router safety rerun unchanged profile passes2,457,600 states/depth19 (`/tmp/quint-ordered-router-safety.log`). Duplicate order itself is witness-tested, not exhaustive safety-profile coverage. Lifecycle/native/descendant remaining scope unchanged.

## Pending captured-domain delivery and corrected admission crash contract

See triage-delivery-review.md: enabled actor remains attached under manifest/domain Pending, source readiness uses strict activation boundary, terminal/outbox/observation/exact settlement/receipt separate. Triage delivery safety/progress1232 states and two controls pass. Runtime Pending callback currently propagates error from watcher; retained retry/wake ownership remains concrete blocker reported to parent.

Parent review corrected invented durable pre-SQL admission queue: crash drops unprepared requests; acknowledged preparation survives. Historical model/property preserved in lifecycle_composition_historical_queue.qnt. Current lifecycle gate passes171987 bounded safety states,46824 replay states and six controls; no whole-system liveness claim.

## Descendant authorization finite safety continuation

`descendant-authorization-review.md` maps atomic parent/deadline inheritance, retained pending witness, bounded expiry ownership, startup/start and SQL/publication fences. `node runner.mjs descendants` passes5 witnesses,147654 safety states/depth24 and four controls. Inherited-only child profile; direct+inherited authorization precedence needs ingress contract review. Legacy child migration, continuations, clock/GC and runtime liveness remain missing. Parent Pending callback repair supersedes old generic-error source finding but is not worker runtime validation.

## Continuation pre-action authorization gap

continuation-admission-review.md / continuation_admission.qnt pin deadline-free ExecutionGate permitting released timer/message/retry/restore RunJob after expiry before cancellation incorporation. Actual-source safety fails; required-contract diagnostic with min(direct,inherited) pre-action guard passes7716 states, ignoring inherited fence fails. Three witness schedules; node runner.mjs continuations exit0 expected-negative wrapper. Parent notified; actual engine binding/linearization remains missing, not accepted by eventual terminal fences. No runtime reproduction or whole-system theorem.

## Implemented continuation invocation fence

continuation-gate-repair-review.md supersedes prior deadline-free engine finding. Current source immutable job bindings survive release, rebuild while paused at boot, and expiry recheck immediately before host invocation returns Aborted/drops cache. New continuation_gate_repair safety319316 states/four witnesses/four controls pass; historical bad schedules preserved. Host internal awaits/syscalls after invocation and full missing-binding/migration contract remain open. Fixed Apalache8822 requires serial cross-session verifier runs; bind errors rejected as tooling, not mutant detection.

## Retention/retired allocation research audit

retention-allocation-audit.md maps ADR011 to local exact carry-forward/checkpoints and current-owner publication fences, generic BigSync re-add/allocation surfaces, semantic archives/unacked outcomes/execution tombstones/dead membership/retired namespace distinctions. Current replay deliberately republishes missing current membership even on unchanged join; generic ensure_part_ref has no retirement guard. This is missing task-backend retirement contract, not a generic re-add bug. Absolute garbage accounting/budgets, Relay-verifiable expiry, all-node only-copy carry-forward, archive/prune and physical purge APIs remain unimplemented/unverified. Research-only: no verifier commands while parent's serial8822 gates run; no thresholds or clock defaults chosen.

## Local register handoff composition

register-handoff-review.md/register_handoff.qnt compose current register joins with selected-owner transaction fence, local source partial knowledge, direct0->3 migration and publication/checkpoint crash cuts. node runner.mjs handoff passes3 witnesses,2116 safety/conditional per-node transfer states and three controls. No Read needed in Relay class, no delete/retirement permission. Multi-batch revision and further-successor races remain open; no global collection proof.
