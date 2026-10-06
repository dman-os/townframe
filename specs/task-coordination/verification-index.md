# Canonical current verification index

This index supersedes chronological worker output as the acceptance map. Supporting reviews preserve historical failures; a newer source fix does not retroactively turn old failures into proof. Whole-system tasks/triage acceptance remains **incomplete**. Reviewer gate required. Source is concurrently changing: hashes in individual reviews identify audited snapshots; rerun plus source correspondence review required after drift.

## Current claims, source owners and executable gates

Run from this directory with `node runner.mjs <mode>`. TypeScript tests seed1; TLC uses pinned Quint0.33.0/Apalache0.62.1, tlc.json two workers/2GiB. Build `.verification/quint-json-limit.jar` from tracked source with `npm run prepare:verification` (full JDK >=21); large translations also need JVM-Xss16m. [README.md](README.md) documents which modes supply Java options and the explicit invocation for inherited options. No toolchain fallback. Raw logs under ignored .verification, outer evidence in /tmp.

| Component/source | Runner mode / actual coverage | Bounds / proof limits / correspondence reference |
|---|---|---|
| big_sync_core encrypted_register | verify; check; simulate; negative; algebra; signature | finite writer/sequence/signature/evidence profiles, full lane safety/algebra and conditional fair replica convergence; coverage.md/results.md. Simulation is not proof. |
| tasks/model.rs TaskTicket | ticket | finite declaration validity, publisher union, terminal joins/summary, atomic rejection/zero boundary and four mutants; task-ticket-results.md. Not TaskStore IO/archive proof. |
| tasks/router.rs RouterMachine/slot | router; guards | original18-event safety2457600 distinct/depth19; guards15 witnesses/seven controls including ordered Vec normalization. Single-use event exploration not all arbitrary repeated histories; reducer-guard-correspondence.md/ordered-registration-review.md. |
| tasks/worker.rs and processor slot/activation reducers | reducers | named router/worker/slot/activation witnesses, activation/slot finite safety and conditional worker progress; verification-continuation.md. Not complete worker event/guard implementation proof. |
| durable domain exact acknowledgements | ack |28 safety/progress states/depth7, exact ledger replay/arrival orders and three controls; domain-ack-contract-proposal.md + later continuation sections. Authenticated original outcomes remain inputs. |
| authenticated successor origins | successor | finite original/capture/nonce/lifetime binding,6-state replay profile,13 witnesses/12 controls; successor-authorization-review.md plus lifetime-policy continuation. Not logical workflow idempotency across arbitrary successors. |
| managed staging merge | deadline |23 safety states/four cases/three controls; deadline-merge-review.md. Publication/clock contracts remain finite abstraction. |
| host expiry and SQL completion | host; admission | finite owner/completion models; admission26 safety/progress states, Waiting cleanup10; host/admission reviews. Historical diagnostics intentionally expected-negative. |
| lifecycle vertical composition | lifecycle | corrected bounded safety171987 states/depth13; terminal-replay temporal46824/depth21 each;25 tests/six controls. Temporal initializer starts after SQL commit. Prepared admission, not volatile queue, owns progress; triage-delivery-review.md supersedes old queue contract. |
| activation/readiness/captured settlement | triage-delivery | activation25 states; composition1232 safety/conditional progress; six witnesses/two controls. Terminal->receipt needs eventual stable domain materialization and fair separate effects; triage-delivery-review.md. Runtime Pending repair separately validated, not materialization liveness proof. |
| descendants + host authorization | descendants |147654 safety states/depth24; five cases/four controls; inherited-only child/grandchild class; descendant-authorization-review.md. Dual-witness min exercised by continuation repair + parent Rust regression. |
| wflow continuation service invocation | continuation-repair |319316 safety states/four witnesses/four controls; immutable binding, post-context check, cache abort, restore/release; continuation-gate-repair-review.md. Does not fence internal service awaits/syscalls after invocation. |
| RegisterStore local handoff | handoff |2116 safety and conditional temporal states;3 cases/3 controls, exact existing register join, partial local knowledge, direct0->3 selection; register-handoff-review.md. Single-batch checkpoint class, not physical retirement. |

### Fairness is conditional, not system liveness

Register convergence needs eventual stable delivery/connectivity plus per-replica fair delivery. Lifecycle replay needs eventual stable repository/observer/time32 and named per-effect fairness; observer-independent outbox draining still requires actual callback readiness. Triage terminal delivery assumes eventually stable captured domain/materialization, online repository and fair settle/delivery/observe/receipt. Handoff localTransfers assumes each retaining node eventually online/selects3 and separate fair source-read/checkpoint/record-publication. None implies every task is created, every native dependency arrives, all peers acknowledge, or safe global deletion. Finite safety and witness schedules never substitute for liveness.

## Historical/diagnostic exclusions

- lifecycle_composition_historical_queue.qnt: known incorrect process-durable pre-SQL queue abstraction; original queued->progress retained for review, excluded from current claim.
- continuation_admission.qnt / continuations mode: former deadline-free source counterexamples and explicitly intended diagnostic guard, NOT current-source proof after engine patch.
- domain_receipt_counterexample.qnt and domain_receipt_arrival_orders.qnt: historical singleton/canonical-only receipt policy failures, not current exact-ack ledger verification.
- Historical host expiry silent-hint/Waiting diagnostics: old ownership gaps; admission repair reviews supersede, expected-negative traces preserved.
- facts mode: historical register safety subset, not full verify acceptance.
- Default-stack compiler failures, reserved-word parser failures, invalid/redundant mutants, port conflicts, timeout and ENOSPC logs: infrastructure/test-design evidence, not semantic pass/fail attribution. Expected-negative runner requires actual violation evidence and rejects parser/bind failures.

Other supported modes: check (register parse/typecheck/tests), simulate (bounded sampled register traces), negative (register mutation/unfair controls), algebra/signature (isolated full register properties). README contains detailed original register commands; no full-model-suite mode is implied.

## Serialization and narrow lock validation

Runner uses available util-linux flock, nonblocking exit75, no-fork exec and OS-held descriptor for the entire mode including mutants. Lock `/tmp/townframe-quint-apalache-8822-<uid>.lock` deliberately ignores per-shell TMPDIR. Process exit/termination releases lock; persistent file is not a stale lock. No dependency installation, polling/wait retry, secret env or alternate server/toolchain. Missing flock is explicit infrastructure failure. `--port-locked` is internal reexec marker, not supported operator bypass.

Direct quint invocations do not acquire runner lock: parent/manual callers must serialize or wrap the same flock path. Existing externally launched verifier cannot be detected by this advisory lock; socket binding errors remain infrastructure failures, never mutant evidence. Tests: node syntax passed; external flock held lock while runner check attempted ->exit75/no gate; after release invalid-mode probe reached usage->exit1 (not busy), proving natural release without running Quint. Logs /tmp/quint-runner-lock-conflict.log and /tmp/quint-runner-lock-release.log. No verifier run during this packaging item.

Parent tested contention against an actual `runner.mjs check` process after it entered its first gate, not merely an externally held lock: the second runner returned75 without running a gate; the original register parse/typecheck/tests completed with0; an invalid-mode probe after release returned the normal usage error1. Evidence: /tmp/parent-runner-lock-conflict.log, /tmp/parent-register-check-lock-gate.log and /tmp/parent-runner-lock-release.log.

## Runtime companions and independent review evidence

Parent independent gates: corrected lifecycle/triage/descendants/continuation-repair in /tmp/parent-{lifecycle,triage-delivery,descendants,continuation-repair}-serial-gate.log; full Router2457600 states /tmp/parent-ordered-router-full-safety.log. Parent independently read `RegisterStore::{carry_forward,check_current,merge_and_commit}` and the handoff model, then passed the complete `handoff` gate in /tmp/parent-handoff-serial-gate.log. Rebuilding the Java agent from tracked source and rerunning passed again in /tmp/parent-handoff-rebuilt-agent-gate.log: 2116 states for both safety and conditional local transfer, all three actual negative controls detected.

Rust companions: wrong accepted attempt/stale outcome plus ordered duplicate registration; min dual-witness deadline; real new/replayed JobInit expired admission; descendant SQL reopen late-success; pending completion independent drain/readiness; durable receipt ordering/reopen and host noobserver/native seams. Parent reports seven seam tests /tmp/parent-lifecycle-runtime-final-gate.log and latest Clippy /tmp/parent-post-review-clippy-v2.log passed. These are targeted seams, not a generated trace bridge covering every model action. reducer-conformance-vectors.json supplies manual semantic schedules; not every vector has executed Rust companion. Parent artifacts are reported evidence unless independently inspected, and binary/source drift must be checked. Earlier Clippy ENOSPC and native receipt failure retained in chronological reports.

Parent native storage regressions `pinned_keys_sequence_gaps_replay_and_consumer_settlement` and `native_relay_retains_ciphertext_without_payload_key_or_read_access` both passed in /tmp/parent-handoff-native-storage-gate.log. They exercise native key/owner replacement, selected successor carry-forward and consumer progress, plus opaque Relay retention without Read. They do not execute all handoff model schedules: arbitrary multi-batch revisions, direct0->3 offline selection and every publication/checkpoint crash cut still lack an exhaustive runtime trace bridge.

## Remaining ADR010/011 obligations

Complete all-event/guard/repeated-history worker/driver/triage correspondence; source materialization/config/predicate and arbitrary concurrent generation histories; native authority/provenance async crash cuts; arbitrary multi-batch handoff/replacement/late-source revisions; all-node only-copy retention. Managed internal service/syscall authorization after invocation, legacy child/binding migration and complete JobInit binding ingress still require runtime contracts. Stable domain operation idempotency/checkpoints across arbitrary successors is not task-ID deduplication.

Physical GC/retirement remains unimplemented/unverified: semantic archive/prune API, separate unacknowledged outcomes, execution tombstone lifetime, authenticated retired-allocation deny namespace at every allocation surface, Relay-verifiable original deadline evidence, explicit skew/rollback contract, absolute current-generation dead-row/garbage-byte budgets and measured defaults. No TTL/threshold/skew policy chosen. retention-allocation-audit.md gives exact source ledger and required APIs. Passing joins/control exchange or local carry-forward cannot certify physical deletion.
