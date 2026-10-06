# Bounded TaskTicket reducer audit

Executed 2026-10-10. Scope is `TaskDeclaration::validate_local`, `TaskTicket::{new,merge,is_terminal,terminal_summary}` in `src/daybook_core/tasks/model.rs`, not TaskStore projection, router/worker progress, native authentication or GC. Source SHA-256: `5cf0e84ceeef7e08e286201940cf3b518323549b3b1055370bb083959cd8ff8a`.

Recovered original worker steering requires source-faithful pure branches, explicit effect/environment contracts, absence separate from legitimate sequence zero, and no invented retirement implementation. Existing reducer files were unfinished artifacts, not accepted coverage. `reducer_progress.qnt` is worker composition and deliberately not run or expanded in this item.

## Correspondence

| Source | Model | Checked scope |
|---|---|---|
| new / terminal map absence | fresh / sequence -1 sentinel | base, insertion at0, replay, presence |
| validate_local | valid | inverted window, required deadline, deadline after retention, authoritative Only placement |
| merge prevalidation | error codes1–6 in exact source order | schema mismatch/unsupported, identity, declaration validation/equality, equal-sequence conflicting fact; atomic conflict rejection |
| publisher records | full Evidence set union | same publisher/different envelope survives; version includes witness |
| per-writer replacement | lane | success dominates cancellation independent of sequence; sequence orders same outcome; zero then later success |
| terminal_summary | summary | greatest success attempt/result, independent of lane sequence; lowest writer cancellation assumes valid map binding |
| join result | Merge.changed | zero insertion changes, exact replay unchanged, rejected merge unchanged |

No reducer semantic mismatch was found within the existing explicitly bounded valid profiles. Model optional scalar0 represents absence and restricts actual timestamp/ID/result classes to positive values; this does not cover real `Some(0)`. Terminal map binding and crypto are upstream contracts. Rust summary compares cancellation map keys against the retained lane's writer field; malformed key/writer inputs are outside this profile. Evidence vectors are represented as canonical sets; duplicate/noncanonical initial vectors are not covered. `Fact.reason` is meaningful only for cancellation in Rust, and fixture successes keep that unused coordinate constant. Local producer panic behavior and arbitrary raw inputs remain unverified.

## Mechanical discovery defect corrected

Quint's default test discovery selects run names ending in `Test`. Existing `coordination_join_checks` had twelve intended runs without that suffix: the actual baseline command executed only the two zero tests. Renamed those runs without changing their bodies; final command runs all fourteen. The standalone ticket checks use the same suffix and run eleven tests. This corrects executable discovery, not protocol behavior; earlier parse success or two-test output must not be described as complete reducer scenario coverage.

## Independent checks and results

From `specs/task-coordination`:

```sh
node runner.mjs ticket
node_modules/.bin/quint test --backend typescript --main coordination_join_checks --seed 1 coordination_join_checks.qnt
```

Both exit0. Final logs `/tmp/quint-ticket-final.log` and `/tmp/quint-existing-joins-final.log`. Runner parses/types three files; eleven standalone tests pass. Combined original join scenarios: fourteen tests pass, with existing RouterSlot assertions unchanged (no router expansion).

Finite safety graph:385 generated/64 distinct/depth7. Six incoming tickets include zero cancellation, positive cancellation/success/replay ordering, independent zero success and extra publisher evidence. Ghost-history oracle selects highest success sequence if any, else highest cancellation sequence; it does not call reducer lane/merge. Finite algebra:16000 generated/8000 distinct/depth1, all triples of twenty compatible history-derived tickets; idempotence, commutativity, associativity and no-error merge checked. Conflicting same-writer/same-sequence facts are rejection scenarios, not falsely included in total join algebra.

Four temporary mutants require exit1 and actual property failure: success-priority reversal, bypassed conflict rejection, zero treated as absence, dropped remote evidence. All observed QNT508 assertion failures, not parser errors. No negative-control scope was removed. These are safety/algebra results, not temporal liveness: no new fair-delivery claim is made for TaskTicket. Existing register checks/zero-boundary files were untouched.

An initial standalone run exposed the same naming/discovery gap (three rather than eleven tests); final rerun after suffix correction is the acceptance evidence. No Rust changed; no Cargo build was justified for model/test-discovery-only edits. Existing Rust source tests for publisher records and deterministic summaries were read, not claimed executed.

Next bounded item: expand TaskTicket descriptor/summary boundary profiles (optional presence distinct from zero, equal-attempt result tie-break and cancellation writer ordering) before claiming arbitrary-input correspondence or composed progress. Router/worker/triage and production fixes require separate assignments. Reviewer gate remains required.


## Boundary continuation

Optional timestamps now distinguish None(-1) from Some(0); shared fixtures migrated without changing actual optional values. Summary selection now handles success attempt0/resultNone correctly and preserves Rust's cancellation map-key-versus-retained-writer comparison. Thirteen isolated tests pass (`/tmp/quint-ticket-boundaries.log`), including optional0, equal-attempt result ordering, valid cancellation order and malformed bindings. Native `TaskStore::ticket` in `tasks/store.rs` derives both terminal map key and lane.writer from `representation.original.writer` before merge; this is a construction invariant in that ingress, not validation inside public pure merge. Malformed pure inputs remain separately exercisable.


### Current schema/deadline and receipt continuation

The current ticket profile uses schema3 and required signed `after`; absence-1 applies only to bounded optional `before`/`retainUntil`/result classes, not `after`. Optional Some(-1) remains outside the finite profile. `node runner.mjs ticket` passed the updated checks/algebra/mutants; `node runner.mjs ack` checks the approved exact-ack ledger boundary (both arrival orders, crash/reopen and three negative controls). See `verification-continuation.md` for exact commands, source correspondence and the blocked native successor-ID proof. No running-effect/GC or full-system acceptance is implied. Earlier optional-deadline/receipt statements are historical snapshots.
