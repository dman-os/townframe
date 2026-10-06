# Successor authorization — bounded independent review

This item reviews the current authorization boundary only. No Rust edits, Cargo, native reruns or delegation were performed. Parent owns production changes and native/remote acceptance. Source hashes are in `/tmp/quint-successor-source-hashes.txt`.

## Source correspondence

| Source seam | Model representation / independent observation |
|---|---|
| `TaskAuthorization::expected_task_id` in `rt/triage/origins.rs` | Collision-free tuple `(slot,capture,nonce,origin,deadline)` stands for the fixed domain-separated hash over fixed32-byte slot/capture/nonce and big-endian signed timestamps. Hash/encoding correctness and randomness are contracts, not Quint theorems. |
| `CapturedProcessorInput.authorization`, `ProcessorTaskInput.authorization`, `task_input` | Authorization participates in the canonical task input; `classify_input` requires exact canonical input equality. Typed/decoded bytes alone do not authenticate it. |
| `classify_input` recomputed-ID guards | Identity is recomputed from the independently proven capture, compared with declaration ID, and authorization ID must equal declaration ID. Authorization deadline must equal the immutable declaration deadline. |
| Historical `configuration_at` and widened duration check | `deadline-origin == witnessed lifetime`; model uses mathematical integer subtraction, mirroring non-overflowing i128 subtraction for two i64 values. Finite test times do not prove all signed extremes or decoder shapes. |
| Pool/publisher rights, provenance, capture generation, input equality | Explicit `trusted` conjunction contract. False never authorizes; model does not prove signatures, rights lookup, native documents or unavailable-proof classifications. |
| Origins transaction, descending successor selection, replay | Last authorized second reuses original; renewed unmet capture after expiry chooses a fresh nonce/ID, retains predecessor, and reopens with the same successor. One two-generation sequence is modeled; SQLite isolation and arbitrary crash/concurrent histories are not proven by the four-state graph. |

There is no origin-database argument in the remote authorization predicate. The database supports local issuance/replay; it is not substituted for remote authentication. `accepted` is the authorization subpredicate, not full `Runnable` classification: source still separately checks expiry, placement, settlement, rights and availability. Missing historical documents can mean NotReady, not necessarily Invalid.

## Checks and negative controls

From `specs/task-coordination`:

```sh
node_modules/.bin/quint test --backend typescript --main successor_authorization --seed 1 successor_authorization.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main successor_authorization --invariant safety successor_authorization.qnt
node runner.mjs successor
```

Direct initial checks passed (`/tmp/quint-successor-{tests,safety}.log`). Final reproducible runner passed12 named tests and safety:5 generated/4 distinct states/depth4 (`/tmp/quint-successor-final.log`). Eleven mutation controls each exit1 with actual assertion failure, not parser/tool failure:

- Drop binding to slot, capture, nonce, origin or deadline independently.
- Ignore authorization task ID or recomputed declaration task ID.
- Ignore declaration deadline or historical configured lifetime.
- Treat unverified bytes as authenticated input.
- Change last-authorized-second replay comparison from `<=` to `<`.

Origin/deadline identity tests keep the other duration/declaration constraints satisfied so redundant guards do not conceal missing hash coordinates. Declaration-ID test changes both claimed IDs consistently; only recomputation rejects it. Baseline rejection tests also distinguish historical lifetime mismatch and a trusted well-formed successor from unverified bytes.

## Parent evidence inspected, not rerun here

`/tmp/parent-successor-native-proof.log`:3/3 passed, including actual identity binding, concurrent/reopen origin selection and local native triage24.422s. `/tmp/parent-ack-deadline-review.log`:8/8 passed. These logs are parent evidence, not this worker's independent Cargo/native execution. The earlier120.020s triage timeout remains an unattributed mixed-snapshot failure; a later pass does not establish full causal attribution.

## Policy/upgrade objections and residual gaps

1. **Future-origin bound:** the reviewed guards enforce finite duration, not a relation between origin time and the executor's admission clock. Fresh authenticated input can choose a future origin/deadline. Local issuance uses current time, but remote proof alone is not a clock-skew or maximum-remaining-authorization theorem. No policy restriction was invented in the model.
2. **Lifetime-changing replay:** an unexpired local row is reused by slot/capture regardless of newly supplied configuration lifetime. A changed lifetime witness rejects that old duration (modeled by `historicalLifetimeBindingTest`). Triage capture generation derives artifact and document configuration facet values, not the shared lifetime directly. Same-capture replay can therefore require an original historical configuration witness, particularly after origin commit before publication. Retaining that witness or defining new-capture semantics is a parent product/design question; no permanent liveness claim or workaround is asserted here.
3. **Earlier prototype-table upgrade:** `CREATE TABLE IF NOT EXISTS` does not add nonce to an already existing earlier origins schema. Fresh/current-schema reopen coverage is not an upgrade/migration theorem. Whether that intermediate schema needs compatibility is parent-owned.
4. Crypto collisions/nonce freshness, integer extremes, arbitrary generation histories, complete authenticated input ingestion, remote/native composition, running-effect expiry and GC remain outside this item.
5. At this successor review snapshot full router safety had the pinned compiler no-verdict. The later router-only investigation resolves translation operationally with a larger JVM stack; see `router-compiler-resolution.md`. Neither item completes the original verification scope.

## Later chosen policy supersedes lifetime-replay objection

Parent implemented fresh authorization on changed configured duration, even before expiry, retaining the original for the same unmet slot/capture. Identical duration reuses while unexpired. Current `node runner.mjs successor` passes13 cases/6 safety states/12 mutation controls; see `deadline-merge-review.md`. This changes neither work generation nor settlement. Earlier12-case/4-state evidence is historical. Future-origin and prototype-upgrade questions remain open.
