# Encrypted-register verification results

Snapshot: 2026-10-10. Parent accepted the bounded register design after independent check/verify/negative execution. These are finite specification results, not a proof of arbitrary Rust inputs or the pending coordination system.

## Executed corrected-source commands

Working directory: `specs/task-coordination`. Quint0.33.0 from the scoped npm lock; TypeScript simulation, Apalache0.62.1 translation and Java/TLC with two workers/2GiB heap. Actual combined smoke:

```sh
npm run check:register && npm run simulate:register && npm run verify:register && npm run negative:register
```

Completed exit0 in129.12 seconds. Eight parse/typecheck commands returned0. Fifteen named tests passed:11 register cases,3 descriptor cases,1 partition/crash/restart convergence witness. Simulation ran100 samples up to30 transitions, seed1; no violation, observed lengths31 states (max/min/average31). Simulation is smoke, not exhaustive proof.

The complete finite TLC results from that run:

| Property / main | Generated | Distinct | Graph depth | TLC seed | Result |
|---|---:|---:|---:|---:|---|
| safety / register_checks | 77,825 | 4,096 | 12 | 8787845067959187875 | exit0; complete graph |
| algebra / register_algebra | 5,488 | 2,744 | 1 | 6042080357935342554 | exit0; all14 cubed initial triples + stutter |
| boundaryClasses / register_validation | 197 | 14 | 3 | 3070869610893336487 | exit0; complete descriptor graph |
| convergence / register_convergence | 294,913 | 32,768 | 16 | -6240993578701199368 | exit0; complete six-input delivery graph |
| convergence / register_signature_convergence | 69,633 | 8,192 | 14 | 6032347732822120669 | exit0; complete five-input signature-conflict graph |

Exact underlying verification command form:

```sh
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main register_checks --invariant safety register_checks.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main register_algebra --invariant algebra register_checks.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main register_validation --invariant boundaryClasses register_validation.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main register_convergence --temporal convergence register_convergence.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main register_signature_convergence --temporal convergence register_convergence.qnt
```

The runner uses the absolute path to the same scoped `tlc.json`. Raw current logs are in ignored `.verification/`. TLC fingerprint seeds are recorded evidence, not CLI-controlled reproducibility promises; exhaustive finite state counts are deterministic for these profiles. Fingerprint collision estimates remain TLC's usual limitation (largest reported optimistic estimate4.7e-10 for the32,768-state graph).

An initial post-fix all-to-all algebra graph was deliberately interrupted: it redundantly generated14 cubed successors from each of14 cubed assignments. Moving the complete triple quantification into init and using a stuttering step retains every algebra input, reduces generated transitions to5,488, and does not weaken the property. That interrupted run is not counted as a pass.

## Negative controls

`npm run negative:register` requires exit1 with an actual violated property/assertion, not merely a parser/typechecker/tool failure. The final runner includes eight temporary mutants and removes all temporary model directories in `finally`:

| Mutant | Exercised consumer-visible model obligation |
|---|---|
| stale-replay | obsolete version cannot replace recovered lane |
| drop-hidden-replay-fence | causally hidden writer lane remains stored and cannot resurrect by replay |
| poison-witnesses | two least distinct semantic witnesses retained |
| partial-invalid-batch | invalid multi-lane input cannot partially mutate |
| foreign-causality | cross-record dependency cannot suppress this-record head |
| wrapper-arrival-rank | canonical order agrees with independent mixed-radix oracle |
| tampered-original | reader cannot accept different exact original identity |
| pre-fix-signature-order | valid signature variants preserve later poison join associativity |

Each focused mutant test observed QNT508 assertion/expect failure. The pre-fix comparator also underwent the **full fair-delivery temporal check**, observing exit1,88,129 generated/10,368 distinct/depth14, seed-5711941839894357822 in the recorded combined run. Both peers had received every genuine input, pending sets were empty, both online and connected, yet final semantic10 poison signatures differed1 versus2; infinite stuttering therefore satisfies delivery fairness while failing convergence. This is not the unfair-control starvation trace.

The separate unfair property removes delivery fairness while retaining eventual stable availability. Recorded combined-run counterexample: exit1,56,972 generated/10,439 distinct/depth8 at discovery, seed7392532358389429869; a nonconverged available state stutters indefinitely. Discovery counts are partial exploration, not a complete graph claim.

The final eight-mutant runner was subsequently observed in a foreground `npm run negative:register`: exit0 in34.96 seconds, including QNT508 from hiddenReplayFenceTest and the full pre-fix fair convergence violation (88,129 generated/10,368 distinct/depth14, seed7516804747477387408). Latest unfair discovery was284,627 generated/32,420 distinct/depth13, seed-6844744905214334488. No temporary mutant directory remained. An earlier asynchronous rerun lost its native completion notification after a parent interrupt; that missing tool result was reported and is not used as independent completion evidence. Parent also reported independently rerunning check/verify/negative, exit0 (99.40 seconds), including all eight mutants. Parent-reported verification is attributed, not claimed child-executed.

## Before/after discovery evidence

Before the Rust/model comparator repair:

- Full safety: exit1,610 generated/204 distinct/depth4 at discovery (final ten-input fixture); cs -> c -> semantic5 conflict loses the minimum header signature. An earlier nine-input run found cs -> c -> b with184 generated/89 distinct/depth4.
- Full algebra: exit1,1,314 generated/663 distinct/depth3 at discovery in the earlier all-to-all profile.
- Full five-input fair convergence: exit1 after complete graph88,129 generated/10,368 distinct/depth14, seed-6626781132432217669; every input delivered, peers nevertheless retained different exact poison witnesses.
- Seeded simulation: exit1 under top-level seed1, reported individual trace reproduction seed0x1996. This was a simulated counterexample, not symbolic proof.

Parent then reported a concrete valid-signature Rust regression failing before (0.127s) and passing after, all10 register Rust tests passing (0.746s), and scoped all-target/all-feature big_sync_core Clippy exit0. This worker executed none of those Rust/Cargo commands. It freshly read corrected `canonical_cmp`, migrated the model comparator and independent rank oracle to header-signature-first, and reran the unchanged full domains/properties. The historical comparator remains an executable mutant; see [exact counterexample](counterexample-signature-order.md).

## Bounds and remaining questions

Three writers; sequences1/2/3; four conflicting semantics at one writer/version; header/wrapper signature classes; two records and keys; ten finite genuine inputs for safety; fourteen algebra states; fourteen descriptor classes; two delivery peers with finite genuine input sets of six/five. No counter saturation, ID reuse, arbitrary-input liveness or cryptographic arithmetic proof. Conditional convergence requires eventual stable availability and weak fairness of delivery per replica; because genuine inputs are finite and successful delivery strictly removes one pending item, no message can starve under that premise. Duplicates do not discharge delivery fairness. Fault transitions remain in the graph.

Current standalone finite profile obligations are checked; crypto/codec/host durability remain explicit abstraction contracts. Router/worker/ticket/processor/triage/execution/transport/driver/system and actual BigSync dependency semantics remain separately pending. Physical purge remains unimplemented. Do not promote the finite experiments or parent approval into environmental product guarantees.


## Zero/absence correspondence correction — replacement owner

Executed 2026-10-10: `npm run check:register && npm run simulate:register && npm run verify:register && npm run negative:register`, exit0; final log `/tmp/quint-zero-final.log`. No Rust edits or new product policy. Absence now uses -1 (outside u64), not zero. Actual sequence0 inserts, replays, poisons on equal-version semantic conflict and recovers at sequence1, matching Rust map absence/insertion and sequence ordering. The existing history oracle supports zero without a special-case guard; empty history alone selects absence.

Sixteen named tests passed (11 existing register, one zero witness, three descriptor, one delivery witness); 100 sampled positive traces passed. Existing positive TLC domains/properties were unchanged: distinct safety4096, algebra2744, validation14, convergence32768, signature convergence8192. New complete graphs: zero safety33 generated/8 distinct; zero algebra250 generated/125 distinct (all triples of five absence/current/poison/recovery states); zero fair convergence3841 generated/512 distinct. This remains finite specification evidence, not implementation proof.

Nine focused mutants now pass the negative-control gate. New `zero-sentinel-confusion` changes the equal-absence branch to treat all equal sequences<=0 as empty; the zero replay/conflict witness observes an actual QNT508 assertion failure. Existing unfair convergence and pre-fix signature-order fair convergence still produce genuine temporal violations; no parser failure counts. The initial zero safety/witness run also exited0 before adding zero algebra/convergence; the final combined run above is the acceptance evidence.

The earlier zero-boundary finding in `register-owner-audit.md` is historical and now addressed for these finite classes. Crypto/codec/host durability, arbitrary integer/frontier coverage and runtime/system composition remain outstanding. No additional Rust commands were needed for this spec-only correction; the independent signature regression/Clippy results in that audit remain attributed to that prior run. Reviewer acceptance is still required.
