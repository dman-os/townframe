# Source correspondence and coverage ledger

Bounded item: standalone encrypted register only. Parent approved the complete ownership-aligned task-coordination architecture, but all other standalone/composition models remain explicitly pending below. No Rust implementation changes belong to this item.

Source read: `src/big_sync_core/encrypted_register.rs`, initial snapshot `2DEC`, then corrected comparator/join snapshot `9AE7`. Corrected-source SHA-256 observed after reread: `8c132a5b441c0f00233a86ba2e7b8c0d405424d3333a7c5b8f9a0279805b16d0`. This is a moving workspace, not a commit claim. The current modeled comparator is header signature, binding, publisher, ciphertext, representation signature. Parent confirmed the pre-fix counterexample with real valid signature variants and changed Rust; the model was explicitly migrated without narrowing its domains. Historical evidence and an executable pre-fix mutant remain retained.

## Abstraction contract

- Three writer identity classes, record classes 1 (this register) and 2 (another record), sequences 1/2/3, semantic IDs 5/10/20/30/40/41/42/50/60, two header and representation signature classes, and two relevant wrapper/key classes. No counter saturation or arbitrary ID recycling is used.
- `semantic` denotes collision-free public semantic identity of the complete original signed statement commitment **and** frontier. It includes exact original body/signature identity through a trusted class mapping; header signature is distinct and may vary without changing semantic identity. No cryptographic arithmetic or hash-collision proof is attempted.
- Class40 observes writer1/version1 on this record; class41 references writer1/version3 on another record; class42 observes this record/version3 and is exercised in a targeted greater-dependency/poisoned-observer scenario. Additional frontier shapes, arbitrary dependency DAGs and all byte lengths are not enumerated. Structural validation represents canonical/noncanonical, self-past/self-future, exact-limit/over-limit and equality classes explicitly. Byte codecs, integer overflow in metadata size calculation, Ed25519 validity and content-addressed identity are trusted contracts, not verified implementation claims.
- `Lane` uses an empty sequence-1 sentinel (outside Rust u64 inputs), one representation for Current, and exactly two headers/no representation for Equivocated. The writer map always has three keys with empty sentinels; Rust omits empty entries. Sentinels are never serialized as a real Rust lane. Sequence zero is legitimate evidence, separately exercised by zero-boundary safety/algebra/fair-convergence profiles. Existing positive profiles retain their original input domains.
- Canonical binding integer denotes lexicographic equivalence/order of `(incarnation,key_ref,key_heads,encoding,parameters)`; key availability is actor-local environment state and is never an ordering input. Publisher/ciphertext/header-signature/wrapper-signature remain separately ranked. The independent oracle uses mixed-radix rank with all coordinates in 0..2, not the reducer comparator.
- Snapshot schema/binding/unique-writer/all-lanes-valid are acceptance classes. Invalid multi-lane envelopes reject before mutation. Descriptor validation is independently exercised, but there is no claim the Rust decoder/cryptographic library is proven correct by a boolean class.
- The reducer has no host durability. Crash/restart in delivery profiles retains a completed snapshot and restores it; pending finite evidence is retained by the environment. A crash before the next atomic reducer merge delays that input. Host SQL, source carry-forward, actor cancellation and receive acknowledgement crash cuts are pending transport/driver models, not silently covered here.

## Register operation / branch mapping

Status terms: **exercised** = named scenario executed; **checked** = corresponding finite graph/property checked successfully; **fails** = intended property has an observed counterexample; **contract** = explicitly abstracted, not implementation proof. See README results for exact commands and counts.

| Rust symbol / branch | Model correspondence | Independent obligation / negative control | Status |
|---|---|---|---|
| RegisterKey encode/decode: opaque boundaries, malformed prefix/length/trailing bytes | record equality classes; `opaqueRecordKeyAndSelfFrontierClassesTest` | different record class rejected; concrete codec remains contract | contract + exercised classes |
| deserialize_lanes duplicate writer | Batch.uniqueWriters false, invalidBatch reason3 | all-or-nothing merge, no duplicate alias | checked acceptance class; codec contract |
| OriginalStatement sign: canonical frontier, statement/header signer failure, verification failure | Descriptor frontier/signature classes; successful original identity token | failure returns no admissible representation | signer/crypto contract, not arithmetic proof |
| valid_frontier: sorted unique `(record,writer)`, self-reference strictly older; cross-record allowed | Descriptor.canonicalFrontier/selfPast; foreign class41 | bad ordering/self-future rejected, cross-record not projection dominance | exercised classes; frontier contents finite |
| LaneHeader verify: schema, frontier, writer signature | Descriptor.schema/canonicalFrontier/selfPast/headerSignature | every invalid class rejected | checked finite classes |
| OriginalStatement verify_header: exact key/writer/sequence/frontier/complete signed commitment, original signature | semantic identity token + readerAccepts | tampered original writer/sequence/semantic/signature rejected; reader mutant | exercised, cryptographic identity contract |
| Representation verify: header, sorted unique key_heads, publisher signature | Descriptor.headerSignature/canonicalKeyHeads/outerSignature | no invalid outer wrapper admitted | checked finite classes |
| canonical_cmp: all five ordering coordinates | before; independent mixed-radix rank; canonicalOrderingAndOutcomeTest | lexicographic rank including each coordinate; reversed binding mutant | exercised |
| validate_snapshot_binding: schema/key | Batch.schema/binding | invalid snapshot cannot mutate any lane | checked acceptance class |
| validate_header: schema, map-writer, key, key/dependency-key budget, frontier count | Descriptor fields, exact2/over3 classes | valid boundary accepted; each invalid field rejected | checked finite classes |
| validate_representation: ciphertext budget, metadata aggregate checked arithmetic | Descriptor.cipherBytes/metadataBytes | over budget rejects before mutation | checked finite classes; checked-add overflow contract |
| validate_lane Current: writer matches map | Descriptor.writer/mapWriter | mismatch rejects entire batch | checked finite class |
| validate_lane Equivocated: both writers bound, equal sequence, distinct semantics | validPoison, poisonedWitnessValidityTest | wrong writer, unequal version, same semantic reject | exercised finite classes |
| verify_lane poison: both signatures, increasing semantic identity | Descriptor header signatures + validPoison order | reversed/noncanonical poison rejected | exercised; crypto contract |
| merge_snapshot: validate all structural and signed evidence BEFORE mutations | accepted + merge; rejectedMultiLaneBatchTest | ghost history unchanged on rejected multi-lane batch; accept-invalid mutant | checked class + exercised negative |
| merge: single representation joins only after validation | singleton + merge | admitted exact representation appears in genuine history | checked exactIdentity |
| restore: same admission as remote merge, not authority proof | restore(batch), snapshotRestore | valid snapshot round trip retains evidence; invalid restore rejects | checked + exercised |
| new/empty, absent-writer insertion Current/Poison | empty; joinLane empty; transferable poison restore | join identity, correct outcome | exercised; algebra includes empty/poison |
| join lower sequence ignored / higher Current or poison replaces | joinLane sequence guards | max received sequence retained; stale replay mutant; poison recovery | checked replayFences + exercised |
| equal seq Current/Current same semantics: smaller wrapper replaces, else unchanged | canonical + joinLane | canonical exact original representation independent of keys/order | exercised; full corrected-source oracle result in results.md |
| equal seq distinct Current/Current: poison | witnesses union | exactly two least distinct identities, no live representation | checked shape/poison exclusion + exercised |
| Current/Poison, Poison/Current, Poison/Poison: up to four candidates, identity then signature | witnesses, transferred poison fixtures | canonical witness replacement; changed vs unchanged | exercised; full corrected-source algebra result in results.md |
| equal poison with same witnesses no-op | set equality, outcome0 | idempotence | exercised; complete finite algebra checked separately |
| canonical same-semantic header signature variants under later conflict | cs/c fixtures, signatureVariantFutureConflictTest and ghost-history expectedLane | never lose smaller signature evidence before poison; full safety/algebra and pre-fix mutant | parent fixed source; corrected model retains all signature classes |
| merge outcome aggregation: equivocation > changed > unchanged | outcome; canonicalOrderingAndOutcomeTest | semantic Changed/Unchanged/EquivocationRetained classes | exercised |
| omission of writer lane not removal | empty sentinels + join; history oracle | keep replay fences on omitted/higher other-writer input | checked replayFences |
| heads: only Current; other writer direct same-record `>=` dominance | dependencies/observes/heads; expectedHeads | independent causal projection, hidden lanes retained; foreign-record mutant | checked causality + exercised |
| representation writer lookup: poison returns None | Current representations size1 vs poison empty | no poison available for consumer scheduling | checked poisonExcluded |
| merge_local: lower-seq assertion, equal-seq only same semantics Current, greater allowed | localAllowed; localProducerAndReaderBoundaryTest | stale/equivocated same seq denied; exact wrapper replay allowed | exercised finite precondition; assertion mechanism contract |

The finite core faithfully exposes all join branches but does not claim every arbitrary raw byte/frontier/cryptographic input checked. The explicit descriptor/codec contracts must accompany any statement of completeness.

## Properties and profiles

- `register_checks.safety`: independently derived latest-version semantic history, canonical representation/poison signature oracle, structural invariant, causal heads, replay fences, exact received identity, poison exclusion. Ten genuine representations; three writers; three sequence levels; four key-availability subsets; invalid envelope/restore stutters. The pre-fix failure is preserved as a negative control; full corrected-source results are in results.md.
- `register_checks.behavioralSafety`: separately named structural, causal, replay, exact representation and poison exclusion facts. This historical diagnostic subset never substitutes for full safety.
- `register_algebra.algebra`: idempotence, commutativity, associativity, identity and snapshot round trip over finite independent picks of fourteen empty/current/poison maps (14 cubed assignments). Three writers, four conflict semantics, signature variants, levels1/2/3. No signature-determinism assumption; full corrected-source results are in results.md.
- `register_validation.boundaryClasses`: fourteen descriptors (one valid, thirteen independently invalid fields) plus deterministic witness/reader/batch cases. This discharges finite acceptance decisions only.
- `register_convergence.convergence`: two replicas, six finite genuine inputs, arbitrary reorder/delay/duplicate, partition/heal, crash/restart. Under eventual stable connectivity/online state and per-replica weak fairness of delivery, all finite genuine inputs eventually deliver and final state equals the independent full-set join oracle. Overwritten writes need not remain heads.
- `register_signature_convergence.convergence`: the same environment with five genuine inputs including opposing canonical wrapper/header-signature variants and a later semantic conflict; this full conflict profile must not be dropped even if the six-input recovery profile passes.
- `unfairConvergence`: removes delivery fairness while retaining the stable-environment premise; expected infinite-delay/stutter failure.
- Nonvacuity: partition/crash/heal/restart then nondeterministically drain the actual finite input count and observe available/converged/safety. Fault transitions remain enabled in the real temporal model. The witness does not stand in for temporal checking.

## Zero-sequence extension

`register_zero_checks` maps Rust map absence to sequence -1 and genuine zero to Current/Poison evidence. Its independent history oracle and shape/heads properties cover insertion, same-input replay, equal-zero semantic conflict, restore, later sequence1 recovery and stale zero replay. `register_zero_algebra` checks all triples of five states obtained from independent histories; `register_zero_convergence` reuses the same fair delivery/fault environment with three zero/recovery inputs. The sentinel-confusion mutant collapses equal-zero evidence and must fail the named zero witness. These finite classes do not prove arbitrary u64/frontier inputs.

## Full architectural scope pending subsequent bounded assignments

| Component | Required standalone or composition coverage | Status |
|---|---|---|
| TaskTicket / TaskStore | declaration/publisher/terminal joins, collision, projection and exact capture | bounded pure-ticket safety/algebra checked; descriptor/summary boundaries and TaskStore pending; see task-ticket-results.md |
| RouterSlot / RouterMachine | all eighteen events/eight actions, authority/claim/offer/capacity/freshness/session branches | original finite18-event safety passes2,457,600 states; complete semantic guard coverage remains pending |
| PoolWorkerMachine | all events/actions, classification token, persistence/start/cancel/restore/finishing/readiness/origin branches | pending |
| ProcessorCell / slot backends | evaluate/settle observations, concurrent desires, exact baseline/settlement, all-lane projection | pending |
| Triage | activation acknowledgement/boundary, config fence, full captured inputs, deterministic planning/enqueue and deferred source settlement | pending |
| CapturedRoutineAdapter | exact admission/journal/gate/recovery/outcome/receipt and native rights after awaits | pending |
| Native driver | every effect/completion token, independent jobs, retained reader, checkpoints, rotation vs revocation | pending |
| Storage / transport / certificates | selected binding, atomic publication, exact representation migration, source-only key provenance, audience, historical keys and consumer transaction | pending |
| System composition | cross-component safety and conditional liveness with explicit dependency assumptions | pending |
| BigSync reducers/walkers/scheduler/cursor/watermark | enumerate actual dependency contracts when composing; no assumed gap-free global-revision delivery per part | pending dependency examination; no standalone proof |
| Physical purge / garbage retirement | intended ADR-only policy, no invented implementation | unimplemented / product decision |

Full pending detail remains in `.agents/skills/quint-verification/references/townframe-coverage.md`. Do not infer completion from the standalone register item or promote environmental experiments into accepted product guarantees.

## Continuing full scope

Current evidence and explicit undelivered original obligations are maintained in `verification-continuation.md`. Historical default-stack router failure and receipt mismatch remain preserved there. Router original finite safety now passes with a larger JVM stack; exact-ack and successor boundaries have separate finite checks. Running-effect expiry/GC and full native composition are not established here. Parent acceptance remains independent of child checks.
