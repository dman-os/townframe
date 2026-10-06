# Replacement-owner register audit

Audit executed 2026-10-10; this is an independent rerun of the existing bounded register item, not acceptance of the unfinished reducer/system models.

## Recovered ownership and decisions

Original OMP child session:
`/home/asdf/.omp/agent/sessions/-repos-rust-townframe/2026-10-02T14-41-27-869Z_01a0fd10-213d-702d-9067-c0b26ec19e8d/Research maintainable Quint adoption and propose architecture.jsonl`.

Text-only extraction produced 13 messages, 79 lines, 14,731 bytes. These are largely parent steering messages, not a complete child report. Read alongside the child's maintained coverage/results artifacts. The failed replacement fork's extraction contained inherited recap/assignment context, not a new Quint finding.

Recovered approvals: ownership-aligned component models, explicit BigSync dependency contracts, no invented physical GC; signature variants remain reachable; full register safety and fair signature-conflict convergence must not be narrowed. Subsequent reducer work had separate source-correspondence and compiler-import issues. Parent approved a scoped parser-size-only JVM adjustment, not metadata pruning. None of that unfinished work is discharged by this audit.

The register Rust source SHA-256 is still
`8c132a5b441c0f00233a86ba2e7b8c0d405424d3333a7c5b8f9a0279805b16d0`, matching the recorded ledger. Fresh inspection confirms signature-first `Representation::canonical_cmp`, bounded semantic/signature poison selection, and the concrete valid-signature regression. No production code was edited.

## Independently executed evidence

From `specs/task-coordination`:

```sh
npm run check:register && npm run simulate:register && npm run verify:register && npm run negative:register
```

Exit 0; log `/tmp/quint-owner-register-audit.log`, component logs in ignored `.verification/`. Existing named tests and 100 sampled traces pass. Full TLC graphs:

| Obligation | Generated | Distinct | Depth | Fingerprint seed |
|---|---:|---:|---:|---:|
| safety | 77,825 | 4,096 | 12 | 7913416756088178389 |
| algebra | 5,488 | 2,744 | 1 | 5479525752680394075 |
| descriptor validation | 197 | 14 | 3 | -6649899112579393697 |
| fair delivery convergence | 294,913 | 32,768 | 16 | -5754577451128832579 |
| fair signature-conflict convergence | 69,633 | 8,192 | 14 | -7328857221367371900 |

All eight focused mutants produced expected assertion failures; unfair convergence and the pre-fix comparator produced actual temporal violations, not parser failures. Pre-fix fair convergence: 88,129 generated / 10,368 distinct / depth 14 / seed 365478580101100613. The runner removed its own temporary mutants. A retained `mutant-akGsUS` directory remains in `.verification/`; its files predate this audit and were preserved rather than attributed to this successful run.

From the repository root, with its already configured Cargo target:

```sh
cargo nextest run -p big_sync_core -E 'test(valid_signature_variants_preserve_poison_join_associativity)'
cargo clippy --all-targets --all-features -p big_sync_core
```

Both exit 0. The exact regression passed in 0.094s; unrelated tests were filtered, not disabled. Logs: `/tmp/quint-owner-rust-regression.log`, `/tmp/quint-owner-clippy.log`. This rerun confirms the corrected source, not a newly executed failing-before experiment; before-fix evidence remains historically attributed.

## Verdict and next bounded item

The existing **positive-sequence finite register profiles** remain reproducibly checked. TLC supplies genuine finite-state temporal checking under eventual stable availability and per-replica delivery fairness; simulation alone does not. Finite pending inputs strictly decrease on delivery, so the stated fairness premise does not need fairness per individual message in this profile. Crypto/codec correctness, host durability, arbitrary populations/frontiers, and composed runtime liveness remain contracts or gaps.

**Concrete correspondence gap:** `register.qnt` uses sequence 0 as empty and `joinLane` collapses equal-zero lanes to empty. Rust absence is map absence; `join_lane` inserts an incoming lane even at sequence 0, and no positive-sequence admission condition was found in the register. Thus the current model is not source-faithful for legitimate zero-sequence inputs. The original parent explicitly recorded this gap. Passing positive profiles must not be described as full input-domain verification.

Recommended next bounded item: distinguish absence from zero in the register model, add zero insertion/replay/equal-sequence conflict/later-sequence recovery witnesses and independent properties, then rerun unchanged positive profiles plus zero-boundary profiles and a sentinel-confusion negative control. This is a model correction, not justification for rejecting zero in Rust. Do this before expanding router/worker/triage coverage. No GC/retirement/epoch policy is selected here.

The current toolchain emits a generated-protobuf vulnerability warning; it remains visible and is not a failed property or a repaired dependency. Reviewer should examine the coverage ledger and this zero boundary before accepting broader verification claims.
