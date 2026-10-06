# Tooling and research evidence

Research-stage snapshot: 2026-10-10. The evidence below concerns adoption research and official samples, not Townframe Rust/spec verification. Subsequent approved register modeling and its separate actual results are recorded in `specs/task-coordination/`.

## Reproducible baseline

Use `@informalsystems/quint@0.33.0`, not floating `latest`. The Quint repository is now `quint-co/quint`; older `informalsystems/quint` links redirect. Release tag v0.33.0 resolves to commit `d7ef819575c1ebcad7ba11018ce6c64377cfbe3c` (annotated tag object `f8d5276513701b8bd782715d450c10eea1e0afd8`). CLI help defaults to Apalache 0.62.1 and exposes `--backend apalache|tlc`, `--temporal`, `--inductive-invariant`, `--out-itf`, and `--tlc-config`.

An isolated installation, requiring no repository dependency changes:

```sh
npm install --prefix /tmp/townframe-quint-research/cli --no-audit --no-fund @informalsystems/quint@0.33.0
/tmp/townframe-quint-research/cli/node_modules/.bin/quint --version
```

Observed runtime: Node v24.20.0, npm 11.19.0, OpenJDK 21.0.12.1. Installation succeeded (79 packages), warning that protobufjs postinstall was skipped by npm policy. Native standalone release download timed out; it was not executed or checksum-verified. The npm CLI was executed successfully. TLC requires Java and the Apalache distribution to compile Quint into TLA+; it is not a dependency-free alternate backend. First verification downloaded Apalache and timed out during startup; cached retry succeeded. The tool wrote its cache under `/home/asdf/.quint` and research output under `/tmp/townframe-quint-research`, not the repository. Java emitted a generated-protobuf security warning; do not suppress it and call that a fix.

## Executed smoke and negative controls

All inputs below are existing official files from the pinned release archive. No Townframe `.qnt` file was created during this research stage.

Let `Q=/tmp/townframe-quint-research/cli/node_modules/.bin/quint` and `R=/tmp/townframe-quint-research/quint-0.33.0` (abbreviations for the actual commands).

| Command | Observed result | Interpretation |
|---|---|---|
| `Q --version` | `0.33.0` | Installed version exercised. |
| `Q verify --help` | TLC and temporal flags listed; Apalache default 0.62.1 | Current supported CLI surface. |
| `Q run --backend typescript --invariant inv --max-steps 3 --max-samples 1 --seed 1 R/quint/testFixture/apalache/temporalTest.qnt` | Exit 0, states n=1,2,3,4; no violation | One simulated trace only, not symbolic verification. |
| Same run with `--max-steps 4` | Exit 1, n=5 violates `n < 5` | Executable negative control demonstrates bound sensitivity. |
| `Q verify --backend tlc --temporal notDoneLeadsToDone R/examples/language-features/weakFairness.qnt` | Cached retry exit 0; 4 states generated, 2 distinct, complete graph depth 2, no temporal violation | Actual finite-state temporal check with weak fairness. First attempt timed out at 180 seconds; retry completed in 10.52 seconds. |
| Same TLC command with `--temporal notDoneLeadsToDoneNoFairness` | Exit 1; `done = FALSE` followed by infinite stuttering | Fairness-free liveness fails as expected; 15.46 seconds. |

During this research stage, Apalache bounded symbolic verification, inductive verification, Townframe model mutations, and Rust trace replay were not exercised. Subsequent register mutations/TLC results are separately documented beside that model. The official simulation comparison must not be relabeled as Apalache bounded proof. The TLC sample proves availability of the temporal path, not applicability to every proposed model expression.

## Sources and discrepancies

Pinned Quint source base: https://github.com/quint-co/quint/tree/d7ef819575c1ebcad7ba11018ce6c64377cfbe3c

Read the following files in that source archive:

- `docs/content/docs/checking-properties.mdx`: simulation, bounded checking, inductive checking and limitations.
- `docs/content/docs/lang.md`, `docs/content/docs/builtin.md`: modes, state/actions, tests, temporal operators.
- `quint/src/verify.ts`, `quint/src/cliCommands.ts`, `quint/integration-tests/verification.md`: actual TLC implementation and fairness tests. Apalache temporal checking is explicitly warned experimental and possibly incorrect.
- `docs/content/docs/model-checkers.mdx`: contains a stale statement that TLC is not integrated. Current CLI, implementation, integration fixtures and executed TLC runs contradict that statement. Prefer version-pinned implementation plus observed behavior over that page.
- `docs/content/docs/model-based-testing.mdx`: explicitly marks its SimpleBank demonstrations unmaintained. Do not cite them as mature Rust production integration.
- `docs/content/posts/quint_connect.mdx`, `docs/content/posts/quint_connect_emerald.mdx`: current bridge approach, manual state/action correspondence, and concrete Emerald integration.

Official Apalache explanation: https://apalache-mc.org/docs/apalache/index.html . Fixed finite parameters and bounded traces are not an unbounded-state proof. Old known-issues pages refer to older versions; do not transfer those limitations wholesale to 0.62.1.

## Real Rust integrations, not toy endorsement

**Malachite**: https://github.com/circlefin/malachite/tree/f20bebd9c8a7c6a395605bfaa97f7d3a536e3bd3 . Read `.github/workflows/mbt.yml`, `specs/consensus/quint/coverage.md`, and `code/crates/test/mbt/src/consensus.rs`. Real Rust consensus state/input/output mapping, a modular specification tree, algorithm-line coverage including explicit gaps, and CI reacting to both source and spec paths. Its CI pins Quint 0.27.0; that is evidence of reproducible maintenance, not a recommendation to downgrade our evaluated baseline. A model and ITF types do not establish every driver path covered.

**Emerald**: https://github.com/informalsystems/emerald/tree/ea9098f30829d87850714dfe7c6fbf69c8f9e857 . The actual model-based-test adoption commit (#201) adds a Rust harness with real application/Reth interactions and mocked consensus; it does not verify the entire consensus implementation. Initial CI is nonblocking (`continue-on-error`) and uses an unpinned global Quint install. This is concrete integration evidence but not a claim of mature gating or long-term tool stability. The official post also discusses correcting a specification assumption after implementation feedback.

**Quint Connect**: https://github.com/quint-co/quint-connect/tree/4f018f54fc7dd4cef341d10111427bab59d3b307 , release v0.1.2. Read `connect/README.md`: Rust `State::from_driver`, `Driver::step`, named actions, trace macros and tagged ITF sums. This reduces trace plumbing, not the need to independently review state extraction, outputs, abstractions and drift. Do not add it as a Townframe dependency before design approval and a useful small bridge experiment.

## Downloaded reference artifacts

Archives and extracted repositories remain outside the workspace at `/tmp/townframe-quint-research` for continuation. SHA-256 values observed with `sha256sum`:

- `quint-v0.33.0.tar.gz`: `6c8523e07843de139bffd37574b1b92f2ffb6d8b438749846ce5a28a8a4abb33`
- `malachite-f20bebd9.tar.gz`: `5be354d48efd0c18d0f042920279c09e59bb4df055017eddccc3045f4c0d9555`
- `emerald-ea9098f3.tar.gz`: `6a7fa63afc3474dc7b66cb49b5c2d61f5efbe5ce070b5cf6ccec52e623e74bc6`
- `quint-connect-4f018f54.tar.gz`: `c504b6654be268122fa50d3b10d75bd6c6d9105d028892a81047c2eef8d3647d`
