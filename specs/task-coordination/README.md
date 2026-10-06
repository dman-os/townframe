# Task-coordination verification

The canonical current acceptance reference is [verification-index.md](verification-index.md). It maps source components, finite model claims, historical diagnostics, runtime companions and remaining ADR obligations. Chronological worker reports are supporting evidence, not the current acceptance summary.

Quint verifies specifications, not Rust. Whole-system verification and physical GC remain incomplete. Review source correspondence and assumptions before interpreting any state count.

Runner gates require Linux/util-linux `flock` and serialize across sessions on `/tmp/townframe-quint-apalache-8822-<uid>.lock`. Contention fails immediately with exit75 and an infrastructure message; no waiting or fallback. Direct Quint commands bypass the runner lock and must not run concurrently. See the index for supported modes and prerequisites.

## Run locally

From `specs/task-coordination`:

```sh
npm ci --no-audit --no-fund
npm run prepare:verification
npm run check:register
npm run simulate:register
npm run verify:register
npm run negative:register
node runner.mjs facts
node runner.mjs algebra
```

`prepare:verification` builds the tracked, reflection-only Java agent; no Jackson build dependency is required. It changes only the RPC parser's maximum JSON string length, not the models or checker properties. `handoff`, `lifecycle`, `router`, and worker progress in `reducers` supply this agent themselves. Large modes inheriting Java options can be run explicitly, for example:

```sh
JAVA_TOOL_OPTIONS="-Xss16m -javaagent:$PWD/.verification/quint-json-limit.jar" node runner.mjs continuation-repair
```

Quint is locked to `@informalsystems/quint@0.33.0`. Node >=22 and a full JDK >=21 (including `javac` and `jar`) are required; exercised environment was Node24.20.0/npm11.19.0/OpenJDK21.0.12.1. TLC compilation downloads/caches Apalache0.62.1 through Quint; first cold startup can be slow. npm may warn that protobufjs postinstall was skipped. Java may emit a generated-protobuf security warning: the runner does not hide it. `tlc.json` requests two workers and `-Xmx2G` (Quint expects the complete JVM heap flag, not bare `2G`). Tool output is kept in ignored `.verification/`; dependency and `_apalache-out/` directories are also ignored locally.

The runner uses TypeScript simulation explicitly so the baseline does not require the native Quint evaluator. Simulation seeds are fixed to1; TLC's fingerprint seed is printed in each raw log and is not configurable by this release's runtime JSON. TLC exhaustively checks its finite graph, subject to its usual fingerprint collision probability; state counts, graph depth and exact property matter more than a random seed for completeness.

- `check:register`: parse/typecheck all model files and run named behavior/edge cases, including a partition/crash/restart nonvacuity witness.
- `simulate:register`: run100 finite sampled traces up to30 transitions with full safety. A source-derived violation makes the command fail rather than being reclassified as successful smoke.
- `verify:register`: full safety, algebra, validation, six-input convergence, full signature-conflict convergence, and dedicated zero-boundary safety/algebra/fair convergence. It intentionally stops on the first failed property. Do not weaken it to make a discovered source bug green.
- `facts`: historical diagnostic subset of structural/causal/replay/exact-representation/poison exclusion facts, finite validation classes and six-input convergence. This is **not** the full safety/algebra claim.
- `algebra` / `signature`: run the full signature-variant algebra or convergence property individually.
- `negative:register`: require the unfair liveness counterexample and nine independent temporary mutants (zero/absence confusion, stale sequence, hidden replay-fence deletion, poison witness selection, invalid-batch admission, foreign-record causality, wrapper ranking, exact original identity, and the actual pre-fix header-signature order). The pre-fix mutant must also violate full fair convergence. A parser/tool failure does not count as success. Temporary directories are removed in `finally`; no mutant model files are retained.

Direct full conflict liveness command:

```sh
node_modules/.bin/quint verify --backend tlc --main register_signature_convergence --temporal convergence --tlc-config tlc.json register_convergence.qnt
```

## Findings and limits

The full history/algebra/fair-convergence properties exposed a real **header-signature canonicalization failure** in the initial source snapshot. Lower binding could win Current while discarding the smaller valid header signature needed by a later poison merge. Parent confirmed distinct valid signatures using real Ed25519 signing, retained a failing-before/passing-after Rust regression, and moved header signature first in `canonical_cmp`. This worker reread that source and migrated the model comparator and independent order oracle without excluding signature classes. An executable temporary pre-fix mutant retains the original fault. Exact before/after details and exercised results are in [results](results.md) and [counterexample](counterexample-signature-order.md); Rust command results are parent-reported, not child-executed.

The positive delivery profile includes six finite genuine inputs, overwrite/recovery, historical wrappers, three writers, two replicas, arbitrary reordering/duplicates/delay, partition/heal and snapshot crash/restart. Its liveness premise is eventual stable connectivity/online state plus weak fairness of each replica's outstanding delivery. Those are explicit experimental environment premises, not accepted product guarantees. The signature-conflict profile remains separate and mandatory. Overwritten writes are not required to remain visible heads; the obligation is equality with the final join of the genuine finite set.

Crypto arithmetic, byte framing/serde, actual host durability, native authority, BigSync contracts and physical purge are outside this standalone proof. The descriptor model covers finite validity/budget/order classes only. There is no counter-saturation shortcut, unlimited-input liveness claim, or arbitrary-population theorem. Read the coverage/abstraction ledger before using any passing result as evidence.

## Bounded ticket reducer checks

`node runner.mjs ticket` checks the existing TaskTicket join in isolation, runs finite history safety/algebra and four negative controls. See [ticket audit](task-ticket-results.md) for source correspondence, actual counts, corrected test discovery and explicit optional-zero/summary boundary gaps. This does not complete TaskStore or reducer/runtime composition.

## Composed lifecycle and runtime boundaries

The [verification index](verification-index.md) is the current coverage map, including executable modes, finite bounds, fairness premises and implementation companions. In particular:

- `lifecycle` separates volatile admission, durable SQL preparation, execution, terminal commit, callback delivery and receipt; progress concerns retained terminal replay, not election-to-execution.
- `triage-delivery` composes activation, captured-domain readiness and exact settlement. Runtime Pending delivery retains the outbox and subscribed wake ownership; actual eventual materialization remains a separate test obligation.
- `descendants` checks immutable inherited authorization through child/grandchild admission, restart, managed publication and SQL completion.
- `continuation-repair` checks restored immutable job binding and expiry immediately before host service invocation. Effects inside an already-running service still need their own fences.
- `handoff` composes the actual register join with local selected-owner fencing and publication-before-checkpoint crash cuts. Local transfer is not a global retirement certificate.

Historical receipt, admission-queue and continuation counterexamples remain available, but must not be described as failures of the repaired current implementation. The index explicitly distinguishes them from current proof profiles. Physical GC, arbitrary-population verification and whole-system liveness remain unimplemented or unproved.
