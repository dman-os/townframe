---
name: quint-verification
description: Research, model, and maintain Rust protocol verification with Quint; use for reducer correspondence, async/durable composition, safety, liveness, counterexamples, and model drift.
---

# Maintainable Quint verification

Quint verifies the specification, not the Rust implementation. Keep an explicit, reviewed correspondence and do not describe bounded exploration as an unbounded implementation proof.

## Start with the contract

1. Read the Rust reducer, its state/event/action types, callers that execute effects, durable storage boundaries, recovery path, and relevant ADRs. Separate implemented behavior from intended policy. Record moving source revisions or snapshots.
2. Before code, agree scope, module boundaries, finite domains, environment assumptions, properties, and a transition/branch coverage ledger. For this repository, see [townframe proposal](references/townframe-coverage.md). Research-only assignments stop at this gate.
3. Give each model one source owner. Use plain records, variants, sets/maps, pure join functions and named actions. Prefer a small composed model plus independently checkable component models. Do not build a speculative generator, generic protocol framework, or duplicate runtime implementation.

## Translate Rust faithfully

- Map durable state, actor-local state, messages, in-flight effects, and observed outcomes separately. An emitted action is not its completed effect. Every delayed completion carries its actual request/session/allocation/version/generation identity; expose stale delivery and repeated delivery.
- Map every event and action variant, relevant guard branch, merge operation, restore path, error, cancellation, and authority transition. Document deliberate abstractions, rather than silently dropping inconvenient branches.
- Split await/transaction/commit/publication/acknowledgement/checkpoint boundaries into separate transitions. Add crash cuts between them. Recovery may retain durable evidence but loses volatile queues and tokens unless actual code persists them.
- Treat signatures, content hashes, encryption, SQL atomicity and native authorization as stated contracts; distinguish valid originals, wrappers, decryptability, Read/Edit/Relay and selected-binding freshness. Never make decryptability a merge-order oracle unless Rust does.
- Fresh identifiers must not recycle while old completions exist. Bounded counters must not wrap; record that saturation excludes further transitions and can introduce artificial deadlocks. Use multiple finite profiles, not one convenient example.

## Check independent properties

Safety checks include authorization at the actual admission point, exact input/receipt identity, no stale completion side effect, persistence-before-start, no checkpoint ahead of incorporated durable effects, join convergence/replay fences, and capacity retention while finishing. Compute expected outcomes from independent ghost histories or semantic relations, not by reusing the transition's guard as the property.

Liveness requires temporal checking. Simulation witnesses and bounded safety checking do not establish eventual progress. State a conditional contract: stable authority and inputs, eventual materialization/delivery, fair processing of each outstanding effect, progressing clocks, and a terminating execution when that is required. Weak fairness concerns continuously enabled actions; strong fairness concerns repeatedly enabled actions. Neither supplies a missing permission, job outcome, or network guarantee. Do not put blanket fairness on a nondeterministic `step` and claim every job progresses.

Use TLC for finite-state temporal/fairness checking with the pinned supported toolchain. Use Apalache for bounded symbolic safety and, where justified, explicitly constructed inductive invariants. Current Quint warns that Apalache temporal checking is experimental and may produce incorrect results. See [tooling and evidence](references/tooling-and-evidence.md) for the verified release and important documentation discrepancy.

## Make counterexamples useful

1. Save the exact tool/version/profile/seed/property and ITF trace where supported.
2. Translate each model action into a Rust event/effect/completion schedule using the correspondence ledger. Check whether the trace is real, an abstraction artifact, or an invalid environment assumption.
3. Reproduce consumer-visible behavior in a targeted Rust test or scenario; keep the regression when it catches a plausible bug. Repair the implementation or specification only after identifying which contract failed.
4. Deliberately break a relevant transition (e.g. admit before persistence, accept a stale token, checkpoint before commit) in a temporary model copy. The intended property must fail. Removing fairness should expose the corresponding starvation trace. Do not retain mutants or assert source text.
5. Record exercised results, state counts/bounds, counterexamples and limitations. Passing a parser or a vacuous implication is not protocol evidence.

## Prevent model drift

Maintain a ledger containing source symbol, event/operation and guard cases, modeled action, independent property, finite profile, negative control, trace/Rust correspondence, and status. Source and spec changes must trigger review together. Initially use a manually reviewed mapping; adopt a trace runner only after proving useful coverage. Quint Connect is optional, not proof by itself. A real bridge must compare Rust outputs and semantic state at each step, not echo model inputs or compare a mapper with itself.

Do not label a component verified while relevant ledger rows remain abstracted, unchecked, or blocked. Keep dependency contracts and unimplemented policies visibly separate from checked standalone models. Research evidence and proposal approval precede model implementation.
