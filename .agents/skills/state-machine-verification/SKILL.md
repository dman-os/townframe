---
name: state-machine-verification
description: How to test and verify a sans-IO state machine, store or protocol in this repo to the standard big_automata set - laws with a naive oracle, differential proptests, a deterministic simulation over a faulty network, Quint+TLC models, full-graph conformance between model and code, and mutation checks. Use when writing or porting a machine, a store contract, or a node-to-node protocol, or when reviewing whether its tests are strong enough.
---

# State-machine verification

The rule: **a test suite earns trust only by catching a bug planted on purpose.** Everything
below is a way to make the tests strong enough to catch one, and cheap enough to keep running.
The worked examples are all in `src/big_automata`.

## 1. State the laws first, in the module docs

Write the contract as numbered laws in the module's docs, before or alongside the code: what
always holds, what the caller may rely on, what is allowed but not required. Examples:
`replay.rs` (14 laws), the rev-stream laws (L1–L9), the part store laws. A law that can't be
checked is a wish. Each law gets at least one named test (`law_7_...`).

## 2. A naive oracle and a differential proptest

Write the dumbest possible model that answers the same reads, with no indexes and no
incremental state (`part_store/oracle.rs`, `okv/oracle.rs`). Then:
- a `Harness::check` that compares the whole observable state of the real thing against the
  oracle after **every** operation, not just at the end;
- a proptest generating op sequences (writes, grants, revokes, reads with random windows and
  limits) and comparing every read exactly.

Make the generators small-domain (3–4 keys, 2–3 parts) so collisions and edge cases happen
constantly.

## 3. A deterministic simulation for interacting machines

For a protocol, run both machines and a network in one test (`replay/tests.rs`):
- the network drops, duplicates, reorders and delays messages, and the server forgets
  (TTL, restart);
- the environment churns (wanted sets, readability flips, writes);
- invariants are checked after every step, and convergence after a quiet drain.

**The quiet-drain check is where the bugs hide in the harness itself.** Three harness bugs we
hit:
- stale answers still in the network claiming "drained";
- an update acknowledged later in the same drain;
- pages computed before the last commit.

Decide "quiet" against the *current* state (current acked set, current head), never against a
flag set by some earlier message. When the simulation finds a real protocol bug, the fix
goes in the laws first, then the code, and the shrunk input becomes a named regression test.

## 4. Quint + TLC for the protocol

Model the interacting machines in Quint with small bounds (`replay/spec/session.qnt`), and
check them exhaustively with `quint verify --backend=tlc` (TLC ships inside the dev shell's
apalache jar). Run on mothership via `ms-run <lane>`.
- Safety: state invariants with ghost variables for history.
- Liveness: one temporal property per run, under explicit weak fairness. When liveness fails,
  first ask whether the *model* lacks a fairness the real system has (a TTL sweep), or lets
  the network replay one message forever. Fix the model, not the property, unless the trace
  is a real protocol hole; then amend the law.
- Keep a `README.md` beside the spec with commands, the configurations, and results with
  state counts.

## 5. Full-graph conformance between model and code (the flotilla method)

For each small machine, write a model of **that machine alone** with its inputs as free
events (`replay/spec/server.qnt`). TLC enumerates the whole reachable graph; a script prints
every edge as (state, event, answer, next state) into a checked-in fixture
(`server.py regen`; `-dump dot` is not enough, since it merges edges and drops events). Then
a Rust test (`server/tests/conformance.rs`):
- drives the **real** machine into each source state through its public API, applies each
  event, and asserts answer and next state;
- runs a BFS over the Rust machine under the same alphabet and asserts it reaches exactly
  the graph.

Per-edge checks are stronger than reachability: a planted bug can stay inside the graph and
be caught only by the per-edge check. Use quint-connect trace replay only for models too big
to enumerate.

## 6. Mutation checks, every time

Before calling tests done, plant two or three bugs in the code under test (flip a `<=`, drop
a branch, skip an access check), run the relevant tests, see each one fail, then revert. A
mutation nothing catches is a missing law or a weak oracle. Fix the tests, not the mutation.
Report the mutations and results with the change.

## 7. Practical rules

- Tests run on mothership (`ms-run`), never the laptop. One heavy run at a time.
- Inline test modules stay under 300 lines; larger suites go in `foo/tests.rs`.
- Proptest default case counts are for CI. When touching a simulation, also run it once at
  `PROPTEST_CASES=4000`.
- Every failure a run surfaces is pinned the same turn: a regression test from the shrunk
  input, or an instrumented repro. "Flaky" is a finding, not an explanation; e.g. a test near
  the nextest timeout gets split, not retried.
