# Full-router compiler investigation and bounded safety result

## Resolution

The unchanged full `router_checks.safety` profile now compiles and completes with a larger JVM stack: `-Xss16m`. The original default-stack failure is preserved. This is a supported JVM resource setting, not a compiler/IR/dependency patch, property weakening or source-semantic refactor. No Rust, Cargo, delegation or full Rust suites were used.

`router_machine.qnt` remains SHA256 `7548ffe38c5632edf65824fa4e43b6d4527fc3bede1e62835e24609f750110b2`, identical to the prior retained source hash. Full source/tool/reproducer hashes are in `/tmp/quint-router-resume-hashes.txt`. `router_checks.qnt` retains initial `offered(2)`, event domain1..18 plus stutter0, both stale-session choices, original step and original safety predicate. No event/domain/property was removed.

## Evidence and exact commands

From `specs/task-coordination`:

```sh
# Preserved failure, exit1 and NO checker verdict:
JAVA_TOOL_OPTIONS="-javaagent:$PWD/.verification/quint-json-limit.jar" NODE_OPTIONS="--require=$PWD/tooling/rpc-diagnostic.cjs" timeout 1800 node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main router_checks --invariant safety router_checks.qnt

# Unchanged full target, exit0:
JAVA_TOOL_OPTIONS="-Xss16m -javaagent:$PWD/.verification/quint-json-limit.jar" NODE_OPTIONS="--require=$PWD/tooling/rpc-diagnostic.cjs" timeout 1800 node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main router_checks --invariant safety router_checks.qnt

# Reproducible scoped runner;11 TS cases then original full safety:
node runner.mjs router
```

- Default stack: `/tmp/quint-router-resume-baseline.log`; after SanyParser, `compileToTlaplus` returns Left `{"explanation":"","errors":[]}`. Earlier `/tmp/quint-router-full.log` and minimized first14/first15 artifacts remain unchanged.
- Larger stack direct full target: `/tmp/quint-router-stack16-full.log`;43,120,641 generated /2,457,600 distinct /depth19, queue empty, no violation,196.971s.
- Runner repeat: `/tmp/quint-router-runner-full.log`;11 named cases pass, same43,120,641 generated /2,457,600 distinct /depth19, queue empty, no violation,208.340s. Runner grants this target30minutes; other runner time budgets stay unchanged.

The runner preserves the existing JSON-size-only agent and heap/TLC configuration. It appends the stack setting to inherited `JAVA_TOOL_OPTIONS`; JVM/protobuf warnings remain visible. No installed package or checker was modified.

## Compiler seam, not an overclaimed root cause

The pinned path is Quint0.33.0 `compileToTlaplus` → Apalache0.62.1 RPC `conn.tla`, failing after SanyParser with an empty failure. Larger JVM stack changes that exact full target from translation failure to completed safety verification. This establishes **stack-sensitive translation and a working operational repair**, not an instrumented proof that the thrown exception was StackOverflowError; the server did not expose an exception type/stack trace.

The new `tooling-repro/router-compiler/offer_declined_core.qnt` isolates allocation removal, conditional block creation and correlated block replacement. It compiles at default stack (7 distinct states; `/tmp/quint-router-decline-core.log`). This shows those operations are not intrinsically unsupported, but its narrowed state/transition graph is diagnostic only and never acceptance evidence. A temporary semantics-preserving extraction of the full decline branch into a helper still failed at default stack (`/tmp/quint-router-extracted-decline.log`); generated duplicate diagnostic copies were removed, original reproducer files preserved. No failed experimental refactor remains in the acceptance model.

## Honest limits

- This verifies the original **finite, one-occurrence18-event** safety profile, not arbitrary repeated histories, complete guard coverage or fair router liveness.
- TLC reports probabilistic fingerprint bounds: calculated optimistic5.4E-6; based on actual fingerprints4.4E-7 direct /1.8E-7 runner repeat. Warnings are retained, not suppressed.
- Compiler default-stack failure remains reproducible; future translator versions/source growth may change stack requirements. The larger stack does not establish compiler correctness.
- Larger graph completion does not establish native execution, running-effect expiry, GC, successor-clock bounds, historical lifetime-witness retention or prototype schema migration.

## Parent evidence inspected separately

`/tmp/parent-successor-remote-ack.log` shows5/5 receipt/recovery/native tests passed, including remote triage53.789s. `/tmp/parent-successor-clippy.log` shows scoped all-target/all-feature Clippy exit0. These are parent evidence only; no Cargo/native commands were run here. The earlier mixed-snapshot native timeout remains unattributed. Parent lifetime-only configuration policy choice, future-origin bound and prototype migration remain pending; no policy was invented by this compiler item.
