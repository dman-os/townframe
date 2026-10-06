# Ordered executor registration normalization

Supersedes the duplicate-task registration gap in reducer-guard-correspondence.md. No production policy change: Rust `RouterMachine::on_event(RegisterExecutor)` collects incoming active_attempts Vec into a task-keyed map; later duplicate task replaces earlier attempt/allocation. Source router.rs SHA256 remains `5ef5dac83ed11f02ed48447e1e0f7659ab56843d6caea48ea34fc555907235d4`.

Quint now separates `Registration.active: List[Active]` (incoming ordered message) from `Registered.active: Set[Active]` (normalized retained map values). `normalizeActive` folds left, excludes any retained entry with matching task, then inserts the complete incoming entry. Both attempt and optional allocation identity are replaced; normalization happens before capacity/active comparison and Unavailable-block clearing. No Router event/action kinds, domains, original safety property or exploration bounds were removed. Existing empty and unique-task registration fixtures migrated to lists; retained state fixtures remain sets.

New executable witnesses:
- guardOrderedDuplicateRegistrationTest: duplicate T1 with conflicting attempt/allocation, both list orders, last value retained, owed1, stale Running P1 cannot overwrite retained P2.
- guardCapacityZeroAndAllocationDedupTest: capacity0 prevents offer; explicit matching allocation deduplicates even different attempt, allocation=None falls back to exact accepted attempt, different fallback attempt counts separately.
- guardAllocationTieOrderingTest: same node/incarnation sessions select lower session ID.

New first-duplicate-wins mutant must fail QNT508. Existing six controls remain enabled; no weakening of properties for acceptance. Seven semantic conformance vectors now include ordered duplicate registration and conflicting Running report for parent Rust bridge.

## Executed commands

From specs/task-coordination:

`node runner.mjs guards`: exit0,13 Router +2 TaskTicket witnesses, all seven mutants detected. Log `/tmp/quint-ordered-guards-final.log`.

`JAVA_TOOL_OPTIONS="-Xss16m -javaagent:$PWD/.verification/quint-json-limit.jar" node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main router_checks --invariant safety router_checks.qnt`: exit0;43,120,641 generated /2,457,600 distinct /depth19 /237963ms. Log `/tmp/quint-ordered-router-safety.log`. Original safety initializer/event profile preserved. Its registration payload is the existing empty active list: new ordered duplicate branches are independently witnessed/mutation-tested, NOT exhaustively explored by that original safety profile. No whole-system theorem inferred.

`node --check specs/task-coordination/runner.mjs` and `python3 -m json.tool specs/task-coordination/reducer-conformance-vectors.json`: passed.

Initial translation attempt `/tmp/quint-ordered-guards.log` failed because Quint record spread cannot change field type from List to Set. Fixed by constructing normalized retained record explicitly and keeping event fixtures list-typed; `/tmp/quint-ordered-guards-v2.log` then passed prior controls. No runtime bug inferred. Historical failures preserved.

Model router_machine.qnt SHA256 `3a81b67b9773ee21171801e2ee01994b5e59779208aaae5f635042050442bcae`.

## Remaining scope

Normalization discrepancy is closed for this semantic seam; Rust bridge still not executed. Allocation PreferOrigin/node/incarnation tie ordering, arbitrary list lengths and exhaustive duplicate-registration event profiles remain future coverage. Publisher vector canonicalization correspondence remains open. Lifecycle model untouched; parent independently reran it successfully in `/tmp/parent-lifecycle-composition-gate.log` (reported parent evidence, not worker execution). Parent descendant expiry tests and Clippy reportedly pass, but active runtime descendant/continuation and host late-success regression remain excluded, not claimed verified. No Rust/Cargo/Git/delegation/full suite work.
