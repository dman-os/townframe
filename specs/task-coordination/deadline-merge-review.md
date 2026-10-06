# Task-deadline staging publication boundary — bounded review

## Current source mapping

- `rt.rs` resolves the original absolute deadline from durable `PreparedAttempt` by dispatch ID, before claiming finalization. Claim ownership seals a cancellation race, **not expiry**. It passes that deadline to staging merge, maps `ExecutionExpired` to cancellation, and rechecks expiry after merge before success hooks.
- `drawer/mutations.rs` requires an explicit optional deadline. Inside synchronous `with_document`, bounded work first checks expiry, then modifies a private target clone. Immediately before visible replacement it checks again; expired work returns `MergeError::ExecutionExpired` without assigning the clone to the visible target. Non-task callers explicitly use None; the unbounded path still mutates the original document without the bounded copy/check behavior.
- Cache invalidation/notification occurs after successful document-lock mutation. An effect issued while authorized can be persisted/notified later. This is not permission to issue new execution effects after expiry, nor a rollback requirement for a previously authorized effect.

`deadline_merge.qnt` models private computation, deadline crossing, atomic visible issuance, later persistence and root recheck. Deadline10 represents the last authorized second;11 is expired. Both bounded/unbounded paths and already-claimed finalization are exercised. It does not model Automerge bytes, clone aliasing, actual SQLite/network persistence or every branch-merge error; those remain native/library contracts.

## Reproducible gates

From `specs/task-coordination`:

```sh
node runner.mjs deadline
node runner.mjs successor
```

- Final deadline gate exit0:4 named cases;31 generated/23 distinct safety states (`/tmp/quint-deadline-merge-final.log`). Three mutants cause actual TLC safety violations: early-only check, computing into the visible target, treating finalization claim as sealing out expiry. Parser failures do not count.
- Successor policy gate exit0:13 named cases;8 generated/6 distinct states;12 actual assertion-failing controls (`/tmp/quint-successor-policy-change.log`). Changed lifetime renews before the old deadline for the **same** slot/capture, retaining old ID/deadline. Identical duration and unexpired replay reuse the original; expiry still renews. No work-generation or settlement change is modeled.
- Initial deadline model had an effect-expression precedence/type error; it was corrected with explicit parentheses. Initial updated successor runner detected an ambiguous mutation site after reuse gained a lifetime guard; that mutation now targets only the authorization proof guard. Neither initial error was treated as a property counterexample or acceptance.

## Chosen successor policy now implemented

`ProcessorTaskOrigins::originate` reuses only when `now <= not_after` **and** widened stored duration equals the configured lifetime. Otherwise it allocates a new nonce/ID/origin/deadline and retains the predecessor. The model's pre-expiry policy-change branch uses the same slot/capture classes and a fresh authorization. This supersedes the earlier lifetime-only replay objection; it does not invent a future-origin clock bound or resolve prototype-schema migration.

## Trust and proof gaps

1. Document-lock check and replacement are modeled as one atomic issued mutation. Actual wall-clock movement between CPU instructions is not a formal real-time guarantee. The finite profile assumes a nondecreasing clock; Jiff wall time/NTP behavior is not proved here.
2. Every task dispatch must still resolve its original durable attempt mapping. Missing mapping is interpreted as non-task/unbounded by the optional lookup. This review does not establish mapping-retention/GC invariants; future cleanup must preserve that distinction.
3. A predeadline visible effect followed by a late root recheck can end cancelled while the authorized target effect remains. It must not be silently relabeled as an expired issuance or assumed rolled back. The model allows later persistence of that earlier mutation.
4. Root's post-merge recheck is modeled at its check point, not as a theorem about later asynchronous hook instructions. Independent host cancellation, continuations, success/domain-publication fences and full execution expiry remain parent work.
5. This is one merge and a two-generation origin profile, not complete native composition, arbitrary repeated histories, crypto or GC. No product policy was invented.

## Parent evidence inspected, not rerun

`/tmp/parent-expiry-effect-tests.log` shows6/6 passed: origins reopen/concurrency, `test_v2_merge`, branch-access refusal, finalization cancellation race, local native triage and remote native triage. Source inspection confirms `test_v2_merge` passes `Some(0)`, asserts `ExecutionExpired` and unchanged main heads. That pins expired-entry no-effect; crossing the deadline during private computation is modeled, not independently exercised by this Rust assertion. `/tmp/parent-expiry-effect-clippy-v2.log` shows scoped Clippy success. `/tmp/parent-router-full-safety.log` independently reports2,457,600 distinct states/depth19. These are parent evidence, not worker Rust/native runs. Earlier mixed-snapshot native timeout remains unattributed.

No Rust edits, Cargo, delegation or full Rust suites were used. Source/model hashes: `/tmp/quint-deadline-publication-hashes.txt`. Parent review remains required; this is not full execution-expiry or original-scope acceptance.
