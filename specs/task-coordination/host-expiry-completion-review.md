# Host expiry ownership and durable completion ordering — bounded review

## Source mapping

`dispatch.rs` inserts immutable attempt proof and pending expiry row with initial native dispatch in one transaction. Backfill selects retained Active/Waiting declarations, uses their original deadlines and leaves existing cancellation-request flags intact. Due reads use the partial `(deadline,dispatch_id)` index, not a periodic scan of retained history. The expiry FK targets retained attempt evidence.

`rt.rs` holds mapped jobs during boot and skips expired workload restoration. It starts its independent watcher before authorization/reconciliation releases execution. The watcher subscribes before initial index read, samples clocks only while a cached pending owner exists, rereads on dispatch hints/Dropped and drains due rows through `fail_local` before marking the request. Stop cancellation and join are wired. No pool observer is needed for already-discovered pending expiry ownership.

The index flag means the expiry request was processed, not universally that an abort committed: `fail_local` can return because a finalization already owns publication. The positive cancellation model excludes that path and assumes reconciliation/termination eventually returns. Original attempt proof is retained after terminal completion.

`complete` validates attempt identity, acquires SQLite write ownership, fences requested success using `unixepoch('now')` in conditional UPDATE, reads back the actual record, removes expiry ownership in the transaction and returns that actual record. Root uses its status for hooks, replies, progress and waiting-dependency release **after** durable completion. Model write/commit is atomic; the relevant clock is UPDATE issuance, not a theorem about later transaction/fsync/notification timestamps. Prior guarded document issuance remains independently necessary.

## Exact executable scope

From `specs/task-coordination`:

```sh
node runner.mjs host
```

Final exit0 (`/tmp/quint-host-expiry-completion-final.log`):

- One requested-success-versus-durable-winner named test.
- `host_expiry`:8 safety states and fair conditional termination. One already-due, boot-discovered owner, no pool observer, one crash cut before/after cancellation/mark/complete; proof retention is an explicit source contract. Cached deadline survives dropped/coalesced hints through boot/index reread.
- `completion_order`:12 safety states, requested success with early/late SQL issuance, atomic queue removal/proof retention, returned record, callbacks and crash **after** completion before response/callback.
- Six mutations produce real property violations: require observer, mark before control, callback before commit, callback based on requested success, omit SQL expiry fence, discard attempt proof.
- Three diagnostic obligations fail with actual counterexamples, deliberately retained: callback delivery after terminal crash, silent admission-hint loss with cached absence, and alternate Waiting-failure queue cleanup.

Initial model typechecking caught a missing parenthesized effect expression; it was fixed before accepting any result. No parser/type errors are treated as mutations. This is not arbitrary concurrency, multi-owner batching, clock adjustment, host interruption or physical durability verification.

## Historical admission/Waiting issues and still-unproved delivery obligation

1. **Alternate terminal cleanup seam:** `set_waiting_failed` persists Failed and clears cancellation marks but does not delete the expiry row. `release_waiting_dispatches` invokes it on dependency failure. If a mapped Waiting task reaches this path, the terminal dispatch retains its expiry row; later `fail_local` sees terminal and returns, and marking requested still retains the row/FK. `waiting_failure_owner_gap` exposes the row-cleanup obligation. No runtime fixture for this path was run here; universal terminal-row cleanup cannot be claimed from `complete` alone.
2. **Silent admission-hint cut:** initial attempt insertion commits before awaiting the in-memory state lock and notifying. If that caller disappears in this gap while watcher cached next=None, no pending-owner timer is enabled. Without another hint/restart, expiry is not discovered. This diagnostic assumes a silently lost post-commit hint; actual caller cancellation reachability/lifecycle protection needs parent validation. It is not the same as a delivered Dropped marker, which does trigger reread. No DB scan/outbox policy was invented.
3. **Terminal callback crash cut:** crash after completed status/queue removal but before hooks/direct reply retains the winner but can lose delivery. Boot `list_unsettled` excludes terminal records; this source seam does not establish hook/direct-reply replay. The diagnostic concerns those callbacks, not waiting-dependency release, which has separate restart reconciliation. Safe ordering is proven in the finite model; eventual or exactly-once callback delivery is not.

## Continuation/descendant limits

The command-invocation path resolves an active parent and its ACL/staging branch, then calls ordinary `dispatch_no_gate_internal`. It does not visibly pass the parent's original task deadline into descendant admission. Parent staging effects can still be guarded at final publication, but this review does not establish inherited cancellation/deadlines for every descendant, continuation or external host action. Later hooks can execute after an authorized SQL issuance; this model does not make that a new universal execution authorization. Independent continuation/success/domain fences remain parent work.

The follow-on source sets processor `not_before_secs=Some(origin)` and requires equality in the adapter: future-dated work waits rather than starting before claimed creation. This does not prove a maximum future-origin offset, collector/admission skew bound or complete expiry/replan notification. Durable domain expiry/replan notification and GC remain open.

## Parent evidence inspected only

- `/tmp/parent-host-expiry-tests.log`: duplicate-dispatch fixture failure (`dispatch already exists: cmdinvoke-cancel-race`) retained; parent fixed initial atomic PreparedAttempt persistence rather than retroactive mapping.
- `/tmp/parent-host-expiry-tests-v2.log`:4/4 pass: SQL expiry/reopen/queue winner test, no-pool-observer late-effect refusal, two finalization/cancellation races.
- `/tmp/parent-host-expiry-clippy.log`: earlier scoped Clippy exit0; post-fixture/native gate was parent-owned/in progress, not claimed here.

No Rust edits, Cargo, delegation or full Rust suites were used. Hashes: `/tmp/quint-host-expiry-completion-hashes.txt`. Parent review remains required. No full execution-expiry, callback-delivery, lifecycle or GC acceptance is implied.


## Admission/Waiting repair follow-on

The first two ownership findings above are historical: repository-owned admission publication now survives caller abandonment, due reads share its transition fence, and Waiting failure atomically deletes expiry ownership with a current-status guard. See `admission-ownership-repair-review.md` and `node runner.mjs admission` (26 admission states,10 cleanup states,6 controls). Original diagnostic counterexamples remain preserved; terminal callback replay is still unfixed. Separate parent actor timeout evidence does not establish full native acceptance here.
