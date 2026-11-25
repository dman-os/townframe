# wflow as the job runner for walkers (scratch, 2026-11-25)

Status: direction from the operator, not yet designed. Source research:
the outbox research report (big_automata lane scratchpad), section 4, reading PR #117
(`feat/native-wflow`) and the wflow crates on that branch.

## The direction

Walkers (the durable outbox, and later the big_repo/daybook workers built on okv) keep owning
*what* to run: their stream, supersession, priority, concurrency caps, providers, memory bounds.
Instead of launching tokio tasks for the work itself, they launch wflow jobs. The reason: the old
workers (`daybook_core/blobs/*`, `big_blobs` transfer and admission) do several things per item
that must each be crash safe, with recovery and cancellation cleanup, and every worker rebuilt a
small job system for that. wflow is the job system; the walkers should not be one again.

Priority and a concurrency cap can be added to wflow, but they are orthogonal to the walkers'
own caps and priorities: the walker decides which items are due and how many run for its domain;
wflow bounds and orders what runs on the node as a whole.

## What wflow is (PR #117)

- A single-writer partition commit authority over a journal (`wflow_core`, pure, with a Quint
  model), a tokio runtime around it (`wflow_tokio`), a SQLite-backed kv store (`wflow`), and two
  session executors: Wasm components (`wflow_wdk_wasm`) and, new in #117, native Rust async fns
  (`wflow_wdk_native`, registered with `LocalNativeHost::register`).
- Positional durable steps: `effect` commits a closure's result, `sleep`/`recv` are durable waits;
  a replay re-runs the workflow and returns recorded step values.
- Retries: fixed or capped-exponential policy (100 ms to 30 s by default) with a budget (8 retries,
  3 restarts); exhaustion settles as `RetriesExhausted`.
- Submit is idempotent by key until the receipt is pruned after `retention_ms`.
- Cancellation is durable and abort-oriented: a fault receipt, the lease fenced, the session
  dropped, deadlines cleared, late results ignored.
- Throughput reported on the in-memory store: about 33k / 26k / 12k jobs/s with 0 / 1 / 8 effects.

## Failings found (to fix before walkers can rely on it)

1. **Not portable.** `wflow_tokio` uses `tokio::spawn` and `tokio::time` throughout
   (`authority_runtime.rs:499,522,861-893,1348`); the journal sits on `sqlite_rs`, which has no
   browser build. `wflow_core` is plausibly portable but declares no `wasm32-unknown-unknown`
   build. Everything on the service stack is headed for the browser, so the runtime needs
   `n0_future` and a store abstraction a browser store can implement.
2. **A second durable store.** A walker's item (an okv row in the backend's database) and its job
   (the wflow journal, a separate SQLite) cannot share a transaction, so "item admitted" and "job
   submitted" are two steps and "pending" has two sources of truth. Idempotent submit makes the
   reconcile at-least-once and cheap, but the journal on the same store (okv) as the walker's
   items would remove it.
3. **Cost per job.** At least submit, grant, one commit per effect, complete: 3-4 journal commits
   through one writer actor. Fine for multi-step work; heavy for a one-call fetch.
4. **No priority, no concurrency cap.** Every submitted job is granted (nothing in the authority
   runtime for either).
5. **Cancellation does not wait.** The cancelled session is dropped, not awaited, so the
   "stopped before replaced" rule the walkers need (a superseding job must not run beside the old
   one) is not provided; a non-cancel-safe step can still be running when its replacement starts.
6. **No cooperative cleanup.** Abort-oriented cancel has no compensation or cleanup step, which is
   exactly what the old blobs workers need (undo a half-done multi-step item).
7. **Superseding needs a new key.** A settled key stays reserved until its receipt is pruned, so a
   superseded item must submit under a versioned key (`object:version`).
8. **Durable backoff.** Backoff deadlines are wall-clock durable; the outbox draft wanted
   "due now after a restart". Arguably wflow's is better; decide once.
9. **Open item in its own ledger:** the authority actor panicking on unexpected session loss was
   still unchecked.

## Related design note: no materialization wakes

The old designs relied on ad-hoc "materialization wake" signals (a document materialized, so wake
a worker). That couples producers to consumers and races (subscribe vs wake, see
`daybook_core/index/facet_set.rs:938-964`, `daybook_core/blobs/pin_worker.rs:1286-1333`). Walkers
should follow rev streams only: whatever they depend on is a row in a store they read, and a new
row is the wake.

## Open questions

- Should wflow's journal live on okv (one store, one transaction with the walker's items), or stay
  its own SQLite with an at-least-once reconcile?
- Where do wflow's node-wide priority and cap sit relative to a walker's per-domain ones?
- Cooperative cancellation: a cleanup step per workflow, or compensation records in the journal?
