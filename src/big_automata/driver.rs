//! The driver: runs one [`PeerSync`](crate::sync::peer) per (peer connection, backend) against a
//! real transport, a real backend and a real clock.
//!
//! One driver per backend (operator, 2026-11-24): the old worker routed parts to several
//! backends (`BackendId`), but every embedder ran one worker per backend anyway, and the only
//! reason to merge them was the multi-peer stat machine, which is now a layer above. A peer with
//! two backends has two drivers and two replay sessions.
//!
//! Runtime-agnostic: it spawns and sleeps through `n0_future`, so the same driver runs under
//! tokio and in the browser (`wasm32-unknown-unknown`). No tokio type appears in its API.
//!
//! # Boundaries
//!
//! | Boundary | What it is |
//! | --- | --- |
//! | Transport | [`BigSyncRpcClient`](crate::sync::rpc): summaries, bucket list and leaves, session update, page round, close |
//! | Backend | `SyncBackend`: `record(job) -> Settle { Done, Fetch }`, `fetch(job)` (the cut in the port ledger) |
//! | Peer cursors | a small table the driver owns, keyed by (peer, backend, part); not the part store |
//! | Clock | injected; the machine never reads time itself |
//!
//! # Laws
//!
//! 1. **One loop owns the machine.** Every input (route change, call outcome, timer) reaches
//!    `PeerSync` from one task, one at a time; the machine is never borrowed across an await.
//!    Calls run as their own futures and report back through the loop.
//! 2. **Every command is executed once, and every call's outcome is reported once**: an
//!    answer, `Lost` for a transport failure, or `Cancelled` for a cancelled backend call. A
//!    `Cancel` drops the call's future; the outcome is still reported (as `Cancelled`) so the
//!    machine frees the slot (ObjectJobs law 7).
//! 3. **The driver adds no concurrency and no queues of its own.** Everything in flight is
//!    something the machine issued, so the machine's bounds (ObjectJobs' window and fetch cap,
//!    one round per lane, one summary call) are the driver's bounds.
//! 4. **Timers.** The loop waits for the next input or the machine's `next_due`, whichever is
//!    first, and flushes after every batch of inputs.
//! 5. **Cursor persistence keeps order and may lag.** `PersistCursor` writes to the peer-cursor
//!    table may be batched, but a part's writes land in the order issued and a cursor never
//!    goes back. Losing the latest writes in a crash is safe: the part replays from an older
//!    cursor, and backends are idempotent.
//! 6. **Connections.** Losing the connection disconnects the machine (every call in flight
//!    reported, nothing survives but persisted cursors). A new connection is a fresh
//!    `PeerSync` with a fresh session id, given the current routes as additions, each part with
//!    the cursor read from the table.
//! 7. **Routes in are incremental**, as `PeerSync` law 1: the embedder sends added and removed
//!    parts and objects. The driver keeps the current route set only to re-add it on a new
//!    connection.
//! 8. **Facts out.** `Synced`/`Unsynced` edges are published per (peer, part) on a channel the
//!    layer above reads; a subscriber first reads the current facts, then follows the edges,
//!    so it never waits on an edge that already happened.
//! 9. **Errors.** Transport errors are `Lost` outcomes, backend errors are `Failed` outcomes;
//!    both retry inside the machine. Anything else is a bug and panics.
//!
//! # The layer above: full sync across peers
//!
//! `wait_for_full_sync(pairs)` is the conjunction of the `Synced` facts of the asked
//! (peer, part) pairs, built from law 8. A pair whose peer or part goes away resolves the waiter
//! with that fact instead of stranding it.
//!
//! # Testing
//!
//! The machine's logic is covered by its own laws and the whole-machine simulation. The driver
//! adds: law tests over an in-process transport and a scripted backend (outcomes reported
//! once, cancel reports `Cancelled`, cursor writes ordered, reconnect re-adds routes with their
//! cursors, a subscriber never misses an edge), and an end-to-end run over `big_automata_iroh`
//! on native and in the browser.
