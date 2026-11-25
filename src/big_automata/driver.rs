//! The driver: runs one [`PeerSync`](crate::sync::peer) per (peer connection, backend) against a
//! real transport, a real backend and a real clock.
//!
//! One driver per backend (operator, 2026-11-24): the old worker routed parts to several
//! backends (`BackendId`), but every embedder ran one worker per backend anyway, and the only
//! reason to merge them was the multi-peer stat machine, which is now a layer above. A peer with
//! two backends has two drivers and two replay sessions.
//!
//! Runtime-agnostic: it sleeps through `n0_future` and spawns nothing (see "Runtimes"), so the
//! same driver runs under tokio and in the browser (`wasm32-unknown-unknown`). No tokio type
//! appears in its API.
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
//!    `PeerSync` from one future, the loop [`PeerDriver::start`] returns for the embedder to
//!    spawn, one input at a time; the machine is never borrowed across an await. Calls are
//!    futures polled inside that loop, not tasks, and report back through it.
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
//!    goes back, except by a restart of the part (`PartLifecycle` law 10). Losing the latest writes in a crash is safe: the part replays from an older
//!    cursor, and backends are idempotent.
//! 6. **Connections.** Losing the connection disconnects the machine: every call in flight is
//!    cancelled and, the machine being gone, nothing is reported (ObjectJobs law 10); nothing
//!    survives but persisted cursors, and every part is `Unsynced`. A new connection is a
//!    fresh `PeerSync` with a fresh session id, given the current routes as additions, each
//!    part with the cursor read from the table. The peer keeps the replaced session until its
//!    TTL and caps the sessions a client holds, so the new connection closes the old session,
//!    best effort (the peer also sweeps expired sessions on every update).
//! 7. **Routes in are incremental**, as `PeerSync` law 1: the embedder sends added and removed
//!    parts and objects. The driver keeps the current route set only to re-add it on a new
//!    connection.
//! 8. **Facts out.** `Synced`/`Unsynced` edges are published per (peer, part) on a channel the
//!    layer above reads; a subscriber first reads the current facts, then follows the edges,
//!    so it never waits on an edge that already happened. A part is `Unsynced` when its route
//!    is added and whenever the connection is lost, and `Removed` when its route is removed
//!    or the driver ends.
//! 9. **Errors.** Transport errors are `Lost` outcomes, backend errors are `Failed` outcomes;
//!    both retry inside the machine. Anything else is a bug and panics.
//!
//! # Runtimes
//!
//! [`PeerDriver::start`] spawns nothing: it returns the handle and the loop future, and the
//! embedder spawns that future on its runtime (`tokio::spawn` natively,
//! `wasm_bindgen_futures::spawn_local` in the browser). The same holds for the calls inside the
//! loop and the closes of replaced sessions: they are polled by the loop in sets whose future
//! types are inferred, never boxed as `dyn Future`.
//!
//! The traits the driver is generic over ([`SyncBackend`], [`PeerCursors`],
//! [`BigSyncRpcClient`](crate::sync::rpc::BigSyncRpcClient)) put no `Send` bound on their
//! futures, and the driver puts none on its type parameters. Whether the loop future is `Send`
//! is then auto-trait leakage from the embedder's concrete types, as for
//! `service_client_core_rs`'s `UnaryTransport`: natively the embedder's types are `Send` and
//! its `tokio::spawn` compiles; a browser backend whose futures hold JS handles is not `Send`
//! and is spawned locally. No trait signature depends on the target.
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
//! adds: law tests (`tests`) over an in-process transport and a scripted backend on a paused
//! clock (outcomes reported once, cancel reports `Cancelled`, cursor writes ordered, reconnect
//! re-adds routes with their cursors, a subscriber never misses an edge, waiters resolve or
//! report removal), and an end-to-end run over `big_automata_iroh` natively. A browser run
//! is not part of this crate's tests: see `big_automata_iroh`.

use crate::interlude::*;

mod facts;
#[cfg(any(test, feature = "test-support"))]
pub mod memory;
mod run;
#[cfg(any(test, feature = "test-support"))]
pub mod table_laws;
#[cfg(test)]
mod tests;

pub use crate::sync::object_jobs::ObjJob;
pub use crate::sync::object_jobs::Settle;
pub use facts::Fact;
pub use facts::FactEvent;
pub use facts::FactStream;
pub use facts::Facts;
pub use facts::Removed;
pub use run::DriverConfig;
pub use run::PeerDriver;
pub use run::RouteUpdate;

use crate::sync::PeerCursor;

/// The backend the driver syncs into: the cut of the backend contract in
/// [`object_jobs`](crate::sync::object_jobs). A failure is retried by the machine on its
/// backoff, so an implementation reports it and keeps no retry of its own. Both calls take a
/// job they have seen before (replays repeat after a reconnect).
///
/// A job that keeps failing waits out its backoff outside the receive window (law 7 of
/// `ObjectJobs`), so it does not stop the peer's other objects, but it still owes its cursors:
/// its part's cursor does not advance past it. Cursor liveness is therefore the backend's
/// responsibility. A backend that cannot apply something yet (a local policy that needs
/// membership which has not arrived, say) should accept and park it, answering `Done`; one that
/// fails forever holds that part's cursor forever.
///
/// The futures carry no `Send` bound, like [`BigSyncRpcClient`](crate::sync::rpc::BigSyncRpcClient)'s
/// (ADR 039: no `FutureForm`): the embedder spawns the driver's loop, so the concrete types
/// decide whether it is `Send`. See "Runtimes" in the module docs.
pub trait SyncBackend {
    /// Short and cancel-safe; never waits on bytes. `Done` lets the cursor pass the job,
    /// `Fetch` asks for [`fetch`](Self::fetch) first.
    fn record(&self, job: ObjJob) -> impl Future<Output = Res<Settle>>;

    /// May be long. Cancel-safe: the driver drops the future to cancel it. Idempotent.
    fn fetch(&self, job: ObjJob) -> impl Future<Output = Res<()>>;
}

/// The cursor each (peer, backend, part) has been consumed to, with the peer's generation it
/// is a position in. A small table the driver owns, not the part store: its keys carry the
/// backend.
///
/// An implementation applies the entries of a write in order and **the last write wins**: it
/// does not compare revs. The driver issues a part's writes in order (law 5) and the machine
/// only lowers a cursor by restarting the part (`PartLifecycle` law 10), when the peer's
/// generation changed or a restore went unminted and its log ends below the cursor: that
/// part's next write is a lower rev, maybe in the stored generation, and must replace the
/// stored one. It may coalesce entries of one write for the same part. A failure is not
/// retried: it crashes the driver. [`table_laws`] states this as checks any implementation
/// runs.
pub trait PeerCursors {
    /// The cursor persisted for each part, in the order asked; `None` for a part never
    /// written.
    fn read(
        &self,
        peer: &PeerKey,
        backend: &str,
        parts: &[PartKey],
    ) -> impl Future<Output = Res<Vec<Option<PeerCursor>>>>;

    fn write(
        &self,
        peer: &PeerKey,
        backend: &str,
        writes: &[(PartKey, PeerCursor)],
    ) -> impl Future<Output = Res<()>>;
}
