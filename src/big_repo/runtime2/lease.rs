//! Leases for runtime2.
//!
//! Leases keep a doc-worker alive while there's interest (a live handle) or
//! in-flight work (a sync/commit). On drop they message the hub, which
//! decrements counters and schedules eviction. Same shape as today's
//! `RuntimeDocLease` / `DocWorkerInternalLease` in `runtime.rs`, lifted to
//! runtime2 types.
//!
//! # Eviction model
//!
//! [`DocWorkerEntry`] tracks two refcounts:
//! - `local_handles`: live [`BigDocHandle`](crate::BigDocHandle) references
//! - `internal_leases`: in-flight operations (commit, sync, apply-session)
//!
//! When both reach zero, the janitor sets an `eviction_deadline`. If no new
//! lease arrives before the deadline, the doc-worker is evicted (stopped and
//! reclaimed). This matches the old runtime's
//! [`DocWorkerEntry`](crate::runtime::DocWorkerEntry) refcount model.
//!
//! [^old-refcount]: `runtime.rs:2130` — the old `DocWorkerEntry` has the same
//!   `local_handles`, `internal_leases`, `eviction_deadline` fields.

use crate::interlude::*;
use crate::runtime2::messages::DocWorkerMsg;
use crate::runtime2::messages::Runtime2Cmd;
use crate::DocumentId;

/// RAII lease held by a `BigDocHandle` (the public doc handle).
///
/// On drop, signals the hub to decrement `local_handles` for the doc-worker,
/// which may schedule eviction if both refcounts reach zero.
///
/// Mirrors `RuntimeDocLease` in `runtime.rs:141` — the old runtime's
/// handle lease that fires `release_doc_lease` on drop.
pub struct DocLease {
    pub(crate) release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DocLease {
    /// Create a new lease. The `release` sender is consumed on drop to signal
    /// the hub.
    pub(crate) fn new(release: tokio::sync::oneshot::Sender<()>) -> Self {
        Self { release: Some(release) }
    }
}

impl Drop for DocLease {
    fn drop(&mut self) {
        // Fire-and-forget; the hub treats a dropped sender as "handle gone".
        let _ = self.release.take();
    }
}

/// RAII lease held by in-flight doc-worker operations.
///
/// While any `DocWorkerInternalLease` is held, the hub will NOT evict
/// the doc-worker (the `internal_leases` refcount in
/// [`DocWorkerEntry`] is > 0). Bundled into messages sent to the doc-worker
/// so the lease lives for the duration of the operation.
///
/// Mirrors the old runtime's `DocWorkerInternalLease` (`runtime.rs:~202`)
/// which is bundled into `CommitDelta`, `SyncWithPeer`, and
/// `ApplySyncSession` messages.
pub struct DocWorkerInternalLease {
    pub(crate) doc_id: DocumentId,
    pub(crate) release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DocWorkerInternalLease {
    /// Create a new internal lease. The `release` sender fires on drop.
    pub(crate) fn new(doc_id: DocumentId, release: tokio::sync::oneshot::Sender<()>) -> Self {
        Self { doc_id, release: Some(release) }
    }
}

impl Drop for DocWorkerInternalLease {
    fn drop(&mut self) {
        let _ = self.release.take();
    }
}

/// The hub's handle to a doc-worker: just the mailbox sender.
///
/// Analogous to the old runtime's `DocWorkerHandle` (`runtime.rs:2102`)
/// which wraps the same `UnboundedSender<DocWorkerMsg>`.
pub struct DocWorkerHandle {
    pub(crate) msg_tx: tokio::sync::mpsc::UnboundedSender<DocWorkerMsg>,
}

impl DocWorkerHandle {
    /// Send a message to the doc-worker. Errors only if the worker is gone
    /// (treated as a `FatalWorkerError` by the hub).
    pub fn send(&self, msg: DocWorkerMsg) -> eyre::Result<()> {
        self.msg_tx.send(msg).map_err(|_| ferr!("doc worker closed"))
    }
}

/// Cooperative cancellation token for a doc-worker.
///
/// Mirrors the old runtime's `DocWorkerStopToken` (`runtime.rs:2100`)
/// which wraps the same `tokio_util::sync::CancellationToken`.
pub struct DocWorkerStopToken {
    pub(crate) cancel: tokio_util::sync::CancellationToken,
}

impl DocWorkerStopToken {
    /// Signal the doc-worker to stop. The worker checks this token at
    /// yield points and exits gracefully.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// The hub's bookkeeping for one doc-worker.
///
/// Matches the old `DocWorkerEntry` at `runtime.rs:2130` field-for-field:
/// - `handle`: mailbox sender
/// - `stop`: cancellation token
/// - `local_handles`: refcount of live [`BigDocHandle`](crate::BigDocHandle) instances
/// - `internal_leases`: refcount of in-flight operations
/// - `eviction_deadline`: set when both refcounts hit zero; the janitor
///    evicts the worker after `doc_worker_idle_ttl`.
/// - `abort_handle`: runtime-neutral cancellation for the worker task;
///    fired by the janitor alongside `stop.cancel()` so the underlying
///    [`TaskSet`](crate::runtime2::TaskSet) can also cancel the task.
pub struct DocWorkerEntry {
    pub handle: DocWorkerHandle,
    pub stop: DocWorkerStopToken,
    /// Runtime-neutral abort handle for the worker task. The janitor fires
    /// this alongside the cancellation token; the [`TaskSet`](crate::runtime2::TaskSet)
    /// retains and joins the underlying runtime task.
    pub abort_handle: futures::stream::AbortHandle,
    /// Number of live [`BigDocHandle`](crate::BigDocHandle) references.
    pub local_handles: usize,
    /// Number of in-flight operations holding internal leases.
    pub internal_leases: usize,
    /// Deadline after which the janitor may evict this doc-worker.
    /// `None` when at least one refcount is non-zero.
    pub eviction_deadline: Option<std::time::Instant>,
}
