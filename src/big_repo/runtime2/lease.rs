//! Leases for runtime2. No Tokio types — uses `futures::channel::oneshot` for
//! lease release signals and `async_channel` for worker mailboxes.

use crate::interlude::*;
use crate::runtime2::messages::DocWorkerMsg;
use crate::DocumentId;

/// RAII lease held by a `BigDocHandle` (the public doc handle).
///
/// On drop, signals the hub to decrement `local_handles` for the doc-worker,
/// which may schedule eviction if both refcounts reach zero.
///
/// Mirrors `RuntimeDocLease` in `runtime.rs:141` — the old runtime's
/// handle lease that fires `release_doc_lease` on drop.
pub struct DocLease {
    pub(crate) release: Option<futures::channel::oneshot::Sender<()>>,
}

impl DocLease {
    /// Create a new lease. The `release` sender is consumed on drop to signal
    /// the hub.
    pub(crate) fn new(release: futures::channel::oneshot::Sender<()>) -> Self {
        Self {
            release: Some(release),
        }
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
    pub(crate) release: Option<futures::channel::oneshot::Sender<()>>,
}

impl DocWorkerInternalLease {
    /// Create a new internal lease. The `release` sender fires on drop.
    pub(crate) fn new(doc_id: DocumentId, release: futures::channel::oneshot::Sender<()>) -> Self {
        Self {
            doc_id,
            release: Some(release),
        }
    }
}

impl Drop for DocWorkerInternalLease {
    fn drop(&mut self) {
        let _ = self.release.take();
    }
}

/// The hub's handle to a doc-worker: just the mailbox sender.
///
/// Uses `async_channel` for backpressure between the hub (synchronous
/// `try_send`) and the async worker loop.
#[derive(Clone)]
pub struct DocWorkerHandle {
    pub(crate) msg_tx: async_channel::Sender<DocWorkerMsg>,
}

impl DocWorkerHandle {
    /// Send a message to the doc-worker synchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is closed (worker gone) or full
    /// (worker backlogged). Both are treated as fatal by the hub.
    pub fn send(&self, msg: DocWorkerMsg) -> eyre::Result<()> {
        self.msg_tx.try_send(msg).map_err(|e| match e {
            async_channel::TrySendError::Closed(_) => ferr!("doc worker closed"),
            async_channel::TrySendError::Full(_) => ferr!("doc worker mailbox full"),
        })
    }
}

/// Runtime-neutral abort handle for a doc-worker.
///
/// Wraps [`futures::future::AbortHandle`]; `cancel()` calls abort.
pub struct DocWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl DocWorkerStopToken {
    /// Signal the doc-worker to stop. The worker should check abort status
    /// at yield points or the enclosing [`TaskSet`](crate::runtime2::TaskSet)
    /// will abort it when the set stops.
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

/// The hub's bookkeeping for one doc-worker.
///
/// Matches the old `DocWorkerEntry` at `runtime.rs:2130` field-for-field:
/// - `handle`: mailbox sender
/// - `stop`: abort handle (replaces CancellationToken)
/// - `local_handles`: refcount of live [`BigDocHandle`](crate::BigDocHandle) instances
/// - `internal_leases`: refcount of in-flight operations
/// - `eviction_deadline`: set when both refcounts hit zero; the janitor
///    evicts the worker after `doc_worker_idle_ttl`.
pub struct DocWorkerEntry {
    pub handle: DocWorkerHandle,
    /// The one authoritative abort handle for this worker. The janitor calls
    /// `stop.cancel()` on eviction; the hub uses this for shutdown.
    pub stop: DocWorkerStopToken,
    /// Number of live [`BigDocHandle`](crate::BigDocHandle) references.
    pub local_handles: usize,
    /// Number of in-flight operations holding internal leases.
    pub internal_leases: usize,
    /// Deadline after which the janitor may evict this doc-worker.
    /// `None` when at least one refcount is non-zero.
    pub eviction_deadline: Option<std::time::Instant>,
}
