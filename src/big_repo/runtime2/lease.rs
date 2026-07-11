//! Leases for runtime2.
//!
//! Leases keep a doc-worker alive while there's interest (a live handle) or
//! in-flight work (a sync/commit). On drop they message the hub, which
//! decrements counters and schedules eviction. Same shape as today's
//! `RuntimeDocLease` / `DocWorkerInternalLease`, lifted to runtime2 types.

use crate::interlude::*;
use crate::runtime2::messages::Runtime2Cmd;
use crate::DocumentId;

/// Held by a `BigDocHandle` (the public doc handle). On drop, tells the hub to
/// release the local lease → may schedule doc-worker eviction.
pub struct DocLease {
    pub(crate) release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DocLease {
    pub(crate) fn new(release: tokio::sync::oneshot::Sender<()>) -> Self {
        Self { release: Some(release) }
    }
}

impl Drop for DocLease {
    fn drop(&mut self) {
        // Fire-and-forget; the hub treats a dropped lease as "handle gone".
        let _ = self.release.take();
    }
}

/// Held by in-flight doc-worker operations (commit, sync, apply-session). While
/// any internal lease is held, the hub will NOT evict the doc-worker. Replaces
/// `DocWorkerInternalLease` (which today is a refcount on `DocWorkerEntry`).
pub struct DocWorkerInternalLease {
    pub(crate) doc_id: DocumentId,
    pub(crate) release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DocWorkerInternalLease {
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
pub struct DocWorkerHandle {
    pub(crate) msg_tx: tokio::sync::mpsc::UnboundedSender<DocWorkerMsg>,
}

impl DocWorkerHandle {
    /// Send a message to the doc-worker. Errors only if the worker is gone
    /// (treated as a fatal-worker event by the hub).
    pub fn send(&self, msg: DocWorkerMsg) -> eyre::Result<()> {
        self.msg_tx.send(msg).map_err(|_| ferr!("doc worker closed"))
    }
}

/// Cancels a doc-worker (child cancellation token). Replaces `DocWorkerStopToken`.
pub struct DocWorkerStopToken {
    pub(crate) cancel: tokio_util::sync::CancellationToken,
}

impl DocWorkerStopToken {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// The hub's bookkeeping for one doc-worker. Refcounts drive eviction:
/// `local_handles` (live `BigDocHandle`s) + `internal_leases` (in-flight ops).
/// When both are zero for `doc_worker_idle_ttl`, the janitor evicts.
pub struct DocWorkerEntry {
    pub handle: DocWorkerHandle,
    pub stop: DocWorkerStopToken,
    pub local_handles: usize,
    pub internal_leases: usize,
    pub eviction_deadline: Option<std::time::Instant>,
}
