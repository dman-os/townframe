//! Leases for runtime2. Lease drops enqueue release commands directly; worker
//! operation leases use oneshots because their owner is an in-flight future.

use crate::interlude::*;
use crate::runtime2::messages::DocWorkerMsg;
use crate::DocumentId;

/// RAII lease held by a `BigDocHandle` (the public doc handle).
///
/// On drop, signals the hub to decrement `local_handles` for the doc-worker,
/// which may schedule eviction if both refcounts reach zero.
pub struct DocLease {
    cmd_tx: async_channel::Sender<crate::runtime2::Runtime2Cmd>,
    doc_id: DocumentId,
}

impl DocLease {
    pub(crate) fn new(
        cmd_tx: async_channel::Sender<crate::runtime2::Runtime2Cmd>,
        doc_id: DocumentId,
    ) -> Self {
        Self { cmd_tx, doc_id }
    }
}

impl Drop for DocLease {
    fn drop(&mut self) {
        if let Err(async_channel::TrySendError::Full(_)) =
            self.cmd_tx
                .try_send(crate::runtime2::Runtime2Cmd::ReleaseDocLease {
                    doc_id: self.doc_id,
                })
        {
            unreachable!("runtime command channel is unbounded");
        }
    }
}

/// RAII lease held by in-flight doc-worker operations.
///
/// While any `DocWorkerInternalLease` is held, the hub will NOT evict
/// the doc-worker (the `internal_leases` refcount in
/// [`DocWorkerEntry`] is > 0). Bundled into messages sent to the doc-worker
/// so the lease lives for the duration of the operation.
///
/// Used by finite mailbox operations such as local commits and quiescence
/// barriers. Remote Subduction sessions no longer acquire worker leases.
#[derive(Debug)]
pub struct DocWorkerInternalLease {
    pub(crate) release: Option<futures::channel::oneshot::Sender<()>>,
}

impl DocWorkerInternalLease {
    /// Create a new internal lease. The `release` sender fires on drop.
    pub(crate) fn new(release: futures::channel::oneshot::Sender<()>) -> Self {
        Self {
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
    /// (worker backlogged). Callers may explicitly handle closure when it races
    /// an expected zero-handle eviction.
    pub fn send(&self, msg: DocWorkerMsg) -> eyre::Result<()> {
        self.msg_tx.try_send(msg).map_err(|err| match err {
            async_channel::TrySendError::Closed(_) => ferr!("doc worker closed"),
            async_channel::TrySendError::Full(_) => ferr!("doc worker mailbox full"),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.msg_tx.is_closed()
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

/// RAII guard for tracked background futures. Emits `TrackedWorkDone` on drop,
/// guaranteeing `tracked_in_flight` is decremented even if the future panics
/// or is cancelled early.
pub struct TrackedWorkGuard {
    evt_tx: Option<async_channel::Sender<crate::runtime2::Runtime2Evt>>,
    kind: crate::runtime2::TrackedWorkKind,
}

impl TrackedWorkGuard {
    pub(crate) fn new(
        evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        kind: crate::runtime2::TrackedWorkKind,
    ) -> Self {
        Self {
            evt_tx: Some(evt_tx),
            kind,
        }
    }
}

impl Drop for TrackedWorkGuard {
    fn drop(&mut self) {
        if let Some(evt_tx) = self.evt_tx.take() {
            let _ = evt_tx.try_send(crate::runtime2::Runtime2Evt::TrackedWorkDone {
                kind: self.kind,
            });
        }
    }
}
