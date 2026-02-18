// FIXME: these should be tied to triage.rs?
// right now, we can't use these from routines since they
// head that the routine is working on might be an old one
// compared to the index

use crate::interlude::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub mod facet_ref;
pub mod facet_set;

pub use facet_ref::{
    DocFacetRefEdge, DocFacetRefIndexEvent, DocFacetRefIndexRepo, DocFacetRefIndexStopToken,
};
pub use facet_set::{
    DocFacetSetIndexEvent, DocFacetSetIndexRepo, DocFacetSetIndexStopToken, DocFacetTagMembership,
};

pub(crate) struct BufferedRepoEvents<E> {
    tx: tokio::sync::mpsc::UnboundedSender<Arc<E>>,
    bootstrap_done: Arc<AtomicBool>,
    pending: Arc<std::sync::Mutex<Vec<Arc<E>>>>,
}

impl<E> BufferedRepoEvents<E>
where
    E: Send + Sync + 'static,
{
    pub(crate) fn new(tx: tokio::sync::mpsc::UnboundedSender<Arc<E>>) -> Self {
        Self {
            tx,
            bootstrap_done: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn listener_callback(&self) -> impl Fn(Arc<E>) + Send + Sync + 'static {
        let tx = self.tx.clone();
        let bootstrap_done = Arc::clone(&self.bootstrap_done);
        let pending = Arc::clone(&self.pending);
        move |event| {
            if bootstrap_done.load(Ordering::Acquire) {
                let _ = tx.send(event);
            } else {
                pending.lock().expect(ERROR_MUTEX).push(event);
            }
        }
    }

    pub(crate) fn push_diff_events<I>(&self, events: I) -> bool
    where
        I: IntoIterator<Item = E>,
    {
        for event in events {
            if self.tx.send(Arc::new(event)).is_err() {
                return false;
            }
        }
        true
    }

    pub(crate) fn finish_bootstrap(&self) -> bool {
        self.bootstrap_done.store(true, Ordering::Release);
        let mut lock = self.pending.lock().expect(ERROR_MUTEX);
        for event in lock.drain(..) {
            if self.tx.send(event).is_err() {
                return false;
            }
        }
        true
    }
}
