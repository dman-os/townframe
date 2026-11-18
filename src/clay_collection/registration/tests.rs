use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::*;
use crate::projection::ProjectionLease;
use crate::types::Cursor;
use crate::types::ReadLimits;
use crate::types::ReplicaId;

mod durable;
mod notifications;
mod semantics;

pub(super) fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected a ready read");
    };
    value
}

/// A writer on the one replica every semantics test runs on.
pub(super) fn writer_id(name: &str) -> WriterId {
    WriterId::new(&ReplicaId::from_bytes([0; ReplicaId::LEN]), name)
}

pub(super) type Changes<K, V> = Vec<Change<K, Stamped<Option<V>>>>;

/// One reader's view of a collection's feed: a lease plus its committed position.
pub(super) struct Feed<K, V> {
    lease: ProjectionLease<K, Stamped<Option<V>>>,
    through: Cursor,
}

impl<K: Ord + Clone, V: Clone> Feed<K, V> {
    /// Acquire a lease and consume its bootstrap page, returning its changes.
    pub(super) fn open(collection: &RegistrationCollection<K, V>) -> (Self, Changes<K, V>) {
        let mut lease = collection.projection().acquire((), None).unwrap();
        let page = ready(lease.read(None, ReadLimits::default())).unwrap();
        assert!(page.bootstrap_complete);
        let changes = page
            .transactions
            .into_iter()
            .flat_map(|transaction| transaction.changes)
            .collect();
        (
            Self {
                lease,
                through: page.through,
            },
            changes,
        )
    }

    /// The transactions published since the last call, one `Vec` of changes each.
    pub(super) fn transactions(&mut self) -> Vec<Changes<K, V>> {
        let page = ready(
            self.lease
                .read(Some(self.through.clone()), ReadLimits::default()),
        )
        .unwrap();
        self.through = page.through;
        page.transactions
            .into_iter()
            .map(|transaction| transaction.changes)
            .collect()
    }

    pub(super) fn assert_quiet(&mut self) {
        let page = self
            .lease
            .read(Some(self.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert_eq!(page.through, self.through, "unexpected publication");
    }
}
