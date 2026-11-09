use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::Source;
use crate::projection::Projection;
use crate::projection::ProjectionLease;
use crate::types::AcquireError;
use crate::types::Change;
use crate::types::Cursor;
use crate::types::Page;
use crate::types::ReadError;
use crate::types::ReadLimits;
use crate::types::RowEvidence;
use crate::types::Transaction;
use crate::types::Version;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Selector {
    All,
    AtMost(i32),
}

#[derive(Debug, Default)]
struct Fixture {
    interests: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct Interest(Arc<AtomicUsize>);

impl Drop for Interest {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Source<u32, i32> for Fixture {
    type Selector = Selector;
    type Interest = Interest;

    fn acquire(&self, selector: Selector) -> Result<(Selector, Interest), AcquireError> {
        let selector = match selector {
            Selector::All => Selector::AtMost(10),
            Selector::AtMost(limit) if limit < 0 => return Err(AcquireError::InvalidSelector),
            Selector::AtMost(limit) if limit > 10 => {
                return Err(AcquireError::UnauthorizedSelector);
            }
            Selector::AtMost(limit) => Selector::AtMost(limit),
        };
        self.interests.fetch_add(1, Ordering::SeqCst);
        Ok((selector, Interest(Arc::clone(&self.interests))))
    }

    fn matches(&self, selector: &Selector, _key: &u32, value: &i32) -> bool {
        match selector {
            Selector::All => unreachable!(),
            Selector::AtMost(limit) => value <= limit,
        }
    }
}

#[derive(Default)]
struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn source(capacity: usize) -> Projection<u32, i32, Fixture> {
    Projection::with_source(
        Fixture::default(),
        NonZeroUsize::new(capacity).unwrap(),
        [1, 2],
    )
}

fn upsert(key: u32, seq: u64, value: i32) -> Change<u32, i32> {
    Change::Upsert {
        key,
        row_version: Version::new(1, seq),
        value,
        evidence: RowEvidence::Authoritative,
        write_context: Some(vec![1, 2, 3]),
    }
}

fn remove(key: u32, seq: u64) -> Change<u32, i32> {
    Change::Remove {
        key,
        row_version: Version::new(1, seq),
    }
}

fn page<K: Clone, V: Clone, S: Source<K, V>>(
    lease: &mut ProjectionLease<K, V, S>,
    committed: Option<Cursor>,
    limits: ReadLimits,
) -> Result<Page<K, V>, ReadError> {
    let mut read = lease.read(committed, limits);
    match Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(page) => page,
        Poll::Pending => panic!("expected a ready page"),
    }
}

fn one() -> ReadLimits {
    ReadLimits {
        max_transactions: Some(1),
        max_changes: Some(1),
    }
}

#[path = "tests_cursors.rs"]
mod cursors;
#[path = "tests_lifecycle.rs"]
mod lifecycle;
#[path = "tests_paging.rs"]
mod paging;
#[path = "tests_resume.rs"]
mod resume;
#[path = "tests_snapshot.rs"]
mod snapshot;
