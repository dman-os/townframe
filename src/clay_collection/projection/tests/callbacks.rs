use std::mem::ManuallyDrop;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::TryLockError;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::task::RawWaker;
use std::task::RawWakerVTable;

use super::*;
use crate::types::RowEvidence;
use crate::types::Version;

type Events = Arc<Mutex<Vec<(&'static str, bool)>>>;

pub(crate) fn unlocked<K, V, S: Source<K, V>>(source: &Projection<K, V, S>) -> bool {
    !matches!(source.state.try_lock(), Err(TryLockError::WouldBlock))
}

struct Probe<T: Send> {
    state: Weak<Mutex<T>>,
    events: Events,
}

impl<T: Send> Probe<T> {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_raw,
        Self::wake_raw,
        Self::wake_by_ref_raw,
        Self::drop_raw,
    );

    fn record(&self, event: &'static str) {
        let state = self.state.upgrade().unwrap();
        let unlocked = !matches!(state.try_lock(), Err(TryLockError::WouldBlock));
        self.events.lock().unwrap().push((event, unlocked));
    }

    fn raw(self: Arc<Self>) -> RawWaker {
        RawWaker::new(Arc::into_raw(self).cast(), &Self::VTABLE)
    }

    unsafe fn clone_raw(data: *const ()) -> RawWaker {
        let probe = ManuallyDrop::new(unsafe { Arc::<Self>::from_raw(data.cast()) });
        probe.record("clone");
        Arc::clone(&probe).raw()
    }

    unsafe fn wake_raw(data: *const ()) {
        let probe = unsafe { Arc::<Self>::from_raw(data.cast()) };
        probe.record("wake");
    }

    unsafe fn wake_by_ref_raw(data: *const ()) {
        let probe = ManuallyDrop::new(unsafe { Arc::<Self>::from_raw(data.cast()) });
        probe.record("wake");
    }

    unsafe fn drop_raw(data: *const ()) {
        let probe = unsafe { Arc::<Self>::from_raw(data.cast()) };
        probe.record("release");
    }
}

impl<T: Send> Drop for Probe<T> {
    fn drop(&mut self) {
        self.record("destroy");
    }
}

fn raw_waker<T: Send>(state: &Arc<Mutex<T>>, events: &Events) -> Waker {
    let probe = Arc::new(Probe {
        state: Arc::downgrade(state),
        events: Arc::clone(events),
    });
    unsafe { Waker::from_raw(probe.raw()) }
}

#[test]
fn initial_and_replacement_waker_clones_run_before_locking() {
    let source = Projection::<u32, u32>::new([1, 2]);
    let mut lease = source.acquire((), None).unwrap();
    let initial = lease
        .read(None, ReadLimits::default())
        .complete_timeout()
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let first = raw_waker(&source.state, &events);
    let second = raw_waker(&source.state, &events);
    let mut read = lease.read(Some(initial.through), ReadLimits::default());
    for waker in [&first, &first, &second] {
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(waker))
                .is_pending()
        );
    }
    let observed = events.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![
            ("clone", true),
            ("clone", true),
            ("release", true),
            ("clone", true),
            ("release", true)
        ]
    );
    events.lock().unwrap().clear();
    source.publish(Vec::new());
    let observed = events.lock().unwrap().clone();
    assert_eq!(observed, vec![("wake", true)]);
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}

#[test]
fn ready_read_releases_unused_waker_candidate_after_unlocking() {
    let source = Projection::<u32, u32>::new([1, 2]);
    let mut lease = source.acquire((), None).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let waker = raw_waker(&source.state, &events);
    assert!(
        source
            .lock()
            .waiters
            .insert(lease.scope, raw_waker(&source.state, &events))
            .is_none()
    );
    let mut read = lease.read(None, ReadLimits::default());
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    let observed = events.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![
            ("clone", true),
            ("release", true),
            ("destroy", true),
            ("release", true)
        ]
    );
    drop(read);
    let observed = events.lock().unwrap().clone();
    assert_eq!(observed.len(), 4);
}

#[test]
fn failed_reads_release_unused_waker_candidates_after_unlocking() {
    for error in [
        ReadError::ResetRequired,
        ReadError::SourceUnavailable,
        ReadError::Revoked,
    ] {
        let source = Projection::<u32, u32>::new([1, 2]);
        let mut lease = source.acquire((), None).unwrap();
        match error {
            ReadError::ResetRequired => source.reset(),
            ReadError::SourceUnavailable => source.mark_source_unavailable(),
            ReadError::Revoked => source.revoke(),
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        let waker = raw_waker(&source.state, &events);
        assert!(
            source
                .lock()
                .waiters
                .insert(lease.scope, raw_waker(&source.state, &events))
                .is_none()
        );
        let mut read = lease.read(None, ReadLimits::default());
        let result = Pin::new(&mut read).poll(&mut Context::from_waker(&waker));
        let observed = events.lock().unwrap().clone();
        assert_eq!(result, Poll::Ready(Err(error)));
        assert_eq!(
            observed,
            vec![
                ("clone", true),
                ("release", true),
                ("destroy", true),
                ("release", true)
            ],
            "read outcome: {error:?}"
        );
        drop(read);
        let observed = events.lock().unwrap().clone();
        assert_eq!(observed.len(), 4, "read outcome: {error:?}");
    }
}

#[derive(Debug)]
struct PanicClone(Arc<AtomicBool>);

impl Clone for PanicClone {
    fn clone(&self) -> Self {
        assert!(!self.0.load(Ordering::SeqCst), "planned clone panic");
        Self(Arc::clone(&self.0))
    }
}

#[test]
fn unwinding_read_releases_waker_candidate_after_unlocking() {
    let source = Projection::new([1, 2]);
    let panic = Arc::new(AtomicBool::new(false));
    source.publish(vec![Change::Upsert {
        key: 1u32,
        row_version: Version::new(1, 1),
        value: PanicClone(Arc::clone(&panic)),
        evidence: RowEvidence::Authoritative,
        write_context: None,
    }]);
    let mut lease = source.acquire((), None).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let waker = raw_waker(&source.state, &events);
    assert!(
        source
            .lock()
            .waiters
            .insert(lease.scope, raw_waker(&source.state, &events))
            .is_none()
    );
    let mut read = lease.read(None, ReadLimits::default());
    panic.store(true, Ordering::SeqCst);
    let result = catch_unwind(AssertUnwindSafe(|| {
        Pin::new(&mut read).poll(&mut Context::from_waker(&waker))
    }));
    assert!(result.is_err());
    let observed = events.lock().unwrap().clone();
    source.state.clear_poison();
    assert_eq!(observed, vec![("clone", true), ("release", true)]);
    drop(read);
    let observed = events.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![
            ("clone", true),
            ("release", true),
            ("release", true),
            ("destroy", true)
        ]
    );
}

#[test]
fn deferred_notifications_commit_before_explicit_dispatch() {
    for reset in [false, true] {
        let source = Projection::<u32, u32>::new([1, 2]);
        let mut lease = source.acquire((), None).unwrap();
        let initial = lease
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let waker = raw_waker(&source.state, &events);
        let mut read = lease.read(Some(initial.through), ReadLimits::default());
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        events.lock().unwrap().clear();
        let notifications = if reset {
            source.reset_deferred()
        } else {
            source.publish_deferred(Vec::new())
        };
        assert!(events.lock().unwrap().is_empty());
        let result = Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop()));
        if reset {
            assert_eq!(result, Poll::Ready(Err(ReadError::ResetRequired)));
        } else {
            assert!(matches!(result, Poll::Ready(Ok(_))));
        }
        notifications.dispatch();
        let observed = events.lock().unwrap().clone();
        assert_eq!(observed, vec![("wake", true)]);
    }
}
