use std::task::Wake;

use super::*;

type Observations = Arc<Mutex<Vec<(&'static str, bool)>>>;

struct ReentrantDrop {
    source: Projection<u32, u32>,
    observations: Observations,
}

impl Wake for ReentrantDrop {
    fn wake(self: Arc<Self>) {
        let unlocked = self.source.state.try_lock().is_ok();
        self.observations.lock().unwrap().push(("wake", unlocked));
    }
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        let unlocked = self.source.state.try_lock().is_ok();
        self.observations.lock().unwrap().push(("drop", unlocked));
    }
}

fn reentrant_waker(source: &Projection<u32, u32>) -> (Waker, Observations) {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let waker = Waker::from(Arc::new(ReentrantDrop {
        source: source.clone(),
        observations: Arc::clone(&observations),
    }));
    (waker, observations)
}

#[test]
fn cancelled_and_timed_out_reads_drop_waiters_after_unlocking() {
    for timeout in [false, true] {
        let source = Projection::<u32, u32>::new([1, 2]);
        let mut lease = source.acquire((), None).unwrap();
        let initial = lease
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap();
        let (waker, observations) = reentrant_waker(&source);
        let mut read = lease.read(Some(initial.through), ReadLimits::default());
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        let observed = observations.lock().unwrap().clone();
        assert!(observed.is_empty());
        if timeout {
            read.complete_timeout().unwrap();
        } else {
            drop(read);
        }
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed, vec![("drop", true)], "timeout: {timeout}");
        assert!(source.lock().waiters.is_empty());
        source.publish(Vec::new());
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed, vec![("drop", true)]);
    }
}

#[test]
fn lease_drop_destroys_waiter_after_unlocking() {
    let source = Projection::<u32, u32>::new([1, 2]);
    let lease = source.acquire((), None).unwrap();
    let (waker, observations) = reentrant_waker(&source);
    assert!(source.lock().waiters.insert(lease.scope, waker).is_none());
    drop(lease);
    let observed = observations.lock().unwrap().clone();
    assert_eq!(observed, vec![("drop", true)]);
    assert!(source.lock().waiters.is_empty());
}

#[test]
fn ready_reads_drop_waiters_after_unlocking() {
    for error in [
        None,
        Some(ReadError::ResetRequired),
        Some(ReadError::SourceUnavailable),
        Some(ReadError::Revoked),
    ] {
        let source = Projection::<u32, u32>::new([1, 2]);
        let mut lease = source.acquire((), None).unwrap();
        let initial = lease
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap();
        match error {
            None => source.publish(Vec::new()),
            Some(ReadError::ResetRequired) => source.reset(),
            Some(ReadError::SourceUnavailable) => source.mark_source_unavailable(),
            Some(ReadError::Revoked) => source.revoke(),
        }
        let (waker, observations) = reentrant_waker(&source);
        assert!(source.lock().waiters.insert(lease.scope, waker).is_none());
        assert_eq!(source.lock().waiters.len(), 1, "read outcome: {error:?}");
        let mut read = lease.read(Some(initial.through), ReadLimits::default());
        let result = Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop()));
        if let Some(error) = error {
            assert_eq!(result, Poll::Ready(Err(error)));
        } else {
            assert!(matches!(result, Poll::Ready(Ok(_))));
        }
        let remaining = source.lock().waiters.len();
        assert_eq!(remaining, 0, "read outcome: {error:?}");
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed, vec![("drop", true)], "read outcome: {error:?}");
        drop(read);
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed, vec![("drop", true)], "read outcome: {error:?}");
    }
}

#[test]
fn timed_out_errors_drop_waiters_after_unlocking() {
    for error in [
        ReadError::ResetRequired,
        ReadError::SourceUnavailable,
        ReadError::Revoked,
    ] {
        let source = Projection::<u32, u32>::new([1, 2]);
        let mut lease = source.acquire((), None).unwrap();
        let initial = lease
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap();
        match error {
            ReadError::ResetRequired => source.reset(),
            ReadError::SourceUnavailable => source.mark_source_unavailable(),
            ReadError::Revoked => source.revoke(),
        }
        let (waker, observations) = reentrant_waker(&source);
        assert!(source.lock().waiters.insert(lease.scope, waker).is_none());
        assert_eq!(
            lease
                .read(Some(initial.through), ReadLimits::default())
                .complete_timeout(),
            Err(error)
        );
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed, vec![("drop", true)]);
        assert!(source.lock().waiters.is_empty());
    }
}

#[test]
fn pending_reads_drop_replaced_waiters_after_unlocking() {
    let source = Projection::<u32, u32>::new([1, 2]);
    let mut lease = source.acquire((), None).unwrap();
    let initial = lease
        .read(None, ReadLimits::default())
        .complete_timeout()
        .unwrap();
    let (waker, observations) = reentrant_waker(&source);
    let mut read = lease.read(Some(initial.through), ReadLimits::default());
    for _ in 0..2 {
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    drop(waker);
    let observed = observations.lock().unwrap().clone();
    assert!(observed.is_empty());
    let (replacement, notified) = reentrant_waker(&source);
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(&replacement))
            .is_pending()
    );
    drop(replacement);
    let observed = observations.lock().unwrap().clone();
    assert_eq!(observed, vec![("drop", true)]);
    let waiter_count = source.lock().waiters.len();
    assert_eq!(waiter_count, 1);
    source.publish(Vec::new());
    let observed = notified.lock().unwrap().clone();
    assert_eq!(observed, vec![("wake", true), ("drop", true)]);
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(read);
    let observed = observations.lock().unwrap().clone();
    assert_eq!(observed, vec![("drop", true)]);
    assert!(source.lock().waiters.is_empty());
}
