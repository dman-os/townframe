use super::*;

#[test]
fn cursor_metadata_is_bounded_by_replay_and_scope_retention() {
    let capacity = NonZeroUsize::new(3).unwrap();
    let source = Projection::<u32, u32>::with_source((), capacity, [1, 2]);
    let assert_bounds = || {
        let state = source.lock();
        assert!(state.scopes.len() <= capacity.get());
        assert!(state.resume_positions.len() <= capacity.get() * (capacity.get() + 1));
        assert_eq!(
            state.resume_positions.len(),
            state
                .scopes
                .values()
                .map(|scope| scope.issued.len())
                .sum::<usize>()
        );
        for (sequence, scope) in &state.resume_positions {
            assert!(*sequence >= state.base() && *sequence <= state.head);
            assert!(state.scopes[scope].issued.contains(sequence));
        }
    };
    let mut lease = source.acquire((), None).unwrap();
    let mut committed = lease
        .read(None, ReadLimits::default())
        .complete_timeout()
        .unwrap()
        .through;
    for _ in 0..32 {
        for _ in 0..capacity.get() {
            source.publish(Vec::new());
        }
        let mut through = committed.clone();
        for limit in 1..=capacity.get() {
            let Poll::Ready(Ok(page)) = Pin::new(&mut lease.read(
                Some(committed.clone()),
                ReadLimits {
                    max_transactions: Some(limit as u32),
                    max_changes: None,
                },
            ))
            .poll(&mut Context::from_waker(Waker::noop())) else {
                panic!("expected retained progress");
            };
            assert!(page.transactions.is_empty());
            assert_ne!(page.through, through);
            through = page.through;
            assert_eq!(lease.issued.len(), limit);
            assert_bounds();
        }
        committed = through;
        assert_eq!(
            lease
                .read(Some(committed.clone()), ReadLimits::default())
                .complete_timeout()
                .unwrap()
                .through,
            committed
        );
        assert!(lease.issued.is_empty());
    }
    for _ in 0..32 {
        source.publish(Vec::new());
        let mut fresh = source.acquire((), None).unwrap();
        fresh
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert_bounds();
    }
    source.reset();
    assert!(source.lock().scopes.is_empty());
    assert!(source.lock().resume_positions.is_empty());
    assert_eq!(
        lease
            .read(Some(committed), ReadLimits::default())
            .complete_timeout(),
        Err(ReadError::ResetRequired)
    );
    assert!(lease.issued.is_empty());
}

#[test]
fn runtime_namespaces_reject_stale_cursors_when_local_positions_repeat() {
    let open = |epoch| {
        let source = Projection::<u32, u32>::new(epoch);
        source.lock().identity[2] = 1;
        source.publish(Vec::new());
        let mut lease = source.acquire((), None).unwrap();
        let cursor = lease
            .read(None, ReadLimits::default())
            .complete_timeout()
            .unwrap()
            .through;
        (source, lease, cursor)
    };
    let (old_source, _, stale) = open([1, 2]);
    assert!(old_source.acquire((), Some(stale.clone())).is_ok());
    for epoch in [[2, 2], [1, 3]] {
        for timeout in [false, true] {
            let (source, mut lease, current) = open(epoch);
            assert_eq!(stale.0[16..], current.0[16..]);
            assert_ne!(stale.0[..16], current.0[..16]);
            assert!(source.acquire((), Some(current)).is_ok());
            assert!(matches!(
                source.acquire((), Some(stale.clone())),
                Err(AcquireError::ResetRequired)
            ));
            let mut read = lease.read(Some(stale.clone()), ReadLimits::default());
            if timeout {
                assert_eq!(read.complete_timeout(), Err(ReadError::ResetRequired));
            } else {
                assert_eq!(
                    Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Err(ReadError::ResetRequired))
                );
            }
        }
    }
}

pub(crate) mod callbacks;
mod teardown;
