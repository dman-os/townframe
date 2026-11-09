use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::source::Source;
use crate::types::AcquireError;
use crate::types::Change;
use crate::types::Cursor;
use crate::types::Page;
use crate::types::ReadError;
use crate::types::ReadLimits;
use crate::types::Transaction;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Live,
    SourceUnavailable,
    Revoked,
}

#[derive(Debug)]
struct Transition<K, V> {
    before: Option<Change<K, V>>,
    after: Change<K, V>,
}

#[derive(Debug)]
struct Scope<T> {
    selector: T,
    snapshot_len: usize,
    start: u64,
    issued: BTreeSet<u64>,
}

#[derive(Debug)]
struct State<K, V, T> {
    current: BTreeMap<K, Change<K, V>>,
    log: VecDeque<Vec<Transition<K, V>>>,
    scopes: BTreeMap<u64, Scope<T>>,
    resume_positions: BTreeSet<(u64, u64)>,
    capacity: NonZeroUsize,
    identity: [u64; 3],
    generation: u64,
    head: u64,
    next_scope: u64,
    status: Status,
    waiters: BTreeMap<u64, Waker>,
}

impl<K, V, T> State<K, V, T> {
    fn base(&self) -> u64 {
        self.head - self.log.len() as u64
    }

    fn prune_issued(&mut self) {
        while self
            .resume_positions
            .first()
            .is_some_and(|(sequence, _)| *sequence < self.base())
        {
            let (sequence, scope) = self.resume_positions.pop_first().unwrap();
            assert!(
                self.scopes
                    .get_mut(&scope)
                    .unwrap()
                    .issued
                    .remove(&sequence)
            );
        }
    }

    fn wake_all(&mut self) -> PendingNotifications {
        PendingNotifications(std::mem::take(&mut self.waiters))
    }
}

#[derive(Debug)]
pub struct Projection<K, V, S: Source<K, V> = ()> {
    state: Arc<Mutex<State<K, V, S::Selector>>>,
    source: Arc<S>,
}

impl<K, V, S: Source<K, V>> Clone for Projection<K, V, S> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            source: Arc::clone(&self.source),
        }
    }
}

impl<K, V> Projection<K, V> {
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self::with_source((), NonZeroUsize::new(256).unwrap(), runtime_epoch)
    }
}

impl<K, V, S: Source<K, V>> Projection<K, V, S> {
    pub fn with_source(source: S, replay_capacity: NonZeroUsize, runtime_epoch: [u64; 2]) -> Self {
        static NEXT_SOURCE: AtomicUsize = AtomicUsize::new(1);
        let mut sequence = NEXT_SOURCE.load(Ordering::Relaxed);
        loop {
            let next = sequence.checked_add(1).expect("source identity exhausted");
            match NEXT_SOURCE.compare_exchange_weak(
                sequence,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => sequence = current,
            }
        }
        let identity = [runtime_epoch[0], runtime_epoch[1], sequence as u64];
        Self {
            state: Arc::new(Mutex::new(State {
                current: BTreeMap::new(),
                log: VecDeque::new(),
                scopes: BTreeMap::new(),
                resume_positions: BTreeSet::new(),
                capacity: replay_capacity,
                identity,
                generation: 1,
                head: 0,
                next_scope: 0,
                status: Status::Live,
                waiters: BTreeMap::new(),
            })),
            source: Arc::new(source),
        }
    }

    pub fn mark_source_unavailable(&self) {
        self.set_status(Status::SourceUnavailable);
    }

    pub fn mark_live(&self) {
        self.set_status(Status::Live);
    }

    pub fn reset(&self) {
        self.reset_deferred().dispatch();
    }

    pub(crate) fn reset_deferred(&self) -> ProjectionDispatch<K, V, S::Selector> {
        let mut state = self.lock();
        state.generation = state
            .generation
            .checked_add(1)
            .expect("generation exhausted");
        let dispatch = ProjectionDispatch {
            notifications: state.wake_all(),
            current: std::mem::take(&mut state.current),
            log: std::mem::take(&mut state.log),
            scopes: std::mem::take(&mut state.scopes),
            keys: Vec::new(),
        };
        state.resume_positions.clear();
        state.head = 0;
        dispatch
    }

    pub fn revoke(&self) {
        self.set_status(Status::Revoked);
    }

    fn set_status(&self, status: Status) {
        let notifications = {
            let mut state = self.lock();
            if state.status == Status::Revoked {
                return;
            }
            state.status = status;
            state.wake_all()
        };
        notifications.dispatch();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State<K, V, S::Selector>> {
        self.state.lock().expect("projection state poisoned")
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, V>> Projection<K, V, S> {
    pub fn publish(&self, changes: Vec<Change<K, V>>) {
        self.publish_deferred(changes).dispatch();
    }

    pub(crate) fn publish_deferred(
        &self,
        changes: Vec<Change<K, V>>,
    ) -> ProjectionDispatch<K, V, S::Selector> {
        let mut dispatch = ProjectionDispatch {
            notifications: PendingNotifications::default(),
            current: BTreeMap::new(),
            log: VecDeque::new(),
            scopes: BTreeMap::new(),
            keys: Vec::new(),
        };
        let mut state = self.lock();
        assert_ne!(
            state.status,
            Status::Revoked,
            "publishing to revoked source"
        );
        let transitions = changes
            .into_iter()
            .map(|after| {
                let before = state
                    .current
                    .remove_entry(after.key())
                    .map(|(key, before)| {
                        dispatch.keys.push(key);
                        before
                    });
                if let Change::Upsert { key, .. } = &after {
                    assert!(state.current.insert(key.clone(), after.clone()).is_none());
                }
                Transition { before, after }
            })
            .collect();
        state.head = state
            .head
            .checked_add(1)
            .expect("source position exhausted");
        state.log.push_back(transitions);
        while state.log.len() > state.capacity.get() {
            dispatch.log.push_back(state.log.pop_front().unwrap());
        }
        state.prune_issued();
        dispatch.notifications = state.wake_all();
        drop(state);
        dispatch
    }

    pub fn acquire(
        &self,
        selector: S::Selector,
        resume: Option<Cursor>,
    ) -> Result<ProjectionLease<K, V, S>, AcquireError> {
        {
            let state = self.lock();
            check_acquire(state.status)?;
        }
        let (selector, interest) = self.source.acquire(selector)?;
        let mut retired_scopes = Vec::new();
        let mut state = self.lock();
        check_acquire(state.status)?;
        let bootstrap = resume.is_none();
        let start = match resume {
            Some(cursor) => {
                let position = decode(&cursor, state.identity, state.generation)
                    .ok_or(AcquireError::ResetRequired)?;
                let scope = state
                    .scopes
                    .get(&position.scope)
                    .filter(|scope| scope.selector == selector)
                    .ok_or(AcquireError::ResetRequired)?;
                if position.snapshot != scope.snapshot_len as u64
                    || !scope.issued.contains(&position.sequence)
                    || position.sequence < scope.start
                    || position.sequence < state.base()
                    || position.sequence > state.head
                {
                    return Err(AcquireError::ResetRequired);
                }
                position.sequence
            }
            None => state.head,
        };
        let snapshot: VecDeque<_> = if bootstrap {
            state
                .current
                .values()
                .filter(|change| selected(&*self.source, &selector, change))
                .cloned()
                .collect()
        } else {
            VecDeque::new()
        };
        state.next_scope = state
            .next_scope
            .checked_add(1)
            .expect("lease identity exhausted");
        let scope = state.next_scope;
        assert!(
            state
                .scopes
                .insert(
                    scope,
                    Scope {
                        selector: selector.clone(),
                        snapshot_len: snapshot.len(),
                        start,
                        issued: BTreeSet::new(),
                    },
                )
                .is_none()
        );
        while state.scopes.len() > state.capacity.get() {
            let (scope, metadata) = state.scopes.pop_first().unwrap();
            for sequence in &metadata.issued {
                assert!(state.resume_positions.remove(&(*sequence, scope)));
            }
            retired_scopes.push(metadata);
        }
        let lease = ProjectionLease {
            state: Arc::clone(&self.state),
            source: Arc::clone(&self.source),
            selector,
            _interest: interest,
            identity: state.identity,
            generation: state.generation,
            scope,
            boundary: state.head,
            start,
            snapshot_len: snapshot.len(),
            snapshot,
            acknowledged: None,
            issued: BTreeSet::new(),
            invalid: false,
        };
        drop(state);
        drop(retired_scopes);
        Ok(lease)
    }
}

#[derive(Debug)]
pub struct ProjectionLease<K, V, S: Source<K, V> = ()> {
    state: Arc<Mutex<State<K, V, S::Selector>>>,
    source: Arc<S>,
    selector: S::Selector,
    _interest: S::Interest,
    identity: [u64; 3],
    generation: u64,
    scope: u64,
    boundary: u64,
    start: u64,
    snapshot_len: usize,
    snapshot: VecDeque<Change<K, V>>,
    acknowledged: Option<Position>,
    issued: BTreeSet<Position>,
    invalid: bool,
}

impl<K, V, S: Source<K, V>> ProjectionLease<K, V, S> {
    pub fn read(
        &mut self,
        committed: Option<Cursor>,
        limits: ReadLimits,
    ) -> ProjectionRead<'_, K, V, S> {
        ProjectionRead {
            lease: self,
            committed,
            limits,
        }
    }

    fn position(
        &mut self,
        state: &State<K, V, S::Selector>,
        committed: Option<&Cursor>,
        retired: &mut VecDeque<Change<K, V>>,
    ) -> Result<Position, ReadError> {
        if state.status == Status::Revoked {
            return Err(ReadError::Revoked);
        }
        if self.invalid || state.generation != self.generation {
            return Err(ReadError::ResetRequired);
        }
        while self
            .issued
            .first()
            .is_some_and(|position| position.sequence < state.base())
        {
            self.issued.pop_first().unwrap();
        }
        let position = match committed {
            Some(cursor) => {
                let position = decode(cursor, self.identity, self.generation)
                    .ok_or(ReadError::ResetRequired)?;
                if self.acknowledged != Some(position) && !self.issued.contains(&position) {
                    return Err(ReadError::ResetRequired);
                }
                position
            }
            None if self.acknowledged.is_none() => Position {
                scope: self.scope,
                snapshot: 0,
                sequence: self.start,
            },
            None => return Err(ReadError::ResetRequired),
        };
        if position.scope != self.scope
            || position.snapshot > self.snapshot_len as u64
            || position.sequence < self.start
            || position.sequence < state.base()
            || position.sequence > state.head
            || (position.snapshot < self.snapshot_len as u64 && position.sequence != self.boundary)
        {
            return Err(ReadError::ResetRequired);
        }
        if committed.is_some() && self.acknowledged != Some(position) {
            let retained_start = self.snapshot_len - self.snapshot.len();
            if position.snapshot == self.snapshot_len as u64 {
                *retired = std::mem::take(&mut self.snapshot);
            } else {
                retired.extend(
                    self.snapshot
                        .drain(..position.snapshot as usize - retained_start),
                );
            }
            self.acknowledged = Some(position);
            self.issued = self.issued.split_off(&position);
            self.issued.remove(&position);
        }
        Ok(position)
    }

    fn page(
        &mut self,
        state: &mut State<K, V, S::Selector>,
        position: Position,
        transactions: Vec<Transaction<K, V>>,
    ) -> Page<K, V> {
        if self.acknowledged != Some(position) {
            self.issued.insert(position);
        }
        if let Some(scope) = state.scopes.get_mut(&position.scope)
            && position.snapshot == scope.snapshot_len as u64
            && scope.issued.insert(position.sequence)
        {
            assert!(
                state
                    .resume_positions
                    .insert((position.sequence, position.scope))
            );
        }
        Page {
            transactions,
            through: encode(self.identity, self.generation, position),
            bootstrap_complete: position.snapshot == self.snapshot_len as u64
                && position.sequence >= self.boundary,
        }
    }
}

impl<K, V, S: Source<K, V>> Drop for ProjectionLease<K, V, S> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.state.lock().expect("projection state poisoned");
            state.waiters.remove(&self.scope)
        };
        drop(waiter);
    }
}

pub struct ProjectionRead<'a, K, V, S: Source<K, V> = ()> {
    lease: &'a mut ProjectionLease<K, V, S>,
    committed: Option<Cursor>,
    limits: ReadLimits,
}

impl<K, V, S: Source<K, V>> ProjectionRead<'_, K, V, S> {
    pub fn complete_timeout(mut self) -> Result<Page<K, V>, ReadError> {
        let mut retired = VecDeque::new();
        let result = {
            let shared = Arc::clone(&self.lease.state);
            let mut state = shared.lock().expect("projection state poisoned");
            self.lease
                .position(&state, self.committed.as_ref(), &mut retired)
                .and_then(|position| {
                    if state.status == Status::SourceUnavailable
                        && position.snapshot == self.lease.snapshot_len as u64
                        && position.sequence == state.head
                    {
                        Err(ReadError::SourceUnavailable)
                    } else {
                        Ok(self.lease.page(&mut state, position, Vec::new()))
                    }
                })
        };
        self.invalidate(&result);
        result
    }

    fn invalidate(&mut self, result: &Result<Page<K, V>, ReadError>) {
        if matches!(result, Err(ReadError::ResetRequired)) {
            self.lease.invalid = true;
            self.lease.issued.clear();
        }
    }
}

impl<K: Clone, V: Clone, S: Source<K, V>> ProjectionRead<'_, K, V, S> {
    fn read_page(
        &mut self,
        state: &mut State<K, V, S::Selector>,
        retired: &mut VecDeque<Change<K, V>>,
    ) -> Result<Option<Page<K, V>>, ReadError> {
        let mut position = self
            .lease
            .position(state, self.committed.as_ref(), retired)?;
        let mut transactions = Vec::new();
        let mut changes = 0;
        let retained_start = self.lease.snapshot_len - self.lease.snapshot.len();
        for change in self
            .lease
            .snapshot
            .iter()
            .skip(position.snapshot as usize - retained_start)
        {
            if full(self.limits, transactions.len(), changes, 1) {
                break;
            }
            transactions.push(Transaction {
                changes: vec![change.clone()],
            });
            changes += 1;
            position.snapshot += 1;
        }
        if !transactions.is_empty() {
            return Ok(Some(self.lease.page(state, position, transactions)));
        }
        let offset = (position.sequence - state.base()) as usize;
        let mut scanned = 0;
        for transition in state.log.iter().skip(offset) {
            if full(self.limits, scanned, changes, 0) {
                break;
            }
            if let Some(max) = self.limits.max_changes
                && changes > 0
            {
                let candidate = transition
                    .iter()
                    .filter(|transition| {
                        selected(&*self.lease.source, &self.lease.selector, &transition.after)
                            || transition.before.as_ref().is_some_and(|before| {
                                selected(&*self.lease.source, &self.lease.selector, before)
                            })
                    })
                    .take((max as usize).saturating_sub(changes) + 1)
                    .count();
                if full(self.limits, scanned, changes, candidate) {
                    break;
                }
            }
            let selected_changes: Vec<_> = transition
                .iter()
                .filter_map(|transition| {
                    if selected(&*self.lease.source, &self.lease.selector, &transition.after) {
                        Some(transition.after.clone())
                    } else if transition.before.as_ref().is_some_and(|before| {
                        selected(&*self.lease.source, &self.lease.selector, before)
                    }) {
                        Some(Change::Remove {
                            key: transition.after.key().clone(),
                            row_version: transition.after.row_version(),
                        })
                    } else {
                        None
                    }
                })
                .collect();
            scanned += 1;
            position.sequence += 1;
            changes += selected_changes.len();
            if !selected_changes.is_empty() {
                transactions.push(Transaction {
                    changes: selected_changes,
                });
            }
        }
        if scanned > 0 {
            return Ok(Some(self.lease.page(state, position, transactions)));
        }
        if state.status == Status::SourceUnavailable {
            return Err(ReadError::SourceUnavailable);
        }
        if self.committed.is_none() {
            return Ok(Some(self.lease.page(state, position, Vec::new())));
        }
        Ok(None)
    }
}

impl<K: Clone, V: Clone, S: Source<K, V>> Future for ProjectionRead<'_, K, V, S> {
    type Output = Result<Page<K, V>, ReadError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut candidate = Some(cx.waker().clone());
        let mut retired = VecDeque::new();
        let retired_waker;
        let result = {
            let shared = Arc::clone(&this.lease.state);
            let mut state = shared.lock().expect("projection state poisoned");
            match this.read_page(&mut state, &mut retired) {
                Ok(None) => {
                    retired_waker = match state.waiters.get_mut(&this.lease.scope) {
                        Some(waker) if waker.will_wake(cx.waker()) => None,
                        Some(waker) => Some(std::mem::replace(waker, candidate.take().unwrap())),
                        None => state
                            .waiters
                            .insert(this.lease.scope, candidate.take().unwrap()),
                    };
                    Poll::Pending
                }
                Ok(Some(page)) => {
                    retired_waker = state.waiters.remove(&this.lease.scope);
                    Poll::Ready(Ok(page))
                }
                Err(error) => {
                    retired_waker = state.waiters.remove(&this.lease.scope);
                    Poll::Ready(Err(error))
                }
            }
        };
        if let Poll::Ready(result) = &result {
            this.invalidate(result);
        }
        drop(retired_waker);
        drop(retired);
        drop(candidate);
        result
    }
}

impl<K, V, S: Source<K, V>> Drop for ProjectionRead<'_, K, V, S> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.lease.state.lock().expect("projection state poisoned");
            state.waiters.remove(&self.lease.scope)
        };
        drop(waiter);
    }
}

fn selected<K, V, S: Source<K, V>>(
    source: &S,
    selector: &S::Selector,
    change: &Change<K, V>,
) -> bool {
    match change {
        Change::Upsert { key, value, .. } => source.matches(selector, key, value),
        Change::Remove { .. } => false,
    }
}

fn full(limits: ReadLimits, count: usize, changes: usize, candidate: usize) -> bool {
    count > 0
        && (limits
            .max_transactions
            .is_some_and(|max| count >= max as usize)
            || (changes > 0
                && limits
                    .max_changes
                    .is_some_and(|max| changes.saturating_add(candidate) > max as usize)))
}

fn check_acquire(status: Status) -> Result<(), AcquireError> {
    match status {
        Status::Live => Ok(()),
        Status::SourceUnavailable => Err(AcquireError::SourceUnavailable),
        Status::Revoked => Err(AcquireError::Revoked),
    }
}

pub(crate) struct ProjectionDispatch<K, V, T> {
    notifications: PendingNotifications,
    current: BTreeMap<K, Change<K, V>>,
    log: VecDeque<Vec<Transition<K, V>>>,
    scopes: BTreeMap<u64, Scope<T>>,
    keys: Vec<K>,
}

impl<K, V, T> ProjectionDispatch<K, V, T> {
    pub(crate) fn dispatch(self) {
        let Self {
            notifications,
            current,
            log,
            scopes,
            keys,
        } = self;
        notifications.dispatch();
        drop((current, log, scopes, keys));
    }
}

#[derive(Debug, Default)]
#[must_use = "dispatch notifications after releasing authority locks"]
pub struct PendingNotifications(BTreeMap<u64, Waker>);

impl PendingNotifications {
    pub fn dispatch(self) {
        for waiter in self.0.into_values() {
            waiter.wake();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Position {
    scope: u64,
    snapshot: u64,
    sequence: u64,
}

fn encode(identity: [u64; 3], generation: u64, position: Position) -> Cursor {
    Cursor(
        [
            identity[0],
            identity[1],
            identity[2],
            generation,
            position.scope,
            position.snapshot,
            position.sequence,
        ]
        .into_iter()
        .flat_map(u64::to_be_bytes)
        .collect(),
    )
}

fn decode(cursor: &Cursor, identity: [u64; 3], generation: u64) -> Option<Position> {
    let bytes: &[u8; 56] = cursor.0.as_slice().try_into().ok()?;
    let fields: [u64; 7] = std::array::from_fn(|index| {
        u64::from_be_bytes(bytes[index * 8..index * 8 + 8].try_into().unwrap())
    });
    if fields[..4] != [identity[0], identity[1], identity[2], generation] {
        return None;
    }
    Some(Position {
        scope: fields[4],
        snapshot: fields[5],
        sequence: fields[6],
    })
}

#[cfg(test)]
#[path = "projection_tests_callbacks.rs"]
pub(crate) mod callback_tests;

#[cfg(test)]
#[path = "projection_tests_teardown.rs"]
mod teardown_tests;

#[cfg(test)]
mod tests {
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
}
