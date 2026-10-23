//! Host-owned contribution collections: the bridge from [`crate::instance`] authority to the
//! `clay_collection` substrate.
//!
//! The host publishes under its mutex, possibly from another thread, while a `View` is
//! single-threaded; [`CollectionView`] is that boundary and owns the pump across it.
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use tokio::sync::mpsc;

use clay_collection::BaseDelta;
use clay_collection::Batch;
use clay_collection::DemandId;
use clay_collection::DemandToken;
use clay_collection::Downstream;
use clay_collection::OperationId;
use clay_collection::OverlayPolicy;
use clay_collection::ReadOnly;
use clay_collection::Replica;
use clay_collection::ScopeId;
use clay_collection::Upstream;
use clay_collection::View;

use crate::instance::AccessError;
use crate::instance::Host;
use crate::instance::Instance;

// ── Types: public collection vocabulary, then internal state records ─────────

/// Host-owned transient contribution collection (ADR 015 §4): owned entries with
/// registration/disposal semantics, not optimistic persistence or source cursors. Entries are
/// plain clone-cheap data (R4) so the same collection shape can cross the wire for guests.
/// A collection's read and insertion handles carry distinct authority. The host constructs the
/// backing store; a domain API (such as Settings) defines the entry type, not the host.
pub struct HostedCollection<T: Clone + Send + Sync + 'static> {
    host: Host,
    provider: Instance,
    pub(crate) state: Arc<Mutex<CollectionState<T>>>,
}

#[derive(Clone)]
pub struct CollectionReader<T: Clone + Send + Sync + 'static> {
    collection: HostedCollection<T>,
    consumer: Instance,
    pub(crate) reader_id: u64,
}

#[derive(Clone)]
pub struct CollectionWriter<T: Clone + Send + Sync + 'static> {
    collection: HostedCollection<T>,
    contributor: Instance,
}

#[derive(Clone)]
pub struct Entry<T: Clone + Send + Sync + 'static> {
    writer: CollectionWriter<T>,
    pub(crate) id: u64,
    generation: u64,
}

pub struct CollectionSubscription<T: Clone + Send + Sync + 'static> {
    receiver: mpsc::UnboundedReceiver<Batch<u64, T>>,
    state: Weak<Mutex<CollectionState<T>>>,
    reader_id: u64,
    id: u64,
}

/// A reader's bound local view: the read-only [`View`] together with the stream that feeds
/// it. The two travel as one because the pump that drains the stream into the view *is* the
/// thread boundary — the host publishes under its mutex, possibly from another thread, while
/// a view's state is single-threaded. A view held without its stream silently stops
/// advancing, which is why this type does not hand one out on its own.
pub struct CollectionView<T: Clone + PartialEq + Send + Sync + 'static> {
    view: View<u64, T, ReadOnly>,
    input: Downstream<u64, T>,
    subscription: CollectionSubscription<T>,
}

pub(crate) struct CollectionState<T> {
    next: u64,
    next_generation: u64,
    next_reader: u64,
    next_subscription: u64,
    pub(crate) replica: Replica<u64, T>,
    generations: BTreeMap<u64, u64>,
    contributors: BTreeSet<u64>,
    /// reader id → subscription id → sender. Grouped by reader so a consumer's close
    /// withdraws exactly its own streams while other readers keep theirs.
    pub(crate) subscriptions: BTreeMap<u64, BTreeMap<u64, mpsc::UnboundedSender<Batch<u64, T>>>>,
}

struct HostUpstream<T: Clone + Send + Sync + 'static> {
    state: Arc<Mutex<CollectionState<T>>>,
    reader_id: u64,
    subscription_id: u64,
}

// ── Public API: constructors, readers, writers, and subscriptions ────────────

impl<T: Clone + Send + Sync + 'static> HostedCollection<T> {
    pub fn new(host: &Host, provider: &Instance) -> Self {
        host.check(provider).unwrap();
        let state = Arc::new(Mutex::new(CollectionState {
            next: 0,
            next_generation: 1,
            next_reader: 0,
            next_subscription: 0,
            replica: Replica::new(1),
            generations: BTreeMap::new(),
            contributors: BTreeSet::new(),
            subscriptions: BTreeMap::new(),
        }));
        let cleanup = state.clone();
        host.own(
            provider,
            Box::new(move || {
                let mut state = cleanup.lock().unwrap();
                let next = state.replica.incarnation() + 1;
                state.replica.reset(next);
                state.generations.clear();
                state.contributors.clear();
                // The collection is gone wholesale; advancing the incarnation resets every
                // downstream replica in one transition and closing the senders ends the streams.
                state.publish(&Batch::new(next));
                state.subscriptions.clear();
            }),
        )
        .unwrap();
        Self {
            host: host.clone(),
            provider: provider.clone(),
            state,
        }
    }
    pub fn reader(&self, consumer: &Instance) -> CollectionReader<T> {
        self.host.check(consumer).unwrap();
        self.host.bind_dependency(consumer, &self.provider).unwrap();
        let mut state = self.state.lock().unwrap();
        state.next_reader += 1;
        let reader_id = state.next_reader;
        assert!(
            state
                .subscriptions
                .insert(reader_id, BTreeMap::new())
                .is_none()
        );
        let cleanup = self.state.clone();
        self.host
            .own(
                consumer,
                Box::new(move || {
                    cleanup.lock().unwrap().subscriptions.remove(&reader_id);
                }),
            )
            .unwrap();
        CollectionReader {
            collection: self.clone(),
            consumer: consumer.clone(),
            reader_id,
        }
    }
    pub fn writer(&self, contributor: &Instance) -> Result<CollectionWriter<T>, AccessError> {
        self.host.bind_dependency(contributor, &self.provider)?;
        Ok(CollectionWriter {
            collection: self.clone(),
            contributor: contributor.clone(),
        })
    }
}

impl<T: Clone + Send + Sync + 'static> CollectionReader<T> {
    pub fn snapshot(&self) -> Result<BTreeMap<u64, T>, AccessError> {
        self.collection.host.check(&self.consumer)?;
        self.collection.host.check(&self.collection.provider)?;
        Ok(self.collection.state.lock().unwrap().snapshot())
    }
    /// Open a stream of this collection's batches. The bootstrap batch and the channel are
    /// wired under one lock, so the stream is self-synchronizing: no change can slip between
    /// "what I read" and "what I am listening to".
    pub fn subscribe(&self) -> Result<CollectionSubscription<T>, AccessError> {
        self.collection.host.check(&self.consumer)?;
        self.collection.host.check(&self.collection.provider)?;
        let mut state = self.collection.state.lock().unwrap();
        let (sender, receiver) = mpsc::unbounded_channel();
        state.next_subscription += 1;
        let subscription = state.next_subscription;
        let initial = state.snapshot_batch();
        // The receiver is held by this stack frame, so this send cannot fail.
        sender
            .send(initial)
            .expect("subscription receiver is held by this stack frame");
        let subscriptions = state
            .subscriptions
            .get_mut(&self.reader_id)
            .ok_or(AccessError::Revoked)?;
        assert!(subscriptions.insert(subscription, sender).is_none());
        Ok(CollectionSubscription {
            receiver,
            state: Arc::downgrade(&self.collection.state),
            reader_id: self.reader_id,
            id: subscription,
        })
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> CollectionReader<T> {
    /// Open a local read-only [`View`] over this reader's stream, with the initial snapshot
    /// batch already applied under one lock acquisition so there is no read-then-subscribe
    /// gap. The view is wired to the host collection as its upstream, so demand it issues is
    /// answered by the authority instead of by itself; it has no write path, because
    /// contributing goes through a [`CollectionWriter`] bound to a contributor.
    pub fn view(&self) -> Result<CollectionView<T>, AccessError> {
        let subscription = self.subscribe()?;
        let (view, input) = View::reader(
            OverlayPolicy::Local,
            1,
            Box::new(HostUpstream {
                state: self.collection.state.clone(),
                reader_id: self.reader_id,
                subscription_id: subscription.id,
            }),
        );
        let mut bound = CollectionView {
            view,
            input,
            subscription,
        };
        bound.pump();
        Ok(bound)
    }
}

impl<T: Clone + Send + Sync + 'static> CollectionWriter<T> {
    pub fn insert(&self, value: T) -> Result<Entry<T>, AccessError> {
        self.collection.host.check(&self.contributor)?;
        self.collection.host.check(&self.collection.provider)?;
        let mut state = self.collection.state.lock().unwrap();
        let contributor_id = self.contributor.id;
        if !state.contributors.contains(&contributor_id) {
            let cleanup = self.collection.state.clone();
            // One owner cleanup per contributor: when the contributor closes, every live
            // entry it still holds is withdrawn in a single atomic batch.
            self.collection.host.own(
                &self.contributor,
                Box::new(move || {
                    let mut state = cleanup.lock().unwrap();
                    state.contributors.remove(&contributor_id);
                    let removed = state.replica.plan_remove_owner(ScopeId(contributor_id));
                    for delta in &removed {
                        if let BaseDelta::Remove { key, .. } = delta {
                            state.generations.remove(key);
                        }
                    }
                    state.apply_and_publish(removed);
                }),
            )?;
            state.contributors.insert(contributor_id);
        }
        state.next += 1;
        let id = state.next;
        state.next_generation += 1;
        let generation = state.next_generation;
        assert!(state.generations.insert(id, generation).is_none());
        let delta = state
            .replica
            .plan_put(&id, Some(ScopeId(contributor_id)), value);
        state.apply_and_publish(vec![delta]);
        Ok(Entry {
            writer: self.clone(),
            id,
            generation,
        })
    }
}

impl<T: Clone + Send + Sync + 'static> Entry<T> {
    /// The collection-scoped id of this entry.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn update(&self, value: T) -> Result<(), AccessError> {
        self.writer
            .collection
            .host
            .check(&self.writer.contributor)?;
        self.writer
            .collection
            .host
            .check(&self.writer.collection.provider)?;
        let mut state = self.writer.collection.state.lock().unwrap();
        if state.generations.get(&self.id) != Some(&self.generation) {
            return Err(AccessError::Revoked);
        }
        let delta =
            state
                .replica
                .plan_put(&self.id, Some(ScopeId(self.writer.contributor.id)), value);
        state.apply_and_publish(vec![delta]);
        Ok(())
    }

    /// Withdraw this entry. A lease is once-only in spirit but idempotent in effect (ADR 015
    /// §6): removing an entry that is already gone — withdrawn here, revoked by owner closure,
    /// or never present — is not an error, because an entry's removal is always the desired
    /// end state. Removal while the writer's contributor or the provider is closed is also
    /// allowed: cleanup must not require live authority. Only this exact (id, generation)
    /// incarnation is withdrawn; a stale lease never removes a replacement.
    pub fn remove(self) {
        self.release();
    }

    /// Withdraw without consuming, for shared handles. Same idempotent semantics as `remove`.
    pub fn release(&self) {
        let mut state = self.writer.collection.state.lock().unwrap();
        if state.generations.get(&self.id) == Some(&self.generation) {
            state.generations.remove(&self.id);
            let delta = state.replica.plan_remove(&self.id);
            state.apply_and_publish(vec![delta]);
        }
    }
}

impl<T: Clone + Send + Sync + 'static> CollectionSubscription<T> {
    fn is_live(&self) -> bool {
        self.state.upgrade().is_some_and(|state| {
            state
                .lock()
                .unwrap()
                .subscriptions
                .get(&self.reader_id)
                .is_some_and(|subscriptions| subscriptions.contains_key(&self.id))
        })
    }

    pub async fn recv(&mut self) -> Option<Batch<u64, T>> {
        if !self.is_live() {
            return None;
        }
        let batch = self.receiver.recv().await;
        if self.is_live() { batch } else { None }
    }

    pub fn try_recv(&mut self) -> Result<Batch<u64, T>, mpsc::error::TryRecvError> {
        if !self.is_live() {
            return Err(mpsc::error::TryRecvError::Disconnected);
        }
        let batch = self.receiver.try_recv();
        if self.is_live() {
            batch
        } else {
            Err(mpsc::error::TryRecvError::Disconnected)
        }
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> CollectionView<T> {
    /// The bound view. Queries answer from what has been pumped in so far.
    pub fn view(&self) -> &View<u64, T, ReadOnly> {
        &self.view
    }

    /// Apply every batch that has arrived, returning how many were folded in. Synchronous
    /// callers pump between reads; a component pumps from its render loop.
    pub fn pump(&mut self) -> usize {
        let mut applied = 0;
        loop {
            match self.subscription.try_recv() {
                Ok(batch) => {
                    self.input.apply(batch);
                    applied += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.view.close();
                    break;
                }
            }
        }
        applied
    }

    pub async fn advance(&mut self) -> bool {
        match self.subscription.recv().await {
            Some(batch) => {
                self.input.apply(batch);
                self.pump();
                true
            }
            None => {
                self.view.close();
                false
            }
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Clone for HostedCollection<T> {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            provider: self.provider.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Drop for CollectionSubscription<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.lock().unwrap().prune(self.reader_id, self.id);
        }
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> Drop for CollectionView<T> {
    fn drop(&mut self) {
        self.view.close();
    }
}

// ── Internals: the collection state machine and the view's upstream bridge ───

impl<T: Clone> CollectionState<T> {
    fn snapshot(&self) -> BTreeMap<u64, T> {
        self.replica
            .rows()
            .map(|(&id, entry)| (id, entry.value.clone()))
            .collect()
    }
    fn base_rows(&self) -> Vec<BaseDelta<u64, T>> {
        self.replica
            .rows()
            .map(|(&key, entry)| BaseDelta::Upsert {
                key,
                version: entry.version,
                value: entry.value.clone(),
            })
            .collect()
    }
    /// Everything the authority holds, as one batch. Carries no coverage: a snapshot a
    /// reader opens on is not an answer to a demand the reader never issued.
    fn snapshot_batch(&self) -> Batch<u64, T> {
        Batch {
            incarnation: self.replica.incarnation(),
            base: self.base_rows(),
            overlays: Vec::new(),
            settled: Vec::new(),
            covered: Vec::new(),
        }
    }
    fn apply_and_publish(&mut self, base: Vec<BaseDelta<u64, T>>) {
        if base.is_empty() {
            return;
        }
        let incarnation = self.replica.incarnation();
        self.replica.apply(incarnation, &base);
        let mut batch = Batch::new(incarnation);
        batch.base = base;
        self.publish(&batch);
    }
    fn publish(&mut self, batch: &Batch<u64, T>) {
        let mut dead = Vec::new();
        for (&reader, subscriptions) in &self.subscriptions {
            for (&subscription, sender) in subscriptions {
                if sender.send(batch.clone()).is_err() {
                    dead.push((reader, subscription));
                }
            }
        }
        for (reader, subscription) in dead {
            self.prune(reader, subscription);
        }
    }
    fn prune(&mut self, reader: u64, subscription: u64) {
        if let Some(subscriptions) = self.subscriptions.get_mut(&reader) {
            subscriptions.remove(&subscription);
        }
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> Upstream<u64, T> for HostUpstream<T> {
    fn attach(&self, _downstream: &Downstream<u64, T>) {}

    fn demand(&self, _token: DemandToken, id: DemandId) {
        let mut state = self.state.lock().unwrap();
        let mut batch = Batch::new(state.replica.incarnation());
        batch.covered.push(id);
        if let Some(sender) = state
            .subscriptions
            .get(&self.reader_id)
            .and_then(|subscriptions| subscriptions.get(&self.subscription_id))
            && sender.send(batch).is_err()
        {
            state.prune(self.reader_id, self.subscription_id);
        }
    }

    fn release(&self, _id: DemandId) {}

    fn write(&self, _id: OperationId, _key: u64, _value: Option<T>, _writer: ScopeId) {
        unreachable!("a read-only view has no write path; contribute through a CollectionWriter")
    }
}
