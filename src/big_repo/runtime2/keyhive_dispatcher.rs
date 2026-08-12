//! Keyhive change notification dispatcher.
//!
//! One dispatcher owns the fan-out of payload-free keyhive change hints to
//! subscribed peers. It replaces the per-subscription broadcast/drain tasks:
//! each change batch is classified once (against the published cache
//! snapshot) and enqueued per destination peer in a [`KeyedBatcher`] with a
//! [`DebouncePolicy`], so bursts collapse into a single wake-up per peer and
//! continuous traffic cannot starve delivery.
//!
//! The dispatcher owns no clocks: the surrounding task supplies `now` and
//! performs delivery. Notifications are lossy hints — reconnect's initial
//! pull repairs missed hints.

use crate::handler::BigRepoKeyhiveProtocol;
use crate::interlude::*;
use crate::rpc::KeyhiveChangedRpcEvent;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use subduction_keyhive::{KeyhivePeerId, message::EventHash};
use utils_rs::batching::{DebouncePolicy, KeyedBatcher};
use uuid::Uuid;

/// A change event reported to the dispatcher.
///
/// `source` is the peer the events were learned from (`None` for locally
/// created events); it is excluded from that batch's conservative fallback.
pub(crate) struct KeyhiveChangeEvent {
    pub hashes: Vec<EventHash>,
    pub source: Option<KeyhivePeerId>,
}

/// The accumulated per-peer notification payload. Payload-free on the wire;
/// the flag feeds counters and traces.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingKeyhiveNotification {
    /// Whether an unclassified (pending/lagged) change forced this wake-up.
    pub conservative: bool,
}

#[derive(Clone)]
pub(crate) struct SubscriptionEntry {
    pub id: Uuid,
    pub tx: irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
}

pub(crate) type SubscriptionMap = Arc<surelock::mutex::Mutex<HashMap<PeerId, SubscriptionEntry>>>;

/// Producer-side handle to the dispatcher task.
#[derive(Clone)]
pub(crate) struct KeyhiveChangeDispatcher {
    events_tx: tokio::sync::mpsc::Sender<KeyhiveChangeEvent>,
    subscriptions: SubscriptionMap,
    overflow: Arc<AtomicBool>,
}

impl KeyhiveChangeDispatcher {
    /// Report a batch of newly incorporated event hashes.
    ///
    /// Fire-and-forget: a full channel marks overflow and triggers a
    /// conservative wake-up once congestion clears.
    pub(crate) fn report(&self, hashes: Vec<EventHash>, source: Option<KeyhivePeerId>) {
        if hashes.is_empty() {
            return;
        }
        if let Err(err) = self
            .events_tx
            .try_send(KeyhiveChangeEvent { hashes, source })
        {
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    self.overflow.store(true, Ordering::Release);
                    warn_loc!("keyhive change hint dropped: dispatcher channel full");
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    // The dispatcher task must outlive every sender (reverse
                    // shutdown: the boot constructs the channel and the
                    // dispatcher is its child). A closed channel here is an
                    // invariant break.
                    panic!(
                        "{ERROR_CHANNEL}: keyhive change dispatcher channel closed while senders alive"
                    );
                }
            }
        }
    }

    /// Register a peer's notification stream. The peer immediately receives
    /// the initial confirmation event. Returns the subscription ID.
    pub(crate) async fn subscribe(
        &self,
        peer_id: PeerId,
        tx: irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
    ) -> Uuid {
        let sub_id = Uuid::new_v4();
        surelock::key::lock_scope(|key| {
            let (mut subscriptions, _key) = key.lock(&self.subscriptions);
            subscriptions.insert(
                peer_id,
                SubscriptionEntry {
                    id: sub_id,
                    tx: tx.clone(),
                },
            );
        });
        if tx
            .send(KeyhiveChangedRpcEvent { initial: true })
            .await
            .is_err()
        {
            // The peer's stream closed before the initial event landed —
            // transient work cancellation (peer disconnected), not an
            // invariant break. Warn and remove the dead subscription so
            // the dispatcher never delivers to it.
            warn_loc!(
                ERROR_CALLER,
                "subscriber stream closed on initial event; removing subscription"
            );
            surelock::key::lock_scope(|key| {
                let (mut subscriptions, _key) = key.lock(&self.subscriptions);
                if let Some(entry) = subscriptions.get(&peer_id)
                    && entry.id == sub_id
                {
                    subscriptions.remove(&peer_id);
                }
            });
        }
        sub_id
    }

    /// Unregister a peer's notification stream for the given subscription ID.
    pub(crate) async fn unsubscribe(&self, peer_id: &PeerId, sub_id: Uuid) {
        surelock::key::lock_scope(|key| {
            let (mut subscriptions, _key) = key.lock(&self.subscriptions);
            if let Some(entry) = subscriptions.get(peer_id)
                && entry.id == sub_id
            {
                subscriptions.remove(peer_id);
            }
        });
    }
}

/// Spawn the dispatcher task.
///
/// The caller creates the events channel and passes both ends: `events_tx`
/// feeds the post-ingestion change reporter (remote events) and
/// `KeyhiveChangeNotifier::note_local_keyhive_changed` (local events);
/// `events_rx` is drained by the task.
pub(crate) fn spawn_keyhive_dispatcher(
    protocol: BigRepoKeyhiveProtocol,
    events_tx: tokio::sync::mpsc::Sender<KeyhiveChangeEvent>,
    events_rx: tokio::sync::mpsc::Receiver<KeyhiveChangeEvent>,
    subscriptions: SubscriptionMap,
    overflow: Arc<AtomicBool>,
    policy: DebouncePolicy,
) -> KeyhiveChangeDispatcher {
    let handle = KeyhiveChangeDispatcher {
        events_tx,
        subscriptions: Arc::clone(&subscriptions),
        overflow: Arc::clone(&overflow),
    };
    tokio::spawn(async move {
        run_dispatcher(events_rx, protocol, subscriptions, overflow, policy).await;
    });
    handle
}

async fn run_dispatcher(
    mut events_rx: tokio::sync::mpsc::Receiver<KeyhiveChangeEvent>,
    protocol: BigRepoKeyhiveProtocol,
    subscriptions: SubscriptionMap,
    overflow: Arc<AtomicBool>,
    policy: DebouncePolicy,
) {
    let mut batcher: KeyedBatcher<PeerId, PendingKeyhiveNotification, DebouncePolicy> =
        KeyedBatcher::new(
            policy,
            |_: &PendingKeyhiveNotification| 0,
            |acc, next| {
                acc.conservative |= next.conservative;
            },
        );
    loop {
        if overflow.swap(false, Ordering::AcqRel) {
            let connected: Vec<PeerId> = surelock::key::lock_scope(|key| {
                let (subs, _key) = key.lock(&subscriptions);
                subs.keys().copied().collect()
            });
            let now = Instant::now();
            for peer_id in connected {
                batcher.push(
                    now,
                    peer_id,
                    PendingKeyhiveNotification { conservative: true },
                );
            }
        }
        let deadline = batcher.next_deadline();
        let sleep = match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()),
            None => {
                tokio::time::sleep_until(tokio::time::Instant::now() + Duration::from_secs(3600))
            }
        };
        tokio::select! {
            evt = events_rx.recv() => {
                match evt {
                    Some(evt) => {
                        classify_and_enqueue(&mut batcher, &protocol, &subscriptions, evt).await;
                    }
                    None => break,
                }
            }
            _ = sleep => {
                let now = Instant::now();
                let due = batcher.take_due(now);
                deliver(&subscriptions, due).await;
            }
        }
    }
}

/// Classify one change batch against the published cache snapshot and enqueue
/// the selected peers in the batcher.
async fn classify_and_enqueue(
    batcher: &mut KeyedBatcher<PeerId, PendingKeyhiveNotification, DebouncePolicy>,
    protocol: &BigRepoKeyhiveProtocol,
    subscriptions: &SubscriptionMap,
    evt: KeyhiveChangeEvent,
) {
    let now = Instant::now();
    let connected: BTreeSet<KeyhivePeerId> = surelock::key::lock_scope(|key| {
        let (subs, _key) = key.lock(subscriptions);
        subs.keys()
            .map(|peer| KeyhivePeerId::from_bytes(*peer.as_bytes()))
            .collect()
    });
    if connected.is_empty() {
        return;
    }
    let changed: BTreeSet<EventHash> = evt.hashes.iter().copied().collect();
    let targets = match protocol
        .notification_targets(subduction_keyhive::VisibilityBatch {
            connected: &connected,
            changed: &changed,
        })
        .await
    {
        Ok(targets) => targets,
        Err(error) => {
            // Classification failed (e.g. cache refresh error): fall back to
            // the conservative branch — wake every connected peer except the
            // source. Notifications are lossy hints; a missed classification
            // must not silently drop a wake-up.
            tracing::warn!(%error, "keyhive notification classification failed; conservative fallback");
            for peer in &connected {
                if Some(peer) != evt.source.as_ref() {
                    let peer_id = PeerId::new(*peer.verifying_key());
                    batcher.push(
                        now,
                        peer_id,
                        PendingKeyhiveNotification { conservative: true },
                    );
                }
            }
            return;
        }
    };
    for peer in &targets.peers {
        if Some(peer) != evt.source.as_ref() {
            let peer_id = PeerId::new(*peer.verifying_key());
            batcher.push(now, peer_id, PendingKeyhiveNotification::default());
        }
    }
    if !targets.unclassified.is_empty() {
        // Pending/lagged hashes: conservatively wake every connected peer
        // except the known source.
        for peer in &connected {
            if Some(peer) != evt.source.as_ref() {
                let peer_id = PeerId::new(*peer.verifying_key());
                batcher.push(
                    now,
                    peer_id,
                    PendingKeyhiveNotification { conservative: true },
                );
            }
        }
    }
}

/// Deliver due notifications, dropping subscriptions whose stream closed.
async fn deliver(subscriptions: &SubscriptionMap, due: Vec<(PeerId, PendingKeyhiveNotification)>) {
    if due.is_empty() {
        return;
    }
    let targets: Vec<(
        PeerId,
        Uuid,
        irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
        PendingKeyhiveNotification,
    )> = surelock::key::lock_scope(|key| {
        let (subs, _key) = key.lock(subscriptions);
        due.into_iter()
            .filter_map(|(peer_id, notif)| {
                subs.get(&peer_id)
                    .map(|entry| (peer_id, entry.id, entry.tx.clone(), notif))
            })
            .collect()
    });
    let mut failed = Vec::new();
    for (peer_id, sub_id, tx, notification) in targets {
        if tx
            .send(KeyhiveChangedRpcEvent { initial: false })
            .await
            .is_err()
        {
            failed.push((peer_id, sub_id));
        } else {
            tracing::debug!(
                %peer_id,
                conservative = notification.conservative,
                "keyhive change notification delivered"
            );
        }
    }
    if !failed.is_empty() {
        surelock::key::lock_scope(|key| {
            let (mut subs, _key) = key.lock(subscriptions);
            for (peer_id, sub_id) in failed {
                if let Some(entry) = subs.get(&peer_id)
                    && entry.id == sub_id
                {
                    subs.remove(&peer_id);
                    debug!(%peer_id, "keyhive notification stream closed; unsubscribed");
                }
            }
        });
    }
}
