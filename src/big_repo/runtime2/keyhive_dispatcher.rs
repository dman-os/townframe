//! Keyhive change notification dispatcher.
//!
//! One dispatcher owns the fan-out of payload-free keyhive change hints to
//! subscribed peers. Its truth source is the durable admission log
//! ([`SqliteBigRepoStore::admission_events_after`]), tailed with an in-memory
//! cursor: every incorporated event is classified and fanned out exactly
//! once, in order, regardless of channel races or restarts. The live change
//! channel (`report` / `try_send_change_event`) is only a wake-up hint whose
//! loss costs latency bounded by [`ADMISSION_IDLE_POLL`], never a hint.
//!
//! Each admitted batch is classified once against the published visibility
//! cache and enqueued per destination peer in a [`KeyedBatcher`] with a
//! [`DebouncePolicy`], so bursts collapse into a single wake-up per peer.
//!
//! Classified hashes wake exactly the peers whose visibility covers them.
//! Unclassified hashes — events the visibility projection does not attribute
//! to any viewer (contact-card prekey ops are the known case) — fall back to
//! waking every connected peer except the source, matching their
//! broadcast-like audience. Only classification *errors* hold the cursor for
//! retry; nothing is ever silently skipped.

use crate::handler::BigRepoKeyhiveProtocol;
use crate::interlude::*;
use crate::rpc::KeyhiveChangedRpcEvent;
use crate::store::sqlite::SqliteBigRepoStore;
use std::time::{Duration, Instant};
use subduction_keyhive::{KeyhivePeerId, message::EventHash};
use utils_rs::batching::{DebouncePolicy, KeyedBatcher};
use uuid::Uuid;

/// Ceiling on how long the admission log can go unpolled when all live hints
/// are lost. Hints make steady-state latency negligible; this only bounds the
/// recovery window. Kept tight because several consumers (stress bootstrap
/// membership propagation) are notification-driven.
const ADMISSION_IDLE_POLL: Duration = Duration::from_millis(250);

/// Maximum admission rows classified per poll.
const ADMISSION_BATCH: u32 = 256;

/// A change hint reported by producers. The durable log already carries the
/// hashes; this only accelerates the next poll.
pub(crate) struct KeyhiveChangeEvent {
    #[allow(dead_code)]
    pub hashes: Vec<EventHash>,
    #[allow(dead_code)]
    pub source: Option<KeyhivePeerId>,
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
    // Keeps the hint channel open without creating protocol/task reference cycles.
    _events_tx: tokio::sync::mpsc::Sender<KeyhiveChangeEvent>,
    subscriptions: SubscriptionMap,
}

pub(crate) fn try_send_change_event(
    events_tx: &tokio::sync::mpsc::Sender<KeyhiveChangeEvent>,
    hashes: Vec<EventHash>,
    source: Option<KeyhivePeerId>,
) {
    if hashes.is_empty() {
        return;
    }
    if let Err(err) = events_tx.try_send(KeyhiveChangeEvent { hashes, source }) {
        match err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                tracing::debug!(
                    "keyhive change hint dropped: dispatcher channel full; admission tail will catch up"
                );
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                panic!(
                    "{ERROR_CHANNEL}: keyhive change dispatcher channel closed while senders alive"
                );
            }
        }
    }
}

impl KeyhiveChangeDispatcher {
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
/// The caller creates the events channel and passes both ends. The protocol's
/// durable-incorporation hook feeds it for both local and remote events;
/// `events_rx` is drained by the task as wake-up hints.
pub(crate) fn spawn_keyhive_dispatcher(
    protocol: BigRepoKeyhiveProtocol,
    store: SqliteBigRepoStore,
    events_tx: tokio::sync::mpsc::Sender<KeyhiveChangeEvent>,
    events_rx: tokio::sync::mpsc::Receiver<KeyhiveChangeEvent>,
    subscriptions: SubscriptionMap,
    policy: DebouncePolicy,
) -> (KeyhiveChangeDispatcher, tokio::task::JoinHandle<()>) {
    let handle = KeyhiveChangeDispatcher {
        _events_tx: events_tx,
        subscriptions: Arc::clone(&subscriptions),
    };
    let join_handle = tokio::spawn(async move {
        run_dispatcher(events_rx, protocol, store, subscriptions, policy).await;
    });
    (handle, join_handle)
}

async fn run_dispatcher(
    mut events_rx: tokio::sync::mpsc::Receiver<KeyhiveChangeEvent>,
    protocol: BigRepoKeyhiveProtocol,
    store: SqliteBigRepoStore,
    subscriptions: SubscriptionMap,
    policy: DebouncePolicy,
) {
    let mut batcher: KeyedBatcher<PeerId, (), DebouncePolicy> =
        KeyedBatcher::new(policy, |_: &()| 0, |(), ()| {});
    // Boot at the current head: pre-boot incorporations are covered by each
    // subscriber's initial pull, same as before the admission tail existed.
    let mut cursor = store.admission_head().await.expect(
        "admission head must be readable at dispatcher boot; storage failures are fatal here",
    );
    loop {
        // Tail the durable admission log until dry, blocked, or failing.
        loop {
            let rows = match store.admission_events_after(cursor, ADMISSION_BATCH).await {
                Ok(rows) => rows,
                Err(error) => {
                    warn_loc!(%error, "admission log tail failed; retrying");
                    break;
                }
            };
            if rows.is_empty() {
                break;
            }
            if classify_rows(&mut batcher, &protocol, &subscriptions, &rows).await {
                cursor = rows.last().map(|row| row.seq).unwrap_or(cursor);
            }
            // On classification error the cursor stays put: the same rows are
            // retried after whatever storage hiccup cleared.
            if rows.len() < ADMISSION_BATCH as usize {
                break;
            }
        }

        let deadline = tokio::time::Instant::from_std(
            batcher
                .next_deadline()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600))
                .min(Instant::now() + ADMISSION_IDLE_POLL),
        );
        tokio::select! {
            evt = events_rx.recv() => {
                match evt {
                    // Wake-up hint only: the loop re-tails the durable log.
                    Some(_) => {}
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let due = batcher.take_due(Instant::now());
                deliver(&subscriptions, due).await;
            }
        }
    }
}

/// Classify one admitted batch and enqueue the selected peers for debounced
/// delivery. Rows are grouped by source so the originating peer is never
/// woken about its own events.
///
/// Returns `true` when the caller may advance its cursor past the batch:
/// every hash either classified to explicit targets or fell back to the
/// conservative wake (unattributable events such as contact-card prekey ops,
/// whose audience is effectively everyone connected). Returns `false` —
/// holding the cursor — only when classification errored (transient IO;
/// the batch is retried).
async fn classify_rows(
    batcher: &mut KeyedBatcher<PeerId, (), DebouncePolicy>,
    protocol: &BigRepoKeyhiveProtocol,
    subscriptions: &SubscriptionMap,
    rows: &[crate::store::sqlite::AdmissionEventRow],
) -> bool {
    let connected: BTreeSet<KeyhivePeerId> = surelock::key::lock_scope(|key| {
        let (subs, _key) = key.lock(subscriptions);
        subs.keys()
            .map(|peer| KeyhivePeerId::from_bytes(*peer.as_bytes()))
            .collect()
    });
    if connected.is_empty() {
        return true;
    }

    // Group hashes by learning source for echo suppression.
    let mut grouped: BTreeMap<Option<KeyhivePeerId>, BTreeSet<EventHash>> = BTreeMap::new();
    for row in rows {
        let source = row
            .source_id
            .as_deref()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .map(KeyhivePeerId::from_bytes);
        grouped.entry(source).or_default().insert(row.event_hash);
    }

    let now = Instant::now();
    for (source, changed) in grouped {
        let targets = match protocol
            .notification_targets(subduction_keyhive::VisibilityBatch {
                connected: &connected,
                changed: &changed,
            })
            .await
        {
            Ok(targets) => targets,
            Err(error) => {
                warn_loc!(%error, "keyhive notification classification failed; retrying");
                return false;
            }
        };
        // Unattributable hashes (prekey/contact-card ops) wake everyone:
        // the visibility projection has no narrower audience for them.
        let unattributed = !targets.unclassified.is_empty();
        for peer in &connected {
            let is_source = Some(peer) == source.as_ref();
            let selected = targets.peers.contains(peer) || (unattributed && !is_source);
            if selected {
                let peer_id = PeerId::new(*peer.verifying_key());
                batcher.push(now, peer_id, ());
            }
        }
    }
    true
}

/// Deliver due notifications concurrently, dropping subscriptions whose stream closed.
async fn deliver(subscriptions: &SubscriptionMap, due: Vec<(PeerId, ())>) {
    if due.is_empty() {
        return;
    }
    let targets: Vec<(
        PeerId,
        Uuid,
        irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
    )> = surelock::key::lock_scope(|key| {
        let (subs, _key) = key.lock(subscriptions);
        due.into_iter()
            .filter_map(|(peer_id, ())| {
                subs.get(&peer_id)
                    .map(|entry| (peer_id, entry.id, entry.tx.clone()))
            })
            .collect()
    });
    let delivery_futures = targets.into_iter().map(|(peer_id, sub_id, tx)| async move {
        if tx
            .send(KeyhiveChangedRpcEvent { initial: false })
            .await
            .is_err()
        {
            Some((peer_id, sub_id))
        } else {
            tracing::debug!(%peer_id, "keyhive change notification delivered");
            None
        }
    });
    let results = futures::future::join_all(delivery_futures).await;
    let failed: Vec<(PeerId, Uuid)> = results.into_iter().flatten().collect();
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
