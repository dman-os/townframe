//! Keyhive change notification dispatcher.
//!
//! One dispatcher owns the fan-out of payload-free keyhive change hints to
//! subscribed peers. Its truth source is the durable admission log
//! ([`SqliteBigRepoStore::admission_events_after`]), tailed with an in-memory
//! cursor: every incorporated event is classified and fanned out exactly
//! once, in order, regardless of channel races or restarts. The live change
//! wake-up notifier; the shared reader bounds recovery by its polling interval.
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
use crate::runtime2::{Timer, keyhive_admission};
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use std::time::Instant;
use subduction_keyhive::{KeyhivePeerId, message::EventHash};
use utils_rs::batching::{DebouncePolicy, KeyedBatcher};
use uuid::Uuid;

/// Maximum admission rows classified per reader call.
const ADMISSION_BATCH: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(256).expect("literal is non-zero");

#[derive(Clone)]
pub(crate) struct SubscriptionEntry {
    pub id: Uuid,
    pub tx: irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
}

pub(crate) type SubscriptionMap = Arc<surelock::mutex::Mutex<HashMap<PeerId, SubscriptionEntry>>>;

/// Producer-side handle to the dispatcher task.
#[derive(Clone)]
pub(crate) struct KeyhiveChangeDispatcher {
    subscriptions: SubscriptionMap,
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

/// Stop token for the dispatcher task.
#[derive(Clone)]
pub(crate) struct KeyhiveDispatcherStopToken {
    abort: futures::future::AbortHandle,
}

impl KeyhiveDispatcherStopToken {
    pub(crate) fn cancel(&self) {
        self.abort.abort();
    }
}

/// The dispatcher task, ready to be spawned on a runtime task set.
///
/// The caller owns the [`run`](Self::run) future (spawned on its task set so
/// it is joined on shutdown) and the [`stop`](Self::stop) token (cancelled
/// before the task set is aborted, matching the other runtime2 workers).
pub(crate) struct SpawnedKeyhiveDispatcher<F: FutureForm> {
    pub(crate) stop: KeyhiveDispatcherStopToken,
    pub(crate) run: F::Future<'static, eyre::Result<()>>,
}

/// TEMP-DIAGNOSTIC: `DAYB_KEYHIVE_DIAG` gates the dispatcher's per-batch
/// instrumentation warns (classification outcome, dropped notifications).
fn dispatch_diag() -> bool {
    static DIAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DIAG.get_or_init(|| std::env::var_os("DAYB_KEYHIVE_DIAG").is_some())
}

/// Spawn the dispatcher task.
///
/// The caller creates the events channel and passes both ends. The protocol's
/// durable-incorporation hook feeds it for both local and remote events;
/// The caller supplies a wake-up notifier. The durable admission log is the
/// source of truth; notification payloads are never carried in memory.
pub(crate) fn spawn_keyhive_dispatcher(
    protocol: BigRepoKeyhiveProtocol,
    store: SqliteBigRepoStore,
    timer: Arc<dyn Timer<Sendable>>,
    notify: Arc<tokio::sync::Notify>,
    subscriptions: SubscriptionMap,
    policy: DebouncePolicy,
) -> (KeyhiveChangeDispatcher, SpawnedKeyhiveDispatcher<Sendable>) {
    let handle = KeyhiveChangeDispatcher {
        subscriptions: Arc::clone(&subscriptions),
    };
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();
    let run = Sendable::from_future(async move {
        let fut = run_dispatcher(notify, protocol, store, timer, subscriptions, policy);
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });
    (
        handle,
        SpawnedKeyhiveDispatcher {
            stop: KeyhiveDispatcherStopToken {
                abort: abort_handle,
            },
            run,
        },
    )
}

async fn run_dispatcher(
    notify: Arc<tokio::sync::Notify>,
    protocol: BigRepoKeyhiveProtocol,
    store: SqliteBigRepoStore,
    timer: Arc<dyn Timer<Sendable>>,
    subscriptions: SubscriptionMap,
    policy: DebouncePolicy,
) -> Res<()> {
    let mut batcher: KeyedBatcher<PeerId, (), DebouncePolicy> =
        KeyedBatcher::new(policy, |_: &()| 0, |(), ()| {});
    // Boot at the current head: pre-boot incorporations are covered by each
    // subscriber's initial pull. The shared reader owns replay-boundary and
    // live-tail cursor mechanics; this worker owns only classification and
    // peer delivery.
    let source = keyhive_admission::Store {
        store: store.clone(),
        timer,
    };
    let mut reader = source.open((), store.admission_head().await?).await?;
    loop {
        let deadline = batcher.next_deadline().map(tokio::time::Instant::from_std);
        tokio::select! {
            read = reader.next(RevisionReadLimits { max_entries: ADMISSION_BATCH }) => {
                match read? {
                    RevisionRead::ReplayComplete { .. } => {}
                    RevisionRead::Entries { entries, .. } => {
                        classify_rows(&mut batcher, &protocol, &subscriptions, &entries).await?;
                    }
                }
            }
            _ = notify.notified() => {
                let due = batcher.take_due(Instant::now());
                deliver(&subscriptions, due).await;
            }
            _ = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
        let due = batcher.take_due(Instant::now());
        deliver(&subscriptions, due).await;
    }
}

/// Classify one admitted batch and enqueue the selected peers for debounced
/// delivery. Rows are grouped by source so the originating peer is never
/// woken about its own events.
///
/// Every hash either classifies to explicit targets or falls back to the
/// conservative wake (unattributable events such as contact-card prekey ops,
/// whose audience is effectively everyone connected). Errors propagate to the
/// caller: the dispatcher must not silently skip rows, so a classification
/// failure surfaces through the task's unwrap rather than being retried
/// forever.
async fn classify_rows(
    batcher: &mut KeyedBatcher<PeerId, (), DebouncePolicy>,
    protocol: &BigRepoKeyhiveProtocol,
    subscriptions: &SubscriptionMap,
    rows: &[keyhive_admission::AdmittedRow],
) -> Res<()> {
    let connected: BTreeSet<KeyhivePeerId> = surelock::key::lock_scope(|key| {
        let (subs, _key) = key.lock(subscriptions);
        subs.keys()
            .map(|peer| KeyhivePeerId::from_bytes(*peer.as_bytes()))
            .collect()
    });
    if connected.is_empty() {
        if dispatch_diag() {
            tracing::warn!(
                rows = rows.len(),
                "KEYHIVE_DISPATCH_DIAG classify skipped: no connected subscribers"
            );
        }
        return Ok(());
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
        let cached_started = Instant::now();
        let mut targets = protocol
            .notification_targets(subduction_keyhive::VisibilityBatch {
                connected: &connected,
                changed: &changed,
            })
            .await?;
        let cached_elapsed = cached_started.elapsed();
        if dispatch_diag() {
            let direct_started = Instant::now();
            let direct = protocol.all_agent_events(&BTreeSet::new()).await?;
            let direct_elapsed = direct_started.elapsed();
            let public_peer =
                KeyhivePeerId::from_identifier(&keyhive_core::principal::public::Public.id());
            let public_hit = direct
                .agent_hashes
                .get(&public_peer)
                .is_some_and(|visible| visible.intersection(&changed).next().is_some());
            let mut direct_peers = BTreeSet::new();
            if public_hit {
                direct_peers.extend(connected.iter().cloned());
            } else {
                for peer in &connected {
                    if direct
                        .agent_hashes
                        .get(peer)
                        .is_some_and(|visible| visible.intersection(&changed).next().is_some())
                    {
                        direct_peers.insert(peer.clone());
                    }
                }
            }
            let local_visible = direct.agent_hashes.get(&protocol.peer_id());
            let direct_unclassified = changed
                .iter()
                .filter(|hash| {
                    !direct
                        .agent_hashes
                        .get(&public_peer)
                        .is_some_and(|visible| visible.contains(*hash))
                        && !local_visible.is_some_and(|visible| visible.contains(*hash))
                })
                .copied()
                .collect::<BTreeSet<_>>();
            let after = protocol
                .notification_targets(subduction_keyhive::VisibilityBatch {
                    connected: &connected,
                    changed: &changed,
                })
                .await?;
            tracing::warn!(
                stable = targets.published_generation == after.published_generation,
                generation = after.published_generation,
                cached_peers = after.peers.len(),
                direct_peers = direct_peers.len(),
                cached_unclassified = after.unclassified.len(),
                direct_unclassified = direct_unclassified.len(),
                peers_equal = after.peers == direct_peers,
                unclassified_equal = after.unclassified == direct_unclassified,
                cached_elapsed_micros = cached_elapsed.as_micros(),
                direct_elapsed_micros = direct_elapsed.as_micros(),
                "KEYHIVE_DISPATCH_DIAG cache/direct comparison"
            );
            targets = after;
        }
        // Unattributable hashes (prekey/contact-card ops) wake everyone:
        // the visibility projection has no narrower audience for them.
        let unattributed = !targets.unclassified.is_empty();
        if dispatch_diag() {
            tracing::warn!(
                source = ?source,
                changed = changed.len(),
                peers = targets.peers.len(),
                unclassified = targets.unclassified.len(),
                connected = connected.len(),
                "KEYHIVE_DISPATCH_DIAG classify group"
            );
        }
        for peer in &connected {
            let is_source = Some(peer) == source.as_ref();
            let selected = targets.peers.contains(peer) || (unattributed && !is_source);
            if true || selected {
                let peer_id = PeerId::new(*peer.verifying_key());
                batcher.push(now, peer_id, ());
            }
        }
    }
    Ok(())
}

/// Deliver due notifications concurrently, dropping subscriptions whose stream closed.
async fn deliver(subscriptions: &SubscriptionMap, due: Vec<(PeerId, ())>) {
    if due.is_empty() {
        return;
    }
    if dispatch_diag() {
        let due_peers: Vec<String> = due.iter().map(|(peer, ())| peer.to_string()).collect();
        tracing::warn!(?due_peers, "KEYHIVE_DISPATCH_DIAG deliver batch");
    }
    let targets: Vec<(
        PeerId,
        Uuid,
        irpc::channel::mpsc::Sender<KeyhiveChangedRpcEvent>,
    )> =
        surelock::key::lock_scope(|key| {
            let (subs, _key) = key.lock(subscriptions);
            due.into_iter()
            .filter_map(|(peer_id, ())| {
                let found = subs.get(&peer_id).map(|entry| (peer_id, entry.id, entry.tx.clone()));
                if dispatch_diag() && found.is_none() {
                    tracing::warn!(
                        peer = %peer_id,
                        "KEYHIVE_DISPATCH_DIAG due peer has no subscription; notification dropped"
                    );
                }
                found
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
