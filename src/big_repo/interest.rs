use crate::interlude::*;
use crate::{BigDocHandle, BigRepo, DocumentId};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::watch;

const SYNC_INTEREST_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct DocInterest {
    repo: Arc<BigRepo>,
    doc_id: DocumentId,
    _lease: Arc<crate::runtime2::DocLease>,
    _presence: Arc<()>,
    usable: watch::Receiver<bool>,
}

impl DocInterest {
    pub fn document_id(&self) -> DocumentId {
        self.doc_id.clone()
    }

    pub fn readiness(&self) -> watch::Receiver<bool> {
        self.usable.clone()
    }

    pub async fn wait_until_ready(&self) -> Res<()> {
        let mut usable = self.readiness();
        usable
            .wait_for(|ready| *ready)
            .await
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        Ok(())
    }

    pub async fn ready(&self) -> Res<BigDocHandle> {
        self.wait_until_ready().await?;
        self.repo
            .get_doc(&self.doc_id)
            .await?
            .into_ready(self.doc_id.clone())
            .map_err(Into::into)
    }

    pub(crate) fn new(repo: Arc<BigRepo>, doc_id: DocumentId, interest: PendingDocLease) -> Self {
        Self {
            repo,
            doc_id,
            _lease: interest.lease,
            _presence: interest.presence,
            usable: interest.usable,
        }
    }
}

pub(crate) struct PendingDocLease {
    pub lease: Arc<crate::runtime2::DocLease>,
    pub presence: Arc<()>,
    pub usable: watch::Receiver<bool>,
}

#[derive(Clone)]
pub struct DocSyncDemand {
    docs: watch::Receiver<Arc<HashSet<DocumentId>>>,
}

pub struct DocSyncDemandDriver {
    future: futures::future::BoxFuture<'static, Res<()>>,
    applied: watch::Receiver<Option<Arc<HashSet<DocumentId>>>>,
}

impl DocSyncDemandDriver {
    pub fn applied_demand(&self) -> watch::Receiver<Option<Arc<HashSet<DocumentId>>>> {
        self.applied.clone()
    }
}

impl std::future::Future for DocSyncDemandDriver {
    type Output = Res<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

impl DocSyncDemand {
    pub fn documents(&self) -> Arc<HashSet<DocumentId>> {
        Arc::clone(&self.docs.borrow())
    }

    pub async fn changed(&mut self) -> Res<Arc<HashSet<DocumentId>>> {
        self.docs.changed().await.map_err(|_| ferr!(ERROR_ACTOR))?;
        Ok(Arc::clone(&self.docs.borrow_and_update()))
    }

    pub fn drive(
        mut self,
        source: big_sync::SubscriptionSource,
        mut peers: watch::Receiver<HashSet<PeerKey>>,
    ) -> DocSyncDemandDriver {
        let (applied_tx, applied) = watch::channel(None);
        DocSyncDemandDriver {
            applied,
            future: Box::pin(async move {
                let mut previous_docs = Arc::new(HashSet::new());
                let mut previous_peers = HashSet::new();
                loop {
                    let docs = Arc::clone(&self.docs.borrow_and_update());
                    let targets = peers.borrow_and_update().clone();
                    for peer in previous_peers.difference(&targets) {
                        source.remove_peer(peer.clone()).await?;
                    }
                    let added: HashMap<_, _> = docs
                        .difference(&previous_docs)
                        .cloned()
                        .map(|doc| (doc, Arc::from(BigRepo::BACKEND_ID)))
                        .collect();
                    let removed: HashSet<_> = previous_docs.difference(&docs).cloned().collect();
                    for peer in &targets {
                        let update = if previous_peers.contains(peer) {
                            big_sync::SubscriptionUpdate {
                                objects: added.clone(),
                                removed_objects: removed.clone(),
                                ..Default::default()
                            }
                        } else {
                            big_sync::SubscriptionUpdate {
                                objects: docs
                                    .iter()
                                    .cloned()
                                    .map(|doc| (doc, Arc::from(BigRepo::BACKEND_ID)))
                                    .collect(),
                                ..Default::default()
                            }
                        };
                        if !update.objects.is_empty() || !update.removed_objects.is_empty() {
                            source.update(peer.clone(), update).await?;
                        }
                    }
                    drop(applied_tx.send_replace(Some(Arc::clone(&docs))));
                    previous_docs = docs;
                    previous_peers = targets;
                    tokio::select! {
                        changed = self.docs.changed() => if changed.is_err() { return Ok(()); },
                        changed = peers.changed() => if changed.is_err() { return Ok(()); },
                    }
                }
            }),
        }
    }
}

pub(crate) struct SyncInterests {
    entries: Mutex<HashMap<DocumentId, InterestEntry>>,
    docs: watch::Sender<Arc<HashSet<DocumentId>>>,
    clock: Arc<dyn crate::runtime2::Clock>,
}

struct InterestEntry {
    callers: usize,
    expires: Option<Instant>,
}

pub(crate) struct SyncInterestLease {
    interests: Arc<SyncInterests>,
    doc_id: DocumentId,
}

impl SyncInterests {
    pub(crate) fn new(clock: Arc<dyn crate::runtime2::Clock>) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            docs: watch::channel(Arc::new(HashSet::new())).0,
            clock,
        })
    }

    pub(crate) fn subscribe(&self) -> DocSyncDemand {
        DocSyncDemand {
            docs: self.docs.subscribe(),
        }
    }

    pub(crate) fn acquire(self: &Arc<Self>, doc_id: DocumentId) -> SyncInterestLease {
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        let is_new = !entries.contains_key(&doc_id);
        let entry = entries.entry(doc_id.clone()).or_insert(InterestEntry {
            callers: 0,
            expires: None,
        });
        entry.callers += 1;
        entry.expires = None;
        if is_new {
            self.docs.send_modify(|docs| {
                assert!(Arc::make_mut(docs).insert(doc_id.clone()));
            });
        }
        SyncInterestLease {
            interests: Arc::clone(self),
            doc_id,
        }
    }

    pub(crate) fn expire(&self) {
        self.expire_at(self.clock.instant());
    }

    fn expire_at(&self, now: Instant) {
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        let expired: Vec<_> = entries
            .iter()
            .filter(|(_, entry)| entry.expires.is_some_and(|due| due <= now))
            .map(|(doc, _)| doc.clone())
            .collect();
        if !expired.is_empty() {
            for doc in &expired {
                entries.remove(doc);
            }
            self.docs.send_modify(|docs| {
                let docs = Arc::make_mut(docs);
                for doc in expired {
                    assert!(docs.remove(&doc));
                }
            });
        }
    }
}

impl Drop for SyncInterestLease {
    fn drop(&mut self) {
        let mut entries = self.interests.entries.lock().expect(ERROR_MUTEX);
        let entry = entries.get_mut(&self.doc_id).expect(ERROR_IMPOSSIBLE);
        entry.callers = entry.callers.checked_sub(1).expect(ERROR_IMPOSSIBLE);
        if entry.callers == 0 {
            entry.expires = Some(self.interests.clock.instant() + SYNC_INTEREST_GRACE);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PausedClock;

    impl crate::runtime2::Clock for PausedClock {
        fn instant(&self) -> Instant {
            tokio::time::Instant::now().into_std()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sync_interest_shared_grace_and_reacquisition() -> Res<()> {
        let interests = SyncInterests::new(Arc::new(PausedClock));
        let mut demand = interests.subscribe();
        let doc_id = DocumentId::random();
        let handle = interests.acquire(doc_id.clone());
        assert_eq!(*demand.changed().await?, HashSet::from([doc_id.clone()]));
        let pending = interests.acquire(doc_id.clone());
        drop(handle);
        tokio::time::advance(Duration::from_secs(60)).await;
        interests.expire();
        assert_eq!(*demand.documents(), HashSet::from([doc_id.clone()]));
        drop(pending);
        tokio::time::advance(Duration::from_secs(29)).await;
        interests.expire();
        assert!(!demand.docs.has_changed()?);
        let reacquired = interests.acquire(doc_id.clone());
        tokio::time::advance(Duration::from_secs(2)).await;
        interests.expire();
        assert_eq!(*demand.documents(), HashSet::from([doc_id.clone()]));
        drop(reacquired);
        tokio::time::advance(Duration::from_secs(29)).await;
        interests.expire();
        assert!(!demand.docs.has_changed()?);
        tokio::time::advance(Duration::from_secs(1)).await;
        interests.expire();
        assert_eq!(*demand.changed().await?, HashSet::new());
        Ok(())
    }

    async fn wait_for_subscriptions(
        worker: &big_sync::BigSyncWorkerHandle,
        drive: &mut tokio::task::JoinHandle<Res<()>>,
        events: &mut tokio::sync::broadcast::Receiver<big_sync::SubscriptionEvent>,
        parts: &HashMap<PeerKey, HashMap<PartKey, big_sync::BackendId>>,
        objects: &HashMap<PeerKey, HashMap<ObjKey, big_sync::BackendId>>,
    ) -> Res<()> {
        loop {
            let snapshot = tokio::select! {
                snapshot = worker.snapshot() => snapshot?,
                result = &mut *drive => {
                    result??;
                    return Err(ferr!("sync demand driver stopped before subscriptions settled"));
                }
            };
            if (&snapshot.peer_parts, &snapshot.peer_objects) == (parts, objects) {
                return Ok(());
            }
            tokio::select! {
                event = events.recv() => { event?; },
                result = &mut *drive => {
                    result??;
                    return Err(ferr!("sync demand driver stopped before subscriptions settled"));
                }
            }
        }
    }

    #[tokio::test]
    async fn repo_sync_demand_incremental_peer_scope_and_consumer_grace() -> Res<()> {
        use automerge::{ReadDoc, transaction::Transactable};
        use big_sync::{HostPartStore, SubscriptionUpdate};

        let (repo, repo_stop) = BigRepo::boot(crate::Config {
            node_identity_seed: [201; 32],
            storage: crate::StorageConfig::Memory,
            scope_key: Arc::from("subscription-boundary-test"),
            hidden_parts: HashSet::new(),
            automerge_frontier_group_scope: crate::WorkerGroupScope::disabled(),
            causal_checkpoint_group_scope: crate::WorkerGroupScope::disabled(),
            group_part_group_scope: crate::WorkerGroupScope::disabled(),
            keyhive_change_notifs: true,
        })
        .await?;
        let missing = DocumentId::random();
        assert!(matches!(
            repo.get_doc_for_projection(&missing).await?,
            crate::DocLookup::Missing
        ));
        assert!(repo.sync_demand().documents().is_empty());
        let remote = Arc::new(big_sync::MemoryPartStore::new());
        let part = PartKey::new("embedder-part");
        let added_part = PartKey::new("embedder-added-part");
        for part in [&part, &added_part] {
            remote.ensure_part(part.clone()).await?;
            remote
                .add_part_member(
                    part.clone(),
                    repo.local_peer_id(),
                    keyhive_core::access::Access::Read,
                )
                .await?;
        }
        let (rpc, rpc_stop) = big_sync::rpc::spawn_big_sync_rpc(HashMap::from([(
            Arc::from("subscription-boundary-test"),
            remote as crate::SharedPartStore,
        )]))
        .await?;
        let backend = Arc::new(crate::BigRepoSyncBackend::boot(Arc::downgrade(&repo)).await?);
        let (worker, worker_stop) = big_sync::spawn_big_sync_worker(
            repo.shared_part_store(),
            HashMap::from([(BigRepo::BACKEND_ID.into(), backend as _)]),
            "subscription-boundary-test",
            Arc::from("subscription-boundary-test"),
        )?;
        let eligible = PeerKey::random();
        let replacement = PeerKey::random();
        let ineligible = PeerKey::random();
        for peer in [&eligible, &replacement, &ineligible] {
            worker
                .set_peer(
                    peer.clone(),
                    Arc::new(rpc.in_memory_client(repo.local_peer_id())),
                    HashMap::new(),
                    HashMap::new(),
                )
                .await?;
        }
        let embedder = worker.subscription_source();
        let kept = ObjKey::random();
        for peer in [&eligible, &replacement] {
            embedder
                .update(
                    peer.clone(),
                    SubscriptionUpdate {
                        parts: [(part.clone(), BigRepo::BACKEND_ID.into())].into(),
                        objects: [(kept.clone(), BigRepo::BACKEND_ID.into())].into(),
                        ..Default::default()
                    },
                )
                .await?;
        }
        let mut initial = automerge::Automerge::new();
        initial
            .transact(|tx| tx.put(automerge::ROOT, "title", "subscription-boundary"))
            .map_err(|error| ferr!("{error:?}"))?;
        let handle = repo.create_doc(initial).await?;
        let doc = handle.document_id();
        let handle_clone = handle.clone();
        let interest = repo.interest(doc.clone()).await?;
        let readiness = interest.readiness();
        assert!(*readiness.borrow());
        let interest_clone = interest.clone();
        let internal = repo
            .get_doc_for_projection(&doc)
            .await?
            .into_ready(doc.clone())?;
        let (peers_tx, peers_rx) = watch::channel(HashSet::from([eligible.clone()]));
        let mut events = worker.subscribe_subscription_events();
        let mut driver = repo.drive_sync_demand(&worker, peers_rx);
        let mut applied = driver.applied_demand();
        assert!(applied.borrow().is_none());
        assert!(
            std::future::Future::poll(
                std::pin::Pin::new(&mut driver),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending()
        );
        assert!(applied.borrow().is_none());
        let mut drive = tokio::spawn(driver);
        let mut expected_parts = HashMap::from([
            (
                eligible.clone(),
                [(part.clone(), BigRepo::BACKEND_ID.into())].into(),
            ),
            (
                replacement.clone(),
                [(part.clone(), BigRepo::BACKEND_ID.into())].into(),
            ),
            (ineligible.clone(), HashMap::new()),
        ]);
        let mut expected_objects = HashMap::from([
            (
                eligible.clone(),
                [kept.clone(), doc.clone()]
                    .into_iter()
                    .map(|obj| (obj, BigRepo::BACKEND_ID.into()))
                    .collect(),
            ),
            (
                replacement.clone(),
                [(kept.clone(), BigRepo::BACKEND_ID.into())].into(),
            ),
            (ineligible, HashMap::new()),
        ]);
        wait_for_subscriptions(
            &worker,
            &mut drive,
            &mut events,
            &expected_parts,
            &expected_objects,
        )
        .await?;
        let expected_demand = HashSet::from([doc.clone()]);
        tokio::select! {
            receipt = applied.wait_for(|docs| docs.as_deref() == Some(&expected_demand)) => {
                assert_eq!(receipt?.as_deref(), Some(&expected_demand));
            }
            result = &mut drive => {
                result??;
                return Err(ferr!("sync demand driver stopped before initial demand applied"));
            }
        }
        embedder
            .update(
                eligible.clone(),
                SubscriptionUpdate {
                    parts: [(added_part.clone(), BigRepo::BACKEND_ID.into())].into(),
                    ..Default::default()
                },
            )
            .await?;
        expected_parts
            .get_mut(&eligible)
            .expect(ERROR_IMPOSSIBLE)
            .insert(added_part, BigRepo::BACKEND_ID.into());
        peers_tx.send(HashSet::from([eligible.clone(), replacement.clone()]))?;
        expected_objects
            .get_mut(&replacement)
            .expect(ERROR_IMPOSSIBLE)
            .insert(doc.clone(), BigRepo::BACKEND_ID.into());
        wait_for_subscriptions(
            &worker,
            &mut drive,
            &mut events,
            &expected_parts,
            &expected_objects,
        )
        .await?;
        peers_tx.send(HashSet::from([replacement.clone()]))?;
        expected_objects
            .get_mut(&eligible)
            .expect(ERROR_IMPOSSIBLE)
            .remove(&doc);
        wait_for_subscriptions(
            &worker,
            &mut drive,
            &mut events,
            &expected_parts,
            &expected_objects,
        )
        .await?;
        drop(handle);
        drop(handle_clone);
        drop(interest);
        let interests = &repo.runtime.interests;
        interests.expire_at(interests.clock.instant() + Duration::from_secs(60));
        assert_eq!(
            *repo.sync_demand().documents(),
            HashSet::from([doc.clone()])
        );
        drop(interest_clone);
        let due = interests.entries.lock().expect(ERROR_MUTEX)[&doc]
            .expires
            .expect(ERROR_IMPOSSIBLE);
        interests.expire_at(due - Duration::from_secs(1));
        assert_eq!(
            *repo.sync_demand().documents(),
            HashSet::from([doc.clone()])
        );
        let reacquired = repo.get_doc(&doc).await?.into_ready(doc.clone())?;
        interests.expire_at(due + Duration::from_secs(1));
        assert_eq!(
            *repo.sync_demand().documents(),
            HashSet::from([doc.clone()])
        );
        wait_for_subscriptions(
            &worker,
            &mut drive,
            &mut events,
            &expected_parts,
            &expected_objects,
        )
        .await?;
        embedder
            .update(
                replacement.clone(),
                SubscriptionUpdate {
                    objects: [(doc.clone(), BigRepo::BACKEND_ID.into())].into(),
                    ..Default::default()
                },
            )
            .await?;
        let mut grace_events = worker.subscribe_subscription_events();
        drop(reacquired);
        let due = interests.entries.lock().expect(ERROR_MUTEX)[&doc]
            .expires
            .expect(ERROR_IMPOSSIBLE);
        interests.expire_at(due);
        assert!(repo.sync_demand().documents().is_empty());
        tokio::select! {
            receipt = applied.wait_for(|docs| docs.as_ref().is_some_and(|docs| docs.is_empty())) => {
                assert_eq!(receipt?.as_deref(), Some(&HashSet::new()));
            }
            result = &mut drive => {
                result??;
                return Err(ferr!("sync demand driver stopped before grace withdrawal applied"));
            }
        }
        assert!(!peers_tx.is_closed());
        assert!(!drive.is_finished());
        assert!(*readiness.borrow());
        assert_eq!(
            grace_events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        );
        let snapshot = worker.snapshot().await?;
        assert_eq!(
            (&snapshot.peer_parts, &snapshot.peer_objects),
            (&expected_parts, &expected_objects)
        );
        embedder
            .update(
                replacement.clone(),
                SubscriptionUpdate {
                    removed_objects: [doc.clone()].into(),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            grace_events.recv().await?,
            big_sync::SubscriptionEvent {
                peer_id: replacement.clone(),
                update: SubscriptionUpdate {
                    removed_objects: [doc.clone()].into(),
                    ..Default::default()
                },
            }
        );
        expected_objects
            .get_mut(&replacement)
            .expect(ERROR_IMPOSSIBLE)
            .remove(&doc);
        let snapshot = worker.snapshot().await?;
        assert_eq!(
            (&snapshot.peer_parts, &snapshot.peer_objects),
            (&expected_parts, &expected_objects)
        );
        assert_eq!(internal.document_id(), doc);
        let projected = repo
            .get_doc_for_projection(&doc)
            .await?
            .into_ready(doc.clone())?;
        let projected_clone = projected.clone();
        drop(projected);
        drop(internal);
        let title = projected_clone
            .with_document_read(|doc| {
                doc.get(automerge::ROOT, "title")
                    .expect(ERROR_IMPOSSIBLE)
                    .map(|(value, _)| value.into_scalar().expect(ERROR_IMPOSSIBLE))
            })
            .await;
        assert_eq!(
            title,
            Some(automerge::ScalarValue::Str("subscription-boundary".into()))
        );
        assert!(repo.sync_demand().documents().is_empty());
        wait_for_subscriptions(
            &worker,
            &mut drive,
            &mut events,
            &expected_parts,
            &expected_objects,
        )
        .await?;
        drop(projected_clone);
        assert!(repo.sync_demand().documents().is_empty());
        let active_interest = repo.interest(doc.clone()).await?;
        tokio::select! {
            receipt = applied.wait_for(|docs| docs.as_deref() == Some(&expected_demand)) => {
                assert_eq!(receipt?.as_deref(), Some(&expected_demand));
            }
            result = &mut drive => {
                result??;
                return Err(ferr!("sync demand driver stopped before reacquired demand applied"));
            }
        }
        let snapshot = worker.snapshot().await?;
        expected_objects
            .get_mut(&replacement)
            .expect(ERROR_IMPOSSIBLE)
            .insert(doc.clone(), BigRepo::BACKEND_ID.into());
        assert_eq!(
            (&snapshot.peer_parts, &snapshot.peer_objects),
            (&expected_parts, &expected_objects)
        );
        drop(peers_tx);
        drive.await??;
        assert_eq!(applied.borrow().as_deref(), Some(&expected_demand));
        assert!(applied.changed().await.is_err());
        assert_eq!(*repo.sync_demand().documents(), expected_demand);
        drop(active_interest);
        expected_objects
            .get_mut(&replacement)
            .expect(ERROR_IMPOSSIBLE)
            .remove(&doc);
        let snapshot = worker.snapshot().await?;
        assert_eq!(
            (&snapshot.peer_parts, &snapshot.peer_objects),
            (&expected_parts, &expected_objects)
        );
        drop(embedder);
        for parts in expected_parts.values_mut() {
            parts.clear();
        }
        for objects in expected_objects.values_mut() {
            objects.clear();
        }
        let snapshot = worker.snapshot().await?;
        assert_eq!(
            (snapshot.peer_parts, snapshot.peer_objects),
            (expected_parts, expected_objects)
        );
        worker_stop.stop().await?;
        rpc_stop.stop().await?;
        repo_stop.stop().await?;
        Ok(())
    }
}
