use super::*;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_sync::HostPartStore;
use big_sync::SubscriptionEvent;
use big_sync::SubscriptionUpdate;
use tokio::sync::broadcast;

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

type ExpectedSubscriptions<K> = HashMap<PeerKey, HashMap<K, big_sync::BackendId>>;

/// A repo wired to a big_sync worker with three peers: `eligible` and
/// `replacement` carry embedder-owned subscriptions to one part and one object,
/// the third carries none. Tracks the subscriptions the worker is expected to
/// hold, so each step of a scenario only states what it changes.
struct DemandFixture {
    repo: Arc<BigRepo>,
    worker: big_sync::BigSyncWorkerHandle,
    eligible: PeerKey,
    replacement: PeerKey,
    added_part: PartKey,
    expected_parts: ExpectedSubscriptions<PartKey>,
    expected_objects: ExpectedSubscriptions<ObjKey>,
    _rpc: big_sync::rpc::BigSyncRpcHandle,
    worker_stop: big_sync::StopToken,
    rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    repo_stop: crate::BigRepoStopToken,
}

impl DemandFixture {
    /// Boots the fixture and returns the embedder's subscription source apart
    /// from it, because dropping that source is itself a scenario step.
    async fn boot() -> Res<(Self, big_sync::SubscriptionSource)> {
        let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(Default::default()));
        let (repo, repo_stop) = BigRepo::boot(
            runtime,
            crate::Config {
                node_identity_seed: [201; 32],
                storage: crate::StorageConfig::Memory,
                scope_key: Arc::from("subscription-boundary-test"),
                hidden_parts: HashSet::new(),
                automerge_frontier_group_scope: crate::WorkerGroupScope::disabled(),
                causal_checkpoint_group_scope: crate::WorkerGroupScope::disabled(),
                group_part_group_scope: crate::WorkerGroupScope::disabled(),
                keyhive_change_notifs: true,
            },
        )
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
        let fixture = Self {
            expected_parts: HashMap::from([
                (
                    eligible.clone(),
                    [(part.clone(), BigRepo::BACKEND_ID.into())].into(),
                ),
                (
                    replacement.clone(),
                    [(part, BigRepo::BACKEND_ID.into())].into(),
                ),
                (ineligible.clone(), HashMap::new()),
            ]),
            expected_objects: HashMap::from([
                (
                    eligible.clone(),
                    [(kept.clone(), BigRepo::BACKEND_ID.into())].into(),
                ),
                (
                    replacement.clone(),
                    [(kept, BigRepo::BACKEND_ID.into())].into(),
                ),
                (ineligible, HashMap::new()),
            ]),
            repo,
            worker,
            eligible,
            replacement,
            added_part,
            _rpc: rpc,
            worker_stop,
            rpc_stop,
            repo_stop,
        };
        Ok((fixture, embedder))
    }

    fn expect_object(&mut self, peer: &PeerKey, doc: &DocumentId) {
        self.expected_objects
            .get_mut(peer)
            .expect(ERROR_IMPOSSIBLE)
            .insert(doc.clone(), BigRepo::BACKEND_ID.into());
    }

    fn forget_object(&mut self, peer: &PeerKey, doc: &DocumentId) {
        self.expected_objects
            .get_mut(peer)
            .expect(ERROR_IMPOSSIBLE)
            .remove(doc);
    }

    /// Asserts the worker holds exactly the expected subscriptions right now,
    /// without waiting for further events.
    async fn assert_subscriptions(&self) -> Res<()> {
        let snapshot = self.worker.snapshot().await?;
        assert_eq!(
            (&snapshot.peer_parts, &snapshot.peer_objects),
            (&self.expected_parts, &self.expected_objects)
        );
        Ok(())
    }

    async fn stop(self) -> Res<()> {
        self.worker_stop.stop().await?;
        self.rpc_stop.stop().await?;
        self.repo_stop.stop().await?;
        Ok(())
    }
}

/// Grace expiry deadline currently recorded for an unheld `doc`.
fn grace_deadline(interests: &SyncInterests, doc: &DocumentId) -> Instant {
    interests.entries.lock().expect(ERROR_MUTEX)[doc]
        .expires
        .expect(ERROR_IMPOSSIBLE)
}

/// The spawned repo sync-demand driver, its applied-demand receipts, and the
/// worker's subscription events from just before it started.
struct DemandDrive {
    drive: tokio::task::JoinHandle<Res<()>>,
    applied: watch::Receiver<Option<Arc<HashSet<DocumentId>>>>,
    events: broadcast::Receiver<SubscriptionEvent>,
}

impl DemandDrive {
    /// Starts the driver, first pinning that it reports no applied demand
    /// before it has been polled to its first await point and after.
    fn start(fixture: &DemandFixture, peers: watch::Receiver<HashSet<PeerKey>>) -> Self {
        let events = fixture.worker.subscribe_subscription_events();
        let mut driver = fixture.repo.drive_sync_demand(&fixture.worker, peers);
        let applied = driver.applied_demand();
        assert!(applied.borrow().is_none());
        assert!(
            std::future::Future::poll(
                std::pin::Pin::new(&mut driver),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending()
        );
        assert!(applied.borrow().is_none());
        Self {
            drive: tokio::spawn(driver),
            applied,
            events,
        }
    }

    /// Waits until the worker holds exactly the fixture's expected
    /// subscriptions, failing if the driver stops first.
    async fn settle(&mut self, fixture: &DemandFixture) -> Res<()> {
        let expected = (&fixture.expected_parts, &fixture.expected_objects);
        loop {
            let snapshot = tokio::select! {
                snapshot = fixture.worker.snapshot() => snapshot?,
                result = &mut self.drive => {
                    result??;
                    return Err(ferr!("sync demand driver stopped before subscriptions settled"));
                }
            };
            if (&snapshot.peer_parts, &snapshot.peer_objects) == expected {
                return Ok(());
            }
            tokio::select! {
                event = self.events.recv() => { event?; },
                result = &mut self.drive => {
                    result??;
                    return Err(ferr!("sync demand driver stopped before subscriptions settled"));
                }
            }
        }
    }

    /// Waits until the driver reports `expected` as its applied demand,
    /// failing with `stage` if the driver stops first.
    async fn wait_applied(&mut self, expected: &HashSet<DocumentId>, stage: &str) -> Res<()> {
        tokio::select! {
            receipt = self.applied.wait_for(|docs| docs.as_deref() == Some(expected)) => {
                assert_eq!(receipt?.as_deref(), Some(expected));
            }
            result = &mut self.drive => {
                result??;
                return Err(ferr!("sync demand driver stopped before {stage} applied"));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn repo_sync_demand_incremental_peer_scope_and_consumer_grace() -> Res<()> {
    let (mut fixture, embedder) = DemandFixture::boot().await?;
    let eligible = fixture.eligible.clone();
    let replacement = fixture.replacement.clone();
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "subscription-boundary"))
        .map_err(|error| ferr!("{error:?}"))?;
    let handle = fixture.repo.create_doc(initial).await?;
    let doc = handle.document_id();
    let handle_clone = handle.clone();
    let interest = fixture.repo.interest(doc.clone()).await?;
    let readiness = interest.readiness();
    assert!(*readiness.borrow());
    let interest_clone = interest.clone();
    let internal = fixture
        .repo
        .get_doc_for_projection(&doc)
        .await?
        .into_ready(doc.clone())?;
    let (peers_tx, peers_rx) = watch::channel(HashSet::from([eligible.clone()]));
    let mut demand = DemandDrive::start(&fixture, peers_rx);
    fixture.expect_object(&eligible, &doc);
    demand.settle(&fixture).await?;
    let expected_demand = HashSet::from([doc.clone()]);
    demand
        .wait_applied(&expected_demand, "initial demand")
        .await?;
    embedder
        .update(
            eligible.clone(),
            SubscriptionUpdate {
                parts: [(fixture.added_part.clone(), BigRepo::BACKEND_ID.into())].into(),
                ..Default::default()
            },
        )
        .await?;
    fixture
        .expected_parts
        .get_mut(&eligible)
        .expect(ERROR_IMPOSSIBLE)
        .insert(fixture.added_part.clone(), BigRepo::BACKEND_ID.into());
    peers_tx.send(HashSet::from([eligible.clone(), replacement.clone()]))?;
    fixture.expect_object(&replacement, &doc);
    demand.settle(&fixture).await?;
    peers_tx.send(HashSet::from([replacement.clone()]))?;
    fixture.forget_object(&eligible, &doc);
    demand.settle(&fixture).await?;
    drop(handle);
    drop(handle_clone);
    drop(interest);
    let interests = Arc::clone(&fixture.repo.runtime.interests);
    interests.expire_at(interests.clock.instant() + Duration::from_secs(60));
    assert_eq!(*fixture.repo.sync_demand().documents(), expected_demand);
    drop(interest_clone);
    let due = grace_deadline(&interests, &doc);
    interests.expire_at(due - Duration::from_secs(1));
    assert_eq!(*fixture.repo.sync_demand().documents(), expected_demand);
    let reacquired = fixture.repo.get_doc(&doc).await?.into_ready(doc.clone())?;
    interests.expire_at(due + Duration::from_secs(1));
    assert_eq!(*fixture.repo.sync_demand().documents(), expected_demand);
    demand.settle(&fixture).await?;
    embedder
        .update(
            replacement.clone(),
            SubscriptionUpdate {
                objects: [(doc.clone(), BigRepo::BACKEND_ID.into())].into(),
                ..Default::default()
            },
        )
        .await?;
    let mut grace_events = fixture.worker.subscribe_subscription_events();
    drop(reacquired);
    interests.expire_at(grace_deadline(&interests, &doc));
    assert!(fixture.repo.sync_demand().documents().is_empty());
    demand
        .wait_applied(&HashSet::new(), "grace withdrawal")
        .await?;
    assert!(!peers_tx.is_closed());
    assert!(!demand.drive.is_finished());
    assert!(*readiness.borrow());
    assert_eq!(
        grace_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    );
    fixture.assert_subscriptions().await?;
    let withdrawal = SubscriptionUpdate {
        removed_objects: [doc.clone()].into(),
        ..Default::default()
    };
    embedder
        .update(replacement.clone(), withdrawal.clone())
        .await?;
    assert_eq!(
        grace_events.recv().await?,
        SubscriptionEvent {
            peer_id: replacement.clone(),
            update: withdrawal,
        }
    );
    fixture.forget_object(&replacement, &doc);
    fixture.assert_subscriptions().await?;
    assert_eq!(internal.document_id(), doc);
    let projected = fixture
        .repo
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
    assert!(fixture.repo.sync_demand().documents().is_empty());
    demand.settle(&fixture).await?;
    drop(projected_clone);
    assert!(fixture.repo.sync_demand().documents().is_empty());
    let active_interest = fixture.repo.interest(doc.clone()).await?;
    demand
        .wait_applied(&expected_demand, "reacquired demand")
        .await?;
    fixture.expect_object(&replacement, &doc);
    fixture.assert_subscriptions().await?;
    drop(peers_tx);
    let DemandDrive {
        drive,
        mut applied,
        events: _events,
    } = demand;
    drive.await??;
    assert_eq!(applied.borrow().as_deref(), Some(&expected_demand));
    assert!(applied.changed().await.is_err());
    assert_eq!(*fixture.repo.sync_demand().documents(), expected_demand);
    drop(active_interest);
    fixture.forget_object(&replacement, &doc);
    fixture.assert_subscriptions().await?;
    drop(embedder);
    for parts in fixture.expected_parts.values_mut() {
        parts.clear();
    }
    for objects in fixture.expected_objects.values_mut() {
        objects.clear();
    }
    fixture.assert_subscriptions().await?;
    fixture.stop().await
}
