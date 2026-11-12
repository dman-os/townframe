use super::*;
use crate::sync::DownloadPeers;
use crate::work_queue::WorkQueue;
use big_repo::SharedBigRepo;
use big_repo::SharedPartStore;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use iroh_blobs::api::TempTag;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use tokio::sync::watch;

mod observe;
mod subscriptions;

type Transfer = BoxFuture<'static, (Attempt, Res<Vec<TempTag>>)>;

#[derive(Clone)]
pub struct InventoryMirrors {
    pub(crate) admission: Admission,
    pub(crate) projection: project::Projection,
    pub(crate) downloads: watch::Sender<Option<DownloadPeers>>,
    #[cfg(test)]
    pub(crate) progress: watch::Sender<u64>,
    #[cfg(test)]
    pub(crate) completions: watch::Sender<u64>,
    queue: WorkQueue,
}

impl InventoryMirrors {
    pub(crate) async fn boot(
        sql: Database,
        service: Arc<crate::BlobService>,
        parts: SharedPartStore,
        repo: SharedBigRepo,
        queue: WorkQueue,
    ) -> Res<(Self, StopToken)> {
        let (admission, owner_stop) = Admission::boot(sql).await?;
        admission.recover().await?;
        let (downloads, changes) = watch::channel(None);
        let mirrors = Self {
            admission,
            projection: project::Projection {
                service,
                parts,
                repo,
            },
            downloads,
            #[cfg(test)]
            progress: watch::channel(0).0,
            #[cfg(test)]
            completions: watch::channel(0).0,
            queue,
        };
        for inventory in mirrors.admission.inventories().await? {
            mirrors.refresh_authorization(&inventory).await?;
            mirrors.queue.enqueue(&inventory).await?;
        }
        let (stop, tasks) = StopToken::new();
        let runtime = mirrors.clone();
        let cancellation = tasks.clone();
        tasks
            .spawn(async move {
                runtime.run(cancellation, changes).await.unwrap();
            })
            .expect("inventory projection starts before shutdown");
        Ok((mirrors, stop.then(owner_stop)))
    }

    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.admission.subscribe_changes()
    }

    pub async fn select_drawer(
        &self,
        drawer: DocId,
        selection: Option<InventorySelection>,
    ) -> Res<()> {
        self.admission.select_root(&drawer, selection).await
    }

    pub async fn override_drawer_inventory(
        &self,
        drawer: DocId,
        selection: Option<InventorySelection>,
    ) -> Res<()> {
        self.admission.override_drawer(&drawer, selection).await
    }

    pub async fn coverage(&self, root: &DocId) -> Res<Coverage> {
        self.admission.coverage(root).await
    }

    pub async fn select_inventory(&self, inventory: DocId, owner: String) -> Res<()> {
        self.admission.select(&inventory, &owner, true).await?;
        self.refresh_authorization(&inventory).await?;
        self.queue.enqueue(&inventory).await
    }

    pub async fn deselect_inventory(&self, inventory: DocId, owner: String) -> Res<()> {
        self.admission.select(&inventory, &owner, false).await
    }

    pub(crate) async fn refresh_authorization(&self, inventory: &DocId) -> Res<()> {
        let readable = self.projection.readable(inventory).await?;
        self.admission.authorize(inventory, readable).await?;
        crate::permissions::seed(&self.projection.repo, &self.projection.parts, inventory).await
    }

    pub(crate) async fn refresh_policy(&self) -> Res<()> {
        for inventory in self.admission.inventories().await? {
            self.refresh_authorization(&inventory).await?;
        }
        self.admission.rearm_discovery().await?;
        self.admission.rearm().await
    }

    pub(crate) async fn reconcile(
        &self,
        inventory: &DocId,
        revision: u64,
        pins: &BTreeMap<BlobId, u64>,
        complete: bool,
    ) -> Res<()> {
        self.refresh_authorization(inventory).await?;
        self.admission.refresh_producers(inventory).await?;
        self.admission
            .pins(inventory, revision, pins, complete)
            .await
    }

    async fn project_pair(
        &self,
        inventory: &DocId,
        digest: BlobId,
        transport: Option<DownloadPeers>,
        transfers: &mut FuturesUnordered<Transfer>,
    ) -> Res<bool> {
        self.refresh_authorization(inventory).await?;
        let state = self
            .admission
            .state(inventory, digest)
            .await?
            .expect("dirty admission pair exists");
        let guards = project::local_bytes(&self.projection.service, digest).await?;
        let available = guards.is_some();
        self.admission
            .available(inventory, digest, available)
            .await?;
        if !available
            && transfers.len() < 8
            && let Some(transport) = transport
        {
            let peers = transport.addresses.keys().cloned().collect();
            if let Some(attempt) = self.admission.begin_from(inventory, digest, peers).await? {
                let length = self
                    .admission
                    .length(inventory, digest)
                    .await?
                    .expect("a fetch attempt has a pin length");
                transfers.push(transport.fetch(
                    self.projection.service.store().clone(),
                    attempt,
                    length,
                ));
            }
        }
        let deferred =
            !available && state.fetch_eligible() && !state.fetching && transfers.len() >= 8;
        // Settle only the generation this decision was made on: an observation or pin that landed
        // while it ran leaves the pair dirty for the next round instead of being settled unseen.
        self.projection
            .apply(&self.admission, inventory, digest, state.generation)
            .await?;
        drop(guards);
        Ok(deferred)
    }

    async fn finish_transfer(&self, attempt: Attempt, result: Res<Vec<TempTag>>) -> Res<()> {
        match result {
            Ok(guards) => {
                self.refresh_authorization(&attempt.inventory).await?;
                let authorized = self.projection.readable(&attempt.inventory).await?;
                self.admission.complete(&attempt, authorized).await?;
                self.admission
                    .available(&attempt.inventory, attempt.digest, true)
                    .await?;
                // Publish the pair as it stands now, whatever moved it during the transfer: its
                // bytes are local, so no change since can need a fetch decision that publishing
                // the newer generation would skip (unlike `project_pair`'s, made before a fetch).
                let generation = self
                    .admission
                    .state(&attempt.inventory, attempt.digest)
                    .await?
                    .expect("a completed attempt's pair exists")
                    .generation;
                self.projection
                    .apply(
                        &self.admission,
                        &attempt.inventory,
                        attempt.digest,
                        generation,
                    )
                    .await?;
                drop(guards);
            }
            Err(error) => {
                tracing::warn!(%error, "inventory byte transfer failed; parked until facts change");
                self.admission.fail(&attempt).await?;
            }
        }
        #[cfg(test)]
        self.completions.send_modify(|revision| *revision += 1);
        Ok(())
    }

    async fn run(
        &self,
        cancellation: Spawner,
        mut peers: watch::Receiver<Option<DownloadPeers>>,
    ) -> Res<()> {
        let mut transfers = FuturesUnordered::<Transfer>::new();
        let mut deferred = std::collections::BTreeSet::new();
        loop {
            let changed = self.admission.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let pending = self.admission.dirty().await?;
            let saturated = pending.len() == 128;
            deferred.extend(
                pending
                    .into_iter()
                    .map(|(inventory, digest, _)| (inventory, digest)),
            );
            for (inventory, digest) in std::mem::take(&mut deferred) {
                let transport = peers.borrow().clone();
                if self
                    .project_pair(&inventory, digest, transport, &mut transfers)
                    .await?
                {
                    deferred.insert((inventory, digest));
                }
            }
            #[cfg(test)]
            self.progress.send_modify(|revision| *revision += 1);
            if saturated {
                continue;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                Some((attempt, result)) = transfers.next(), if !transfers.is_empty() => {
                    self.finish_transfer(attempt, result).await?;
                }
                _ = changed => (),
                result = peers.changed() => {
                    result.expect("inventory transport publisher outlives runtime");
                    self.admission.rearm().await?;
                }
            }
        }
    }
}
