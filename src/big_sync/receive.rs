use crate::interlude::*;
use big_sync_core::PeerKey;
use big_sync_core::rpc::ReplayPage;
use big_sync_core::rpc::ReplaySubscriptionTarget;
use big_sync_core::rpc::SubscriptionTarget;
use big_sync_core::rpc::TargetVerdict;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReceiveRoute {
    pub backend: String,
    pub target: ReplaySubscriptionTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReceipt {
    pub id: u64,
    pub replay: ReceivedReplay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceivedReplay {
    pub peer_id: PeerKey,
    pub requested: Vec<SubscriptionTarget>,
    pub page: ReplayPage,
    pub routes: Vec<ReceiveRoute>,
}

impl ReceivedReplay {
    pub(crate) fn bounds(&self) -> Res<Vec<(&ReceiveRoute, u64, u64)>> {
        let mut bounds = Vec::new();
        let mut seen = HashSet::new();
        for route in &self.routes {
            eyre::ensure!(seen.insert(route.target.clone()), "duplicate receive route");
            let requested = self
                .requested
                .iter()
                .find(|target| ReplaySubscriptionTarget::from(*target) == route.target)
                .ok_or_else(|| ferr!("receive route missing request bound"))?;
            let verdict = self
                .page
                .targets
                .iter()
                .find(|(target, _)| ReplaySubscriptionTarget::from(target) == route.target);
            if let Some((_, TargetVerdict::Events { resume, .. })) = verdict {
                eyre::ensure!(
                    *resume >= requested.cursor(),
                    "replay resume precedes request"
                );
                bounds.push((route, requested.cursor(), *resume));
            }
        }
        Ok(bounds)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveProgress {
    pub received: u64,
    pub applied: u64,
}

#[async_trait]
pub trait ReplayReceiver: Send + Sync {
    async fn receive_replay(
        &self,
        receipt: &ReplayReceipt,
        targets: &[ReplaySubscriptionTarget],
    ) -> Res<()>;
}

#[async_trait]
pub trait ReceiveJournal: Send + Sync {
    async fn progress(&self, peer: PeerKey, route: ReceiveRoute) -> Res<ReceiveProgress>;
    async fn pending(&self) -> Res<Option<ReplayReceipt>>;
    async fn accept(&self, replay: ReceivedReplay) -> Res<Option<ReplayReceipt>>;
    async fn applied(&self, receipt_id: u64) -> Res<()>;
}

pub(crate) async fn drain(
    journal: &dyn ReceiveJournal,
    backends: &HashMap<crate::BackendId, Arc<dyn crate::SyncBackend>>,
) -> Res<()> {
    let Some(receipt) = journal.pending().await? else {
        return Ok(());
    };
    let mut deliveries = BTreeMap::<&str, Vec<ReplaySubscriptionTarget>>::new();
    for route in &receipt.replay.routes {
        deliveries
            .entry(&route.backend)
            .or_default()
            .push(route.target.clone());
    }
    for (backend_id, targets) in deliveries {
        let backend = backends.get(backend_id).ok_or_else(|| {
            ferr!(
                "pending replay receipt {} requires backend {backend_id}",
                receipt.id
            )
        })?;
        let receiver = backend.replay_receiver().ok_or_else(|| {
            ferr!("pending replay receipt requires durable receiver {backend_id}")
        })?;
        receiver.receive_replay(&receipt, &targets).await?;
    }
    journal.applied(receipt.id).await
}

#[cfg(test)]
#[path = "receive_tests.rs"]
pub(crate) mod tests;
