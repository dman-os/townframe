use crate::interlude::*;
use crate::part_store::HostPartStore;

#[cfg(test)]
use big_sync_core::ObjId;
use big_sync_core::part_store::CursorIndex;
use big_sync_core::{PartId, PeerId};

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::future::Future;

/// One BigSync worker/store frontier participating in a network-rest fence.
#[derive(Clone)]
pub struct NetworkRestTarget {
    pub worker: crate::BigSyncWorkerHandle,
    pub store: Arc<dyn HostPartStore>,
    pub peer_ids: Vec<PeerId>,
    pub part_ids: Vec<PartId>,
}

async fn cursor_snapshot(targets: &[NetworkRestTarget]) -> Res<Vec<BTreeMap<PartId, CursorIndex>>> {
    let mut snapshots = Vec::with_capacity(targets.len());
    for target in targets {
        let requested: HashSet<_> = target.part_ids.iter().copied().collect();
        let summaries = target
            .store
            .summarize_parts(requested)
            .await?
            .map_err(|error| ferr!("unable to summarize network-rest parts: {error:?}"))?;
        snapshots.push(
            summaries
                .into_iter()
                .map(|(part, summary)| (part, summary.latest_cursor))
                .collect(),
        );
    }
    Ok(snapshots)
}

/// Reach the fixed point of BigSync frontier fences and caller-supplied local
/// quiescence. Internally generated events are included by repeating whenever
/// any local part cursor advances; two complete stable rounds close the race
/// where a notification crosses the first observation boundary.
pub async fn wait_for_network_rest<F, Fut>(targets: &[NetworkRestTarget], mut quiesce: F) -> Res<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Res<()>>,
{
    let mut stable_rounds = 0_u8;
    let mut before = cursor_snapshot(targets).await?;
    tracing::debug!(?before, "network-rest initial cursors");
    loop {
        for target in targets {
            if !target.peer_ids.is_empty() && !target.part_ids.is_empty() {
                target
                    .worker
                    .wait_for_full_sync(
                        target.peer_ids.iter().copied(),
                        target.part_ids.iter().copied(),
                    )
                    .await?;
            }
        }
        quiesce().await?;

        let after = cursor_snapshot(targets).await?;
        tracing::debug!(
            ?before,
            ?after,
            stable_rounds,
            "network-rest round completed"
        );
        if after == before {
            stable_rounds += 1;
            if stable_rounds == 2 {
                return Ok(());
            }
        } else {
            stable_rounds = 0;
        }
        before = after;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ObservedObjSnapshot {
    pub payload: Option<serde_json::Value>,
    pub parts: BTreeSet<PartId>,
}

#[derive(Debug, Clone)]
#[cfg(test)]
pub(crate) struct ObservedStoreSnapshot {
    pub objs: BTreeMap<ObjId, ObservedObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

#[cfg(test)]
impl PartialEq for ObservedStoreSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.objs == other.objs
    }
}

#[cfg(test)]
impl Eq for ObservedStoreSnapshot {}

#[async_trait]
#[cfg(test)]
pub(crate) trait ObservedStore: HostPartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot>;
}
