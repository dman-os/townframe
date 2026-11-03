use super::*;
use crate::receive::ReceiveJournal;
use crate::receive::ReceiveProgress;
use crate::receive::ReceiveRoute;
use crate::receive::ReceivedReplay;
use crate::receive::ReplayReceipt;

#[derive(Default)]
pub(super) struct MemoryReceiveState {
    sequence: u64,
    pending: Option<ReplayReceipt>,
    frontiers: HashMap<(PeerKey, ReceiveRoute), ReceiveProgress>,
}

#[async_trait]
impl ReceiveJournal for MemoryPartStore {
    async fn progress(&self, peer: PeerKey, route: ReceiveRoute) -> Res<ReceiveProgress> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _) = key.lock(&self.inner);
            guard
                .receive
                .frontiers
                .get(&(peer, route))
                .copied()
                .unwrap_or_default()
        }))
    }

    async fn pending(&self) -> Res<Option<ReplayReceipt>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _) = key.lock(&self.inner);
            guard.receive.pending.clone()
        }))
    }

    async fn accept(&self, replay: ReceivedReplay) -> Res<Option<ReplayReceipt>> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _) = key.lock(&self.inner);
            let state = &mut guard.receive;
            eyre::ensure!(
                state.pending.is_none(),
                "unfinished replay receipt must drain first"
            );
            let bounds = replay.bounds()?;
            for (route, predecessor, _) in &bounds {
                let progress = state
                    .frontiers
                    .get(&(replay.peer_id.clone(), (*route).clone()))
                    .copied()
                    .unwrap_or_default();
                if progress.received != *predecessor {
                    return Ok(None);
                }
            }
            for (route, _, resume) in bounds {
                state
                    .frontiers
                    .entry((replay.peer_id.clone(), route.clone()))
                    .or_default()
                    .received = resume;
            }
            state.sequence = state.sequence.checked_add(1).expect(ERROR_IMPOSSIBLE);
            let receipt = ReplayReceipt {
                id: state.sequence,
                replay,
            };
            state.pending = Some(receipt.clone());
            Ok(Some(receipt))
        })
    }

    async fn applied(&self, receipt_id: u64) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _) = key.lock(&self.inner);
            let state = &mut guard.receive;
            let receipt = state
                .pending
                .as_ref()
                .ok_or_else(|| ferr!("no pending replay receipt"))?;
            eyre::ensure!(
                receipt.id == receipt_id,
                "replay receipt acknowledgment mismatch"
            );
            for (route, predecessor, resume) in receipt.replay.bounds()? {
                let progress = state
                    .frontiers
                    .get_mut(&(receipt.replay.peer_id.clone(), route.clone()))
                    .expect(ERROR_IMPOSSIBLE);
                eyre::ensure!(
                    progress.applied == predecessor && progress.received == resume,
                    "replay acknowledgment chain mismatch"
                );
            }
            let receipt = state.pending.take().expect(ERROR_IMPOSSIBLE);
            for (route, _, resume) in receipt.replay.bounds()? {
                state
                    .frontiers
                    .get_mut(&(receipt.replay.peer_id.clone(), route.clone()))
                    .expect(ERROR_IMPOSSIBLE)
                    .applied = resume;
            }
            Ok(())
        })
    }
}
