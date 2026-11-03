use super::*;
use crate::receive::ReceivedReplay;
use big_sync_core::rpc::ReplaySubscriptionTarget;
use big_sync_core::tasks::MachineTaskDeets;
use big_sync_core::tasks::MachineTaskErrDeets;
use big_sync_core::tasks::MachineTaskError;
use big_sync_core::tasks::TaskResultDeets;
use big_sync_core::tasks::replay_page::ReplayPageTaskError;
use big_sync_core::tasks::replay_page::ReplayPageTaskErrorDeets;

impl BigSyncWorker {
    fn receive_route(
        &self,
        peer: &PeerKey,
        target: ReplaySubscriptionTarget,
    ) -> Option<ReceiveRoute> {
        let state = self.peers.get(peer)?;
        let backend = match &target {
            ReplaySubscriptionTarget::Part { part_id } => state.parts.get(part_id),
            ReplaySubscriptionTarget::Object { obj_id } => state.objects.get(obj_id),
        }?;
        self.sync_backends
            .get(backend)
            .expect(ERROR_IMPOSSIBLE)
            .replay_receiver()?;
        Some(ReceiveRoute {
            backend: backend.to_string(),
            target,
        })
    }

    pub(super) async fn prepare_receive_task(
        &self,
        task: &mut MachineTask,
    ) -> Res<Vec<ReceiveRoute>> {
        let mut routes = Vec::new();
        match &mut task.deets {
            MachineTaskDeets::DecidePeerStrategy(strategy) => {
                for part in &strategy.parts {
                    let Some(route) = self.receive_route(
                        &strategy.peer_id,
                        ReplaySubscriptionTarget::Part {
                            part_id: part.clone(),
                        },
                    ) else {
                        continue;
                    };
                    let journal = self
                        .part_store
                        .receive_journal()
                        .expect("durable receiver requires a receive journal");
                    let progress = journal.progress(strategy.peer_id.clone(), route).await?;
                    strategy
                        .received_cursors
                        .insert(part.clone(), progress.received);
                }
            }
            MachineTaskDeets::ReplayPage(replay) => {
                for (_, target) in &mut replay.targets {
                    let Some(route) = self
                        .receive_route(&replay.peer_id, ReplaySubscriptionTarget::from(&*target))
                    else {
                        continue;
                    };
                    let journal = self
                        .part_store
                        .receive_journal()
                        .expect("durable receiver requires a receive journal");
                    let progress = journal
                        .progress(replay.peer_id.clone(), route.clone())
                        .await?;
                    *target = route.target.with_cursor(progress.received);
                    routes.push(route);
                }
            }
            MachineTaskDeets::LeafBuckets(_) | MachineTaskDeets::ListBuckets(_) => {}
        }
        Ok(routes)
    }
}

pub(super) async fn gate_result(
    store: &dyn crate::HostPartStore,
    backends: &HashMap<BackendId, Arc<dyn SyncBackend>>,
    msg: MachineTaskMsg,
    mut routes: Vec<ReceiveRoute>,
) -> Res<MachineTaskMsg> {
    if routes.is_empty() {
        return Ok(msg);
    }
    let MachineTaskMsg::MachineTaskResult(result) = &msg else {
        return Ok(msg);
    };
    let TaskResultDeets::ReplayPage(replay) = &result.deets else {
        return Ok(msg);
    };
    routes.retain(|route| {
        replay
            .requested
            .iter()
            .any(|target| ReplaySubscriptionTarget::from(target) == route.target)
    });
    if routes.is_empty() {
        return Ok(msg);
    }
    let journal = store
        .receive_journal()
        .expect("durable receiver requires a receive journal");
    crate::receive::drain(journal, backends).await?;
    let accepted = journal
        .accept(ReceivedReplay {
            peer_id: replay.peer_id.clone(),
            requested: replay.requested.clone(),
            page: replay.page.clone(),
            routes,
        })
        .await?;
    if accepted.is_none() {
        return Ok(MachineTaskMsg::MachineTaskError(MachineTaskError {
            task_id: result.task_id,
            deets: MachineTaskErrDeets::ReplayPage(ReplayPageTaskError {
                peer_id: replay.peer_id.clone(),
                targets: replay.requested.clone(),
                updated_subscription_id: None,
                deets: ReplayPageTaskErrorDeets::StaleReceiveBounds,
            }),
        }));
    }
    crate::receive::drain(journal, backends).await?;
    Ok(msg)
}

#[cfg(test)]
#[path = "worker_receive_tests.rs"]
mod tests;
