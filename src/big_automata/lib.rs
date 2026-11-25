mod interlude {
    pub use utils_rs::prelude::*;

    pub use future_form::FutureForm;

    pub use crate::ids::*;
    pub use structstruck::strike;

    pub use crate::store::{CursorIndex, ObjPayload};
    pub use std::collections::{HashMap as Map, HashSet as Set};
}

mod cursor;
mod fingerprint;
mod ids;
mod mpsc;
mod okv;
mod outbox;
mod peer;
mod rpc;
mod scheduler;
mod stat;
mod store;
mod tasks;
mod utils;
mod watermark;

use crate::interlude::*;

use scheduler::{TaskId, TaskSeed};
use tasks::{MachineTask, SyncTask};

#[derive(Default, Debug, Clone, Copy)]
pub enum AllowedStrat {
    Cursor,
    #[default]
    CursorAndBucket,
}

strike! {
    #[structstruck::each[derive(Debug)]]
    pub enum BigSyncEvent {
        UpdatePeerEvent (pub struct {
            pub peer_key: PeerKey,
            /// Partitions to sync from the peer
            pub parts: Set<PartKey>,
            /// Objects to follow directly from the peer.
            pub objects: Set<ObjKey>,
            pub allowed_strat_override: Map<PartKey, AllowedStrat>
        }),
        RemovePeerEvent (pub struct {
            pub peer_key: PeerKey,
        }),
        WaitForFullSyncEvent (pub struct {
            pub waiter_id: u64,
            pub peer_keys: Set<PeerKey>,
            pub part_keys: Set<PartKey>,
        }),
        SyncCompletedEvent (pub struct {
            pub task_id: TaskId,
            pub peer_key: PeerKey,
            pub completion: SyncTaskCompletion,
        }),
        SyncFailedEvent (pub struct {
            pub task_id: TaskId,
            pub peer_key: PeerKey,
            pub obj_id: ObjKey,
            pub err: eyre::Report,
        }),
        SyncStaleEvent (pub struct {
            pub task_id: TaskId,
            pub peer_key: PeerKey,
            pub obj_id: ObjKey,
        }),
        RemoveCompletedEvent (pub struct {
            pub task_id: TaskId,
            pub peer_key: PeerKey,
            pub obj_id: ObjKey,
        }),
        RemoveFailedEvent (pub struct  {
            pub task_id: TaskId,
            pub peer_key: PeerKey,
            pub obj_id: ObjKey,
            pub err: eyre::Report,
        }),
    }
}

pub type Clock = Box<dyn Fn() -> std::time::Instant>;

strike! {
    pub struct BigSyncMachine {
        all_seen_peer: Set<PeerKey>,
        peers: Map<PeerKey, peer::PeerState>,
        stat: stat::StatMachine,

        clock: Clock,
        sync_tasks: scheduler::Scheduler<tasks::SyncTaskDeets>,
        machine_tasks: scheduler::Scheduler<tasks::MachineTaskDeets>,
        default_allowed_strat: AllowedStrat,
    }
}

impl BigSyncMachine {
    pub fn new(clock: Clock) -> Self {
        Self {
            clock,

            all_seen_peer: default(),
            peers: default(),
            stat: default(),
            sync_tasks: default(),
            machine_tasks: default(),
            default_allowed_strat: default(),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn handle_evt(&mut self, evt: BigSyncEvent) {
        use BigSyncEvent::*;
        match evt {
            UpdatePeerEvent(evt) => self.handle_update_peer_evt(evt),
            RemovePeerEvent(evt) => self.handle_remove_peer_evt(evt),
            WaitForFullSyncEvent(evt) => todo!(),
            SyncCompletedEvent(evt) => todo!(),
            SyncFailedEvent(evt) => todo!(),
            SyncStaleEvent(evt) => todo!(),
            RemoveCompletedEvent(evt) => todo!(),
            RemoveFailedEvent(evt) => todo!(),
        }
    }
}

// tasks
impl BigSyncMachine {
    pub fn drain_sync_spawn_queue(&mut self) -> std::vec::Drain<'_, TaskSeed<SyncTask>> {
        self.sync_tasks.drain_spawn_queue()
    }

    pub fn drain_machine_spawn_queue(&mut self) -> std::vec::Drain<'_, TaskSeed<MachineTask>> {
        self.machine_tasks.drain_spawn_queue()
    }

    pub fn drain_stop_queue(
        &mut self,
    ) -> std::iter::Chain<
        std::collections::hash_set::Drain<'_, TaskId>,
        std::collections::hash_set::Drain<'_, TaskId>,
    > {
        self.machine_tasks
            .drain_stop_queue()
            .chain(self.sync_tasks.drain_stop_queue())
    }
}

// stat
impl BigSyncMachine {
    #[tracing::instrument(skip(self))]
    fn handle_wait_for_full_sync_evt(
        &mut self,
        WaitForFullSyncEvent {
            waiter_id,
            peer_keys,
            part_keys,
        }: WaitForFullSyncEvent,
    ) {
        // FIXME: why do we need to do this?
        // for key in &evt.peer_keys {
        //     self.refresh_peer_replay_worker(key.clone(), true);
        // }
        self.stat
            .add_full_sync_waiter(waiter_id, peer_keys, part_keys);
    }

    pub fn drain_stat_evts(&mut self) -> std::vec::Drain<'_, stat::SyncStatEvent> {
        self.stat.drain_evts()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestSyncWorker {
        bsm: BigSyncMachine,
    }
    impl TestSyncWorker {
        fn machine_loop() {
            loop {
                let next_due = self.next_wake();
                let wake_at =
                    next_due.unwrap_or_else(|| std::time::Instant::now() + IDLE_SLEEP_CEILING);
                let wake_timer = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at));
                tokio::pin!(wake_timer);
                tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => {
                        trace!("machine loop cancelled");
                        break;
                    }
                    msg = self.task_rx.recv() => {
                        let Ok(msg) = msg else {
                            break;
                        };
                        trace!(?msg, "received task msg");
                        self.machine.handle_task_msg(msg);
                    }
                    evt = self.sync_rx.recv() => {
                        let Ok(evt) = evt else {
                            break;
                        };
                        debug!(worker = %self.label, ?evt, "big_sync worker received sync event");
                        self.machine.handle_evt(evt);
                    }
                    msg = self.host_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        debug!(worker = %self.label, ?msg, "big_sync worker received host msg");
                        self.handle_msg(msg).await?;
                    }
                    _ = &mut wake_timer => {
                        if next_due.is_none() {
                            // Nothing was paced and no zombie was outstanding when this
                            // iteration went to sleep, so every other tail dependency is
                            // event-driven: this wake is the safety net firing. A regular
                            // cadence here means some dependency is periodic but is not
                            // expressed as a deadline.
                            debug!(
                                worker = %self.label,
                                "big sync worker woke on the idle ceiling with nothing due"
                            );
                        }
                        self.machine.handle_tick(std::time::Instant::now());
                    }
                };
                while let Some((id, cmd)) = self.machine.get_cmd() {
                    trace!(?cmd, "executing machine cmd");
                    match cmd {
                        BigSyncMachineCommand::SetPartCursor {
                            peer_id,
                            part_id,
                            cursor,
                        } => {
                            self.part_store
                                .set_peer_part_cursor(peer_id, part_id, cursor)
                                .await?;
                        }
                    }
                    self.machine.handle_cmd_success(id);
                }

                self.batch_stop_tasks().await?;
                self.machine_spawn_queue
                    .extend(self.machine.drain_machine_spawn_queue());
                while let Some(task) = self.machine_spawn_queue.pop_front() {
                    trace!(?task, "spawning machine task");
                    self.spawn_machine_task(task, Arc::clone(&shutdown)).await?;
                }

                for task in self.bsm.drain_sync_spawn_queue() {
                    self.sync_spawn_queue.push_front(task);
                }
                while self.sync_tasks.len() < MAX_ACTIVE_SYNC_TASKS {
                    let Some(task) = self.sync_spawn_queue.pop_front() else {
                        break;
                    };
                    debug!(worker = %self.label, task_id = task.id, peer_id = %task.deets.peer_id, obj_id = %task.deets.obj_id, "spawning sync task");
                    self.spawn_sync_task(task).await?;
                }
                self.sweep_finished_zombies();
                for event in self.machine.drain_evts() {
                    debug!(worker = %self.label, ?event, "dispatching stat event");
                    if let big_sync_core::SyncStatEvent::FullSyncWaiterSatisfied { waiter_id } =
                        event
                        && let Some(resp) = self.full_sync_waiters.remove(&waiter_id)
                    {
                        info!(worker = %self.label, waiter_id, "full sync waiter satisfied");
                        resp.send(Ok(()))
                            .inspect_err(|_| warn_loc!(ERROR_CALLER))
                            .ok();
                    }
                    self.stats_tx.send(event).ok();
                }
            }
        }
    }

    #[test]
    fn full_sync_waiter_smoke() {
        let bsm = BigSyncMachine::default();
        let peer_a = PeerKey::rand32();
        let part_a = PartKey::rand32();
        let events = [
            //
            BigSyncEvent::UpdatePeerEvent(UpdatePeerEvent {
                peer_key: peer_a,
                parts: [part_a].into(),
                objects: default(),
            }),
        ];
    }
}
