use crate::interlude::*;

use big_sync_core::{
    mpsc,
    part_store::{ObjPayload, PartitionStore, PeerPartCursors},
    rpc::BigSyncRpcClient,
    ObjId, PartId, PeerId,
};
use future_form::{FutureForm, Sendable};
use utils_rs::prelude::futures::future::BoxFuture;

#[derive(Clone)]
pub struct TaskTrap {
    tx: tokio::sync::mpsc::Sender<eyre::Report>,
}

pub enum Never {}

impl TaskTrap {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<eyre::Report>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        (Self { tx }, rx)
    }

    async fn run_or_trap<F, O>(&self, fut: F) -> O
    where
        F: std::future::Future<Output = Res<O>>,
    {
        match fut.await {
            Ok(val) => val,
            Err(err) => {
                self.trap(err).await;
                unreachable!()
            }
        }
    }

    async fn trap(&self, err: eyre::Report) -> Never {
        self.tx.send(err).await.expect(ERROR_CHANNEL);
        std::future::pending::<()>().await;
        unreachable!()
    }
}

pub struct TrappedPartStore {
    pub trap: TaskTrap,
    pub inner: Arc<dyn crate::part_store::HostPartitionStore>,
}

impl PartitionStore<Sendable> for TrappedPartStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        let fut = self.inner.member_count(part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        let fut = self.inner.obj_payload(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> BoxFuture<'a, ()> {
        let fut = self.inner.upsert_obj(obj_id, payload.clone(), parts.into());
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        let fut = self.inner.obj_parts(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let fut = self.inner.add_obj_to_parts(obj_id, parts.into());
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        let fut = self.inner.remove_obj_from_part(obj_id, part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, PeerPartCursors> {
        let fut = self.inner.get_peer_part_cursor(peer_id, part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> BoxFuture<'a, ()> {
        let fut = self.inner.set_peer_part_cursor(peer_id, part_id, cursors);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}

pub struct TrappedRpcClient {
    pub trap: TaskTrap,
    pub inner: Arc<dyn crate::rpc::HostBigRpcClient>,
}

impl BigSyncRpcClient<Sendable> for TrappedRpcClient {
    fn peer_summary<'a>(
        &'a self,
        req: big_sync_core::rpc::PeerSummaryRequest,
    ) -> BoxFuture<'a, big_sync_core::rpc::BigSyncRpcResult<big_sync_core::rpc::PeerSummaryResult>>
    {
        let fut = self.inner.peer_summary(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn sub_parts<'a>(
        &'a self,
        req: big_sync_core::rpc::SubPartsRequest,
    ) -> BoxFuture<
        'a,
        big_sync_core::rpc::BigSyncRpcResult<
            Result<mpsc::Receiver<big_sync_core::rpc::SubEvent>, big_sync_core::rpc::SubPartsError>,
        >,
    > {
        let fut = self.inner.sub_parts(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}
