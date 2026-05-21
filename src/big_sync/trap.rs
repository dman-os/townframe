use crate::interlude::*;

use big_sync_core::{
    mpsc,
    part_store::{CursorIndex, ObjPayload, PartStoreReadOnly},
    rpc::{
        BigSyncRpcClient, BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest,
        LeafBucketResult, LeafBucketsError, LeafBucketsRequest, ListPartsError, PeerSummaryRequest,
        PeerSummaryResult, SubEvent, SubPartsRequest,
    },
    BuckId, ObjId, PartId, PeerId,
};
use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;

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

impl PartStoreReadOnly<Sendable> for TrappedPartStore {
    #[tracing::instrument(skip(self))]
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        let fut = self.inner.member_count(part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    #[tracing::instrument(skip(self))]
    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        let fut = self.inner.obj_payload(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    #[tracing::instrument(skip(self))]
    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        let fut = self.inner.obj_parts(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    #[tracing::instrument(skip(self))]
    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, CursorIndex> {
        let fut = self.inner.get_peer_part_cursor(peer_id, part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    #[tracing::instrument(skip(self))]
    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartId,
        id: BuckId,
    ) -> BoxFuture<'a, BucketSummary> {
        let fut = self.inner.get_bucket_summary(part_id, id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}

// impl PartStore<Sendable> for TrappedPartStore {
//     #[tracing::instrument(skip(self, payload), fields(part_count = parts.len()))]
//     fn upsert_obj<'a>(
//         &'a self,
//         obj_id: ObjId,
//         payload: &ObjPayload,
//         parts: &[PartId],
//     ) -> BoxFuture<'a, ()> {
//         let lease = self.lease.take();
//         let fut = self
//             .inner
//             .upsert_obj(obj_id, payload.clone(), parts.into(), lease);
//         Sendable::from_future(self.trap.run_or_trap(fut))
//     }
//
//     #[tracing::instrument(skip(self), fields(part_count = parts.len()))]
//     fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
//         let lease = self.lease.take();
//         let fut = self.inner.add_obj_to_parts(obj_id, parts.into(), lease);
//         Sendable::from_future(self.trap.run_or_trap(fut))
//     }
//
//     #[tracing::instrument(skip(self))]
//     fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
//         let lease = self.lease.take();
//         let fut = self.inner.remove_obj_from_part(obj_id, part_id, lease);
//         Sendable::from_future(self.trap.run_or_trap(fut))
//     }
//
//     #[tracing::instrument(skip(self))]
//     fn set_peer_part_cursor<'a>(
//         &'a self,
//         peer_id: PeerId,
//         part_id: PartId,
//         cursor: CursorIndex,
//     ) -> BoxFuture<'a, ()> {
//         let fut = self.inner.set_peer_part_cursor(peer_id, part_id, cursor);
//
//         Sendable::from_future(self.trap.run_or_trap(fut))
//     }
// }

pub struct TrappedRpcClient {
    pub trap: TaskTrap,
    pub inner: Arc<dyn crate::rpc::HostBigRpcClient>,
}

impl BigSyncRpcClient<Sendable> for TrappedRpcClient {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> BoxFuture<'a, BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        let fut = self.inner.peer_summary(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn sub_parts<'a>(
        &'a self,
        req: SubPartsRequest,
    ) -> BoxFuture<'a, BigSyncRpcResult<Result<mpsc::Receiver<SubEvent>, ListPartsError>>> {
        let fut = self.inner.sub_parts(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn get_changed_buckets<'a>(
        &'a self,
        req: GetChangedBucketsRequest,
    ) -> BoxFuture<'a, BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        let fut = self.inner.get_changed_buckets(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn leaf_buckets<'a>(
        &'a self,
        req: LeafBucketsRequest,
    ) -> BoxFuture<'a, BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        let fut = self.inner.leaf_buckets(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}
