//! TODO: rate limiting

use crate::interlude::*;

use crate::mpsc::Receiver;
use crate::part_store::{CursorIndex, ObjPayload, PeerPartCursors};

pub trait BigSyncRpcClient<K: FutureForm> {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> K::Future<'a, BigSyncRpcResult<PeerSummaryResult>>;

    fn sub_parts<'a>(
        &'a self,
        req: SubPartsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<Receiver<SubEvent>, SubPartsError>>>;
}

impl<K: FutureForm, T: BigSyncRpcClient<K> + ?Sized> BigSyncRpcClient<K> for Arc<T> {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> K::Future<'a, BigSyncRpcResult<PeerSummaryResult>> {
        (**self).peer_summary(req)
    }

    fn sub_parts<'a>(
        &'a self,
        req: SubPartsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<Receiver<SubEvent>, SubPartsError>>> {
        (**self).sub_parts(req)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
pub enum SubStreamKind {
    Member,
    Objects,
}

structstruck::strike! {
    pub struct PeerSummaryRequest {
        pub parts: Set<PartId>,
    }
}

structstruck::strike! {
    pub struct PeerSummaryResult {
        /// Only known partitions where the requestor has
        /// accessed are returned here. If an expected partition
        /// is missing, either access is denied or the peer doesn't
        /// know of the partitions yet. Request again with backoff
        /// in case peer learns of partitions from the current node
        pub parts: Map<
            PartId,
            pub struct PartSummary {
                pub latest_cursor: CursorIndex,
                pub member_count: u64,
            }
        >,
    }
}

structstruck::strike! {
    pub struct SubPartsRequest {
        pub parts: Vec<
            pub struct PartitionStreamCursorRequest {
                pub part_id: PartId,
                pub cursors: PeerPartCursors,
            }
        >,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, Serialize, Deserialize)]]
    pub enum SubEvent {
        MemberEvent(pub struct PartitionMemberEvent {
            pub cursor: CursorIndex,
            pub part_id: PartId,
            pub deets: pub enum PartitionMemberEventDeets {
                MemberUpsert(ObjId),
                MemberRemove(ObjId),
            },
        }),
        ObjChangeEvent(pub struct ObjChangeEvent {
            pub cursor: CursorIndex,
            pub part_id: PartId,
            pub obj_id: ObjId,
            pub payload: ObjPayload,
        }),
        ReplayComplete { stream: SubStreamKind },
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum RpcError {
    /// TransportError
    TransportError,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SubPartsError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartId> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
