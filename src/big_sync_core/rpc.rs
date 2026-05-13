//! TODO: rate limiting

use crate::interlude::*;

use crate::mpsc::Receiver;
use crate::part_store::{CursorIndex, ObjPayload};

pub trait BigSyncRpcClient<K: FutureForm> {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    fn sub_parts<'a>(
        &'a self,
        req: SubPartsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>>;
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
            pub struct PartStreamCursorRequest {
                pub part_id: PartId,
                pub cursor: CursorIndex,
            }
        >,
    }
}

structstruck::strike! {
    pub struct PartPage {
        pub events: Vec<pub enum PartEvent {
            #![derive(Debug, Clone)]
            MemberEvent(PartMemberEvent),
            ObjChangeEvent(ObjChangeEvent)
        }>,
        pub next_cursor: Option<CursorIndex>,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, Serialize, Deserialize)]]
    pub enum SubEvent {
        MemberEvent(pub struct PartMemberEvent {
            pub cursor: CursorIndex,
            pub part_id: PartId,
            pub deets: pub enum PartMemberEventDeets {
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
        ReplayComplete ,
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum RpcError {
    /// TransportError
    TransportError,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum ListPartsError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartId> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
