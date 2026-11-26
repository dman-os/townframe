use crate::interlude::*;

use crate::store::ObjPayload;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplayTarget {
    Part {
        part_key: PartKey,
        cursor: CursorIndex,
    },
    Object {
        obj_key: ObjKey,
        /// Since only a single version of an object is kept, this just
        /// means that the object is not returned at part of the replay
        /// if there have been no updates to it since.
        cursor: CursorIndex,
    },
}

impl ReplayTarget {
    pub fn cursor(&self) -> CursorIndex {
        match self {
            Self::Part { cursor, .. } | Self::Object { cursor, .. } => *cursor,
        }
    }
}

pub type ReplaySessionId = Uuid;
pub type ReplaySubscriptionId = Uuid;
pub type ReplayTargetId = u16;
pub type ReplayRequestId = Uuid;

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    /// Update a replay session.
    /// Used to also create a new session by using a new session id.
    pub struct UpdateReplaySessionRequest {
        session_id: ReplaySessionId,
        subscription_id: ReplaySubscriptionId,
        /// The generation to increment the session to.
        generation: u64,
        additions: Vec<pub struct ReplaySubscriptionTargetEntry {
            pub id: ReplayTargetId,
            pub target: ReplayTarget,
        }>,
        /// Removals are applied before additions, so one request may remove a target and
        /// re-add the same part under a fresh target id. An `Update` no newer than the generation
        /// the responder holds is not applied, and the answer names the generation that was held
        /// instead.
        removals: Vec<ReplayTargetId>,
    }
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct UpdateReplaySessionResponse {
        /// Session update coroboration
        generation: u64,
        rejected: Vec<(
            ReplayTargetId,
            // FIXME: strike! bug prevents nested enums
            UpdateTargetVerdict
        )>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpdateTargetVerdict {
    /// Part unknown or unauthorized
    UnknownPart,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageReplaySessionRequest {
    session_id: ReplaySessionId,
    subscription_id: ReplaySubscriptionId,
    request_id: ReplayRequestId,
    supersede: Option<ReplayRequestId>,
    targets: Vec<(ReplayTargetId, CursorIndex)>,
    limit: u32,
    hold_ms: u32,
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct PageReplaySessionResponse {
        /// The events this page carries, collapsed per object and ordered by position.
        pub events: Vec<pub enum AbbreviatedPartEvent {
            Changed(pub struct ObjChanged {
                pub cursor: CursorIndex,
                pub part_keys: Vec<ReplayTargetId>,
                pub obj_key: ObjKey,
                // NOTE: IRPC uses postcard encoding
                // which doesn't support serde_json::Value
                // types
                #[serde(
                    serialize_with = "crate::utils::value_as_cbor",
                    deserialize_with = "crate::utils::value_from_cbor"
                )]
                pub payload: ObjPayload,
            }),
            Removed(pub struct ObjRemovedFromPart {
                pub cursor: CursorIndex,
                pub part_key: ReplayTargetId,
                pub obj_key: ObjKey,
            }),
        }>,
        pub verdicts: Vec<(
            ReplayTargetId,
            PageTargetVerdict
        )>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PageTargetVerdict {
    Events { resume: CursorIndex, drained: bool },
    UnknownPart,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CloseReplaySession {
    session_id: ReplaySessionId,
    subscription_id: ReplaySubscriptionId,
}
