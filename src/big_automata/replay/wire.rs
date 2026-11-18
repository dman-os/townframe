//! What the replay session protocol says on the wire. The laws these messages obey are in the
//! [module docs](super).
//!
//! These types are the protocol's compatibility surface: a field's meaning never changes once
//! shipped, and new behaviour arrives as new optional fields or new messages.

use crate::interlude::*;

use crate::part_store::ObjPayload;
use crate::rev::Rev;

/// A session, named by the client. A client opening a session after the server forgot the old
/// one picks a fresh id, so nothing of the forgotten session can be mistaken for the new one.
pub type SessionId = Uuid;

/// A target within one session, named by the client and never reused in that session.
pub type TargetId = u32;

/// The ordering of a session's updates. Generation 0 opens a session.
pub type Generation = u64;

/// What a target follows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Route {
    /// Every membership change in the part, with the content of its live members.
    Part { part_key: PartKey },
    /// One object's content changes, in the parts the client may read.
    Object { obj_key: ObjKey },
}

/// Open a session (generation 0, unknown id) or change its target set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSession {
    pub session_id: SessionId,
    pub generation: Generation,
    pub additions: Vec<(TargetId, Route)>,
    /// Applied before `additions`. An id the session does not hold is a no-op.
    pub removals: Vec<TargetId>,
}

/// The answer to an [`UpdateSession`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionUpdated {
    /// The generation the server holds after the update. Equal to the request's generation
    /// when it applied; the newer generation it already held when it did not.
    pub generation: Generation,
    /// The additions of `generation` that the server did not take. Every other addition of
    /// that generation is held.
    pub refused: Vec<TargetId>,
}

/// One round: page some of the session's targets from the client's cursors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageSession {
    pub session_id: SessionId,
    pub targets: Vec<(TargetId, Rev)>,
    /// The most log rows the page may carry across all its targets. A single revision is
    /// never split, so a page may exceed it by the rows of one revision.
    pub limit: u32,
    /// How long the server may wait for rows when none of the targets has any.
    pub hold_ms: u32,
}

/// The answer to a [`PageSession`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    /// The rows of the targets answered with [`Verdict::Events`], in revision order.
    pub events: Vec<PageEvent>,
    /// One verdict per requested target.
    pub verdicts: Vec<(TargetId, Verdict)>,
}

/// One row of a page, naming the requested targets it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageEvent {
    /// The object is a live member of the parts behind `targets`, and this is its content.
    Changed {
        rev: Rev,
        obj_key: ObjKey,
        targets: Vec<TargetId>,
        payload: ObjPayload,
    },
    /// The object left the part behind `target`.
    Removed {
        rev: Rev,
        obj_key: ObjKey,
        target: TargetId,
    },
}

/// What a round answered about one target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// The page carries every row of the target in `(cursor, resume]`. `drained`: the read
    /// reached the newest revision of the round's snapshot.
    Events { resume: Rev, drained: bool },
    /// The client may not read the target now, or its route is unknown; the two answer alike.
    Refused,
}

/// Drop a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSession {
    pub session_id: SessionId,
}

/// Why a session request was not answered.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum SessionError {
    /// the server does not hold this session
    UnknownSession,
    /// the round named a target the session does not hold
    UnknownTarget { target: TargetId },
    /// the request is over a server limit
    Limit,
    /// the request is malformed: {0}
    Invalid(String),
}
