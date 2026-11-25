//! The network: every outward call and its answer is a message in one bag. Ops deliver any
//! of them (reordering and delay), drop, duplicate, or tamper with a page.

use crate::interlude::*;

use crate::fingerprint::FingerprintSeed;
use crate::part_store::bucket_summary::BucketSummary;
use crate::replay::wire::PageEvent;
use crate::replay::wire::PageSession;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::TargetId;
use crate::replay::wire::UpdateSession;
use crate::replay::wire::Verdict;
use crate::rev::Rev;
use crate::sync::peer::CallId;
use crate::sync::peer::SummaryReply;
use crate::sync::rpc::bucket::LeafBucketPage;
use crate::sync::rpc::bucket::LeafBucketRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Summary,
    Session,
    Round,
    List,
    Leaf,
}

#[derive(Debug, Clone)]
pub(super) enum Req {
    Summary(Map<PartKey, Rev>),
    Session(UpdateSession),
    Round(PageSession),
    /// One stage of a listing: the driver relists past pages that filter to nothing, so a
    /// call can take several stages.
    List {
        part: PartKey,
        offset: BuckId,
        since: Rev,
        level: BuckLevel,
    },
    Leaf {
        part: PartKey,
        since: Rev,
        buckets: Vec<LeafBucketRequest>,
        seed: FingerprintSeed,
    },
}

#[derive(Debug, Clone)]
pub(super) enum Resp {
    Summary(SummaryReply),
    Session(Result<SessionUpdated, SessionError>),
    Round(PageSession, Result<SessionPage, SessionError>),
    /// `None`: the peer may not serve the part.
    List {
        part: PartKey,
        offset: BuckId,
        since: Rev,
        level: BuckLevel,
        raw: Option<Vec<BucketSummary>>,
    },
    Leaf {
        part: PartKey,
        seed: FingerprintSeed,
        raw: Option<Map<BuckId, LeafBucketPage>>,
    },
}

#[derive(Debug, Clone)]
pub(super) enum Body {
    Req(Req),
    Resp(Resp),
}

/// A page the peer tampered with. The machine must reject the whole page: nothing of it
/// applies, not even its verdicts.
#[derive(Debug, Clone, Copy)]
pub(super) enum Hostile {
    Always,
    /// An object-target removal: hostile only while the client still holds the target as an
    /// object route (a dropped route's events are legitimate and skipped).
    IfObject(TargetId),
}

#[derive(Debug, Clone)]
pub(super) struct Msg {
    pub call: CallId,
    pub body: Body,
    pub hostile: Option<Hostile>,
}

impl Msg {
    pub(super) fn kind(&self) -> Kind {
        match &self.body {
            Body::Req(Req::Summary(..)) | Body::Resp(Resp::Summary(..)) => Kind::Summary,
            Body::Req(Req::Session(..)) | Body::Resp(Resp::Session(..)) => Kind::Session,
            Body::Req(Req::Round(..)) | Body::Resp(Resp::Round(..)) => Kind::Round,
            Body::Req(Req::List { .. }) | Body::Resp(Resp::List { .. }) => Kind::List,
            Body::Req(Req::Leaf { .. }) | Body::Resp(Resp::Leaf { .. }) => Kind::Leaf,
        }
    }
}

/// Tamper with an answered round: one violation of law 3, chosen by `kind` (its low two bits
/// pick the violation, bit 2 a variant of it). `object_target` is a target of the round
/// the client holds as an object route, if any. Returns what the machine must do with the page,
/// or `None` when the page offers nothing to tamper with.
pub(super) fn tamper(
    request: &PageSession,
    page: &mut SessionPage,
    kind: u8,
    object_target: Option<TargetId>,
    obj: ObjKey,
) -> Option<Hostile> {
    // `(target, asked cursor, resume)` of a target answered with events
    let window_of = |wanted: Option<TargetId>| {
        page.verdicts
            .iter()
            .find_map(|(id, verdict)| match verdict {
                Verdict::Events { resume, .. } if wanted.is_none_or(|wanted| wanted == *id) => {
                    let (_, asked) = request.targets.iter().find(|(target, _)| target == id)?;
                    Some((*id, *asked, *resume))
                }
                _ => None,
            })
    };
    let low = kind & 4 == 0;
    let change = |rev, targets| PageEvent::Changed {
        rev,
        obj_key: obj.clone(),
        targets,
        payload: serde_json::json!(0),
    };
    let (event, hostile) = match kind & 3 {
        // names no target
        0 => (change(1, vec![]), Hostile::Always),
        // names a target the round did not ask for
        1 => {
            let unasked = request.targets.iter().map(|(id, _)| *id).max()? + 1000;
            (change(1, vec![unasked]), Hostile::Always)
        }
        // a row outside its target's window: above `resume`, or at the asked cursor
        2 => {
            let (id, asked, resume) = window_of(None)?;
            (
                change(if low { resume + 1 } else { asked }, vec![id]),
                Hostile::Always,
            )
        }
        // a removal on an object route, inside its window when it has one
        _ => {
            let id = object_target?;
            let (id, asked, resume) = window_of(Some(id))?;
            let rev = if resume > asked { resume } else { asked + 1 };
            (
                PageEvent::Removed {
                    rev,
                    obj_key: obj,
                    target: id,
                },
                Hostile::IfObject(id),
            )
        }
    };
    page.events.push(event);
    Some(hostile)
}
