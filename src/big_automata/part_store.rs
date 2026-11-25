//! The part store: objects and their payloads, their membership in parts, principals' access to
//! parts, the bucket index over each part's members, and peers' cursors, all stamped by one log.
//!
//! **The log is the global cursor.** It is an okv-shaped table of [`LogKey`] → [`LogEntry`]:
//! one row per membership (`Member { obj, part }`) and per grant (`Access { part, principal }`),
//! collapsed to the latest write, with removals and revocations kept as tombstones. Every
//! committed write transaction that changes a row stamps all its rows with one new [`Rev`], and
//! that rev is the cursor peers resume from. Membership and access share the cursor by
//! construction, so how far a principal is behind on a part is a count of log rows in a rev
//! window ([`PartStoreServe::part_summaries`]).
//!
//! - **A payload change re-stamps the object's member rows** at the new rev, so a part's replay
//!   carries content changes as row changes and an object route is the key range of the
//!   object's rows. There is no separate object lane; the cost is one row write per part the
//!   object is in.
//! - **Membership before payload is pending.** Adding an object that has no payload yet writes
//!   no log row and no bucket entry; when the payload arrives, every pending membership goes
//!   live in that transaction, stamped once. A live membership always has a payload; payload
//!   removal is explicit: remove the object from every part, then drop the payload.
//! - **A write that changes nothing stamps nothing:** re-writing the payload an object holds,
//!   adding a live member, removing a non-member, granting the level a principal has.
//!
//! Three traits, one per audience: [`PartStoreWrite`] for the embedder that owns the data,
//! [`PartStoreRead`] for the sync client, [`PartStoreServe`] for the responder (which assembles
//! pages, sessions and budgets on top; none of that is here).

use crate::interlude::*;

use crate::part_store::bucket_summary::BucketSummary;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;

pub mod bucket_summary;
pub mod memory;

#[cfg(any(test, feature = "test-support"))]
pub mod laws;
#[cfg(any(test, feature = "test-support"))]
pub mod oracle;

#[cfg(test)]
mod tests;

/// An object's content.
pub type ObjPayload = serde_json::Value;

/// A payload's identity, computed once when the payload is written: the live fingerprint of a
/// bucket sums its members' digests, so a summary never rereads payloads.
///
/// A blake3 hash of the payload's RFC 8785 (JCS) canonical JSON, the same canonicalization
/// `utils_rs::hash::hash_obj` uses. Canonical, because map key order is otherwise insertion
/// order whenever any crate in the build turns on `serde_json`'s `preserve_order`, and two
/// peers holding the same payload must agree on its digest. The canonical form is streamed
/// into the hasher, never built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PayloadDigest(pub [u8; 32]);

impl PayloadDigest {
    /// Fails for a payload JCS cannot represent: an integer outside the I-JSON safe range
    /// (beyond 2^53 - 1 in magnitude). Payloads arrive from peers, so this is input
    /// validation, and the store refuses the write rather than inventing a digest.
    pub fn of(payload: &ObjPayload) -> Res<Self> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"big-sync-payload-digest-v3\0");
        json_canon::to_writer(&mut hasher, payload)
            .wrap_err("payload has no RFC 8785 canonical form")?;
        Ok(Self(*hasher.finalize().as_bytes()))
    }
}

/// What a principal may do with a part. Every level may read (fetch) the part; the finer
/// levels are recorded for the embedder and not interpreted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AccessLevel {
    Pull,
    Read,
    Write,
    Admin,
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]]
    /// A log row's key. Members sort object first, so an object's rows are one key range.
    pub enum LogKey {
        Member { obj: ObjKey, part: PartKey },
        Access { part: PartKey, principal: PeerKey },
    }
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq)]]
    /// A live log row's value. Removals and revocations are tombstones (`None`).
    pub enum LogEntry {
        Member(pub struct Membership {
            /// The rev at which the membership went live; re-stamps keep it.
            pub added_at: Rev,
        }),
        Access(AccessLevel),
    }
}

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq)]]
    /// Which log rows a read is about.
    pub enum LogSelector {
        /// Every row, member and access: a full replay of the store.
        All,
        /// Member rows by route. A row is selected when a route it is on has a bound below the
        /// row's rev: a part route covers the part's member rows, an object route the object's
        /// member rows in every part. Access rows are on no route.
        ///
        /// A removal carries no news to a reader that never saw the membership: a removed row
        /// is skipped on a route whose cursor (the larger of its bound and the read's `after`)
        /// is below the rev at which the membership *first* went live. A reader that saw any
        /// incarnation live did so at or after that rev, so its cursor is never below it and
        /// it always gets the removal. The first presence, not the latest: a reader that saw
        /// the first incarnation must hear the removal of a later one, because the log
        /// collapsed the removal in between away.
        Routes(pub struct LogRoutes {
            pub parts: BTreeMap<PartKey, Rev>,
            pub objects: BTreeMap<ObjKey, Rev>,
        }),
    }
}

impl LogSelector {
    /// Whether a read of `(after, ..]` hands out `key`'s row at `rev`. `removed_added_at` is
    /// `Some(first_added_at)` when the row is a removed membership: the rev at which that
    /// membership first went live.
    pub fn selects(
        &self,
        key: &LogKey,
        rev: Rev,
        after: Rev,
        removed_added_at: Option<Rev>,
    ) -> bool {
        match (self, key) {
            (Self::All, _) => true,
            (Self::Routes(routes), LogKey::Member { obj, part }) => {
                let (on_part, on_object) = routes.carriers(obj, part, rev, after, removed_added_at);
                on_part || on_object
            }
            (Self::Routes(_), LogKey::Access { .. }) => false,
        }
    }
}

impl LogRoutes {
    /// Which routes carry the member row `(obj, part)` at `rev` to a read of `(after, ..]`:
    /// `(part route, object route)`. A route carries the row when its bound is below `rev` and
    /// the removal rule of [`LogSelector::Routes`] lets it through. The two answers are kept
    /// apart because an [authorized read](PartStoreServe::read_authorized) admits them on
    /// different grounds.
    pub fn carriers(
        &self,
        obj: &ObjKey,
        part: &PartKey,
        rev: Rev,
        after: Rev,
        removed_added_at: Option<Rev>,
    ) -> (bool, bool) {
        let on_route = |bound: Rev| {
            rev > bound && removed_added_at.is_none_or(|added_at| added_at <= bound.max(after))
        };
        (
            self.parts.get(part).copied().is_some_and(on_route),
            self.objects.get(obj).copied().is_some_and(on_route),
        )
    }
}

/// What a log row costs against [`Limits::max_bytes`](crate::rev::Limits): its key bytes, plus
/// 8 for a membership and 1 for a grant (a tombstone's value costs nothing).
pub fn log_row_bytes(key: &LogKey, value: Option<&LogEntry>) -> usize {
    let key_bytes = match key {
        LogKey::Member { obj, part } => obj.0.as_bytes().len() + part.0.as_bytes().len(),
        LogKey::Access { part, principal } => {
            part.0.as_bytes().len() + principal.0.as_bytes().len()
        }
    };
    let value_bytes = match value {
        None => 0,
        Some(LogEntry::Member(_)) => 8,
        Some(LogEntry::Access(_)) => 1,
    };
    key_bytes + value_bytes
}

/// An object as the sync client sees it.
///
/// A payload promotes every pending membership in the transaction it arrives, so an
/// object's memberships are either all live (it has content) or all pending (it has none), and
/// `parts` names them either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjState {
    /// The payload and its stored digest; `None` while the object only has pending memberships.
    pub content: Option<(ObjPayload, PayloadDigest)>,
    /// The parts the object is a member of: live with content, pending without.
    pub parts: BTreeSet<PartKey>,
}

/// A part as a principal sees it from a cursor: what a peer decides its sync strategy on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartSummary {
    /// The rev of the part's latest member row. The cursor strat resumes from it, and a bucket
    /// walk hands over to the cursor strat at it: the walk reads state no older than this
    /// summary, so replaying from here may repeat rows but never skips one. Access rows do not
    /// move it: a peer must not learn when other principals' access changed.
    pub latest_cursor: Rev,
    /// The part's live members.
    pub member_count: u64,
    /// The deepest bucket level the store keeps summaries for; the asker picks its working
    /// level from this and `member_count`.
    pub deepest_bucket_level: BuckLevel,
    pub dirty: PartDirtyCount,
}

/// How far a principal is behind on a part: the part's log rows past a cursor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartDirtyCount {
    /// Member rows of the part, live or removed.
    pub member_changes: u64,
    /// The principal's access row on the part, granted or revoked: 0 or 1.
    pub access_changes: u64,
}

/// Whether a principal may read a route. Each answer is distinct: a part the store does not know
/// is not a denial, and a denial is not an empty read.
///
/// An object route is `Permitted` while the object is a live member of a part the principal
/// may read, and `Denied` otherwise. `UnknownPart` does not occur for an object route: an object
/// the store does not know is `Denied` like one it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadVerdict {
    Permitted,
    Denied,
    UnknownPart,
}

/// One [authorized read](PartStoreServe::read_authorized): each route's verdict and the rows
/// they let through, all taken from one snapshot of the store. The caller owns it and reuses
/// it across reads like a [`Page`]: a read clears and refills every field.
///
/// `rows.through` is the progress of the permitted routes only. A route answered `Denied` or
/// `UnknownPart` read nothing, so its reader keeps its cursor where it was: should access be
/// granted later, reading from that cursor returns the backlog.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AuthorizedPage {
    /// Each part route's verdict, in the routes' key order.
    pub parts: Vec<ReadVerdict>,
    /// Each object route's verdict, in the routes' key order.
    pub objects: Vec<ReadVerdict>,
    pub rows: Page<LogKey, LogEntry>,
}

/// One entry of a bucket leaf: a member of the part in that bucket, live with its digest or
/// removed (dead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEntry {
    pub obj: ObjKey,
    pub live: Option<PayloadDigest>,
}

/// The embedder's writes. Every method stages one batch in the caller's transaction, and the
/// transaction commits all of them at once: nothing is visible before commit, and everything
/// is after. A committed transaction that changed any log row stamps them all with one new rev.
///
/// Batches are slices of keys (cheap to clone) or owned payloads; there is no per-key call.
pub trait PartStoreWrite<Async: FutureForm> {
    type Tx;

    /// Make sure the parts exist. Membership and grants create their part too.
    fn ensure_parts<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        parts: &'a [PartKey],
    ) -> Async::Future<'a, Res<()>>;
    /// Set objects' payloads. A changed payload re-stamps the object's member rows and promotes
    /// its pending memberships; an unchanged one is a no-op.
    fn set_payloads<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        payloads: Vec<(ObjKey, ObjPayload)>,
    ) -> Async::Future<'a, Res<()>>;
    /// Add objects to parts, `(obj, part)`. An object without a payload is pending in the part.
    fn add_members<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        members: &'a [(ObjKey, PartKey)],
    ) -> Async::Future<'a, Res<()>>;
    /// Remove objects from parts, `(obj, part)`. A live membership leaves a tombstone; a pending
    /// one leaves nothing. The payload stays.
    fn remove_members<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        members: &'a [(ObjKey, PartKey)],
    ) -> Async::Future<'a, Res<()>>;
    /// Grant principals access to parts, `(part, principal, level)`.
    fn grant_access<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        grants: &'a [(PartKey, PeerKey, AccessLevel)],
    ) -> Async::Future<'a, Res<()>>;
    /// Revoke principals' access to parts, `(part, principal)`; a revocation is a tombstone.
    fn revoke_access<'a>(
        &'a self,
        tx: &'a mut Self::Tx,
        revokes: &'a [(PartKey, PeerKey)],
    ) -> Async::Future<'a, Res<()>>;
}

/// The sync client's reads, batched, and its cursor writes.
pub trait PartStoreRead<Async: FutureForm> {
    /// The objects' states, in the order asked; `None` for an object with no payload (unknown,
    /// or only pending).
    fn obj_states<'a>(
        &'a self,
        objs: &'a [ObjKey],
    ) -> Async::Future<'a, Res<Vec<Option<ObjState>>>>;
    /// The part's bucket summaries, in the order asked; a bucket with no members is the empty
    /// summary.
    fn bucket_summaries<'a>(
        &'a self,
        part: &'a PartKey,
        ids: &'a [BuckId],
    ) -> Async::Future<'a, Res<Vec<BucketSummary>>>;
    /// The part's live members.
    fn member_count<'a>(&'a self, part: &'a PartKey) -> Async::Future<'a, Res<u64>>;
    /// The peer's cursors on the parts, in the order asked.
    fn peer_part_cursors<'a>(
        &'a self,
        peer: &'a PeerKey,
        parts: &'a [PartKey],
    ) -> Async::Future<'a, Res<Vec<Option<Rev>>>>;
    /// Move peers' cursors, `(peer, part, cursor)`. A cursor only advances: a lower one than
    /// stored is ignored.
    fn set_peer_part_cursors<'a>(
        &'a self,
        cursors: &'a [(PeerKey, PartKey, Rev)],
    ) -> Async::Future<'a, Res<()>>;
}

/// The responder's reads. Every one answers a principal, and every one decides access and
/// reads under one snapshot (one lock in memory, one read transaction in SQLite), so no grant or
/// revocation can commit between deciding and reading.
///
/// Access is decided from the principal's *current* access rows, never from history: an access
/// row records a change, it does not grant the rows written before it. A revocation cuts the
/// principal off everything of the part at once, rows, buckets and leaves it had not read yet
/// included, and a grant opens the part's whole backlog from the reader's cursor.
///
/// A part the principal may not read and a part the store does not know answer alike (`None`)
/// wherever the answer goes on the wire, so a refusal never confirms that a part exists; only
/// [`read_authorized`](Self::read_authorized)'s verdicts tell them apart.
///
/// The log as a [`RevSource`](crate::rev::RevSource) is not here: that read is unauthorized and belongs to the store's
/// local consumers (its embedder, indexes, replays), never to a peer.
pub trait PartStoreServe<Async: FutureForm> {
    /// The log rows `principal` may read on `routes` in `(after, until]`, with each route's
    /// verdict, into `page`.
    ///
    /// - A part route is `Permitted` while the principal has an access row on the part, and
    ///   carries the part's member rows only then.
    /// - An object route is `Permitted` while the object is a live member of a part the
    ///   principal may read ([`ReadVerdict`]). It then carries the object's member rows in the
    ///   parts the principal may read, and no others: a row names its part, so handing out a
    ///   row of an unreadable part would disclose that membership. A denied object route
    ///   carries nothing, not even the removal from its last readable part; that part's own
    ///   route carries it.
    /// - Access rows are on no route.
    ///
    /// `after`, `until`, `limits` and the page's shape are
    /// [`RevSource::read`](crate::rev::RevSource::read)'s.
    fn read_authorized<'a>(
        &'a self,
        principal: &'a PeerKey,
        routes: &'a LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut AuthorizedPage,
    ) -> Async::Future<'a, Res<()>>;
    /// For each `(part, since)`, the part's summary as `principal` sees it from `since`, in the
    /// order asked; `None` for a part it may not read or the store does not know.
    fn part_summaries<'a>(
        &'a self,
        principal: &'a PeerKey,
        parts: &'a [(PartKey, Rev)],
    ) -> Async::Future<'a, Res<Vec<Option<PartSummary>>>>;
    /// The objects' states as `principal` may see them, in the order asked: the content, and
    /// the live parts it may read. `None` for an object that is not a live member of any part
    /// it may read: content is only ever readable through a part.
    fn readable_obj_states<'a>(
        &'a self,
        principal: &'a PeerKey,
        objs: &'a [ObjKey],
    ) -> Async::Future<'a, Res<Vec<Option<ObjState>>>>;
    /// The part's non-empty buckets changed past `since`, in bucket order, from `from` up to
    /// `to_level`, at most `limit`; `None` for a part `principal` may not read.
    fn bucket_list<'a>(
        &'a self,
        principal: &'a PeerKey,
        part: &'a PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> Async::Future<'a, Res<Option<Vec<BucketSummary>>>>;
    /// For each `(bucket, after)`, the part's members (live and dead) in that bucket in
    /// (deepest bucket, key) order after `after`, at most `limit`, in the order asked; a bucket
    /// unchanged past `since` has none. `None` for a part `principal` may not read. One call
    /// answers a whole leaf request, so all its buckets come from one snapshot.
    fn bucket_leaves<'a>(
        &'a self,
        principal: &'a PeerKey,
        part: &'a PartKey,
        bucks: &'a [(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
    ) -> Async::Future<'a, Res<Option<Vec<Vec<LeafEntry>>>>>;
}
