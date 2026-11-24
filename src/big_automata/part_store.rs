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
//!   removal is explicit: remove the object from every part, then drop the payload
//!   ([`PartStoreWrite::drop_payloads`]). [`PartStoreRead::partless_objects`] lists the objects
//!   that are ready for it.
//! - **Dropping a payload** (laws 1 to 4 below). It changes no log row, so it stamps nothing.
//! - **A write that changes nothing stamps nothing:** re-writing the payload an object holds,
//!   adding a live member, removing a non-member, granting the level a principal has.
//!
//! # Payload laws
//!
//! 1. **A dropped payload is gone.** After the transaction that drops an object's payload
//!    commits, [`obj_states`](PartStoreRead::obj_states) and
//!    [`readable_obj_states`](PartStoreServe::readable_obj_states) answer `None` for it and no
//!    authorized read carries it (it has no live row to carry it for). Its tombstones stay in
//!    the log, so removals still reach routes. Dropping a payload an object does not hold (never
//!    set, already dropped, or an object with only pending memberships) is a no-op.
//! 2. **Only a part-less object's payload may be dropped.** Dropping the payload of an object
//!    that holds one and is a live member of any part is a caller bug and panics, the same
//!    panic in every backend. It is not an `Err`: the order (remove from every part, then
//!    drop) is the embedder's own sequencing, a store that returned an error could not be
//!    recovered from by anything but fixing the caller, and an `Err` at commit time could not be
//!    rolled back by the memory store. The check sees the transaction's own earlier batches, so
//!    `remove_members` then `drop_payloads` in one transaction is valid and the reverse order
//!    is the bug. A backend must not commit a transaction after its drop panicked.
//! 3. **A part-less object is listed until its payload is dropped.**
//!    [`partless_objects`](PartStoreRead::partless_objects) lists, in key order, exactly the
//!    objects that hold a payload and are a live member of no part: an object whose memberships
//!    were all removed, and nothing else (a live member is not listed; an object with no payload
//!    is not listed, pending memberships included). It leaves the list when its payload is
//!    dropped, and when it is added to a part again.
//! 4. **Dropping is a batch of its transaction.** It is invisible before commit, gone with a
//!    rollback, and after commit it has happened in order with the transaction's other batches
//!    (a payload set after the drop in the same transaction survives; a membership added after
//!    the drop is pending).
//!
//! # Generation laws
//!
//! A store has a [`Generation`], the identity of its log, which a client's cursor is a position
//! in: a peer that kept its identity but not its data (a recreated store, a restore from an older
//! backup) would otherwise have its clients resume from cursors that mean nothing on the new
//! log, silently missing the rows stamped below them.
//!
//! 1. **A store has a generation from its creation**, minted then, so plain data loss needs no
//!    operator action: no two stores share one, and it is never [`Generation::default`].
//! 2. **It is stable.** Writes, rollbacks and reads do not change it, and neither does a
//!    restart of the store over the same files.
//! 3. **Minting changes it and nothing else.** [`PartStoreWrite::mint_generation`] gives a
//!    generation the store never had, and stamps no rev, wakes no watch and changes no row. A
//!    restore from a backup brings back the backup's files, generation included, so the
//!    operator mints after one: the store cannot tell a restored file from a live one.
//! 4. **Every part summary carries the current generation**, a mint included from the next
//!    answer on ([`PartSummary::generation`]); a client compares it with the one its cursor was
//!    stored under (`PartLifecycle` law 10).
//!
//! Three traits, one per audience: [`PartStoreWrite`] for the embedder that owns the data,
//! [`PartStoreRead`] for the sync client, [`PartStoreServe`] for the responder (which assembles
//! pages, sessions and budgets on top; none of that is here).

use crate::interlude::*;

use crate::part_store::bucket_summary::BucketSummary;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevWatch;

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
        /// Removals are never filtered: a removal after a route's bound is delivered whatever
        /// rev the membership went live at. A reader's persisted cursor only passes settled
        /// work, so it may already hold a membership that went live above that cursor;
        /// skipping the removal for want of a cursor that "saw" the membership would strand
        /// it there.
        Routes(pub struct LogRoutes {
            pub parts: BTreeMap<PartKey, Rev>,
            pub objects: BTreeMap<ObjKey, Rev>,
        }),
    }
}

impl LogSelector {
    /// Whether a read hands out `key`'s row at `rev`.
    pub fn selects(&self, key: &LogKey, rev: Rev) -> bool {
        match (self, key) {
            (Self::All, _) => true,
            (Self::Routes(routes), LogKey::Member { obj, part }) => {
                let (on_part, on_object) = routes.carriers(obj, part, rev);
                on_part || on_object
            }
            (Self::Routes(_), LogKey::Access { .. }) => false,
        }
    }
}

impl LogRoutes {
    /// Which routes carry the member row `(obj, part)` at `rev`: `(part route, object route)`.
    /// A route carries the row when its bound is below `rev`. The two answers are kept apart
    /// because an [authorized read](PartStoreServe::read_authorized) admits them on different
    /// grounds.
    pub fn carriers(&self, obj: &ObjKey, part: &PartKey, rev: Rev) -> (bool, bool) {
        (
            self.parts.get(part).is_some_and(|bound| rev > *bound),
            self.objects.get(obj).is_some_and(|bound| rev > *bound),
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

/// The identity of a store's log: minted when the store is created and again by
/// [`PartStoreWrite::mint_generation`]. A cursor is a position in one generation's log; a
/// client holding a cursor of another generation knows nothing about where it stands in this
/// one.
///
/// Generations cover data loss, a peer that keeps its identity but not its data (a lost or
/// recreated store, a restore from an older backup). A new store gets a new generation by being
/// created. A restore brings back the files of the old one, with the old generation, so only
/// the operator can tell the store: [`PartStoreWrite::mint_generation`] after a restore.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Generation(pub Uuid);

impl Generation {
    pub fn mint() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

/// A part as a principal sees it from a cursor: what a peer decides its sync strategy on. It is
/// also the wire answer of the summary call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartSummary {
    /// The generation of the store that answers: the log that `latest_cursor` and the dirt
    /// counts are positions in. A client compares it with the generation of the cursor it
    /// holds.
    pub generation: Generation,
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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

/// One [authorized read](PartStoreServe::read_authorized): each route's verdict, the rows
/// they let through and the content of those rows' objects, all taken from one snapshot of the
/// store. The caller owns it and reuses it across reads like a [`Page`]: a read clears and
/// refills every field.
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
    /// The payload of every object with a live member row in `rows`, sorted by object and one
    /// entry per object, so a payload is copied out once however many rows name its object.
    ///
    /// It is read in the same snapshot as the rows, so it is the content those rows stand for:
    /// a payload change re-stamps every member row of its object, so a newer payload would have
    /// moved the rows past the snapshot's `until` and out of the page with it. A live row
    /// always has a payload (membership before payload is pending, and pending writes no row).
    pub payloads: Vec<(ObjKey, ObjPayload)>,
}

impl AuthorizedPage {
    /// The payload of `obj`, which has a live member row in the page.
    pub fn payload_slot(&self, obj: &ObjKey) -> usize {
        self.payloads
            .binary_search_by(|(key, _)| key.cmp(obj))
            .expect("an authorized page carries the payload of every live row's object")
    }
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
pub trait PartStoreWrite {
    type Tx;

    /// Make sure the parts exist. Membership and grants create their part too.
    fn ensure_parts(
        &self,
        tx: &mut Self::Tx,
        parts: &[PartKey],
    ) -> impl Future<Output = Res<()>> + Send;
    /// Set objects' payloads. A changed payload re-stamps the object's member rows and promotes
    /// its pending memberships; an unchanged one is a no-op. Unchanged means the same canonical
    /// content (equal [`PayloadDigest`]), so `1` and `1.0` are the same payload and the stored
    /// form is kept.
    fn set_payloads(
        &self,
        tx: &mut Self::Tx,
        payloads: Vec<(ObjKey, ObjPayload)>,
    ) -> impl Future<Output = Res<()>> + Send;
    /// Add objects to parts, `(obj, part)`. An object without a payload is pending in the part.
    fn add_members(
        &self,
        tx: &mut Self::Tx,
        members: &[(ObjKey, PartKey)],
    ) -> impl Future<Output = Res<()>> + Send;
    /// Remove objects from parts, `(obj, part)`. A live membership leaves a tombstone; a pending
    /// one leaves nothing. The payload stays.
    fn remove_members(
        &self,
        tx: &mut Self::Tx,
        members: &[(ObjKey, PartKey)],
    ) -> impl Future<Output = Res<()>> + Send;
    /// Drop the payloads of objects that are members of no part (payload laws 1, 2 and 4). An
    /// object with no payload is skipped; one that holds a payload and is a live member of a
    /// part panics, see law 2. Stamps nothing: a part-less object has no live log row, and its
    /// tombstones stay.
    fn drop_payloads(
        &self,
        tx: &mut Self::Tx,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<()>> + Send;
    /// Mint a new [generation](Generation) for the store and return it. An admin operation, not
    /// part of a transaction: it takes effect at once and durably, and changes no log row, so
    /// it stamps nothing. Run it after restoring the store from a backup, whose files carry the
    /// generation they had; a store cannot tell a restored file from a live one by itself.
    fn mint_generation(&self) -> impl Future<Output = Res<Generation>> + Send;
    /// Grant principals access to parts, `(part, principal, level)`.
    fn grant_access(
        &self,
        tx: &mut Self::Tx,
        grants: &[(PartKey, PeerKey, AccessLevel)],
    ) -> impl Future<Output = Res<()>> + Send;
    /// Revoke principals' access to parts, `(part, principal)`; a revocation is a tombstone.
    fn revoke_access(
        &self,
        tx: &mut Self::Tx,
        revokes: &[(PartKey, PeerKey)],
    ) -> impl Future<Output = Res<()>> + Send;
}

/// The sync client's reads, batched, and its cursor writes.
pub trait PartStoreRead {
    /// The objects' states, in the order asked; `None` for an object with no payload (unknown,
    /// or only pending).
    fn obj_states(
        &self,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send;
    /// The part's bucket summaries, in the order asked; a bucket with no members is the empty
    /// summary.
    fn bucket_summaries(
        &self,
        part: &PartKey,
        ids: &[BuckId],
    ) -> impl Future<Output = Res<Vec<BucketSummary>>> + Send;
    /// The part's live members.
    fn member_count(&self, part: &PartKey) -> impl Future<Output = Res<u64>> + Send;
    /// Objects that hold a payload and are a live member of no part (payload law 3), in key
    /// order, at most `limit`, strictly after `after`: pass the last key returned to page. The
    /// candidates for [`PartStoreWrite::drop_payloads`].
    fn partless_objects(
        &self,
        limit: usize,
        after: Option<&ObjKey>,
    ) -> impl Future<Output = Res<Vec<ObjKey>>> + Send;
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
/// local consumers (its embedder, indexes, replays), never to a peer. Its rev and its watch
/// are, because they disclose nothing per principal: a responder picks a read's `until` from
/// [`latest_rev`](Self::latest_rev) and waits on [`watch`](Self::watch) for a held round.
pub trait PartStoreServe {
    /// The rev of the last committed transaction: a valid `until` for
    /// [`read_authorized`](Self::read_authorized).
    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send;
    /// A handle on the latest committed rev, moved from the commit path after the commit is
    /// visible to reads (as [`RevSource::watch`](crate::rev::RevSource::watch)).
    fn watch(&self) -> RevWatch;
    /// The store's current [generation](Generation): the one every [`PartSummary`] carries.
    fn generation(&self) -> impl Future<Output = Res<Generation>> + Send;
    /// The log rows `principal` may read on `routes` in `(after, until]`, with each route's
    /// verdict and the payloads of the rows' objects, into `page`.
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
    /// - [`AuthorizedPage::payloads`] holds the payload of each object with a live row in the
    ///   page, from the same snapshot.
    ///
    /// `after`, `until`, `limits` and the page's shape are
    /// [`RevSource::read`](crate::rev::RevSource::read)'s, except that the window may be empty:
    /// `after == until` decides every verdict and reads no row (`rows.through == until`). A
    /// responder whose targets are all caught up still owes the client their verdicts (law 7 of
    /// [`replay`](crate::replay)).
    fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) -> impl Future<Output = Res<()>> + Send;
    /// For each `(part, since)`, the part's summary as `principal` sees it from `since`, in the
    /// order asked; `None` for a part it may not read or the store does not know.
    fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> impl Future<Output = Res<Vec<Option<PartSummary>>>> + Send;
    /// The objects' states as `principal` may see them, in the order asked: the content, and
    /// the live parts it may read. `None` for an object that is not a live member of any part
    /// it may read: content is only ever readable through a part.
    fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send;
    /// The part's non-empty buckets changed past `since`, in bucket order, from `from` up to
    /// `to_level`, at most `limit`; `None` for a part `principal` may not read.
    fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<BucketSummary>>>> + Send;
    /// For each `(bucket, after)`, the part's members (live and dead) in that bucket in
    /// (deepest bucket, key) order after `after`, at most `limit`, in the order asked; a bucket
    /// unchanged past `since` has none. `None` for a part `principal` may not read. One call
    /// answers a whole leaf request, so all its buckets come from one snapshot.
    fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<Vec<LeafEntry>>>>> + Send;
}
