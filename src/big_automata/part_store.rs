//! The part store: objects and their payloads, their membership in parts, principals' access to
//! parts, the bucket index over each part's members, and peers' cursors, all stamped by one log.
//!
//! **The log is the global cursor.** It is an okv-shaped table of [`LogKey`] → [`LogEntry`]:
//! one row per membership (`Member { obj, part }`) and per grant (`Access { part, principal }`),
//! collapsed to the latest write, with removals and revocations kept as tombstones. Every
//! committed write transaction that changes a row stamps all its rows with one new [`Rev`], and
//! that rev is the cursor peers resume from. Membership and access share the cursor by
//! construction, so how far a principal is behind on a part is a count of log rows in a rev
//! window ([`PartStoreServe::part_dirty_count`]).
//!
//! - **A payload change re-stamps the object's member rows** at the new rev, so a part's replay
//!   carries content changes as row changes and an object route is the key range of the
//!   object's rows. There is no separate object lane; the cost is one row write per part the
//!   object is in.
//! - **Membership before payload is pending.** Adding an object that has no payload yet writes
//!   no log row and no bucket entry; when the payload (or a [`PayloadState::Withheld`]) arrives,
//!   every pending membership goes live in that transaction, stamped once.
//! - **A write that changes nothing stamps nothing:** re-writing the payload an object holds,
//!   adding a live member, removing a non-member, granting the level a principal has.
//!
//! Three traits, one per audience: [`PartStoreWrite`] for the embedder that owns the data,
//! [`PartStoreRead`] for the sync client, [`PartStoreServe`] for the responder (which assembles
//! pages, sessions and budgets on top; none of that is here).

use crate::interlude::*;

use crate::fingerprint::Fingerprint;
use crate::fingerprint::FingerprintSeed;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::store::bucket::BucketSummary;

pub mod memory;

#[cfg(any(test, feature = "test-support"))]
pub mod laws;
#[cfg(any(test, feature = "test-support"))]
pub mod oracle;

#[cfg(test)]
mod tests;

/// A payload's identity for the bucket index, computed once when the payload is written: the
/// live fingerprint of a bucket sums its members' digests, so a summary never rereads payloads,
/// and an object whose payload is [withheld](PayloadState::Withheld) still has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PayloadDigest(pub u64);

/// Fixed on every node, like the bucket seeds, so digests agree across peers.
const PAYLOAD_DIGEST_SEED: FingerprintSeed = FingerprintSeed::new(0x7061796c6f6164, 0x646967657374);

impl PayloadDigest {
    pub fn of(payload: &ObjPayload) -> Self {
        Self(
            Fingerprint::new(
                &PAYLOAD_DIGEST_SEED,
                &("big-sync-payload-digest-v1", payload),
            )
            .as_u64(),
        )
    }
}

/// What the store holds of an object's content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadState {
    Payload(ObjPayload),
    /// The object exists and its content is known by digest, but this store does not hold or
    /// advertise the payload: a reader fetches it elsewhere. Not a deletion: the object stays a
    /// live member of its parts.
    Withheld(PayloadDigest),
}

impl PayloadState {
    pub fn digest(&self) -> PayloadDigest {
        match self {
            Self::Payload(payload) => PayloadDigest::of(payload),
            Self::Withheld(digest) => *digest,
        }
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
        Routes(pub struct LogRoutes {
            pub parts: BTreeMap<PartKey, Rev>,
            pub objects: BTreeMap<ObjKey, Rev>,
        }),
    }
}

impl LogSelector {
    pub fn selects(&self, key: &LogKey, rev: Rev) -> bool {
        match (self, key) {
            (Self::All, _) => true,
            (Self::Routes(routes), LogKey::Member { obj, part }) => {
                routes.parts.get(part).is_some_and(|bound| rev > *bound)
                    || routes.objects.get(obj).is_some_and(|bound| rev > *bound)
            }
            (Self::Routes(_), LogKey::Access { .. }) => false,
        }
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjState {
    pub payload: PayloadState,
    pub digest: PayloadDigest,
    /// The parts the object is a live member of.
    pub parts: BTreeSet<PartKey>,
}

/// How far a principal is behind on a part: the part's log rows past a cursor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartDirtyCount {
    /// Member rows of the part, live or removed.
    pub member_changes: u64,
    /// The principal's access row on the part, granted or revoked: 0 or 1.
    pub access_changes: u64,
}

/// Whether a principal may read a part. Each answer is distinct: a part the store does not know
/// is not a denial, and a denial is not an empty read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadVerdict {
    Permitted,
    Denied,
    UnknownPart,
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
#[async_trait]
pub trait PartStoreWrite: Send + Sync {
    type Tx: Send;

    /// Make sure the parts exist. Membership and grants create their part too.
    async fn ensure_parts(&self, tx: &mut Self::Tx, parts: &[PartKey]) -> Res<()>;
    /// Set or withhold objects' payloads. A changed payload re-stamps the object's member rows
    /// and promotes its pending memberships; an unchanged one is a no-op.
    async fn set_payloads(
        &self,
        tx: &mut Self::Tx,
        payloads: Vec<(ObjKey, PayloadState)>,
    ) -> Res<()>;
    /// Add objects to parts, `(obj, part)`. An object without a payload is pending in the part.
    async fn add_members(&self, tx: &mut Self::Tx, members: &[(ObjKey, PartKey)]) -> Res<()>;
    /// Remove objects from parts, `(obj, part)`. A live membership leaves a tombstone; a pending
    /// one leaves nothing. The payload stays.
    async fn remove_members(&self, tx: &mut Self::Tx, members: &[(ObjKey, PartKey)]) -> Res<()>;
    /// Grant principals access to parts, `(part, principal, level)`.
    async fn grant_access(
        &self,
        tx: &mut Self::Tx,
        grants: &[(PartKey, PeerKey, AccessLevel)],
    ) -> Res<()>;
    /// Revoke principals' access to parts, `(part, principal)`; a revocation is a tombstone.
    async fn revoke_access(&self, tx: &mut Self::Tx, revokes: &[(PartKey, PeerKey)]) -> Res<()>;
}

/// The sync client's reads, batched, and its cursor writes.
#[async_trait]
pub trait PartStoreRead: Send + Sync {
    /// The objects' states, in the order asked; `None` for an object with no payload state
    /// (unknown, or only pending).
    async fn obj_states(&self, objs: &[ObjKey]) -> Res<Vec<Option<ObjState>>>;
    /// The part's bucket summaries, in the order asked; a bucket with no members is the empty
    /// summary.
    async fn bucket_summaries(&self, part: &PartKey, ids: &[BuckId]) -> Res<Vec<BucketSummary>>;
    /// The part's live members.
    async fn member_count(&self, part: &PartKey) -> Res<u64>;
    /// The peer's cursors on the parts, in the order asked.
    async fn peer_part_cursors(&self, peer: &PeerKey, parts: &[PartKey]) -> Res<Vec<Option<Rev>>>;
    /// Move peers' cursors, `(peer, part, cursor)`. A cursor only advances: a lower one than
    /// stored is ignored.
    async fn set_peer_part_cursors(&self, cursors: &[(PeerKey, PartKey, Rev)]) -> Res<()>;
}

/// The responder's reads: the log as a [`RevSource`], authorization against the log's current
/// access rows, dirt counts and the bucket index. No pages, sessions or budgets.
#[async_trait]
pub trait PartStoreServe:
    RevSource<Key = LogKey, Value = LogEntry, Selector = LogSelector>
{
    /// For each part, whether `principal` may read it: a current access row permits.
    async fn read_verdicts(&self, principal: &PeerKey, parts: &[PartKey]) -> Res<Vec<ReadVerdict>>;
    /// The parts `obj` is a live member of that `principal` may read.
    async fn readable_parts_of(&self, principal: &PeerKey, obj: &ObjKey) -> Res<Vec<PartKey>>;
    /// The part's log rows with a rev past `since`; the access half counts `principal`'s row
    /// (`None` is the local principal, whom access does not gate).
    async fn part_dirty_count(
        &self,
        part: &PartKey,
        principal: Option<&PeerKey>,
        since: Rev,
    ) -> Res<PartDirtyCount>;
    /// The part's non-empty buckets in bucket order, from `from` up to `to_level`, at most
    /// `limit`.
    async fn bucket_list(
        &self,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        limit: usize,
    ) -> Res<Vec<BucketSummary>>;
    /// The part's members (live and dead) in bucket `buck`, in key order after `after`, at most
    /// `limit`.
    async fn bucket_leaves(
        &self,
        part: &PartKey,
        buck: BuckId,
        after: Option<&ObjKey>,
        limit: usize,
    ) -> Res<Vec<LeafEntry>>;
}
