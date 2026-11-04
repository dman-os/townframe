/// Ordering for one row within one source incarnation.
///
/// `incarnation` is the authority's epoch: a rebuilt frontier, a restarted provider, or
/// anything else that restarts its counters. Rows are only comparable within an
/// incarnation; adopting a new one requires explicitly clearing the mirror or resetting
/// the replica. `seq` is chosen by whoever is authoritative for the row; heads and other
/// causal positions are not orderings and never enter here.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Version {
    pub incarnation: u64,
    pub seq: u64,
}

impl Version {
    pub const fn new(incarnation: u64, seq: u64) -> Self {
        Self { incarnation, seq }
    }
}

/// The scope an overlay's writer belongs to. Disposing the scope withdraws everything
/// that scope wrote; the underlying writes, if already submitted, are unaffected.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopeId(pub u64);

/// Identity of one submitted write, minted where the write originated and carried
/// unchanged to settlement, so every holder of the overlay retires it on the same
/// outcome.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(pub u64);

/// Host-minted identity of a registration scope within its collection. It carries no
/// mutation authority and is not stable across restarts; the stable identity of a
/// writer is its [`WriterId`]. Closing the scope owner retracts its candidates,
/// independently of writer-handle lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RegistrationScopeId(pub(crate) u64);

impl RegistrationScopeId {
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Identity of one replica of a collection store: one node's (or browser profile's) copy,
/// generated once when its store is first opened and persisted in that store, so it is stable
/// across boots. Fixed width, which is what lets [`WriterId`] order by replica first.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicaId([u8; ReplicaId::LEN]);

impl ReplicaId {
    pub const LEN: usize = 16;

    pub const fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }
}

/// A stable, totally ordered writer identity: the replica the write happened on plus the micro
/// that wrote it (a micro id), supplied when a scope is created. It is the tie-break between
/// concurrent writes with equal lamports. The micro id alone would tie across nodes once
/// collections sync (two nodes' installations of the same micro would stamp different values
/// with equal `(lamport, writer)`), and the replica alone would conflate micros on one node.
///
/// The encoding is explicit because it is persisted in every durable row and echoed through the
/// guest's stamps: the replica's [`ReplicaId::LEN`] bytes, then the micro id's UTF-8 bytes. The
/// replica is fixed width, so bytewise order is (replica, micro), which is the order stamps
/// use.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WriterId(std::sync::Arc<[u8]>);

impl WriterId {
    pub fn new(replica: &ReplicaId, micro: &str) -> Self {
        Self(
            [replica.as_bytes().as_slice(), micro.as_bytes()]
                .concat()
                .into(),
        )
    }

    /// An identity already encoded: read back from the durable store, or echoed by a guest as
    /// part of a stamp it observed. It is only ever compared, so its shape is not re-checked.
    pub fn from_encoded(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The causal position of one write to one key. Stamps order lamport first, then writer
/// id; the highest stamp among a key's live candidates is the key's winner.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Stamp {
    pub lamport: u64,
    pub writer: WriterId,
}

/// A projected value together with the stamp of the write that produced it, so a reader
/// can pass the stamp back as a [`WriteBasis`]. The projection's `row_version` is only a
/// feed position: a recomputed winner may carry an older stamp under a newer version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stamped<V> {
    pub stamp: Stamp,
    pub value: V,
}

/// What a writer claims to have seen when it writes a key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBasis {
    /// No claim (concurrent creation): lamport `1`, always accepted.
    Concurrent,
    /// The writer saw this stamp: lamport is one above it, always accepted. A stamp
    /// from a replica that is behind simply loses to a higher one.
    Observed(Stamp),
    /// Accepted only if the current winner is exactly this stamp (`None`: the key is
    /// absent, whether never written or deleted). Lamport is one above the current
    /// winner, tombstones included, so the write supersedes what it compared against.
    CompareAndSet(Option<Stamp>),
}

/// An opaque read position in one projection generation.
///
/// The guest treats it as bytes and echoes it back as `committed`; only the projection
/// that minted it can interpret it. Identity combines the caller-supplied runtime epoch,
/// which must be fresh across runtimes sharing cursors, with a checked local source counter.
/// A cursor from another runtime epoch, source, or generation fails with `ResetRequired`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Cursor(pub Vec<u8>);

/// Whether a row is the authority's committed value or a prediction still awaiting
/// settlement. Optimism lives host-side: a pending row renders, carries its evidence,
/// and is replaced by the authoritative row when the write settles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowEvidence {
    Authoritative,
    Pending,
}

/// One row transition as it crosses the read waist.
///
/// `write_context` is opaque, source-defined mutation context carried with the row.
/// It is separate from capability attachments carried by encoded values
/// (ADRs 017, 029, and 030).
#[derive(Clone, Debug, PartialEq)]
pub enum Change<K, V> {
    Upsert {
        key: K,
        row_version: Version,
        value: V,
        evidence: RowEvidence,
        write_context: Option<Vec<u8>>,
    },
    Remove {
        key: K,
        row_version: Version,
    },
}

impl<K, V> Change<K, V> {
    pub fn key(&self) -> &K {
        match self {
            Change::Upsert { key, .. } | Change::Remove { key, .. } => key,
        }
    }

    pub fn row_version(&self) -> Version {
        match self {
            Change::Upsert { row_version, .. } | Change::Remove { row_version, .. } => *row_version,
        }
    }
}

/// The changes one authority transition made visible at once. A page never delivers part
/// of a transaction: limits bound how many transactions arrive, not how much of one does.
#[derive(Clone, Debug, PartialEq)]
pub struct Transaction<K, V> {
    pub changes: Vec<Change<K, V>>,
}

/// One read result: whole transactions from the reader's committed position forward, the
/// cursor that position advances to, and whether the reader has caught up to the point
/// its lease opened at.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<K, V> {
    pub transactions: Vec<Transaction<K, V>>,
    pub through: Cursor,
    pub bootstrap_complete: bool,
}

/// Size bounds for one read. `None` is unbounded. A single transaction larger than
/// `max_changes` still arrives whole, because splitting it would break its atomicity and
/// withholding it would stall the lease.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadLimits {
    pub max_transactions: Option<u32>,
    pub max_changes: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadError {
    /// The committed cursor no longer describes this projection generation. The lease is
    /// invalid: the reader clears its mirror and reacquires without a resume cursor.
    ResetRequired,
    /// The source lost authority and no cached progress can be formed.
    SourceUnavailable,
    /// The lease was withdrawn; the reader stops and does not reacquire.
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcquireError {
    /// The resume cursor is from a superseded generation or otherwise undecodable.
    ResetRequired,
    SourceUnavailable,
    Revoked,
    InvalidSelector,
    UnauthorizedSelector,
}

/// Why a registration mutation was refused. `current` is the stamp of the visible
/// winner, so the writer can rebase and retry; `None` means the key is absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationError {
    Conflict { current: Option<Stamp> },
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WriteOutcome {
    /// Accepted by the authority. The overlay stays until base reaches `version`, so the
    /// committed value arrives by replication instead of flashing the row back first.
    Durable {
        version: Version,
    },
    Rejected,
    /// Transport failed and the authority may or may not have taken the write. Treated as
    /// a rejection for visibility; the row's next authoritative change decides.
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(lamport: u64, replica: u8, micro: &str) -> Stamp {
        Stamp {
            lamport,
            writer: WriterId::new(&ReplicaId::from_bytes([replica; ReplicaId::LEN]), micro),
        }
    }

    /// Stamps order by lamport, then replica, then micro id, so two replicas writing different
    /// values at the same lamport never tie, whatever their micros are called, and one replica's
    /// micros are ordered among themselves.
    #[test]
    fn stamps_order_by_lamport_then_replica_then_micro() {
        let ordered = [
            stamp(1, 9, "z"),
            stamp(2, 1, "a"),
            stamp(2, 1, "b"),
            // A longer micro id that extends a shorter one sorts after it.
            stamp(2, 1, "b2"),
            stamp(2, 2, "a"),
            stamp(3, 0, "a"),
        ];
        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1], "{pair:?}");
        }
        // The same micro on two replicas is two writers; the same (replica, micro) is one.
        assert_ne!(stamp(2, 1, "a"), stamp(2, 2, "a"));
        assert_eq!(stamp(2, 1, "a"), stamp(2, 1, "a"));
    }

    /// The durable and wire form is the replica's fixed-width bytes followed by the micro id's.
    #[test]
    fn writer_encoding_is_replica_bytes_then_micro_bytes() {
        let replica = ReplicaId::from_bytes([7; ReplicaId::LEN]);
        let writer = WriterId::new(&replica, "did:example:x/app");
        assert_eq!(
            writer.as_bytes(),
            [&[7; ReplicaId::LEN][..], b"did:example:x/app"].concat()
        );
        assert_eq!(WriterId::from_encoded(writer.as_bytes()), writer);
    }
}
