use serde_json::Value as Json;

/// Ordering for one row within one source incarnation.
///
/// `incarnation` is the authority's epoch: a rebuilt frontier, a restarted provider, or
/// anything else that restarts its counters. Rows are only comparable within an
/// incarnation, so a higher incarnation always resets the replica rather than ranking
/// against it. `seq` is chosen by whoever is authoritative for the row; heads and other
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

/// Identity of one submitted write, minted by the view the write originated in and
/// carried unchanged to every hop, so all hops retire the same overlay together.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(pub u64);

/// Identity of one demand. Minted by the view that issued it and carried unchanged, so
/// coverage reported by the far end resolves the origin's own lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DemandId(pub u64);

/// What a consumer asks for upstream. Opaque here by design: the waist transports it and
/// the narrow end of the chain interprets it, which is why the component is called a
/// narrower. Carrying JSON keeps a host change from forcing every guest to rebuild.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DemandToken(pub Json);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Coverage {
    Unknown,
    Partial,
    Complete,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BaseDelta<K, V> {
    Upsert { key: K, version: Version, value: V },
    Remove { key: K, version: Version },
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
    /// a rejection for visibility: replication still restores the value if it landed.
    Unknown,
}

/// A pending overlay travelling between hops so sibling views see the same row.
///
/// Overlays and base rows never merge on the wire: a consumer must be able to tell a
/// value that has been accepted from one that is still someone's local intent. `value` is
/// an option because a delete is also a whole-row prediction — the row is absent until
/// replication confirms the removal.
#[derive(Clone, Debug, PartialEq)]
pub enum OverlayDelta<K, V> {
    Put {
        id: OperationId,
        key: K,
        value: Option<V>,
        writer: ScopeId,
    },
    Clear {
        id: OperationId,
    },
}

/// One atomic transition of a hop.
///
/// Every field lands in the same visible change: a view that forwarded base and overlay
/// halves separately would show a frame where the row reverts to base before the
/// overlay is re-applied, or loses a settled write's evidence.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Batch<K, V> {
    pub incarnation: u64,
    pub base: Vec<BaseDelta<K, V>>,
    pub overlays: Vec<OverlayDelta<K, V>>,
    pub settled: Vec<(OperationId, WriteOutcome)>,
    pub covered: Vec<DemandId>,
}

impl<K, V> Batch<K, V> {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            base: Vec::new(),
            overlays: Vec::new(),
            settled: Vec::new(),
            covered: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
            && self.overlays.is_empty()
            && self.settled.is_empty()
            && self.covered.is_empty()
    }
}
