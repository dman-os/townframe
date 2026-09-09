// FIXME: cleanup/reintegrate the legacy items in here

use crate::interlude::*;

use crate::DocumentId;
use crate::runtime2::group_part_id;
use std::time::Duration;

// ─── Constants ─────────────────────────────────────────────────────────────────

const DEFAULT_DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
const DEFAULT_DOC_SYNC_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SUBDUCTION_NONCE_TTL: Duration = Duration::from_secs(60);
const DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(30);

// ─── SyncPolicy ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BigRepoSyncPolicy {
    pub(crate) doc_worker_idle_ttl: Duration,
    pub(crate) backend_doc_sync_timeout: Duration,
    pub(crate) subduction_nonce_ttl: Duration,
    pub(crate) subduction_default_roundtrip_timeout: Duration,
}

impl Default for BigRepoSyncPolicy {
    fn default() -> Self {
        Self {
            doc_worker_idle_ttl: DEFAULT_DOC_WORKER_IDLE_TTL,
            backend_doc_sync_timeout: DEFAULT_DOC_SYNC_TIMEOUT,
            subduction_nonce_ttl: DEFAULT_SUBDUCTION_NONCE_TTL,
            subduction_default_roundtrip_timeout: DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT,
        }
    }
}

// ─── Errors ────────────────────────────────────────────────────────────────────

/** A keyhive sync round was cancelled because the connection or peer went
away. This is a NORMAL lifecycle event (peer restart, reconnect,
shutdown) — the reconnect path runs its own sync round, so callers
should treat it as retryable rather than fatal. */
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, displaydoc::Display)]
pub struct KeyhiveSyncCancelled {
    /// Why the sync was cancelled.
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, displaydoc::Display)]
pub enum SyncDocPolicyError {
    /// The local policy has no document definition.
    /// The local policy does not know this document. A hive only knows a
    /// document it can fetch, so this means no local access path exists.
    DocumentNotFound,
    /// The local policy knows the document but rejects content authored by a
    /// principal it does not see as holding edit access (stale or divergent
    /// local membership view).
    InsufficientAccess,
    /// The policy rejected an identifier as malformed.
    InvalidIdentifier,
    /// The remote peer refused the request without exposing a finer reason.
    Unauthorized,
    /// The policy returned an implementation-specific rejection.
    Other(String),
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SyncDocError {
    /// Document not found
    NotFound,
    /// The remote peer refused the document request.
    Unauthorized,
    /// The local storage policy rejected the document sync: {0}
    Policy(#[source] SyncDocPolicyError),
    /// TransportError
    TransportError,
    /// IO error: {0}
    IoError(#[source] eyre::Report),
    /// Unexpected {0}
    Other(#[from] eyre::Report),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncDocOutcome {
    /// The active document worker merged the newly persisted state.
    Ready,
    /// State is durably stored, but no active worker was eager-materialized.
    Stored,
    /// State is stored but materialization is blocked by these dependencies.
    Pending(Vec<crate::runtime2::io::MaterializationBlocker>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncDocReceipt {
    pub outcome: SyncDocOutcome,
}

#[derive(Debug, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum GetDocError {
    /// document {0} is not found
    NotFound(DocumentId),
    /// document {0} is pending materialization
    PendingMaterialization(DocumentId),
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum CreateDocError {
    /// keyhive doc creation failed: {0}
    Keyhive(#[from] eyre::Report),
    /// storage put failed: {0}
    Put(#[from] PutDocError),
}

#[derive(thiserror::Error, displaydoc::Display, Debug)]
pub enum PutDocError {
    /// IdOccupied {id}
    IdOccupied { id: DocumentId },
    /// {0:}
    Other(#[from] eyre::Report),
}

// ─── DocLookup ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocLookup<T> {
    Missing,
    PendingMaterialization,
    Ready(T),
}

impl<T> DocLookup<T> {
    pub(crate) fn map_ready<U>(self, map: impl FnOnce(T) -> U) -> DocLookup<U> {
        match self {
            Self::Missing => DocLookup::Missing,
            Self::PendingMaterialization => DocLookup::PendingMaterialization,
            Self::Ready(value) => DocLookup::Ready(map(value)),
        }
    }

    pub fn into_ready(self, doc_id: DocumentId) -> Result<T, GetDocError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Missing => Err(GetDocError::NotFound(doc_id)),
            Self::PendingMaterialization => Err(GetDocError::PendingMaterialization(doc_id)),
        }
    }
}

// ─── LiveDocBundle ─────────────────────────────────────────────────────────────

/// Monotonic source of bundle ids. A global counter (not a per-worker one) so
/// a stale handle's id can never collide with a bundle served by a restarted
/// worker.
static NEXT_BUNDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    /// Unique id of this bundle instance. Commits carry it so the worker can
    /// reject commits originating from invalidated (broken) or replaced
    /// bundles.
    id: u64,
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: surelock::mutex::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    partially_decrypted: std::sync::atomic::AtomicBool,
    #[educe(Debug(ignore))]
    broken: std::sync::atomic::AtomicBool,
    #[educe(Debug(ignore))]
    causal_epoch: std::sync::RwLock<Option<[u8; 32]>>,
    #[educe(Debug(ignore))]
    pub barrier_notify: Arc<tokio::sync::Notify>,
}

impl LiveDocBundle {
    pub(crate) fn new(
        doc_id: DocumentId,
        doc: automerge::Automerge,
        partially_decrypted: bool,
        causal_epoch: Option<[u8; 32]>,
    ) -> Self {
        Self {
            id: NEXT_BUNDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            doc_id,
            doc: surelock::mutex::Mutex::new(doc),
            partially_decrypted: std::sync::atomic::AtomicBool::new(partially_decrypted),
            broken: std::sync::atomic::AtomicBool::new(false),
            causal_epoch: std::sync::RwLock::new(causal_epoch),
            barrier_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Identity used to correlate commit requests with this bundle instance.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Whether a commit from this bundle was rejected (no write access, key
    /// unavailable, ...). A broken bundle must be re-acquired to reload the
    /// last persisted state; further commits from it are rejected by the
    /// worker.
    pub fn is_broken(&self) -> bool {
        self.broken.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn mark_broken(&self) {
        self.broken
            .store(true, std::sync::atomic::Ordering::Release);
        self.barrier_notify.notify_waiters();
    }

    /// Whether some locally stored Sedimentree heads are not represented in
    /// the materialized Automerge document because their keys are unavailable.
    pub fn is_partially_decrypted(&self) -> bool {
        self.partially_decrypted
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_partially_decrypted(&self, partial: bool) {
        self.partially_decrypted
            .store(partial, std::sync::atomic::Ordering::Release);
    }

    /// The current BeeKEM/PCS epoch observed while materializing this document.
    pub fn current_causal_epoch(&self) -> Option<[u8; 32]> {
        *self
            .causal_epoch
            .read()
            .expect("bundle epoch lock poisoned")
    }

    pub(crate) fn update_causal_epoch(&self, epoch: Option<[u8; 32]>) {
        *self
            .causal_epoch
            .write()
            .expect("bundle epoch lock poisoned") = epoch;
        self.barrier_notify.notify_waiters();
    }

    /// Await the bundle's BeeKEM/PCS epoch to reach `target` (the epoch the
    /// keyhive reports after the admission that triggered this publish). The
    /// hub forwards every CGKA op to the doc worker, which re-materializes
    /// and updates the bundle epoch; this resolves once the bundle has caught
    /// up to the keyhive's current epoch.
    pub async fn await_beekem_epoch(&self, target: Option<[u8; 32]>) -> Res<()> {
        loop {
            let notified = self.barrier_notify.notified();
            tokio::pin!(notified);
            if self.current_causal_epoch() == target {
                return Ok(());
            }
            if self.is_broken() {
                return Err(ferr!("doc bundle marked broken while awaiting epoch"));
            }
            notified.await;
        }
    }
}

// ─── LiveDocHandle ────────────────────────────────────────────────────────────

/// A live document bundle paired with the caller's eviction lease.
///
/// The doc-worker retains the bundle strongly (so repeated acquisitions do
/// not re-materialize), but the lease is owned by the caller: when the last
/// caller drops its handle, `local_handles` reaches zero and the worker
/// becomes evictable after the idle TTL. Cloning shares the lease, so the
/// worker stays alive while any clone is held.
#[derive(Clone)]
pub struct LiveDocHandle {
    pub(crate) bundle: Arc<LiveDocBundle>,
    _lease: Arc<crate::runtime2::DocLease>,
    presence: Arc<()>,
}

impl LiveDocHandle {
    pub(crate) fn new(bundle: Arc<LiveDocBundle>, lease: crate::runtime2::DocLease) -> Self {
        Self {
            bundle,
            _lease: Arc::new(lease),
            presence: Arc::new(()),
        }
    }

    pub(crate) fn presence(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.presence)
    }
}

/// Which keyhive groups a background worker (automerge frontier, causal
/// checkpoint, group part) does work for.
///
/// This is transitional, static boot configuration: the intended end state is
/// a dedicated admission-log-driven worker maintaining a durable per-worker
/// observed-group set, so operators gain dynamic group sponsorship without
/// restarting the repo.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum WorkerGroupScope {
    /// Work on documents from every keyhive group. The default everywhere:
    /// existing deployments and tests keep today's semantics.
    #[default]
    All,
    /// Only documents that belong to at least one of these keyhive group
    /// authority ids. The set holds the group-part ids (the
    /// [`group_part_id`] conversion is done once at construction, not at
    /// every consumption site). Group membership is evaluated against live
    /// keyhive state at event-processing time — never against group-part
    /// assignment — so eligibility never races the group-part worker, and a
    /// document that joins an eligible group later is picked up by its next
    /// admitted event (the delegating event itself).
    Groups(std::collections::HashSet<PartId>),
}

impl WorkerGroupScope {
    /// A scope that admits nothing — disables the worker.
    pub fn disabled() -> Self {
        Self::Groups(std::collections::HashSet::new())
    }

    /// Is a document whose containing-group ids are `doc_groups` eligible for
    /// this worker?
    pub fn admits_doc_groups(&self, doc_groups: &std::collections::BTreeSet<[u8; 32]>) -> bool {
        match self {
            Self::All => true,
            Self::Groups(scope) => doc_groups
                .iter()
                .any(|group| scope.contains(&group_part_id(*group))),
        }
    }

    pub fn admits_group(&self, group: &[u8; 32]) -> bool {
        match self {
            Self::All => true,
            Self::Groups(scope) => scope.contains(&group_part_id(*group)),
        }
    }

    /// The explicit group-part set for a selective scope, or `None` for
    /// `All`.
    ///
    /// Sites use this to decide explicitly whether they consider every event
    /// (`None` — no group lookups at all) or filter events by these groups
    /// (`Some(set)` — the set is the group list, never derived from a keyhive
    /// enumeration).
    pub fn groups(&self) -> Option<&std::collections::HashSet<PartId>> {
        match self {
            Self::All => None,
            Self::Groups(groups) => Some(groups),
        }
    }
}

/// Live handle onto a worker's [`WorkerGroupScope`] that the embedder can
/// update at runtime — a relay, for example, grows and shrinks its scope as
/// the set of documents it uses to communicate with its clients changes.
/// Workers read the current value at event-processing time and observe
/// [`GroupScopeHandle::changed`] to rescan newly eligible or newly ineligible
/// documents.
///
/// The controller side lives with the embedder ([`BigRepo`]); each spawned
/// worker holds a clone of the receiver handle.
#[derive(Debug, Clone)]
pub struct GroupScopeHandle {
    rx: tokio::sync::watch::Receiver<WorkerGroupScope>,
}

/// The write side of a [`GroupScopeHandle`].
#[derive(Debug)]
pub struct GroupScopeController {
    tx: tokio::sync::watch::Sender<WorkerGroupScope>,
}

impl GroupScopeController {
    pub fn new(initial: WorkerGroupScope) -> Self {
        let (tx, _) = tokio::sync::watch::channel(initial);
        Self { tx }
    }

    /// Publish a new scope. A redundant set (unchanged value) does not wake
    /// workers.
    pub fn set(&self, scope: WorkerGroupScope) {
        self.tx.send_if_modified(|current| {
            let changed = *current != scope;
            if changed {
                *current = scope;
            }
            changed
        });
    }

    pub fn handle(&self) -> GroupScopeHandle {
        GroupScopeHandle {
            rx: self.tx.subscribe(),
        }
    }

    /// The scope as of this call.
    pub fn get(&self) -> WorkerGroupScope {
        self.tx.borrow().clone()
    }
}

impl GroupScopeHandle {
    /// The scope as of this call.
    pub fn get(&self) -> WorkerGroupScope {
        self.rx.borrow().clone()
    }

    /// Resolves when the scope changes. If the controller has been dropped
    /// the scope is frozen forever and this resolves immediately; callers
    /// that observe changes in a select loop must treat closure as
    /// "never changes again" (park on `std::future::pending`), not spin.
    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        // `watch::RecvError` is Closed-only (unit struct — watch receivers
        // cannot lag), so closure is the only failure and callers park on it.
        self.rx.changed().await
    }
}

#[cfg(test)]
mod worker_scope_tests {
    use super::WorkerGroupScope;
    use std::collections::{BTreeSet, HashSet};

    #[test]
    fn all_and_selective_scopes_match_group_membership() {
        let eligible = [7; 32];
        let other = [9; 32];
        let groups = BTreeSet::from([eligible]);
        assert!(WorkerGroupScope::All.admits_doc_groups(&groups));
        assert!(
            WorkerGroupScope::Groups(HashSet::from([super::group_part_id(eligible)]))
                .admits_doc_groups(&groups)
        );
        assert!(
            !WorkerGroupScope::Groups(HashSet::from([super::group_part_id(other)]))
                .admits_doc_groups(&groups)
        );
        assert!(
            WorkerGroupScope::Groups(HashSet::from([super::group_part_id(eligible)]))
                .admits_group(&eligible)
        );
        assert!(
            !WorkerGroupScope::Groups(HashSet::from([super::group_part_id(other)]))
                .admits_group(&eligible)
        );
    }

    #[test]
    fn groups_accessor_makes_the_site_decision_explicit() {
        let eligible = [7; 32];
        let other = [9; 32];
        // `All` exposes no group set: sites consider every event with no
        // group lookups.
        assert!(WorkerGroupScope::All.groups().is_none());
        // A selective scope exposes its explicit group-part set directly —
        // the set is the group list, never derived from a keyhive
        // enumeration, and the `group_part_id` conversion is done once at
        // construction.
        let scope = WorkerGroupScope::Groups(HashSet::from([super::group_part_id(eligible)]));
        let groups = scope.groups().expect("selective scope exposes its set");
        assert!(groups.contains(&super::group_part_id(eligible)));
        assert!(!groups.contains(&super::group_part_id(other)));
    }
}

// ─── Keyhive event type aliases ────────────────────────────────────────────────

pub(crate) type SignedAddKeyOp =
    keyhive_crypto::signed::Signed<keyhive_core::principal::individual::op::add_key::AddKeyOp>;
pub(crate) type SignedRotateKeyOp = keyhive_crypto::signed::Signed<
    keyhive_core::principal::individual::op::rotate_key::RotateKeyOp,
>;
pub(crate) type SignedCgkaOp = keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>;

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_lookup_into_ready() {
        let doc_id = DocumentId::new([23; 32]);

        let err = DocLookup::<()>::Missing
            .into_ready(doc_id)
            .expect_err("missing doc should fail");
        assert!(matches!(err, GetDocError::NotFound(id) if id == doc_id));

        let err = DocLookup::<()>::PendingMaterialization
            .into_ready(doc_id)
            .expect_err("pending doc should fail");
        assert!(matches!(err, GetDocError::PendingMaterialization(id) if id == doc_id));

        let ok = DocLookup::Ready(42usize)
            .into_ready(doc_id)
            .expect("ready doc should succeed");
        assert_eq!(ok, 42);
    }
}
