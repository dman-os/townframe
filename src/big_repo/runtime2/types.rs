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
    /// The local document worker was stopping, so the received session could
    /// not be applied to the live document. Retryable: the next round spawns a
    /// fresh worker.
    WorkerUnavailable,
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

// ─── DocLeaseKind ──────────────────────────────────────────────────────────────

/// Whether an acquired document handle counts as a live *caller*.
///
/// Caller handles are user-facing leases: their presence is what makes a
/// document "live", which is the condition for routing received content into
/// the materialized bundle and for emitting user-visible change notifications.
/// Background workers that merely need the bundle (the Automerge frontier
/// publisher) use [`DocLeaseKind::Internal`] so a document nobody holds stays
/// un-live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocLeaseKind {
    Caller,
    Internal,
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

/// Why a [`LiveDocBundle`] was invalidated.
///
/// A broken bundle cannot serve commits: its in-memory document may hold
/// mutations that never persisted. Recording *which* event broke it is what
/// lets the write gate report the real cause — it previously claimed an earlier
/// rejected commit in every case, including a worker teardown where no commit
/// had been rejected at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokenReason {
    /// The doc-worker that owned the bundle went away (evicted, or its loop
    /// exited). No commit was involved.
    WorkerDropped,
    /// A commit was rejected because the local principal no longer holds write
    /// access to the document.
    CommitRejectedNoWriteAccess,
    /// A commit was rejected because its encrypted payload could not be
    /// persisted: the document key was unavailable.
    CommitRejectedKeyUnavailable,
}

impl BrokenReason {
    /// The refusal handed back to a caller whose handle names an invalidated
    /// bundle. Each reason names its own cause: one wording for all of them
    /// claimed an earlier rejected commit even for a plain worker teardown,
    /// where no commit had been rejected, which is what made a broken-handle
    /// report unreadable.
    pub(crate) fn refusal_message(self) -> &'static str {
        match self {
            Self::WorkerDropped => {
                "document write rejected: the doc worker serving this handle was replaced; re-acquire the document"
            }
            Self::CommitRejectedNoWriteAccess => {
                "document write rejected: handle invalidated by an earlier rejected commit (no write access); re-acquire the document"
            }
            Self::CommitRejectedKeyUnavailable => {
                "document write rejected: handle invalidated by an earlier rejected commit (document key unavailable); re-acquire the document"
            }
        }
    }
}

/// How a wait for CGKA materialization ended.
///
/// Deliberately not a `Result`: the wait has no failure mode of its own, and the
/// one way it can end early — the bundle being invalidated while a waiter is
/// parked — is the eviction race callers are expected to defer on. Modelling it
/// as an error conflated the two: a caller propagating it turned an eviction
/// into a task error while its own broken-bundle check deferred for the same
/// event, so the outcome depended on which check observed the break first, and
/// the error carried no cause. Here the cause rides along with the outcome, and a
/// caller that does want to fail on a break can match [`Self::Broken`] and build
/// its own error from the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationOutcome {
    /// The bundle reached at least the requested operation count.
    Materialized,
    /// The bundle was invalidated while waiting, and this is why.
    Broken(BrokenReason),
}

/// Whether a commit may proceed against the bundle the worker serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandleValidity {
    /// The commit names the served, healthy bundle.
    Valid,
    /// The commit names a different bundle: the handle predates a reload of the
    /// document, or the worker serves no bundle at all.
    StaleHandle,
    /// The served bundle was invalidated, and this is why.
    Broken(BrokenReason),
}

/// Classify a commit against the bundle the worker currently serves.
///
/// A broken bundle is classified broken even when the ids disagree: the handle
/// points at a bundle that will never accept a commit, and the recorded reason
/// is the actionable half of that fact.
pub(crate) fn handle_validity(
    served_bundle_id: Option<u64>,
    bundle_id: u64,
    broken_reason: Option<BrokenReason>,
) -> HandleValidity {
    match (broken_reason, served_bundle_id == Some(bundle_id)) {
        (Some(reason), _) => HandleValidity::Broken(reason),
        (None, true) => HandleValidity::Valid,
        (None, false) => HandleValidity::StaleHandle,
    }
}

/// The refusal to hand back for a commit that failed the handle-validity gate,
/// or `None` when the commit may proceed.
///
/// The worker's commit gate and a handle's own fast-fail path both render their
/// refusal through here, so the wording has exactly one definition.
pub(crate) fn invalid_handle_message(validity: HandleValidity) -> Option<&'static str> {
    match validity {
        HandleValidity::Valid => None,
        HandleValidity::StaleHandle => {
            Some("document write rejected: commit from a stale handle; re-acquire the document")
        }
        HandleValidity::Broken(reason) => Some(reason.refusal_message()),
    }
}

#[derive(Clone, Copy)]
struct CausalEpochState {
    epoch: Option<[u8; 32]>,
    cgka_ops_count: usize,
}

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
    broken: std::sync::RwLock<Option<BrokenReason>>,
    #[educe(Debug(ignore))]
    causal_state: std::sync::RwLock<CausalEpochState>,
    #[educe(Debug(ignore))]
    pub barrier_notify: Arc<tokio::sync::Notify>,
}

impl LiveDocBundle {
    pub(crate) fn new(
        doc_id: DocumentId,
        doc: automerge::Automerge,
        partially_decrypted: bool,
        causal_epoch: Option<[u8; 32]>,
        cgka_ops_count: usize,
    ) -> Self {
        Self {
            id: NEXT_BUNDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            doc_id,
            doc: surelock::mutex::Mutex::new(doc),
            partially_decrypted: std::sync::atomic::AtomicBool::new(partially_decrypted),
            broken: std::sync::RwLock::new(None),
            causal_state: std::sync::RwLock::new(CausalEpochState {
                epoch: causal_epoch,
                cgka_ops_count,
            }),
            barrier_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Identity used to correlate commit requests with this bundle instance.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Whether this bundle is invalid: a commit from it was rejected, or the
    /// worker that owned it went away. A broken bundle must be re-acquired to
    /// reload the last persisted state; further commits from it are rejected by
    /// the worker.
    pub fn is_broken(&self) -> bool {
        self.broken
            .read()
            .expect("bundle broken-reason lock poisoned")
            .is_some()
    }

    /// Why this bundle was invalidated, or `None` while it is healthy.
    ///
    /// The first reason recorded wins: it is the cause, and any later one (a
    /// teardown that follows a rejected commit, say) is only a consequence.
    /// Callers report this instead of guessing a cause.
    pub(crate) fn broken_reason(&self) -> Option<BrokenReason> {
        *self
            .broken
            .read()
            .expect("bundle broken-reason lock poisoned")
    }

    pub(crate) fn mark_broken(&self, reason: BrokenReason) {
        {
            let mut current = self
                .broken
                .write()
                .expect("bundle broken-reason lock poisoned");
            if current.is_none() {
                *current = Some(reason);
            }
        }
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
        self.causal_state
            .read()
            .expect("bundle causal state lock poisoned")
            .epoch
    }

    /// The number of CGKA operations in the keyhive state used for this document.
    pub fn materialized_cgka_ops_count(&self) -> usize {
        self.causal_state
            .read()
            .expect("bundle causal state lock poisoned")
            .cgka_ops_count
    }

    pub(crate) fn update_causal_state(&self, epoch: Option<[u8; 32]>, cgka_ops_count: usize) {
        let changed = {
            let mut state = self
                .causal_state
                .write()
                .expect("bundle causal state lock poisoned");
            if state.epoch == epoch && state.cgka_ops_count == cgka_ops_count {
                false
            } else {
                state.epoch = epoch;
                state.cgka_ops_count = cgka_ops_count;
                true
            }
        };
        if changed {
            self.barrier_notify.notify_waiters();
        }
    }

    /// Await materialization against at least `target` CGKA operations.
    /// The count is per-document and monotonic for a shared Keyhive state, so
    /// it provides ordering without interpreting opaque KEM fingerprints.
    ///
    /// The wait has no failure mode of its own: the only thing that can end it
    /// early is the bundle being invalidated while parked, which is the eviction
    /// race callers defer on. That case comes back as a value carrying its reason
    /// rather than as an error, so a caller cannot mistake a break for a failure —
    /// see [`MaterializationOutcome`].
    pub async fn await_cgka_ops_count(&self, target: usize) -> MaterializationOutcome {
        loop {
            let notified = self.barrier_notify.notified();
            tokio::pin!(notified);
            if let Some(reason) = self.broken_reason() {
                return MaterializationOutcome::Broken(reason);
            }
            if self.materialized_cgka_ops_count() >= target {
                return MaterializationOutcome::Materialized;
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
    Groups(std::collections::HashSet<PartKey>),
}

impl WorkerGroupScope {
    /// A scope that admits nothing — disables the worker.
    pub fn disabled() -> Self {
        Self::Groups(std::collections::HashSet::new())
    }

    /// Whether this scope is `Groups(∅)`, which admits no document at all.
    ///
    /// Callers that decide eligibility against a *live* Keyhive membership set can
    /// use this to skip that traversal: with no groups in the scope the answer is
    /// `false` for every document, whatever Keyhive says. The traversal is a
    /// shared-lock walk, so skipping it matters for the disabled configuration.
    pub fn admits_nothing(&self) -> bool {
        matches!(self, Self::Groups(groups) if groups.is_empty())
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
    pub fn groups(&self) -> Option<&std::collections::HashSet<PartKey>> {
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
    fn disabled_scope_admits_no_document_and_says_so() {
        let disabled = WorkerGroupScope::disabled();
        assert!(disabled.admits_nothing());
        assert!(!disabled.admits_doc_groups(&BTreeSet::from([[7; 32]])));
        // The predicate is about the scope, not an accident of one document: a
        // selective scope holding any group must not report itself as admitting
        // nothing, and `All` never does.
        assert!(!WorkerGroupScope::All.admits_nothing());
        assert!(
            !WorkerGroupScope::Groups(HashSet::from([super::group_part_id([7; 32])]))
                .admits_nothing()
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
            .into_ready(doc_id.clone())
            .expect_err("missing doc should fail");
        assert!(matches!(err, GetDocError::NotFound(id) if id == doc_id));

        let err = DocLookup::<()>::PendingMaterialization
            .into_ready(doc_id.clone())
            .expect_err("pending doc should fail");
        assert!(matches!(err, GetDocError::PendingMaterialization(id) if id == doc_id));

        let ok = DocLookup::Ready(42usize)
            .into_ready(doc_id)
            .expect("ready doc should succeed");
        assert_eq!(ok, 42);
    }

    #[tokio::test]
    async fn await_cgka_ops_count_accepts_equal_or_advanced_state() {
        let bundle = std::sync::Arc::new(LiveDocBundle::new(
            DocumentId::new([7; 32]),
            automerge::Automerge::new(),
            false,
            Some([1; 32]),
            1,
        ));
        assert_eq!(
            bundle.await_cgka_ops_count(1).await,
            MaterializationOutcome::Materialized,
            "equal count should complete immediately"
        );

        let waiting_bundle = std::sync::Arc::clone(&bundle);
        let waiter = tokio::spawn(async move { waiting_bundle.await_cgka_ops_count(2).await });
        tokio::task::yield_now().await;
        bundle.update_causal_state(Some([1; 32]), 3);
        assert_eq!(
            waiter.await.expect("count waiter task should not panic"),
            MaterializationOutcome::Materialized,
            "advanced count should release the waiter"
        );
    }

    /// The wait must report a break *as a break*, with its cause. It used to
    /// return an error that only said the bundle was marked broken, which a
    /// caller propagating it turned into a task failure — for an event whose
    /// other observer in the same worker deferred — and the cause was lost in
    /// that path either way.
    #[tokio::test]
    async fn await_cgka_ops_count_reports_a_break_with_its_reason() {
        // Broken before the wait starts: a bundle that will never materialize
        // again must report the break instead of parking for a count it can no
        // longer reach.
        let broken = std::sync::Arc::new(LiveDocBundle::new(
            DocumentId::new([8; 32]),
            automerge::Automerge::new(),
            false,
            Some([1; 32]),
            1,
        ));
        broken.mark_broken(BrokenReason::WorkerDropped);
        assert_eq!(
            broken.await_cgka_ops_count(2).await,
            MaterializationOutcome::Broken(BrokenReason::WorkerDropped),
            "a broken bundle reports the break, not a count it never reached"
        );

        // Broken while parked: the waiter wakes with the cause it was broken by.
        let waiting = std::sync::Arc::new(LiveDocBundle::new(
            DocumentId::new([9; 32]),
            automerge::Automerge::new(),
            false,
            Some([1; 32]),
            1,
        ));
        let parked = std::sync::Arc::clone(&waiting);
        let waiter = tokio::spawn(async move { parked.await_cgka_ops_count(5).await });
        tokio::task::yield_now().await;
        waiting.mark_broken(BrokenReason::CommitRejectedNoWriteAccess);
        assert_eq!(
            waiter.await.expect("count waiter task should not panic"),
            MaterializationOutcome::Broken(BrokenReason::CommitRejectedNoWriteAccess),
            "the waiter carries the reason, so the caller's defer can log why it deferred"
        );
    }

    /// The commit gate must name the real cause. A worker teardown records
    /// `WorkerDropped` with no rejected commit behind it, so the single generic
    /// wording ("an earlier rejected commit") was a lie in exactly the case that
    /// was hardest to read; the two commit-rejection reasons keep that wording
    /// because for them it is true.
    #[test]
    fn commit_gate_reports_the_real_reason() {
        // A healthy bundle is not a refusal at all.
        let valid = handle_validity(Some(7), 7, None);
        assert_eq!(valid, HandleValidity::Valid);
        assert!(invalid_handle_message(valid).is_none());

        // A handle that predates a reload of the document names the reload.
        let stale = handle_validity(Some(9), 7, None);
        assert_eq!(stale, HandleValidity::StaleHandle);
        let stale_message = invalid_handle_message(stale).expect("a stale handle is refused");
        assert!(stale_message.contains("stale handle"), "{stale_message}");

        // Teardown is not a rejected commit.
        let dropped = handle_validity(Some(7), 7, Some(BrokenReason::WorkerDropped));
        assert_eq!(dropped, HandleValidity::Broken(BrokenReason::WorkerDropped));
        let dropped_message = invalid_handle_message(dropped).expect("a broken bundle is refused");
        assert!(
            !dropped_message.contains("earlier rejected commit"),
            "a teardown must not be reported as a rejected commit: {dropped_message}"
        );
        assert!(dropped_message.contains("replaced"), "{dropped_message}");

        // Both commit-rejection reasons keep the rejected-commit wording and
        // stay distinguishable from each other.
        let no_access = invalid_handle_message(handle_validity(
            Some(7),
            7,
            Some(BrokenReason::CommitRejectedNoWriteAccess),
        ))
        .expect("refused");
        let no_key = invalid_handle_message(handle_validity(
            Some(7),
            7,
            Some(BrokenReason::CommitRejectedKeyUnavailable),
        ))
        .expect("refused");
        assert!(no_access.contains("no write access"), "{no_access}");
        assert!(no_key.contains("document key unavailable"), "{no_key}");
        assert_ne!(no_access, no_key);

        // A broken bundle whose id also disagrees is still reported by reason:
        // the reason is the actionable half of the fact.
        assert_eq!(
            handle_validity(Some(9), 7, Some(BrokenReason::WorkerDropped)),
            HandleValidity::Broken(BrokenReason::WorkerDropped)
        );
    }

    /// Every reason names its own cause, and only a reason that actually means
    /// "a commit was rejected" may say so. The inner `match`es are exhaustive,
    /// so a new variant cannot silently inherit another variant's wording.
    #[test]
    fn every_broken_reason_names_its_own_cause() {
        let all = [
            BrokenReason::WorkerDropped,
            BrokenReason::CommitRejectedNoWriteAccess,
            BrokenReason::CommitRejectedKeyUnavailable,
        ];
        let mut seen = std::collections::BTreeSet::new();
        for reason in all {
            let message = reason.refusal_message();
            assert!(
                seen.insert(message),
                "two reasons share one refusal: {message}"
            );

            let claims_rejected_commit = message.contains("earlier rejected commit");
            let is_rejected_commit = match reason {
                BrokenReason::WorkerDropped => false,
                BrokenReason::CommitRejectedNoWriteAccess
                | BrokenReason::CommitRejectedKeyUnavailable => true,
            };
            assert_eq!(
                claims_rejected_commit, is_rejected_commit,
                "{reason:?} claims the wrong cause: {message}"
            );

            let names_own_cause = match reason {
                BrokenReason::WorkerDropped => message.contains("worker"),
                BrokenReason::CommitRejectedNoWriteAccess => message.contains("no write access"),
                BrokenReason::CommitRejectedKeyUnavailable => {
                    message.contains("document key unavailable")
                }
            };
            assert!(
                names_own_cause,
                "{reason:?} does not name its cause: {message}"
            );
        }
        assert_eq!(seen.len(), 3, "all three reasons are distinct");
    }
}
