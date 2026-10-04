//! The blob encryption worker (ADR 003 §14, §19).
//!
//! Owns one job: for every blob a document declares, make an encrypted
//! representation exist locally and be reachable through the document, in the
//! order §19 fixes. It is maintenance, not a reconciler of derived state: the
//! representation *pins* are derived from the facets this worker writes, by the
//! pin workers, so nothing here authors a pin.
//!
//! Scope is one keyhive group (§19, "one worker per group"): a document is
//! eligible when it is a member of `domain_group`, evaluated against live
//! keyhive state at processing time. That makes eligibility a *state*, and a
//! state produces no delta of its own — so the worker is fed by three
//! revision streams and every skip shape is re-armed by one of them (see the
//! machine comment and ADR 003 §13, "the blob plane follows the docs plane's
//! stream architecture"); a restarted node is servable again because the
//! virtual-provider registry re-registers at boot while the entries are
//! durable.
//!
//! Ordering per (document, blob) — the prefixes of this sequence are the only
//! atomicity available across documents, so every prefix is safe to crash in:
//!
//! 1. §11 pass over `P`: compute `C`, install the virtual entry, register the
//!    pair (installed ⟹ servable ⟹ rooted, `CipherBlobProvider::install`)
//! 2. write the JWK facet, in a document of its own
//! 3. write the `cipherBlob` facet — `C` becomes nameable
//! 4. (no step) the pin is derived from that facet, never written here
//! 5. append the `?via=` resolution URL to the `Blob` facet — the commit point

use crate::interlude::*;

use daybook_types::doc::{
    AddDocArgs, Blob, BranchId, BranchPath, BranchPathBuf, ChangeHashSet, CipherBlob, DocId,
    DocPatch, FacetKey, FacetRaw, FacetTag, Jwk, Representation, WellKnownFacet, WellKnownFacetTag,
};
use tokio_util::sync::CancellationToken;

use crate::blobs::encrypt::{
    CONTENT_ENCODING_AES128GCM, CipherBlobProvider, CipherKeySource, EncodingParams, JwkOct,
    MasterKey,
};
use crate::blobs::key_source::DocKeySource;
use crate::blobs::{BlobId, blob_id_to_digest_str, digest_str_to_blob_id_lenient};
use crate::drawer::{DrawerRepo, FacetWriteScope};
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{DocFacetSetIndexRepo, FacetSetRevisionStore, FacetSetSelector};
use crate::repos::RepoStopToken;
use big_repo::BigKeyhiveGroup;
use big_repo::SharedPartStore;
use big_sync::DeltaWalkerStateRepo as _;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync::{
    DeltaWalkerSparseStateRepo as _, DeltaWalkerSparseStateTransaction as _,
    DeltaWalkerStateTransaction as _,
};
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits, RevisionedStore as _};
use big_sync_core::rpc::PartEvent;
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use big_sync_core::{ObjKey, PartKey};
use iroh_blobs::Hash;
use iroh_blobs::api::proto::BlobStatus;

/// Walker state for the facet-set machine. Independent of the pin workers'
/// cursors: this worker reads the same source for a different question.
pub(crate) const ENCRYPTION_WORKER_STATE_ID: &str = "@daybook/core/blob-encryption-worker";

/// ADR 003 §11: the pass is a full sequential read of the plaintext. One blob
/// in flight is the entire budget, and it is deliberately *this worker's* own
/// budget - a minutes-long disk-bound pass must not occupy a pin-reconciliation
/// slot.
/// FIXME: use experiment to find the right value for this
/// since it's not just disk reads but encryption and hashing too
/// which are CPU bound
const ENCRYPTION_TASK_BUDGET: usize = 1;

/// A failed document delta is rescheduled with backoff rather than dropped or
/// waved through: the walker cursor must not advance past work that never
/// happened, and a transient store or drawer failure must not cost the document
/// until the next boot's pass.
const ENCRYPTION_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Test seam for the rescheduling contract. Every production failure this
/// worker can see is either racy or needs a write to the document - and a
/// document write would itself produce the delta the contract must be tested
/// without.
///
/// State is per machine, never process-global: one instance is born with its
/// worker's [`Ctx`] and the spawned feeder carries the same handle, so two
/// concurrently running workers (and the tests that drive them) cannot
/// cross-infect each other. Production builds compile a zero-sized no-op so
/// the machine's plumbing is byte-for-byte the same in both builds.
/// Fault-injection and attribution state, one instance per machine: test
/// builds arm failures and count attempts on their own worker's handle, so
/// concurrently running workers (and the tests that drive them) cannot
/// cross-infect each other the way the previous process-global statics did.
/// Production builds carry the zero-sized no-op impl below; the `Ctx` field,
/// the feeder handle and the exec seams' call sites are identical in both.
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(test)]
pub(crate) struct Faults {
    /// Reconciliation failure injection.
    pub(super) fail_reconcile: AtomicBool,
    /// Rotation failure injection: entry-side and after-install.
    pub(super) fail_rotate: AtomicBool,
    pub(super) fail_rotate_after_install: AtomicBool,
    attempts: AtomicUsize,
    presence_resolves: AtomicUsize,
}

#[cfg(test)]
impl Default for Faults {
    fn default() -> Self {
        Self {
            fail_reconcile: AtomicBool::new(false),
            fail_rotate: AtomicBool::new(false),
            fail_rotate_after_install: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            presence_resolves: AtomicUsize::new(0),
        }
    }
}

#[cfg(test)]
impl Faults {
    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    pub(super) fn presence_resolves(&self) -> usize {
        self.presence_resolves.load(Ordering::SeqCst)
    }

    /// `true` only when this machine's reconcile seam is armed; every call
    /// counts an attempt, armed or not, because the stream tests poll the
    /// counter to prove the trigger plane actually ran a document's task.
    fn fail_next_reconcile(&self) -> bool {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.fail_reconcile.load(Ordering::SeqCst)
    }

    // The rotation seam's two points are separate because they have different
    // recovery stories (ADR 003 §19): failing at entry retries into the same
    // §19 sequence with nothing registered, so the retry leaves no unreferenced
    // pair; failing after the §19 step-1 install leaves the interrupted
    // attempt's pair rooted-but-unreferenced, which is the declared crash
    // window, not a bug to assert away.
    fn fail_next_rotate(&self, after_install: bool) -> bool {
        let armed = if after_install {
            &self.fail_rotate_after_install
        } else {
            &self.fail_rotate
        };
        if !armed.load(Ordering::SeqCst) {
            return false;
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// Presence-line attribution: only `handle_blob_arrival`, reading the
    /// value-digest projection, increments this.
    fn count_presence(&self) {
        self.presence_resolves.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(not(test))]
pub(crate) struct Faults;

#[cfg(not(test))]
impl Default for Faults {
    fn default() -> Self {
        Faults
    }
}

#[cfg(not(test))]
impl Faults {
    /// The one live production path (`handle_blob_arrival` compiles in both
    /// builds); a production handle stores nothing and just does not count.
    #[inline]
    fn count_presence(&self) {}
}

/// A fresh per-machine fault handle: the real state in test builds, the
/// unit no-op in production.
pub(crate) fn new_faults() -> Arc<Faults> {
    #[cfg(test)]
    let faults = Faults::default();
    #[cfg(not(test))]
    let faults = Faults;
    Arc::new(faults)
}

/// The branch every writer in the workspace uses; the delta machine resolves
/// the real path from the delta's physical branch id.
const MAIN_BRANCH: &str = "main";

/// Spawn the blob encryption worker.
///
/// Refuses to run when the repo has no encrypted-representation inventory: a
/// representation installed without one is never advertised to a relay and its
/// release is never reached, so the `ct:`/`pt:` tags would root the ciphertext's
/// outboard and the plaintext that serves it forever (ADR 003 §19, "the release
/// path is deliberate"). Skipping quietly is the one option that leaks.
/// Everything `spawn_blob_encryption_worker` reads from the booted repo.
/// Named fields (not a positional tuple) keep the two spawn sites
/// self-describing.
pub(crate) struct BlobEncryptionWorkerArgs {
    pub drawer_repo: Arc<DrawerRepo>,
    pub sql: SqlCtx,
    pub blobs_repo: Arc<crate::blobs::BlobsRepo>,
    pub facet_set_store: Arc<FacetSetRevisionStore>,
    pub facet_index: Arc<DocFacetSetIndexRepo>,
    pub domain_group: BigKeyhiveGroup,
    pub encryption_inventory_doc_id: DocumentId,
    pub parent_cancel_token: CancellationToken,
    /// The §15 rotation trigger; `None` where nothing sends rotations.
    pub rotation_rx: Option<RotationRequestRx>,
    /// The feeders' inputs: the doc part store (eligibility-group membership
    /// events), the eligibility part itself, and the local-only presence
    /// store (`/blobs` arrivals). The spawn builds the trigger channel and
    /// starts the feeder around them; there is no feeder-less encryption
    /// worker.
    pub feeder_repo_part_store: SharedPartStore,
    pub feeder_eligibility_part: PartKey,
    pub feeder_presence_store: SharedPartStore,
}

/// One machine request, at the loop head both requester shapes go through:
/// they buffer ahead of walker reads for the same reason — both carry (or
/// settle) the branch's pending delta cursor.
enum TaskRequest {
    Rotate(RotationRequest),
    Trigger(EncryptDocTrigger),
}

/// A document the presence plane has re-armed: the feeder resolved a blob
/// arrival or an eligibility-group membership event to this document and asks
/// the machine to run its reconcile-shaped task.
#[derive(Debug, Clone)]
pub(crate) struct EncryptDocTrigger {
    pub doc_id: DocId,
}

/// The feeder's dependencies over the two presence-plane revision streams.
///
/// One loop per line, feeding one trigger channel. The triggers become the
/// same keyed tasks the delta machine runs, so all re-arm paths serialize per
/// document by construction.
#[derive(Clone)]
pub(crate) struct EncryptionFeederArgs {
    /// The repository's doc part store: group-part membership events (the
    /// eligibility plane) are revisions of this store.
    pub repo_part_store: SharedPartStore,
    /// The part whose members are the documents eligible for encrypted
    /// representations.
    pub eligibility_part: PartKey,
    /// The local-only presence store: `/blobs` membership events are the
    /// blob-arrival plane.
    pub presence_store: SharedPartStore,
    pub facet_index: Arc<DocFacetSetIndexRepo>,
    pub sql: SqlCtx,
    pub trigger_tx: tokio::sync::mpsc::Sender<EncryptDocTrigger>,
    /// The worker machine's fault/attribution handle, shared with the feeder's
    /// presence line so `count_presence` lands on the same per-machine state.
    pub faults: Arc<Faults>,
}

/// Spawn the two feeder lines. Each line owns a durable cursor in the walker
/// state store and opens a *pull* revision reader at that cursor: restarts
/// resume committed work, never re-walk it, and the reader blocks on the
/// store's frontier notification between events. Cursors persist per line;
/// the whole feeder dies with the trigger channel's receiver.
/// The worker stopped and its trigger receiver went away; a line's send ends
/// with this, and `run_tail` maps it to [`FeederError::Shutdown`].
#[derive(Debug, thiserror::Error)]
#[error("the encryption worker's trigger channel closed")]
pub(crate) struct TriggerChannelClosed;

/// Why a feeder line ended. Shutdown is the worker dropping the trigger
/// receiver - the designed way lines die (see
/// `spawn_encryption_trigger_feeder`). Everything else is a real failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FeederError {
    /// The worker dropped the trigger receiver: the designed line end.
    #[error("trigger channel closed; feeder stops")]
    Shutdown,
    /// The line failed: the re-arm plane is dead at one cursor on one part.
    #[error("{0}")]
    Failed(eyre::Report),
}

pub(crate) async fn spawn_encryption_trigger_feeder(
    args: EncryptionFeederArgs,
) -> Res<tokio::task::JoinHandle<()>> {
    let EncryptionFeederArgs {
        repo_part_store,
        eligibility_part,
        presence_store,
        facet_index,
        sql,
        trigger_tx,
        faults,
    } = args;
    let state_repo = big_sync::SqliteDeltaWalkerStateRepo::new(
        sql.read_pool.clone(),
        sql.write_pool.clone(),
        ENCRYPTION_WORKER_STATE_ID.to_string(),
        "presence-tail".to_string(),
    )
    .await
    .map_err(|error| eyre::eyre!("encryption feeder state store: {error:?}"))?;

    let presence_line = tokio::spawn(tail_presence_line(
        presence_store,
        SqliteDeltaWalkerStateRepo::clone(&state_repo),
        Arc::clone(&facet_index),
        trigger_tx.clone(),
        Arc::clone(&faults),
    ));
    let eligibility_line = tokio::spawn(tail_eligibility_line(
        repo_part_store,
        eligibility_part,
        SqliteDeltaWalkerStateRepo::clone(&state_repo),
        Arc::clone(&facet_index),
        trigger_tx,
    ));
    Ok(tokio::spawn(async move {
        // A feeder line ends in exactly two ways: on error (Err), or when the
        // worker drops the trigger receiver (the machine maps its send
        // failure to the dedicated shutdown error). Watch the FIRST line to
        // end - the other line's own end follows on shutdown - and make a
        // failure fatal: the panic is escalated by the process-wide panic
        // handler, so a dead trigger plane can never idle until restart.
        let (line_name, first) = tokio::select! {
            result = presence_line => ("presence", result),
            result = eligibility_line => ("eligibility", result),
        };
        match first {
            // The worker shut down and its receiver went away: not a failure.
            Ok(Err(FeederError::Shutdown)) => {}
            Ok(Err(err)) => panic!("encryption feeder line {line_name} failed: {err:?}"),
            // A line never returns Ok in production, and a task abort happens
            // only when this supervisor itself is aborted for shutdown.
            Ok(Ok(())) | Err(_) => {
                tracing::warn!("encryption feeder line {line_name} ended without an error");
            }
        }
    }))
}

/// Feeder cursors, persisted in the walker state store's key rows. Handling
/// an event and committing its cursor are not atomic: a crash between the two
/// replays the event on restart, and at-least-once is the contract here —
/// triggers are idempotent re-arms (the machine merges them into the pending
/// per-key task), the event itself was already applied idempotently by the
/// same keyed task identity.
const PRESENCE_CURSOR_KEY: &[u8] = b"presence-cursor";
const ELIGIBILITY_CURSOR_KEY: &[u8] = b"eligibility-cursor";

async fn feeder_cursor(state: &big_sync::SqliteDeltaWalkerStateRepo, key: &[u8]) -> Res<u64> {
    let raw = state
        .get(key)
        .await
        .map_err(|error| eyre::eyre!("{error:?}"))?;
    let Some(raw) = raw else {
        return Ok(0);
    };
    let bytes: [u8; 8] = raw
        .as_slice()
        .try_into()
        .map_err(|_| eyre::eyre!("feeder cursor {key:?} is not a u64"))?;
    Ok(u64::from_le_bytes(bytes))
}

async fn feeder_commit_cursor(
    state: &big_sync::SqliteDeltaWalkerStateRepo,
    key: &'static [u8],
    cursor: u64,
) -> Res<()> {
    let mut tx = state
        .begin()
        .await
        .map_err(|error| eyre::eyre!("{error:?}"))?;
    tx.put(key.to_vec(), cursor.to_le_bytes().to_vec())
        .await
        .map_err(|error| eyre::eyre!("{error:?}"))?;
    tx.commit().await.map_err(|error| eyre::eyre!("{error:?}"))
}

/// Which presence-plane line a tail drives. Concrete dispatch instead of a
/// passed async closure: every borrow lives in one owned enum, the future
/// stays Send without regional-lifetime puzzles, and the two lines differ
/// only in their association step.
#[derive(Clone)]
enum TailKind {
    /// `/blobs` arrivals: re-arm documents whose `Blob` facet names the digest.
    Presence {
        facet_index: Arc<DocFacetSetIndexRepo>,
        trigger_tx: tokio::sync::mpsc::Sender<EncryptDocTrigger>,
        /// The machine's fault/attribution handle: the arrival attribution is
        /// per machine, not process-global.
        faults: Arc<Faults>,
    },
    /// Eligibility-group membership: re-arm the documents a branch doc carries.
    Eligibility {
        facet_index: Arc<DocFacetSetIndexRepo>,
        trigger_tx: tokio::sync::mpsc::Sender<EncryptDocTrigger>,
    },
}

impl TailKind {
    async fn handle(&self, obj_id: ObjKey) -> Res<()> {
        match self {
            TailKind::Presence {
                facet_index,
                trigger_tx,
                faults,
            } => handle_blob_arrival(facet_index, &obj_id, trigger_tx, faults).await,
            TailKind::Eligibility {
                facet_index,
                trigger_tx,
            } => handle_branch_eligible(facet_index, &obj_id, trigger_tx).await,
        }
    }
}

/// The `line stopped` exits below are deliberate: a feeder line failure must
/// not be swallowed (the machine's re-derivation would silently rot); the
/// supervisor join reports it and the machine sees the trigger channel close.
async fn run_tail(
    mut reader: Box<dyn big_sync::LocalPartRevisionReader>,
    cursor_key: &'static [u8],
    state_repo: SqliteDeltaWalkerStateRepo,
    kind: TailKind,
) -> Result<(), FeederError> {
    loop {
        let read = reader
            .next(RevisionReadLimits {
                max_entries: std::num::NonZeroUsize::new(64).expect("64 is non-zero"),
            })
            .await
            .map_err(|error| {
                FeederError::Failed(eyre::eyre!("feeder tail read failed: {error:?}"))
            })?;
        match read {
            RevisionRead::Entries { revision, entries } => {
                for event in &entries {
                    if let Err(error) = kind.handle(event_obj_id(event)).await {
                        // The receiver dying is shutdown, not a handler
                        // failure: the trigger channel IS the line's exit.
                        if error.is::<TriggerChannelClosed>() {
                            return Err(FeederError::Shutdown);
                        }
                        return Err(FeederError::Failed(eyre::eyre!(
                            "feeder trigger handler failed kind = {}: {error:?}",
                            event_kind(event)
                        )));
                    }
                }
                feeder_commit_cursor(&state_repo, cursor_key, revision)
                    .await
                    .map_err(|error| {
                        FeederError::Failed(eyre::eyre!("feeder cursor commit failed: {error:?}"))
                    })?;
            }
            RevisionRead::ReplayComplete { .. } => {}
        }
    }
}

/// The `/blobs` arrival line. A member add re-arms every document whose `Blob`
/// facet names the digest; a removal is nothing here — the release path rides
/// the pin worker's inventory diff, driven by facet deltas.
async fn tail_presence_line(
    presence_store: SharedPartStore,
    state_repo: SqliteDeltaWalkerStateRepo,
    facet_index: Arc<DocFacetSetIndexRepo>,
    trigger_tx: tokio::sync::mpsc::Sender<EncryptDocTrigger>,
    faults: Arc<Faults>,
) -> Result<(), FeederError> {
    let cursor = feeder_cursor(&state_repo, PRESENCE_CURSOR_KEY)
        .await
        .map_err(|error| {
            FeederError::Failed(eyre::eyre!(
                "blob-presence feeder cursor unreadable: {error:?}"
            ))
        })?;
    let req = big_sync_core::rpc::SubPartsRequest {
        lower_bound: cursor,
        targets: [big_sync_core::rpc::SubscriptionTarget::Part {
            part_id: crate::repo::blob_presence_part_id(),
            cursor: 0,
        }]
        .into_iter()
        .collect(),
    };
    let reader = match presence_store.open_revision_reader(req).await {
        Ok(Ok(reader)) => reader,
        Ok(Err(error)) => {
            return Err(FeederError::Failed(eyre::eyre!(
                "blob-presence feeder reader refused: {error:?}"
            )));
        }
        Err(error) => {
            return Err(FeederError::Failed(eyre::eyre!(
                "blob-presence feeder reader unavailable: {error:?}"
            )));
        }
    };
    run_tail(
        reader,
        PRESENCE_CURSOR_KEY,
        state_repo,
        TailKind::Presence {
            facet_index,
            trigger_tx,
            faults,
        },
    )
    .await
}

/// The eligibility line. A member add on the eligibility group's part re-arms
/// the documents the (branch-doc) member carries; a removal means the branch
/// left the group — a release concern the pin worker already runs (its facet
/// diff drops the pins; nothing here).
async fn tail_eligibility_line(
    repo_part_store: SharedPartStore,
    eligibility_part: PartKey,
    state_repo: SqliteDeltaWalkerStateRepo,
    facet_index: Arc<DocFacetSetIndexRepo>,
    trigger_tx: tokio::sync::mpsc::Sender<EncryptDocTrigger>,
) -> Result<(), FeederError> {
    let cursor = feeder_cursor(&state_repo, ELIGIBILITY_CURSOR_KEY)
        .await
        .map_err(|error| {
            FeederError::Failed(eyre::eyre!(
                "eligibility feeder cursor unreadable: {error:?}"
            ))
        })?;
    let req = big_sync_core::rpc::SubPartsRequest {
        lower_bound: cursor,
        targets: [big_sync_core::rpc::SubscriptionTarget::Part {
            part_id: eligibility_part,
            cursor: 0,
        }]
        .into_iter()
        .collect(),
    };
    let reader = match repo_part_store.open_revision_reader(req).await {
        Ok(Ok(reader)) => reader,
        Ok(Err(error)) => {
            return Err(FeederError::Failed(eyre::eyre!(
                "eligibility feeder reader refused: {error:?}"
            )));
        }
        Err(error) => {
            return Err(FeederError::Failed(eyre::eyre!(
                "eligibility feeder reader unavailable: {error:?}"
            )));
        }
    };
    run_tail(
        reader,
        ELIGIBILITY_CURSOR_KEY,
        state_repo,
        TailKind::Eligibility {
            facet_index,
            trigger_tx,
        },
    )
    .await
}

/// The object a part event names. Changed events carry the member's key;
/// removals the same key, so the line's handler sees one shape.
fn event_obj_id(event: &PartEvent) -> ObjKey {
    match event {
        PartEvent::Changed(evt) => evt.obj_id.clone(),
        PartEvent::Removed(evt) => evt.obj_id.clone(),
    }
}

fn event_kind(event: &PartEvent) -> &'static str {
    match event {
        PartEvent::Changed(_) => "changed",
        PartEvent::Removed(_) => "removed",
    }
}

/// Blob arrival → the documents whose `Blob` facet value names the digest.
///
/// The association is the facet-set index's value-digest projection, not the
/// facet key's id: a production `Blob` facet is authored with
/// `FacetKey::from(WellKnownFacetTag::Blob)`, so its id is `DEFAULT_FACET_ID`
/// and only the value carries the digest (in either ADR 003 §3 spelling —
/// `plaintext_blob_id` canonicalizes to the one `BlobId` compared here).
async fn handle_blob_arrival(
    facet_index: &DocFacetSetIndexRepo,
    obj_id: &ObjKey,
    trigger_tx: &tokio::sync::mpsc::Sender<EncryptDocTrigger>,
    faults: &Faults,
) -> Res<()> {
    let Ok(bytes32) = obj_id.to_bytes32() else {
        // A key that is not a 32-byte digest is not a blob's presence row.
        return Ok(());
    };
    let docs = facet_index
        .list_docs_for_blob_digest(&BlobId::new(bytes32))
        .await?;
    for membership in docs {
        faults.count_presence();
        trigger_tx
            .send(EncryptDocTrigger {
                doc_id: membership.doc_id,
            })
            .await
            .map_err(|_| TriggerChannelClosed)?;
    }
    Ok(())
}

/// A branch doc joined the eligibility group → the documents/branches that
/// branch doc carries. The membership object is the branch doc's key, which
/// is the same identity the facet-set rows key branches by.
async fn handle_branch_eligible(
    facet_index: &DocFacetSetIndexRepo,
    obj_id: &ObjKey,
    trigger_tx: &tokio::sync::mpsc::Sender<EncryptDocTrigger>,
) -> Res<()> {
    let bytes = obj_id.as_bytes().to_vec();
    let branch_doc_id = big_repo::DocumentId::new(bytes);
    let docs = facet_index
        .list_docs_for_branch_id(&branch_doc_id.to_string())
        .await?;
    for membership in docs {
        trigger_tx
            .send(EncryptDocTrigger {
                doc_id: membership.doc_id,
            })
            .await
            .map_err(|_| TriggerChannelClosed)?;
    }
    Ok(())
}

pub(crate) async fn spawn_blob_encryption_worker(
    args: BlobEncryptionWorkerArgs,
) -> Res<RepoStopToken> {
    let BlobEncryptionWorkerArgs {
        drawer_repo,
        sql,
        blobs_repo,
        facet_set_store,
        facet_index,
        domain_group,
        encryption_inventory_doc_id,
        feeder_repo_part_store,
        feeder_eligibility_part,
        feeder_presence_store,
        parent_cancel_token,
        rotation_rx,
    } = args;
    let encryption_inventory_doc_id = drawer_repo
        .resolve_doc_id_for_branch_doc_id(encryption_inventory_doc_id)
        .await?;
    let store = blobs_repo.iroh_store();
    let provider = blobs_repo.cipher_provider();
    // One fault handle per machine: the Ctx and the presence feeder share it,
    // so injected fault state never lives outside this worker.
    let faults = new_faults();
    let ctx = Arc::new(Ctx {
        drawer_repo: Arc::clone(&drawer_repo),
        sql: sql.clone(),
        store,
        provider,
        domain_id: domain_facet_id(&domain_group),
        domain_group,
        encryption_inventory_doc_id,
        faults: Arc::clone(&faults),
    });

    // The presence-plane feeder and its trigger channel: the feeder resolves
    // stream events to documents, the machine consumes the channel; the
    // receiver is dropped when the worker stops, which stops the feeder.
    let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel::<EncryptDocTrigger>(64);
    let feeder = spawn_encryption_trigger_feeder(EncryptionFeederArgs {
        repo_part_store: feeder_repo_part_store,
        eligibility_part: feeder_eligibility_part,
        presence_store: feeder_presence_store,
        facet_index: Arc::clone(&facet_index),
        sql: sql.clone(),
        trigger_tx,
        faults: Arc::clone(&faults),
    })
    .await?;

    let cancel_token = parent_cancel_token.child_token();
    // The worker's own child token: the feeder's lines stop with it too.
    let worker_cancel = cancel_token.child_token();
    let worker_handle = tokio::spawn(async move {
        let _feeder = feeder;
        let mut worker = Worker::new(ctx, rotation_rx, Some(trigger_rx));
        worker
            .run(facet_set_store, worker_cancel)
            .await
            .expect("blob encryption worker error");
    });
    Ok(RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

/// `grp:<base58(multibase(group_id))>` - the facet key id that names a domain
/// governed by a keyhive group (ADR 003 §19).
fn domain_facet_id(group: &BigKeyhiveGroup) -> String {
    format!(
        "grp:{}",
        utils_rs::hash::encode_base58_multibase(group.id().to_bytes())
    )
}

/// The `DocumentId` behind a `DocId`, for the keyhive membership query.
fn physical_doc_id(doc_id: &DocId) -> Res<big_repo::DocumentId> {
    doc_id
        .to_string()
        .parse()
        .map_err(|_| eyre::eyre!("document id '{doc_id}' is not a keyhive document id"))
}

/// Scheduling key: one physical branch. Collisions only serialize two branches
/// onto one slot, which the inline loop absorbs anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct EncryptionKey(u64);

fn encryption_facet_key(branch_id: &BranchId) -> EncryptionKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    EncryptionKey(hasher.finish())
}

/// The plaintext blob a `Blob` facet names.
///
/// The facet's `digest` is authoritative (ADR 003 §3 makes the multihash
/// spelling canonical), and the `db+blob` URL is the same digest in the bare
/// spelling, which is what a URL carries (`blob_url_contract_unchanged`).
/// Shared with the facet-set index's value-digest projection, which is also
/// keyed by what the facet VALUE names (`plaintext_blob_id` is the one
/// extraction helper the blob plane reads `Blob` facet values with).
pub(crate) fn plaintext_blob_id(blob: &Blob) -> Option<BlobId> {
    if let Some(blob_id) = digest_str_to_blob_id_lenient(&blob.digest) {
        return Some(blob_id);
    }
    for url_str in blob.urls.as_deref().unwrap_or_default() {
        let Ok(url) = url_str.parse::<url::Url>() else {
            continue;
        };
        if url.scheme() != crate::blobs::BLOB_SCHEME && url.scheme() != "daybook-blob" {
            continue;
        }
        if let Some(blob_id) = digest_str_to_blob_id_lenient(url.path().trim_start_matches('/')) {
            return Some(blob_id);
        }
    }
    None
}

/// Does this `Blob` facet already resolve through `cipher_key`?
fn resolves_through(blob: &Blob, cipher_key: &FacetKey) -> bool {
    let suffix = format!("via={cipher_key}");
    blob.urls
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|url| url.ends_with(&suffix))
}

/// Shared state: one domain, one inventory, one provider.
struct Ctx {
    drawer_repo: Arc<DrawerRepo>,
    sql: SqlCtx,
    store: iroh_blobs::api::Store,
    provider: Arc<CipherBlobProvider>,
    /// The facet key id naming this worker's domain.
    domain_id: String,
    domain_group: BigKeyhiveGroup,
    /// The encrypted-representation inventory. Required: see the spawn fn.
    encryption_inventory_doc_id: DocId,
    /// This machine's fault seams and attribution counters (`Faults`).
    /// Production builds carry the zero-sized no-op; tests hold the same
    /// handle the machine uses, which is what makes injected state per
    /// machine instead of process-global. Production code never reads it
    /// (the seams are test-only), hence the dead-code allowance there.
    #[cfg_attr(not(test), allow(dead_code))]
    faults: Arc<Faults>,
}

impl Ctx {
    /// Is `doc_id` a member of this worker's domain group, right now?
    ///
    /// This is `WorkerGroupScope::Groups`' predicate in the direction the
    /// drawer can answer it: the reverse index
    /// (`BigKeyhiveHandle::group_ids_containing_document`) is crate-private to
    /// big_repo, so membership is read as the group's access *on* the document,
    /// live, from the same keyhive the granting code writes to.
    async fn doc_is_eligible(&self, doc_id: &DocId) -> Res<bool> {
        use big_repo::keyhive_core::principal::identifier::Identifier;
        let physical = physical_doc_id(doc_id)?;
        // PR #51's string keys made `DocumentId` an `ObjKey`, so the 32-byte
        // width check is fallible at this edge — a key of another width is not
        // materializable as a signing key.
        let vk_bytes = physical
            .to_bytes32()
            .map_err(|err| eyre::eyre!("document id '{doc_id}' is not a signing key: {err}"))?;
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&vk_bytes)
            .map_err(|err| eyre::eyre!("document id '{doc_id}' is not a signing key: {err}"))?;
        let doc_ident = Identifier::from(vk);
        let group_ident = Identifier::from(self.domain_group.id());
        // `agent_access_on` reads the document's transitive members, which is the
        // same relation `group_ids_containing_document` reads from the other
        // side: the group is in there if and only if the document belongs to it.
        Ok(self
            .drawer_repo
            .big_repo
            .keyhive()
            .agent_access_on(&group_ident, doc_ident)
            .await
            .is_some())
    }

    /// The facet key a `Blob` facet's representation lives under.
    ///
    /// §19 names the domain in the facet key; §7 requires the ids to
    /// distinguish the several representations one document may hold, and a
    /// document with several blobs is ordinary here (a plug manifest declares
    /// one `Blob` facet per component, keyed by its hash). The blob facet's own
    /// id is the stable, derivable discriminator: it is already a sibling facet
    /// key in this same document, so appending it to the domain leaks nothing
    /// to the domain's readers that reading the document does not.
    fn cipher_facet_key(&self, blob_key: &FacetKey) -> FacetKey {
        FacetKey {
            tag: WellKnownFacetTag::CipherBlob.into(),
            id: format!("{}/{}", self.domain_id, blob_key.id),
        }
    }

    /// The JWK facet of this domain's key document. One key per key document
    /// under `keyScope: Document` (§19), so the id is the domain alone.
    fn jwk_facet_key(&self) -> FacetKey {
        FacetKey {
            tag: WellKnownFacetTag::Jwk.into(),
            id: self.domain_id.clone(),
        }
    }

    async fn blob_status(&self, hash: Hash) -> Res<Option<u64>> {
        match self
            .store
            .blobs()
            .status(hash)
            .await
            .map_err(|err| eyre::eyre!("{err:?}"))?
        {
            BlobStatus::Complete { size } => Ok(Some(size)),
            other => {
                tracing::debug!(%hash, status = ?other, "blob-encryption: entry not complete");
                Ok(None)
            }
        }
    }
}

/// A §15 rotation trigger: mint fresh keying material for every representation
/// the document already names, under its (unchanged) cipherBlob facet key.
#[derive(Debug, Clone)]
pub(crate) struct RotationRequest {
    pub doc_id: DocId,
}

pub(crate) type RotationRequestRx = tokio::sync::mpsc::Receiver<RotationRequest>;

/// The trigger surface for §15 rotation: send a `RotationRequest`, the facet
/// machine runs it as an `EncryptionTask` keyed like every other task, so a
/// rotation and a reconcile/delta for the same branch cannot race. The parent
/// wires the runtime facade around it (`Rt::request_doc_representations_rotation`).
pub(crate) fn rotation_channel() -> (
    tokio::sync::mpsc::Sender<RotationRequest>,
    RotationRequestRx,
) {
    tokio::sync::mpsc::channel(8)
}

/// Private machine owner.
struct Worker {
    ctx: Arc<Ctx>,
    /// The §15 rotation trigger. `None` where nothing rotates (a caller that
    /// never sends simply leaves the machine arm pending).
    rotation_rx: Option<RotationRequestRx>,
    /// The presence-plane triggers; `None` where no feeder runs.
    trigger_rx: Option<tokio::sync::mpsc::Receiver<EncryptDocTrigger>>,
}

/// What a document task does at its branch: make the declared blobs represented
/// at the given heads, or (§15) mint fresh keying material for the
/// representations that already exist. Both share one key identity, so a
/// rotation and a reconcile for the same branch cannot run concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncryptionTaskKind {
    Reconcile,
    Rotate,
}

/// One document's representation work, keyed by the walker's branch key.
#[derive(Debug, Clone)]
struct EncryptionTask {
    key: EncryptionKey,
    kind: EncryptionTaskKind,
    cursor: u64,
    doc_id: DocId,
    branch_id: BranchId,
    /// The delta's branch heads. `None` means the facet is gone or the branch
    /// carries no state: there is nothing to represent, but the walker's cursor
    /// still advances past it.
    heads: Option<ChangeHashSet>,
}

/// The only outcome a task reports: the document's state at its heads is
/// durable, or there was nothing to do there. Anything that failed is an `Err`
/// and is rescheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncryptionTaskOutput {
    Applied,
}

/// One document's representation work, off the worker's loop.
async fn run_encryption_task(task: EncryptionTask, ctx: Arc<Ctx>) -> Res<EncryptionTaskOutput> {
    let Some(branch) = ctx.branch_path_for(&task.doc_id, &task.branch_id).await? else {
        return Ok(EncryptionTaskOutput::Applied);
    };
    match task.kind {
        EncryptionTaskKind::Reconcile => {
            let heads = match &task.heads {
                Some(heads) => heads.clone(),
                // A presence-plane trigger runs against the state current when
                // it executes: a trigger enqueued behind other work must not
                // apply to the heads snapshot of its enqueue.
                None => {
                    let branch = BranchPathBuf::from(MAIN_BRANCH);
                    let Some(heads) = ctx
                        .drawer_repo
                        .get_branch_heads_for_path(&task.doc_id, &branch)
                        .await?
                    else {
                        return Ok(EncryptionTaskOutput::Applied);
                    };
                    heads
                }
            };
            let heads = &heads;
            #[cfg(test)]
            {
                eyre::ensure!(
                    !ctx.faults.fail_next_reconcile(),
                    "injected reconciliation failure for {}",
                    task.doc_id
                );
            }
            ctx.reconcile_document(&task.doc_id, &branch, heads).await?;
        }
        // §15: heads are read at execution time, not at enqueue time — the
        // rotation is committed at whatever heads are current when it runs, so
        // a request enqueued behind a delta applies to that delta's state.
        EncryptionTaskKind::Rotate => {
            #[cfg(test)]
            {
                eyre::ensure!(
                    !ctx.faults.fail_next_rotate(false),
                    "injected rotation failure (entry) for {}",
                    task.doc_id
                );
            }
            ctx.rotate_document(&task.doc_id, &branch).await?;
        }
    }
    Ok(EncryptionTaskOutput::Applied)
}

impl std::ops::Deref for Worker {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Worker {
    fn new(
        ctx: Arc<Ctx>,
        rotation_rx: Option<RotationRequestRx>,
        trigger_rx: Option<tokio::sync::mpsc::Receiver<EncryptDocTrigger>>,
    ) -> Self {
        Self {
            ctx,
            rotation_rx,
            trigger_rx,
        }
    }

    /// A `ConcurrentDeltaWalker` over the facet-set source, keyed by branch, and
    /// a keyed scheduler over the same key: one delta becomes one task, and the
    /// walker's cursor advances only in the completion handler, once the
    /// document's state is durable. The budget is deliberately *this* worker's
    /// own (§11's pass is a minutes-long disk-bound read, so it must not occupy
    /// a pin-reconciliation slot).
    async fn run(
        &mut self,
        facet_set_store: Arc<FacetSetRevisionStore>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        tracing::debug!(
            domain = %self.domain_id,
            inventory = %self.encryption_inventory_doc_id,
            "blob-encryption: worker starting"
        );
        // The worker starts reactive from its first revision: three sources —
        // the facet-set delta walker, the `/blobs` presence stream and the
        // eligibility-group membership stream (the last two via the feeder)
        // — cover every ordering (blob before doc, doc before blob,
        // eligibility before or after either), so no boot pass over the
        // corpus precedes the machine. See the feeder's state-id comment and
        // ADR 003 §13.
        let state = SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            ENCRYPTION_WORKER_STATE_ID,
            "facets",
        )
        .await
        .map_err(|error| ferr!("initializing blob-encryption FacetSet walker state: {error}"))?;
        let durable = state.progress().await?.upstream_revision;
        let reader = facet_set_store
            .open(
                FacetSetSelector::Tags(vec![
                    WellKnownFacetTag::Blob,
                    WellKnownFacetTag::CipherBlob,
                ]),
                durable,
            )
            .await
            .map_err(|error| ferr!("opening blob-encryption FacetSet reader: {error}"))?;
        let mut walker: ConcurrentDeltaWalker<
            '_,
            FacetSetRevisionStore,
            SqliteDeltaWalkerStateRepo,
            EncryptionKey,
        > = ConcurrentDeltaWalker::open(reader, state, |entry: &FacetDelta| {
            encryption_facet_key(&entry.key.branch_id)
        })
        .await
        .map_err(|error| ferr!("opening blob-encryption FacetSet walker: {error}"))?;
        let mut tasks = TokioKeyedScheduler::new(ENCRYPTION_TASK_BUDGET);
        // The newest unacked delta per key.
        let mut pending: HashMap<EncryptionKey, EncryptionTask> = HashMap::new();
        // Requests that arrived while the budget was busy, dispatched at the
        // next loop head ahead of new walker reads: both shape-carry a pending
        // delta cursor they settle, so neither may sit behind unbounded
        // delta traffic.
        let mut buffered: std::collections::VecDeque<TaskRequest> = Default::default();
        // Liveness of the two requester channels; a closed recv returns None
        // instantly, so these flags are what keep the select from spinning.
        let mut rot_alive = self.rotation_rx.is_some();
        let mut tri_alive = self.trigger_rx.is_some();
        loop {
            let available = ENCRYPTION_TASK_BUDGET.saturating_sub(tasks.active_count());
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                completion = tasks.next_completion() => {
                    self.on_task_completion(&mut tasks, &mut walker, &mut pending, completion?)
                        .await?;
                }
                // Budget-gated request dispatch: a fresh rotation or presence
                // Request dispatch: always polled — a trigger or rotation is
                // only *buffered* here, and both shapes carry the branch's
                // pending delta cursor they settle, so none may sit behind
                // unbounded delta traffic. Budget-gating happens at the loop
                // head's dispatch, never on whether we listen at all: a
                // listen-guarded arm deadlocks with budget free and an idle
                // walker, starving every buffered request. A channel close
                // ends *that* arm for good (the flag flips on the first None;
                // a closed recv returns None instantly, so without the flag
                // the select would spin); the feeder's exit decides how loud
                // that is.
                request = async {
                    tokio::select! {
                        biased;
                        rotation = async {
                            let received = match (self.rotation_rx.as_mut(), rot_alive) {
                                (Some(rx), true) => rx.recv().await,
                                _ => std::future::pending().await,
                            };
                            if received.is_none() {
                                rot_alive = false;
                            }
                            received
                        } => rotation.map(TaskRequest::Rotate),
                        trigger = async {
                            let received = match (self.trigger_rx.as_mut(), tri_alive) {
                                (Some(rx), true) => rx.recv().await,
                                _ => std::future::pending().await,
                            };
                            if received.is_none() {
                                tri_alive = false;
                            }
                            received
                        } => trigger.map(TaskRequest::Trigger),
                    }
                } => {
                    if let Some(request) = request {
                        buffered.push_back(request)
                    }
                }
                read = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        walker
                            .next(
                                std::num::NonZeroUsize::new(available)
                                    .expect("available is non-zero"),
                            )
                            .await
                    }
                } => match read? {
                    ConcurrentDeltaRead::ReplayComplete { .. } => {}
                    ConcurrentDeltaRead::Entries { entries, .. } => {
                        for delta in entries {
                            self.on_delta(&mut tasks, &mut pending, delta)?;
                        }
                    }
                },
                _ = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    tasks.tick(std::time::Instant::now())?;
                }
            }
            // Loop head: dispatch a buffered request into now-available
            // budget, before the next read can claim it.
            if ENCRYPTION_TASK_BUDGET.saturating_sub(tasks.active_count()) > 0
                && let Some(request) = buffered.pop_front()
            {
                match request {
                    TaskRequest::Rotate(request) => {
                        self.enqueue_rotation(&mut tasks, &mut pending, request)
                            .await?
                    }
                    TaskRequest::Trigger(request) => {
                        self.enqueue_trigger(&mut tasks, &mut pending, request)
                            .await?
                    }
                }
            }
        }
    }

    /// Turn a rotation request into the same keyed task shape the delta machine
    /// runs, on the same scheduler, so a rotation and a reconcile for one
    /// branch serialize by construction.
    ///
    /// A rotation also *subsumes* whatever reconciliation is pending for the
    /// branch: `rotate_document` runs the same reconcile first and then rotates,
    /// so the pending delta's cursor is carried onto the rotation task and
    /// acknowledged by its successful completion — leaving that cursor unacked
    /// instead would gate the walker's durable prefix forever.
    async fn enqueue_rotation(
        &mut self,
        tasks: &mut TokioKeyedScheduler<EncryptionKey, EncryptionTask, EncryptionTaskOutput>,
        pending: &mut HashMap<EncryptionKey, EncryptionTask>,
        request: RotationRequest,
    ) -> Res<()> {
        let Some(branch_id) = self.ctx.main_branch_id(&request.doc_id).await? else {
            tracing::warn!(
                doc_id = %request.doc_id,
                "blob-encryption: rotation requested for a document with no main branch; dropping"
            );
            return Ok(());
        };
        let key = encryption_facet_key(&branch_id);
        let carried = pending
            .remove(&key)
            .map(|pending| pending.cursor)
            .unwrap_or(0);
        let task = EncryptionTask {
            key,
            kind: EncryptionTaskKind::Rotate,
            cursor: carried,
            doc_id: request.doc_id,
            branch_id,
            // Rotation reads current heads at execution time.
            heads: None,
        };
        self.start_task(tasks, task)
    }

    /// A presence-plane trigger becomes the same keyed task a delta is, but
    /// with no walker position of its own: `heads: None` reads the state
    /// current at execution time, and `cursor` only carries whatever pending
    /// delta it *subsumed* — a trigger that lands while a delta for the same
    /// branch is unscheduled has effectively run that delta's reconcile, so
    /// the delta's cursor settles with the trigger's completion.
    async fn enqueue_trigger(
        &mut self,
        tasks: &mut TokioKeyedScheduler<EncryptionKey, EncryptionTask, EncryptionTaskOutput>,
        pending: &mut HashMap<EncryptionKey, EncryptionTask>,
        request: EncryptDocTrigger,
    ) -> Res<()> {
        let Some(branch_id) = self.ctx.main_branch_id(&request.doc_id).await? else {
            tracing::debug!(
                doc_id = %request.doc_id,
                "blob-encryption: trigger for a document with no main branch; dropping"
            );
            return Ok(());
        };
        let key = encryption_facet_key(&branch_id);
        let carried = pending
            .remove(&key)
            .map(|pending| pending.cursor)
            .unwrap_or(0);
        let task = EncryptionTask {
            key,
            kind: EncryptionTaskKind::Reconcile,
            cursor: carried,
            doc_id: request.doc_id,
            branch_id,
            heads: None,
        };
        self.start_task(tasks, task)
    }

    /// A delta becomes a task keyed by its branch, so a newer delta for the same
    /// branch supersedes an older one: `reconcile_document` recomputes the
    /// document's full blob state at its heads.
    fn on_delta(
        &mut self,
        tasks: &mut TokioKeyedScheduler<EncryptionKey, EncryptionTask, EncryptionTaskOutput>,
        pending: &mut HashMap<EncryptionKey, EncryptionTask>,
        delta: ConcurrentDelta<EncryptionKey, FacetDelta>,
    ) -> Res<()> {
        let entry = &delta.entry;
        let task = EncryptionTask {
            key: delta.key,
            kind: EncryptionTaskKind::Reconcile,
            cursor: delta.cursor,
            doc_id: entry.key.document_id.clone(),
            branch_id: entry.key.branch_id.clone(),
            heads: entry.current_branch_heads.clone().or_else(|| {
                entry
                    .current
                    .as_ref()
                    .map(|snapshot| snapshot.branch_heads.clone())
            }),
        };
        if let Some(existing) = pending.get(&task.key)
            && existing.cursor >= task.cursor
        {
            return Ok(());
        }
        pending.insert(task.key, task.clone());
        self.start_task(tasks, task)
    }

    fn start_task(
        &mut self,
        tasks: &mut TokioKeyedScheduler<EncryptionKey, EncryptionTask, EncryptionTaskOutput>,
        task: EncryptionTask,
    ) -> Res<()> {
        let future = run_encryption_task(task.clone(), Arc::clone(&self.ctx));
        tasks.replace(task.key, task, future)?;
        Ok(())
    }

    /// The walker cursor advances here and nowhere else: either the task's
    /// effect is durable, or the task failed and is rescheduled with backoff.
    /// A process that dies in between replays the delta from the walker's own
    /// state, so the failure is not lost either way.
    async fn on_task_completion(
        &mut self,
        tasks: &mut TokioKeyedScheduler<EncryptionKey, EncryptionTask, EncryptionTaskOutput>,
        walker: &mut ConcurrentDeltaWalker<
            '_,
            FacetSetRevisionStore,
            SqliteDeltaWalkerStateRepo,
            EncryptionKey,
        >,
        pending: &mut HashMap<EncryptionKey, EncryptionTask>,
        completion: TokioTaskCompletion<EncryptionTask, EncryptionTaskOutput>,
    ) -> Res<()> {
        let task = completion.command;
        match completion.result {
            Ok(EncryptionTaskOutput::Applied) => {
                if task.kind == EncryptionTaskKind::Rotate {
                    // The rotation subsumed the pending delta it carried, so
                    // settling that cursor here is what a reconcile completion
                    // would do; cursor 0 means nothing was carried.
                    if task.cursor != 0 {
                        walker.ack(task.key, task.cursor).await?;
                    }
                    return Ok(());
                }
                // The document's state is durable; only now may the walker
                // cursor advance past it. Cursor 0 attaches no walker position
                // (a presence trigger that subsumed nothing) and acking it
                // would be a move through no revisions.
                if task.cursor != 0 {
                    walker.ack(task.key, task.cursor).await?;
                }
                if pending
                    .get(&task.key)
                    .is_some_and(|existing| existing.cursor == task.cursor)
                {
                    pending.remove(&task.key);
                }
            }
            Err(error) => {
                // Not durable: the cursor must not advance and the document
                // must not be dropped, so it is rescheduled with backoff.
                tracing::warn!(
                    doc_id = %task.doc_id,
                    %error,
                    "blob-encryption: document delta failed; rescheduling"
                );
                let future = run_encryption_task(task.clone(), Arc::clone(&self.ctx));
                tasks.retry(
                    task.key,
                    task.clone(),
                    completion.retry,
                    ENCRYPTION_RETRY_DELAY,
                    future,
                )?;
            }
        }
        Ok(())
    }
}

impl Ctx {
    /// The branch path a delta's physical branch id belongs to.
    async fn branch_path_for(
        &self,
        doc_id: &DocId,
        branch_id: &BranchId,
    ) -> Res<Option<BranchPathBuf>> {
        let Some(entry) = self.drawer_repo.get_entry(doc_id).await? else {
            return Ok(None);
        };
        Ok(entry
            .branches
            .iter()
            .find(|(_, branch)| branch.branch_doc_id.to_string() == branch_id.0)
            .map(|(path, _)| BranchPathBuf::from(path.clone())))
    }

    /// The physical branch id behind `MAIN_BRANCH`, the same identity the delta
    /// machine keys its tasks with (`encryption_facet_key` collapses it), so a
    /// presence-trigger task and a delta task for the same branch share one
    /// key.
    async fn main_branch_id(&self, doc_id: &DocId) -> Res<Option<BranchId>> {
        let Some(entry) = self.drawer_repo.get_entry(doc_id).await? else {
            return Ok(None);
        };
        Ok(entry
            .branches
            .get(MAIN_BRANCH)
            .map(|branch| BranchId(branch.branch_doc_id.to_string())))
    }

    /// §19's sequence for every blob this document declares, at `heads`.
    async fn reconcile_document(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        let elig = self.doc_is_eligible(doc_id).await?;
        if !elig {
            return Ok(());
        }
        let Some(keys) = self
            .drawer_repo
            .facet_keys_at_branch_heads(doc_id, branch, heads)
            .await?
        else {
            return Ok(());
        };
        let blob_keys: Vec<FacetKey> = keys
            .iter()
            .filter(|key| key.tag == FacetTag::WellKnown(WellKnownFacetTag::Blob))
            .cloned()
            .collect();
        if blob_keys.is_empty() {
            return Ok(());
        }
        let mut read_keys = blob_keys.clone();
        read_keys.extend(blob_keys.iter().map(|key| self.cipher_facet_key(key)));
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch_heads(doc_id, branch, heads, Some(read_keys))
            .await?
        else {
            return Ok(());
        };

        for blob_key in &blob_keys {
            let Some(raw) = doc.facets.get(blob_key) else {
                continue;
            };
            let blob = match WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)? {
                WellKnownFacet::Blob(blob) => blob,
                other => {
                    // The tag said Blob, so this is an invariant break rather
                    // than caller input; name it and move on instead of
                    // stopping every other document.
                    tracing::warn!(
                        doc_id = %doc_id,
                        facet = %blob_key,
                        tag = ?other.tag(),
                        "blob-encryption: Blob facet decoded to another variant"
                    );
                    continue;
                }
            };
            let Some(plaintext) = plaintext_blob_id(&blob) else {
                tracing::warn!(
                    doc_id = %doc_id,
                    facet = %blob_key,
                    "blob-encryption: Blob facet names no blob digest"
                );
                continue;
            };
            // §14 requires the representation to be locally producible: a blob
            // this node does not hold cannot be encrypted, and there is no
            // queue for "fetch it first" in this worker.
            if self
                .blob_status(crate::blobs::blob_id_to_iroh_hash(plaintext.clone()))
                .await?
                .is_none()
            {
                tracing::debug!(
                    doc_id = %doc_id,
                    plaintext = %plaintext,
                    "blob-encryption: plaintext not stored locally, not mirroring"
                );
                continue;
            }
            let cipher_key = self.cipher_facet_key(blob_key);
            match doc.facets.get(&cipher_key) {
                Some(existing) => {
                    self.reuse_representation(
                        doc_id,
                        branch,
                        plaintext,
                        blob_key,
                        &cipher_key,
                        existing,
                    )
                    .await?
                }
                None => {
                    self.create_representation(
                        doc_id,
                        branch,
                        heads,
                        plaintext,
                        blob_key,
                        &cipher_key,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// §15 representation rotation for every representation this document
    /// already names, at its current heads.
    ///
    /// Runs the same reconcile first, so the subsumes-pending-delta claim the
    /// scheduler leans on holds: after this method succeeds, the document's
    /// blob state is current at whatever heads are live now — reconciliation
    /// and rotation in one act. Only *existing* representations rotate; a blob
    /// with no cipherBlob facet is left to the reconcile branch above.
    async fn rotate_document(&self, doc_id: &DocId, branch: &BranchPathBuf) -> Res<()> {
        if !self.doc_is_eligible(doc_id).await? {
            return Ok(());
        }
        let Some(heads) = self
            .drawer_repo
            .get_branch_heads_for_path(doc_id, branch)
            .await?
        else {
            return Ok(());
        };
        self.reconcile_document(doc_id, branch, &heads).await?;
        self.rotate_representations(doc_id, branch, &heads).await
    }

    async fn rotate_representations(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        let Some(keys) = self
            .drawer_repo
            .facet_keys_at_branch_heads(doc_id, branch, heads)
            .await?
        else {
            return Ok(());
        };
        let blob_keys: Vec<FacetKey> = keys
            .iter()
            .filter(|key| key.tag == FacetTag::WellKnown(WellKnownFacetTag::Blob))
            .cloned()
            .collect();
        if blob_keys.is_empty() {
            return Ok(());
        }
        let mut read_keys = blob_keys.clone();
        read_keys.extend(blob_keys.iter().map(|key| self.cipher_facet_key(key)));
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch_heads(doc_id, branch, heads, Some(read_keys))
            .await?
        else {
            return Ok(());
        };
        for blob_key in &blob_keys {
            let Some(raw) = doc.facets.get(blob_key) else {
                continue;
            };
            let blob = match WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)? {
                WellKnownFacet::Blob(blob) => blob,
                other => {
                    tracing::warn!(
                        doc_id = %doc_id,
                        facet = %blob_key,
                        tag = ?other.tag(),
                        "blob-rotation: Blob facet decoded to another variant"
                    );
                    continue;
                }
            };
            let Some(plaintext) = plaintext_blob_id(&blob) else {
                continue;
            };
            let Some(cipher_raw) = doc.facets.get(&self.cipher_facet_key(blob_key)) else {
                // §15 rotates representations that exist; creation is the
                // reconcile path's job.
                continue;
            };
            self.rotate_representation(
                doc_id,
                branch,
                plaintext,
                blob_key,
                &self.cipher_facet_key(blob_key),
                cipher_raw,
            )
            .await?;
        }
        Ok(())
    }

    /// One §15 representation rotation: `C1/K1 -> C2/K2`, the cipherBlob facet
    /// updated in place under its unchanged facet key (the id names the domain
    /// and the sibling `Blob` facet, not the ciphertext — §19, so application
    /// references do not move). Fresh keying material per §9's rule
    /// (salt-only rotation is deliberately impossible); the framing is
    /// carried over unchanged, so only the keying rotates.
    ///
    /// Order is §19's, adapted: the new representation is servable and rooted
    /// before anything names it, the JWK lands in a fresh key document (§16's
    /// migration: a new key-storage document per rotation; stopping grants on
    /// the old one is the §16 stronger-isolation step, not this operation),
    /// and the facet update is the commit point. The resolution URL names the
    /// facet key — unchanged — and is ensured last anyway. The old
    /// representation's release is nobody's direct write here: its pin leaves
    /// the desired set with this facet delta, and the pin worker's release
    /// leaf drops the old `ct:`/`pt:` tags (§19, "the release path is
    /// deliberate").
    async fn rotate_representation(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        plaintext: BlobId,
        blob_key: &FacetKey,
        cipher_key: &FacetKey,
        existing: &FacetRaw,
    ) -> Res<()> {
        let cipher =
            match WellKnownFacet::from_json(existing.clone(), WellKnownFacetTag::CipherBlob)? {
                WellKnownFacet::CipherBlob(cipher) => cipher,
                other => {
                    eyre::bail!(
                        "facet {cipher_key} in document {doc_id} decoded to {:?}, not a cipherBlob",
                        other.tag()
                    )
                }
            };
        // Rotation re-encrypts, so it needs the plaintext, like §14 does; a
        // plaintext this node does not hold is a skip with a warn, the same
        // shape reconcile_document takes.
        if self
            .blob_status(crate::blobs::blob_id_to_iroh_hash(plaintext.clone()))
            .await?
            .is_none()
        {
            tracing::debug!(
                doc_id = %doc_id,
                plaintext = %plaintext,
                "blob-rotation: plaintext not stored locally, not rotating"
            );
            return Ok(());
        }
        // The framing comes from the facet being rotated, so a rotation is
        // strictly a keying change.
        let encoding = EncodingParams::from_encoding_parameters(
            &cipher.content_encoding,
            &cipher.encoding_parameters,
        )?;
        #[cfg(test)]
        {
            eyre::ensure!(
                !self.faults.fail_next_rotate(false),
                "injected rotation failure (entry) for {doc_id}"
            );
        }
        // 1. §11 pass over P under the fresh key: C2 servable + rooted.
        let new_key = MasterKey::random();
        let c2_hash = self
            .provider
            .install(
                &self.store,
                &new_key,
                crate::blobs::blob_id_to_iroh_hash(plaintext.clone()),
                encoding,
            )
            .await?;
        eyre::ensure!(
            c2_hash != crate::blobs::blob_id_to_iroh_hash(plaintext.clone()),
            "rotation of {plaintext} did not change the representation digest"
        );
        let c2_len = self.blob_status(c2_hash).await?.ok_or_else(|| {
            eyre::eyre!("representation {c2_hash} is not complete immediately after install")
        })?;
        #[cfg(test)]
        {
            eyre::ensure!(
                !self.faults.fail_next_rotate(true),
                "injected rotation failure (after install) for {doc_id}"
            );
        }

        // 2. The fresh key document, for §16's migration shape. Same facet key
        // as any key document of this domain: the keyScope's one-key-per-doc
        // rule is per (document, domain), and a rotation mints a new document.
        let key_doc_id = self
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from(MAIN_BRANCH),
                facets: default(),
                user_path: None,
            })
            .await?;
        let jwk_key = self.jwk_facet_key();
        self.write_jwk_facet(&key_doc_id, &jwk_key, &new_key)
            .await?;
        let key_heads = self
            .drawer_repo
            .get_branch_heads_for_path(&key_doc_id, BranchPath::new(MAIN_BRANCH))
            .await?
            .ok_or_else(|| eyre::eyre!("key document {key_doc_id} has no {MAIN_BRANCH} branch"))?;
        let key_ref = format!("db+facet:///{key_doc_id}/{jwk_key}");

        // 3. The facet update, built at fresh heads (a change has to descend
        // from the state it replaces) — the commit point.
        let heads = self
            .drawer_repo
            .get_branch_heads_for_path(doc_id, branch)
            .await?
            .ok_or_else(|| eyre::eyre!("document {doc_id} has no {branch} branch"))?;
        self.write_cipher_facet(
            doc_id, branch, &heads, cipher_key, c2_hash, c2_len, &key_ref, key_heads, encoding,
        )
        .await?;

        // 5. The resolution already names this facet key; ensure it anyway, in
        // case the representation's first commit never got this far.
        self.write_resolution_url(doc_id, branch, blob_key, cipher_key)
            .await?;
        Ok(())
    }

    /// Steps 1-3 and 5 for a blob that has no representation yet.
    async fn create_representation(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        heads: &ChangeHashSet,
        plaintext: BlobId,
        blob_key: &FacetKey,
        cipher_key: &FacetKey,
    ) -> Res<()> {
        // A fresh key per (document, domain): §19's default `keyScope`.
        let key = MasterKey::random();

        // 1. §11 pass over P: compute C, install the virtual entry, register the
        // pair. `install` is the only public way in, so installed implies
        // servable and rooted.
        let c_hash = self
            .provider
            .install(
                &self.store,
                &key,
                crate::blobs::blob_id_to_iroh_hash(plaintext),
                EncodingParams::DEFAULT,
            )
            .await?;
        let c_len = self.blob_status(c_hash).await?.ok_or_else(|| {
            eyre::eyre!("representation {c_hash} is not complete immediately after install")
        })?;

        // 2. The JWK facet, in a key document of its own: §19 keeps the key out
        // of the document whose readers may only be entitled to serve.
        let key_doc_id = self
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from(MAIN_BRANCH),
                facets: default(),
                user_path: None,
            })
            .await?;
        let jwk_key = self.jwk_facet_key();
        self.write_jwk_facet(&key_doc_id, &jwk_key, &key).await?;
        let key_heads = self
            .drawer_repo
            .get_branch_heads_for_path(&key_doc_id, BranchPath::new(MAIN_BRANCH))
            .await?
            .ok_or_else(|| eyre::eyre!("key document {key_doc_id} has no {MAIN_BRANCH} branch"))?;
        let key_ref = format!("db+facet:///{key_doc_id}/{jwk_key}");

        // 3. The cipherBlob facet: from here C is nameable, so a reader that
        // resolves through this document can reach a representation that is
        // already servable.
        self.write_cipher_facet(
            doc_id,
            branch,
            heads,
            cipher_key,
            c_hash,
            c_len,
            &key_ref,
            key_heads,
            EncodingParams::DEFAULT,
        )
        .await?;

        // 5. The commit point, last: everything above is recoverable state, and
        // this is the write that makes the document resolve.
        self.write_resolution_url(doc_id, branch, blob_key, cipher_key)
            .await?;
        Ok(())
    }

    /// A representation already exists: make it servable *now*, and finish the
    /// commit point if a crash got in the way.
    ///
    /// This is what a restart needs. The virtual-provider registry is in-process
    /// state and the entries it serves are durable, so a node that restarts
    /// holds no pair for a `C` it already installed; re-registering the pair from
    /// the facet's own key and framing is the recovery.
    async fn reuse_representation(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        plaintext: BlobId,
        blob_key: &FacetKey,
        cipher_key: &FacetKey,
        existing: &FacetRaw,
    ) -> Res<()> {
        let cipher =
            match WellKnownFacet::from_json(existing.clone(), WellKnownFacetTag::CipherBlob)? {
                WellKnownFacet::CipherBlob(cipher) => cipher,
                other => {
                    eyre::bail!(
                        "facet {cipher_key} in document {doc_id} decoded to {:?}, not a cipherBlob",
                        other.tag()
                    )
                }
            };
        let ct_blob_id = digest_str_to_blob_id_lenient(&cipher.representation.digest).ok_or_else(|| {
            eyre::eyre!(
                "cipherBlob {cipher_key} in document {doc_id} names digest {:?}, which is not a blob digest",
                cipher.representation.digest
            )
        })?;
        let c_hash = crate::blobs::blob_id_to_iroh_hash(ct_blob_id);
        let p_hash = crate::blobs::blob_id_to_iroh_hash(plaintext.clone());

        // The key and the framing come from the facet that pinned them, so a
        // rotated or repointed key cannot silently change what C decrypts under.
        let keys = DocKeySource::new(
            Arc::clone(&self.drawer_repo),
            doc_id.clone(),
            branch.clone(),
        );
        let key = keys.key_for(&c_hash).await?;
        let encoding = keys.encoding_for(&c_hash).await?;
        match self.blob_status(c_hash).await? {
            // The entry is still there: only the in-process pair is missing.
            Some(_) => {
                self.provider
                    .register_pair(&self.store, c_hash, &key, p_hash, encoding)
                    .await?;
            }
            // A crash between the §11 pass and the facet write can leave the
            // facet without its entry; re-derivation is deterministic, so the
            // check is that it reproduces the digest the facet already names.
            None => {
                let recomputed = self
                    .provider
                    .install(&self.store, &key, p_hash, encoding)
                    .await?;
                eyre::ensure!(
                    recomputed == c_hash,
                    "re-deriving the representation for {plaintext} produced {recomputed}, but \
                     cipherBlob {cipher_key} names {c_hash}"
                );
            }
        }

        self.write_resolution_url(doc_id, branch, blob_key, cipher_key)
            .await?;

        Ok(())
    }

    async fn write_jwk_facet(
        &self,
        key_doc_id: &DocId,
        jwk_key: &FacetKey,
        key: &MasterKey,
    ) -> Res<()> {
        let jwk = JwkOct::from_master_key(key);
        self.drawer_repo
            .update_at_heads_with_scope(
                DocPatch {
                    id: key_doc_id.clone(),
                    facets_set: [(
                        jwk_key.clone(),
                        FacetRaw::from(WellKnownFacet::Jwk(Jwk {
                            kty: jwk.kty,
                            members: serde_json::json!({ "k": jwk.k }),
                        })),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new(MAIN_BRANCH),
                None,
                FacetWriteScope::System,
            )
            .await
            .map_err(|err| eyre::eyre!("writing JWK facet {jwk_key} into {key_doc_id}: {err}"))?;
        Ok(())
    }

    /// Step 3: the facet that names `C`, keyed by the domain and the blob it
    /// mirrors, with the key state it was built under pinned.
    #[expect(clippy::too_many_arguments)]
    async fn write_cipher_facet(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        heads: &ChangeHashSet,
        cipher_key: &FacetKey,
        c_hash: Hash,
        c_len: u64,
        key_ref: &str,
        key_heads: ChangeHashSet,
        encoding: EncodingParams,
    ) -> Res<()> {
        eyre::ensure!(
            !key_heads.0.is_empty(),
            "refusing to write cipherBlob {cipher_key} with empty keyRefHeads: an empty-heads \
             reference means 'this document', and the key never lives beside the representation \
             (ADR 003 §19)"
        );
        let cipher = CipherBlob {
            representation: Representation {
                // §3: the multihash spelling is canonical for a facet digest.
                digest: blob_id_to_digest_str(BlobId::new(*c_hash.as_bytes())),
                length_octets: c_len,
            },
            content_encoding: CONTENT_ENCODING_AES128GCM.to_string(),
            key_ref: key_ref
                .parse()
                .map_err(|err| eyre::eyre!("cipherBlob keyRef {key_ref:?} is not a URL: {err}"))?,
            key_ref_heads: key_heads,
            encoding_parameters: encoding.to_encoding_parameters(),
        };
        self.drawer_repo
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        cipher_key.clone(),
                        FacetRaw::from(WellKnownFacet::CipherBlob(cipher)),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                branch,
                Some(heads.clone()),
                FacetWriteScope::System,
            )
            .await
            .map_err(|err| {
                eyre::eyre!("writing cipherBlob facet {cipher_key} into {doc_id}: {err}")
            })?;
        Ok(())
    }

    /// Step 5: the only write that makes the document *resolve* to `C`.
    ///
    /// Read and written at the **current** heads, not at the heads the decision
    /// was made at: this change has to descend from the `cipherBlob` write above
    /// it, or a reader following the history of the resolution would not see the
    /// representation it resolves to. Reading here is also what keeps a
    /// concurrent edit to the `Blob` facet from being clobbered.
    async fn write_resolution_url(
        &self,
        doc_id: &DocId,
        branch: &BranchPathBuf,
        blob_key: &FacetKey,
        cipher_key: &FacetKey,
    ) -> Res<()> {
        let Some(heads) = self
            .drawer_repo
            .get_branch_heads_for_path(doc_id, branch)
            .await?
        else {
            return Ok(());
        };
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch_heads(
                doc_id,
                branch,
                &heads,
                Some(vec![blob_key.clone()]),
            )
            .await?
        else {
            return Ok(());
        };
        let Some(raw) = doc.facets.get(blob_key) else {
            return Ok(());
        };
        let mut blob = match WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)? {
            WellKnownFacet::Blob(blob) => blob,
            other => {
                eyre::bail!(
                    "facet {blob_key} in document {doc_id} decoded to {:?}, not a Blob",
                    other.tag()
                )
            }
        };
        if resolves_through(&blob, cipher_key) {
            return Ok(());
        }
        // The path must name the plaintext digest, not the facet key's own id:
        // every reader of `Blob.urls` (`blob_pins_from_facet_value` in the pin
        // worker, `plaintext_blob_id` here) treats the `db+blob` path as a
        // blob digest, and a production facet's id may be any base58 string —
        // `BlobId`'s zero-padding parse of foreign ids would turn it into a
        // pin on a hash that names no blob.
        let plaintext = plaintext_blob_id(&blob).ok_or_else(|| {
            eyre::eyre!("the Blob facet {blob_key} of document {doc_id} names no blob digest")
        })?;
        let url = format!(
            "{}:///{}?via={}",
            crate::blobs::BLOB_SCHEME,
            plaintext,
            cipher_key
        );
        blob.urls.get_or_insert_with(Vec::new).push(url);
        self.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(blob_key.clone(), FacetRaw::from(WellKnownFacet::Blob(blob)))]
                        .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                branch,
                Some(heads),
            )
            .await
            .map_err(|err| {
                eyre::eyre!("adding the resolution URL to {blob_key} in {doc_id}: {err}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
