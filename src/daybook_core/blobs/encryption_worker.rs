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
//! state produces no delta of its own — hence the pass over the facet index
//! before the delta machine, which is also what makes a restarted node servable
//! again (the virtual-provider registry is in-process; the entries are durable).
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
use big_sync::DeltaWalkerStateRepo as _;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use iroh_blobs::Hash;
use iroh_blobs::api::proto::BlobStatus;

/// Walker state for the facet-set machine. Independent of the pin workers'
/// cursors: this worker reads the same source for a different question.
pub(crate) const ENCRYPTION_WORKER_STATE_ID: &str = "@daybook/core/blob-encryption-worker";

/// ADR 003 §11: the pass is a full sequential read of the plaintext. One blob
/// in flight is the entire budget, and it is deliberately *this worker's* own
/// budget - a minutes-long disk-bound pass must not occupy a pin-reconciliation
/// slot.
const ENCRYPTION_TASK_BUDGET: usize = 1;

/// A failed document delta is rescheduled with backoff rather than dropped or
/// waved through: the walker cursor must not advance past work that never
/// happened, and a transient store or drawer failure must not cost the document
/// until the next boot's pass.
const ENCRYPTION_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Test seam for the rescheduling contract. Every production failure this worker
/// can see is either racy or needs a write to the document - and a document
/// write would itself produce the delta the contract must be tested without.
/// Compiled out of production builds.
#[cfg(test)]
mod faults {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    pub(super) static FAIL_RECONCILE: AtomicBool = AtomicBool::new(false);
    pub(super) static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    pub(super) fn attempts() -> usize {
        ATTEMPTS.load(Ordering::SeqCst)
    }

    pub(super) fn fail_next_reconcile() -> bool {
        if !FAIL_RECONCILE.load(Ordering::SeqCst) {
            return false;
        }
        ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        true
    }
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
    pub encryption_inventory_doc_id: Option<DocumentId>,
    pub parent_cancel_token: CancellationToken,
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
        parent_cancel_token,
    } = args;
    let Some(encryption_inventory_doc_id) = encryption_inventory_doc_id else {
        eyre::bail!(
            "refusing to run the blob encryption worker: this repo has no encrypted-representation \
             inventory, so a representation could be neither advertised nor released (ADR 003 §19)"
        );
    };
    let encryption_inventory_doc_id = drawer_repo
        .resolve_doc_id_for_branch_doc_id(encryption_inventory_doc_id)
        .await?;
    let store = blobs_repo.iroh_store();
    let provider = blobs_repo.cipher_provider();
    let ctx = Arc::new(Ctx {
        drawer_repo,
        sql,
        store,
        provider,
        domain_id: domain_facet_id(&domain_group),
        domain_group,
        encryption_inventory_doc_id,
    });

    let cancel_token = parent_cancel_token.child_token();
    let worker_handle = tokio::spawn({
        let ctx = Arc::clone(&ctx);
        let cancel_token = cancel_token.clone();
        async move {
            let mut worker = Worker::new(ctx);
            worker
                .run(facet_set_store, facet_index, cancel_token)
                .await
                .expect("blob encryption worker error");
        }
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
fn plaintext_blob_id(blob: &Blob) -> Option<BlobId> {
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

/// Private machine owner.
struct Worker {
    ctx: Arc<Ctx>,
}

/// One document's representation work, keyed by the walker's branch key.
#[derive(Debug, Clone)]
struct EncryptionTask {
    key: EncryptionKey,
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
    let Some(heads) = &task.heads else {
        return Ok(EncryptionTaskOutput::Applied);
    };
    let Some(branch) = ctx.branch_path_for(&task.doc_id, &task.branch_id).await? else {
        return Ok(EncryptionTaskOutput::Applied);
    };
    #[cfg(test)]
    {
        eyre::ensure!(
            !faults::fail_next_reconcile(),
            "injected reconciliation failure for {}",
            task.doc_id
        );
    }
    ctx.reconcile_document(&task.doc_id, &branch, heads).await?;
    Ok(EncryptionTaskOutput::Applied)
}

impl std::ops::Deref for Worker {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Worker {
    fn new(ctx: Arc<Ctx>) -> Self {
        Self { ctx }
    }

    async fn run(
        &mut self,
        facet_set_store: Arc<FacetSetRevisionStore>,
        facet_index: Arc<DocFacetSetIndexRepo>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        tracing::debug!(
            domain = %self.domain_id,
            inventory = %self.encryption_inventory_doc_id,
            "blob-encryption: worker starting"
        );
        self.reconcile_eligible_documents(&facet_index, cancel_token.clone())
            .await?;
        self.run_facet_machine(facet_set_store, cancel_token).await
    }

    /// The pass that makes eligibility-as-a-state work, and the one that makes a
    /// restarted node servable again.
    ///
    /// The facet index answers "which documents declare a blob" without
    /// replaying history, so this covers documents that became eligible (or
    /// were written) before this worker existed - a delta-only worker cannot
    /// see them, because being eligible is not an event.
    ///
    /// Each indexed document becomes the *same* keyed task the delta machine
    /// runs, on this pass's own scheduler with the machine's retry mechanics:
    /// one task per `EncryptionKey` source, failures rescheduled with
    /// [`ENCRYPTION_RETRY_DELAY`] backoff until the document's state is durable,
    /// so a store or drawer failure in one document's step is not a lost
    /// boot-pass retry ("only at the next boot"). The scheduler budget is
    /// deliberately still this worker's own; the machine opens only after the
    /// pass, so the two never run the same document at once.
    async fn reconcile_eligible_documents(
        &self,
        facet_index: &DocFacetSetIndexRepo,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let memberships = facet_index
            .list_docs_for_tag(WellKnownFacetTag::Blob.as_str())
            .await?;
        let mut seen = std::collections::HashSet::new();
        let mut tasks = TokioKeyedScheduler::new(ENCRYPTION_TASK_BUDGET);
        // The number of documents whose state is not durable yet: a failed
        // completion stays counted, its task comes back on the retry path.
        let mut outstanding = 0usize;
        for membership in memberships {
            if !seen.insert(membership.doc_id.clone()) {
                continue;
            }
            let branch = BranchPathBuf::from(MAIN_BRANCH);
            let Some(heads) = self
                .drawer_repo
                .get_branch_heads_for_path(&membership.doc_id, &branch)
                .await?
            else {
                continue;
            };
            let Some(branch_id) = self.main_branch_id(&membership.doc_id).await? else {
                continue;
            };
            let key = encryption_facet_key(&branch_id);
            let task = EncryptionTask {
                key,
                // The pass has no walker cursor to advance; the cursor is the
                // delta machine's bookkeeping and is unused here.
                cursor: 0,
                doc_id: membership.doc_id.clone(),
                branch_id: branch_id.clone(),
                heads: Some(heads),
            };
            let future = run_encryption_task(task.clone(), Arc::clone(&self.ctx));
            tasks.replace(key, task, future)?;
            outstanding += 1;
        }
        while outstanding > 0 {
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                completion = tasks.next_completion() => {
                    let completion = completion?;
                    if completion.result.is_ok() {
                        // Durable; there is no walker cursor to ack here.
                        outstanding -= 1;
                        continue;
                    }
                    // Not durable: the document must not be dropped, so it is
                    // rescheduled with backoff, exactly as a failed delta is.
                    tracing::warn!(
                        doc_id = %completion.command.doc_id,
                        error = %completion.result.as_ref().unwrap_err(),
                        "blob-encryption: boot-pass document task failed; rescheduling"
                    );
                    let task = completion.command;
                    let future = run_encryption_task(task.clone(), Arc::clone(&self.ctx));
                    tasks.retry(
                        task.key,
                        task.clone(),
                        completion.retry,
                        ENCRYPTION_RETRY_DELAY,
                        future,
                    )?;
                }
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
        }
        Ok(())
    }

    /// A `ConcurrentDeltaWalker` over the facet-set source, keyed by branch, and
    /// a keyed scheduler over the same key: one delta becomes one task, and the
    /// walker's cursor advances only in the completion handler, once the
    /// document's state is durable. The budget is deliberately *this* worker's
    /// own (§11's pass is a minutes-long disk-bound read, so it must not occupy
    /// a pin-reconciliation slot).
    async fn run_facet_machine(
        &mut self,
        facet_set_store: Arc<FacetSetRevisionStore>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
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
        }
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
                // The document's state is durable; only now may the walker
                // cursor advance past it.
                walker.ack(task.key, task.cursor).await?;
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
    /// boot-pass task and a delta task for the same branch share one key.
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
        if !self.doc_is_eligible(doc_id).await? {
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
                    .await?
                }
            }
        }
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
            doc_id, branch, heads, cipher_key, c_hash, c_len, &key_ref, key_heads,
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
            // A crash between the pass and the facet write can leave the facet
            // without its entry; re-deriving is deterministic, so the check is
            // that it reproduces the digest the facet already names.
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
            encoding_parameters: EncodingParams::DEFAULT.to_encoding_parameters(),
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
        let url = format!(
            "{}:///{}?via={}",
            crate::blobs::BLOB_SCHEME,
            blob_key.id,
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
mod tests {
    use super::*;
    use crate::blobs::blob_id_to_iroh_hash;
    use crate::blobs::encrypt::{TAG_CT_PREFIX, TAG_PT_PREFIX};
    use crate::index::facet_set::DocFacetTagMembership;
    use crate::test_support::{DaybookTestContext, test_cx};
    use daybook_types::doc::BlobPin;

    /// The worker's context, built the way `rt` builds it (the group comes from
    /// the authority, the inventory from the repo config).
    async fn test_ctx(ctx: &DaybookTestContext, group: Option<BigKeyhiveGroup>) -> Res<Arc<Ctx>> {
        let authority =
            crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;
        let group = match group {
            Some(group) => group,
            None => authority.encrypted_blob_docs.clone(),
        };
        let inventory = ctx
            .rt
            .rcx
            .encryption_inventory_doc_id
            .clone()
            .expect("test repos always create the encrypted-representation inventory");
        Ok(Arc::new(Ctx {
            drawer_repo: Arc::clone(&ctx.drawer_repo),
            sql: ctx.rt.rcx.sql.clone(),
            store: ctx.rt.blobs_repo.iroh_store(),
            provider: ctx.rt.blobs_repo.cipher_provider(),
            domain_id: domain_facet_id(&group),
            domain_group: group,
            encryption_inventory_doc_id: ctx
                .drawer_repo
                .resolve_doc_id_for_branch_doc_id(inventory)
                .await?,
        }))
    }

    /// A document with one `Blob` facet for a plaintext this node stores: the
    /// shape a photo's document has.
    async fn stage_document_with_blob(
        ctx: &DaybookTestContext,
        plaintext: BlobId,
    ) -> Res<(DocId, FacetKey)> {
        let blob_key = FacetKey {
            tag: WellKnownFacetTag::Blob.into(),
            id: plaintext.to_string(),
        };
        let doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from(MAIN_BRANCH),
                facets: [(
                    blob_key.clone(),
                    FacetRaw::from(WellKnownFacet::Blob(Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 4096,
                        // §3's canonical spelling, which is what a conformant
                        // writer produces.
                        digest: blob_id_to_digest_str(plaintext.clone()),
                        inline: None,
                        urls: Some(vec![format!(
                            "{}:///{}",
                            crate::blobs::BLOB_SCHEME,
                            plaintext
                        )]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        Ok((doc_id, blob_key))
    }

    async fn read_facet(
        drawer: &DrawerRepo,
        doc_id: &DocId,
        key: &FacetKey,
    ) -> Res<Option<FacetRaw>> {
        let Some(heads) = drawer
            .get_branch_heads_for_path(doc_id, &BranchPathBuf::from(MAIN_BRANCH))
            .await?
        else {
            return Ok(None);
        };
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch_heads(
                doc_id,
                &BranchPathBuf::from(MAIN_BRANCH),
                &heads,
                Some(vec![key.clone()]),
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(doc.facets.get(key).cloned())
    }

    async fn read_cipherblob(
        drawer: &DrawerRepo,
        doc_id: &DocId,
        key: &FacetKey,
    ) -> Res<Option<CipherBlob>> {
        let Some(raw) = read_facet(drawer, doc_id, key).await? else {
            return Ok(None);
        };
        match WellKnownFacet::from_json(raw, WellKnownFacetTag::CipherBlob)? {
            WellKnownFacet::CipherBlob(cipher) => Ok(Some(cipher)),
            other => eyre::bail!("expected a cipherBlob facet, got {:?}", other.tag()),
        }
    }

    async fn read_blob(drawer: &DrawerRepo, doc_id: &DocId, key: &FacetKey) -> Res<Option<Blob>> {
        let Some(raw) = read_facet(drawer, doc_id, key).await? else {
            return Ok(None);
        };
        match WellKnownFacet::from_json(raw, WellKnownFacetTag::Blob)? {
            WellKnownFacet::Blob(blob) => Ok(Some(blob)),
            other => eyre::bail!("expected a Blob facet, got {:?}", other.tag()),
        }
    }

    /// The whole sequence, end to end: after one reconcile the document names a
    /// representation that a *reader* - resolving the key through the document,
    /// exactly as a serving node does - can fetch and decrypt back to the
    /// plaintext.
    #[tokio::test(flavor = "multi_thread")]
    async fn eligible_document_gets_a_servable_representation() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Worker::new(test_ctx(&ctx, None).await?);
        let plaintext_bytes = b"blob-encryption worker: eligible".to_vec();
        let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
            .await?
            .expect("document has a branch");

        worker
            .reconcile_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH), &heads)
            .await?;

        let cipher = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("the document now names a representation");
        let c = digest_str_to_blob_id_lenient(&cipher.representation.digest)
            .expect("the facet digest is a blob digest");
        assert_eq!(
            cipher.representation.digest,
            blob_id_to_digest_str(c.clone()),
            "a facet digest is the ADR §3 multihash spelling"
        );
        assert_eq!(cipher.content_encoding, CONTENT_ENCODING_AES128GCM);
        assert_eq!(
            cipher.encoding_parameters,
            EncodingParams::DEFAULT.to_encoding_parameters()
        );
        assert!(
            !cipher.key_ref_heads.0.is_empty(),
            "a cross-document keyRef must pin the key state it meant"
        );
        assert_ne!(
            cipher.representation.digest,
            blob_id_to_digest_str(plaintext),
            "the representation must not be the plaintext digest"
        );

        // Servable: fetch C from the store and decrypt it through the document
        // layer, which is what a peer does.
        let c_hash = blob_id_to_iroh_hash(c);
        assert!(
            worker.blob_status(c_hash).await?.is_some(),
            "the representation entry is complete after install"
        );
        let keys = DocKeySource::new(
            Arc::clone(&ctx.drawer_repo),
            doc_id.clone(),
            BranchPathBuf::from(MAIN_BRANCH),
        );
        let decrypted = crate::blobs::encrypt::get_decrypted(&worker.store, &keys, c_hash).await?;
        assert_eq!(decrypted, plaintext_bytes);

        // The commit point, and it is what makes the document *resolve*.
        let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is still there");
        assert!(
            resolves_through(&blob, &cipher_key),
            "the Blob facet must resolve through the representation, got {:?}",
            blob.urls
        );
        ctx.stop().await?;
        Ok(())
    }

    /// The gate itself: the same document, the same worker, a group the document
    /// is not a member of. Nothing is produced.
    ///
    /// Fails on the eligibility check being absent - the positive test above
    /// passes either way, so this is the one that pins "eligible" rather than
    /// "every document gets encrypted".
    #[tokio::test(flavor = "multi_thread")]
    async fn document_outside_the_domain_group_is_not_encrypted() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let outsider_group = ctx
            .rt
            .rcx
            .big_repo
            .create_group_with_parents(Vec::new())
            .await?;
        let worker = Worker::new(test_ctx(&ctx, Some(outsider_group)).await?);
        let plaintext = ctx
            .rt
            .blobs_repo
            .put(b"blob-encryption worker: outsider")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &BranchPathBuf::from(MAIN_BRANCH))
            .await?
            .expect("document has a branch");

        worker
            .reconcile_document(&doc_id, &BranchPathBuf::from(MAIN_BRANCH), &heads)
            .await?;

        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_none(),
            "a document outside the domain group must not get a representation"
        );
        let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is untouched");
        assert!(
            !resolves_through(&blob, &cipher_key),
            "nothing may point at a representation that was not produced"
        );
        ctx.stop().await?;
        Ok(())
    }

    /// A second pass reproduces the same representation instead of minting a new
    /// key: the salt is a pure function of (key, plaintext), so the only way to
    /// stay stable is to reuse the facet's own key.
    #[tokio::test(flavor = "multi_thread")]
    async fn second_pass_reuses_the_representation() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Worker::new(test_ctx(&ctx, None).await?);
        let plaintext = ctx
            .rt
            .blobs_repo
            .put(b"blob-encryption worker: idempotent")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let branch = BranchPathBuf::from(MAIN_BRANCH);
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch)
            .await?
            .expect("document has a branch");

        worker.reconcile_document(&doc_id, &branch, &heads).await?;
        let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("first pass installs a representation");
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch)
            .await?
            .expect("document has a branch");
        worker.reconcile_document(&doc_id, &branch, &heads).await?;

        let second = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("second pass keeps a representation");
        assert_eq!(
            first.representation.digest, second.representation.digest,
            "a re-run must not mint a second representation"
        );
        let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is still there");
        let via_count = blob
            .urls
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|url| url.contains("?via="))
            .count();
        assert_eq!(
            via_count, 1,
            "the commit point is written once, got {:?}",
            blob.urls
        );
        ctx.stop().await?;
        Ok(())
    }

    /// The inventory write the pin worker's `apply_inventory_diff` performs:
    /// one BlobPin facet per ciphertext, on the inventory doc's `main` branch
    /// through the plain (user-scoped) facet write. Tests drive the same
    /// production write when they stand in for the pin diff.
    async fn write_inventory_pin(
        drawer: &DrawerRepo,
        inventory_doc_id: &DocId,
        digest: &str,
        pin: Option<BlobPin>,
    ) -> Res<()> {
        let key = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: digest.to_string(),
        };
        let (facets_set, facets_remove) = match pin {
            Some(pin) => (
                [(key.clone(), FacetRaw::from(WellKnownFacet::BlobPin(pin)))].into(),
                vec![],
            ),
            None => (std::collections::HashMap::new(), vec![key]),
        };
        drawer
            .update_at_heads(
                DocPatch {
                    id: inventory_doc_id.clone(),
                    user_path: None,
                    facets_set,
                    facets_remove,
                },
                BranchPath::new(MAIN_BRANCH),
                None,
            )
            .await?;
        Ok(())
    }

    /// A released representation is not dead: the facet that named it is gone,
    /// its inventory pin is gone, and its store roots are gone - but the
    /// plaintext is still local, so the next pass re-authorizes it and the
    /// document may declare the blob again. The re-install mints a fresh
    /// random key (the old pair was released; nothing may reuse released key
    /// material), so the new representation names a different digest, while
    /// the same document-facing cipherBlob facet id carries it and the
    /// released pair's roots stay released.
    #[tokio::test(flavor = "multi_thread")]
    async fn released_representation_is_reinstalled_with_a_fresh_key_by_the_next_pass() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Worker::new(test_ctx(&ctx, None).await?);
        let plaintext_bytes = b"blob-encryption worker: released then redeclared".to_vec();
        let plaintext = ctx.rt.blobs_repo.put(&plaintext_bytes).await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let branch = BranchPathBuf::from(MAIN_BRANCH);
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch)
            .await?
            .expect("document has a branch");

        worker.reconcile_document(&doc_id, &branch, &heads).await?;
        let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("first pass installs a representation");
        let c1 = digest_str_to_blob_id_lenient(&first.representation.digest)
            .expect("the facet digest is a blob digest");
        let c1_hash = blob_id_to_iroh_hash(c1);
        let p_hash = blob_id_to_iroh_hash(plaintext.clone());
        assert_eq!(
            worker
                .store
                .tags()
                .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
                .await?
                .expect("a registered pair roots its ciphertext")
                .hash,
            c1_hash,
        );
        // The pin worker saw the facet and pinned it (the write shape above).
        write_inventory_pin(
            &ctx.drawer_repo,
            &worker.encryption_inventory_doc_id,
            &first.representation.digest,
            Some(BlobPin {
                length_octets: first.representation.length_octets,
            }),
        )
        .await?;

        // The release, production-shaped end to end: the facet is re-authored
        // away (system-managed, so the writer's scope applies), the inventory
        // pin leaves, and the pin worker's release leaf drops the pair roots.
        ctx.drawer_repo
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    user_path: None,
                    facets_set: std::collections::HashMap::new(),
                    facets_remove: vec![cipher_key.clone()],
                },
                &branch,
                None,
                FacetWriteScope::System,
            )
            .await?;
        write_inventory_pin(
            &ctx.drawer_repo,
            &worker.encryption_inventory_doc_id,
            &first.representation.digest,
            None,
        )
        .await?;
        crate::blobs::encrypt::drop_pair_tags(&worker.store, c1_hash).await?;
        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_none(),
            "the released facet is gone"
        );
        assert!(
            worker
                .store
                .tags()
                .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
                .await?
                .is_none(),
            "the released pair is un-rooted"
        );

        // The same pass mechanics cover the re-install: the Blob facet is
        // still on the document, so the pass finds a declared blob with no
        // representation and creates one; re-declaring is not a worker input.
        let heads = ctx
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch)
            .await?
            .expect("document has a branch");
        worker.reconcile_document(&doc_id, &branch, &heads).await?;

        let second = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("the pass re-installs the declared blob's representation");
        assert_ne!(
            second.representation.digest, first.representation.digest,
            "a re-install after a release mints fresh key material; the old key is released"
        );
        let c2 = digest_str_to_blob_id_lenient(&second.representation.digest)
            .expect("the facet digest is a blob digest");
        let c2_hash = blob_id_to_iroh_hash(c2);
        let ct_tag = worker
            .store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c2_hash}"))
            .await?
            .expect("the re-installed pair is rooted");
        assert_eq!(ct_tag.hash, c2_hash);
        let pt_tag = worker
            .store
            .tags()
            .get(format!("{TAG_PT_PREFIX}{c2_hash}"))
            .await?
            .expect("the re-installed pair roots its plaintext");
        assert_eq!(pt_tag.hash, p_hash);
        assert!(
            worker
                .store
                .tags()
                .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
                .await?
                .is_none(),
            "the released pair's roots stay released across the re-install"
        );
        // A reader resolving through the document decrypts the new
        // representation back to the plaintext.
        let keys = DocKeySource::new(
            Arc::clone(&ctx.drawer_repo),
            doc_id.clone(),
            BranchPathBuf::from(MAIN_BRANCH),
        );
        let decrypted = crate::blobs::encrypt::get_decrypted(&worker.store, &keys, c2_hash).await?;
        assert_eq!(decrypted, plaintext_bytes);
        let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is still there");
        assert!(
            resolves_through(&blob, &cipher_key),
            "the re-installed representation is what the document resolves through, got {:?}",
            blob.urls
        );
        ctx.stop().await?;
        Ok(())
    }

    /// A stale inventory pin cannot authorize a re-install: the pin worker's
    /// row may outlive its facet by one diff, but the pass re-produces a
    /// representation only for a declared blob whose plaintext this node
    /// stores (§14). Released, with the plaintext gone, the pass must leave
    /// every plane exactly as it found it - no facet, no re-rooted pair, and
    /// no pin bookkeeping on its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn released_inventory_pin_with_absent_plaintext_is_not_resurrected_by_the_pass() -> Res<()>
    {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Worker::new(test_ctx(&ctx, None).await?);
        let plaintext = ctx
            .rt
            .blobs_repo
            .put(b"blob-encryption worker: released without a local plaintext")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext.clone()).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let branch = BranchPathBuf::from(MAIN_BRANCH);

        worker
            .reconcile_document(&doc_id, &branch, &worker_heads(&ctx, &doc_id).await?)
            .await?;
        let first = read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
            .await?
            .expect("first pass installs a representation");
        let c1 = digest_str_to_blob_id_lenient(&first.representation.digest)
            .expect("the facet digest is a blob digest");
        let c1_hash = blob_id_to_iroh_hash(c1.clone());
        let urls_before = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
            .await?
            .expect("the Blob facet is there")
            .urls;
        // The released state, production-shaped: facet re-authored away, pair
        // roots dropped, and the stale pin that outlives the diff still in the
        // inventory - the window the release contract must not leak through.
        write_inventory_pin(
            &ctx.drawer_repo,
            &worker.encryption_inventory_doc_id,
            &first.representation.digest,
            Some(BlobPin {
                length_octets: first.representation.length_octets,
            }),
        )
        .await?;
        ctx.drawer_repo
            .update_at_heads_with_scope(
                DocPatch {
                    id: doc_id.clone(),
                    user_path: None,
                    facets_set: std::collections::HashMap::new(),
                    facets_remove: vec![cipher_key.clone()],
                },
                &branch,
                None,
                FacetWriteScope::System,
            )
            .await?;
        write_inventory_pin(
            &ctx.drawer_repo,
            &worker.encryption_inventory_doc_id,
            &first.representation.digest,
            None,
        )
        .await?;
        crate::blobs::encrypt::drop_pair_tags(&worker.store, c1_hash).await?;
        // Now the plaintext is gone too: a fresh store with nothing in it is
        // the state every released blob ends in once GC has run.
        let fresh = tempfile::tempdir()?;
        let fresh_repo = crate::blobs::BlobsRepo::new(
            fresh.path().join("blobs"),
            daybook_types::doc::UserPathBuf::from("/test-user"),
        )
        .await?;
        let bare = Worker::new(Arc::new(Ctx {
            drawer_repo: Arc::clone(&ctx.drawer_repo),
            sql: ctx.rt.rcx.sql.clone(),
            store: fresh_repo.iroh_store(),
            provider: fresh_repo.cipher_provider(),
            domain_id: worker.domain_id.clone(),
            domain_group: worker.domain_group.clone(),
            encryption_inventory_doc_id: worker.encryption_inventory_doc_id.clone(),
        }));

        bare.reconcile_document(&doc_id, &branch, &worker_heads(&ctx, &doc_id).await?)
            .await?;

        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_none(),
            "a stale pin row must not produce a representation"
        );
        assert!(
            bare.blob_status(c1_hash).await?.is_none(),
            "the pass must not have written the released ciphertext into the bare store"
        );
        assert!(
            bare.store
                .tags()
                .get(format!("{TAG_CT_PREFIX}{c1_hash}"))
                .await?
                .is_none()
                && bare
                    .store
                    .tags()
                    .get(format!("{TAG_PT_PREFIX}{c1_hash}"))
                    .await?
                    .is_none(),
            "the pass must not have re-rooted the released pair in the bare store"
        );
        let pin_key = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: first.representation.digest.clone(),
        };
        assert!(
            read_facet(
                &ctx.drawer_repo,
                &worker.encryption_inventory_doc_id,
                &pin_key
            )
            .await?
            .is_none(),
            "the pass owns no pin writes; the stale pin was released above"
        );
        assert_eq!(
            read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
                .await?
                .expect("the Blob facet is still there")
                .urls,
            urls_before,
            "a skipped document is written to nowhere"
        );
        fresh_repo.shutdown().await?;
        ctx.stop().await?;
        Ok(())
    }

    async fn worker_heads(ctx: &DaybookTestContext, doc_id: &DocId) -> Res<ChangeHashSet> {
        ctx.drawer_repo
            .get_branch_heads_for_path(doc_id, &BranchPathBuf::from(MAIN_BRANCH))
            .await?
            .ok_or_else(|| eyre::eyre!("document {doc_id} has no {MAIN_BRANCH} branch"))
    }

    /// The pass covers documents that were already eligible before the worker
    /// existed - the case a delta-only worker cannot see, because eligibility is
    /// a state rather than an event.
    #[tokio::test(flavor = "multi_thread")]
    async fn pass_reconciles_documents_that_predate_the_worker() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Worker::new(test_ctx(&ctx, None).await?);
        let plaintext = ctx
            .rt
            .blobs_repo
            .put(b"blob-encryption worker: backfill")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);

        // The index is populated asynchronously; the pass runs once it can see
        // the document, which is exactly what a booting repo does.
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(membership) = facet_index
                .list_docs_for_tag(WellKnownFacetTag::Blob.as_str())
                .await?
                .into_iter()
                .find(|membership: &DocFacetTagMembership| membership.doc_id == doc_id)
            {
                assert_eq!(membership.facet_tag, WellKnownFacetTag::Blob.as_str());
                break;
            }
            eyre::ensure!(
                std::time::Instant::now() < deadline,
                "facet index never listed {doc_id} as a Blob-facet document"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        worker
            .reconcile_eligible_documents(&facet_index, CancellationToken::new())
            .await?;

        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_some(),
            "the pass must produce a representation for an already-eligible document"
        );
        ctx.stop().await?;
        Ok(())
    }

    /// A boot-pass document whose work fails is not waved through and not
    /// dropped: it is rescheduled by the pass's own scheduler with backoff, and
    /// the representation is applied once the cause clears - without a second
    /// boot pass. No write to the document happens in between, so the
    /// rescheduled task is the only thing that can produce it.
    #[tokio::test(flavor = "multi_thread")]
    async fn boot_pass_failure_is_rescheduled_and_applied_without_a_second_boot() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker = Arc::new(Worker::new(test_ctx(&ctx, None).await?));
        let plaintext = ctx
            .rt
            .blobs_repo
            .put(b"boot pass: failure then retry")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, plaintext).await?;
        let cipher_key = worker.cipher_facet_key(&blob_key);
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        await_indexed(&ctx, &doc_id).await?;

        // Armed before the pass starts: the document's boot task is the one
        // that fails, and every attempt at it fails while the latch holds.
        faults::FAIL_RECONCILE.store(true, std::sync::atomic::Ordering::SeqCst);
        let cancel_token = CancellationToken::new();
        let pass = tokio::spawn({
            let worker = Arc::clone(&worker);
            let facet_index = Arc::clone(&facet_index);
            let cancel_token = cancel_token.clone();
            async move {
                worker
                    .reconcile_eligible_documents(&facet_index, cancel_token)
                    .await
            }
        });

        // More than one attempt is the reschedule itself: the retry delay is
        // 2s, so reaching a second attempt can only happen on the retry path.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while faults::attempts() < 2 {
            eyre::ensure!(
                std::time::Instant::now() < deadline,
                "the pass never rescheduled the failed boot task, saw {} attempt(s)",
                faults::attempts()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_none(),
            "nothing durable may exist for a boot task whose work failed"
        );

        // Clearing the cause is what lets the retry succeed, and the pass
        // finishes on its own: nothing here re-runs it.
        faults::FAIL_RECONCILE.store(false, std::sync::atomic::Ordering::SeqCst);
        pass.await??;
        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_some(),
            "the rescheduled boot task must apply without a second boot pass"
        );
        ctx.stop().await?;
        Ok(())
    }

    /// Without an inventory the worker refuses to start rather than installing a
    /// representation nothing can advertise or release.
    #[tokio::test(flavor = "multi_thread")]
    async fn refuses_to_run_without_an_encryption_inventory() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let authority =
            crate::authority::ensure(&ctx.rt.rcx.big_repo, &ctx.rt.rcx.sql, None).await?;
        let error = match spawn_blob_encryption_worker(BlobEncryptionWorkerArgs {
            drawer_repo: Arc::clone(&ctx.drawer_repo),
            sql: ctx.rt.rcx.sql.clone(),
            blobs_repo: Arc::clone(&ctx.rt.blobs_repo),
            facet_set_store: ctx.rt.doc_facet_set_index_repo.revision_store(),
            facet_index: Arc::clone(&ctx.rt.doc_facet_set_index_repo),
            domain_group: authority.encrypted_blob_docs.clone(),
            encryption_inventory_doc_id: None,
            parent_cancel_token: tokio_util::sync::CancellationToken::new(),
        })
        .await
        {
            Ok(_) => eyre::bail!("the worker must refuse a repo with no encryption inventory"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("encrypted-representation inventory"),
            "the refusal must name what is missing, got: {error}"
        );
        ctx.stop().await?;
        Ok(())
    }

    /// The pure derivations, with no repo in the picture: the blob a `Blob`
    /// facet names is what decides whether anything gets mirrored at all.
    #[test]
    fn plaintext_blob_id_reads_both_digest_spellings() {
        let blob_id = BlobId::new([3u8; 32]);
        let base = Blob {
            mime: "application/octet-stream".to_string(),
            length_octets: 1,
            digest: blob_id_to_digest_str(blob_id.clone()),
            inline: None,
            urls: None,
        };
        assert_eq!(
            plaintext_blob_id(&base),
            Some(blob_id.clone()),
            "ADR 003 §3's canonical multihash spelling"
        );
        let url_only = Blob {
            digest: "not-a-digest".to_string(),
            urls: Some(vec![format!("db+blob:///{blob_id}")]),
            ..base.clone()
        };
        assert_eq!(
            plaintext_blob_id(&url_only),
            Some(blob_id),
            "a db+blob URL carries the bare spelling"
        );
        let neither = Blob {
            digest: "not-a-digest".to_string(),
            urls: Some(vec!["https://example.com/not-a-blob".to_string()]),
            ..base
        };
        assert_eq!(plaintext_blob_id(&neither), None);
    }

    /// Wait until the facet index lists `doc_id` as a Blob-facet document: the
    /// pass reads exactly this list, so listing it settles the pass's input.
    async fn await_indexed(ctx: &DaybookTestContext, doc_id: &DocId) -> Res<()> {
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if facet_index
                .list_docs_for_tag(WellKnownFacetTag::Blob.as_str())
                .await?
                .into_iter()
                .any(|membership: DocFacetTagMembership| &membership.doc_id == doc_id)
            {
                return Ok(());
            }
            eyre::ensure!(
                std::time::Instant::now() < deadline,
                "facet index never listed {doc_id} as a Blob-facet document"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Wait for a representation at `cipher_key`, naming `what` when it does not
    /// arrive.
    async fn await_representation(
        ctx: &DaybookTestContext,
        doc_id: &DocId,
        cipher_key: &FacetKey,
        what: &str,
    ) -> Res<CipherBlob> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(cipher) = read_cipherblob(&ctx.drawer_repo, doc_id, cipher_key).await? {
                return Ok(cipher);
            }
            eyre::ensure!(
                std::time::Instant::now() < deadline,
                "{what} never wrote a representation for {doc_id} at {cipher_key}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Start the worker and wait until its pass is done, proven by the
    /// representation it writes for a document staged before it starts. Every
    /// document staged after that point is the delta machine's.
    async fn spawn_worker_after_pass_barrier(
        ctx: &DaybookTestContext,
        worker_ctx: &Arc<Ctx>,
    ) -> Res<(tokio::task::JoinHandle<Res<()>>, CancellationToken)> {
        let before = ctx
            .rt
            .blobs_repo
            .put(b"delta machine: before the pass")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(ctx, before).await?;
        await_indexed(ctx, &doc_id).await?;
        let mut worker = Worker::new(Arc::clone(worker_ctx));
        let cancel_token = CancellationToken::new();
        let facet_index = Arc::clone(&ctx.rt.doc_facet_set_index_repo);
        let handle = tokio::spawn({
            let cancel_token = cancel_token.clone();
            let revision_store = facet_index.revision_store();
            async move { worker.run(revision_store, facet_index, cancel_token).await }
        });
        await_representation(
            ctx,
            &doc_id,
            &worker_ctx.cipher_facet_key(&blob_key),
            "the pass",
        )
        .await?;
        Ok((handle, cancel_token))
    }

    /// A document written *after* the pass is encrypted by the delta machine.
    ///
    /// `run` reconciles the facet index once and only then opens the walker, so
    /// the pass cannot see a document that did not exist when it walked the
    /// index. This representation can therefore only come from the
    /// `ConcurrentDeltaWalker` over the facet-set revisions.
    #[tokio::test(flavor = "multi_thread")]
    async fn document_written_after_the_pass_is_encrypted_by_the_delta_machine() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker_ctx = test_ctx(&ctx, None).await?;
        let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

        let after = ctx
            .rt
            .blobs_repo
            .put(b"delta machine: after the pass")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, after).await?;
        let cipher_key = worker_ctx.cipher_facet_key(&blob_key);
        let cipher =
            await_representation(&ctx, &doc_id, &cipher_key, "the facet delta machine").await?;

        // §19: the facet names a ciphertext this node holds (step 1's
        // installation), and the Blob facet resolves through it (step 5).
        let c = digest_str_to_blob_id_lenient(&cipher.representation.digest)
            .ok_or_else(|| eyre::eyre!("representation digest is not a blob id"))?;
        assert!(
            worker_ctx
                .blob_status(crate::blobs::blob_id_to_iroh_hash(c))
                .await?
                .is_some(),
            "the representation the machine wrote must be servable"
        );
        // Step 5 is a write of its own, after the facet: wait for the document
        // to resolve through the representation rather than racing that write.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let blob = read_blob(&ctx.drawer_repo, &doc_id, &blob_key)
                .await?
                .expect("the Blob facet is still there");
            if resolves_through(&blob, &cipher_key) {
                break;
            }
            eyre::ensure!(
                std::time::Instant::now() < deadline,
                "the Blob facet never resolved through the representation"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        cancel_token.cancel();
        handle.await??;
        ctx.stop().await?;
        Ok(())
    }

    /// A delta with nothing to represent produces nothing and does not stall the
    /// machine - the documents behind it are still processed.
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_with_nothing_to_represent_does_not_stall_the_machine() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker_ctx = test_ctx(&ctx, None).await?;
        let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

        // A Blob facet that names no plaintext: a peer-authored revision this
        // node cannot act on. Nothing gets stored and no pin is derivable, so
        // this is the delta the machine must skip *without* losing its place.
        let nameless_key = FacetKey {
            tag: WellKnownFacetTag::Blob.into(),
            id: "nameless".to_string(),
        };
        let nameless_doc = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from(MAIN_BRANCH),
                facets: [(
                    nameless_key.clone(),
                    FacetRaw::from(WellKnownFacet::Blob(Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 4096,
                        digest: "not-a-digest".to_string(),
                        inline: None,
                        urls: None,
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // The document behind it: if the machine stalled on the skipped delta,
        // this representation never appears.
        let stored = ctx
            .rt
            .blobs_repo
            .put(b"delta machine: after the skip")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, stored).await?;
        await_representation(
            &ctx,
            &doc_id,
            &worker_ctx.cipher_facet_key(&blob_key),
            "the machine, after a delta with nothing to represent",
        )
        .await?;
        assert!(
            read_cipherblob(
                &ctx.drawer_repo,
                &nameless_doc,
                &worker_ctx.cipher_facet_key(&nameless_key),
            )
            .await?
            .is_none(),
            "a Blob facet that names no plaintext must not get a representation"
        );

        cancel_token.cancel();
        handle.await??;
        ctx.stop().await?;
        Ok(())
    }

    /// A document delta whose work fails is not waved through: the walker cursor
    /// must not advance past it, and the document must not be lost - it is
    /// rescheduled, and the representation appears once the cause clears. No
    /// write to the document happens in between, so the rescheduled task is the
    /// only thing that can produce it.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_document_delta_is_rescheduled_until_its_work_is_durable() -> Res<()> {
        let ctx = test_cx(utils_rs::function_full!()).await?;
        let worker_ctx = test_ctx(&ctx, None).await?;
        let (handle, cancel_token) = spawn_worker_after_pass_barrier(&ctx, &worker_ctx).await?;

        // Armed before the document exists: the delta this document produces is
        // the one that fails. Nothing below writes to the document again, so a
        // representation can only come from the rescheduled task.
        faults::FAIL_RECONCILE.store(true, std::sync::atomic::Ordering::SeqCst);
        let stored = ctx
            .rt
            .blobs_repo
            .put(b"delta machine: failure then retry")
            .await?;
        let (doc_id, blob_key) = stage_document_with_blob(&ctx, stored).await?;
        let cipher_key = worker_ctx.cipher_facet_key(&blob_key);

        // While the work keeps failing nothing durable may exist, and the delta
        // must come back rather than being waved through. The retry delay is 2s,
        // so 3.5s has to contain more than one attempt.
        tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
        assert!(
            read_cipherblob(&ctx.drawer_repo, &doc_id, &cipher_key)
                .await?
                .is_none(),
            "nothing durable may exist for a document whose work failed"
        );
        assert!(
            faults::attempts() >= 2,
            "a failed delta must be rescheduled, saw {} attempt(s)",
            faults::attempts()
        );

        // Clearing the cause is what lets the retry succeed.
        faults::FAIL_RECONCILE.store(false, std::sync::atomic::Ordering::SeqCst);
        await_representation(&ctx, &doc_id, &cipher_key, "the rescheduled delta").await?;

        cancel_token.cancel();
        handle.await??;
        ctx.stop().await?;
        Ok(())
    }
}
