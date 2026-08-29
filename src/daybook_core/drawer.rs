// FIXME: use nested ids for facet keys
// FIXME: use ensure_alive method for cancellation checks
// FIXME: is validating facet references on entry useful? Would that not race?

use crate::app::SqlCtx;
use crate::interlude::*;
use crate::plugs::PlugsRepo;

mod cache;
pub mod dmeta;
mod events;
mod facet_recovery;
mod meta;
mod mutations;
mod queries;
#[cfg(test)]
mod tests;
pub mod types;

pub use crate::drawer::types::{DocBundle, DocEntry, DocEntryDiff, DocNBranches, DrawerEvent};
pub use meta::doc_version_updates;
pub use meta::version_updates;

use big_repo::{
    BigKeyhiveGroup, BigRepoLocalFilter, BigRepoLocalListenerRegistration,
    BigRepoLocalNotification, SharedBigRepo, SharedPartStore,
};
use cache::FacetCacheKey;
use cache::*;
use types::{BranchSnapshot, DocDeleteTombstone};
use utils_rs::lru::SharedKeyedLruPool;

use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, DocId, FacetKey, FacetRaw, FacetRef};
use daybook_types::url::{FACET_SELF_DOC_ID, parse_facet_ref};

use tokio_util::sync::CancellationToken;

/// Recover the exact facet heads and author recorded by dmeta at one branch
/// head set. This keeps index projections on the same write-point semantics
/// as the existing drawer history APIs without requiring a branch path.
pub(crate) fn facet_snapshot_metadata(
    doc: &automerge::Automerge,
    facet_key: &FacetKey,
    heads: &[automerge::ChangeHash],
) -> Res<(ChangeHashSet, ActorId)> {
    let facet_heads = facet_recovery::recover_facet_heads_at(doc, facet_key, heads)?;
    let actor_id = facet_recovery::facet_write_points(doc, facet_key, &[], heads)?
        .into_iter()
        .last()
        .map(|(_, actor_id)| actor_id)
        .ok_or_else(|| ferr!("active facet has no dmeta write point"))?;
    Ok((ChangeHashSet(Arc::from(facet_heads)), actor_id))
}

/// Exact-head facet hydration owned by the drawer, which is the sole owner
/// of BigRepo document handles at this layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExactFacetHydration {
    Deferred,
    Absent,
    Present {
        branch_heads: ChangeHashSet,
        facet_heads: ChangeHashSet,
        actor_id: ActorId,
    },
}

/// Exact-head user-facet value hydration owned by the drawer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExactFacetValueHydration {
    Deferred,
    Absent,
    Present(FacetRaw),
}

/// The complete current user-facet membership and dmeta provenance at exact
/// branch heads. Values are intentionally not hydrated here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactDmetaState {
    pub document_id: DocId,
    pub branch_id: daybook_types::doc::BranchId,
    pub branch_heads: ChangeHashSet,
    pub facets: HashMap<FacetKey, (ChangeHashSet, ActorId)>,
    pub all_facet_keys: Vec<FacetKey>,
}

/// Drawer-owned wakeup for projections waiting on local materialization.
/// Payloads are intentionally hidden: consumers must retry from their durable
/// source cursor rather than treating a notification as projection data.
pub(crate) struct MaterializationWake {
    _registration: BigRepoLocalListenerRegistration,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationChange {
    pub branch_id: daybook_types::doc::BranchId,
    pub heads: Option<ChangeHashSet>,
}

impl MaterializationWake {
    pub(crate) async fn changed(&mut self) -> Res<MaterializationChange> {
        loop {
            let Some(batch) = self.receiver.recv().await else {
                return Err(ferr!("Drawer materialization listener closed"));
            };
            let Some(notification) = batch.into_iter().next() else {
                continue;
            };
            let (doc_id, heads) = match notification {
                BigRepoLocalNotification::DocCreated { doc_id, heads }
                | BigRepoLocalNotification::DocImported { doc_id, heads }
                | BigRepoLocalNotification::DocHeadsUpdated { doc_id, heads }
                | BigRepoLocalNotification::DocMaterializationReady { doc_id, heads } => {
                    (doc_id, Some(heads))
                }
                BigRepoLocalNotification::DocMaterializationPending { doc_id } => (doc_id, None),
            };
            return Ok(MaterializationChange {
                branch_id: daybook_types::doc::BranchId(doc_id.to_string()),
                heads: heads.map(ChangeHashSet),
            });
        }
    }

    pub(crate) async fn wait(&mut self) -> Res<()> {
        self.changed().await.map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchKind {
    Replicated,
    Local,
}

/// Identifies whether a facet mutation comes from an ordinary caller or from
/// repository-owned system-facet machinery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FacetWriteScope {
    User,
    System,
}

pub struct DrawerRepo {
    pub big_repo: SharedBigRepo,
    partition_store: SharedPartStore,
    drawer_doc_id: DocumentId,
    content_docs_group: BigKeyhiveGroup,
    drawer_group: BigKeyhiveGroup,
    pending_documents_group: BigKeyhiveGroup,
    local_actor_id: ActorId,
    local_peer_id: PeerId,
    local_user_path: daybook_types::doc::UserPathBuf,

    // LRU Caches
    entry_cache: surelock::mutex::Mutex<HashMap<DocId, DocEntry>>,
    facet_cache: surelock::mutex::Mutex<FacetCacheState>,
    facet_schema_validators:
        surelock::mutex::Mutex<HashMap<(String, String), Arc<jsonschema::Validator>>>,
    branch_handles: surelock::mutex::Mutex<HashMap<DocumentId, big_repo::BigDocHandle>>,

    // LRU Pools (Policy only)
    entry_pool: SharedKeyedLruPool<DocId>,
    doc_pool: SharedKeyedLruPool<FacetCacheKey>,

    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<big_repo::BigRepoChangeListenerRegistration>,
    current_heads: surelock::mutex::Mutex<ChangeHashSet>,
    drawer_doc_handle: big_repo::BigDocHandle,
    meta_store_sql: SqlCtx,
    plugs_repo: Option<Arc<crate::plugs::PlugsRepo>>,
}

struct ValidatedReference {
    doc_id: DocId,
    facet_key: FacetKey,
    url_value: String,
    heads: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct BranchRefRow {
    pub(crate) branch_doc_id: DocumentId,
    branch_kind: BranchKind,
}

#[cfg(test)]
#[expect(dead_code)]
#[derive(Debug, Clone)]
struct BranchStateRow {
    branch_path: String,
    branch_doc_id: DocumentId,
    latest_heads: ChangeHashSet,
    branch_kind: BranchKind,
}

impl DrawerRepo {
    pub fn drawer_doc_id(&self) -> &DocumentId {
        &self.drawer_doc_id
    }

    pub fn meta_store_sql(&self) -> &SqlCtx {
        &self.meta_store_sql
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn load(
        big_repo: SharedBigRepo,
        partition_store: SharedPartStore,
        drawer_doc_id: DocumentId,
        local_user_path: daybook_types::doc::UserPathBuf,
        meta_db_pool: SqlCtx,
        _local_state_root: PathBuf,
        entry_pool: SharedKeyedLruPool<DocId>,
        doc_pool: SharedKeyedLruPool<FacetCacheKey>,
        #[cfg(not(test))] plugs_repo: Arc<PlugsRepo>,
        #[cfg(test)] plugs_repo: Option<Arc<PlugsRepo>>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let authority = crate::authority::ensure(&big_repo, &meta_db_pool, None).await?;
        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "drawer-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let drawer_am_handle = big_repo
            .get_doc(&drawer_doc_id)
            .await?
            .into_ready(drawer_doc_id)?;

        let initial_heads = drawer_am_handle
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await;

        // Listen for changes to docs.map
        let (ticket, notif_rx) = big_repo
            .subscribe_change_listener(big_repo::BigRepoChangeFilter {
                doc_id: Some(big_repo::BigRepoDocIdFilter::new(drawer_doc_id)),
                path: vec!["docs".into(), "map".into()],
                origin: None,
            })
            .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            local_peer_id: big_repo.local_peer_id(),
            big_repo,
            partition_store,
            drawer_doc_id,
            content_docs_group: authority.content_docs.clone(),
            drawer_group: authority.default_drawer.clone(),
            pending_documents_group: authority.pending_documents_group(),
            local_actor_id,
            local_user_path,
            entry_cache: surelock::mutex::Mutex::new(HashMap::new()),
            facet_cache: surelock::mutex::Mutex::new(FacetCacheState::new()),
            facet_schema_validators: surelock::mutex::Mutex::new(HashMap::new()),
            branch_handles: surelock::mutex::Mutex::new(HashMap::new()),
            entry_pool,
            doc_pool,
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
            current_heads: surelock::mutex::Mutex::new(initial_heads),
            drawer_doc_handle: drawer_am_handle,
            meta_store_sql: meta_db_pool,
            #[cfg(not(test))]
            plugs_repo: Some(Arc::clone(&plugs_repo)),
            #[cfg(test)]
            plugs_repo: plugs_repo.clone(),
        });
        // The local branch schema must exist before the plugs repo's
        // attach_drawer (which registers the config doc and warms the derived
        // cache) runs; register/get call paths read drawer_local_branches.
        repo.ensure_local_branch_schema().await?;
        // ADR 007 §2: the plugs repo is loaded before the drawer (the drawer
        // needs it for facet validation); attach the drawer back so the plugs
        // repo can read manifest docs and write the plugg config facet through
        // it.
        if let Some(plugs_repo) = &repo.plugs_repo {
            plugs_repo.attach_drawer(Arc::clone(&repo)).await?;
        }
        repo.migrate_content_doc_authority().await?;
        repo.ensure_replicated_branch_partitions().await?;
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.notifs_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn migrate_content_doc_authority(&self) -> Res<()> {
        const MIGRATION_KEY: &str = "global.authority.content_docs_and_drawer_migrated";
        if crate::repo::globals::get_string_global(&self.meta_store_sql, MIGRATION_KEY)
            .await?
            .is_some()
        {
            return Ok(());
        }
        for item in self.list().await? {
            let Some(entry) = self.get_entry(&item.doc_id).await? else {
                continue;
            };
            for branch in entry.branches.values() {
                self.big_repo
                    .add_admin_member_to_doc(branch.branch_doc_id, self.content_docs_group.clone())
                    .await?;
                self.big_repo
                    .add_admin_member_to_doc(branch.branch_doc_id, self.drawer_group.clone())
                    .await?;
            }
        }
        crate::repo::globals::upsert_string_global(&self.meta_store_sql, MIGRATION_KEY, "1")
            .await?;
        Ok(())
    }
    fn branch_kind_for_path(
        &self,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<BranchKind> {
        if branch_path == daybook_types::doc::BranchPath::new("main") {
            return Ok(BranchKind::Replicated);
        }
        if branch_path == "/tmp" || branch_path.starts_with("/tmp/") {
            return Ok(BranchKind::Local);
        }
        if branch_path.is_absolute() {
            return Ok(BranchKind::Replicated);
        }
        eyre::bail!("invalid branch path '{}'", branch_path);
    }

    pub(crate) fn replicated_partition_id(&self) -> PartId {
        big_repo::group_part_id(self.drawer_group.id().to_bytes())
    }

    async fn add_branch_to_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: DocumentId,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
            let part_id = self.replicated_partition_id();
            let heads = am_utils_rs::serialize_commit_heads(heads);
            self.partition_store
                .set_obj_payload(
                    branch_doc_id,
                    serde_json::json!({
                        "heads": heads
                    }),
                )
                .await?;
            self.partition_store
                .add_obj_to_parts(branch_doc_id, vec![part_id])
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn ensure_replicated_branch_partitions(&self) -> Res<()> {
        let (_, entries) = self.current_drawer_entries().await?;
        let part_id = self.replicated_partition_id();
        for (_doc_id, entry) in entries {
            for (branch_name, branch_ref) in &entry.branches {
                let branch_path = daybook_types::doc::BranchPath::new(branch_name.as_str());
                if self.branch_kind_for_path(branch_path)? == BranchKind::Replicated {
                    self.partition_store
                        .add_obj_to_parts(branch_ref.branch_doc_id, vec![part_id])
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn remove_branch_from_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: DocumentId,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
            let part_id = self.replicated_partition_id();
            let obj_id = big_sync_core::ObjId::new(*branch_doc_id.as_bytes());
            self.partition_store
                .remove_obj_from_part(obj_id, part_id)
                .await?;
            self.big_repo
                .revoke_doc_access(branch_doc_id, self.drawer_group.clone())
                .await?;
            self.big_repo
                .revoke_doc_access(branch_doc_id, self.content_docs_group.clone())
                .await?;
        }
        Ok(())
    }

    pub(crate) fn content_actor_id(
        &self,
        user_path: Option<&daybook_types::doc::UserPath>,
        branch_doc_id: DocumentId,
    ) -> ActorId {
        let base_user_path = user_path.unwrap_or_else(|| &self.local_user_path);
        let scoped_user_path = base_user_path
            .join("branches")
            .join(branch_doc_id.to_string());
        daybook_types::doc::user_path::to_actor_id(&scoped_user_path)
    }
    /// The content actor a write to the given doc/branch (user_path None)
    /// would use — the author of the store's own writes. None until the
    /// doc's branch is registered.
    pub(crate) async fn resolve_content_actor(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Option<ActorId> {
        let branch_ref = self.get_branch_ref(doc_id, branch_path).await.ok()??;
        Some(self.content_actor_id(None, branch_ref.branch_doc_id))
    }

    pub(crate) async fn get_branch_heads_by_doc_id(
        &self,
        branch_doc_id: DocumentId,
    ) -> Res<Option<ChangeHashSet>> {
        let Some(handle) = self.get_handle_by_branch_doc_id(branch_doc_id).await? else {
            return Ok(None);
        };
        let latest_heads = handle
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await;
        Ok(Some(latest_heads))
    }

    pub(crate) async fn get_branch_heads_for_path(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<ChangeHashSet>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            debug!(%doc_id, %branch_path, op = "get_branch_heads_for_path", "no branch ref");
            return Ok(None);
        };
        let Some(heads) = self
            .get_branch_heads_by_doc_id(branch_ref.branch_doc_id)
            .await?
        else {
            debug!(%doc_id, %branch_path, branch_doc_id = %branch_ref.branch_doc_id, op = "get_branch_heads_for_path", "branch doc heads unavailable");
            return Ok(None);
        };
        Ok(Some(heads))
    }

    pub(crate) async fn get_handle_by_branch_doc_id(
        &self,
        document_id: DocumentId,
    ) -> Res<Option<big_repo::BigDocHandle>> {
        if let Some(handle) = surelock::key::lock_scope(|key| {
            let (handles, _key) = key.lock(&self.branch_handles);
            handles.get(&document_id).cloned()
        }) {
            return Ok(Some(handle));
        }
        match self.big_repo.get_doc(&document_id).await? {
            big_repo::DocLookup::Ready(handle) => {
                surelock::key::lock_scope(|key| {
                    let (mut handles, _key) = key.lock(&self.branch_handles);
                    handles.insert(document_id, handle.clone());
                });
                Ok(Some(handle))
            }
            other => {
                // TEMP-INSTRUMENTATION: classify live resolution failures.
                let variant = match &other {
                    big_repo::DocLookup::Ready(_) => "Ready",
                    big_repo::DocLookup::PendingMaterialization => "PendingMaterialization",
                    big_repo::DocLookup::Missing => "Missing",
                };
                tracing::warn!(
                    %document_id,
                    lookup = variant,
                    op = "get_handle_by_branch_doc_id",
                    "branch doc not ready"
                );
                Ok(None)
            }
        }
    }

    async fn resolve_handle_for_branch_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<big_repo::BigDocHandle>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            debug!(%doc_id, %branch_path, op = "resolve_handle_for_branch_heads", "no branch ref");
            return Ok(None);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(branch_ref.branch_doc_id)
            .await?
        else {
            debug!(%doc_id, %branch_path, branch_doc_id = %branch_ref.branch_doc_id, op = "resolve_handle_for_branch_heads", "no handle");
            return Ok(None);
        };
        let (contains_all_heads, missing_heads) = handle
            .with_document_read(|doc| {
                let mut missing = Vec::new();
                for head in heads.iter() {
                    if doc.get_change_by_hash(head).is_none() {
                        missing.push(head.to_string());
                    }
                }
                Ok::<(bool, Vec<String>), eyre::Report>((missing.is_empty(), missing))
            })
            .await?;
        if !contains_all_heads {
            debug!(%doc_id, %branch_path, ?heads, ?missing_heads, "presence probe: resolve: heads missing from doc");
            return Ok(None);
        }
        debug!(%doc_id, %branch_path, "presence probe: resolve: handle ok");
        Ok(Some(handle))
    }

    async fn latest_doc_delete_tombstone(
        &self,
        doc_id: &DocId,
        heads: &Arc<[automerge::ChangeHash]>,
    ) -> Res<Option<DocDeleteTombstone>> {
        let Some(tags) = self
            .drawer_doc_handle
            .hydrate_path_at_heads::<Vec<DocDeleteTombstone>>(
                heads,
                automerge::ROOT,
                vec![
                    "docs".into(),
                    "map_deleted".into(),
                    autosurgeon::Prop::Key(doc_id.to_string().into()),
                ],
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(tags.last().cloned())
    }

    async fn facet_keys_at_branch_snapshot(
        &self,
        _doc_id: &DocId,
        snapshot: &BranchSnapshot,
    ) -> Res<Option<HashSet<FacetKey>>> {
        let branch_doc_id = snapshot.branch_doc_id;
        let Some(handle) = self.get_handle_by_branch_doc_id(branch_doc_id).await? else {
            return Ok(None);
        };
        let keys = handle
            .with_document_read(|am_doc| {
                let facets_obj = match automerge::ReadDoc::get_at(
                    am_doc,
                    automerge::ROOT,
                    "facets",
                    &snapshot.branch_heads,
                )? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                    _ => return Ok::<HashSet<FacetKey>, eyre::Report>(HashSet::new()),
                };
                let mut out = HashSet::new();
                for item in automerge::ReadDoc::map_range_at(
                    am_doc,
                    &facets_obj,
                    ..,
                    &snapshot.branch_heads,
                ) {
                    out.insert(FacetKey::from(item.key.to_string().as_str()));
                }
                Ok(out)
            })
            .await?;
        Ok(Some(keys))
    }

    async fn non_tmp_branch_snapshots_for_entry(
        &self,
        branches: HashMap<String, types::StoredBranchRef>,
    ) -> Res<HashMap<String, BranchSnapshot>> {
        let mut out = HashMap::new();
        for (branch_name, branch_ref) in branches {
            let branch_path = daybook_types::doc::BranchPath::new(&branch_name);
            if branch_path.starts_with(BranchPath::new("/tmp")) {
                continue;
            }
            let Some(branch_heads) = self
                .get_branch_heads_by_doc_id(branch_ref.branch_doc_id)
                .await?
            else {
                continue;
            };
            out.insert(
                branch_name.clone(),
                BranchSnapshot {
                    branch_doc_id: branch_ref.branch_doc_id,
                    branch_heads,
                },
            );
        }
        Ok(out)
    }

    async fn facet_manifest_for_tag(
        &self,
        facet_tag: &str,
        write_scope: FacetWriteScope,
    ) -> Res<Option<daybook_types::manifest::FacetManifest>> {
        if let Some(plugs_repo) = &self.plugs_repo {
            return match plugs_repo.get_facet_manifest_by_tag(facet_tag).await {
                crate::plugs::FacetManifestLookup::Found(facet_manifest) => {
                    Ok(Some(facet_manifest))
                }
                crate::plugs::FacetManifestLookup::PlugDisabled { plug_id } => {
                    eyre::bail!(
                        "facet tag '{}' is owned by disabled plug '{}'",
                        facet_tag,
                        plug_id
                    );
                }
                crate::plugs::FacetManifestLookup::UnknownTag
                    if write_scope == FacetWriteScope::System =>
                {
                    Ok(Self::system_facet_manifest(facet_tag))
                }
                crate::plugs::FacetManifestLookup::UnknownTag => Ok(None),
            };
        }
        if write_scope == FacetWriteScope::System || cfg!(test) {
            return Ok(Self::system_facet_manifest(facet_tag));
        }
        Ok(None)
    }

    fn system_facet_manifest(facet_tag: &str) -> Option<daybook_types::manifest::FacetManifest> {
        static SYSTEM_FACET_MANIFESTS: std::sync::OnceLock<
            HashMap<String, daybook_types::manifest::FacetManifest>,
        > = std::sync::OnceLock::new();
        SYSTEM_FACET_MANIFESTS
            .get_or_init(|| {
                crate::plugs::system_plugs()
                    .into_iter()
                    .flat_map(|plug| plug.facets)
                    .map(|manifest| (manifest.key_tag.to_string(), manifest))
                    .collect()
            })
            .get(facet_tag)
            .cloned()
    }

    pub(crate) async fn validate_facets(
        &self,
        incoming_facets: &HashMap<FacetKey, FacetRaw>,
        removed_facet_keys: &[FacetKey],
        resulting_facet_keys: &HashSet<FacetKey>,
        write_scope: FacetWriteScope,
    ) -> Res<()> {
        Self::validate_facet_write_scope(incoming_facets, removed_facet_keys, write_scope)?;

        for (facet_key, facet_value) in incoming_facets {
            let facet_tag = facet_key.tag.to_string();
            let facet_manifest = self.facet_manifest_for_tag(&facet_tag, write_scope).await?;
            let Some(facet_manifest) = facet_manifest else {
                eyre::bail!(
                    "facet tag '{}' has no registered manifest in plugs repo",
                    facet_tag
                );
            };

            let schema_json = serde_json::to_value(&facet_manifest.value_schema)?;
            let schema_cache_key = (facet_tag.clone(), serde_json::to_string(&schema_json)?);
            let validator = {
                let compiled = jsonschema::validator_for(&schema_json).map_err(|err| {
                    eyre::eyre!(
                        "failed to compile facet schema validator for facet_manifest tag '{}': {err}",
                        facet_manifest.key_tag
                    )
                })?;
                let compiled = Arc::new(compiled);
                surelock::key::lock_scope(|key| {
                    let (mut cache, _key) = key.lock(&self.facet_schema_validators);
                    if let Some(existing) = cache.get(&schema_cache_key) {
                        Arc::clone(existing)
                    } else {
                        cache.insert(schema_cache_key, Arc::clone(&compiled));
                        compiled
                    }
                })
            };
            if let Err(validation_error) = validator.validate(facet_value) {
                eyre::bail!(
                    "facet '{}' failed schema validation: {}",
                    facet_key,
                    validation_error
                );
            }

            for reference_manifest in &facet_manifest.references {
                self.validate_facet_reference(
                    resulting_facet_keys,
                    facet_key,
                    facet_value,
                    reference_manifest,
                )?;
            }
        }
        Ok(())
    }

    fn validate_facet_write_scope(
        incoming_facets: &HashMap<FacetKey, FacetRaw>,
        removed_facet_keys: &[FacetKey],
        write_scope: FacetWriteScope,
    ) -> Res<()> {
        if write_scope == FacetWriteScope::User
            && let Some(facet_key) =
                incoming_facets
                    .keys()
                    .chain(removed_facet_keys)
                    .find(|facet_key| {
                        matches!(
                            &facet_key.tag,
                            daybook_types::doc::FacetTag::WellKnown(tag)
                                if tag.is_system_managed()
                        )
                    })
        {
            eyre::bail!("ordinary facet writes cannot modify system-managed facet '{facet_key}'");
        }
        Ok(())
    }

    fn validate_facet_reference(
        &self,
        resulting_facet_keys: &HashSet<FacetKey>,
        origin_facet_key: &FacetKey,
        origin_facet_value: &FacetRaw,
        reference_manifest: &daybook_types::manifest::FacetReferenceManifest,
    ) -> Res<()> {
        let selected_values = daybook_types::reference::select_json_path_values(
            origin_facet_value,
            reference_manifest.json_path(),
        )?;
        if selected_values.is_empty() {
            eyre::bail!(
                "facet '{}' reference path '{}' is missing",
                origin_facet_key,
                reference_manifest.json_path()
            );
        }

        let mut referenced_facets = Vec::new();
        match reference_manifest {
            daybook_types::manifest::FacetReferenceManifest::UrlString { .. }
            | daybook_types::manifest::FacetReferenceManifest::UrlStringSplit { .. }
            | daybook_types::manifest::FacetReferenceManifest::UrlStringMany { .. } => {
                for selected_value in selected_values {
                    match selected_value {
                        serde_json::Value::String(url_value) => {
                            referenced_facets
                                .push(Self::validate_reference_url(url_value, origin_facet_key)?);
                        }
                        serde_json::Value::Array(url_values) => {
                            for url_value in url_values {
                                let serde_json::Value::String(url_string) = url_value else {
                                    eyre::bail!(
                                        "facet '{}' reference path '{}' must contain URL strings",
                                        origin_facet_key,
                                        reference_manifest.json_path()
                                    );
                                };
                                referenced_facets.push(Self::validate_reference_url(
                                    url_string,
                                    origin_facet_key,
                                )?);
                            }
                        }
                        _ => {
                            eyre::bail!(
                                "facet '{}' reference path '{}' must contain URL strings",
                                origin_facet_key,
                                reference_manifest.json_path()
                            );
                        }
                    }
                }
            }
            daybook_types::manifest::FacetReferenceManifest::UrlObject { .. }
            | daybook_types::manifest::FacetReferenceManifest::UrlObjectMany { .. } => {
                for selected_value in selected_values {
                    let facet_ref: FacetRef = serde_json::from_value(selected_value.clone())
                        .wrap_err_with(|| {
                            format!(
                                "facet '{}' reference path '{}' must contain reference objects",
                                origin_facet_key,
                                reference_manifest.json_path()
                            )
                        })?;
                    referenced_facets.push(Self::validate_reference_object(
                        &facet_ref,
                        origin_facet_key,
                    )?);
                }
            }
        }

        if let Some(at_commit_json_path) = reference_manifest.at_commit_json_path() {
            let at_commit_values = daybook_types::reference::select_json_path_values(
                origin_facet_value,
                at_commit_json_path,
            )?;
            if at_commit_values.is_empty() {
                eyre::bail!(
                    "facet '{}' at_commit path '{}' is missing",
                    origin_facet_key,
                    at_commit_json_path
                );
            }
            if at_commit_values.len() != 1 {
                eyre::bail!(
                    "facet '{}' at_commit path '{}' must resolve to a single value",
                    origin_facet_key,
                    at_commit_json_path
                );
            }

            let self_reference_mode = match at_commit_values[0] {
                serde_json::Value::Array(values) => {
                    if values.is_empty() {
                        true
                    } else {
                        let mut commit_head_strings = Vec::with_capacity(values.len());
                        for value in values {
                            let serde_json::Value::String(commit_head) = value else {
                                eyre::bail!(
                                    "facet '{origin_facet_key}' at_commit path '{at_commit_json_path}' must be an array of commit-hash strings",
                                );
                            };
                            commit_head_strings.push(commit_head.clone());
                        }
                        am_utils_rs::parse_commit_heads(&commit_head_strings).wrap_err_with(|| {
                                format!(
                                    "facet '{origin_facet_key}' at_commit path '{at_commit_json_path}' contains invalid commit hash values",
                                )
                            })?;
                        false
                    }
                }
                _ => {
                    eyre::bail!(
                        "facet '{origin_facet_key}' at_commit path '{at_commit_json_path}' must be an array of commit hashes",
                    );
                }
            };

            if self_reference_mode {
                for referenced_facet in referenced_facets {
                    if referenced_facet.doc_id == FACET_SELF_DOC_ID
                        && !resulting_facet_keys.contains(&referenced_facet.facet_key)
                    {
                        eyre::bail!(
                            "facet '{}' self-reference target '{}' must exist in validated facet set",
                            origin_facet_key,
                            referenced_facet.facet_key
                        );
                    }
                }
            }
        } else {
            for referenced_facet in referenced_facets {
                let commit_head_strings = if referenced_facet.heads.is_empty() {
                    if referenced_facet.doc_id == FACET_SELF_DOC_ID
                        && resulting_facet_keys.contains(&referenced_facet.facet_key)
                    {
                        // Empty heads means "self in this validated facet set".
                        continue;
                    }
                    let parsed_url = url::Url::parse(&referenced_facet.url_value)?;
                    let Some(fragment) = parsed_url.fragment() else {
                        eyre::bail!(
                            "facet '{}' reference '{}' must include commit heads in URL fragment when at_commit_json_path is not declared",
                            origin_facet_key,
                            referenced_facet.url_value
                        );
                    };
                    fragment
                        .split('|')
                        .filter(|segment| !segment.is_empty())
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                } else {
                    referenced_facet.heads.clone()
                };
                if commit_head_strings.is_empty() {
                    eyre::bail!(
                        "facet '{}' reference '{}' has empty commit-heads",
                        origin_facet_key,
                        referenced_facet.url_value
                    );
                }
                am_utils_rs::parse_commit_heads(&commit_head_strings).wrap_err_with(|| {
                    format!(
                        "facet '{}' reference '{}' has invalid commit-heads",
                        origin_facet_key, referenced_facet.url_value
                    )
                })?;
            }
        }

        Ok(())
    }

    fn validate_reference_url(
        url_value: &str,
        origin_facet_key: &FacetKey,
    ) -> Res<ValidatedReference> {
        let parsed_url = url::Url::parse(url_value).wrap_err_with(|| {
            format!(
                "facet '{}' contains invalid reference URL '{}'",
                origin_facet_key, url_value
            )
        })?;
        let parsed_facet_ref = parse_facet_ref(&parsed_url).wrap_err_with(|| {
            format!(
                "facet '{}' contains invalid facet reference URL '{}'",
                origin_facet_key, url_value
            )
        })?;
        Ok(ValidatedReference {
            doc_id: parsed_facet_ref.doc_id,
            facet_key: parsed_facet_ref.facet_key,
            url_value: url_value.to_string(),
            heads: vec![],
        })
    }

    fn validate_reference_object(
        facet_ref: &FacetRef,
        origin_facet_key: &FacetKey,
    ) -> Res<ValidatedReference> {
        let parsed_facet_ref = parse_facet_ref(&facet_ref.r#ref).wrap_err_with(|| {
            format!(
                "facet '{}' contains invalid facet reference URL '{}'",
                origin_facet_key, facet_ref.r#ref
            )
        })?;
        Ok(ValidatedReference {
            doc_id: parsed_facet_ref.doc_id,
            facet_key: parsed_facet_ref.facet_key,
            url_value: facet_ref.r#ref.to_string(),
            heads: facet_ref.heads.clone(),
        })
    }

    fn local_origin(&self) -> crate::event_origin::EventOrigin {
        crate::event_origin::EventOrigin::Local {
            actor_id: self.local_actor_id.to_string(),
        }
    }
}

impl crate::repos::Repo for DrawerRepo {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}
