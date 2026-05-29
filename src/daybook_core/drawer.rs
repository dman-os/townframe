// FIXME: use nested ids for facet keys
// FIXME: use ensure_alive method for cancellation checks
// FIXME: break aprart file, NVIM is lagging like 500ms on each scroll
// FIXME: remove tmp branches from the branch doc and use sqliite

use crate::app::SqlCtx;
use crate::interlude::*;
use crate::plugs::PlugsRepo;

mod cache;
pub mod dmeta;
mod events;
mod facet_recovery;
pub mod lru;
mod meta;
mod mutations;
mod queries;
#[cfg(test)]
mod tests;
pub mod types;

pub use crate::drawer::types::{DocBundle, DocEntry, DocEntryDiff, DocNBranches, DrawerEvent};

use big_repo::{SharedBigRepo, SharedPartStore};
use cache::FacetCacheKey;
use cache::*;
use lru::SharedKeyedLruPool;
use types::{BranchSnapshot, DocDeleteTombstone};

use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, DocId, FacetKey, FacetRaw};
use daybook_types::url::{parse_facet_ref, FACET_SELF_DOC_ID};

use tokio_util::sync::CancellationToken;

const DRAWER_REPLICATED_PARTITION_PREFIX: &str = "drawer.replicated";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchKind {
    Replicated,
    Local,
}
pub struct DrawerRepo {
    pub big_repo: SharedBigRepo,
    partition_store: SharedPartStore,
    drawer_doc_id: DocumentId,
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
}

#[derive(Debug, Clone)]
struct BranchRefRow {
    branch_doc_id: DocumentId,
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
        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "drawer-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let drawer_am_handle = big_repo
            .get_doc(&drawer_doc_id)
            .await?
            .ok_or_eyre("drawer doc not found")?;

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
            plugs_repo: Some(plugs_repo),
            #[cfg(test)]
            plugs_repo,
        });
        repo.ensure_local_branch_schema().await?;

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
        eyre::bail!("invalid branch path '{}'", branch_path)
    }

    pub(crate) fn replicated_partition_id_for_drawer(_drawer_doc_id: &DocumentId) -> PartId {
        crate::part_id_from_label(DRAWER_REPLICATED_PARTITION_PREFIX)
    }

    pub(crate) fn replicated_partition_id(&self) -> PartId {
        Self::replicated_partition_id_for_drawer(&self.drawer_doc_id)
    }

    async fn add_branch_to_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: DocumentId,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
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
                .add_obj_to_parts(branch_doc_id, vec![self.replicated_partition_id()])
                .await?;
        }
        Ok(())
    }

    async fn remove_branch_from_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: DocumentId,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
            self.partition_store
                .remove_obj_from_part(branch_doc_id, self.replicated_partition_id())
                .await?;
        }
        Ok(())
    }

    fn content_actor_id(
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

    async fn get_branch_heads_by_doc_id(
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

    async fn get_branch_heads_for_path(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<ChangeHashSet>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(None);
        };
        self.get_branch_heads_by_doc_id(branch_ref.branch_doc_id)
            .await
    }

    async fn get_handle_by_branch_doc_id(
        &self,
        document_id: DocumentId,
    ) -> Res<Option<big_repo::BigDocHandle>> {
        if let Some(handle) = surelock::key::lock_scope(|key| {
            let (handles, _key) = key.lock(&self.branch_handles);
            handles.get(&document_id).cloned()
        }) {
            return Ok(Some(handle));
        }
        let has_local = self.big_repo.get_doc(&document_id).await?.is_some();
        if !has_local {
            return Ok(None);
        }
        let Some(handle) = self.big_repo.get_doc(&document_id).await? else {
            return Ok(None);
        };
        surelock::key::lock_scope(|key| {
            let (mut handles, _key) = key.lock(&self.branch_handles);
            handles.insert(document_id, handle.clone());
        });
        Ok(Some(handle))
    }

    async fn resolve_handle_for_branch_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<big_repo::BigDocHandle>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            return Ok(None);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(branch_ref.branch_doc_id)
            .await?
        else {
            return Ok(None);
        };
        let (contains_all_heads, _missing_heads) = handle
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
            return Ok(None);
        }
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
    ) -> Res<HashSet<FacetKey>> {
        let branch_doc_id = snapshot.branch_doc_id;
        let handle = self
            .big_repo
            .get_doc(&branch_doc_id)
            .await?
            .ok_or_eyre("branch doc handle missing for tombstoned branch")?;
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
        Ok(keys)
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
    ) -> Option<daybook_types::manifest::FacetManifest> {
        if let Some(plugs_repo) = &self.plugs_repo {
            return plugs_repo.get_facet_manifest_by_tag(facet_tag).await;
        }

        // FIXME: I hate this
        if cfg!(test) {
            static SYSTEM_FACET_MANIFESTS: std::sync::OnceLock<
                HashMap<String, daybook_types::manifest::FacetManifest>,
            > = std::sync::OnceLock::new();
            let system_facet_manifests = SYSTEM_FACET_MANIFESTS.get_or_init(|| {
                let mut out = HashMap::new();
                for plug_manifest in crate::plugs::system_plugs() {
                    for facet_manifest in plug_manifest.facets {
                        out.insert(facet_manifest.key_tag.to_string(), facet_manifest);
                    }
                }
                out
            });
            system_facet_manifests.get(facet_tag).cloned()
        } else {
            None
        }
    }

    pub async fn validate_facets(
        &self,
        incoming_facets: &HashMap<FacetKey, FacetRaw>,
        resulting_facet_keys: &HashSet<FacetKey>,
    ) -> Res<()> {
        for (facet_key, facet_value) in incoming_facets {
            let facet_tag = facet_key.tag.to_string();
            let Some(facet_manifest) = self.facet_manifest_for_tag(&facet_tag).await else {
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

    fn validate_facet_reference(
        &self,
        resulting_facet_keys: &HashSet<FacetKey>,
        origin_facet_key: &FacetKey,
        origin_facet_value: &FacetRaw,
        reference_manifest: &daybook_types::manifest::FacetReferenceManifest,
    ) -> Res<()> {
        let selected_values = daybook_types::reference::select_json_path_values(
            origin_facet_value,
            &reference_manifest.json_path,
        )?;
        if selected_values.is_empty() {
            eyre::bail!(
                "facet '{}' reference path '{}' is missing",
                origin_facet_key,
                reference_manifest.json_path
            );
        }

        let mut referenced_facets = Vec::new();
        for selected_value in selected_values {
            match selected_value {
                serde_json::Value::String(url_value) => {
                    referenced_facets
                        .push(self.validate_reference_url(url_value, origin_facet_key)?);
                }
                serde_json::Value::Array(url_values) => {
                    for url_value in url_values {
                        let serde_json::Value::String(url_string) = url_value else {
                            eyre::bail!(
                                "facet '{}' reference path '{}' must contain URL strings",
                                origin_facet_key,
                                reference_manifest.json_path
                            );
                        };
                        referenced_facets
                            .push(self.validate_reference_url(url_string, origin_facet_key)?);
                    }
                }
                _ => {
                    eyre::bail!(
                        "facet '{}' reference path '{}' must contain URL strings",
                        origin_facet_key,
                        reference_manifest.json_path
                    );
                }
            }
        }

        if let Some(at_commit_json_path) = &reference_manifest.at_commit_json_path {
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
                let parsed_url = url::Url::parse(&referenced_facet.url_value)?;
                let Some(fragment) = parsed_url.fragment() else {
                    eyre::bail!(
                        "facet '{}' reference '{}' must include commit heads in URL fragment when at_commit_json_path is not declared",
                        origin_facet_key,
                        referenced_facet.url_value
                    );
                };
                let commit_head_strings: Vec<String> = fragment
                    .split('|')
                    .filter(|segment| !segment.is_empty())
                    .map(ToString::to_string)
                    .collect();
                if commit_head_strings.is_empty() {
                    if referenced_facet.doc_id == FACET_SELF_DOC_ID
                        && resulting_facet_keys.contains(&referenced_facet.facet_key)
                    {
                        // Empty fragment means "self in this validated facet set" for URL refs
                        // when at_commit_json_path is omitted (e.g. Body.order).
                        continue;
                    }
                    eyre::bail!(
                        "facet '{}' reference '{}' has empty commit-head fragment",
                        origin_facet_key,
                        referenced_facet.url_value
                    );
                }
                am_utils_rs::parse_commit_heads(&commit_head_strings).wrap_err_with(|| {
                    format!(
                        "facet '{}' reference '{}' has invalid commit-head fragment",
                        origin_facet_key, referenced_facet.url_value
                    )
                })?;
            }
        }

        Ok(())
    }

    fn validate_reference_url(
        &self,
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
        })
    }

    fn local_origin(&self) -> crate::event_origin::SwitchEventOrigin {
        crate::event_origin::SwitchEventOrigin::Local {
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
