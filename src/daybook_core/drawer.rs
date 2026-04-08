// FIXME: use nested ids for facet keys
// FIXME: use ensure_alive method for cancellation checks
// FIXME: break aprart file, NVIM is lagging like 500ms on each scroll
// FIXME: remove tmp branches from the branch doc and use sqliite

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

use cache::*;
use lru::SharedKeyedLruPool;
use types::{BranchSnapshot, DocDeleteTombstone};

use automerge::ReadDoc;
use daybook_types::doc::{ChangeHashSet, DocId, FacetKey, FacetRaw};
use daybook_types::url::{parse_facet_ref, FACET_SELF_DOC_ID};

use std::str::FromStr;
use tokio_util::sync::CancellationToken;

const DRAWER_REPLICATED_PARTITION_PREFIX: &str = "drawer.replicated";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchKind {
    Replicated,
    Local,
}
pub struct DrawerRepo {
    pub big_repo: SharedBigRepo,
    drawer_doc_id: DocumentId,
    local_actor_id: ActorId,
    local_peer_id: String,
    local_user_path: daybook_types::doc::UserPath,

    // LRU Caches
    entry_cache: Arc<DHashMap<DocId, DocEntry>>,
    facet_cache: std::sync::Mutex<FacetCacheState>,
    facet_schema_validators:
        std::sync::Mutex<HashMap<(String, String), Arc<jsonschema::Validator>>>,
    branch_handles: Arc<DHashMap<String, am_utils_rs::repo::BigDocHandle>>,

    // LRU Pools (Policy only)
    entry_pool: SharedKeyedLruPool<DocId>,

    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<am_utils_rs::repo::BigRepoChangeListenerRegistration>,
    _change_broker_leases: Vec<Arc<am_utils_rs::repo::BigRepoDocChangeBrokerLease>>,
    current_heads: std::sync::Mutex<ChangeHashSet>,
    drawer_am_handle: samod::DocHandle,
    meta_db_pool: sqlx::SqlitePool,
    plugs_repo: Option<Arc<crate::plugs::PlugsRepo>>,
}

struct ValidatedReference {
    doc_id: DocId,
    facet_key: FacetKey,
    url_value: String,
}

#[derive(Debug, Clone)]
struct BranchRefRow {
    branch_doc_id: String,
    branch_kind: BranchKind,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct BranchStateRow {
    branch_path: String,
    branch_doc_id: String,
    latest_heads: ChangeHashSet,
    branch_kind: BranchKind,
}

impl DrawerRepo {
    pub fn drawer_doc_id(&self) -> &DocumentId {
        &self.drawer_doc_id
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        big_repo: SharedBigRepo,
        drawer_doc_id: DocumentId,
        local_user_path: daybook_types::doc::UserPath,
        meta_db_pool: sqlx::SqlitePool,
        _local_state_root: PathBuf,
        entry_pool: SharedKeyedLruPool<DocId>,
        doc_pool: SharedKeyedLruPool<FacetCacheKey>,
        #[cfg(not(test))] plugs_repo: Arc<PlugsRepo>,
        #[cfg(test)] plugs_repo: Option<Arc<PlugsRepo>>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(&local_user_path, "drawer-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let drawer_am_handle = big_repo
            .find_doc_handle(&drawer_doc_id)
            .await?
            .ok_or_eyre("drawer doc not found")?;

        let initial_heads =
            drawer_am_handle.with_document(|doc| ChangeHashSet(doc.get_heads().into()));

        let broker = big_repo
            .ensure_change_broker(drawer_am_handle.clone())
            .await?;

        // Listen for changes to docs.map
        let (ticket, notif_rx) = big_repo
            .subscribe_change_listener(am_utils_rs::repo::BigRepoChangeFilter {
                doc_id: Some(am_utils_rs::repo::BigRepoDocIdFilter::new(
                    drawer_doc_id.clone(),
                )),
                path: vec!["docs".into(), "map".into()],
                origin: None,
            })
            .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            local_peer_id: big_repo.samod_repo().peer_id().to_string(),
            big_repo,
            drawer_doc_id,
            local_actor_id,
            local_user_path,
            entry_cache: Arc::new(DHashMap::new()),
            facet_cache: std::sync::Mutex::new(FacetCacheState::new(doc_pool)),
            facet_schema_validators: std::sync::Mutex::new(HashMap::new()),
            branch_handles: Arc::new(DHashMap::new()),
            entry_pool,
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
            _change_broker_leases: vec![broker],
            current_heads: initial_heads.into(),
            drawer_am_handle,
            meta_db_pool,
            #[cfg(not(test))]
            plugs_repo: Some(plugs_repo),
            #[cfg(test)]
            plugs_repo,
        });
        repo.ensure_local_branch_schema().await?;
        repo.migrate_legacy_local_branches_from_drawer_map().await?;

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
        if branch_path == &daybook_types::doc::BranchPath::from("main") {
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

    pub(crate) fn replicated_partition_id_for_drawer(
        _drawer_doc_id: &DocumentId,
    ) -> am_utils_rs::sync::protocol::PartitionId {
        DRAWER_REPLICATED_PARTITION_PREFIX.to_string()
    }

    pub(crate) fn replicated_partition_id(&self) -> am_utils_rs::sync::protocol::PartitionId {
        Self::replicated_partition_id_for_drawer(&self.drawer_doc_id)
    }

    async fn add_branch_to_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: &str,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
            self.big_repo
                .partition_store()
                .add_member(
                    &self.replicated_partition_id(),
                    branch_doc_id,
                    &serde_json::json!({}),
                )
                .await?;
        }
        Ok(())
    }

    async fn remove_branch_from_partitions_if_needed(
        &self,
        branch_kind: BranchKind,
        branch_doc_id: &str,
    ) -> Res<()> {
        if branch_kind == BranchKind::Replicated {
            self.big_repo
                .partition_store()
                .remove_member(
                    &self.replicated_partition_id(),
                    branch_doc_id,
                    &serde_json::json!({}),
                )
                .await?;
        }
        Ok(())
    }

    fn content_actor_id(
        &self,
        user_path: Option<&daybook_types::doc::UserPath>,
        branch_doc_id: &str,
    ) -> ActorId {
        let base_user_path = user_path
            .cloned()
            .unwrap_or_else(|| self.local_user_path.clone());
        let scoped_user_path = base_user_path.join("branches").join(branch_doc_id);
        daybook_types::doc::user_path::to_actor_id(&scoped_user_path)
    }

    async fn get_branch_heads_by_doc_id(&self, branch_doc_id: &str) -> Res<Option<ChangeHashSet>> {
        let Some(handle) = self.get_handle_by_branch_doc_id(branch_doc_id).await? else {
            return Ok(None);
        };
        let latest_heads = handle
            .with_document_local(|doc| ChangeHashSet(doc.get_heads().into()))
            .await?;
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
        self.get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
            .await
    }

    async fn get_handle_by_branch_doc_id(
        &self,
        branch_doc_id: &str,
    ) -> Res<Option<am_utils_rs::repo::BigDocHandle>> {
        if let Some(handle) = self.branch_handles.get(branch_doc_id) {
            return Ok(Some(handle.clone()));
        }
        let document_id = DocumentId::from_str(branch_doc_id)?;
        let has_local = self.big_repo.local_contains_document(&document_id).await?;
        if !has_local {
            return Ok(None);
        }
        let Some(handle) = self.big_repo.find_doc(&document_id).await? else {
            return Ok(None);
        };
        self.branch_handles
            .insert(branch_doc_id.to_string(), handle.clone());
        Ok(Some(handle))
    }

    async fn resolve_handle_for_branch_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<am_utils_rs::repo::BigDocHandle>> {
        let Some(branch_ref) = self.get_branch_ref(doc_id, branch_path).await? else {
            debug!(
                ?doc_id,
                branch_path = %branch_path,
                heads = ?am_utils_rs::serialize_commit_heads(heads.as_ref()),
                "resolve_handle_for_branch_heads: branch ref not found"
            );
            return Ok(None);
        };
        let Some(handle) = self
            .get_handle_by_branch_doc_id(&branch_ref.branch_doc_id)
            .await?
        else {
            debug!(
                ?doc_id,
                branch_path = %branch_path,
                branch_doc_id = %branch_ref.branch_doc_id,
                heads = ?am_utils_rs::serialize_commit_heads(heads.as_ref()),
                "resolve_handle_for_branch_heads: branch handle not found locally"
            );
            return Ok(None);
        };
        let (contains_all_heads, missing_heads) = handle
            .with_document_local(|doc| {
                let mut missing = Vec::new();
                for head in heads.iter() {
                    if doc.get_change_by_hash(head).is_none() {
                        missing.push(head.to_string());
                    }
                }
                Ok::<(bool, Vec<String>), eyre::Report>((missing.is_empty(), missing))
            })
            .await??;
        if !contains_all_heads {
            debug!(
                ?doc_id,
                branch_path = %branch_path,
                branch_doc_id = %branch_ref.branch_doc_id,
                heads = ?am_utils_rs::serialize_commit_heads(heads.as_ref()),
                ?missing_heads,
                "resolve_handle_for_branch_heads: branch is missing requested heads"
            );
            return Ok(None);
        }
        debug!(
            ?doc_id,
            branch_path = %branch_path,
            branch_doc_id = %branch_ref.branch_doc_id,
            heads = ?am_utils_rs::serialize_commit_heads(heads.as_ref()),
            "resolve_handle_for_branch_heads: resolved successfully"
        );
        Ok(Some(handle))
    }

    async fn latest_doc_delete_tombstone(
        &self,
        doc_id: &DocId,
        heads: &Arc<[automerge::ChangeHash]>,
    ) -> Res<Option<DocDeleteTombstone>> {
        let Some((tags, _)) = self
            .big_repo
            .hydrate_path_at_heads::<Vec<DocDeleteTombstone>>(
                &self.drawer_doc_id,
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
        let branch_doc_id = DocumentId::from_str(&snapshot.branch_doc_id)
            .wrap_err_with(|| format!("invalid branch doc id '{}'", snapshot.branch_doc_id))?;
        let handle = self
            .big_repo
            .find_doc_handle(&branch_doc_id)
            .await?
            .ok_or_eyre("branch doc handle missing for tombstoned branch")?;
        let keys = handle.with_document(|am_doc| {
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
            for item in
                automerge::ReadDoc::map_range_at(am_doc, &facets_obj, .., &snapshot.branch_heads)
            {
                out.insert(FacetKey::from(item.key.to_string().as_str()));
            }
            Ok(out)
        })?;
        Ok(keys)
    }

    async fn non_tmp_branch_snapshots_for_entry(
        &self,
        _doc_id: &DocId,
        entry: &DocEntry,
    ) -> Res<HashMap<String, BranchSnapshot>> {
        let mut out = HashMap::new();
        for (branch_name, branch_ref) in &entry.branches {
            let branch_path = daybook_types::doc::BranchPath::from(branch_name.as_str());
            if branch_path.to_string().starts_with("/tmp/") {
                continue;
            }
            let Some(branch_heads) = self
                .get_branch_heads_by_doc_id(&branch_ref.branch_doc_id)
                .await?
            else {
                continue;
            };
            out.insert(
                branch_name.clone(),
                BranchSnapshot {
                    branch_doc_id: branch_ref.branch_doc_id.clone(),
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
            let mut cache = self.facet_schema_validators.lock().unwrap();
            let validator = if let Some(existing) = cache.get(&schema_cache_key) {
                Arc::clone(existing)
            } else {
                let compiled = jsonschema::validator_for(&schema_json).map_err(|err| {
                    eyre::eyre!(
                        "failed to compile facet schema validator for facet_manifest tag '{}': {err}",
                        facet_manifest.key_tag
                    )
                })?;
                let compiled = Arc::new(compiled);
                cache.insert(schema_cache_key, Arc::clone(&compiled));
                compiled
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
