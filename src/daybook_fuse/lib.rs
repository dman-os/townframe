#![allow(unused)]

mod interlude {
    pub use std::collections::{BTreeMap, BTreeSet};
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
    pub use std::time::Duration;

    pub use daybook_types::doc::{self, Doc, DocId};
    pub use tokio_util::sync::CancellationToken;
    pub use utils_rs::prelude::*;
}

use interlude::*;

use daybook_core::drawer::DrawerRepo;

#[derive(Debug, Clone)]
pub struct Config {
    pub root_path: PathBuf,
    pub metadata_db_path: PathBuf,
    pub branch_path: daybook_types::doc::BranchPath,
    pub poll_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            root_path: PathBuf::from("./.daybook/livetree"),
            metadata_db_path: PathBuf::from("./.daybook/pauperfuse/livetree.sqlite"),
            branch_path: daybook_types::doc::BranchPath::from("main"),
            poll_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunReport {
    pub backend_delta_count: usize,
    pub provider_delta_count: usize,
    pub effect_count: usize,
    pub scanned_doc_count: usize,
    pub changed_doc_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct StatusReport {
    pub in_sync_count: usize,
    pub provider_only_count: usize,
    pub backend_only_count: usize,
    pub diverged_count: usize,
    pub scanned_doc_count: usize,
    pub changed_doc_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SyncState {
    InSync,
    ProviderOnly,
    BackendOnly,
    Diverged,
}

pub struct DaybookFuseCtx {
    pub ppf_ctx: pauperfuse::Ctx,
    pub drawer_repo: Arc<DrawerRepo>,
    pub config: Config,
}

impl DaybookFuseCtx {
    pub fn new(config: Config, drawer_repo: Arc<DrawerRepo>) -> Self {
        let ppf_config = pauperfuse::Config {
            root_path: config.root_path.clone(),
        };
        Self {
            ppf_ctx: pauperfuse::Ctx::new(ppf_config),
            drawer_repo,
            config,
        }
    }

    fn reset_ppf_ctx(&mut self) {
        self.ppf_ctx = pauperfuse::Ctx::new(pauperfuse::Config {
            root_path: self.config.root_path.clone(),
        });
    }
}

pub async fn bootstrap_livetree(ctx: &mut DaybookFuseCtx) -> Res<()> {
    pull_changes(ctx).await?;
    Ok(())
}

pub async fn status(ctx: &mut DaybookFuseCtx) -> Res<StatusReport> {
    tokio::fs::create_dir_all(&ctx.config.root_path).await?;

    let metadata_store =
        pauperfuse::store::sqlite::SqliteStateStore::open(&ctx.config.metadata_db_path).await?;
    let previous_state = metadata_store.load_state().await?;

    let provider_snapshot = collect_provider_snapshot(ctx).await?;
    let backend_snapshot = collect_backend_snapshot(ctx).await?;

    let current_state = pauperfuse::store::build_persisted_state(
        previous_state.provider_state_id,
        previous_state.backend_state_id,
        &provider_snapshot,
        &backend_snapshot,
    );

    let mut status_report = StatusReport {
        scanned_doc_count: current_state.objects.len(),
        changed_doc_count: pauperfuse::store::count_changed_docs(&previous_state, &current_state),
        ..StatusReport::default()
    };

    let mut all_doc_ids: BTreeSet<String> = default();
    all_doc_ids.extend(provider_snapshot.keys().cloned());
    all_doc_ids.extend(backend_snapshot.keys().cloned());

    for doc_id in all_doc_ids {
        let provider = provider_snapshot.get(&doc_id);
        let backend = backend_snapshot.get(&doc_id);

        match classify_sync_state(provider, backend) {
            SyncState::InSync => status_report.in_sync_count += 1,
            SyncState::ProviderOnly => status_report.provider_only_count += 1,
            SyncState::BackendOnly => status_report.backend_only_count += 1,
            SyncState::Diverged => status_report.diverged_count += 1,
        }
    }

    Ok(status_report)
}

pub async fn pull_changes(ctx: &mut DaybookFuseCtx) -> Res<RunReport> {
    tokio::fs::create_dir_all(&ctx.config.root_path).await?;
    ctx.reset_ppf_ctx();

    let metadata_store =
        pauperfuse::store::sqlite::SqliteStateStore::open(&ctx.config.metadata_db_path).await?;
    let previous_state = metadata_store.load_state().await?;

    let provider_snapshot = collect_provider_snapshot(ctx).await?;
    let backend_snapshot = collect_backend_snapshot(ctx).await?;

    let provider_deltas = provider_to_backend_deltas(&provider_snapshot, &backend_snapshot);
    for provider_delta in provider_deltas.iter().cloned() {
        ctx.ppf_ctx.ingest_provider_delta(provider_delta)?;
    }

    let _report = ctx.ppf_ctx.reconcile()?;
    let effects = ctx.ppf_ctx.effects();
    for effect in effects.iter().cloned() {
        apply_effect(ctx, effect).await?;
    }

    let refreshed_backend_snapshot = collect_backend_snapshot(ctx).await?;
    let next_state = pauperfuse::store::build_persisted_state(
        previous_state
            .provider_state_id
            .saturating_add(provider_deltas.len() as u64),
        previous_state.backend_state_id,
        &provider_snapshot,
        &refreshed_backend_snapshot,
    );
    metadata_store.save_state(&next_state).await?;

    Ok(RunReport {
        backend_delta_count: 0,
        provider_delta_count: provider_deltas.len(),
        effect_count: effects.len(),
        scanned_doc_count: next_state.objects.len(),
        changed_doc_count: pauperfuse::store::count_changed_docs(&previous_state, &next_state),
    })
}

pub async fn push_changes(ctx: &mut DaybookFuseCtx) -> Res<RunReport> {
    tokio::fs::create_dir_all(&ctx.config.root_path).await?;
    ctx.reset_ppf_ctx();

    let metadata_store =
        pauperfuse::store::sqlite::SqliteStateStore::open(&ctx.config.metadata_db_path).await?;
    let previous_state = metadata_store.load_state().await?;

    let provider_snapshot = collect_provider_snapshot(ctx).await?;
    let backend_snapshot = collect_backend_snapshot(ctx).await?;

    let backend_deltas = backend_to_provider_deltas(&provider_snapshot, &backend_snapshot);
    for backend_delta in backend_deltas.iter().cloned() {
        ctx.ppf_ctx.ingest_backend_delta(backend_delta)?;
    }

    let _report = ctx.ppf_ctx.reconcile()?;
    let effects = ctx.ppf_ctx.effects();
    for effect in effects.iter().cloned() {
        apply_effect(ctx, effect).await?;
    }

    let refreshed_provider_snapshot = collect_provider_snapshot(ctx).await?;
    let next_state = pauperfuse::store::build_persisted_state(
        previous_state.provider_state_id,
        previous_state
            .backend_state_id
            .saturating_add(backend_deltas.len() as u64),
        &refreshed_provider_snapshot,
        &backend_snapshot,
    );
    metadata_store.save_state(&next_state).await?;

    Ok(RunReport {
        backend_delta_count: backend_deltas.len(),
        provider_delta_count: 0,
        effect_count: effects.len(),
        scanned_doc_count: next_state.objects.len(),
        changed_doc_count: pauperfuse::store::count_changed_docs(&previous_state, &next_state),
    })
}

pub async fn reconcile_once(ctx: &mut DaybookFuseCtx) -> Res<RunReport> {
    let push_report = push_changes(ctx).await?;
    let pull_report = pull_changes(ctx).await?;

    Ok(RunReport {
        backend_delta_count: push_report.backend_delta_count,
        provider_delta_count: pull_report.provider_delta_count,
        effect_count: push_report.effect_count + pull_report.effect_count,
        scanned_doc_count: std::cmp::max(
            push_report.scanned_doc_count,
            pull_report.scanned_doc_count,
        ),
        changed_doc_count: push_report.changed_doc_count + pull_report.changed_doc_count,
    })
}

fn make_relative_json_path(doc_id: &str) -> PathBuf {
    PathBuf::from(format!("{doc_id}.json"))
}

fn parse_doc_id_from_relative_json_path(relative_path: &Path) -> Option<String> {
    let extension = relative_path.extension()?.to_str()?;
    if extension != "json" {
        return None;
    }
    let stem = relative_path.file_stem()?.to_str()?;
    if stem.is_empty() {
        return None;
    }
    Some(stem.to_string())
}

fn make_object_snapshot(doc_id: &str, bytes: Vec<u8>) -> pauperfuse::ObjectSnapshot {
    pauperfuse::ObjectSnapshot {
        object_id: pauperfuse::ObjectId::from(doc_id.to_string()),
        relative_path: make_relative_json_path(doc_id),
        bytes,
    }
}

fn make_object_ref(doc_id: &str) -> pauperfuse::ObjectRef {
    pauperfuse::ObjectRef {
        object_id: pauperfuse::ObjectId::from(doc_id.to_string()),
        relative_path: make_relative_json_path(doc_id),
    }
}

fn classify_sync_state(
    provider_bytes: Option<&Vec<u8>>,
    backend_bytes: Option<&Vec<u8>>,
) -> SyncState {
    match (provider_bytes, backend_bytes) {
        (Some(provider), Some(backend)) => {
            if provider == backend {
                SyncState::InSync
            } else {
                SyncState::Diverged
            }
        }
        (Some(_), None) => SyncState::ProviderOnly,
        (None, Some(_)) => SyncState::BackendOnly,
        (None, None) => SyncState::InSync,
    }
}

fn provider_to_backend_deltas(
    provider_snapshot: &BTreeMap<DocId, Vec<u8>>,
    backend_snapshot: &BTreeMap<DocId, Vec<u8>>,
) -> Vec<pauperfuse::ProviderDelta> {
    let mut provider_deltas = Vec::new();

    for (doc_id, provider_bytes) in provider_snapshot {
        let maybe_backend_bytes = backend_snapshot.get(doc_id);
        if maybe_backend_bytes != Some(provider_bytes) {
            provider_deltas.push(pauperfuse::ProviderDelta::Upsert(make_object_snapshot(
                doc_id,
                provider_bytes.clone(),
            )));
        }
    }

    for backend_doc_id in backend_snapshot.keys() {
        if !provider_snapshot.contains_key(backend_doc_id) {
            provider_deltas.push(pauperfuse::ProviderDelta::Remove(make_object_ref(
                backend_doc_id,
            )));
        }
    }

    provider_deltas
}

fn backend_to_provider_deltas(
    provider_snapshot: &BTreeMap<DocId, Vec<u8>>,
    backend_snapshot: &BTreeMap<DocId, Vec<u8>>,
) -> Vec<pauperfuse::BackendDelta> {
    let mut backend_deltas = Vec::new();

    for (doc_id, backend_bytes) in backend_snapshot {
        let maybe_provider_bytes = provider_snapshot.get(doc_id);
        if maybe_provider_bytes != Some(backend_bytes) {
            backend_deltas.push(pauperfuse::BackendDelta::Upsert(make_object_snapshot(
                doc_id,
                backend_bytes.clone(),
            )));
        }
    }

    for provider_doc_id in provider_snapshot.keys() {
        if !backend_snapshot.contains_key(provider_doc_id) {
            backend_deltas.push(pauperfuse::BackendDelta::Remove(make_object_ref(
                provider_doc_id,
            )));
        }
    }

    backend_deltas
}

async fn collect_provider_snapshot(ctx: &mut DaybookFuseCtx) -> Res<BTreeMap<DocId, Vec<u8>>> {
    let mut provider_snapshot = BTreeMap::new();

    for doc_entry in ctx.drawer_repo.list().await? {
        let Some(branch_path) = doc_entry.main_branch_path() else {
            continue;
        };

        let maybe_doc = ctx
            .drawer_repo
            .get_doc_with_facets_at_branch(&doc_entry.doc_id, &branch_path, None)
            .await?;

        if let Some(doc) = maybe_doc {
            let bytes = stable_doc_json_bytes(&doc)?;
            provider_snapshot.insert(doc_entry.doc_id.clone(), bytes);
        }
    }

    Ok(provider_snapshot)
}

fn stable_doc_json_bytes(doc: &Doc) -> Res<Vec<u8>> {
    #[derive(serde::Serialize)]
    struct StableDoc {
        id: String,
        facets: BTreeMap<String, serde_json::Value>,
    }

    let mut facets = BTreeMap::new();
    for (facet_key, facet_value) in &doc.facets {
        facets.insert(facet_key.to_string(), facet_value.clone());
    }

    let stable_doc = StableDoc {
        id: doc.id.clone(),
        facets,
    };

    Ok(serde_json::to_vec_pretty(&stable_doc)?)
}

async fn collect_backend_snapshot(ctx: &mut DaybookFuseCtx) -> Res<BTreeMap<DocId, Vec<u8>>> {
    let mut backend_snapshot = BTreeMap::new();

    if !tokio::fs::try_exists(&ctx.config.root_path).await? {
        return Ok(backend_snapshot);
    }

    let mut read_dir = tokio::fs::read_dir(&ctx.config.root_path).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }

        let relative_path = PathBuf::from(entry.file_name());
        let Some(doc_id) = parse_doc_id_from_relative_json_path(&relative_path) else {
            continue;
        };

        let absolute_path = ctx.config.root_path.join(relative_path);
        let bytes = tokio::fs::read(absolute_path).await?;
        backend_snapshot.insert(doc_id, bytes);
    }

    Ok(backend_snapshot)
}

async fn apply_effect(ctx: &mut DaybookFuseCtx, effect: pauperfuse::Effect) -> Res<()> {
    match effect {
        pauperfuse::Effect::BackendWriteFile(snapshot) => {
            let absolute_path = ctx.config.root_path.join(&snapshot.relative_path);
            if let Some(parent_dir) = absolute_path.parent() {
                tokio::fs::create_dir_all(parent_dir).await?;
            }
            tokio::fs::write(absolute_path, snapshot.bytes).await?;
        }
        pauperfuse::Effect::BackendRemoveFile(object_ref) => {
            let absolute_path = ctx.config.root_path.join(&object_ref.relative_path);
            if tokio::fs::try_exists(&absolute_path).await? {
                tokio::fs::remove_file(absolute_path).await?;
            }
        }
        pauperfuse::Effect::ProviderObserveUpsert(snapshot) => {
            let parsed_doc: Doc =
                serde_json::from_slice(&snapshot.bytes).wrap_err("invalid doc json in livetree")?;

            if parsed_doc.id != snapshot.object_id.0 {
                eyre::bail!(
                    "mismatched doc id in livetree edit: path id={}, payload id={}",
                    snapshot.object_id.0,
                    parsed_doc.id
                );
            }

            let branch_path = ctx.config.branch_path.clone();
            let maybe_existing_doc = ctx
                .drawer_repo
                .get_doc_with_facets_at_branch(&snapshot.object_id.0, &branch_path, None)
                .await?;

            let Some(existing_doc) = maybe_existing_doc else {
                eyre::bail!(
                    "livetree edit references unknown doc id: {}",
                    snapshot.object_id.0
                );
            };

            let patch = daybook_types::doc::Doc::diff(&existing_doc, &parsed_doc);
            if !patch.is_empty() {
                ctx.drawer_repo
                    .update_at_heads(patch, branch_path, None)
                    .await?;
            }
        }
        pauperfuse::Effect::ProviderObserveRemove(object_ref) => {
            ctx.drawer_repo.del(&object_ref.object_id.0).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_core::drawer::lru::KeyedLruPool;
    use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};

    struct TestHarness {
        drawer_repo: Arc<DrawerRepo>,
        drawer_stop: Option<daybook_core::repos::RepoStopToken>,
        acx_stop: Option<utils_rs::am::AmCtxStopToken>,
        temp_dir: tempfile::TempDir,
    }

    impl TestHarness {
        async fn new() -> Res<Self> {
            let temp_dir = tempfile::tempdir()?;
            let am_path = temp_dir.path().join("samod");
            tokio::fs::create_dir_all(&am_path).await?;

            let sql_path = temp_dir.path().join("sqlite.db");
            let sql_url = format!("sqlite://{}", sql_path.display());
            let sql_ctx = daybook_core::repo::SqlCtx::new(&sql_url).await?;
            daybook_core::repo::set_local_user_path(&sql_ctx.db_pool, "/test-device").await?;

            let (acx, acx_stop) = utils_rs::am::AmCtx::boot(
                utils_rs::am::Config {
                    storage: utils_rs::am::StorageConfig::Disk { path: am_path },
                    peer_id: "daybook_fuse_test".to_string(),
                },
                Option::<samod::AlwaysAnnounce>::None,
            )
            .await?;

            let doc_app = tokio::sync::OnceCell::new();
            let doc_drawer = tokio::sync::OnceCell::new();
            daybook_core::repo::init_from_globals(&acx, &sql_ctx.db_pool, &doc_app, &doc_drawer)
                .await?;

            let local_actor_id = daybook_types::doc::user_path::to_actor_id(
                &daybook_types::doc::UserPath::from("/test-device"),
            );

            let (drawer_repo, drawer_stop) = DrawerRepo::load(
                acx,
                doc_drawer
                    .get()
                    .expect("drawer doc init")
                    .document_id()
                    .clone(),
                local_actor_id,
                Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
                Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
            )
            .await?;

            Ok(Self {
                drawer_repo,
                drawer_stop: Some(drawer_stop),
                acx_stop: Some(acx_stop),
                temp_dir,
            })
        }

        async fn shutdown(mut self) -> Res<()> {
            if let Some(stop) = self.drawer_stop.take() {
                stop.stop().await?;
            }
            if let Some(stop) = self.acx_stop.take() {
                stop.stop().await?;
            }
            Ok(())
        }

        async fn create_doc(&self, title: &str) -> Res<String> {
            let doc_id = self
                .drawer_repo
                .add(AddDocArgs {
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    facets: [(
                        FacetKey::from(WellKnownFacetTag::TitleGeneric),
                        WellKnownFacet::TitleGeneric(title.to_string()).into(),
                    )]
                    .into(),
                    user_path: None,
                })
                .await?;
            Ok(doc_id)
        }
    }

    fn test_config(base: &Path) -> Config {
        Config {
            root_path: base.join("livetree"),
            metadata_db_path: base.join("metadata").join("livetree.sqlite"),
            ..Config::default()
        }
    }

    #[test]
    fn test_parse_doc_id_from_relative_json_path() {
        let relative_path = PathBuf::from("abc123.json");
        assert_eq!(
            parse_doc_id_from_relative_json_path(&relative_path),
            Some("abc123".to_string())
        );

        let non_json = PathBuf::from("abc123.txt");
        assert!(parse_doc_id_from_relative_json_path(&non_json).is_none());
    }

    #[test]
    fn test_make_relative_json_path() {
        assert_eq!(
            make_relative_json_path("doc-1"),
            PathBuf::from("doc-1.json")
        );
    }

    #[test]
    fn test_sync_state_table() {
        let same = vec![1, 2, 3];
        let other = vec![9, 9, 9];

        let cases = vec![
            (Some(&same), Some(&same), SyncState::InSync),
            (Some(&same), Some(&other), SyncState::Diverged),
            (Some(&same), None, SyncState::ProviderOnly),
            (None, Some(&same), SyncState::BackendOnly),
            (None, None, SyncState::InSync),
        ];

        for (provider, backend, expected) in cases {
            let observed = classify_sync_state(provider, backend);
            assert_eq!(observed, expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bootstrap_materializes_provider_docs() -> Res<()> {
        let harness = TestHarness::new().await?;
        let doc_id = harness.create_doc("Materialize Test").await?;

        let config = test_config(harness.temp_dir.path());
        let mut fuse_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));

        bootstrap_livetree(&mut fuse_ctx).await?;

        let json_path = config.root_path.join(format!("{doc_id}.json"));
        assert!(tokio::fs::try_exists(&json_path).await?);
        let bytes = tokio::fs::read(json_path).await?;
        let parsed: Doc = serde_json::from_slice(&bytes)?;
        assert_eq!(parsed.id, doc_id);

        assert!(tokio::fs::try_exists(&config.metadata_db_path).await?);

        harness.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_push_updates_drawer_from_livetree_edit() -> Res<()> {
        let harness = TestHarness::new().await?;
        let doc_id = harness.create_doc("Before Push").await?;

        let config = test_config(harness.temp_dir.path());
        let mut fuse_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));

        bootstrap_livetree(&mut fuse_ctx).await?;

        let json_path = config.root_path.join(format!("{doc_id}.json"));
        let original_bytes = tokio::fs::read(&json_path).await?;
        let mut parsed_doc: Doc = serde_json::from_slice(&original_bytes)?;
        parsed_doc.facets.insert(
            FacetKey::from(WellKnownFacetTag::TitleGeneric),
            serde_json::to_value(WellKnownFacet::TitleGeneric("After Push".to_string()))?,
        );
        tokio::fs::write(&json_path, serde_json::to_vec_pretty(&parsed_doc)?).await?;

        let report = push_changes(&mut fuse_ctx).await?;
        assert!(report.effect_count > 0);

        let stored_doc = harness
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
            .expect("doc should exist");

        let title_val = stored_doc
            .facets
            .get(&FacetKey::from(WellKnownFacetTag::TitleGeneric))
            .expect("title facet");
        let title = WellKnownFacet::from_json(title_val.clone(), WellKnownFacetTag::TitleGeneric)?;
        let WellKnownFacet::TitleGeneric(title) = title else {
            panic!("unexpected title facet shape")
        };
        assert_eq!(title, "After Push");

        harness.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pull_updates_livetree_from_provider_edit() -> Res<()> {
        let harness = TestHarness::new().await?;
        let doc_id = harness.create_doc("Before Pull").await?;

        let config = test_config(harness.temp_dir.path());
        let mut fuse_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));

        bootstrap_livetree(&mut fuse_ctx).await?;

        let current_doc = harness
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
            .expect("doc should exist");

        let mut edited_doc = (*current_doc).clone();
        edited_doc.facets.insert(
            FacetKey::from(WellKnownFacetTag::TitleGeneric),
            serde_json::to_value(WellKnownFacet::TitleGeneric("After Pull".to_string()))?,
        );

        let patch = Doc::diff(&current_doc, &edited_doc);
        harness
            .drawer_repo
            .update_at_heads(patch, daybook_types::doc::BranchPath::from("main"), None)
            .await?;

        let report = pull_changes(&mut fuse_ctx).await?;
        assert!(report.effect_count > 0);

        let json_path = config.root_path.join(format!("{doc_id}.json"));
        let bytes = tokio::fs::read(json_path).await?;
        let parsed_doc: Doc = serde_json::from_slice(&bytes)?;

        let title_val = parsed_doc
            .facets
            .get(&FacetKey::from(WellKnownFacetTag::TitleGeneric))
            .expect("title facet");
        let title = WellKnownFacet::from_json(title_val.clone(), WellKnownFacetTag::TitleGeneric)?;
        let WellKnownFacet::TitleGeneric(title) = title else {
            panic!("unexpected title facet shape")
        };
        assert_eq!(title, "After Pull");

        harness.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_status_detects_divergence() -> Res<()> {
        let harness = TestHarness::new().await?;
        let doc_id = harness.create_doc("Status Before").await?;

        let config = test_config(harness.temp_dir.path());
        let mut fuse_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));

        bootstrap_livetree(&mut fuse_ctx).await?;

        let json_path = config.root_path.join(format!("{doc_id}.json"));
        let bytes = tokio::fs::read(&json_path).await?;
        let mut parsed_doc: Doc = serde_json::from_slice(&bytes)?;
        parsed_doc.facets.insert(
            FacetKey::from(WellKnownFacetTag::TitleGeneric),
            serde_json::to_value(WellKnownFacet::TitleGeneric("Status After".to_string()))?,
        );
        tokio::fs::write(&json_path, serde_json::to_vec_pretty(&parsed_doc)?).await?;

        let status_report = status(&mut fuse_ctx).await?;
        assert_eq!(status_report.diverged_count, 1);

        harness.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pull_second_run_is_incremental_noop() -> Res<()> {
        let harness = TestHarness::new().await?;
        harness.create_doc("Incremental Pull").await?;

        let config = test_config(harness.temp_dir.path());

        let mut first_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));
        let first_report = pull_changes(&mut first_ctx).await?;
        assert!(first_report.provider_delta_count > 0);

        let mut second_ctx = DaybookFuseCtx::new(config.clone(), Arc::clone(&harness.drawer_repo));
        let second_report = pull_changes(&mut second_ctx).await?;
        assert_eq!(second_report.provider_delta_count, 0);
        assert_eq!(second_report.effect_count, 0);

        harness.shutdown().await?;
        Ok(())
    }
}
