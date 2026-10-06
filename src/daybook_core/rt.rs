use crate::interlude::*;

use crate::config::ConfigRepo;
use crate::index::{DocFacetRefIndexRepo, DocFacetSetIndexRepo};
use crate::local_state::SqliteLocalStateRepo;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use crate::repo::RepoCtx;

use daybook_types::manifest;

use wash_runtime::{
    host::{Host as WashHost, HostApi},
    types::Component,
    wit::WitInterface,
};
use wflow::{
    wflow_core::partition::{
        RetryPolicy,
        job_events::{JobError, JobRunResult},
        log::PartitionLogEntry,
    },
    wflow_tokio::partition::{
        PartitionLogRef, TokioPartitionWorkerHandle, state::PartitionWorkingState,
    },
};

pub mod dispatch;
pub mod init;
pub mod triage;
pub mod wash_plugin;

use dispatch::{
    ActiveDispatch, ActiveDispatchArgs, ActiveDispatchDeets, DispatchOnSuccessHook, DispatchRepo,
    FacetRoutineArgs, facet_routine_args_fingerprint,
};
use init::InitRepo;
use wash_plugin::stateless_view;

pub const PROCESSOR_RUNLOG_PARTITION_ID: &str = "processor-runlog/v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessorRunlogDone {
    pub done_by_peer_id: String,
    pub done_token: String,
    pub done_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RenderedFacetView {
    pub plug_id: String,
    pub view_key: String,
    pub view_json: String,
    pub plugin_state_json: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedStatelessViewProvider {
    plug_id: String,
    view_key: String,
    plug_manifest: Arc<manifest::PlugManifest>,
    view_manifest: Arc<manifest::ViewManifest>,
}

pub struct RtConfig {
    pub device_id: String,
    pub startup_progress_task_id: Option<String>,
}

pub struct Rt {
    pub config: RtConfig,
    pub rcx: Arc<RepoCtx>,
    pub cancel_token: tokio_util::sync::CancellationToken,
    pub plugs_repo: Arc<PlugsRepo>,
    pub drawer: Arc<DrawerRepo>,
    pub config_repo: Arc<ConfigRepo>,
    pub wflow_ingress: Arc<dyn wflow::WflowIngress>,
    pub dispatch_repo: Arc<dispatch::DispatchRepo>,
    pub init_repo: Arc<InitRepo>,
    pub progress_repo: Arc<crate::progress::ProgressRepo>,
    pub wflow_part_state: Arc<PartitionWorkingState>,
    pub wcx: wflow::Ctx,
    pub wash_host: Arc<WashHost>,
    pub wflow_plugin: Arc<wash_plugin_wflow::WflowPlugin>,
    pub daybook_plugin: Arc<wash_plugin::DaybookPlugin>,
    pub stateless_view_plugin: Arc<wash_plugin::StatelessViewPlugin>,
    pub sqlite_plugin: Arc<wash_plugin_sqlite::SqlPlugin>,
    pub blobs_repo: Arc<BlobsRepo>,
    pub doc_facet_set_index_repo: Arc<DocFacetSetIndexRepo>,
    pub doc_facet_ref_index_repo: Arc<DocFacetRefIndexRepo>,
    pub sqlite_local_state_repo: Arc<SqliteLocalStateRepo>,
    local_wflow_part_id: String,
}

pub struct RtStopToken {
    wflow_part_handle: TokioPartitionWorkerHandle,
    rt: Arc<Rt>,
    partition_watcher: tokio::task::JoinHandle<()>,
    doc_processor_stop: crate::rt::triage::DocProcessorStopToken,
    blob_pin_worker_stop: crate::repos::RepoStopToken,
    blob_pins_part_worker_stop: crate::repos::RepoStopToken,
    doc_facet_set_index_stop: crate::repos::RepoStopToken,
    plugs_config_consumer_stop: crate::repos::RepoStopToken,
    plugs_manifest_consumer_stop: crate::repos::RepoStopToken,
    doc_facet_ref_index_stop: crate::repos::RepoStopToken,
}

impl RtStopToken {
    pub async fn stop(self) -> Res<()> {
        self.rt.cancel_token.cancel();

        utils_rs::wait_on_handle_with_timeout(self.partition_watcher, Duration::from_secs(10))
            .await?;
        self.doc_processor_stop.stop().await?;

        self.plugs_manifest_consumer_stop.stop().await?;
        self.plugs_config_consumer_stop.stop().await?;

        if let Err(err) = self.doc_facet_set_index_stop.stop().await {
            warn!(
                ?err,
                "error stopping doc_facet_set_index_repo during shutdown - continuing"
            );
        }

        if let Err(err) = self.doc_facet_ref_index_stop.stop().await {
            warn!(
                ?err,
                "error stopping doc_facet_ref_index_repo during shutdown - continuing"
            );
        }
        if let Err(err) = self.blob_pins_part_worker_stop.stop().await {
            warn!(
                ?err,
                "error stopping blob_pins_part_worker during shutdown - continuing"
            );
        }
        if let Err(err) = self.blob_pin_worker_stop.stop().await {
            warn!(
                ?err,
                "error stopping blob_pin_worker during shutdown - continuing"
            );
        }

        // Stop wflow partition worker
        if let Err(err) = self.wflow_part_handle.stop().await {
            warn!(
                ?err,
                "error stopping wflow_part_handle during shutdown - continuing"
            );
        }

        if let Err(err) = Arc::clone(&self.rt.wash_host).stop().await.to_eyre() {
            warn!(
                ?err,
                "error stopping wash_host during shutdown - continuing"
            );
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum DispatchArgs {
    DocInvoke {
        doc_id: String,
        branch_path: daybook_types::doc::BranchPathBuf,
        heads: ChangeHashSet,
    },
    DocFacet {
        doc_id: String,
        branch_path: daybook_types::doc::BranchPathBuf,
        heads: ChangeHashSet,
        facet_key: Option<String>,
    },
    DocRoutine {
        doc_id: String,
        branch_path: daybook_types::doc::BranchPathBuf,
        heads: ChangeHashSet,
        invocation: dispatch::RoutineInvocation,
        changed_facet_keys: Vec<String>,
        wflow_args_json: Option<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum InvokeCommandFromWflowError {
    #[error("{0}")]
    Denied(String),
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

impl Rt {
    async fn emit_startup_progress_status(
        progress_repo: &Arc<crate::progress::ProgressRepo>,
        startup_progress_task_id: Option<&str>,
        message: String,
    ) -> Res<()> {
        let Some(task_id) = startup_progress_task_id else {
            return Ok(());
        };
        progress_repo
            .add_update(
                task_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: Some("App startup".to_string()),
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message,
                    },
                },
            )
            .await
    }

    fn startup_timing_note(
        stage_started: std::time::Instant,
        total_started: std::time::Instant,
    ) -> String {
        let stage_ms = stage_started.elapsed().as_millis();
        let total_ms = total_started.elapsed().as_millis();
        let from_app_start = format!(" from_app_start_ms={}", utils_rs::app_startup_elapsed_ms());
        format!("stage_ms={stage_ms} total_ms={total_ms}{from_app_start}")
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    #[expect(clippy::too_many_arguments)]
    pub async fn boot(
        config: RtConfig,
        rcx: Arc<RepoCtx>,
        drawer: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        dispatch_repo: Arc<DispatchRepo>,
        progress_repo: Arc<crate::progress::ProgressRepo>,
        blobs_repo: Arc<BlobsRepo>,
        config_repo: Arc<ConfigRepo>,
        init_repo: Arc<InitRepo>,
        sqlite_local_state_repo: Arc<SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, RtStopToken)> {
        let total_started = std::time::Instant::now();
        let startup_progress_task_id = config.startup_progress_task_id.clone();
        // One cancel token for the whole Rt, created up front so workers
        // booted before the Rt struct is constructed still share it.
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let authority = crate::authority::ensure(&rcx.big_repo, &rcx.sql, None).await?;
        crate::repo::ensure_authority_partitions(
            &rcx.part_store,
            &authority,
            &rcx.core_inventory_doc_id,
            &rcx.docs_inventory_doc_id,
        )
        .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: ensured partitions".to_string(),
        )
        .await?;

        let wcx = wflow::Ctx::init(Some(rcx.layout.repo_root.join("wflows.db"))).await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: initialized wflow ctx".to_string(),
        )
        .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: init repo and local-state repos ready".to_string(),
        )
        .await?;

        let stage_started = std::time::Instant::now();
        let (doc_facet_set_index_repo, doc_facet_set_index_stop) =
            crate::index::DocFacetSetIndexRepo::boot(
                Arc::clone(&sqlite_local_state_repo),
                Arc::clone(&drawer),
                Arc::clone(&rcx.frontier_part_store),
                cancel_token.clone(),
            )
            .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded facet-set index ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;
        let stage_started = std::time::Instant::now();
        let blob_pins_part_worker_stop = crate::blobs::spawn_blob_pins_part_worker(
            Arc::clone(&rcx.blob_part_store),
            Arc::clone(&sqlite_local_state_repo),
            Arc::clone(&drawer),
            doc_facet_set_index_repo.revision_store(),
            cancel_token.clone(),
        )
        .await?;
        let blob_pin_worker_stop = crate::blobs::spawn_blob_pin_worker(
            Arc::clone(&drawer),
            rcx.sql.clone(),
            rcx.core_inventory_doc_id.clone(),
            rcx.docs_inventory_doc_id.clone(),
            doc_facet_set_index_repo.revision_store(),
            Arc::clone(&plugs_repo),
            cancel_token.clone(),
        )
        .await?;
        // The blob-inventory access rows (ADR 013) belong to whoever serves those parts to
        // peers, so `IrohSyncRepo::boot` owns that writer, not this runtime.
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: blob pin workers started ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;
        let stage_started = std::time::Instant::now();
        let (doc_facet_ref_index_repo, doc_facet_ref_index_stop) =
            crate::index::DocFacetRefIndexRepo::boot(
                Arc::clone(&drawer),
                Arc::clone(&plugs_repo),
                Arc::clone(&sqlite_local_state_repo),
                doc_facet_set_index_repo.revision_store(),
                cancel_token.clone(),
            )
            .await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: loaded facet-ref index ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;

        let wflow_plugin = Arc::new(wash_plugin_wflow::WflowPlugin::new(Arc::clone(
            &wcx.metastore,
        )));
        let daybook_plugin = Arc::new(wash_plugin::DaybookPlugin::new(
            Arc::clone(&drawer),
            Arc::clone(&dispatch_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
            Arc::clone(&config_repo),
            Arc::clone(&plugs_repo),
        ));
        let stateless_view_plugin = Arc::new(wash_plugin::StatelessViewPlugin::new());
        let sqlite_plugin = Arc::new(wash_plugin_sqlite::SqlPlugin::new());
        let wash_host = wflow::build_wash_host(vec![
            #[expect(clippy::clone_on_ref_ptr)]
            wflow_plugin.clone(),
            #[expect(clippy::clone_on_ref_ptr)]
            daybook_plugin.clone(),
            #[expect(clippy::clone_on_ref_ptr)]
            stateless_view_plugin.clone(),
            #[expect(clippy::clone_on_ref_ptr)]
            sqlite_plugin.clone(),
        ])
        .await?;

        let wash_host = wash_host
            .start()
            .await
            .to_eyre()
            .wrap_err("error starting wash host")?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: wash host started".to_string(),
        )
        .await?;

        let mut bundles_to_load: HashSet<(String, String)> = default();
        for (_dispatch_id, dispach) in dispatch_repo.list().await {
            match &dispach.deets {
                ActiveDispatchDeets::Wflow {
                    plug_id,
                    bundle_name,
                    ..
                } => {
                    bundles_to_load.insert((plug_id.clone(), bundle_name.clone()));
                }
            }
        }
        for (plug_id, bundle_name) in bundles_to_load {
            let plug_id_for_log = plug_id.clone();
            let bundle_name_for_log = bundle_name.clone();
            let plug_man = plugs_repo.get(&plug_id).await.ok_or_else(|| {
                ferr!("plug with active dispatch not found in repo: plug={plug_id} bundle={bundle_name}")
            })?;
            let bundle_man = plug_man.wflow_bundles.get(&bundle_name[..]).ok_or_else(|| {
                ferr!("bundle with active dispatch not found in repo: plug={plug_id} bundle={bundle_name}")
            })?;

            let _workload_id = ensure_bundle_workload_running(
                &wcx,
                &wash_host,
                &blobs_repo,
                plug_id,
                bundle_name,
                bundle_man,
            )
            .await?;
            Self::emit_startup_progress_status(
                &progress_repo,
                startup_progress_task_id.as_deref(),
                format!(
                    "rt boot: resumed workload plug={plug_id_for_log} bundle={bundle_name_for_log}"
                ),
            )
            .await?;
        }

        let part_idx = 0;
        let (wflow_part_handle, wflow_part_state) =
            wflow::start_partition_worker(&wcx, Arc::clone(&wflow_plugin), part_idx).await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: partition worker started".to_string(),
        )
        .await?;
        let part_log = PartitionLogRef::new(Arc::clone(&wcx.logstore));
        let wflow_ingress = Arc::new(wflow::ingress::PartitionLogIngress::new(
            part_log,
            Arc::clone(&wcx.metastore),
        ));
        let local_wflow_part_id = format!("{}/{part_idx}", config.device_id);

        let rt = Arc::new(Self {
            config,
            local_wflow_part_id,
            cancel_token,
            plugs_repo,
            drawer,
            rcx,
            wflow_ingress,
            dispatch_repo,
            init_repo,
            progress_repo,
            wcx,
            wash_host,
            wflow_plugin,
            daybook_plugin,
            stateless_view_plugin,
            sqlite_plugin,
            blobs_repo,
            doc_facet_set_index_repo: Arc::clone(&doc_facet_set_index_repo),
            doc_facet_ref_index_repo: Arc::clone(&doc_facet_ref_index_repo),
            sqlite_local_state_repo: Arc::clone(&sqlite_local_state_repo),
            config_repo,
            wflow_part_state,
        });
        rt.daybook_plugin.attach_rt(Arc::downgrade(&rt));

        let plugs_config_sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(crate::plugs::PLUGS_CONFIG_CONSUMER_STATE_ID)
            .await?;
        let plugs_config_consumer_stop = crate::plugs::spawn_plugs_config_consumer(
            rt.doc_facet_set_index_repo.revision_store(),
            Arc::clone(&rt.drawer),
            Arc::clone(&rt.plugs_repo),
            plugs_config_sql,
            rt.cancel_token.clone(),
        )
        .await?;
        let plugs_manifest_sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(crate::plugs::PLUG_MANIFEST_CONSUMER_STATE_ID)
            .await?;
        let plugs_manifest_consumer_stop = crate::plugs::spawn_facet_set_plugs_manifest_consumer(
            rt.doc_facet_set_index_repo.revision_store(),
            Arc::clone(&rt.plugs_repo),
            plugs_manifest_sql,
            rt.cancel_token.clone(),
        )
        .await?;
        // Ensure init routines are queued at boot according to each init run mode.
        // ADR 007 §6: boot queues inits for the active set only.
        let mut plug_ids = rt
            .plugs_repo
            .list_active_plugs()
            .await
            .into_iter()
            .map(|plug| plug.id())
            .collect::<Vec<_>>();
        plug_ids.sort();
        let stage_started = std::time::Instant::now();
        for plug_id in plug_ids {
            rt.ensure_plug_init_dispatches(
                &plug_id,
                startup_progress_task_id.as_deref(),
                Some(total_started),
            )
            .await?;
        }
        Self::emit_startup_progress_status(
            &rt.progress_repo,
            startup_progress_task_id.as_deref(),
            format!(
                "rt boot: plug init queue complete ({})",
                Self::startup_timing_note(stage_started, total_started)
            ),
        )
        .await?;

        let doc_processor_stop = crate::rt::triage::spawn_doc_processor_driver(
            Arc::clone(&rt),
            rt.doc_facet_set_index_repo.revision_store(),
            Arc::clone(&rt.plugs_repo),
            rt.cancel_token.clone(),
        )
        .await?;

        let partition_watcher = tokio::spawn({
            let repo = Arc::clone(&rt);
            async move { repo.keep_up_with_partition().await.unwrap() }
        });

        Ok((
            Arc::clone(&rt),
            RtStopToken {
                rt,
                partition_watcher,
                doc_processor_stop,
                blob_pin_worker_stop,
                blob_pins_part_worker_stop,
                doc_facet_set_index_stop,
                plugs_config_consumer_stop,
                plugs_manifest_consumer_stop,
                doc_facet_ref_index_stop,
                wflow_part_handle,
            },
        ))
    }
    pub fn processor_runlog_item_id(doc_id: &str, processor_full_id: &str) -> ObjKey {
        let bytes = format!("v1|doc:{doc_id}|proc:{processor_full_id}");
        let digest = blake3::hash(bytes.as_bytes());
        ObjKey::new(*digest.as_bytes())
    }

    pub async fn get_processor_runlog_done(
        &self,
        doc_id: &str,
        processor_full_id: &str,
    ) -> Res<Option<ProcessorRunlogDone>> {
        let item_id = Self::processor_runlog_item_id(doc_id, processor_full_id);
        let payload = self.rcx.derived_part_store.obj_payload(item_id).await?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let done = serde_json::from_value::<ProcessorRunlogDone>(payload)?;
        Ok(Some(done))
    }

    pub async fn render_facet_view(
        &self,
        doc_id: &str,
        branch_path: &daybook_types::doc::BranchPathBuf,
        facet_key: &daybook_types::doc::FacetKey,
        requested_view: Option<manifest::ViewRef>,
        ui_state_json: Option<String>,
    ) -> Res<RenderedFacetView> {
        let doc_id = doc_id.to_string();
        let view = self
            .resolve_stateless_view_provider(facet_key, requested_view)
            .await?;

        let view_bundle = match &view.view_manifest.provider {
            manifest::ViewProviderManifest::StatelessWasm { bundle, export } => {
                eyre::ensure!(
                    export.as_str() == "render-facet-view",
                    "stateless view provider '{}' in plug '{}' uses unsupported export '{}'",
                    view.view_key,
                    view.plug_id,
                    export
                );
                bundle.to_string()
            }
        };
        let bundle_man = view
            .plug_manifest
            .wflow_bundles
            .get(view_bundle.as_str())
            .ok_or_else(|| {
                ferr!(
                    "stateless view provider bundle '{}' not found in plug '{}'",
                    view_bundle,
                    view.plug_id
                )
            })?;

        async {
            let bundle_components = load_bundle_components(&self.blobs_repo, bundle_man).await?;
            let component_name = format!("stateless-view-{}", view.view_key);
            let workload_id: Arc<str> = Arc::from(format!(
                "stateless-view/{}/{}/{}",
                view.plug_id,
                view.view_key,
                uuid::Uuid::new_v4()
            ));
            let engine = wash_runtime::engine::Engine::builder()
                .build()
                .map_err(|err| eyre::eyre!(err.to_string()))?;
            let components = bundle_components
                .into_iter()
                .enumerate()
                .map(|(component_idx, mut component)| {
                    component.name = format!("{component_name}-{component_idx}");
                    component
                })
                .collect::<Vec<_>>();
            let unresolved_workload = engine
                .initialize_workload(
                    Arc::clone(&workload_id),
                    wash_runtime::types::Workload {
                        namespace: view.plug_manifest.namespace.clone(),
                        name: format!("{}-{}", view.view_key, facet_key),
                        annotations: HashMap::new(),
                        service: None,
                        components,
                        host_interfaces: stateless_view_host_interfaces(),
                        volumes: vec![],
                    },
                )
                .map_err(|err| eyre::eyre!(err.to_string()))?;

            let plugins: HashMap<&'static str, Arc<dyn wash_runtime::plugin::HostPlugin>> =
                HashMap::from([
                    (
                        wash_plugin::DaybookPlugin::ID,
                        Arc::clone(&self.daybook_plugin)
                            as Arc<dyn wash_runtime::plugin::HostPlugin>,
                    ),
                    (
                        wash_plugin::StatelessViewPlugin::ID,
                        Arc::clone(&self.stateless_view_plugin)
                            as Arc<dyn wash_runtime::plugin::HostPlugin>,
                    ),
                    (
                        wash_plugin_sqlite::SqlPlugin::ID,
                        Arc::clone(&self.sqlite_plugin)
                            as Arc<dyn wash_runtime::plugin::HostPlugin>,
                    ),
                ]);

            let resolved_workload = unresolved_workload
                .resolve(
                    Some(&plugins),
                    Arc::new(wash_runtime::host::http::NullServer::default()),
                )
                .await
                .map_err(|err| eyre::eyre!(err.to_string()))?;

            let Some(first_component_id) = resolved_workload
                .components()
                .read()
                .await
                .keys()
                .next()
                .map(|component_id| component_id.to_string())
            else {
                return Err(ferr!(
                    "stateless view bundle for '{}' did not contain any components",
                    view.view_key
                ));
            };

            let Some((doc, heads)) = self
                .drawer
                .get_with_heads(&doc_id, branch_path, None)
                .await
                .map_err(|err| eyre::eyre!("{err}"))?
            else {
                return Err(ferr!(
                    "doc '{}' not found at branch '{}'",
                    doc_id,
                    branch_path
                ));
            };
            eyre::ensure!(
                doc.facets.contains_key(facet_key),
                "facet '{}' not found in doc '{}'",
                facet_key,
                doc_id
            );

            let mut store = resolved_workload
                .new_store(&first_component_id)
                .await
                .map_err(|err| eyre::eyre!(err.to_string()))?;
            let target_facet_acl = manifest::RoutineFacetAccess {
                owner_plug_id: None,
                tag: facet_key.tag.to_string().into(),
                key_id: Some(facet_key.id.clone()),
                read: true,
                write: false,
                create: false,
                delete: false,
            };
            let doc_tokens = dispatch::DocFacetTokens {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                staging_branch_path: branch_path.clone(),
                heads,
                facet_acl: vec![target_facet_acl],
            };
            let primary_doc = wash_plugin::build_doc_facet_tokens(
                store.data_mut(),
                &self.daybook_plugin,
                &doc_tokens,
            )
            .await
            .map_err(|err| eyre::eyre!("error building doc facet tokens: {err}"))?;

            let args = stateless_view::RenderFacetViewArgs {
                view_key: view.view_key.clone(),
                target_facet_key: facet_key.to_string(),
                primary_doc,
                config_docs: vec![],
                ui_state_json,
            };

            let instance_pre = resolved_workload
                .instantiate_pre(&first_component_id)
                .await
                .map_err(|err| eyre::eyre!(err.to_string()))?;
            let instance_pre = wash_plugin::AllGuestPre::new(instance_pre).map_err(|err| {
                eyre::eyre!("error pre instantiating stateless view component: {err}")
            })?;
            let instance = instance_pre
                .instantiate_async(&mut store)
                .await
                .map_err(|err| eyre::eyre!(err.to_string()))?;
            let response = instance
                .townframe_daybook_stateless_view()
                .call_render_facet_view(&mut store, &args)
                .await
                .map_err(|err| eyre::eyre!("error rendering stateless view: {err}"))?;
            let response = response.map_err(|err| match err {
                stateless_view::RenderViewError::InvalidRequest(msg) => {
                    eyre::eyre!("stateless view rejected request: {msg}")
                }
                stateless_view::RenderViewError::Denied(msg) => {
                    eyre::eyre!("stateless view denied request: {msg}")
                }
                stateless_view::RenderViewError::InvalidView(msg) => {
                    eyre::eyre!("stateless view reported invalid view: {msg}")
                }
                stateless_view::RenderViewError::Other(msg) => {
                    eyre::eyre!("stateless view error: {msg}")
                }
            })?;
            let view_spec =
                serde_json::from_str::<daybook_types::view::ViewSpec>(&response.view_json)
                    .wrap_err("stateless view returned invalid ViewSpec JSON")?;
            view_spec
                .validate()
                .wrap_err("stateless view returned invalid ViewSpec shape")?;

            Ok(RenderedFacetView {
                plug_id: view.plug_id,
                view_key: view.view_key,
                view_json: response.view_json,
                plugin_state_json: response.plugin_state_json,
            })
        }
        .await
    }

    async fn resolve_stateless_view_provider(
        &self,
        facet_key: &daybook_types::doc::FacetKey,
        requested_view: Option<manifest::ViewRef>,
    ) -> Res<ResolvedStatelessViewProvider> {
        let facet_tag = facet_key.tag.to_string();
        let (view_ref, owner_plug_id) = if let Some(view_ref) = requested_view {
            let owner_plug_id = if let Some(plug_id) = view_ref.plug_id.clone() {
                plug_id
            } else {
                self.plugs_repo
                    .get_owner_plug_id_by_facet_tag(&facet_tag)
                    .await
                    .ok_or_else(|| {
                        ferr!(
                            "facet '{}' has no owning plug for local view resolution",
                            facet_tag
                        )
                    })?
            };
            (view_ref, owner_plug_id)
        } else {
            let facet_manifest = match self.plugs_repo.get_facet_manifest_by_tag(&facet_tag).await {
                crate::plugs::FacetManifestLookup::Found(facet_manifest) => facet_manifest,
                crate::plugs::FacetManifestLookup::PlugDisabled { plug_id } => {
                    return Err(ferr!(
                        "facet '{}' is owned by disabled plug '{}'",
                        facet_tag,
                        plug_id
                    ));
                }
                crate::plugs::FacetManifestLookup::UnknownTag => {
                    return Err(ferr!("facet manifest not found for tag '{}'", facet_tag));
                }
            };
            match facet_manifest.display_config.deets {
                manifest::FacetDisplayDeets::CustomView { view, .. } => {
                    let owner_plug_id = self
                        .plugs_repo
                        .get_owner_plug_id_by_facet_tag(&facet_tag)
                        .await
                        .ok_or_else(|| {
                            ferr!(
                                "facet '{}' has no owning plug for local view resolution",
                                facet_tag
                            )
                        })?;
                    (view, owner_plug_id)
                }
                _ => {
                    return Err(ferr!(
                        "facet '{}' does not declare a custom view and no explicit view was provided",
                        facet_tag
                    ));
                }
            }
        };

        let plug_id = view_ref
            .plug_id
            .clone()
            .unwrap_or_else(|| owner_plug_id.clone());
        let plug_manifest = self
            .plugs_repo
            .get(&plug_id)
            .await
            .ok_or_else(|| ferr!("view provider plug '{}' not found", plug_id))?;
        let view_manifest = plug_manifest
            .views
            .get(view_ref.view_key.as_str())
            .cloned()
            .ok_or_else(|| {
                ferr!(
                    "view '{}' not found in view provider plug '{}'",
                    view_ref.view_key,
                    plug_id
                )
            })?;

        Ok(ResolvedStatelessViewProvider {
            plug_id,
            view_key: view_ref.view_key.to_string(),
            plug_manifest,
            view_manifest,
        })
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(worker = "rt-partition-watcher")
    )]
    async fn keep_up_with_partition(&self) -> Res<()> {
        use futures::StreamExt;

        // let dispatch = self
        //     .dispatch_repo
        //     .get(dispatch_id)
        //     .await
        //     .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        //
        // match &dispatch.deets {
        //     ActiveDispatchDeets::Wflow {
        //         entry_id,
        //         wflow_job_id,
        //         ..
        //     } => {}
        // }
        let part_log = PartitionLogRef::new(Arc::clone(&self.wcx.logstore));
        let last_seen_idx = self
            .dispatch_repo
            .get_wflow_part_frontier(&self.local_wflow_part_id)
            .await
            .unwrap_or(0);
        let mut stream = part_log.tail(last_seen_idx);

        loop {
            let entry = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    break;
                }
                entry = stream.next() => {
                    entry
                }
            };
            let Some(entry) = entry else {
                warn!("log stream closed");
                // Stream ended
                break;
            };
            let (idx, entry) = entry?;
            if let Some(entry) = entry
                && let Err(err) = self.handle_wflow_entry(idx, entry).await
            {
                if self.cancel_token.is_cancelled() {
                    debug!(error = %err, "ignoring wflow entry error during shutdown");
                    break;
                }
                return Err(err);
            };
            if let Err(err) = self
                .dispatch_repo
                .set_wflow_part_frontier(self.local_wflow_part_id.clone(), idx)
                .await
            {
                if self.cancel_token.is_cancelled() {
                    debug!(error = %err, "ignoring frontier write error during shutdown");
                    break;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    fn ensure_rt_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("rt is shutting down");
        }
        Ok(())
    }

    async fn collect_plug_and_dependency_order(&self, plug_id: &str) -> Res<Vec<String>> {
        fn dep_base_id(dep_id_full: &str) -> Res<String> {
            if dep_id_full.starts_with('@') {
                let without_prefix = dep_id_full
                    .strip_prefix('@')
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                let base = without_prefix
                    .split('@')
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                Ok(format!("@{base}"))
            } else {
                let base = dep_id_full
                    .split('@')
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| eyre::eyre!("invalid dependency id: {dep_id_full}"))?;
                Ok(base.to_string())
            }
        }

        let mut permanent = HashSet::new();
        let mut temporary = HashSet::new();
        let mut out = vec![];
        let mut stack: Vec<(String, bool)> = vec![(plug_id.to_string(), false)];
        while let Some((cur, expanded)) = stack.pop() {
            if permanent.contains(&cur) {
                continue;
            }
            if expanded {
                temporary.remove(&cur);
                permanent.insert(cur.clone());
                out.push(cur);
                continue;
            }
            if !temporary.insert(cur.clone()) {
                eyre::bail!("circular plug dependencies detected around {cur}");
            }
            stack.push((cur.clone(), true));
            let plug = self
                .plugs_repo
                .get(&cur)
                .await
                .ok_or_else(|| ferr!("plug not found in repo: {cur}"))?;
            let mut deps = plug
                .dependencies
                .keys()
                .map(|raw| dep_base_id(raw))
                .collect::<Res<Vec<_>>>()?;
            deps.sort();
            for dep in deps.into_iter().rev() {
                if !permanent.contains(&dep) {
                    stack.push((dep, false));
                }
            }
        }
        Ok(out)
    }

    async fn ensure_plug_init_dispatches(
        &self,
        plug_id: &str,
        startup_progress_task_id: Option<&str>,
        total_started: Option<std::time::Instant>,
    ) -> Res<Vec<String>> {
        let stage_started = std::time::Instant::now();
        let order = self.collect_plug_and_dependency_order(plug_id).await?;
        let mut unresolved_init_dispatch_ids = vec![];
        for plug_id in order {
            let plug = self
                .plugs_repo
                .get(&plug_id)
                .await
                .ok_or_else(|| ferr!("plug not found in repo: {plug_id}"))?;
            let mut init_keys = plug.inits.keys().cloned().collect::<Vec<_>>();
            init_keys.sort();

            for init_key in init_keys {
                let init_manifest = plug.inits.get(&init_key).ok_or_else(|| {
                    ferr!("init not found in plug manifest: {plug_id}/{init_key}")
                })?;
                let init_id = init::InitRepo::init_id(&plug_id, &plug.version, &init_key.0);
                if self
                    .init_repo
                    .is_done(&init_manifest.run_mode, &init_id)
                    .await?
                {
                    self.init_repo
                        .report_boot_init_stage(
                            &init_manifest.run_mode,
                            &plug_id,
                            &init_key.0,
                            "already done",
                            init::BootInitProgressContext {
                                startup_progress_task_id_override: startup_progress_task_id
                                    .map(str::to_owned),
                                stage_started,
                                total_started,
                            },
                        )
                        .await?;
                    continue;
                }
                if let Some(running_dispatch_id) =
                    self.init_repo.get_running_dispatch(&init_id).await
                {
                    unresolved_init_dispatch_ids.push(running_dispatch_id);
                    self.init_repo
                        .report_boot_init_stage(
                            &init_manifest.run_mode,
                            &plug_id,
                            &init_key.0,
                            "running",
                            init::BootInitProgressContext {
                                startup_progress_task_id_override: startup_progress_task_id
                                    .map(str::to_owned),
                                stage_started,
                                total_started,
                            },
                        )
                        .await?;
                    continue;
                }
                let daybook_types::manifest::InitDeets::InvokeRoutine { routine_name } =
                    &init_manifest.deets;
                // ADR 007 §2: the config doc is created at enablement and the
                // mapping retained across disablement; an enabled plug always
                // has one.
                let config_doc_id = self
                    .plugs_repo
                    .get_plug_config_doc_id(&plug_id)
                    .await
                    .ok_or_eyre(format!(
                        "plug {plug_id} has no config doc; expected one at enablement"
                    ))?;
                let config_heads = self
                    .drawer
                    .get_doc_branches(&config_doc_id)
                    .await?
                    .and_then(|doc| doc.branches.get("main").cloned())
                    .ok_or_else(|| ferr!("config doc missing main branch for plug {plug_id}"))?;
                let dispatch_id = self
                    .dispatch_no_gate(
                        &plug_id,
                        &routine_name.0,
                        DispatchArgs::DocRoutine {
                            doc_id: config_doc_id,
                            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                            heads: config_heads,
                            invocation: dispatch::RoutineInvocation::Command,
                            changed_facet_keys: vec![],
                            wflow_args_json: None,
                        },
                        vec![DispatchOnSuccessHook::InitMarkDone {
                            init_id: init_id.clone(),
                            run_mode: init_manifest.run_mode.clone(),
                        }],
                        unresolved_init_dispatch_ids.clone(),
                    )
                    .await?;
                self.init_repo
                    .set_running_dispatch(&init_id, &dispatch_id)
                    .await?;
                unresolved_init_dispatch_ids.push(dispatch_id);
                self.init_repo
                    .report_boot_init_stage(
                        &init_manifest.run_mode,
                        &plug_id,
                        &init_key.0,
                        "queued",
                        init::BootInitProgressContext {
                            startup_progress_task_id_override: startup_progress_task_id
                                .map(str::to_owned),
                            stage_started,
                            total_started,
                        },
                    )
                    .await?;
            }
        }
        Ok(unresolved_init_dispatch_ids)
    }
    #[tracing::instrument(skip(self, entry))]
    async fn handle_wflow_entry(&self, entry_id: u64, entry: PartitionLogEntry) -> Res<()> {
        let PartitionLogEntry::JobEffectResult(event) = entry else {
            return Ok(());
        };
        self.handle_wflow_result(entry_id, &event.job_id, &event.result)
            .await
    }

    async fn handle_wflow_result(
        &self,
        entry_id: u64,
        job_id: &str,
        result: &JobRunResult,
    ) -> Res<()> {
        let terminal_effect;
        let result = if let JobRunResult::StepEffect(effect) = result
            && let wflow::wflow_core::partition::job_events::JobEffectResultDeets::EffectErr(
                error @ JobError::Terminal { .. },
            ) = &effect.deets
        {
            terminal_effect = JobRunResult::WflowErr(error.clone());
            &terminal_effect
        } else {
            result
        };
        let Some((dispatch_id, _)) = job_id.rsplit_once('-') else {
            return Ok(());
        };
        {
            let Some(dispatch) = self.dispatch_repo.get_active(dispatch_id).await else {
                return Ok(());
            };
            let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &dispatch.deets;
            let Some(wflow_job_id) = wflow_job_id.as_ref() else {
                return Ok(());
            };
            if wflow_job_id.as_str() != job_id {
                debug!(
                    %dispatch_id,
                    active_job_id = %wflow_job_id,
                    completed_job_id = %job_id,
                    wflow_key = %dispatch.execution.wflow().key,
                    "ignoring stale job result for replaced dispatch"
                );
                return Ok(());
            }
        }

        let is_done = match result {
            JobRunResult::Success { value_json } => {
                info!(?value_json, "success on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Info,
                                message: "dispatch completed successfully".to_string(),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::Aborted => {
                info!("dispatch wflow aborted");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: "dispatch aborted".to_string(),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WorkerErr(err) => {
                error!(?err, "worker error on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Error,
                                message: format!("worker error: {err:?}"),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WflowErr(JobError::Terminal { error_json }) => {
                error!(?error_json, "terminal error on dispatch wflow");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Error,
                                message: format!("terminal error: {error_json}"),
                            },
                        },
                    )
                    .await?;
                true
            }
            JobRunResult::WflowErr(JobError::Transient {
                error_json,
                retry_policy: None,
            })
            | JobRunResult::WflowErr(JobError::Transient {
                error_json,
                retry_policy: Some(RetryPolicy::Immediate),
            }) => {
                warn!("transient error on dispatch wflow: {error_json:?}");
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: format!("transient error, retrying: {error_json}"),
                            },
                        },
                    )
                    .await?;
                false
            }
            JobRunResult::StepEffect(..) | JobRunResult::StepWait(..) => false,
        };
        if is_done {
            let Some(dispatch) = self.dispatch_repo.get_any(dispatch_id).await else {
                return Ok(());
            };
            // Re-read exact job identity before claiming this attempt. Replacement
            // and claim validation share the repository's short transition lock.
            if dispatch.status.is_terminal() {
                debug!(
                    %dispatch_id,
                    status = ?dispatch.status,
                    "ignoring terminal result; dispatch already settled"
                );
                return Ok(());
            }
            let ActiveDispatchDeets::Wflow {
                wflow_job_id: current_job_id,
                ..
            } = &dispatch.deets;
            if current_job_id.as_deref() != Some(job_id) {
                debug!(
                    %dispatch_id,
                    %job_id,
                    current_job_id = ?current_job_id,
                    "ignoring stale job result for replaced dispatch"
                );
                return Ok(());
            }
            let Some(finalization) = self
                .dispatch_repo
                .claim_finalization(dispatch_id, &dispatch)
                .await?
            else {
                return Ok(());
            };
            #[cfg(test)]
            if let Some(gate) = self.finalization_gate.lock().await.take() {
                gate.reached.send(()).expect(ERROR_CHANNEL);
                tokio::select! {
                    _ = self.cancel_token.cancelled() => return Ok(()),
                    result = gate.resume => result.expect(ERROR_CHANNEL),
                }
            }
            // Handle staging branch cleanup based on success/failure
            let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path: target_branch_path,
                staging_branch_path,
                ..
            }) = &dispatch.args;

            // Cancellation owns this outcome only after its durable mark commits.
            // A publication claim seals out later cancellation without waiting.
            let cancelled_win = finalization.cancelled;
            let merged_successfully =
                !cancelled_win && matches!(result, JobRunResult::Success { .. });

            if merged_successfully {
                // Merge staging branch into target branch
                info!(
                    %dispatch_id,
                    %entry_id,
                    ?doc_id,
                    ?staging_branch_path,
                    ?target_branch_path,
                    "merging staging branch into target"
                );
                match self
                    .drawer
                    .merge_from_branch(doc_id, target_branch_path, staging_branch_path, None)
                    .await
                {
                    Ok(()) => {}
                    Err(crate::drawer::types::DrawerError::BranchNotFound { name }) => {
                        debug!(
                            %dispatch_id,
                            %entry_id,
                            ?doc_id,
                            ?name,
                            ?staging_branch_path,
                            ?target_branch_path,
                            "staging branch missing during merge; wflow made no facet changes"
                        );
                    }
                    Err(err) => {
                        error!(
                            %dispatch_id,
                            %entry_id,
                            ?doc_id,
                            ?staging_branch_path,
                            ?target_branch_path,
                            ?err,
                            "staging merge returned error"
                        );
                        return Err(eyre::eyre!(err).wrap_err("error merging staging branch"));
                    }
                }

                if merged_successfully {
                    // Delete the staging branch after successful merge
                    info!(
                        %dispatch_id,
                        %entry_id,
                        ?doc_id,
                        ?staging_branch_path,
                        "deleting staging branch after successful merge"
                    );
                    self.drawer
                        .delete_branch(doc_id, staging_branch_path, None)
                        .await
                        .or_else(|err| match err {
                            crate::drawer::types::DrawerError::BranchNotFound { .. } => {
                                debug!(
                                    %dispatch_id,
                                    %entry_id,
                                    ?doc_id,
                                    ?staging_branch_path,
                                    "staging branch already removed after successful merge"
                                );
                                Ok(false)
                            }
                            other => Err(other),
                        })
                        .wrap_err("error deleting staging branch after merge")?;
                }
            }
            if !merged_successfully {
                // Delete staging branch on failure if it exists.
                info!(
                    %dispatch_id,
                    %entry_id,
                    ?doc_id,
                    ?staging_branch_path,
                    "deleting staging branch due to failure"
                );
                self.drawer
                    .delete_branch(doc_id, staging_branch_path, None)
                    .await
                    .or_else(|err| match err {
                        crate::drawer::types::DrawerError::BranchNotFound { .. } => Ok(false),
                        other => Err(other),
                    })
                    .wrap_err("error deleting staging branch")?;
                for hook in &dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, .. } => {
                            self.init_repo
                                .clear_running_dispatch(init_id, dispatch_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorSettlement { .. } => {}
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let reply = {
                                let (status, value_json, error_json) =
                                    command_invoke_reply_from_result(
                                        result,
                                        dispatch_id,
                                        cancelled_win,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
            } else {
                for hook in &dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, run_mode } => {
                            self.init_repo.mark_done(run_mode, init_id).await?;
                            self.init_repo
                                .clear_running_dispatch(init_id, dispatch_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorSettlement {
                            slot,
                            capture,
                            domain,
                        } => {
                            let store = self
                                .processor_slot_store(&slot.processor_full_id, domain.as_ref())
                                .await?
                                .ok_or_eyre(
                                    "captured processor settlement domain is not materialized",
                                )?;
                            store
                                .settle(
                                    slot,
                                    triage::slots::ProcessorSettlement {
                                        capture: capture.clone(),
                                        attempt_id: dispatch_id.to_string(),
                                    },
                                )
                                .await?;
                        }
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let reply = {
                                let (status, value_json, error_json) =
                                    command_invoke_reply_from_result(
                                        result,
                                        dispatch_id,
                                        cancelled_win,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
            }

            let final_status = if merged_successfully {
                dispatch::DispatchStatus::Succeeded
            } else if cancelled_win || matches!(result, JobRunResult::Aborted) {
                dispatch::DispatchStatus::Cancelled
            } else {
                dispatch::DispatchStatus::Failed
            };
            self.progress_repo
                .add_update(
                    dispatch_id,
                    crate::progress::ProgressUpdate {
                        at: jiff::Timestamp::now(),
                        title: None,
                        deets: crate::progress::ProgressUpdateDeets::Completed {
                            state: if matches!(final_status, dispatch::DispatchStatus::Succeeded) {
                                crate::progress::ProgressFinalState::Succeeded
                            } else if matches!(final_status, dispatch::DispatchStatus::Cancelled) {
                                crate::progress::ProgressFinalState::Cancelled
                            } else {
                                crate::progress::ProgressFinalState::Failed
                            },
                            message: None,
                        },
                    },
                )
                .await?;
            self.dispatch_repo
                .complete(dispatch_id.into(), final_status.clone(), &dispatch)
                .await?;
            finalization.finish();
            self.release_waiting_dispatches(
                dispatch_id,
                matches!(final_status, dispatch::DispatchStatus::Succeeded),
            )
            .await?;
        }
        Ok(())
    }

    async fn release_waiting_dispatches(
        &self,
        completed_dispatch_id: &str,
        is_success: bool,
    ) -> Res<()> {
        let waiting_dispatches = self
            .dispatch_repo
            .list_waiting_on(completed_dispatch_id)
            .await;
        for (waiting_id, waiting_dispatch) in waiting_dispatches {
            if !is_success {
                self.dispatch_repo.set_waiting_failed(&waiting_id).await?;
                self.progress_repo
                    .add_update(
                        &waiting_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Completed {
                                state: crate::progress::ProgressFinalState::Failed,
                                message: Some(
                                    "dependency dispatch failed; waiting dispatch cancelled"
                                        .to_string(),
                                ),
                            },
                        },
                    )
                    .await?;
                for hook in &waiting_dispatch.on_success_hooks {
                    match hook {
                        DispatchOnSuccessHook::InitMarkDone { init_id, .. } => {
                            self.init_repo
                                .clear_running_dispatch(init_id, &waiting_id)
                                .await?;
                        }
                        DispatchOnSuccessHook::ProcessorSettlement { .. } => {}
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let error_json = serde_json::json!({
                                "kind": "dependency-failed",
                                "dispatch_id": waiting_id,
                                "message": "dependency dispatch failed before command invocation could run",
                            });
                            let reply = daybook_pdk::InvokeCommandReply {
                                request_id: request_id.clone(),
                                status: daybook_pdk::InvokeCommandStatus::Failed,
                                value_json: None,
                                error_json: Some(error_json.to_string()),
                            };
                            self.wflow_ingress
                                .send_message(
                                    Arc::from(parent_wflow_job_id.as_str()),
                                    Arc::from(request_id.as_str()),
                                    serde_json::to_string(&reply).expect(ERROR_JSON),
                                )
                                .await
                                .wrap_err_with(|| {
                                    format!(
                                        "error sending failed command invoke reply to parent job {parent_wflow_job_id}"
                                    )
                                })?;
                        }
                    }
                }
                continue;
            }
            if let Some(ready_dispatch) = self
                .dispatch_repo
                .remove_waiting_dependency(&waiting_id, completed_dispatch_id)
                .await?
            {
                #[cfg(test)]
                if let Some(gate) = self.waiting_activation_gate.lock().await.take() {
                    gate.reached.send(()).expect(ERROR_CHANNEL);
                    gate.resume.await.expect(ERROR_CHANNEL);
                }
                self.start_waiting_dispatch(waiting_id, ready_dispatch)
                    .await?;
            }
        }
        Ok(())
    }

    /// Boot runs this only after the existing reducer's replay-complete fence,
    /// before exposing admission. Absence then proves a prepared attempt never
    /// appended JobInit; present active/archive state is the original job.
    async fn reconcile_retained_dispatches(&self, archived_only: bool) -> Res<()> {
        for (dispatch_id, dispatch) in self.dispatch_repo.list_unsettled().await {
            self.reconcile_retained_dispatch(dispatch_id, dispatch, archived_only)
                .await?;
        }
        if archived_only {
            return Ok(());
        }
        // A crash may also land after dependency removal but before activation,
        // or after a dependency settled but before its waiting dependents woke.
        for (dispatch_id, dispatch) in self.dispatch_repo.list_unsettled().await {
            if dispatch.status != dispatch::DispatchStatus::Waiting {
                continue;
            }
            if dispatch.waiting_on_dispatch_ids.is_empty() {
                self.start_waiting_dispatch(dispatch_id, dispatch).await?;
                continue;
            }
            for dependency in &dispatch.waiting_on_dispatch_ids {
                if let Some(receipt) = self.dispatch_repo.get_any(dependency).await
                    && receipt.status.is_terminal()
                {
                    self.release_waiting_dispatches(
                        dependency,
                        receipt.status == dispatch::DispatchStatus::Succeeded,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_retained_dispatch(
        &self,
        dispatch_id: String,
        dispatch: Arc<DispatchAttempt>,
        archived_only: bool,
    ) -> Res<()> {
        if dispatch.status != dispatch::DispatchStatus::Active {
            return Ok(());
        }
        let ActiveDispatchDeets::Wflow {
            wflow_job_id,
            entry_id,
            ..
        } = &dispatch.deets;
        let job_id = wflow_job_id
            .as_ref()
            .ok_or_else(|| ferr!("active dispatch {dispatch_id} has no job identity"))?;
        let retained = {
            let jobs = self.wflow_part_state.read_jobs().await;
            let (job, archived) = if let Some(job) = jobs.active.get(job_id.as_str()) {
                (Some(job), false)
            } else {
                (jobs.archive.get(job_id.as_str()), true)
            };
            job.map(|job| {
                assert_eq!(
                    serde_json::to_value(&job.wflow).expect(ERROR_JSON),
                    serde_json::to_value(dispatch.execution.wflow()).expect(ERROR_JSON),
                    "retained workflow disagrees with captured dispatch {dispatch_id}"
                );
                let ActiveDispatchArgs::FacetRoutine(args) = &dispatch.args;
                assert_eq!(
                    job.init_args_json.as_ref(),
                    args.wflow_args_json.as_deref().unwrap_or("null"),
                    "retained arguments disagree with captured dispatch {dispatch_id}"
                );
                assert!(job.init_entry_id > 0 && job.last_event_entry_id >= job.init_entry_id);
                let terminal = archived.then(|| {
                    if job.cancelling {
                        JobRunResult::Aborted
                    } else {
                        job.runs
                            .last()
                            .expect("archived job must retain its terminal run")
                            .result
                            .clone()
                    }
                });
                (job.init_entry_id, job.last_event_entry_id, terminal)
            })
        };
        if archived_only && !matches!(&retained, Some((_, _, Some(_)))) {
            return Ok(());
        }
        if let Some((init_entry_id, last_event_entry_id, terminal)) = retained {
            if let Some(recorded) = entry_id {
                assert_eq!(
                    *recorded, init_entry_id,
                    "dispatch journal identity changed"
                );
            } else {
                self.record_job_admission(&dispatch_id, &dispatch, init_entry_id)
                    .await?;
            }
            if let Some(result) = terminal {
                self.handle_wflow_result(last_event_entry_id, job_id, &result)
                    .await?;
            }
        } else {
            eyre::ensure!(
                entry_id.is_none(),
                "dispatch {dispatch_id} claims an admission absent from the recovered journal"
            );
            if self
                .dispatch_repo
                .task_attempt_for_dispatch(&dispatch_id)
                .await?
                .is_some()
            {
                // The pool owner must freshly authorize this prepared
                // identity before it may append its first JobInit.
                return Ok(());
            }
            self.start_active_dispatch(&dispatch_id, &dispatch).await?;
        }
        Ok(())
    }

    async fn record_job_admission(
        &self,
        dispatch_id: &str,
        dispatch: &DispatchAttempt,
        entry: u64,
    ) -> Res<()> {
        let mut deets = dispatch.deets.clone();
        let ActiveDispatchDeets::Wflow {
            entry_id,
            wflow_partition_id,
            ..
        } = &mut deets;
        *entry_id = Some(entry);
        *wflow_partition_id = Some(self.local_wflow_part_id.clone());
        self.dispatch_repo
            .update_active_deets(dispatch_id, dispatch, deets)
            .await?;
        Ok(())
    }

    async fn start_active_dispatch(
        &self,
        dispatch_id: &str,
        dispatch: &DispatchAttempt,
    ) -> Res<()> {
        #[cfg(test)]
        if !self
            .admission_barrier(DispatchAdmissionCut::BeforeJobInit)
            .await
        {
            return Ok(());
        }
        let Some(current) = self.dispatch_repo.get_active(dispatch_id).await else {
            return Ok(());
        };
        if !current.same_attempt(dispatch) {
            return Ok(());
        }
        if dispatch.cancellation_requested() {
            Box::pin(self.handle_wflow_result(
                0,
                dispatch_job_id(dispatch)?,
                &JobRunResult::Aborted,
            ))
            .await?;
            return Ok(());
        }
        let ActiveDispatchDeets::Wflow {
            plug_id,
            bundle_name,
            ..
        } = &dispatch.deets;
        let prepare = async {
            ensure_bundle_workload_running(
                &self.wash_host,
                &self.blobs_repo,
                plug_id,
                bundle_name,
                &dispatch.execution,
            )
            .await?;
            let ActiveDispatchArgs::FacetRoutine(args) = &dispatch.args;
            self.wflow_ingress
                .add_job(
                    Arc::from(dispatch_job_id(dispatch)?),
                    dispatch.execution.wflow().clone(),
                    args.wflow_args_json
                        .clone()
                        .unwrap_or_else(|| "null".into()),
                    None,
                )
                .await
        }
        .await;
        let entry = match prepare {
            Ok(entry) => entry,
            Err(error) => {
                self.dispatch_repo
                    .complete(
                        dispatch_id.into(),
                        dispatch::DispatchStatus::Failed,
                        dispatch,
                    )
                    .await
                    .wrap_err_with(|| {
                        format!("failed to settle startup failure for {dispatch_id}: {error:?}")
                    })?;
                return Err(error)
                    .wrap_err_with(|| format!("cannot start captured dispatch {dispatch_id}"));
            }
        };
        #[cfg(test)]
        if !self
            .admission_barrier(DispatchAdmissionCut::AfterJobInit)
            .await
        {
            return Ok(());
        }
        // Append is durable even if this metadata write fails. Recovery repairs
        // its exact entry identity; it must not cancel or enqueue another job.
        self.record_job_admission(dispatch_id, dispatch, entry)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn pause_finalization(
        &self,
        reached: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        *self.finalization_gate.lock().await = Some(DispatchTestGate { reached, resume });
    }

    #[cfg(test)]
    pub(crate) async fn pause_admission(
        &self,
        cut: DispatchAdmissionCut,
        reached: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        *self.admission_gate.lock().await = Some((cut, DispatchTestGate { reached, resume }));
    }

    #[cfg(test)]
    async fn admission_barrier(&self, cut: DispatchAdmissionCut) -> bool {
        let gate = {
            let mut gate = self.admission_gate.lock().await;
            if gate.as_ref().is_some_and(|(wanted, _)| *wanted == cut) {
                gate.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.reached.send(()).expect(ERROR_CHANNEL);
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => return false,
                resumed = gate.resume => resumed.expect(ERROR_CHANNEL),
            }
        }
        true
    }

    async fn start_waiting_dispatch(
        &self,
        dispatch_id: String,
        waiting_dispatch: Arc<DispatchAttempt>,
    ) -> Res<()> {
        let ActiveDispatchDeets::Wflow {
            plug_id,
            routine_name,
            bundle_name,
            wflow_job_id,
            ..
        } = &waiting_dispatch.deets;
        let Some(job_id) = wflow_job_id.as_ref() else {
            eyre::bail!("waiting dispatch missing job id: {dispatch_id}");
        };
        let initial_deets = ActiveDispatchDeets::Wflow {
            wflow_partition_id: None,
            entry_id: None,
            plug_id: plug_id.clone(),
            routine_name: routine_name.clone(),
            bundle_name: bundle_name.clone(),
            wflow_job_id: Some(job_id.clone()),
        };
        // Refuse a cancelled/replaced ready snapshot before execution setup.
        let Some(active_dispatch) = self
            .dispatch_repo
            .activate_waiting(&dispatch_id, &waiting_dispatch, initial_deets)
            .await?
        else {
            return Ok(());
        };
        self.start_active_dispatch(&dispatch_id, &active_dispatch)
            .await?;
        self.progress_repo
            .add_update(
                &dispatch_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: None,
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message: "dependencies resolved, dispatch queued".to_string(),
                    },
                },
            )
            .await?;
        Ok(())
    }

    pub async fn dispatch(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
    ) -> Res<String> {
        self.dispatch_raw(plug_id, routine_name, args, vec![]).await
    }

    pub async fn invoke_command_from_wflow_job(
        &self,
        parent_wflow_job_id: &str,
        target_command_url: &str,
        request: daybook_pdk::InvokeCommandRequest,
    ) -> Result<String, InvokeCommandFromWflowError> {
        self.ensure_rt_live()?;
        let parent_dispatch = self
            .dispatch_repo
            .get_by_wflow_job(parent_wflow_job_id)
            .await
            .ok_or_else(|| {
                ferr!("no active dispatch found for parent job: {parent_wflow_job_id}")
            })?;
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            doc_id,
            staging_branch_path,
            command_invoke_acl_snapshot,
            ..
        }) = &parent_dispatch.args;
        let ActiveDispatchDeets::Wflow {
            plug_id: _,
            routine_name,
            ..
        } = &parent_dispatch.deets;

        let target_ref = daybook_pdk::parse_command_url_str(target_command_url)
            .map_err(|err| ferr!("invalid command URL in invoke token: {err}"))?;
        let mut is_allowed = false;
        for allowlisted_url in command_invoke_acl_snapshot {
            let parsed = daybook_pdk::parse_command_url(allowlisted_url).map_err(|err| {
                ferr!(
                    "invalid command_invoke_acl_snapshot entry '{}': {err}",
                    allowlisted_url
                )
            })?;
            if parsed.plug_id == target_ref.plug_id
                && parsed.command_name == target_ref.command_name
            {
                is_allowed = true;
                break;
            }
        }
        if !is_allowed {
            return Err(InvokeCommandFromWflowError::Denied(format!(
                "command target '{}' is not allowlisted by routine '{}'",
                target_command_url, routine_name
            )));
        }

        let target_plug_manifest =
            self.plugs_repo
                .get(&target_ref.plug_id)
                .await
                .ok_or_else(|| {
                    ferr!(
                        "target plug not found in command URL: {}",
                        target_ref.plug_id
                    )
                })?;
        let target_command_manifest = target_plug_manifest
            .commands
            .get(target_ref.command_name.as_str())
            .ok_or_else(|| {
                ferr!(
                    "target command not found in command URL: {}/{}",
                    target_ref.plug_id,
                    target_ref.command_name
                )
            })?;
        let manifest::CommandDeets::DocCommand {
            routine_name: target_routine_name,
        } = &target_command_manifest.deets;

        let fixed_dispatch_id = {
            let mut identity = String::new();
            use std::fmt::Write as _;
            write!(
                &mut identity,
                "{}|{}|{}",
                dispatch_stable_identity(&parent_dispatch),
                target_command_url,
                request.request_id
            )
            .expect("writing to string should never fail");
            let encoded = utils_rs::hash::blake3_hash_bytes_multibase(identity.as_bytes());
            format!("cmdinvoke-{encoded}")
        };
        let waiting_on_dispatch_ids = self
            .ensure_plug_init_dispatches(&target_ref.plug_id, None, None)
            .await?;
        let staging_heads = self
            .drawer
            .get_doc_branches(doc_id)
            .await?
            .and_then(|entry| entry.branches.get(staging_branch_path.as_str()).cloned())
            .ok_or_else(|| {
                ferr!(
                    "missing staging branch heads for command invoke: doc_id={doc_id} branch={}",
                    staging_branch_path.as_str()
                )
            })?;

        self.dispatch_no_gate_internal(
            &target_ref.plug_id,
            &target_routine_name.0,
            DispatchArgs::DocRoutine {
                doc_id: doc_id.clone(),
                branch_path: staging_branch_path.clone(),
                heads: staging_heads,
                invocation: dispatch::RoutineInvocation::Command,
                changed_facet_keys: vec![],
                wflow_args_json: Some(request.args_json.clone()),
            },
            vec![DispatchOnSuccessHook::CommandInvokeReply {
                parent_wflow_job_id: parent_wflow_job_id.to_string(),
                request_id: request.request_id,
            }],
            waiting_on_dispatch_ids,
            Some(fixed_dispatch_id),
            true,
        )
        .await
        .map_err(InvokeCommandFromWflowError::Other)
    }

    async fn dispatch_raw(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
    ) -> Res<String> {
        self.ensure_rt_live()?;
        let waiting_on_dispatch_ids = self
            .ensure_plug_init_dispatches(plug_id, None, None)
            .await?;
        self.dispatch_no_gate_internal(
            plug_id,
            routine_name,
            args,
            on_success_hooks,
            waiting_on_dispatch_ids,
            None,
            false,
        )
        .await
    }

    pub(crate) async fn dispatch_no_gate(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
        waiting_on_dispatch_ids: Vec<String>,
    ) -> Res<String> {
        self.dispatch_no_gate_internal(
            plug_id,
            routine_name,
            args,
            on_success_hooks,
            waiting_on_dispatch_ids,
            None,
            false,
        )
        .await
    }

    async fn prepare_routine(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        fixed_dispatch_id: Option<String>,
    ) -> Res<task_adapter::PreparedRoutine> {
        self.ensure_rt_live()?;

        // Select enablement once, then derive every execution field from the same
        // exact manifest revision. Delayed execution never revisits enablement.
        let enabled_ref = self
            .plugs_repo
            .enabled_ref(plug_id)
            .await?
            .ok_or_else(|| ferr!("plug not enabled: {plug_id}/{routine_name}"))?;
        let manifest_ref = PlugsRepo::parse_enabled_ref(&enabled_ref)?;
        let (manifest_heads, plug_man) = self
            .plugs_repo
            .materialize_active(plug_id, &enabled_ref)
            .await?
            .ok_or_else(|| ferr!("enabled manifest not readable: {plug_id}/{routine_name}"))?;
        let routine_man = plug_man
            .routines
            .get(routine_name)
            .ok_or_else(|| ferr!("routine not found in plug manifest: {plug_id}/{routine_name}"))?;

        let (dispatch_id, mut args) = match args {
            DispatchArgs::DocFacet {
                doc_id,
                heads,
                branch_path,
                facet_key,
            } => {
                let facet_key =
                    facet_key.ok_or_else(|| ferr!("missing facet_key for doc facet dispatch"))?;
                let dispatch_id = {
                    let mut identity = String::new();
                    use std::fmt::Write as _;
                    write!(
                        &mut identity,
                        "{}|{}|{}|{}|{}",
                        doc_id,
                        branch_path,
                        am_utils_rs::serialize_commit_heads(heads.as_ref()).join(","),
                        plug_id,
                        routine_name,
                    )
                    .expect("writing to string should never fail");
                    write!(&mut identity, "|{facet_key}")
                        .expect("writing to string should never fail");
                    utils_rs::hash::blake3_hash_bytes_multibase(identity.as_bytes())
                };
                let dispatch_id = fixed_dispatch_id
                    .clone()
                    .unwrap_or_else(|| format!("{plug_id}/{routine_name}-{dispatch_id}"));

                let mut config_docs_by_owner: std::collections::BTreeMap<
                    String,
                    Vec<daybook_types::manifest::RoutineFacetAccess>,
                > = std::collections::BTreeMap::new();
                for access in routine_man.config_facet_acl() {
                    let owner = access
                        .owner_plug_id
                        .clone()
                        .unwrap_or_else(|| plug_id.to_string());
                    config_docs_by_owner
                        .entry(owner)
                        .or_default()
                        .push(access.clone());
                }
                let config_docs: Vec<dispatch::DocFacetTokens> = config_docs_by_owner
                    .into_values()
                    .map(|facet_acl| dispatch::DocFacetTokens {
                        doc_id: String::new(),
                        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        staging_branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        heads: daybook_types::doc::ChangeHashSet(Vec::new().into()),
                        facet_acl,
                    })
                    .collect();

                let primary_doc = dispatch::DocFacetTokens {
                    doc_id: doc_id.clone(),
                    branch_path: daybook_types::doc::BranchPathBuf::from(branch_path.as_str()),
                    staging_branch_path: daybook_types::doc::BranchPathBuf::from(
                        "/tmp/placeholder",
                    ),
                    heads: heads.clone(),
                    facet_acl: routine_man.facet_acl().to_vec(),
                };

                (
                    dispatch_id,
                    ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                        doc_id,
                        branch_path,
                        heads,
                        invocation: dispatch::RoutineInvocation::Command,
                        primary_doc,
                        config_docs,
                        local_state_acl: routine_man.local_state_acl.clone(),
                        command_invoke_acl_snapshot: routine_man.command_invoke_acl().to_vec(),
                        wflow_args_json: None,
                        staging_branch_path: daybook_types::doc::BranchPathBuf::from(
                            "/tmp/placeholder",
                        ), // Will be set when job is created
                    }),
                )
            }
            DispatchArgs::DocRoutine {
                doc_id,
                branch_path,
                heads,
                invocation,
                changed_facet_keys,
                wflow_args_json,
            } => {
                let invocation = match invocation {
                    dispatch::RoutineInvocation::Command => dispatch::RoutineInvocation::Command,
                    dispatch::RoutineInvocation::Processor(mut processor_invocation) => {
                        processor_invocation.trigger_doc_id = doc_id.clone();
                        processor_invocation.changed_facet_keys = changed_facet_keys;
                        dispatch::RoutineInvocation::Processor(processor_invocation)
                    }
                };

                let dispatch_id = {
                    let mut identity = String::new();
                    use std::fmt::Write as _;
                    let invocation_kind = match &invocation {
                        dispatch::RoutineInvocation::Command => "command",
                        dispatch::RoutineInvocation::Processor(_) => "processor",
                    };
                    write!(
                        &mut identity,
                        "{}|{}|{}|{}|{}|{}",
                        doc_id,
                        branch_path,
                        am_utils_rs::serialize_commit_heads(heads.as_ref()).join(","),
                        plug_id,
                        routine_name,
                        invocation_kind
                    )
                    .expect("writing to string should never fail");
                    utils_rs::hash::blake3_hash_bytes_multibase(identity.as_bytes())
                };
                let dispatch_id = fixed_dispatch_id.clone().unwrap_or_else(|| {
                    format!("{plug_id}/{routine_name}/{branch_path}-{dispatch_id}")
                });

                let mut config_docs_by_owner: std::collections::BTreeMap<
                    String,
                    Vec<daybook_types::manifest::RoutineFacetAccess>,
                > = std::collections::BTreeMap::new();
                for access in routine_man.config_facet_acl() {
                    let owner = access
                        .owner_plug_id
                        .clone()
                        .unwrap_or_else(|| plug_id.to_string());
                    config_docs_by_owner
                        .entry(owner)
                        .or_default()
                        .push(access.clone());
                }
                let config_docs: Vec<dispatch::DocFacetTokens> = config_docs_by_owner
                    .into_values()
                    .map(|facet_acl| dispatch::DocFacetTokens {
                        doc_id: String::new(),
                        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        staging_branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                        heads: daybook_types::doc::ChangeHashSet(Vec::new().into()),
                        facet_acl,
                    })
                    .collect();

                let primary_doc = dispatch::DocFacetTokens {
                    doc_id: doc_id.clone(),
                    branch_path: daybook_types::doc::BranchPathBuf::from(branch_path.as_str()),
                    staging_branch_path: daybook_types::doc::BranchPathBuf::from(
                        "/tmp/placeholder",
                    ),
                    heads: heads.clone(),
                    facet_acl: routine_man.facet_acl().to_vec(),
                };

                (
                    dispatch_id,
                    ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                        doc_id: doc_id.clone(),
                        branch_path: branch_path.clone(),
                        heads: heads.clone(),
                        invocation,
                        primary_doc,
                        config_docs,
                        local_state_acl: routine_man.local_state_acl.clone(),
                        command_invoke_acl_snapshot: routine_man.command_invoke_acl().to_vec(),
                        wflow_args_json,
                        staging_branch_path: daybook_types::doc::BranchPathBuf::from(
                            "/tmp/placeholder",
                        ), // Will be set when job is created
                    }),
                )
            }
            DispatchArgs::DocInvoke { .. } => {
                return Err(ferr!("doc-invoke dispatch is not supported for routines"));
            }
        };
        // Configuration is invocation input: persist its exact document and heads
        // before enqueueing, so a delayed or recovered job cannot read a newer revision.
        let (configuration_document, configuration_heads, config_bindings) =
            self.plugs_repo.capture_config_bindings().await?;
        let ActiveDispatchArgs::FacetRoutine(facet_args) = &mut args;
        for config_doc in &mut facet_args.config_docs {
            let owner = config_doc.facet_acl[0]
                .owner_plug_id
                .as_deref()
                .unwrap_or(plug_id);
            let doc_id = config_bindings
                .get(owner)
                .cloned()
                .ok_or_else(|| ferr!("plug {owner} has no captured configuration association"))?;
            let heads = self
                .drawer
                .get_doc_branches(&doc_id)
                .await?
                .and_then(|doc| doc.branches.get("main").cloned())
                .ok_or_else(|| ferr!("config doc missing main branch for plug {owner}"))?;
            config_doc.doc_id = doc_id;
            config_doc.heads = heads;
        }
        let manifest::RoutineImpl::Wflow { key, bundle } = &routine_man.r#impl;
        let bundle_man = plug_man
            .wflow_bundles
            .get(bundle.0.as_str())
            .ok_or_else(|| ferr!("bundle not found: {plug_id}/{bundle}"))?;
        let execution = capture_wflow_execution(
            &self.blobs_repo,
            plug_id,
            bundle.0.as_str(),
            key.0.as_str(),
            &manifest_ref.doc_id,
            manifest_heads,
            bundle_man,
        )
        .await?;
        Ok(task_adapter::PreparedRoutine {
            dispatch_id,
            input: task_adapter::CapturedRoutineInput::V1 {
                plug_id: plug_id.to_owned(),
                routine_name: routine_name.to_owned(),
                bundle_name: bundle.0.clone(),
                args,
                execution,
                configuration_document,
                configuration_heads,
                pool_binding: None,
                processor: None,
            },
        })
    }

    #[expect(clippy::too_many_arguments)]
    async fn dispatch_no_gate_internal(
        &self,
        plug_id: &str,
        routine_name: &str,
        args: DispatchArgs,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
        waiting_on_dispatch_ids: Vec<String>,
        fixed_dispatch_id: Option<String>,
        reuse_terminal_on_match: bool,
    ) -> Res<String> {
        let prepared = self
            .prepare_routine(plug_id, routine_name, args, fixed_dispatch_id)
            .await?;
        self.dispatch_prepared_no_gate(
            plug_id,
            routine_name,
            prepared,
            on_success_hooks,
            waiting_on_dispatch_ids,
            reuse_terminal_on_match,
        )
        .await
    }

    async fn dispatch_prepared_no_gate(
        &self,
        plug_id: &str,
        routine_name: &str,
        prepared: task_adapter::PreparedRoutine,
        on_success_hooks: Vec<DispatchOnSuccessHook>,
        waiting_on_dispatch_ids: Vec<String>,
        reuse_terminal_on_match: bool,
    ) -> Res<String> {
        let mut dispatch_id = prepared.dispatch_id;
        let task_adapter::CapturedRoutineInput::V1 {
            mut args,
            execution,
            bundle_name,
            ..
        } = prepared.input;
        if let Some(existing) = self.dispatch_repo.get_any(&dispatch_id).await {
            let can_reuse = serde_json::to_string(&existing.on_success_hooks).expect(ERROR_JSON)
                == serde_json::to_string(&on_success_hooks).expect(ERROR_JSON)
                && existing.waiting_on_dispatch_ids == waiting_on_dispatch_ids
                && serde_json::to_vec(&existing.execution).expect(ERROR_JSON)
                    == serde_json::to_vec(&execution).expect(ERROR_JSON)
                && logical_dispatch_args(&existing.args) == logical_dispatch_args(&args);
            let reuse_status_ok = matches!(
                existing.status,
                dispatch::DispatchStatus::Waiting | dispatch::DispatchStatus::Active
            );
            if can_reuse && reuse_status_ok {
                warn!(?dispatch_id, "dispatch already exists with same identity");
                return Ok(dispatch_id);
            }
            if can_reuse && reuse_terminal_on_match {
                debug!(
                    ?dispatch_id,
                    status = ?existing.status,
                    "skipping terminal dispatch reuse without CommandInvokeReply replay"
                );
            }
            dispatch_id = format!("{dispatch_id}-{}", Uuid::new_v4().bs58());
        }

        let is_waiting = !waiting_on_dispatch_ids.is_empty();

        let job_id = format!("{dispatch_id}-{id}", id = Uuid::new_v4().bs58());
        let staging_branch_path = daybook_types::doc::BranchPathBuf::from(format!("/tmp/{job_id}"));
        let ActiveDispatchArgs::FacetRoutine(facet_args) = &mut args;
        facet_args.staging_branch_path = staging_branch_path.clone();
        facet_args.primary_doc.staging_branch_path = staging_branch_path;
        let deets = ActiveDispatchDeets::Wflow {
            wflow_partition_id: None,
            entry_id: None,
            plug_id: plug_id.into(),
            routine_name: routine_name.to_string(),
            bundle_name: bundle_name.clone(),
            wflow_job_id: Some(job_id.clone()),
        };
        let active_dispatch = Arc::new(DispatchAttempt::new(ActiveDispatch {
            args,
            deets,
            execution,
            status: if is_waiting {
                dispatch::DispatchStatus::Waiting
            } else {
                dispatch::DispatchStatus::Active
            },
            waiting_on_dispatch_ids,
            on_success_hooks,
        }));
        let ActiveDispatchArgs::FacetRoutine(args) = &active_dispatch.args;
        debug!(
            %dispatch_id,
            arg_fingerprint = %facet_routine_args_fingerprint(args),
            doc_id = ?args.doc_id,
            branch_path = %args.branch_path,
            staging_branch_path = %args.staging_branch_path,
            heads = ?am_utils_rs::serialize_commit_heads(args.heads.as_ref()),
            "dispatch_no_gate_internal prepared dispatch args"
        );
        if let Err(add_err) = self
            .dispatch_repo
            .add(dispatch_id.clone(), Arc::clone(&active_dispatch))
            .await
        {
            let ActiveDispatchDeets::Wflow {
                wflow_job_id,
                entry_id,
                ..
            } = &active_dispatch.deets;
            if entry_id.is_some()
                && let Some(wflow_job_id) = wflow_job_id.as_ref()
                    && let Err(cancel_err) = self
                        .wflow_ingress
                        .cancel_job(
                            Arc::from(wflow_job_id.as_ref()),
                            format!(
                                "rollback scheduling for dispatch {dispatch_id} after dispatch store failure"
                            ),
                        )
                        .await
                    {
                        warn!(
                            %dispatch_id,
                            %wflow_job_id,
                            ?cancel_err,
                            "failed to rollback queued wflow job after dispatch add failure"
                        );
                    }
            return Err(add_err);
        }

        if !is_waiting {
            let Some(bundle_man) = plug_man.wflow_bundles.get(bundle_name.as_str()) else {
                if let Err(cleanup_err) = self
                    .dispatch_repo
                    .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                    .await
                {
                    warn!(
                        %dispatch_id,
                        ?cleanup_err,
                        "failed to mark dispatch failed after missing bundle error"
                    );
                }
                return Err(ferr!(
                    "bundle not found in plug manifest: routine={plug_id}/{routine_name} bundle={bundle_name} key={wflow_key}"
                ));
            };
            if let Err(err) = ensure_bundle_workload_running(
                &self.wcx,
                &self.wash_host,
                &self.blobs_repo,
                plug_id.into(),
                bundle_name.clone(),
                bundle_man,
            )
            .await
            {
                if let Err(cleanup_err) = self
                    .dispatch_repo
                    .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                    .await
                {
                    warn!(
                        %dispatch_id,
                        ?cleanup_err,
                        "failed to mark dispatch failed after workload start error"
                    );
                }
                return Err(err);
            }

            let wflow_args_json = {
                let ActiveDispatchArgs::FacetRoutine(ref facet_args) = active_dispatch.args;
                facet_args
                    .wflow_args_json
                    .clone()
                    .unwrap_or_else(|| serde_json::to_string(&()).expect(ERROR_JSON))
            };
            let entry_id = match self
                .wflow_ingress
                .add_job(
                    job_id.clone().into(),
                    wflow_key.as_str(),
                    wflow_args_json,
                    None,
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    if let Err(cleanup_err) = self
                        .dispatch_repo
                        .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                        .await
                    {
                        warn!(
                            %dispatch_id,
                            ?cleanup_err,
                            "failed to mark dispatch failed after job scheduling error"
                        );
                    }
                    return Err(err).wrap_err_with(|| {
                        format!("error scheduling job for {plug_id}/{routine_name}")
                    });
                }
            };
            let deets = ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some(self.local_wflow_part_id.clone()),
                entry_id: Some(entry_id),
                plug_id: plug_id.into(),
                routine_name: routine_name.to_string(),
                bundle_name: bundle_name.clone(),
                wflow_key: wflow_key.clone(),
                wflow_job_id: Some(job_id.clone()),
            };
            if let Err(err) = self
                .dispatch_repo
                .update_active_deets(&dispatch_id, deets)
                .await
            {
                if let Err(cancel_err) = self
                    .wflow_ingress
                    .cancel_job(
                        Arc::from(job_id.as_str()),
                        format!("rollback scheduling for dispatch {dispatch_id}"),
                    )
                    .await
                {
                    warn!(
                        %dispatch_id,
                        %job_id,
                        ?cancel_err,
                        "failed to rollback queued wflow job after dispatch deets update failure"
                    );
                }
                if let Err(cleanup_err) = self
                    .dispatch_repo
                    .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                    .await
                {
                    warn!(
                        %dispatch_id,
                        ?cleanup_err,
                        "failed to mark dispatch failed after deets update error"
                    );
                }
                return Err(err);
            }
        }
        let mut tags = vec![
            "/type/dispatch".to_string(),
            format!("/dispatch/{dispatch_id}"),
        ];
        let title = match &active_dispatch.args {
            ActiveDispatchArgs::FacetRoutine(facet_args) => {
                tags.push(format!("/docs/{}", facet_args.doc_id));
                dispatch_id.clone()
            }
        };
        self.progress_repo
            .upsert_task(crate::progress::CreateProgressTaskArgs {
                id: dispatch_id.clone(),
                tags,
                retention: crate::progress::ProgressRetentionPolicy::UserDismissable,
            })
            .await?;
        self.progress_repo
            .add_update(
                &dispatch_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: Some(title),
                    deets: crate::progress::ProgressUpdateDeets::Status {
                        severity: crate::progress::ProgressSeverity::Info,
                        message: if is_waiting {
                            "dispatch waiting on dependencies".to_string()
                        } else {
                            "dispatch queued".to_string()
                        },
                    },
                },
            )
            .await?;
        Ok(dispatch_id)
    }

    pub async fn cancel_dispatch(&self, dispatch_id: &str) -> Res<()> {
        self.ensure_rt_live()?;

        let dispatch = self
            .dispatch_repo
            .get_any(dispatch_id)
            .await
            .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        if dispatch.status.is_terminal() {
            debug!(%dispatch_id, status = ?dispatch.status, "cancel ignored; dispatch already settled");
            return Ok(());
        }
        let marked_now = self.dispatch_repo.cancel(dispatch_id, &dispatch).await?;
        if !marked_now {
            debug!(%dispatch_id, "cancel ignored; already requested or too late");
            return Ok(());
        }
        // Activation is serialized with the mark write and refuses marked
        // waiting attempts. Re-read so a concurrently activated job receives
        // execution cancellation rather than bypassing its staging cleanup.
        let Some(current) = self.dispatch_repo.get_any(dispatch_id).await else {
            return Ok(());
        };
        if !current.same_attempt(&dispatch) {
            return Ok(());
        }
        let dispatch = current;
        if matches!(dispatch.status, dispatch::DispatchStatus::Waiting) {
            self.dispatch_repo
                .complete(
                    dispatch_id.into(),
                    dispatch::DispatchStatus::Cancelled,
                    &dispatch,
                )
                .await?;
            self.release_waiting_dispatches(dispatch_id, false).await?;
            self.progress_repo
                .add_update(
                    dispatch_id,
                    crate::progress::ProgressUpdate {
                        at: jiff::Timestamp::now(),
                        title: None,
                        deets: crate::progress::ProgressUpdateDeets::Completed {
                            state: crate::progress::ProgressFinalState::Cancelled,
                            message: Some("dispatch cancelled while waiting".to_string()),
                        },
                    },
                )
                .await?;
            return Ok(());
        }
        match &dispatch.deets {
            ActiveDispatchDeets::Wflow {
                wflow_job_id,
                entry_id,
                ..
            } => {
                let Some(wflow_job_id) = wflow_job_id.as_ref() else {
                    return Ok(());
                };
                if entry_id.is_none() {
                    return Ok(());
                }
                self.progress_repo
                    .add_update(
                        dispatch_id,
                        crate::progress::ProgressUpdate {
                            at: jiff::Timestamp::now(),
                            title: None,
                            deets: crate::progress::ProgressUpdateDeets::Status {
                                severity: crate::progress::ProgressSeverity::Warn,
                                message: "cancellation requested".to_string(),
                            },
                        },
                    )
                    .await?;
                self.wflow_ingress
                    .cancel_job(
                        Arc::from(wflow_job_id.as_ref()),
                        format!("cancel requested for dispatch {dispatch_id}"),
                    )
                    .await
                    .wrap_err_with(|| format!("error cancelling dispatch {dispatch_id}"))?;
            }
        }
        Ok(())
    }

    /// Wait until a log entry matches the provided condition
    /// The callback receives (entry_id, log_entry) and should return true when the condition is met
    pub async fn wait_for_dispatch_end(
        &self,
        dispatch_id: &str,
        timeout: std::time::Duration,
    ) -> Res<()> {
        self.ensure_rt_live()?;

        use crate::repos::{Repo, SubscribeOpts};

        let listener_handle = self.dispatch_repo.subscribe(SubscribeOpts::new(128));

        // check if the dispatch exists first
        let Some(dispatch) = self.dispatch_repo.get_any(dispatch_id).await else {
            return Ok(());
        };
        if matches!(
            dispatch.status,
            dispatch::DispatchStatus::Succeeded
                | dispatch::DispatchStatus::Failed
                | dispatch::DispatchStatus::Cancelled
        ) {
            return Ok(());
        }

        tokio::time::timeout(timeout, async {
            loop {
                let event = listener_handle
                    .recv_async()
                    .await
                    .map_err(|err| eyre::eyre!("dispatch listener closed: {err:?}"))?;
                match &*event {
                    dispatch::DispatchEvent::DispatchDeleted { id, .. } if id == dispatch_id => {
                        return Ok::<(), eyre::Report>(());
                    }
                    dispatch::DispatchEvent::DispatchUpdated { id, .. }
                    | dispatch::DispatchEvent::DispatchAdded { id, .. }
                        if id == dispatch_id =>
                    {
                        if let Some(cur) = self.dispatch_repo.get_any(dispatch_id).await {
                            if matches!(
                                cur.status,
                                dispatch::DispatchStatus::Succeeded
                                    | dispatch::DispatchStatus::Failed
                                    | dispatch::DispatchStatus::Cancelled
                            ) {
                                return Ok::<(), eyre::Report>(());
                            }
                        } else {
                            return Ok::<(), eyre::Report>(());
                        }
                    }
                    _ => {}
                }
            }
        })
        .await??;

        Ok(())
    }
}

async fn upsert_processor_runlog_item(
    partition_store: &SharedPartStore,
    done_by_peer_id: &str,
    doc_id: &str,
    processor_full_id: &str,
    done_token: &str,
) -> Res<()> {
    let item_id = Rt::processor_runlog_item_id(doc_id, processor_full_id);
    let payload = serde_json::json!({
        "done_by_peer_id": done_by_peer_id,
        "done_token": done_token,
        "done_at": jiff::Timestamp::now().to_string(),
    });
    partition_store
        .set_obj_payload(item_id.clone(), payload)
        .await?;
    partition_store
        .add_obj_to_parts(
            item_id,
            vec![crate::part_id_from_label(
                crate::rt::PROCESSOR_RUNLOG_PARTITION_ID,
            )],
        )
        .await?;
    Ok(())
}

fn dispatch_stable_identity(dispatch: &ActiveDispatch) -> String {
    match (&dispatch.deets, &dispatch.args) {
        (
            ActiveDispatchDeets::Wflow {
                plug_id,
                routine_name,
                ..
            },
            ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path,
                heads,
                ..
            }),
        ) => format!(
            "{plug_id}/{routine_name}|{doc_id}|{}|{}",
            branch_path.as_str(),
            serde_json::to_string(heads).expect(ERROR_JSON)
        ),
    }
}

/// Build the command-invoke reply for a terminal dispatch result.
///
/// `cancelled_win` is the finalization ordering outcome: a cancellation that
/// won reports `Cancelled` regardless of a success that arrived afterwards, so a
/// parent job never treats an unpublished command as having run. Otherwise the
/// result maps directly, with a post-run merge failure reported as `Failed`.
fn command_invoke_reply_from_result(
    result: &JobRunResult,
    dispatch_id: &str,
    cancelled_win: bool,
    merged_successfully: bool,
) -> (
    daybook_pdk::InvokeCommandStatus,
    Option<String>,
    Option<String>,
) {
    if cancelled_win {
        return (daybook_pdk::InvokeCommandStatus::Cancelled, None, None);
    }
    if !merged_successfully {
        let error_json = serde_json::json!({
            "kind": "merge-failed",
            "dispatch_id": dispatch_id,
            "message": "workflow run succeeded but post-run branch merge failed",
        });
        return (
            daybook_pdk::InvokeCommandStatus::Failed,
            None,
            Some(error_json.to_string()),
        );
    }
    match result {
        JobRunResult::Success { value_json } => (
            daybook_pdk::InvokeCommandStatus::Succeeded,
            Some(value_json.to_string()),
            None,
        ),
        JobRunResult::Aborted => (daybook_pdk::InvokeCommandStatus::Cancelled, None, None),
        JobRunResult::WflowErr(JobError::Terminal { error_json }) => (
            daybook_pdk::InvokeCommandStatus::Failed,
            None,
            Some(error_json.to_string()),
        ),
        JobRunResult::WorkerErr(err) => {
            let error_json = serde_json::json!({
                "kind": "worker-error",
                "dispatch_id": dispatch_id,
                "error": format!("{err:?}"),
            });
            (
                daybook_pdk::InvokeCommandStatus::Failed,
                None,
                Some(error_json.to_string()),
            )
        }
        JobRunResult::WflowErr(JobError::Transient { error_json, .. }) => {
            let wrapped = serde_json::json!({
                "kind": "transient-exhausted",
                "dispatch_id": dispatch_id,
                "error_json": error_json,
            });
            (
                daybook_pdk::InvokeCommandStatus::Failed,
                None,
                Some(wrapped.to_string()),
            )
        }
        JobRunResult::StepEffect(_) | JobRunResult::StepWait(_) => {
            unreachable!("non-terminal result reached terminal invoke reply")
        }
    }
}

async fn load_bundle_components(
    blobs_repo: &BlobsRepo,
    bundle_man: &manifest::WflowBundleManifest,
) -> Res<Vec<Component>> {
    let mut components = Vec::new();
    for url in &bundle_man.component_urls {
        let wasm_bytes = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| eyre::eyre!("invalid file path in url: {}", url))?;
                tokio::fs::read(&path).await.wrap_err_with(|| {
                    format!("failed to read component file: {}", path.display())
                })?
            }
            scheme if scheme == crate::blobs::BLOB_SCHEME => {
                let hash = url.path().trim_start_matches('/');
                let blob_id = hash
                    .parse::<crate::blobs::BlobId>()
                    .wrap_err_with(|| format!("invalid blob hash in component URL: {hash}"))?;
                let path = blobs_repo
                    .get_path(blob_id)
                    .await
                    .wrap_err_with(|| format!("blob not found in BlobsRepo: {}", hash))?;
                tokio::fs::read(&path)
                    .await
                    .wrap_err_with(|| format!("failed to read blob file: {}", path.display()))?
            }
            _ => {
                return Err(eyre::eyre!(
                    "Unsupported URL scheme for component: {}",
                    url.scheme()
                ));
            }
        };
        components.push(Component {
            bytes: wasm_bytes.into(),
            ..default()
        });
    }
    Ok(components)
}

fn stateless_view_host_interfaces() -> Vec<WitInterface> {
    vec![
        WitInterface::from("townframe:wflow/host"),
        WitInterface::from("townframe:daybook/drawer"),
        WitInterface::from("townframe:daybook/capabilities"),
        WitInterface::from("townframe:daybook/facet-routine"),
        WitInterface::from("townframe:sqlite/sqlite-connection"),
        WitInterface::from("townframe:daybook/mltools-ocr"),
        WitInterface::from("townframe:daybook/mltools-embed"),
        WitInterface::from("townframe:daybook/mltools-image-tools"),
        WitInterface::from("townframe:daybook/mltools-llm-chat"),
        WitInterface::from("townframe:api-utils/utils"),
    ]
}

fn dispatch_job_id(dispatch: &DispatchAttempt) -> Res<&str> {
    let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &dispatch.deets;
    wflow_job_id
        .as_deref()
        .ok_or_else(|| ferr!("captured dispatch has no retained workflow job identity"))
}

fn logical_dispatch_args(args: &ActiveDispatchArgs) -> serde_json::Value {
    let ActiveDispatchArgs::FacetRoutine(args) = args;
    let mut args = args.clone();
    // Staging is attempt-local, not part of the admitted invocation identity.
    args.staging_branch_path = "main".into();
    args.primary_doc.staging_branch_path = args.primary_doc.branch_path.clone();
    for config in &mut args.config_docs {
        config.staging_branch_path = config.branch_path.clone();
    }
    serde_json::to_value(args).expect(ERROR_JSON)
}

async fn capture_wflow_execution(
    blobs_repo: &BlobsRepo,
    plug_id: &str,
    bundle_name: &str,
    handler_key: &str,
    manifest_doc_id: &str,
    manifest_heads: ChangeHashSet,
    bundle: &manifest::WflowBundleManifest,
) -> Res<dispatch::CapturedWflowExecution> {
    use wflow::wflow_core::metastore::*;
    let mut heads = manifest_heads.as_ref().to_vec();
    heads.sort();
    heads.dedup();
    eyre::ensure!(!heads.is_empty(), "captured manifest has no heads");
    let manifest_heads = ChangeHashSet(heads.into());
    let mut handler_keys: Vec<String> = bundle.keys.iter().map(|key| key.0.clone()).collect();
    handler_keys.sort();
    handler_keys.dedup();
    eyre::ensure!(
        handler_keys.iter().any(|key| key == handler_key),
        "routine handler {handler_key} is not declared by bundle {bundle_name}"
    );
    let mut component_blobs = Vec::with_capacity(bundle.component_urls.len());
    for url in &bundle.component_urls {
        let blob = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| ferr!("invalid component file URL: {url}"))?;
                blobs_repo.put_path_copy(&path).await?
            }
            crate::blobs::BLOB_SCHEME => url.path().trim_start_matches('/').parse()?,
            _ => eyre::bail!("unsupported component URL scheme: {url}"),
        };
        component_blobs.push(blob);
    }
    eyre::ensure!(
        !component_blobs.is_empty(),
        "captured bundle has no components"
    );
    let identity = serde_json::to_vec(&(
        "daybook/captured-wflow/v1",
        plug_id,
        manifest_doc_id,
        "main",
        &manifest_heads,
        bundle_name,
        &component_blobs,
        &handler_keys,
    ))
    .expect(ERROR_JSON);
    let digest = utils_rs::hash::blake3_hash_bytes_multibase(&identity);
    Ok(dispatch::CapturedWflowExecution::V1 {
        manifest_doc_id: manifest_doc_id.into(),
        manifest_branch: "main".into(),
        manifest_heads,
        wflow: WflowMeta {
            key: handler_key.into(),
            service: WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                workload_id: format!("{plug_id}/{bundle_name}/{digest}"),
            }),
        },
        component_blobs,
        handler_keys,
    })
}

async fn ensure_bundle_workload_running(
    wash_host: &WashHost,
    blobs_repo: &BlobsRepo,
    plug_id: &str,
    bundle_name: &str,
    execution: &dispatch::CapturedWflowExecution,
) -> Res<String> {
    let workload_id = execution.workload_id().to_owned();
    loop {
        let status = wash_host
            .workload_status(wash_runtime::types::WorkloadStatusRequest {
                workload_id: workload_id.clone(),
            })
            .await
            .map_err(|err| eyre::eyre!("failed to query workload status: {err:#}"))?;
        match status.workload_status.workload_state {
            wash_runtime::types::WorkloadState::Running => break,
            wash_runtime::types::WorkloadState::Starting => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            wash_runtime::types::WorkloadState::NotFound => {
                start_bundle_workload(
                    wash_host,
                    blobs_repo,
                    workload_id.clone(),
                    plug_id,
                    bundle_name,
                    execution,
                )
                .await
                .wrap_err("error starting bundle wflow")?;
            }
            wash_runtime::types::WorkloadState::Unspecified
            | wash_runtime::types::WorkloadState::Completed
            | wash_runtime::types::WorkloadState::Stopping
            | wash_runtime::types::WorkloadState::Error => {
                eyre::bail!("unexpected workload status for {workload_id}: {status:?}");
            }
        }
    }
    Ok(workload_id)
}
async fn start_bundle_workload(
    wash_host: &WashHost,
    blobs_repo: &BlobsRepo,
    workload_id: String,
    plug_id: &str,
    bundle_name: &str,
    execution: &dispatch::CapturedWflowExecution,
) -> Res<()> {
    let dispatch::CapturedWflowExecution::V1 {
        component_blobs,
        handler_keys,
        ..
    } = execution;
    let mut components = Vec::with_capacity(component_blobs.len());
    for blob_id in component_blobs {
        let wasm_bytes = blobs_repo.get_bytes(blob_id.clone()).await?;
        eyre::ensure!(
            crate::blobs::BlobId::new(*blake3::hash(&wasm_bytes).as_bytes()) == *blob_id,
            "captured component blob has incorrect content: {blob_id}"
        );
        components.push(Component {
            bytes: wasm_bytes.into(),
            ..default()
        });
    }

    let _resp = wash_host
        .workload_start(wash_runtime::types::WorkloadStartRequest {
            workload_id,
            workload: wash_runtime::types::Workload {
                namespace: plug_id.to_owned(),
                name: bundle_name.to_owned(),
                annotations: HashMap::new(),
                service: None,
                components,
                host_interfaces: vec![
                    WitInterface {
                        config: [("wflow_keys".to_owned(), handler_keys.join(","))].into(),
                        ..WitInterface::from("townframe:wflow/bundle")
                    },
                    // FIXME: the following syntax is not supported here
                    // WitInterface::from("townframe:daybook/drawer,capabilities,facet-routine"),
                    WitInterface::from("townframe:daybook/drawer"),
                    WitInterface::from("townframe:daybook/capabilities"),
                    WitInterface::from("townframe:daybook/facet-routine"),
                    WitInterface::from("townframe:sqlite/sqlite-connection"),
                    WitInterface::from("townframe:daybook/mltools-ocr"),
                    WitInterface::from("townframe:daybook/mltools-embed"),
                    WitInterface::from("townframe:daybook/mltools-llm-chat"),
                    // WitInterface::from("wasi:keyvalue/store"),
                ],
                volumes: vec![],
            },
        })
        .await
        .to_eyre()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn delayed_dispatch_reads_captured_configuration() -> Res<()> {
        let cx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
        crate::test_support::import_and_enable_test_plug(&cx).await?;
        let config_id = cx
            .rt
            .plugs_repo
            .get_plug_config_doc_id("@daybook/test")
            .await
            .ok_or_eyre("missing test configuration")?;
        let early_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::Any("org.example.test.config".into()),
            id: "early".into(),
        };
        cx.drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: config_id.clone(),
                    facets_set: [(early_key.clone(), serde_json::json!({"revision": "early"}))]
                        .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;
        let target = cx
            .drawer_repo
            .add(daybook_types::doc::AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    daybook_types::doc::FacetKey::from(
                        daybook_types::doc::WellKnownFacetTag::LabelGeneric,
                    ),
                    daybook_types::doc::WellKnownFacet::LabelGeneric("seed".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        let (_, heads) = cx
            .drawer_repo
            .get_with_heads(&target, daybook_types::doc::BranchPath::new("main"), None)
            .await?
            .ok_or_eyre("missing target")?;
        // The dependency receipt controls execution; configuration changes while
        // the real persisted dispatch is waiting, without a timer or engine mock.
        let dispatch_id = cx
            .rt
            .dispatch_no_gate_internal(
                "@daybook/test",
                "report-full-command",
                DispatchArgs::DocRoutine {
                    doc_id: target.clone(),
                    branch_path: "main".into(),
                    heads,
                    invocation: dispatch::RoutineInvocation::Command,
                    changed_facet_keys: vec![],
                    wflow_args_json: None,
                },
                vec![],
                vec!["configuration-barrier".into()],
                None,
                false,
            )
            .await?;
        let late_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::Any("org.example.test.config".into()),
            id: "late".into(),
        };
        cx.drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: config_id,
                    facets_set: [(late_key.clone(), serde_json::json!({"revision": "late"}))]
                        .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;
        cx.rt
            .release_waiting_dispatches("configuration-barrier", true)
            .await?;
        cx.rt
            .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(120))
            .await?;
        let completed = cx.dispatch_repo.get_any(&dispatch_id).await.unwrap();
        assert_eq!(completed.status, dispatch::DispatchStatus::Succeeded);
        let path = cx
            .rt
            .sqlite_local_state_repo
            .get_sqlite_file_path("@daybook/test/capability-report")
            .await?;
        let sql = sqlx_utils_rs::SqlCtx::url(&format!("sqlite://{}", path.display())).await?;
        let report: String =
            sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?")
                .bind(&target)
                .fetch_one(&sql.read_pool)
                .await?;
        let report: serde_json::Value = serde_json::from_str(&report)?;
        let config_keys: Vec<Vec<String>> =
            serde_json::from_value(report["config_doc_facet_keys"].clone())?;
        let late_key = late_key.to_string();
        let early_key = early_key.to_string();
        assert!(
            config_keys.iter().flatten().any(|key| key == &early_key),
            "captured configuration must retain the facet present at admission"
        );
        assert!(
            config_keys.iter().flatten().all(|key| key != &late_key),
            "a delayed workflow must not acquire configuration published after its capture"
        );
        drop(sql);
        cx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delayed_dispatch_keeps_manifest_and_owned_file_component() -> Res<()> {
        async fn enable_revision(
            cx: &crate::test_support::DaybookTestContext,
            doc_id: &str,
            manifest: &manifest::PlugManifest,
        ) -> Res<()> {
            cx.drawer_repo
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.into(),
                        facets_set: [(
                            daybook_types::doc::FacetKey::from(
                                daybook_types::doc::WellKnownFacetTag::PlugManifest,
                            ),
                            daybook_types::doc::WellKnownFacet::PlugManifest(manifest.clone())
                                .into(),
                        )]
                        .into(),
                        facets_remove: vec![],
                        user_path: None,
                    },
                    daybook_types::doc::BranchPath::new("main"),
                    None,
                )
                .await?;
            let reference: url::Url =
                format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
                    .parse()?;
            cx.rt.plugs_repo.enable_plug(&reference).await?;
            Ok(())
        }

        let cx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
        crate::test_support::import_and_enable_test_plug(&cx).await?;
        let reference = cx
            .rt
            .plugs_repo
            .enabled_ref("@daybook/test")
            .await?
            .unwrap();
        let locator = PlugsRepo::parse_enabled_ref(&reference)?;
        let (_, original) = cx
            .rt
            .plugs_repo
            .materialize_active("@daybook/test", &reference)
            .await?
            .unwrap();
        let mut revision = (*original).clone();
        let manifest::RoutineImpl::Wflow { bundle, key } =
            &revision.routines["report-full-command"].r#impl;
        let bundle_name = bundle.0.clone();
        let handler_key = key.0.clone();
        let component_url = &revision.wflow_bundles[bundle_name.as_str()].component_urls[0];
        let blob_id: crate::blobs::BlobId = component_url.path().trim_start_matches('/').parse()?;
        let mut component = cx.rt.blobs_repo.get_bytes(blob_id).await?;
        let component_path = cx._temp_dir.path().join("captured-component.wasm");
        tokio::fs::write(&component_path, &component).await?;
        let bundle = Arc::make_mut(
            revision
                .wflow_bundles
                .get_mut(bundle_name.as_str())
                .unwrap(),
        );
        eyre::ensure!(
            bundle.component_urls.len() == 1,
            "fixture requires one component"
        );
        bundle.component_urls = vec![url::Url::from_file_path(&component_path).unwrap()];
        revision.version.major += 1;
        enable_revision(&cx, &locator.doc_id, &revision).await?;

        let target = cx
            .drawer_repo
            .add(daybook_types::doc::AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    daybook_types::doc::FacetKey::from(
                        daybook_types::doc::WellKnownFacetTag::LabelGeneric,
                    ),
                    daybook_types::doc::WellKnownFacet::LabelGeneric("seed".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        let (_, heads) = cx
            .drawer_repo
            .get_with_heads(&target, daybook_types::doc::BranchPath::new("main"), None)
            .await?
            .unwrap();
        let args = || DispatchArgs::DocRoutine {
            doc_id: target.clone(),
            branch_path: "main".into(),
            heads: heads.clone(),
            invocation: dispatch::RoutineInvocation::Command,
            changed_facet_keys: vec![],
            wflow_args_json: None,
        };
        let delayed = cx
            .rt
            .dispatch_no_gate_internal(
                "@daybook/test",
                "report-full-command",
                args(),
                vec![],
                vec!["manifest-barrier".into()],
                None,
                false,
            )
            .await?;
        let captured_a = cx.dispatch_repo.get_any(&delayed).await.unwrap();
        assert_eq!(captured_a.status, dispatch::DispatchStatus::Waiting);
        assert_eq!(captured_a.execution.wflow().key, handler_key);

        // A valid component custom section creates byte-distinct artifact B.
        // Its same SDK handler receives B's minimal ACL snapshot, making the
        // durable capability report distinguish B from the waiting A revision.
        component.extend_from_slice(&[0, 10, 9]);
        component.extend_from_slice(b"capture-b");
        tokio::fs::write(&component_path, &component).await?;
        revision.routines.insert(
            "report-full-command".into(),
            Arc::clone(&revision.routines["report-minimal-command"]),
        );
        revision.version.major += 1;
        enable_revision(&cx, &locator.doc_id, &revision).await?;
        let current = cx
            .rt
            .dispatch_no_gate_internal(
                "@daybook/test",
                "report-full-command",
                args(),
                vec![],
                vec![],
                None,
                false,
            )
            .await?;
        cx.rt
            .wait_for_dispatch_end(&current, std::time::Duration::from_secs(120))
            .await?;
        let captured_b = cx.dispatch_repo.get_any(&current).await.unwrap();
        assert_eq!(captured_b.status, dispatch::DispatchStatus::Succeeded);
        assert_eq!(captured_b.execution.wflow().key, handler_key);
        assert_ne!(
            captured_a.execution.workload_id(),
            captured_b.execution.workload_id()
        );
        let dispatch::CapturedWflowExecution::V1 {
            component_blobs: a_blobs,
            ..
        } = &captured_a.execution;
        let dispatch::CapturedWflowExecution::V1 {
            component_blobs: b_blobs,
            ..
        } = &captured_b.execution;
        assert_ne!(a_blobs, b_blobs);
        let path = cx
            .rt
            .sqlite_local_state_repo
            .get_sqlite_file_path("@daybook/test/capability-report")
            .await?;
        let sql = sqlx_utils_rs::SqlCtx::url(&format!("sqlite://{}", path.display())).await?;
        let b_report: String =
            sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?")
                .bind(&target)
                .fetch_one(&sql.read_pool)
                .await?;
        let b_report: serde_json::Value = serde_json::from_str(&b_report)?;
        assert_eq!(b_report["config_doc_facet_keys"], serde_json::json!([]));

        // Removing the original pathname cannot change already captured A.
        tokio::fs::remove_file(&component_path).await?;
        cx.rt
            .release_waiting_dispatches("manifest-barrier", true)
            .await?;
        cx.rt
            .wait_for_dispatch_end(&delayed, std::time::Duration::from_secs(120))
            .await?;
        assert_eq!(
            cx.dispatch_repo.get_any(&delayed).await.unwrap().status,
            dispatch::DispatchStatus::Succeeded
        );
        let a_report: String =
            sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?")
                .bind(&target)
                .fetch_one(&sql.read_pool)
                .await?;
        let a_report: serde_json::Value = serde_json::from_str(&a_report)?;
        assert_ne!(
            a_report["config_doc_facet_keys"],
            b_report["config_doc_facet_keys"]
        );
        assert_eq!(a_report["invocation"]["kind"], "Command");
        drop(sql);
        cx.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn processor_slots_stay_in_the_derived_scope() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_root = temp_root.path().join("repo");
        tokio::fs::create_dir_all(&repo_root).await?;
        let rtx = crate::repo::RepoCtx::init(
            &repo_root,
            crate::repo::RepoOpenOptions::default(),
            "derived-scope-test".into(),
            "derived-scope-test".into(),
        )
        .await?;

        let part_id = crate::part_id_from_label(PROCESSOR_RUNLOG_PARTITION_ID);
        let item_id = Rt::processor_runlog_item_id("doc-1", "@daybook/plabels/label-note");

        // Open ensures the partition in the derived scope.
        assert!(
            rtx.derived_part_store
                .summarize_parts(std::collections::HashSet::from([part_id.clone()]))
                .await??
                .contains_key(&part_id),
            "open should ensure the processor-runlog partition in the derived scope"
        );

        upsert_processor_runlog_item(
            &rtx.derived_part_store,
            "peer-a",
            "doc-1",
            "@daybook/plabels/label-note",
            "token-1",
        )
        .await?;

        // The document scope must not learn about the item at all: the automerge
        // frontier worker reads that scope's match-all part stream as documents.
        assert!(
            rtx.part_store.obj_payload(item_id.clone()).await?.is_none(),
            "processor-runlog items must not be written to the document scope"
        );
        assert!(
            rtx.part_store.obj_parts(item_id.clone()).await?.is_empty(),
            "processor-runlog items must not join a document-scope partition"
        );
        assert_eq!(
            rtx.derived_part_store.obj_parts(item_id).await?,
            vec![part_id]
        );

        rtx.shutdown().await?;
        Ok(())
    }

    fn success_effect_result(job_id: &str) -> PartitionLogEntry {
        PartitionLogEntry::JobEffectResult(wflow::wflow_core::partition::job_events::JobRunEvent {
            job_id: Arc::from(job_id),
            timestamp: jiff::Timestamp::now(),
            effect_id: wflow::wflow_core::partition::effects::EffectId {
                entry_id: 1,
                effect_idx: 0,
            },
            run_id: 0,
            worker_id: None,
            start_at: jiff::Timestamp::now(),
            end_at: jiff::Timestamp::now(),
            result: JobRunResult::Success {
                value_json: Arc::from("{}"),
            },
        })
    }

    /// A dispatch racing cancellation vs. a successful workflow result: a target
    /// doc, a staging branch carrying a facet change, and a processor settlement
    /// success hook. `success_effect` and `cancel` drive the two orderings.
    struct CancelRaceFixture {
        cx: crate::test_support::DaybookTestContext,
        doc_id: daybook_types::doc::DocId,
        title_key: daybook_types::doc::FacetKey,
        staging: daybook_types::doc::BranchPathBuf,
        job_id: String,
        dispatch_id: String,
        slot_key: triage::slots::ProcessorSlotKey,
        capture: triage::slots::ProcessorCapture,
    }

    impl CancelRaceFixture {
        fn title(&self, value: &str) -> daybook_types::doc::FacetRaw {
            daybook_types::doc::WellKnownFacet::TitleGeneric(value.to_string()).into()
        }

        async fn setup(waiting_on_dispatch_ids: Vec<String>) -> Res<Self> {
            let cx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
            // The per-node settlement hook writes derived-scope state; the
            // in-crate test harness builds the RepoCtx directly, so ensure the
            // derived partition is present as a real boot would.
            crate::repo::ensure_derived_partitions(&cx.rt.rcx.derived_part_store).await?;
            let drawer = Arc::clone(&cx.drawer_repo);
            let title_key = daybook_types::doc::FacetKey::from(
                daybook_types::doc::WellKnownFacetTag::TitleGeneric,
            );
            let title: daybook_types::doc::FacetRaw =
                daybook_types::doc::WellKnownFacet::TitleGeneric("base".to_string()).into();

            let doc_id = drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    facets: [(title_key.clone(), title)].into(),
                    user_path: None,
                })
                .await?;
            let main_heads = drawer
                .get_doc_branches(&doc_id)
                .await?
                .ok_or_eyre("missing doc branches after add")?
                .branches
                .get("main")
                .cloned()
                .ok_or_eyre("missing main branch after add")?;

            let staging = daybook_types::doc::BranchPathBuf::from("/tmp/cancel-race-stage");
            let staged_title: daybook_types::doc::FacetRaw =
                daybook_types::doc::WellKnownFacet::TitleGeneric("staged".to_string()).into();
            drawer
                .create_branch_at_heads_from_branch(
                    &doc_id,
                    &staging,
                    daybook_types::doc::BranchPath::new("main"),
                    &main_heads,
                    /*user_path*/ None,
                )
                .await?;
            drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set: [(title_key.clone(), staged_title)].into(),
                        facets_remove: vec![],
                        user_path: None,
                    },
                    &staging,
                    Some(main_heads.clone()),
                )
                .await?;

            let dispatch_id = "cmdinvoke-cancel-race".to_string();
            let job_id = format!("{dispatch_id}-job");
            let slot_key = triage::slots::ProcessorSlotKey {
                document_id: doc_id.clone(),
                branch_path: "main".into(),
                processor_full_id: "@test/cancel-race-proc".into(),
            };
            let capture =
                triage::slots::ProcessorCapture::new(main_heads.clone(), None, [1; 32], [2; 32]);
            cx.dispatch_repo
                .add(
                    dispatch_id.clone(),
                    Arc::new(DispatchAttempt::new(ActiveDispatch {
                        execution: dispatch::CapturedWflowExecution::fixture(),
                        deets: ActiveDispatchDeets::Wflow {
                            wflow_partition_id: Some("part".into()),
                            entry_id: Some(1),
                            plug_id: "@test/plug".into(),
                            routine_name: "routine".into(),
                            bundle_name: "bundle".into(),
                            wflow_job_id: Some(job_id.clone()),
                        },
                        args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                            doc_id: doc_id.clone(),
                            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                            staging_branch_path: staging.clone(),
                            heads: main_heads.clone(),
                            invocation: dispatch::RoutineInvocation::Command,
                            primary_doc: dispatch::DocFacetTokens {
                                doc_id: doc_id.clone(),
                                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                                staging_branch_path: staging.clone(),
                                heads: main_heads.clone(),
                                facet_acl: vec![],
                            },
                            config_docs: vec![],
                            local_state_acl: vec![],
                            command_invoke_acl_snapshot: vec![],
                            wflow_args_json: None,
                        }),
                        status: if waiting_on_dispatch_ids.is_empty() {
                            dispatch::DispatchStatus::Active
                        } else {
                            dispatch::DispatchStatus::Waiting
                        },
                        waiting_on_dispatch_ids,
                        on_success_hooks: vec![DispatchOnSuccessHook::ProcessorSettlement {
                            slot: slot_key.clone(),
                            capture: capture.clone(),
                            domain: None,
                        }],
                    })),
                )
                .await?;

            Ok(Self {
                cx,
                doc_id,
                title_key,
                staging,
                job_id,
                dispatch_id,
                slot_key,
                capture,
            })
        }

        async fn deliver_success(&self) -> Res<()> {
            self.cx
                .rt
                .handle_wflow_entry(/*entry_id*/ 7, success_effect_result(&self.job_id))
                .await
        }

        async fn cancel(&self) -> Res<()> {
            self.cx.rt.cancel_dispatch(&self.dispatch_id).await
        }

        async fn target_title(&self) -> Res<Option<daybook_types::doc::FacetRaw>> {
            let doc = self
                .cx
                .drawer_repo
                .get_doc_with_facets_at_branch(
                    &self.doc_id,
                    daybook_types::doc::BranchPath::new("main"),
                    /*facet_keys*/ None,
                )
                .await?
                .ok_or_eyre("target doc missing")?;
            Ok(doc.facets.get(&self.title_key).cloned())
        }

        async fn staging_exists(&self) -> Res<bool> {
            Ok(self
                .cx
                .drawer_repo
                .get_branch_ref(&self.doc_id, &self.staging)
                .await?
                .is_some())
        }

        async fn settlement(&self) -> Res<Option<triage::slots::ProcessorSettlement>> {
            let slot = self.cx.rt.processor_slots.slot(&self.slot_key).await?;
            Ok(slot
                .settlements()
                .find(|item| item.capture == self.capture)
                .cloned())
        }

        async fn status(&self) -> Res<dispatch::DispatchStatus> {
            Ok(self
                .cx
                .dispatch_repo
                .get_any(&self.dispatch_id)
                .await
                .ok_or_eyre("dispatch missing")?
                .status
                .clone())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_effect_failure_settles_without_publication() -> Res<()> {
        use wflow::wflow_core::partition::job_events::{JobEffectResult, JobEffectResultDeets};
        let fixture = CancelRaceFixture::setup(vec![]).await?;
        let PartitionLogEntry::JobEffectResult(mut event) = success_effect_result(&fixture.job_id)
        else {
            unreachable!();
        };
        event.result = JobRunResult::StepEffect(JobEffectResult {
            step_id: 0,
            attempt_id: 0,
            start_at: event.start_at,
            end_at: event.end_at,
            deets: JobEffectResultDeets::EffectErr(JobError::Terminal {
                error_json: Arc::from("{\"reason\":\"host effect denied\"}"),
            }),
        });
        fixture
            .cx
            .rt
            .handle_wflow_entry(7, PartitionLogEntry::JobEffectResult(event))
            .await?;
        assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Failed);
        assert_eq!(fixture.target_title().await?, Some(fixture.title("base")));
        assert!(!fixture.staging_exists().await?);
        assert!(fixture.settlement().await?.is_none());
        fixture.cx.stop().await?;
        Ok(())
    }

    /// Durable cancellation that wins the ordering forbids the successful result
    /// that arrives afterwards from publishing staging or running success hooks,
    /// and settles the dispatch as Cancelled. This drives the real
    /// `handle_wflow_entry`, so it pins target content and hook effects.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_dispatch_refuses_late_success_publication() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let fixture = CancelRaceFixture::setup(vec![]).await?;

        // Cancellation durably accepted before the success result arrives.
        fixture.cancel().await?;
        fixture.deliver_success().await?;

        assert!(
            fixture.settlement().await?.is_none(),
            "a cancellation-winning dispatch must not run success hooks"
        );
        assert_eq!(
            fixture.target_title().await?,
            Some(fixture.title("base")),
            "a cancellation-winning dispatch must not merge staging into the target"
        );
        assert!(
            !fixture.staging_exists().await?,
            "a cancellation-winning dispatch must clean up its staging branch"
        );
        assert_eq!(
            fixture.status().await?,
            dispatch::DispatchStatus::Cancelled,
            "a cancellation that won the ordering must finish Cancelled"
        );

        fixture.cx.stop().await?;
        Ok(())
    }

    /// A successful publication that wins the ordering settles the dispatch as
    /// Succeeded; a cancellation arriving afterwards must not claim a rollback of
    /// already-published effects.
    #[tokio::test(flavor = "multi_thread")]
    async fn success_finalization_refuses_late_cancellation_rollback() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let fixture = CancelRaceFixture::setup(vec![]).await?;

        // The successful result is finalized before the cancellation arrives.
        fixture.deliver_success().await?;
        assert_eq!(
            fixture.status().await?,
            dispatch::DispatchStatus::Succeeded,
            "a successful publication must settle the dispatch as Succeeded"
        );

        fixture.cancel().await?;

        assert_eq!(
            fixture.target_title().await?,
            Some(fixture.title("staged")),
            "a late cancellation must not roll back published target content"
        );
        assert!(
            !fixture.staging_exists().await?,
            "a successful publication must clean up its staging branch"
        );
        assert_eq!(
            fixture
                .settlement()
                .await?
                .ok_or_eyre("success hook must have run")?
                .capture,
            fixture.capture,
            "a successful publication must run its success hooks"
        );
        assert_eq!(
            fixture.status().await?,
            dispatch::DispatchStatus::Succeeded,
            "a late cancellation must not change an already-settled Succeeded dispatch"
        );

        fixture.cx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_during_claimed_publication_returns_without_waiting() -> Res<()> {
        let fixture = CancelRaceFixture::setup(vec![]).await?;
        let (claimed, claimed_rx) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *fixture.cx.rt.finalization_gate.lock().await = Some(DispatchTestGate {
            reached: claimed,
            resume: resume_rx,
        });
        let ((), ()) = tokio::try_join!(fixture.deliver_success(), async {
            claimed_rx.await?;
            // Publication remains paused until cancellation returns. Any
            // wait over publication deadlocks this test by construction.
            fixture.cancel().await?;
            assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Active);
            assert_eq!(fixture.target_title().await?, Some(fixture.title("base")));
            assert!(fixture.settlement().await?.is_none());
            resume.send(()).expect(ERROR_CHANNEL);
            eyre::Ok(())
        },)?;
        assert_eq!(fixture.target_title().await?, Some(fixture.title("staged")));
        assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Succeeded);
        assert!(!fixture.staging_exists().await?);
        assert_eq!(
            fixture.settlement().await?.unwrap().capture,
            fixture.capture,
        );
        fixture.cx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_success_cannot_publish_or_settle_replacement_attempt() -> Res<()> {
        let fixture = CancelRaceFixture::setup(vec![]).await?;
        let old = fixture
            .cx
            .dispatch_repo
            .get_any(&fixture.dispatch_id)
            .await
            .unwrap();
        let mut deets = old.deets.clone();
        let replacement_job = format!("{}-replacement", fixture.dispatch_id);
        let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &mut deets;
        *wflow_job_id = Some(replacement_job.clone());
        fixture
            .cx
            .dispatch_repo
            .update_active_deets(&fixture.dispatch_id, &old, deets)
            .await?;

        fixture.deliver_success().await?;
        assert_eq!(fixture.target_title().await?, Some(fixture.title("base")));
        assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Active);
        assert!(fixture.staging_exists().await?);
        assert!(fixture.settlement().await?.is_none());

        fixture
            .cx
            .rt
            .handle_wflow_entry(8, success_effect_result(&replacement_job))
            .await?;
        assert_eq!(fixture.target_title().await?, Some(fixture.title("staged")));
        assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Succeeded);
        assert!(!fixture.staging_exists().await?);
        assert_eq!(
            fixture.settlement().await?.unwrap().capture,
            fixture.capture
        );
        fixture.cx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_ready_dispatch_refuses_waiting_activation() -> Res<()> {
        let fixture = CancelRaceFixture::setup(vec!["dependency".into()]).await?;
        let (ready, ready_rx) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        *fixture.cx.rt.waiting_activation_gate.lock().await = Some(DispatchTestGate {
            reached: ready,
            resume: resume_rx,
        });
        tokio::try_join!(
            fixture.cx.rt.release_waiting_dispatches("dependency", true),
            async {
                ready_rx.await?;
                fixture.cancel().await?;
                assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Cancelled);
                resume.send(()).expect(ERROR_CHANNEL);
                eyre::Ok(())
            },
        )?;
        assert_eq!(fixture.status().await?, dispatch::DispatchStatus::Cancelled);
        assert_eq!(fixture.target_title().await?, Some(fixture.title("base")));
        assert!(fixture.settlement().await?.is_none());
        assert!(
            fixture
                .cx
                .dispatch_repo
                .get_active(&fixture.dispatch_id)
                .await
                .is_none()
        );
        assert!(
            fixture
                .cx
                .dispatch_repo
                .get_by_wflow_job(&fixture.job_id)
                .await
                .is_none()
        );
        fixture.cx.stop().await?;
        Ok(())
    }
}
