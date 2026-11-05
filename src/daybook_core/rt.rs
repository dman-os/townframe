use crate::interlude::*;
use wflow::wflow_core::partition::job_events::JobError;
use wflow::wflow_core::partition::job_events::JobRunResult;

use crate::config::ConfigRepo;
use crate::index::DocFacetRefIndexRepo;
use crate::index::DocFacetSetIndexRepo;
use crate::local_state::SqliteLocalStateRepo;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use crate::repo::RepoCtx;
use big_repo::SharedPartStore;
use daybook_types::manifest;

use wash_runtime::host::Host as WashHost;
use wash_runtime::host::HostApi;
use wash_runtime::types::Component;
use wash_runtime::wit::WitInterface;

pub mod dispatch;
pub mod init;
/// Repository-database tables owned by the runtime: dispatch bookkeeping
/// (`dispatch`) and per-node init completion (`init`).
pub(crate) mod sqlite {
    pub(crate) mod queries {
        include!("rt/sqlite/gen.rs");
    }
}
pub mod triage;
pub mod wash_plugin;

use dispatch::ActiveDispatch;
use dispatch::ActiveDispatchArgs;
use dispatch::ActiveDispatchDeets;
use dispatch::DispatchOnSuccessHook;
use dispatch::DispatchRepo;
use dispatch::FacetRoutineArgs;
use dispatch::facet_routine_args_fingerprint;
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
    /// Whether to spawn the background blob workers: pins-part, pin, encryption.
    ///
    /// Off for tests that drive one of these workers themselves. An ambient
    /// worker shares the document, the domain and the facet keys such a test
    /// asserts on, so its assertions stop being attributable to the code under
    /// test, and two writers race the same facet (a concurrent append to the
    /// `Blob` facet's url list merges into two identical entries).
    pub spawn_blob_workers: bool,
}

pub struct Rt {
    pub config: RtConfig,
    pub rcx: Arc<RepoCtx>,
    pub cancel_token: tokio_util::sync::CancellationToken,
    pub plugs_repo: Arc<PlugsRepo>,
    pub drawer: Arc<DrawerRepo>,
    pub config_repo: Arc<ConfigRepo>,
    pub authority: Arc<wflow::AuthorityRuntime>,
    pub dispatch_repo: Arc<dispatch::DispatchRepo>,
    pub init_repo: Arc<InitRepo>,
    pub progress_repo: Arc<crate::progress::ProgressRepo>,
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
    result_watchers: Arc<tokio_util::task::TaskTracker>,
    waiter_jobs: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Sender half of the §15 rotation trigger (`rotation_channel()`); held
    /// only when blob workers spawn. `None` otherwise — a rotation request on a
    /// repo that runs no encryption worker is an error at the call site, not a
    /// silently dropped message.
    doc_rotation_tx:
        Option<tokio::sync::mpsc::Sender<crate::blobs::encryption_worker::RotationRequest>>,
}

pub struct RtStopToken {
    rt: Arc<Rt>,
    result_watchers: Arc<tokio_util::task::TaskTracker>,
    doc_processor_stop: crate::rt::triage::DocProcessorStopToken,
    blob_pin_worker_stop: Option<crate::repos::RepoStopToken>,
    blob_pins_part_worker_stop: Option<crate::repos::RepoStopToken>,
    blob_encryption_worker_stop: Option<crate::repos::RepoStopToken>,
    doc_facet_set_index_stop: crate::repos::RepoStopToken,
    plugs_config_consumer_stop: crate::repos::RepoStopToken,
    plugs_manifest_consumer_stop: crate::repos::RepoStopToken,
    doc_facet_ref_index_stop: crate::repos::RepoStopToken,
}

impl RtStopToken {
    pub async fn stop(self) -> Res<()> {
        self.rt.cancel_token.cancel();

        self.result_watchers.close();
        self.result_watchers.wait().await;
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
        if let Some(stop) = self.blob_encryption_worker_stop
            && let Err(err) = stop.stop().await
        {
            warn!(
                ?err,
                "error stopping blob_encryption_worker during shutdown - continuing"
            );
        }
        if let Some(stop) = self.blob_pins_part_worker_stop
            && let Err(err) = stop.stop().await
        {
            warn!(
                ?err,
                "error stopping blob_pins_part_worker during shutdown - continuing"
            );
        }
        if let Some(stop) = self.blob_pin_worker_stop
            && let Err(err) = stop.stop().await
        {
            warn!(
                ?err,
                "error stopping blob_pin_worker during shutdown - continuing"
            );
        }

        self.rt.authority.stop().await;

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
    /// §15 trigger: rotate the document's existing cipherBlob representations —
    /// fresh key document, re-encrypted ciphertext, pair roots re-rooted
    /// through the worker's keyed-task machinery (ADR 003 §15/§19). The task
    /// shares the reconcile/delta scheduler and its budget, so a rotation for a
    /// document cannot race its install/delta work.
    ///
    /// Errors when this repo spawned no encryption worker (`spawn_blob_workers`
    /// unset): refusing here is the runtime telling the caller the worker that
    /// owns the rotation does not exist, rather than the request vanishing into
    /// a channel nobody reads.
    pub async fn request_doc_representations_rotation(
        &self,
        doc_id: daybook_types::doc::DocId,
    ) -> Res<()> {
        let Some(rotation_tx) = self.doc_rotation_tx.as_ref() else {
            return Err(eyre::eyre!(
                "cannot rotate {:?}: no blob encryption worker is running in this repo",
                &doc_id
            ));
        };
        rotation_tx
            .send(crate::blobs::encryption_worker::RotationRequest {
                doc_id: doc_id.clone(),
            })
            .await
            .map_err(|_| eyre::eyre!("the encryption worker's rotation channel closed"))
    }
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
        // A failed boot must not leak the workers it already started: every
        // task below is a child of `cancel_token` (each worker holds a child
        // token), so cancelling the token on any error return tears every
        // child down - the same effect as aborting a task set, expressed
        // through the runtime's own cancellation primitive. Disarmed exactly
        // where boot hands the token to the returned stop token.
        struct CancelTokenOnBootError(Option<tokio_util::sync::CancellationToken>);
        impl Drop for CancelTokenOnBootError {
            fn drop(&mut self) {
                if let Some(token) = self.0.take() {
                    token.cancel();
                }
            }
        }
        let mut cancel_on_boot_error = CancelTokenOnBootError(Some(cancel_token.clone()));
        let authority = crate::authority::ensure(&rcx.big_repo, &rcx.sql, None).await?;
        crate::repo::ensure_authority_partitions(
            &rcx.part_store,
            &authority,
            &rcx.core_inventory_doc_id,
            &rcx.docs_inventory_doc_id,
        )
        .await?;

        // The blob presence plane's write edge (ADR 003 §13): from here on,
        // every put/download announces itself, and the boot announce replays
        // the blobs already on disk. Both are idempotent in the store, so a
        // restart costs row reads, and the encryption worker's feeder can open
        // its stream before any of these facts is missed.
        blobs_repo.set_blob_presence_sink(Arc::clone(&rcx.blob_presence_store));
        blobs_repo.announce_held_blobs().await?;
        Self::emit_startup_progress_status(
            &progress_repo,
            startup_progress_task_id.as_deref(),
            "rt boot: ensured partitions".to_string(),
        )
        .await?;

        let wflow_db = rcx.sqlite_runtime.database(
            rcx.layout.repo_root.join("wflows.db"),
            &[],
            "_sqlite_rs_migrations",
        );
        let wcx = wflow::Ctx::init(wflow_db).await?;
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
        let blob_pins_part_worker_stop = if config.spawn_blob_workers {
            Some(
                crate::blobs::spawn_blob_pins_part_worker(
                    Arc::clone(&rcx.blob_part_store),
                    Arc::clone(&sqlite_local_state_repo),
                    Arc::clone(&drawer),
                    doc_facet_set_index_repo.revision_store(),
                    cancel_token.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        let blob_pin_worker_stop = if config.spawn_blob_workers {
            Some(
                crate::blobs::spawn_blob_pin_worker(crate::blobs::BlobPinWorkerArgs {
                    drawer_repo: Arc::clone(&drawer),
                    sql: rcx.sql.clone(),
                    core_inventory_doc_id: rcx.core_inventory_doc_id.clone(),
                    docs_inventory_doc_id: rcx.docs_inventory_doc_id.clone(),
                    encryption_inventory_doc_id: rcx.encryption_inventory_doc_id.clone(),
                    blobs_repo: Arc::clone(&blobs_repo),
                    facet_set_store: doc_facet_set_index_repo.revision_store(),
                    plugs_repo: Arc::clone(&plugs_repo),
                    parent_cancel_token: cancel_token.clone(),
                })
                .await?,
            )
        } else {
            None
        };
        // The blob-inventory access rows (ADR 013) belong to whoever serves those parts to
        // peers, so `IrohSyncRepo::boot` owns that writer, not this runtime.
        // Started after the pin workers, so it stops before them: it is the
        // worker that writes the facets their pin derivation reads. Without an
        // encrypted-representation inventory it refuses to run - installing a
        // representation nothing can advertise or release is the one option that
        // leaks (ADR 003 §19).
        let mut doc_rotation_tx_sender: Option<
            tokio::sync::mpsc::Sender<crate::blobs::encryption_worker::RotationRequest>,
        > = None;
        let blob_encryption_worker_stop = if config.spawn_blob_workers {
            Some(
                crate::blobs::spawn_blob_encryption_worker(
                    crate::blobs::BlobEncryptionWorkerArgs {
                        drawer_repo: Arc::clone(&drawer),
                        sql: rcx.sql.clone(),
                        blobs_repo: Arc::clone(&blobs_repo),
                        facet_set_store: doc_facet_set_index_repo.revision_store(),
                        facet_index: Arc::clone(&doc_facet_set_index_repo),
                        domain_group: authority.encryption_candidates.clone(),
                        encryption_inventory_doc_id: rcx.encryption_inventory_doc_id.clone(),
                        parent_cancel_token: cancel_token.clone(),
                        // A rotation request for an unspawned worker is refused
                        // at `Rt::request_doc_representations_rotation`, so the
                        // channel existence mirrors `spawn_blob_workers`.
                        rotation_rx: {
                            let (doc_rotation_tx, doc_rotation_rx) =
                                crate::blobs::encryption_worker::rotation_channel();
                            doc_rotation_tx_sender = Some(doc_rotation_tx);
                            Some(doc_rotation_rx)
                        },
                        // The §15 trigger rides with the worker: the runtime
                        // holds the sender, tests/other callers hold theirs.
                        feeder_repo_part_store: Arc::clone(&rcx.part_store),
                        feeder_eligibility_part: authority.encryption_candidates_part_id(),
                        feeder_presence_store: Arc::clone(&rcx.blob_presence_store),
                    },
                )
                .await?,
            )
        } else {
            None
        };
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

        let authority_host: Arc<dyn wflow::wflow_tokio::authority_runtime::session::Host> =
            Arc::clone(&wflow_plugin);
        let authority = Arc::new(wcx.start_authority_runtime(0, authority_host).await?);
        let result_watchers = Arc::new(tokio_util::task::TaskTracker::new());
        let waiter_jobs = Arc::new(std::sync::Mutex::new(HashSet::new()));

        let rt = Arc::new(Self {
            config,
            doc_rotation_tx: doc_rotation_tx_sender,
            cancel_token,
            plugs_repo,
            drawer,
            rcx,
            authority,
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
            result_watchers: Arc::clone(&result_watchers),
            waiter_jobs,
        });
        rt.daybook_plugin.attach_rt(Arc::downgrade(&rt));

        let plugs_config_sql = sqlite_local_state_repo
            .ensure_sqlite_db(crate::plugs::PLUGS_CONFIG_CONSUMER_STATE_ID)
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
            .ensure_sqlite_db(crate::plugs::PLUG_MANIFEST_CONSUMER_STATE_ID)
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

        for (dispatch_id, dispatch) in rt.dispatch_repo.list().await {
            if dispatch.status == dispatch::DispatchStatus::Active
                && let ActiveDispatchDeets::Wflow {
                    wflow_job_id: Some(job_id),
                    ..
                } = &dispatch.deets
            {
                rt.install_wflow_waiter(dispatch_id, job_id.clone());
            }
        }

        cancel_on_boot_error.0.take();
        Ok((
            Arc::clone(&rt),
            RtStopToken {
                result_watchers,
                rt,
                doc_processor_stop,
                blob_pin_worker_stop,
                blob_pins_part_worker_stop,
                blob_encryption_worker_stop,
                doc_facet_set_index_stop,
                plugs_config_consumer_stop,
                plugs_manifest_consumer_stop,
                doc_facet_ref_index_stop,
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

            let plugin_bindings = wash_runtime::plugin::PluginBindings::default();
            let host_ref = self.wash_host.host_ref();
            let meters = wash_runtime::observability::Meters::default();
            let resolved_workload = unresolved_workload
                .resolve(Some(&plugins), &plugin_bindings, &host_ref, &meters)
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
            let response = store
                .run_concurrent(async move |accessor| {
                    instance
                        .townframe_daybook_stateless_view()
                        .call_render_facet_view(accessor, args)
                        .await
                })
                .await
                .map_err(|err| eyre::eyre!("error rendering stateless view: {err}"))?;
            let response =
                response.map_err(|err| eyre::eyre!("error calling stateless view: {err}"))?;
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

    fn install_wflow_waiter(self: &Arc<Self>, dispatch_id: String, job_id: String) {
        assert!(
            self.waiter_jobs
                .lock()
                .expect(ERROR_MUTEX)
                .insert(job_id.clone()),
            "workflow waiter registered twice for job {job_id}"
        );
        let rt = Arc::clone(self);
        self.result_watchers.spawn(async move {
            tokio::select! {
                _ = rt.cancel_token.cancelled() => {
                    assert!(rt.waiter_jobs.lock().expect(ERROR_MUTEX).remove(&job_id));
                }
                receipt = rt.authority.handle(job_id.clone()).await_receipt() => {
                    let receipt = receipt.expect("active persisted workflow dispatch lost its authority receipt");
                    rt.handle_wflow_receipt(dispatch_id, job_id.clone(), receipt).await.unwrap();
                    assert!(rt.waiter_jobs.lock().expect(ERROR_MUTEX).remove(&job_id));
                }
            }
        });
    }

    #[tracing::instrument(skip(self, receipt), err(Debug))]
    async fn handle_wflow_receipt(
        self: &Arc<Self>,
        dispatch_id: String,
        job_id: String,
        receipt: wflow::wflow_core::partition::authority::Receipt,
    ) -> Res<()> {
        let Some(dispatch) = self.dispatch_repo.get_active(&dispatch_id).await else {
            return Ok(());
        };
        let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(active_job_id),
            wflow_key,
            ..
        } = &dispatch.deets
        else {
            return Ok(());
        };
        if active_job_id != &job_id {
            debug!(%dispatch_id, %job_id, active_job_id, %wflow_key, "ignoring stale authority receipt");
            return Ok(());
        }
        let result = match &receipt.outcome {
            wflow::wflow_core::partition::authority::Outcome::Completed(terminal) => {
                match serde_json::from_str::<wflow::wflow_wdk::types::JobResult>(&terminal.value)
                    .wrap_err("authority completed receipt contained invalid JobResult")?
                {
                    Ok(value_json) => JobRunResult::Success {
                        value_json: value_json.into(),
                    },
                    Err(wflow::wflow_wdk::types::JobError::Terminal(error_json)) => {
                        JobRunResult::WflowErr(JobError::Terminal {
                            error_json: error_json.into(),
                        })
                    }
                    Err(wflow::wflow_wdk::types::JobError::Transient(_)) => {
                        unreachable!(
                            "authority completed receipt contained a transient workflow result"
                        )
                    }
                }
            }
            wflow::wflow_core::partition::authority::Outcome::Faulted(fault) => {
                match &fault.failure {
                    wflow::wflow_core::partition::JobFailure::Cancelled { .. } => {
                        JobRunResult::Aborted
                    }
                    failure => JobRunResult::WorkerErr(
                        wflow::wflow_core::partition::job_events::JobRunWorkerError::Other {
                            msg: format!("authority fault: {failure:?}"),
                        },
                    ),
                }
            }
        };
        let (severity, message) = match &result {
            JobRunResult::Success { .. } => (
                crate::progress::ProgressSeverity::Info,
                "dispatch completed successfully".to_string(),
            ),
            JobRunResult::Aborted => (
                crate::progress::ProgressSeverity::Warn,
                "dispatch aborted".to_string(),
            ),
            JobRunResult::WorkerErr(err) => (
                crate::progress::ProgressSeverity::Error,
                format!("worker error: {err:?}"),
            ),
            JobRunResult::WflowErr(JobError::Terminal { error_json }) => (
                crate::progress::ProgressSeverity::Error,
                format!("terminal error: {error_json}"),
            ),
            JobRunResult::WflowErr(JobError::Transient { error_json, .. }) => (
                crate::progress::ProgressSeverity::Error,
                format!("transient error exhausted: {error_json}"),
            ),
            JobRunResult::StepEffect(_) | JobRunResult::StepWait(_) => {
                unreachable!("authority receipt contained non-terminal workflow result")
            }
        };
        self.progress_repo
            .add_update(
                &dispatch_id,
                crate::progress::ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: None,
                    deets: crate::progress::ProgressUpdateDeets::Status { severity, message },
                },
            )
            .await?;

        {
            // Handle staging branch cleanup based on success/failure
            let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id,
                branch_path: target_branch_path,
                staging_branch_path,
                ..
            }) = &dispatch.args;

            let merged_successfully = matches!(&result, JobRunResult::Success { .. });

            if merged_successfully {
                // Merge staging branch into target branch
                info!(
                    %dispatch_id,
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
                        DispatchOnSuccessHook::ProcessorRunLog { .. } => {}
                        DispatchOnSuccessHook::CommandInvokeReply {
                            parent_wflow_job_id,
                            request_id,
                        } => {
                            let reply = {
                                let (status, value_json, error_json) =
                                    command_invoke_reply_from_result(
                                        &result,
                                        dispatch_id,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.authority.send_message(parent_wflow_job_id, request_id, serde_json::to_string(&reply).expect(ERROR_JSON)).await
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
                        DispatchOnSuccessHook::ProcessorRunLog {
                            doc_id,
                            processor_full_id,
                            done_token,
                        } => {
                            self.record_processor_runlog_done(
                                doc_id,
                                processor_full_id,
                                done_token,
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
                                        &result,
                                        dispatch_id,
                                        merged_successfully,
                                    );
                                daybook_pdk::InvokeCommandReply {
                                    request_id: request_id.clone(),
                                    status,
                                    value_json,
                                    error_json,
                                }
                            };
                            self.authority.send_message(parent_wflow_job_id, request_id, serde_json::to_string(&reply).expect(ERROR_JSON)).await
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
            } else if matches!(&result, JobRunResult::Aborted) {
                dispatch::DispatchStatus::Cancelled
            } else {
                dispatch::DispatchStatus::Failed
            };
            self.progress_repo
                .add_update(
                    &dispatch_id,
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
                .complete(dispatch_id.clone(), final_status.clone())
                .await?;
            self.release_waiting_dispatches(
                &dispatch_id,
                matches!(final_status, dispatch::DispatchStatus::Succeeded),
            )
            .await?;
        }
        Ok(())
    }

    async fn release_waiting_dispatches(
        self: &Arc<Self>,
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
                        DispatchOnSuccessHook::ProcessorRunLog { .. } => {}
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
                            self.authority.send_message(parent_wflow_job_id, request_id, serde_json::to_string(&reply).expect(ERROR_JSON)).await
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
                self.start_waiting_dispatch(waiting_id, ready_dispatch)
                    .await?;
            }
        }
        Ok(())
    }

    async fn record_processor_runlog_done(
        &self,
        doc_id: &str,
        processor_full_id: &str,
        done_token: &str,
    ) -> Res<()> {
        upsert_processor_runlog_item(
            &self.rcx.derived_part_store,
            &self.config.device_id,
            doc_id,
            processor_full_id,
            done_token,
        )
        .await
    }

    async fn start_waiting_dispatch(
        self: &Arc<Self>,
        dispatch_id: String,
        waiting_dispatch: Arc<ActiveDispatch>,
    ) -> Res<()> {
        let ActiveDispatchDeets::Wflow {
            plug_id,
            bundle_name,
            wflow_key,
            wflow_job_id,
            ..
        } = &waiting_dispatch.deets;
        let Some(job_id) = wflow_job_id.as_ref() else {
            eyre::bail!("waiting dispatch missing job id: {dispatch_id}");
        };
        let plug_man = self
            .plugs_repo
            .get(plug_id)
            .await
            .ok_or_else(|| ferr!("plug not found in repo: {plug_id}"))?;
        let bundle_man = plug_man
            .wflow_bundles
            .get(bundle_name.as_str())
            .ok_or_else(|| {
                ferr!("bundle not found in plug manifest: plug={plug_id} bundle={bundle_name}")
            })?;
        ensure_bundle_workload_running(
            &self.wcx,
            &self.wash_host,
            &self.blobs_repo,
            plug_id.clone(),
            bundle_name.clone(),
            bundle_man,
        )
        .await?;
        self.dispatch_repo
            .activate_waiting(&dispatch_id, waiting_dispatch.deets.clone())
            .await?;
        let args_json = match &waiting_dispatch.args {
            ActiveDispatchArgs::FacetRoutine(args) => args
                .wflow_args_json
                .clone()
                .unwrap_or_else(|| serde_json::to_string(&()).expect(ERROR_JSON)),
        };
        if let Err(err) = self
            .authority
            .submit(wflow::Submission::new(
                job_id.clone(),
                wflow_key.clone(),
                args_json,
            ))
            .await
        {
            self.dispatch_repo
                .complete(dispatch_id.clone(), dispatch::DispatchStatus::Failed)
                .await?;
            return Err(ferr!(
                "error scheduling deferred job for dispatch {dispatch_id}: {err:?}"
            ));
        }
        self.install_wflow_waiter(dispatch_id.clone(), job_id.clone());
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

    async fn dispatch_no_gate(
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
        self.ensure_rt_live()?;

        let plug_man = self
            .plugs_repo
            .get(plug_id)
            .await
            .ok_or_else(|| ferr!("plug not found in repo: {plug_id}/{routine_name}"))?;
        let routine_man = plug_man
            .routines
            .get(routine_name)
            .ok_or_else(|| ferr!("routine not found in plug manifest: {plug_id}/{routine_name}"))?;

        let (mut dispatch_id, mut args) = match args {
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

                let mut config_docs_by_owner: std::collections::HashMap<
                    String,
                    Vec<daybook_types::manifest::RoutineFacetAccess>,
                > = std::collections::HashMap::new();
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

                let mut config_docs_by_owner: std::collections::HashMap<
                    String,
                    Vec<daybook_types::manifest::RoutineFacetAccess>,
                > = std::collections::HashMap::new();
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
        if let Some(existing) = self.dispatch_repo.get_any(&dispatch_id).await {
            let can_reuse = serde_json::to_string(&existing.on_success_hooks).expect(ERROR_JSON)
                == serde_json::to_string(&on_success_hooks).expect(ERROR_JSON)
                && existing.waiting_on_dispatch_ids == waiting_on_dispatch_ids;
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

        let (job_id, _staging_branch_path, bundle_name, wflow_key) = match &routine_man.r#impl {
            manifest::RoutineImpl::Wflow {
                key,
                bundle: bundle_name,
            } => {
                // let fqk = format!("{workload_id}/{key}");
                let job_id = format!("{dispatch_id}-{id}", id = Uuid::new_v4().bs58());
                let staging_branch_path =
                    daybook_types::doc::BranchPathBuf::from(format!("/tmp/{}", job_id));

                // Update args with staging branch path
                let ActiveDispatchArgs::FacetRoutine(ref mut facet_args) = args;
                facet_args.staging_branch_path = staging_branch_path.clone();
                facet_args.primary_doc.staging_branch_path = staging_branch_path.clone();

                (
                    job_id,
                    staging_branch_path,
                    bundle_name.0.clone(),
                    key.0.clone(),
                )
            }
        };
        let deets = ActiveDispatchDeets::Wflow {
            plug_id: plug_id.into(),
            routine_name: routine_name.to_string(),
            bundle_name: bundle_name.clone(),
            wflow_key: wflow_key.clone(),
            wflow_job_id: Some(job_id.clone()),
        };
        let active_dispatch = Arc::new(ActiveDispatch {
            args,
            deets,
            status: if is_waiting {
                dispatch::DispatchStatus::Waiting
            } else {
                dispatch::DispatchStatus::Active
            },
            waiting_on_dispatch_ids,
            on_success_hooks,
        });
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
        if is_waiting {
            self.dispatch_repo
                .add(dispatch_id.clone(), Arc::clone(&active_dispatch))
                .await?;
        } else {
            let Some(bundle_man) = plug_man.wflow_bundles.get(bundle_name.as_str()) else {
                return Err(ferr!(
                    "bundle not found in plug manifest: routine={plug_id}/{routine_name} bundle={bundle_name} key={wflow_key}"
                ));
            };
            ensure_bundle_workload_running(
                &self.wcx,
                &self.wash_host,
                &self.blobs_repo,
                plug_id.clone(),
                bundle_name.clone(),
                bundle_man,
            )
            .await?;
            let wflow_args_json = match &active_dispatch.args {
                ActiveDispatchArgs::FacetRoutine(args) => args
                    .wflow_args_json
                    .clone()
                    .unwrap_or_else(|| serde_json::to_string(&()).expect(ERROR_JSON)),
            };
            self.authority
                .submit(wflow::Submission::new(
                    job_id.clone(),
                    wflow_key.clone(),
                    wflow_args_json,
                ))
                .await
                .map_err(|err| {
                    ferr!("error scheduling job for {plug_id}/{routine_name}: {err:?}")
                })?;
            if let Err(add_err) = self
                .dispatch_repo
                .add(dispatch_id.clone(), Arc::clone(&active_dispatch))
                .await
            {
                match self
                    .authority
                    .cancel(
                        job_id.clone(),
                        format!("rollback scheduling for dispatch {dispatch_id}"),
                    )
                    .await
                {
                    Ok(_) | Err(wflow::CancelError::AlreadyTerminal) => {
                        self.authority.receipt(&job_id).await?.ok_or_else(|| {
                            ferr!("authority cancellation settled without receipt for {job_id}")
                        })?;
                    }
                    Err(err) => {
                        return Err(ferr!("failed to cancel admitted job {job_id}: {err:?}")
                            .wrap_err(add_err));
                    }
                }
                return Err(add_err);
            }
            self.install_wflow_waiter(dispatch_id.clone(), job_id.clone());
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

    pub async fn cancel_dispatch(self: &Arc<Self>, dispatch_id: &str) -> Res<()> {
        self.ensure_rt_live()?;

        let dispatch = self
            .dispatch_repo
            .get_any(dispatch_id)
            .await
            .ok_or_else(|| ferr!("dispatch not found under {dispatch_id}"))?;
        if matches!(
            dispatch.status,
            dispatch::DispatchStatus::Succeeded
                | dispatch::DispatchStatus::Failed
                | dispatch::DispatchStatus::Cancelled
        ) {
            return Ok(());
        }
        if matches!(dispatch.status, dispatch::DispatchStatus::Waiting) {
            self.dispatch_repo
                .complete(dispatch_id.to_string(), dispatch::DispatchStatus::Cancelled)
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
        let marked_now = self.dispatch_repo.mark_cancelled(dispatch_id).await?;
        if !marked_now {
            return Ok(());
        }
        let ActiveDispatchDeets::Wflow {
            wflow_job_id: Some(job_id),
            ..
        } = &dispatch.deets
        else {
            eyre::bail!("dispatch {dispatch_id} has no workflow job id");
        };
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
        match self
            .authority
            .cancel(
                job_id.clone(),
                format!("cancel requested for dispatch {dispatch_id}"),
            )
            .await
        {
            Ok(_) => {}
            Err(wflow::CancelError::AlreadyTerminal) => {
                let watcher_installed =
                    self.waiter_jobs.lock().expect(ERROR_MUTEX).contains(job_id);
                if !watcher_installed {
                    let receipt = self.authority.receipt(job_id).await?.ok_or_else(|| {
                        ferr!("authority reported terminal job without receipt: {job_id}")
                    })?;
                    self.handle_wflow_receipt(dispatch_id.to_string(), job_id.clone(), receipt)
                        .await?;
                }
            }
            Err(err) => return Err(ferr!("error cancelling dispatch {dispatch_id}: {err:?}")),
        }
        Ok(())
    }

    /// Wait until a dispatch reaches a terminal state.
    pub async fn wait_for_dispatch_end(
        &self,
        dispatch_id: &str,
        timeout: std::time::Duration,
    ) -> Res<()> {
        self.ensure_rt_live()?;

        use crate::repos::Repo;
        use crate::repos::SubscribeOpts;

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

fn command_invoke_reply_from_result(
    result: &JobRunResult,
    dispatch_id: &str,
    merged_successfully: bool,
) -> (
    daybook_pdk::InvokeCommandStatus,
    Option<String>,
    Option<String>,
) {
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

async fn ensure_bundle_workload_running(
    wcx: &wflow::Ctx,
    wash_host: &WashHost,
    blobs_repo: &BlobsRepo,
    plug_id: String,
    bundle_name: String,
    bundle_man: &manifest::WflowBundleManifest,
) -> Res<String> {
    let workload_id = format!("{plug_id}/{bundle_name}");
    for key in &bundle_man.keys {
        use wflow::wflow_core::metastore::*;
        match wcx.metastore.get_wflow(key).await? {
            Some(meta) => {
                if let WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                    workload_id: meta_workload_id,
                }) = &meta.service
                {
                    if meta_workload_id != &workload_id {
                        eyre::bail!(
                            "wflow under key '{key}' in metatstore '{meta:?}' doesn't match workload id '{workload_id}'",
                        );
                    }
                } else {
                    eyre::bail!(
                        "wflow under key '{key}' in metatstore '{meta:?}' doesn't match workload type for workload '{workload_id}'",
                    );
                }
            }
            None => {
                if let Err(err) = wcx
                    .metastore
                    .set_wflow(
                        &key[..],
                        &WflowMeta {
                            key: key.0.clone(),
                            service: WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                                workload_id: workload_id.clone(),
                            }),
                        },
                    )
                    .await
                {
                    if let Some(meta) = wcx.metastore.get_wflow(key).await? {
                        if let WflowServiceMeta::Wasmcloud(WasmcloudWflowServiceMeta {
                            workload_id: meta_workload_id,
                        }) = &meta.service
                        {
                            if meta_workload_id != &workload_id {
                                return Err(err);
                            }
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }
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
                    plug_id.clone(),
                    bundle_name.clone(),
                    bundle_man,
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
    plug_id: String,
    bundle_name: String,
    bundle_man: &manifest::WflowBundleManifest,
) -> Res<()> {
    // Load wasm bytes from component URLs
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

    let _resp = wash_host
        .workload_start(wash_runtime::types::WorkloadStartRequest {
            workload_id,
            workload: wash_runtime::types::Workload {
                namespace: plug_id,
                name: bundle_name,
                annotations: HashMap::new(),
                service: None,
                components,
                host_interfaces: vec![
                    WitInterface {
                        config: [(
                            "wflow_keys".to_owned(),
                            bundle_man
                                .keys
                                // .iter()
                                // .map(|key| format!("{workload_id}/{key}"))
                                // .collect::<Vec<_>>()
                                .join(","),
                        )]
                        .into(),
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
    use big_sync::HostPartStore;

    async fn make_partition_store()
    -> Res<(std::sync::Arc<dyn HostPartStore>, big_sync_core::PartKey)> {
        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let sql = crate::app::open_database(&runtime, crate::app::SqlConfig::memory()).await?;
        let part_id = crate::part_id_from_label(PROCESSOR_RUNLOG_PARTITION_ID);
        let store =
            big_sync::SqlitePartStore::new(sql, "rt-test", big_sync_core::BuckId::MAX_LEVEL)
                .await?;
        Ok((std::sync::Arc::new(store), part_id))
    }

    #[tokio::test]
    async fn processor_runlog_upsert_is_bounded_for_same_doc_processor() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (store, part_id) = make_partition_store().await?;
        store
            .ensure_part(crate::part_id_from_label(
                crate::rt::PROCESSOR_RUNLOG_PARTITION_ID,
            ))
            .await?;
        let item_id = Rt::processor_runlog_item_id("doc-1", "@daybook/plabels/label-note");

        upsert_processor_runlog_item(
            &store,
            "peer-a",
            "doc-1",
            "@daybook/plabels/label-note",
            "token-1",
        )
        .await?;
        upsert_processor_runlog_item(
            &store,
            "peer-a",
            "doc-1",
            "@daybook/plabels/label-note",
            "token-2",
        )
        .await?;

        assert_eq!(store.obj_parts(item_id.clone()).await?, vec![part_id]);

        let payload = store
            .obj_payload(item_id)
            .await?
            .ok_or_eyre("expected runlog payload row")?;
        assert_eq!(payload["done_by_peer_id"], serde_json::json!("peer-a"));
        assert_eq!(payload["done_token"], serde_json::json!("token-2"));
        Ok(())
    }

    #[tokio::test]
    async fn processor_runlog_stays_in_the_derived_scope() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_root = temp_root.path().join("repo");
        tokio::fs::create_dir_all(&repo_root).await?;
        let rtx = crate::repo::RepoCtx::init(
            Arc::new(sqlite_rs::SqliteRuntime::new(default())),
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
}
