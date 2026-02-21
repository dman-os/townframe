use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::repos::plugs::PlugsRepoFfi;
use crate::repos::progress::ProgressRepoFfi;
use daybook_core::config::{ConfigEvent, ConfigRepo};
use daybook_core::plugs::manifest::FacetDisplayHint;
use daybook_core::progress::{
    CreateProgressTaskArgs, ProgressFinalState, ProgressRetentionPolicy, ProgressSeverity,
    ProgressUnit, ProgressUpdate, ProgressUpdateDeets,
};

#[derive(uniffi::Record)]
pub struct FacetKeyDisplayHintEntry {
    pub key: String,
    pub config: FacetDisplayHint,
}

#[derive(uniffi::Object)]
pub struct ConfigRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<ConfigRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for ConfigRepoFfi {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(ConfigRepoFfi, ConfigEvent);

#[uniffi::export]
impl ConfigRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, plug_repo))]
    async fn load(fcx: SharedFfiCtx, plug_repo: Arc<PlugsRepoFfi>) -> Result<Arc<Self>, FfiError> {
        let fcx = Arc::clone(&fcx);
        let cx = Arc::clone(fcx.repo_ctx());
        let (repo, stop_token) = fcx
            .do_on_rt(ConfigRepo::load(
                cx.acx().clone(),
                cx.doc_app().document_id().clone(),
                Arc::clone(&plug_repo.repo),
                daybook_types::doc::UserPath::from(cx.local_user_path().to_string()),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self {
            fcx,
            repo,
            stop_token: Some(stop_token).into(),
        }))
    }

    async fn stop(&self) -> Result<(), FfiError> {
        if let Some(token) = self.stop_token.lock().await.take() {
            token.stop().await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_facet_display_hint(&self, id: String) -> Option<FacetDisplayHint> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move { repo.get_facet_display_hint(id).await })
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_display_hints(self: Arc<Self>) -> HashMap<String, FacetDisplayHint> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move { repo.list_display_hints().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_facet_display_hint(
        &self,
        key: String,
        config: FacetDisplayHint,
    ) -> Result<(), FfiError> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                repo.set_facet_display_hint(key, config)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn get_mltools_config_json(&self) -> Result<String, FfiError> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                let config = repo.get_mltools_config().await;
                serde_json::to_string(&config)
                    .map_err(eyre::Report::from)
                    .map_err(FfiError::from)
            })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_mltools_config_json(&self, config_json: String) -> Result<(), FfiError> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                let config = serde_json::from_str(&config_json).map_err(eyre::Report::from)?;
                repo.set_mltools_config(config).await?;
                Ok::<(), FfiError>(())
            })
            .await
    }

    #[tracing::instrument(err, skip(self, progress_repo))]
    async fn provision_mobile_default_mltools(
        &self,
        progress_repo: Arc<ProgressRepoFfi>,
    ) -> Result<(), FfiError> {
        let repo = Arc::clone(&self.repo);
        let repo_root = self.fcx.repo_ctx().repo_root().to_path_buf();
        self.fcx
            .do_on_rt(async move {
                let task_id = "mltools/mobile_default".to_string();
                let tags = vec![
                    "/type/download".to_string(),
                    "/mltools/model/mobile_default".to_string(),
                    "/mltools/model/nomic-ai/nomic-embed-text-v1.5".to_string(),
                ];
                progress_repo
                    .repo
                    .upsert_task(CreateProgressTaskArgs {
                        id: task_id.clone(),
                        tags,
                        retention: ProgressRetentionPolicy::UserDismissable,
                    })
                    .await?;

                let emit_status = |message: String, severity| ProgressUpdate {
                    at: jiff::Timestamp::now(),
                    title: Some("MLTools mobile_default".to_string()),
                    deets: ProgressUpdateDeets::Status { severity, message },
                };

                progress_repo
                    .repo
                    .add_update(
                        &task_id,
                        emit_status(
                            "starting model provisioning".to_string(),
                            ProgressSeverity::Info,
                        ),
                    )
                    .await?;

                let progress_repo_for_cb = Arc::clone(&progress_repo.repo);
                let task_id_for_cb = task_id.clone();
                let observer = mltools::models::MobileDefaultObserver::new(move |event| {
                    let repo = Arc::clone(&progress_repo_for_cb);
                    let task_id = task_id_for_cb.clone();
                    tokio::spawn(async move {
                        let update = match event {
                            mltools::models::MobileDefaultEvent::DownloadStarted {
                                source,
                                file,
                            } => ProgressUpdate {
                                at: jiff::Timestamp::now(),
                                title: Some("MLTools mobile_default".to_string()),
                                deets: ProgressUpdateDeets::Status {
                                    severity: ProgressSeverity::Info,
                                    message: format!("{source}: starting {file}"),
                                },
                            },
                            mltools::models::MobileDefaultEvent::DownloadProgress {
                                source,
                                file,
                                downloaded_bytes,
                                total_bytes,
                            } => ProgressUpdate {
                                at: jiff::Timestamp::now(),
                                title: Some("MLTools mobile_default".to_string()),
                                deets: ProgressUpdateDeets::Amount {
                                    severity: ProgressSeverity::Info,
                                    done: downloaded_bytes,
                                    total: total_bytes,
                                    unit: ProgressUnit::Bytes,
                                    message: Some(format!("{source}: {file}")),
                                },
                            },
                            mltools::models::MobileDefaultEvent::DownloadCompleted {
                                source,
                                file,
                            } => ProgressUpdate {
                                at: jiff::Timestamp::now(),
                                title: Some("MLTools mobile_default".to_string()),
                                deets: ProgressUpdateDeets::Status {
                                    severity: ProgressSeverity::Info,
                                    message: format!("{source}: completed {file}"),
                                },
                            },
                            mltools::models::MobileDefaultEvent::DownloadFailed {
                                source,
                                file,
                                message,
                            } => ProgressUpdate {
                                at: jiff::Timestamp::now(),
                                title: Some("MLTools mobile_default".to_string()),
                                deets: ProgressUpdateDeets::Status {
                                    severity: ProgressSeverity::Error,
                                    message: format!("{source}: failed {file}: {message}"),
                                },
                            },
                        };
                        repo.add_update(&task_id, update).await.unwrap_or_log();
                    });
                });

                let download_dir = repo_root.join("mltools/mobile_default");
                let provision_result: Res<_> =
                    mltools::models::mobile_default_with_observer(download_dir, Some(&observer))
                        .await;

                match provision_result {
                    Ok(config) => {
                        repo.set_mltools_config(config).await?;
                        progress_repo
                            .repo
                            .add_update(
                                &task_id,
                                ProgressUpdate {
                                    at: jiff::Timestamp::now(),
                                    title: Some("MLTools mobile_default".to_string()),
                                    deets: ProgressUpdateDeets::Completed {
                                        state: ProgressFinalState::Succeeded,
                                        message: Some("model provisioning completed".to_string()),
                                    },
                                },
                            )
                            .await?;
                        Ok::<(), FfiError>(())
                    }
                    Err(err) => {
                        progress_repo
                            .repo
                            .add_update(
                                &task_id,
                                emit_status(
                                    format!("download failed: {err:#}"),
                                    ProgressSeverity::Error,
                                ),
                            )
                            .await
                            .unwrap_or_log();
                        progress_repo
                            .repo
                            .add_update(
                                &task_id,
                                ProgressUpdate {
                                    at: jiff::Timestamp::now(),
                                    title: Some("MLTools mobile_default".to_string()),
                                    deets: ProgressUpdateDeets::Completed {
                                        state: ProgressFinalState::Failed,
                                        message: Some("model provisioning failed".to_string()),
                                    },
                                },
                            )
                            .await
                            .unwrap_or_log();
                        Err(err.into())
                    }
                }
            })
            .await
    }
}
