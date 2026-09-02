use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use big_sync_core::revisioned_store::RevisionReadLimits;
use daybook_core::plugs::{OciImportOptions, PlugsRepo, PlugsRevisionSelector, PlugsWatchChange};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, uniffi::Record)]
pub struct PlugSummary {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub title: String,
    pub desc: String,
    pub facet_count: u32,
    pub view_count: u32,
    pub routine_count: u32,
    pub processor_count: u32,
    pub command_count: u32,
}

#[derive(uniffi::Object)]
pub struct PlugsRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<PlugsRepo>,
    registry: Arc<daybook_core::repos::ListenersRegistry>,
    watch_cancel_token: CancellationToken,
    watch_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for PlugsRepoFfi {
    type Event = PlugsWatchChange;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.watch_cancel_token
    }
}

crate::uniffi_repo_listeners!(PlugsRepoFfi, PlugsWatchChange);

#[uniffi::export]
impl PlugsRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, blobs_repo))]
    async fn load(
        fcx: SharedFfiCtx,
        blobs_repo: Arc<crate::repos::blobs::BlobsRepoFfi>,
    ) -> Result<Arc<Self>, FfiError> {
        let (repo, stop_token) = fcx
            .do_on_rt(PlugsRepo::load(
                Arc::clone(&fcx.rcx.big_repo),
                Arc::clone(&blobs_repo.repo),
                fcx.rcx.doc_app.document_id(),
                daybook_types::doc::UserPathBuf::from(fcx.rcx.local_user_path.clone()),
                Arc::clone(&fcx.rcx.sqlite_local_state_repo),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        let registry = daybook_core::repos::ListenersRegistry::new();
        let watch_cancel_token = CancellationToken::new();
        let watch_repo = Arc::clone(&repo);
        let watch_registry = Arc::clone(&registry);
        let watch_cancel = watch_cancel_token.clone();
        let watch_handle = fcx
            .do_on_rt(async move {
                Some(tokio::spawn(async move {
                    let mut watch = watch_repo
                        .watch(PlugsRevisionSelector::All)
                        .await
                        .expect(ERROR_IMPOSSIBLE);
                    loop {
                        let read = tokio::select! {
                            _ = watch_cancel.cancelled() => return,
                            read = watch.next(RevisionReadLimits::default()) => read.expect(ERROR_IMPOSSIBLE),
                        };
                        let big_sync_core::revisioned_store::RevisionRead::Entries {
                            entries, ..
                        } = read
                        else {
                            unreachable!("PlugsWatch hides replay completion")
                        };
                        watch_registry.notify(entries);
                    }
                }))
            })
            .await
            .expect(ERROR_IMPOSSIBLE);
        Ok(Arc::new(Self {
            fcx,
            repo,
            registry,
            watch_cancel_token,
            watch_handle: Some(watch_handle).into(),
            stop_token: Some(stop_token).into(),
        }))
    }

    async fn stop(&self) -> Result<(), FfiError> {
        self.watch_cancel_token.cancel();
        let watch_handle = self.watch_handle.lock().await.take();
        let stop_token = self.stop_token.lock().await.take();
        self.fcx
            .do_on_rt(async move {
                if let Some(handle) = watch_handle {
                    handle.await.expect(ERROR_IMPOSSIBLE);
                }
                if let Some(token) = stop_token {
                    token.stop().await?;
                }
                Ok::<(), FfiError>(())
            })
            .await
    }

    async fn import_from_oci_layout(&self, path: String) -> Result<(), FfiError> {
        let repo = Arc::clone(&self.repo);
        let path = PathBuf::from(path);
        self.fcx
            .do_on_rt(async move {
                repo.import_from_oci_layout(&path, OciImportOptions::default())
                    .await?;
                Ok::<(), FfiError>(())
            })
            .await
    }

    async fn inspect_oci_layout(&self, path: String) -> Result<PlugSummary, FfiError> {
        let repo = Arc::clone(&self.repo);
        let path = PathBuf::from(path);
        self.fcx
            .do_on_rt(async move {
                let manifest = repo.inspect_oci_layout(&path).await?;
                Ok::<_, FfiError>(plug_summary_from_manifest(manifest))
            })
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_plugs(&self) -> Vec<PlugSummary> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                let mut plugs = repo
                    .list_plugs()
                    .await
                    .into_iter()
                    .map(|manifest| PlugSummary {
                        id: manifest.id(),
                        namespace: manifest.namespace.clone(),
                        name: manifest.name.clone(),
                        version: manifest.version.to_string(),
                        title: manifest.title.clone(),
                        desc: manifest.desc.clone(),
                        facet_count: manifest.facets.len().try_into().unwrap(),
                        view_count: manifest.views.len().try_into().unwrap(),
                        routine_count: manifest.routines.len().try_into().unwrap(),
                        processor_count: manifest.processors.len().try_into().unwrap(),
                        command_count: manifest.commands.len().try_into().unwrap(),
                    })
                    .collect::<Vec<_>>();
                plugs.sort_by(|left, right| left.id.cmp(&right.id));
                plugs
            })
            .await
    }
}

fn plug_summary_from_manifest(manifest: daybook_types::manifest::PlugManifest) -> PlugSummary {
    PlugSummary {
        id: manifest.id(),
        namespace: manifest.namespace,
        name: manifest.name,
        version: manifest.version.to_string(),
        title: manifest.title,
        desc: manifest.desc,
        facet_count: manifest.facets.len().try_into().unwrap(),
        view_count: manifest.views.len().try_into().unwrap(),
        routine_count: manifest.routines.len().try_into().unwrap(),
        processor_count: manifest.processors.len().try_into().unwrap(),
        command_count: manifest.commands.len().try_into().unwrap(),
    }
}
