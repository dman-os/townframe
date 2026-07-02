use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use daybook_core::plugs::{OciImportOptions, PlugsEvent, PlugsRepo};
use std::path::PathBuf;

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
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for PlugsRepoFfi {
    type Event = PlugsEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(PlugsRepoFfi, PlugsEvent);

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
        let stop_token = self.stop_token.lock().await.take();
        self.fcx
            .do_on_rt(async move {
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
