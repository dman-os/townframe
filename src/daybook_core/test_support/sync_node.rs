use crate::{
    blobs::BlobsRepo,
    config::ConfigRepo,
    drawer::DrawerRepo,
    plugs::PlugsRepo,
    progress::ProgressRepo,
    repo::{RepoCtx, RepoOpenOptions},
    repos::RepoStopToken,
    rt::{Rt, RtConfig, RtStopToken, dispatch::DispatchRepo, init::InitRepo},
    sync::{IrohSyncRepo, IrohSyncRepoStopToken},
};
use daybook_types::doc::UserPathBuf;
use std::{ops::Deref, path::Path, sync::Arc};
use utils_rs::prelude::*;

// Retry only typed native Pending; denied access and invariant failures remain visible.
pub(crate) async fn native_admission<T, F: std::future::Future<Output = Res<T>>>(
    mut operation: impl FnMut() -> F,
) -> Res<T> {
    loop {
        match operation().await {
            Err(error)
                if matches!(
                    error.downcast_ref::<big_repo::CoordinationError>(),
                    Some(big_repo::CoordinationError::Pending)
                ) =>
            {
                tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY).await;
            }
            result => return result,
        }
    }
}
// The facade owns teardown even when a scenario returns early or unwinds. Its
// state is taken for explicit stop/reopen, so Drop cannot shut it down twice.
pub(crate) struct SyncTestNode {
    state: Option<NodeState>,
}
pub(crate) struct NodeState {
    pub(crate) ctx: Arc<RepoCtx>,
    pub(crate) rt: Arc<Rt>,
    pub(crate) drawer: Arc<DrawerRepo>,
    pub(crate) blobs_repo: Arc<BlobsRepo>,
    pub(crate) progress_repo: Arc<ProgressRepo>,
    pub(crate) plugs_repo: Arc<PlugsRepo>,
    pub(crate) sync_repo: Arc<IrohSyncRepo>,
    owners: Owners,
}
impl Deref for SyncTestNode {
    type Target = NodeState;
    fn deref(&self) -> &NodeState {
        self.state.as_ref().expect("stopped node")
    }
}

// Named owners preserve dependency order; each successful bootstrap step is
// guarded before the next fallible await. Before a router exists, we own blob
// shutdown. Once routed, router shutdown is the sole FsStore shutdown owner.
#[derive(Default)]
pub(crate) struct Owners {
    pub(crate) ctx: Option<Arc<RepoCtx>>,
    pub(crate) big_repo: Option<big_repo::BigRepoStopToken>,
    pub(crate) big_sync: Option<big_sync::StopToken>,
    pub(crate) local_state: Option<RepoStopToken>,
    pub(crate) blobs: Option<Arc<BlobsRepo>>,
    pub(crate) plugs: Option<RepoStopToken>,
    pub(crate) drawer: Option<RepoStopToken>,
    pub(crate) config: Option<RepoStopToken>,
    pub(crate) dispatch: Option<RepoStopToken>,
    pub(crate) progress: Option<RepoStopToken>,
    pub(crate) init: Option<RepoStopToken>,
    pub(crate) rt: Option<RtStopToken>,
    pub(crate) sync: Option<IrohSyncRepoStopToken>,
    stopped: bool,
}
impl Owners {
    pub(crate) fn new(ctx: Arc<RepoCtx>) -> Self {
        let mut owners = Self::default();
        owners.ctx = Some(ctx);
        owners
    }
    // The memory fixture creates RepoCtx after its native authority documents.
    // Guard the native root immediately, then transfer its token into RepoCtx.
    pub(crate) fn before_context(root: big_repo::BigRepoStopToken) -> Self {
        let mut owners = Self::default();
        owners.big_repo = Some(root);
        owners
    }
    pub(crate) async fn stop(&mut self, runtime_first: bool) -> Res<()> {
        if runtime_first && let Some(stop) = self.rt.take() {
            stop.stop().await?;
        }
        if let Some(stop) = self.sync.take() {
            stop.stop().await?;
            drop(self.blobs.take());
        }
        if let Some(stop) = self.rt.take() {
            stop.stop().await?;
        }
        // Reverse construction order: init consumes progress, and runtime
        // consumers stop before their repositories and native context.
        for owner in [
            &mut self.init,
            &mut self.progress,
            &mut self.dispatch,
            &mut self.config,
            &mut self.drawer,
            &mut self.plugs,
        ] {
            if let Some(stop) = owner.take() {
                stop.cancel_token.cancel();
                stop.stop().await?;
            }
        }
        if let Some(blobs) = self.blobs.take() {
            blobs.shutdown().await?;
        }
        if let Some(stop) = self.big_sync.take() {
            stop.stop().await?;
        }
        if let Some(stop) = self.local_state.take() {
            stop.stop().await?;
        }
        if let Some(ctx) = self.ctx.take() {
            ctx.shutdown().await?;
        } else {
            self.big_repo
                .take()
                .expect("unguarded native root")
                .stop()
                .await?;
        }
        self.stopped = true;
        Ok(())
    }
}
impl Drop for Owners {
    fn drop(&mut self) {
        if !self.stopped {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.stop(/*runtime_first*/ false))
            })
            .expect("native node teardown failed");
        }
    }
}
impl NodeState {
    async fn stop(self, runtime_first: bool) -> Res<()> {
        let Self {
            sync_repo,
            mut owners,
            ctx,
            rt,
            drawer,
            blobs_repo,
            progress_repo,
            plugs_repo,
        } = self;
        // Stop tokens retain their live services. Release facade copies before
        // draining children so document handles do not outlive the native hub.
        drop((
            sync_repo,
            rt,
            drawer,
            blobs_repo,
            progress_repo,
            plugs_repo,
            ctx,
        ));
        owners.stop(runtime_first).await
    }
}
impl SyncTestNode {
    pub(crate) async fn open(root: &Path, options: RepoOpenOptions, name: &str) -> Res<Self> {
        // Keep recursive native boot off the scenario's large async poll stack.
        let root = root.to_owned();
        let name = name.to_owned();
        tokio::spawn(async move { Self::open_inner(&root, options, &name).await })
            .await
            .unwrap()
    }
    async fn open_inner(root: &Path, options: RepoOpenOptions, name: &str) -> Res<Self> {
        let ctx = RepoCtx::open(root, options, name.into()).await?;
        let mut owners = Owners::new(Arc::clone(&ctx));
        let blobs =
            BlobsRepo::new(ctx.layout.blobs_root.clone(), ctx.local_user_path.clone()).await?;
        owners.blobs = Some(Arc::clone(&blobs));
        let (plugs, stop) = PlugsRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&blobs),
            ctx.doc_config.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            Arc::clone(&ctx.sqlite_local_state_repo),
        )
        .await?;
        owners.plugs = Some(stop);
        let (drawer, stop) = DrawerRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&ctx.part_store),
            ctx.doc_drawer.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
            ctx.layout.repo_root.join("local_state"),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
            )),
            #[cfg(test)]
            Some(Arc::clone(&plugs)),
            #[cfg(not(test))]
            Arc::clone(&plugs),
        )
        .await?;
        owners.drawer = Some(stop);
        let (config, stop) = ConfigRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            Arc::clone(&plugs),
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
        )
        .await?;
        owners.config = Some(stop);
        let (dispatch, stop) = DispatchRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
        )
        .await?;
        owners.dispatch = Some(stop);
        let (progress, stop) = ProgressRepo::boot(ctx.sql.clone()).await?;
        owners.progress = Some(stop);
        let (init, stop) = InitRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
            Arc::clone(&progress),
            None,
        )
        .await?;
        owners.init = Some(stop);
        let (rt, stop) = Rt::boot(
            RtConfig::new(name.into(), None),
            Arc::clone(&ctx),
            Arc::clone(&drawer),
            Arc::clone(&plugs),
            dispatch,
            Arc::clone(&progress),
            Arc::clone(&blobs),
            Arc::clone(&config),
            init,
            Arc::clone(&ctx.sqlite_local_state_repo),
        )
        .await?;
        owners.rt = Some(stop);
        let (sync_repo, stop) = IrohSyncRepo::boot(
            Arc::clone(&ctx),
            config,
            Arc::clone(&blobs),
            Some(Arc::clone(&progress)),
        )
        .await?;
        owners.sync = Some(stop);
        Ok(Self {
            state: Some(NodeState {
                ctx,
                rt,
                drawer,
                blobs_repo: blobs,
                progress_repo: progress,
                plugs_repo: plugs,
                sync_repo,
                owners,
            }),
        })
    }
    #[cfg(test)]
    pub(crate) fn shutdown_order(&self) -> crate::sync::ShutdownOrder {
        self.owners
            .sync
            .as_ref()
            .expect("stopped transport")
            .shutdown_order()
    }
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.state
            .take()
            .expect("stopped node")
            .stop(/*runtime_first*/ false)
            .await
    }
    #[cfg(test)]
    pub(crate) async fn stop_runtime_first(mut self) -> Res<()> {
        self.state
            .take()
            .expect("stopped node")
            .stop(/*runtime_first*/ true)
            .await
    }
}
impl Drop for SyncTestNode {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(state.stop(/*runtime_first*/ false))
            })
            .expect("native node teardown failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn native_node_error_scope_releases_storage_for_reopen() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("node");
        RepoCtx::init(
            &root,
            RepoOpenOptions::default(),
            "node".into(),
            "teardown".into(),
        )
        .await?
        .shutdown()
        .await?;
        let payload = b"retained across error-scope teardown";
        let mut blob = None;
        let failed: Res<()> = async {
            let node = SyncTestNode::open(&root, RepoOpenOptions::default(), "node").await?;
            blob = Some(node.blobs_repo.put(payload).await?);
            eyre::bail!("scenario refused after publication")
        }
        .await;
        drop(failed.expect_err("exercise the early-error return"));
        // Reopening the same FsStore fails if the error scope leaked its router.
        let reopened = SyncTestNode::open(&root, RepoOpenOptions::default(), "node").await?;
        assert_eq!(reopened.blobs_repo.get_bytes(blob.unwrap()).await?, payload);
        reopened.stop().await?;
        Ok(())
    }
}
