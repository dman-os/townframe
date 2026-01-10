use crate::interlude::*;

use crate::drawer::DrawerEvent;
use crate::plugs::PlugsEvent;
use crate::rt::dispatch::DispatchEvent;
use crate::rt::{DispatchArgs, Rt};
use daybook_types::doc::{Doc, DocId};

use crate::plugs::manifest::{
    DocPredicateClause, KeyGeneric, ProcessorDeets, RoutineManifestDeets,
};
pub use wflow::{PartitionLogIngress, WflowIngress};

#[derive(Default, Debug, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct DocTriageWorkerStateStore {
    pub drawer_heads: Option<ChangeHashSet>,
    pub plug_heads: Option<ChangeHashSet>,
    pub dispatch_heads: Option<ChangeHashSet>,

    pub dispatch_to_job: HashMap<String, (DocId, String, String)>,
    pub job_to_dispatch: HashMap<String, String>,
}

#[async_trait]
impl crate::stores::Store for DocTriageWorkerStateStore {
    const PROP: &str = "triage";
}

/// Worker that listens to drawer events and schedules workflows
pub struct DocTriageWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl DocTriageWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5 * 1000).await?;
        Ok(())
    }
}

impl Drop for DocTriageWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort()
        }
    }
}

pub async fn spawn_doc_triage_worker(
    rt: Arc<Rt>,
    app_doc_id: DocumentId,
) -> Res<DocTriageWorkerHandle> {
    use crate::repos::Repo;
    use crate::stores::Store;

    let store = DocTriageWorkerStateStore::load(&rt.acx, &app_doc_id).await?;
    let store = crate::stores::StoreHandle::new(store, rt.acx.clone(), app_doc_id.clone());

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<Arc<DrawerEvent>>();
    let (plug_event_tx, mut plug_event_rx) =
        tokio::sync::mpsc::unbounded_channel::<Arc<PlugsEvent>>();
    let (dispatch_event_tx, mut dispatch_event_rx) =
        tokio::sync::mpsc::unbounded_channel::<Arc<DispatchEvent>>();

    let listener = rt.drawer.register_listener({
        let event_tx = event_tx.clone();
        move |event| event_tx.send(event).expect(ERROR_CHANNEL)
    });

    let plug_listener = rt.plugs_repo.register_listener({
        let plug_event_tx = plug_event_tx.clone();
        move |event| {
            plug_event_tx.send(event).expect(ERROR_CHANNEL);
        }
    });

    let dispatch_listener = rt.dispatch_repo.register_listener({
        let dispatch_event_tx = dispatch_event_tx.clone();
        move |event| {
            dispatch_event_tx.send(event).expect(ERROR_CHANNEL);
        }
    });

    // Catch up on missed events
    let (initial_drawer_heads, initial_dispatch_heads) = store
        .query_sync(|s| (s.drawer_heads.clone(), s.dispatch_heads.clone()))
        .await;

    if let Some(heads) = initial_drawer_heads {
        if !heads.0.is_empty() {
            let events = rt.drawer.diff_events(heads, None).await?;
            for event in events {
                event_tx.send(Arc::new(event).into()).expect(ERROR_CHANNEL);
            }
        }
    }

    if let Some(heads) = initial_dispatch_heads {
        if !heads.0.is_empty() {
            let events = rt.dispatch_repo.diff_events(heads, None).await?;
            for event in events {
                dispatch_event_tx
                    .send(Arc::new(event).into())
                    .expect(ERROR_CHANNEL);
            }
        }
    }

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            // NOTE: we don't want to drop the listeners before we're done
            let _listener = listener;
            let _plug_listener = plug_listener;
            let _dispatch_listener = dispatch_listener;

            let mut worker = DocTriageWorker {
                store,
                rt,
                cached_processors: Vec::new(),
            };
            worker.refresh_processors().await?;

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("DocTriageWorker cancelled");
                        break;
                    }
                    event = plug_event_rx.recv() => {
                        let Some(event) = event  else{
                            break;
                        };
                        worker.handle_plugs_event(event).await?;
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        worker.handle_drawer_event(event).await?;
                    }
                    event = dispatch_event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        worker.handle_dispatch_event(event).await?;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async move {
        fut.await.unwrap_or_log();
    });

    Ok(DocTriageWorkerHandle {
        join_handle: Some(join_handle),
        cancel_token,
    })
}

struct PreparedProcessor {
    plug_id: String,
    routine_name: KeyGeneric,
    predicate: DocPredicateClause,
    routine_deets: RoutineManifestDeets,
}

struct DocTriageWorker {
    rt: Arc<Rt>,
    store: crate::stores::StoreHandle<DocTriageWorkerStateStore>,
    cached_processors: Vec<PreparedProcessor>,
}

impl DocTriageWorker {
    async fn refresh_processors(&mut self) -> Res<()> {
        let plugs = self.rt.plugs_repo.list_plugs().await;
        let mut cached = Vec::new();
        for plug in plugs {
            let plug_id = plug.id();
            for processor in plug.processors.values() {
                match &processor.deets {
                    ProcessorDeets::DocProcessor {
                        predicate,
                        routine_name,
                    } => {
                        let routine = plug.routines.get(routine_name).ok_or_else(|| {
                            ferr!(
                                "routine {} not found in plug {} manifest",
                                routine_name,
                                plug_id
                            )
                        })?;

                        cached.push(PreparedProcessor {
                            plug_id: plug_id.clone(),
                            routine_name: routine_name.clone(),
                            predicate: predicate.clone(),
                            routine_deets: routine.deets.clone(),
                        });
                    }
                }
            }
        }
        self.cached_processors = cached;
        Ok(())
    }

    async fn handle_plugs_event(&mut self, event: Arc<PlugsEvent>) -> Res<()> {
        let heads = match &*event {
            PlugsEvent::ListChanged { heads } => heads.clone(),
            PlugsEvent::PlugChanged { heads, .. } => heads.clone(),
            PlugsEvent::PlugDeleted { heads, .. } => heads.clone(),
        };
        self.store
            .mutate_sync(|s| {
                s.plug_heads = Some(heads);
            })
            .await?;
        self.refresh_processors().await?;
        Ok(())
    }

    async fn handle_dispatch_event(&mut self, event: Arc<DispatchEvent>) -> Res<()> {
        let heads = match &*event {
            DispatchEvent::DispatchAdded { heads, .. } => heads.clone(),
            DispatchEvent::ListChanged { heads } => heads.clone(),
            DispatchEvent::DispatchUpdated { heads, .. } => heads.clone(),
            DispatchEvent::DispatchDeleted { id, heads } => {
                self.store
                    .mutate_sync(|s| {
                        if let Some(job) = s.dispatch_to_job.remove(id) {
                            let job_key = format!("{}:{}:{}", job.0, job.1, job.2);
                            s.job_to_dispatch.remove(&job_key);
                        }
                    })
                    .await?;
                heads.clone()
            }
        };
        self.store
            .mutate_sync(|s| {
                s.dispatch_heads = Some(heads);
            })
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_drawer_event(&mut self, event: Arc<DrawerEvent>) -> Res<()> {
        match &*event {
            DrawerEvent::ListChanged { drawer_heads } => {
                self.store
                    .mutate_sync(|s| {
                        s.drawer_heads = Some(drawer_heads.clone());
                    })
                    .await?;
            }
            DrawerEvent::DocUpdated {
                id,
                drawer_heads,
                branches_diffs,
            } => {
                // For updates, we only care about branches that actually changed
                for (branch_name, diff) in branches_diffs {
                    if let Some(heads) = &diff.to {
                        // Use get_if_latest to avoid work on stale headss
                        if let Some(doc) =
                            self.rt.drawer.get_if_latest(id, branch_name, heads).await?
                        {
                            self.triage(id, heads, &doc, branch_name.clone())
                                .await
                                .wrap_err("error triaging doc")?;
                        } else {
                            debug!(?id, ?branch_name, "skipping triage for stale heads");
                        }
                    }
                }

                self.store
                    .mutate_sync(|s| {
                        s.drawer_heads = Some(drawer_heads.clone());
                    })
                    .await?;
            }
            DrawerEvent::DocDeleted { drawer_heads, .. } => {
                self.store
                    .mutate_sync(|s| {
                        s.drawer_heads = Some(drawer_heads.clone());
                    })
                    .await?;
            }
            DrawerEvent::DocAdded {
                id,
                entry,
                drawer_heads,
            } => {
                for (branch_name, heads) in &entry.branches {
                    // Use get_if_latest even for added docs, although they're usually latest
                    if let Some(doc) = self.rt.drawer.get_if_latest(id, branch_name, heads).await? {
                        self.triage(id, heads, &doc, branch_name.clone())
                            .await
                            .wrap_err("error triaging doc")?;
                    }
                }
                self.store
                    .mutate_sync(|s| {
                        s.drawer_heads = Some(drawer_heads.clone());
                    })
                    .await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, doc))]
    async fn triage(
        &mut self,
        doc_id: &DocId,
        doc_heads: &ChangeHashSet,
        doc: &Doc,
        branch_name: String,
    ) -> Res<()> {
        debug!(
            processor_count = self.cached_processors.len(),
            "triaging doc"
        );

        for processor in &self.cached_processors {
            let matches = processor.predicate.matches(doc);
            if matches {
                info!(
                    plug_id = %processor.plug_id,
                    routine_name = %processor.routine_name,
                    ?doc_id,
                    "dispatching job"
                );

                let args = match &processor.routine_deets {
                    RoutineManifestDeets::DocInvoke {} => DispatchArgs::DocInvoke {
                        doc_id: doc_id.clone(),
                        branch_name: branch_name.clone(),
                        heads: doc_heads.clone(),
                    },
                    RoutineManifestDeets::DocProp { .. } => DispatchArgs::DocProp {
                        doc_id: doc_id.clone(),
                        branch_name: branch_name.clone(),
                        heads: doc_heads.clone(),
                        prop_id: None,
                    },
                };

                let job_key = format!(
                    "{}:{}:{}",
                    doc_id, processor.plug_id, processor.routine_name.0
                );

                // Check if already in-flight
                let old_dispatch = self
                    .store
                    .query_sync(|s| s.job_to_dispatch.get(&job_key).cloned())
                    .await;
                if let Some(dispatch_id) = old_dispatch {
                    info!(?dispatch_id, "cancelling inflight job");
                    self.rt.cancel_dispatch(&dispatch_id).await?;
                }

                let dispatch_id = self
                    .rt
                    .dispatch(&processor.plug_id, &processor.routine_name.0, args)
                    .await?;

                // Track mapping
                self.store
                    .mutate_sync(|s| {
                        s.job_to_dispatch.insert(job_key, dispatch_id.clone());
                        s.dispatch_to_job.insert(
                            dispatch_id,
                            (
                                doc_id.clone(),
                                processor.plug_id.clone(),
                                processor.routine_name.0.clone(),
                            ),
                        );
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::test_cx;
    use daybook_types::doc::{AddDocArgs, DocContent, WellKnownPropTag};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_triage_worker_smoke() -> Res<()> {
        let ctx = test_cx("triage_smoke").await?;

        // Add a doc that should trigger the pseudo-label processor
        let _doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_name: "main".into(),
                props: [(
                    WellKnownPropTag::Content.into(),
                    daybook_types::doc::WellKnownProp::Content(DocContent::Text(
                        "Hello world".into(),
                    ))
                    .into(),
                )]
                .into(),
            })
            .await?;

        // Wait a bit for triage to run and dispatch
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Check if a dispatch was created
        let dispatches = ctx.dispatch_repo.list().await;
        info!(?dispatches, "current dispatches");
        // There should be a dispatch for pseudo-label
        assert!(dispatches.iter().any(|(_, d)| {
            match &d.deets {
                crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } => {
                    wflow_key == "test-label"
                }
            }
        }));

        ctx.stop().await?;
        Ok(())
    }
}
