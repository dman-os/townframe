//! Probably better named a Sequencer but that's too long
use crate::interlude::*;
use std::collections::BTreeMap;

use crate::drawer::DrawerEvent;
use crate::plugs::manifest::{
    DocPredicateEvalMode, DocPredicateEvalRequirement, DocPredicateEvalResolved,
};
use crate::plugs::PlugsEvent;
use crate::rt::dispatch::DispatchEvent;
use crate::rt::Rt;
use daybook_types::doc::BranchPath;
use daybook_types::doc::{Doc, DocId, FacetKey};

const SUBSCRIPTION_CAPACITY: usize = 256;

#[derive(Default, Debug, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct SwitchStateStore {
    pub drawer_heads: Option<ChangeHashSet>,
    pub plug_heads: Option<ChangeHashSet>,
    pub dispatch_heads: Option<ChangeHashSet>,
    pub config_heads: Option<ChangeHashSet>,

    pub dispatch_to_job: HashMap<String, (DocId, String, String)>,
    pub job_to_dispatch: HashMap<String, String>,
}

#[async_trait]
impl crate::stores::Store for SwitchStateStore {
    fn prop() -> Cow<'static, str> {
        "switch".into()
    }
}

/// Worker that listens to drawer events and schedules workflows
pub struct SwitchWorkerHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl SwitchWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        let join_handle = self.join_handle.take().expect("join_handle already taken");
        utils_rs::wait_on_handle_with_timeout(join_handle, 5 * 1000).await?;
        Ok(())
    }
}

impl Drop for SwitchWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort()
        }
    }
}

fn switch_worker_is_shutting_down(
    worker_cancel_token: &tokio_util::sync::CancellationToken,
    rt_cancel_token: &tokio_util::sync::CancellationToken,
) -> bool {
    worker_cancel_token.is_cancelled() || rt_cancel_token.is_cancelled()
}

#[derive(Debug, Clone)]
pub enum SwitchEvent {
    Drawer(Arc<DrawerEvent>),
    Plugs(Arc<PlugsEvent>),
    Dispatch(Arc<DispatchEvent>),
    Config(Arc<crate::config::ConfigEvent>),
}

#[derive(Debug, Clone)]
pub struct SwtchSinkInterest {
    pub consume_drawer: bool,
    pub consume_plugs: bool,
    pub consume_dispatch: bool,
    pub consume_config: bool,
    pub drawer_predicate: Option<crate::plugs::manifest::DocPredicateClause>,
}

#[derive(Default, Debug, Clone)]
pub struct SwitchSinkOutcome {
    pub drawer_predicate_update: Option<crate::plugs::manifest::DocPredicateClause>,
}

pub struct SwitchSinkCtx<'a> {
    // FIXME: why are these optional?
    pub rt: Option<&'a Arc<Rt>>,
    pub store: Option<&'a crate::stores::StoreHandle<SwitchStateStore>>,
}

#[async_trait]
pub trait SwitchSink {
    fn interest(&self) -> SwtchSinkInterest;
    async fn on_event(
        &mut self,
        event: &SwitchEvent,
        ctx: &SwitchSinkCtx<'_>,
    ) -> Res<SwitchSinkOutcome>;
}

struct PreparedSwitchSink {
    name: String,
    listener: Box<dyn SwitchSink + Send + Sync>,
    consume_drawer: bool,
    consume_plugs: bool,
    consume_dispatch: bool,
    consume_config: bool,
    drawer_predicate: Option<crate::plugs::manifest::DocPredicateClause>,
}

pub async fn spawn_switch_worker(
    rt: Arc<Rt>,
    app_doc_id: DocumentId,
    sinks: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>>,
) -> Res<SwitchWorkerHandle> {
    use crate::repos::{Repo, SubscribeOpts};
    use crate::stores::Store;

    let store = SwitchStateStore::load(&rt.acx, &app_doc_id).await?;
    let store = crate::stores::StoreHandle::new(
        store,
        rt.acx.clone(),
        app_doc_id.clone(),
        rt.local_actor_id.clone(),
    );

    let drawer_listener = rt
        .drawer
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let plug_listener = rt
        .plugs_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let config_listener = rt
        .config_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));
    let dispatch_listener = rt
        .dispatch_repo
        .subscribe(SubscribeOpts::new(SUBSCRIPTION_CAPACITY));

    // Catch up on missed events
    let (initial_drawer_heads, initial_dispatch_heads, initial_plug_heads, initial_config_heads) =
        store
            .query_sync(|store| {
                (
                    store.drawer_heads.clone(),
                    store.dispatch_heads.clone(),
                    store.plug_heads.clone(),
                    store.config_heads.clone(),
                )
            })
            .await;

    let empty_heads = ChangeHashSet(vec![].into());
    let drawer_heads_now = rt.drawer.get_drawer_heads();
    let dispatch_heads_now = rt.dispatch_repo.get_dispatch_heads();
    let plug_heads_now = rt.plugs_repo.get_plugs_heads();
    let config_heads_now = ChangeHashSet(rt.config_repo.get_config_heads().await?);

    let mut worker = SwitchWorker {
        store,
        rt,
        prepared_sinks: prepare_sinks(sinks),
        predicate_requirements: HashSet::new(),
        predicate_resolved: HashMap::new(),
    };

    let events = worker
        .rt
        .plugs_repo
        .diff_events(
            initial_plug_heads.unwrap_or_else(|| empty_heads.clone()),
            Some(plug_heads_now),
        )
        .await?;
    for event in events {
        let event = Arc::new(event);
        worker
            .track_event_heads(&SwitchEvent::Plugs(Arc::clone(&event)))
            .await
            .unwrap_or_log();
        worker
            .dispatch_to_listeners(&SwitchEvent::Plugs(event))
            .await
            .unwrap_or_log();
    }

    let events = worker
        .rt
        .drawer
        .diff_events(
            initial_drawer_heads.unwrap_or_else(|| empty_heads.clone()),
            Some(drawer_heads_now),
        )
        .await?;
    for event in events {
        let event = Arc::new(event);
        worker
            .track_event_heads(&SwitchEvent::Drawer(Arc::clone(&event)))
            .await
            .unwrap_or_log();
        worker
            .dispatch_to_listeners(&SwitchEvent::Drawer(event))
            .await
            .unwrap_or_log();
    }

    let events = worker
        .rt
        .dispatch_repo
        .diff_events(
            initial_dispatch_heads.unwrap_or_else(|| empty_heads.clone()),
            Some(dispatch_heads_now),
        )
        .await?;
    for event in events {
        let event = Arc::new(event);
        worker
            .track_event_heads(&SwitchEvent::Dispatch(Arc::clone(&event)))
            .await
            .unwrap_or_log();
        worker
            .dispatch_to_listeners(&SwitchEvent::Dispatch(event))
            .await
            .unwrap_or_log();
    }

    let events = worker
        .rt
        .config_repo
        .diff_events(
            initial_config_heads.unwrap_or(empty_heads),
            Some(config_heads_now),
        )
        .await?;
    for event in events {
        let event = Arc::new(event);
        worker
            .track_event_heads(&SwitchEvent::Config(Arc::clone(&event)))
            .await
            .unwrap_or_log();
        worker
            .dispatch_to_listeners(&SwitchEvent::Config(event))
            .await
            .unwrap_or_log();
    }

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let rt_cancel_token = worker.rt.cancel_token.clone();
    let fut = {
        let cancel_token = cancel_token.clone();
        let rt_cancel_token = rt_cancel_token.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        debug!("SwitchWorker cancelled");
                        break;
                    }
                    event = plug_listener.recv_lossy_async() => {
                        let event = match event {
                            Ok(event) => event,
                            Err(error) => {
                                trace!(?error, "SwitchWorker plug_listener recv closed");
                                break;
                            }
                        };
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Plugs(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Plugs(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    event = config_listener.recv_lossy_async() => {
                        let event = match event {
                            Ok(event) => event,
                            Err(error) => {
                                trace!(?error, "SwitchWorker config_listener recv closed");
                                break;
                            }
                        };
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Config(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Config(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    event = drawer_listener.recv_lossy_async() => {
                        let event = match event {
                            Ok(event) => event,
                            Err(error) => {
                                trace!(?error, "SwitchWorker drawer_listener recv closed");
                                break;
                            }
                        };
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Drawer(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Drawer(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                    event = dispatch_listener.recv_lossy_async() => {
                        let event = match event {
                            Ok(event) => event,
                            Err(error) => {
                                trace!(?error, "SwitchWorker dispatch_listener recv closed");
                                break;
                            }
                        };
                        if let Err(error) = worker.track_event_heads(&SwitchEvent::Dispatch(Arc::clone(&event))).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                        if let Err(error) = worker.dispatch_to_listeners(&SwitchEvent::Dispatch(event)).await {
                            if switch_worker_is_shutting_down(&cancel_token, &rt_cancel_token) {
                                debug!(?error, "SwitchWorker exiting during shutdown");
                                break;
                            }
                            return Err(error);
                        }
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_cancel_token = cancel_token.clone();
    let join_rt_cancel_token = rt_cancel_token.clone();
    let join_handle = tokio::spawn(async move {
        if let Err(err) = fut.await {
            if switch_worker_is_shutting_down(&join_cancel_token, &join_rt_cancel_token) {
                debug!(?err, "SwitchWorker exiting during shutdown");
            } else {
                error!(?err, "SwitchWorker failed");
            }
        }
    });

    Ok(SwitchWorkerHandle {
        join_handle: Some(join_handle),
        cancel_token,
    })
}

fn prepare_sinks(
    listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>>,
) -> Vec<PreparedSwitchSink> {
    listeners
        .into_iter()
        .map(|(name, listener)| {
            let interest = listener.interest();
            PreparedSwitchSink {
                name,
                listener,
                consume_drawer: interest.consume_drawer,
                consume_plugs: interest.consume_plugs,
                consume_dispatch: interest.consume_dispatch,
                consume_config: interest.consume_config,
                drawer_predicate: interest.drawer_predicate,
            }
        })
        .collect()
}

struct SwitchWorker {
    rt: Arc<Rt>,
    store: crate::stores::StoreHandle<SwitchStateStore>,
    prepared_sinks: Vec<PreparedSwitchSink>,
    predicate_requirements: HashSet<DocPredicateEvalRequirement>,
    predicate_resolved: HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
}

impl SwitchWorker {
    #[tracing::instrument(skip(self))]
    async fn dispatch_to_listeners(&mut self, event: &SwitchEvent) -> Res<()> {
        for index in 0..self.prepared_sinks.len() {
            if !self.listener_interested_in_event(index, event).await? {
                continue;
            }
            let ctx = SwitchSinkCtx {
                rt: Some(&self.rt),
                store: Some(&self.store),
            };

            let listener_name = self.prepared_sinks[index].name.clone();
            let outcome = self.prepared_sinks[index]
                .listener
                .on_event(event, &ctx)
                .await?;
            if let Some(next_predicate) = outcome.drawer_predicate_update {
                self.prepared_sinks[index].drawer_predicate = Some(next_predicate);
            }
            debug!(listener = %listener_name, "switch listener handled event");
        }
        Ok(())
    }

    async fn listener_interested_in_event(
        &mut self,
        index: usize,
        event: &SwitchEvent,
    ) -> Res<bool> {
        match event {
            SwitchEvent::Drawer(event) => {
                if !self.prepared_sinks[index].consume_drawer {
                    return Ok(false);
                }
                let predicate = self.prepared_sinks[index].drawer_predicate.clone();
                self.drawer_event_matches_listener(event, predicate.as_ref())
                    .await
            }
            SwitchEvent::Plugs(_) => Ok(self.prepared_sinks[index].consume_plugs),
            SwitchEvent::Dispatch(_) => Ok(self.prepared_sinks[index].consume_dispatch),
            SwitchEvent::Config(_) => Ok(self.prepared_sinks[index].consume_config),
        }
    }

    async fn drawer_event_matches_listener(
        &mut self,
        event: &Arc<DrawerEvent>,
        predicate: Option<&crate::plugs::manifest::DocPredicateClause>,
    ) -> Res<bool> {
        fn resolve_meta_predicate_requirements(
            requirements: &HashSet<DocPredicateEvalRequirement>,
            meta_doc: &Doc,
            out: &mut HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
        ) {
            out.clear();
            for requirement in requirements {
                match requirement {
                    DocPredicateEvalRequirement::FacetsOfTag(tag) => {
                        let source_facets = meta_doc
                            .facets
                            .iter()
                            .filter(|(facet_key, _)| facet_key.tag.to_string() == tag.0)
                            .map(|(facet_key, facet_raw)| (facet_key.clone(), facet_raw.clone()))
                            .collect::<Vec<_>>();
                        out.insert(
                            requirement.clone(),
                            DocPredicateEvalResolved::FacetsOfTag(source_facets),
                        );
                    }
                    DocPredicateEvalRequirement::FullDoc
                    | DocPredicateEvalRequirement::FacetManifest => {
                        // Switch prefilter stays cheap by default. Missing resolved requirements
                        // are handled conservatively by predicate evaluation in ApproxInterest mode.
                    }
                }
            }
        }

        let Some(predicate) = predicate else {
            return Ok(true);
        };
        match &**event {
            DrawerEvent::ListChanged { .. } => Ok(true),
            DrawerEvent::DocDeleted { .. } => Ok(true),
            DrawerEvent::DocAdded {
                id,
                entry,
                drawer_heads,
            } => {
                let Some(heads) = entry.branches.get("main") else {
                    return Ok(false);
                };
                let branch_path = BranchPath::from("main");
                let Some(facet_keys_set) = self
                    .rt
                    .drawer
                    .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                    .await?
                else {
                    return Ok(false);
                };
                let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                self.predicate_requirements.clear();
                predicate.append_requirements(&mut self.predicate_requirements);
                resolve_meta_predicate_requirements(
                    &self.predicate_requirements,
                    &meta_doc,
                    &mut self.predicate_resolved,
                );
                Ok(predicate.evaluate(
                    &meta_doc,
                    DocPredicateEvalMode::ApproxInterest,
                    &self.predicate_resolved,
                ))
            }
            DrawerEvent::DocUpdated {
                id,
                entry,
                diff,
                drawer_heads,
                ..
            } => {
                if !diff
                    .moved_branch_names
                    .iter()
                    .any(|branch_name| branch_name == "main")
                {
                    return Ok(false);
                }
                let referenced_tags = predicate.referenced_tags();
                if !diff.changed_facet_keys.iter().any(|facet_key| {
                    referenced_tags
                        .iter()
                        .any(|tag| tag.0 == facet_key.tag.to_string())
                }) {
                    return Ok(false);
                }
                let Some(heads) = entry.branches.get("main") else {
                    return Ok(false);
                };
                let branch_path = BranchPath::from("main");
                let Some(facet_keys_set) = self
                    .rt
                    .drawer
                    .get_facet_keys_if_latest(id, &branch_path, heads, drawer_heads)
                    .await?
                else {
                    return Ok(false);
                };
                let meta_doc = facet_keys_set_to_meta_doc(id, &facet_keys_set);
                self.predicate_requirements.clear();
                predicate.append_requirements(&mut self.predicate_requirements);
                resolve_meta_predicate_requirements(
                    &self.predicate_requirements,
                    &meta_doc,
                    &mut self.predicate_resolved,
                );
                Ok(predicate.evaluate(
                    &meta_doc,
                    DocPredicateEvalMode::ApproxInterest,
                    &self.predicate_resolved,
                ))
            }
        }
    }

    async fn track_event_heads(&self, event: &SwitchEvent) -> Res<()> {
        match event {
            SwitchEvent::Drawer(event) => {
                let drawer_heads = match &**event {
                    DrawerEvent::ListChanged { drawer_heads } => drawer_heads.clone(),
                    DrawerEvent::DocAdded { drawer_heads, .. } => drawer_heads.clone(),
                    DrawerEvent::DocUpdated { drawer_heads, .. } => drawer_heads.clone(),
                    DrawerEvent::DocDeleted { drawer_heads, .. } => drawer_heads.clone(),
                };
                self.store
                    .mutate_sync(|store| {
                        store.drawer_heads = Some(drawer_heads);
                    })
                    .await?;
            }
            SwitchEvent::Plugs(event) => {
                let plug_heads = match &**event {
                    PlugsEvent::PlugAdded { heads, .. } => heads.clone(),
                    PlugsEvent::PlugChanged { heads, .. } => heads.clone(),
                    PlugsEvent::PlugDeleted { heads, .. } => heads.clone(),
                };
                self.store
                    .mutate_sync(|store| {
                        store.plug_heads = Some(plug_heads);
                    })
                    .await?;
            }
            SwitchEvent::Dispatch(event) => {
                let dispatch_heads = match &**event {
                    DispatchEvent::DispatchAdded { heads, .. } => heads.clone(),
                    DispatchEvent::DispatchDeleted { heads, .. } => heads.clone(),
                };
                self.store
                    .mutate_sync(|store| {
                        store.dispatch_heads = Some(dispatch_heads);
                    })
                    .await?;
            }
            SwitchEvent::Config(event) => {
                let config_heads = match &**event {
                    crate::config::ConfigEvent::Changed { heads } => heads.clone(),
                };
                self.store
                    .mutate_sync(|store| {
                        store.config_heads = Some(config_heads);
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

pub fn facet_keys_set_to_meta_doc(doc_id: &DocId, facet_keys_set: &HashSet<FacetKey>) -> Doc {
    let facets: HashMap<FacetKey, daybook_types::doc::FacetRaw> = facet_keys_set
        .iter()
        .map(|key| (key.clone(), serde_json::Value::Null))
        .collect();
    Doc {
        id: doc_id.clone(),
        facets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::test_cx;
    use crate::rt::dispatch::ActiveDispatch;
    use daybook_types::doc::{AddDocArgs, DocPatch, WellKnownFacetTag};
    use std::sync::{Arc as StdArc, Mutex};

    struct TestListener {
        name: String,
        calls: StdArc<Mutex<Vec<String>>>,
        interest: SwtchSinkInterest,
        outcome: Option<SwitchSinkOutcome>,
    }

    #[async_trait]
    impl SwitchSink for TestListener {
        fn interest(&self) -> SwtchSinkInterest {
            self.interest.clone()
        }

        async fn on_event(
            &mut self,
            _event: &SwitchEvent,
            _ctx: &SwitchSinkCtx<'_>,
        ) -> Res<SwitchSinkOutcome> {
            self.calls
                .lock()
                .expect("switch test call lock poisoned")
                .push(self.name.clone());
            Ok(self.outcome.clone().unwrap_or_default())
        }
    }

    async fn dispatch_test_event(
        listeners: &mut [PreparedSwitchSink],
        event: &SwitchEvent,
    ) -> Res<()> {
        for listener in listeners.iter_mut() {
            let is_interested = match event {
                SwitchEvent::Drawer(_) => listener.consume_drawer,
                SwitchEvent::Plugs(_) => listener.consume_plugs,
                SwitchEvent::Dispatch(_) => listener.consume_dispatch,
                SwitchEvent::Config(_) => listener.consume_config,
            };
            if !is_interested {
                continue;
            }
            let outcome = listener
                .listener
                .on_event(
                    event,
                    &SwitchSinkCtx {
                        rt: None,
                        store: None,
                    },
                )
                .await?;
            if let Some(next_predicate) = outcome.drawer_predicate_update {
                listener.drawer_predicate = Some(next_predicate);
            }
        }
        Ok(())
    }

    fn count_dispatches_with_wflow_key(
        dispatches: &[(String, std::sync::Arc<ActiveDispatch>)],
        key: &str,
    ) -> usize {
        dispatches
            .iter()
            .filter(|(_, d)| {
                matches!(
                    &d.deets,
                    crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == key
                )
            })
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_worker_smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_smoke").await?;

        // Add a doc that should trigger the test-label processor
        let _doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hello world".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // Wait for the dispatch to be created
        let mut dispatch_id: Option<String> = None;
        for _ in 0..300 {
            let dispatches = ctx.dispatch_repo.list().await;
            if let Some((id, _d)) = dispatches.iter().find(|(_, d)| {
                matches!(
                    &d.deets,
                    crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "test-label"
                )
            }) {
                dispatch_id = Some(id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch not found")?;

        // Wait for the dispatch to complete
        ctx.rt
            .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
            .await?;

        ctx.stop().await?;
        Ok(())
    }

    /// Global early-out: when only facets outside any processor's read set change, switch does not load the doc or schedule any processor.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_skip_when_no_processor_read_set_changed() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_skip_unrelated").await?;

        let doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hello world".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // Wait for test-label dispatch from the add and wait for completion
        let mut initial_dispatch_id: Option<String> = None;
        for _ in 0..300 {
            let dispatches = ctx.dispatch_repo.list().await;
            if let Some((dispatch_id, _dispatch)) = dispatches.iter().find(|(_, dispatch)| {
                matches!(
                    &dispatch.deets,
                    crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "test-label"
                )
            }) {
                initial_dispatch_id = Some(dispatch_id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let initial_dispatch_id =
            initial_dispatch_id.ok_or_eyre("initial test-label dispatch not found")?;
        ctx.rt
            .wait_for_dispatch_end(&initial_dispatch_id, std::time::Duration::from_secs(90))
            .await?;

        let dispatches_before = ctx.dispatch_repo.list().await;
        let test_label_count_before =
            count_dispatches_with_wflow_key(&dispatches_before, "test-label");

        // Update only Title (no processor in default plugs has Title in read set for switch)
        ctx.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        WellKnownFacetTag::TitleGeneric.into(),
                        daybook_types::doc::WellKnownFacet::TitleGeneric("A title".into()).into(),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?;

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        let dispatches_after = ctx.dispatch_repo.list().await;
        let test_label_count_after =
            count_dispatches_with_wflow_key(&dispatches_after, "test-label");

        assert_eq!(
            test_label_count_before, test_label_count_after,
            "switch should skip when only unrelated facet (Title) changed; test-label count should not increase"
        );

        ctx.stop().await?;
        Ok(())
    }

    /// DocAdded still triggers switch using facet-key view (no full doc load).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_switch_doc_added_facet_key_matching() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let ctx = test_cx("switch_doc_added").await?;

        let _doc_id = ctx
            .drawer_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: [(
                    WellKnownFacetTag::Note.into(),
                    daybook_types::doc::WellKnownFacet::Note("Hi".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let mut dispatch_id: Option<String> = None;
        for _ in 0..300 {
            let dispatches = ctx.dispatch_repo.list().await;
            if let Some((id, _)) = dispatches.iter().find(|(_, d)| {
                matches!(
                    &d.deets,
                    crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "test-label"
                )
            }) {
                dispatch_id = Some(id.clone());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        assert!(
            dispatch_id.is_some(),
            "DocAdded with Note should trigger test-label via facet-key matching"
        );

        ctx.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_order_is_deterministic() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [
            (
                "zeta".to_string(),
                Box::new(TestListener {
                    name: "zeta".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
            (
                "alpha".to_string(),
                Box::new(TestListener {
                    name: "alpha".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
        ]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Dispatch(Arc::new(DispatchEvent::DispatchDeleted {
                id: "hello".into(),
                heads: ChangeHashSet(Vec::new().into()),
            })),
        )
        .await?;
        let called = calls
            .lock()
            .expect("switch test call lock poisoned")
            .clone();
        assert_eq!(called, vec!["alpha".to_string(), "zeta".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_routes_by_interest() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [
            (
                "dispatch_only".to_string(),
                Box::new(TestListener {
                    name: "dispatch_only".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: true,
                        consume_config: false,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
            (
                "config_only".to_string(),
                Box::new(TestListener {
                    name: "config_only".to_string(),
                    calls: StdArc::clone(&calls),
                    interest: SwtchSinkInterest {
                        consume_drawer: false,
                        consume_plugs: false,
                        consume_dispatch: false,
                        consume_config: true,
                        drawer_predicate: None,
                    },
                    outcome: None,
                }) as Box<dyn SwitchSink + Send + Sync>,
            ),
        ]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Dispatch(Arc::new(DispatchEvent::DispatchDeleted {
                id: "hello".into(),
                heads: ChangeHashSet(Vec::new().into()),
            })),
        )
        .await?;
        let called = calls
            .lock()
            .expect("switch test call lock poisoned")
            .clone();
        assert_eq!(called, vec!["dispatch_only".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_switch_listener_predicate_update_applies() -> Res<()> {
        let calls = StdArc::new(Mutex::new(Vec::new()));
        let listeners: BTreeMap<String, Box<dyn SwitchSink + Send + Sync>> = [(
            "predicated".to_string(),
            Box::new(TestListener {
                name: "predicated".to_string(),
                calls: StdArc::clone(&calls),
                interest: SwtchSinkInterest {
                    consume_drawer: true,
                    consume_plugs: true,
                    consume_dispatch: false,
                    consume_config: false,
                    drawer_predicate: None,
                },
                outcome: Some(SwitchSinkOutcome {
                    drawer_predicate_update: Some(
                        crate::plugs::manifest::DocPredicateClause::HasTag(
                            crate::plugs::manifest::FacetTag("example.tag".into()),
                        ),
                    ),
                }),
            }) as Box<dyn SwitchSink + Send + Sync>,
        )]
        .into();
        let mut runtime_listeners = prepare_sinks(listeners);
        dispatch_test_event(
            &mut runtime_listeners,
            &SwitchEvent::Plugs(Arc::new(PlugsEvent::PlugAdded {
                id: "id".into(),
                heads: ChangeHashSet(Vec::new().into()),
            })),
        )
        .await?;
        let predicate = runtime_listeners[0].drawer_predicate.clone();
        assert!(matches!(
            predicate,
            Some(crate::plugs::manifest::DocPredicateClause::HasTag(_))
        ));
        Ok(())
    }
}
