use crate::interlude::*;

use crate::drawer::{DrawerEvent, DrawerRepo};
use daybook_types::doc::{Doc, DocId, DocPropKey};

pub use wflow::{PartitionLogIngress, WflowIngress};

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
    repo: Arc<DrawerRepo>,
    ingress: Arc<dyn WflowIngress>,
    config_repo: Arc<crate::config::ConfigRepo>,
) -> Res<DocTriageWorkerHandle> {
    use crate::config::ConfigEvent;
    use crate::repos::Repo;

    // Get initial config and heads
    let initial_config = config_repo.get_triage_config_sync().await;
    let initial_heads = config_repo.get_config_heads().await?;

    // Use shared state for config that can be updated
    let config_state = Arc::new(tokio::sync::RwLock::new((initial_config, initial_heads)));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<DocChangeEvent>();
    let (config_event_tx, mut config_event_rx) =
        tokio::sync::mpsc::unbounded_channel::<ConfigEvent>();

    let listener = repo.register_listener({
        let event_tx = event_tx.clone();
        move |event| event_tx.send(event.into()).expect(ERROR_CHANNEL)
    });

    let config_listener = config_repo.register_listener({
        let config_event_tx = config_event_tx.clone();
        move |event: Arc<ConfigEvent>| {
            config_event_tx.send((*event).clone()).expect(ERROR_CHANNEL);
        }
    });

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let fut = {
        let ingress = ingress.clone();
        let cancel_token = cancel_token.clone();
        let config_repo = config_repo.clone();
        let config_state = config_state.clone();
        async move {
            // NOTE: we don't want to drop the listeners before we're done
            let _listener = listener;
            let _config_listener = config_listener;

            let retry = |event: DocChangeEvent| {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    async move {
                        let new_backoff =
                            utils_rs::backoff(event.last_attempt_backoff_ms, 60 * 1000).await;
                        event_tx
                            .send(DocChangeEvent {
                                last_attempt_backoff_ms: new_backoff,
                                ..event
                            })
                            .expect(ERROR_CHANNEL);
                    }
                });
            };

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("DocTriageWorker cancelled");
                        break;
                    }
                    config_event = config_event_rx.recv() => {
                        if let Some(ConfigEvent::Changed) = config_event {
                            // Reload config and heads on change
                            let new_config = config_repo.get_triage_config_sync().await;
                            match config_repo.get_config_heads().await {
                                Ok(new_heads) => {
                                    *config_state.write().await = (new_config, new_heads);
                                    info!("Config updated in triage worker");
                                }
                                Err(err) => {
                                    error!(?err, "error getting config heads");
                                }
                            }
                        }
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        match &*event.inner {
                            DrawerEvent::ListChanged => {
                                // noop
                            }
                            DrawerEvent::DocUpdated { .. } => {
                                // TODO: handle doc updates
                            }
                            DrawerEvent::DocDeleted { .. } => {
                                // TODO: handle doc deletions
                            }
                            DrawerEvent::DocAdded { id, entry } => {
                                let mut retry_event = false;
                                for (branch_name, heads) in &entry.branches {
                                    // Get the doc
                                    let doc = match repo.get_at_heads(id, heads).await {
                                        Ok(Some(doc)) => doc,
                                        Ok(None) => {
                                            warn!(?branch_name, doc_id = ?id, "doc not found at heads");
                                            continue;
                                        }
                                        Err(err) => {
                                            error!(?err, doc_id = ?id, "error getting doc");
                                            retry_event = true;
                                            break;
                                        }
                                    };

                                    // Convert heads to ChangeHash array
                                    let doc_heads = Arc::clone(&heads.0);

                                    // Get current config and heads
                                    let (triage_config, config_heads) = {
                                        let state = config_state.read().await;
                                        (state.0.clone(), state.1.clone())
                                    };

                                    // Call triage with current config
                                    if let Err(err) = triage(
                                        &ingress,
                                        &triage_config,
                                        &config_heads,
                                        id,
                                        &doc_heads,
                                        &doc,
                                    )
                                    .await
                                    {
                                        error!(?err, doc_id = ?id, "error in triage");
                                        retry_event = true;
                                        break;
                                    }
                                }
                                if retry_event {
                                    retry(event);
                                    continue;
                                }
                            }
                        }
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

struct DocChangeEvent {
    inner: Arc<DrawerEvent>,
    last_attempt_backoff_ms: u64,
}

impl From<Arc<DrawerEvent>> for DocChangeEvent {
    fn from(inner: Arc<DrawerEvent>) -> Self {
        Self {
            inner,
            last_attempt_backoff_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
pub enum CancellationPolicy {
    NoSupport,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
pub struct Processor {
    pub cancellation_policy: CancellationPolicy,
    pub predicate: ThroughJson<PredicateClause>,
    pub wflow_key: String,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq, Default)]
pub struct TriageConfig {
    pub processors: HashMap<String, Processor>,
}

#[tracing::instrument(skip(ingress, config))]
pub async fn triage(
    ingress: &Arc<dyn WflowIngress>,
    config: &TriageConfig,
    config_heads: &Arc<[automerge::ChangeHash]>,
    doc_id: &DocId,
    doc_heads: &Arc<[automerge::ChangeHash]>,
    doc: &Doc,
) -> Res<()> {
    for (processor_id, processor) in &config.processors {
        // Deserialize predicate from JSON
        if processor.predicate.matches(doc) {
            let job_id = {
                use data_encoding::BASE32;
                use std::hash::{Hash, Hasher};
                // FIXME: we probably want to use a stable hasher impl
                let mut hasher = std::hash::DefaultHasher::default();
                doc_heads.hash(&mut hasher);
                processor_id.hash(&mut hasher);
                config_heads.hash(&mut hasher);
                let hash = hasher.finish();
                BASE32.encode(&hash.to_le_bytes())
            };
            // Serialize DocAddedEvent as args
            let heads_str = utils_rs::am::serialize_commit_heads(doc_heads.as_ref());
            let args = daybook_types::doc::DocAddedEvent {
                id: doc_id.clone(),
                heads: heads_str,
            };
            ingress
                .add_job(
                    job_id.into(),
                    &processor.wflow_key,
                    serde_json::to_string(&args).expect(ERROR_JSON),
                    None,
                )
                .await
                .wrap_err_with(|| format!("error scheduling job for {processor_id}"))?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateClause {
    HasKey(DocPropKey),
    Or(Vec<PredicateClause>),
    And(Vec<PredicateClause>),
    Not(Box<PredicateClause>),
}

impl PredicateClause {
    pub fn matches(&self, doc: &Doc) -> bool {
        match self {
            PredicateClause::HasKey(check_key) => doc.props.keys().any(|key| check_key == key),
            PredicateClause::Not(inner) => !inner.matches(doc),
            PredicateClause::Or(clauses) => clauses.iter().any(|clause| clause.matches(doc)),
            PredicateClause::And(clauses) => clauses.iter().all(|clause| clause.matches(doc)),
        }
    }
}
