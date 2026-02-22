#[cfg(feature = "automerge-repo")]
use crate::interlude::*;

#[cfg(feature = "automerge-repo")]
use automerge::ReadDoc;
#[cfg(feature = "automerge-repo")]
use autosurgeon::Prop;
#[cfg(feature = "automerge-repo")]
use samod::DocumentId;
#[cfg(feature = "automerge-repo")]
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "automerge-repo")]
#[derive(Debug, Clone)]
pub struct ChangeNotification {
    pub patch: Arc<automerge::Patch>,
    pub heads: Arc<[automerge::ChangeHash]>,
    pub actor_ids: Arc<[automerge::ActorId]>,
}

#[cfg(feature = "automerge-repo")]
impl ChangeNotification {
    pub fn is_local_only(&self, local_actor_id: &automerge::ActorId) -> bool {
        self.actor_ids.len() == 1 && self.actor_ids.first() == Some(local_actor_id)
    }
}

#[cfg(feature = "automerge-repo")]
pub struct ChangeFilter {
    pub doc_id: Option<DocIdFilter>,
    pub path: Vec<Prop<'static>>,
}

#[cfg(feature = "automerge-repo")]
struct ChangeListener {
    id: Uuid,
    filter: ChangeFilter,
    on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
}

#[cfg(feature = "automerge-repo")]
type ChangeTx = mpsc::UnboundedSender<(DocumentId, Vec<ChangeNotification>)>;

#[cfg(feature = "automerge-repo")]
pub struct ChangeListenerManager {
    listeners: RwLock<Vec<ChangeListener>>,
    change_tx: tokio::sync::Mutex<Option<ChangeTx>>,
    brokers: DHashMap<DocumentId, Arc<DocChangeBroker>>,
    cancel_token: CancellationToken,
}

#[cfg(feature = "automerge-repo")]
pub struct ChangeListenerManagerStopToken {
    pub cancel_token: CancellationToken,
    pub switchboard_handle: Option<JoinHandle<()>>,
    pub manager: Arc<ChangeListenerManager>,
}

#[cfg(feature = "automerge-repo")]
impl ChangeListenerManagerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();

        drop(self.manager.change_tx.lock().await.take());

        // Stop all brokers
        for entry in self.manager.brokers.iter() {
            entry.value().cancel_token.cancel();
        }

        if let Some(handle) = self.switchboard_handle {
            handle.await.wrap_err("switchboard task error")?;
        }
        Ok(())
    }
}

#[cfg(feature = "automerge-repo")]
pub struct DocChangeBroker {
    doc_id: DocumentId,
    cancel_token: CancellationToken,
}

#[cfg(feature = "automerge-repo")]
pub struct DocChangeBrokerStopToken {
    pub join_handle: JoinHandle<()>,
    pub cancel_token: CancellationToken,
}

#[cfg(feature = "automerge-repo")]
impl DocChangeBrokerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await.wrap_err("tokio task error")?;
        Ok(())
    }
}

#[cfg(feature = "automerge-repo")]
impl DocChangeBroker {
    pub fn filter(&self) -> DocIdFilter {
        DocIdFilter {
            doc_id: self.doc_id.clone(),
        }
    }
}

#[cfg(feature = "automerge-repo")]
#[non_exhaustive]
pub struct DocIdFilter {
    pub doc_id: DocumentId,
}

#[cfg(feature = "automerge-repo")]
impl ChangeListenerManager {
    pub fn boot() -> (Arc<Self>, ChangeListenerManagerStopToken) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let listeners = RwLock::new(Vec::new());
        let main_cancel_token = CancellationToken::new();
        let out = Self {
            listeners,
            change_tx: Some(change_tx).into(),
            brokers: default(),
            cancel_token: main_cancel_token.child_token(),
        };
        let out = Arc::new(out);

        // Start the change notification worker
        let handle = Arc::clone(&out).spawn_switchboard(change_rx);

        (
            Arc::clone(&out),
            ChangeListenerManagerStopToken {
                cancel_token: main_cancel_token,
                switchboard_handle: Some(handle),
                manager: out,
            },
        )
    }

    /// Start listening for events on the given document
    pub async fn add_doc(
        &self,
        handle: samod::DocHandle,
    ) -> Res<(Arc<DocChangeBroker>, Option<DocChangeBrokerStopToken>)> {
        if let Some(arc) = self.brokers.get(handle.document_id()) {
            return Ok((arc.clone(), None));
        }
        let Some(change_tx) = self.change_tx.lock().await.as_ref().map(|tx| tx.clone()) else {
            return Err(ferr!("repo is shuting down"));
        };

        let doc_id = handle.document_id().clone();
        let span = tracing::info_span!("doc listener task", ?doc_id);
        let main_cancel_token = self.cancel_token.child_token();
        let cancel_token_task = main_cancel_token.clone();
        let fut = async move {
            debug!("listening on doc");

            let heads = handle.with_document(|doc| doc.get_heads());
            let mut heads: Arc<[automerge::ChangeHash]> = heads.into();

            use futures::StreamExt;

            let mut doc_change_stream = handle.changes();
            loop {
                let changes = tokio::select! {
                    biased;
                    _ = cancel_token_task.cancelled() => {
                        break;
                    },
                    val = doc_change_stream.next() => {
                        val
                    }
                };
                let Some(changes) = changes else {
                    break;
                };
                let (new_heads, all_changes) = handle.with_document(|doc| {
                    let mut event_hashes =
                        std::collections::HashSet::<automerge::ChangeHash>::new();
                    let mut stack = changes.new_heads.clone();
                    while let Some(hash) = stack.pop() {
                        if !event_hashes.insert(hash) {
                            continue;
                        }
                        if let Some(change) = doc.get_change_by_hash(&hash) {
                            stack.extend(change.deps().iter().copied());
                        }
                    }

                    let mut actor_ids: Vec<automerge::ActorId> = vec![];
                    for change in doc.get_changes(&heads) {
                        if !event_hashes.contains(&change.hash()) {
                            continue;
                        }
                        if !actor_ids.iter().any(|id| id == change.actor_id()) {
                            actor_ids.push(change.actor_id().clone());
                        }
                    }
                    actor_ids.sort_by_key(|id| id.to_string());

                    let patches = doc.diff(&heads, &changes.new_heads[..]);
                    let new_heads: Arc<[automerge::ChangeHash]> = Arc::from(&changes.new_heads[..]);
                    let actor_ids: Arc<[automerge::ActorId]> = Arc::from(actor_ids);

                    let collected_changes = patches
                        .into_iter()
                        .map(|patch| {
                            let patch = Arc::new(patch);
                            ChangeNotification {
                                patch,
                                heads: Arc::clone(&new_heads),
                                actor_ids: Arc::clone(&actor_ids),
                            }
                        })
                        .collect::<Vec<_>>();

                    (new_heads, collected_changes)
                });

                trace!(?all_changes, "XXX changes observed");

                // Notify listeners about changes
                if !all_changes.is_empty() {
                    if let Err(err) = change_tx.send((handle.document_id().clone(), all_changes)) {
                        warn!("failed to send change notifications: {err}");
                    }
                }

                heads = new_heads;
            }
            eyre::Ok(())
        }
        .instrument(span);
        let join_handle = tokio::spawn(async { fut.await.unwrap() });

        let out = DocChangeBroker {
            cancel_token: main_cancel_token.child_token(),
            doc_id: doc_id.clone(),
        };
        let stop_token = DocChangeBrokerStopToken {
            join_handle,
            cancel_token: main_cancel_token,
        };
        let out = Arc::new(out);
        self.brokers.insert(doc_id, Arc::clone(&out));
        Ok((out, Some(stop_token)))
    }

    /// Register a change listener
    /// The listener will receive notifications for changes at the path or any subpath
    pub async fn add_listener(
        self: &Arc<Self>,
        filter: ChangeFilter,
        on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
    ) -> ChangeListenerRegistration {
        let id = Uuid::new_v4();
        let mut listeners = self.listeners.write().await;
        listeners.push(ChangeListener {
            id,
            filter,
            on_change,
        });
        ChangeListenerRegistration {
            manager: Arc::downgrade(self),
            id,
        }
    }

    /// Start the change notification worker
    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<(DocumentId, Vec<ChangeNotification>)>,
    ) -> JoinHandle<()> {
        let fut = async move {
            // Group notifications by listener
            let mut listener_notifications: std::collections::HashMap<
                usize,
                Vec<ChangeNotification>,
            > = std::collections::HashMap::new();
            loop {
                let Some((id, notifications)) = change_rx.recv().await else {
                    break;
                };

                listener_notifications.clear();

                let listeners = self.listeners.read().await;
                for (listener_idx, listener) in listeners.iter().enumerate() {
                    if listener
                        .filter
                        .doc_id
                        .as_ref()
                        .map(|target| target.doc_id != id)
                        .unwrap_or_default()
                    {
                        continue;
                    }
                    let mut relevant_notifications = Vec::new();

                    for notification in &notifications {
                        if path_prefix_matches(&listener.filter.path, &notification.patch.path[..])
                        {
                            relevant_notifications.push(notification.clone());
                        }
                    }

                    if !relevant_notifications.is_empty() {
                        listener_notifications.insert(listener_idx, relevant_notifications);
                    }
                }

                // Send batched notifications to each listener
                for (listener_idx, notifications) in listener_notifications.drain() {
                    if let Some(listener) = listeners.get(listener_idx) {
                        (listener.on_change)(notifications);
                    }
                }
            }
            eyre::Ok(())
        }
        .instrument(tracing::info_span!("change notif switchboard task"));
        tokio::spawn(async { fut.await.unwrap() })
    }
}

pub struct ChangeListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for ChangeListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            tokio::spawn(async move {
                let mut listeners = manager.listeners.write().await;
                listeners.retain(|listener| listener.id != id);
            });
        }
    }
}

/// Check if a change path matches a listener path (including subpaths)
#[cfg(feature = "automerge-repo")]
pub fn path_prefix_matches(
    listener_path: &[Prop<'_>],
    change_path: &[(automerge::ObjId, automerge::Prop)],
) -> bool {
    if listener_path.len() > change_path.len() {
        return false;
    }

    for (idx, listener_prop) in listener_path.iter().enumerate() {
        if !prop_matches(listener_prop, &change_path[idx].1) {
            return false;
        }
    }
    true
}

/// Check if two properties match (handles different property types)
#[cfg(feature = "automerge-repo")]
pub fn prop_matches(listener_prop: &Prop<'_>, change_prop: &automerge::Prop) -> bool {
    match (listener_prop, change_prop) {
        (Prop::Key(listener_key), automerge::Prop::Map(change_key)) => listener_key == change_key,
        (Prop::Index(listener_idx), automerge::Prop::Seq(change_idx)) => {
            *listener_idx == (*change_idx as u32)
        }
        _ => false,
    }
}
