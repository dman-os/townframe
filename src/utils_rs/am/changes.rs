use crate::interlude::*;

use automerge::{transaction::Transactable, ReadDoc};
use autosurgeon::Prop;
use samod::DocumentId;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

#[derive(Debug, Clone)]
pub struct ChangeNotification {
    pub patch: Arc<automerge::Patch>,
    pub heads: Arc<[automerge::ChangeHash]>,
    // TODO: timestamp
}

pub struct ChangeFilter {
    pub doc_id: Option<DocumentId>,
    pub path: Vec<Prop<'static>>,
}

struct ChangeListener {
    filter: ChangeFilter,
    on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
}

pub struct ChangeListenerManager {
    listeners: RwLock<Vec<ChangeListener>>,
    change_tx: mpsc::UnboundedSender<(DocumentId, Vec<ChangeNotification>)>,
    brokers: DHashMap<DocumentId, Arc<DocChangeBroker>>,
}

struct DocChangeBroker {
    join_handle: JoinHandle<Res<()>>,
    term_signal_tx: tokio::sync::watch::Sender<bool>,
}

impl DocChangeBroker {
    pub async fn stop(self) -> Res<()> {
        self.term_signal_tx.send(true).wrap_err("already stopped")?;
        self.join_handle.await.wrap_err("tokio task error")?
    }
}

impl ChangeListenerManager {
    pub fn boot() -> Arc<Self> {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let listeners = RwLock::new(Vec::new());
        let out = Self {
            listeners,
            change_tx,
            brokers: default(),
        };
        let out = Arc::new(out);

        // Start the change notification worker
        out.clone().spawn_switchboard(change_rx);

        out
    }

    /// Start listening for events on the given document
    /// TODO: the returned handle should allow unregistration
    pub fn add_doc(self: &Arc<Self>, handle: samod::DocHandle) -> Arc<DocChangeBroker> {
        if let Some(arc) = self.brokers.get(handle.document_id()) {
            return arc.clone();
        }
        let this = self.clone();

        let doc_id = handle.document_id().clone();
        let span = tracing::info_span!("doc listener task", ?doc_id);
        let (term_signal_tx, mut term_signal_rx) = tokio::sync::watch::channel(false);
        let join_handle = tokio::spawn(
            async move {
                info!("listening on doc");

                let heads = handle.with_document(|doc| doc.get_heads());
                let mut heads: Arc<[automerge::ChangeHash]> = heads.into();

                use futures::StreamExt;

                let mut doc_change_stream = handle.changes();
                loop {
                    let changes = tokio::select! {
                        biased;
                        _ = term_signal_rx.wait_for(|signal| *signal) => {
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
                        let patches = doc.diff(&heads, &changes.new_heads[..]);
                        // let meta = doc.get_changes_meta(&changes.new_heads[..]);
                        let new_heads: Arc<[automerge::ChangeHash]> = changes.new_heads.into();

                        let collected_changes = patches
                            .into_iter()
                            .map(|patch| {
                                let patch = Arc::new(patch);
                                ChangeNotification {
                                    patch,
                                    heads: new_heads.clone(),
                                }
                            })
                            .collect::<Vec<_>>();

                        (new_heads, collected_changes)
                    });

                    info!(?all_changes, "changes observed");

                    // Notify listeners about changes
                    if !all_changes.is_empty() {
                        if let Err(err) = this
                            .change_tx
                            .send((handle.document_id().clone(), all_changes))
                        {
                            warn!("failed to send change notifications: {err}");
                        }
                    }

                    heads = new_heads;
                }
                Ok(())
            }
            .instrument(span),
        );

        let out = DocChangeBroker {
            join_handle,
            term_signal_tx,
        };
        let out = Arc::new(out);
        self.brokers.insert(doc_id, out.clone());
        out
    }

    /// Register a change listener
    /// The listener will receive notifications for changes at the path or any subpath
    pub async fn add_listener<F>(&self, filter: ChangeFilter, on_change: F)
    where
        F: Fn(Vec<ChangeNotification>) + Send + Sync + 'static,
    {
        let mut listeners = self.listeners.write().await;
        listeners.push(ChangeListener {
            filter,
            on_change: Box::new(on_change),
        });
    }

    /// Start the change notification worker
    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<(DocumentId, Vec<ChangeNotification>)>,
    ) -> JoinHandle<Res<()>> {
        tokio::spawn(
            async move {
                // Group notifications by listener
                let mut listener_notifications: std::collections::HashMap<
                    usize,
                    Vec<ChangeNotification>,
                > = std::collections::HashMap::new();
                while let Some((id, notifications)) = change_rx.recv().await {
                    listener_notifications.clear();

                    let listeners = self.listeners.read().await;
                    for (listener_idx, listener) in listeners.iter().enumerate() {
                        if listener
                            .filter
                            .doc_id
                            .as_ref()
                            .map(|target| *target != id)
                            .unwrap_or_default()
                        {
                            continue;
                        }
                        let mut relevant_notifications = Vec::new();

                        for notification in &notifications {
                            if path_matches(&listener.filter.path, &notification.patch.path[..]) {
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
            .instrument(tracing::info_span!("change notif switchboard task")),
        )
    }
}

/// Check if a change path matches a listener path (including subpaths)
pub fn path_matches(
    listener_path: &[Prop<'static>],
    change_path: &[(automerge::ObjId, automerge::Prop)],
) -> bool {
    if listener_path.len() > change_path.len() {
        return false;
    }

    for (i, listener_prop) in listener_path.iter().enumerate() {
        if !prop_matches(listener_prop, &change_path[i].1) {
            return false;
        }
    }
    true
}

/// Check if two properties match (handles different property types)
pub fn prop_matches(listener_prop: &Prop<'static>, change_prop: &automerge::Prop) -> bool {
    match (listener_prop, change_prop) {
        (Prop::Key(listener_key), automerge::Prop::Map(change_key)) => listener_key == change_key,
        (Prop::Index(listener_idx), automerge::Prop::Seq(change_idx)) => {
            *listener_idx == (*change_idx as u32)
        }
        _ => false,
    }
}
