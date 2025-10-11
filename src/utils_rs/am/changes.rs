use crate::interlude::*;

use automerge::PatchAction;
use autosurgeon::Prop;
use samod::DocumentId;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

#[derive(Debug, Clone)]
pub struct ChangeNotification {
    pub path: Vec<Prop<'static>>,
    pub action: PatchAction,
    pub timestamp: std::time::Instant,
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
}

impl ChangeListenerManager {
    pub fn boot() -> Arc<Self> {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let listeners = RwLock::new(Vec::new());
        let out = Self {
            listeners,
            change_tx,
        };
        let out = Arc::new(out);

        // Start the change notification worker
        out.clone().spawn_switchboard(change_rx);

        out
    }

    /// Start listening for events on the given document
    /// TODO: the returned handle should allow unregistration
    pub fn spawn_doc_listener(self: Arc<Self>, handle: samod::DocHandle) -> JoinHandle<Res<()>> {
        let this = self.clone();
        let span = tracing::info_span!("doc listener task", doc_id = ?handle.document_id());
        tokio::spawn(
            async move {
                info!("listening on doc");

                let mut heads = handle.with_document(|doc| doc.get_heads());
                use futures::StreamExt;

                let mut doc_change_stream = handle.changes();
                while let Some(changes) = doc_change_stream.next().await {
                    let (new_heads, all_changes) = handle.with_document(|doc| {
                        let patches = doc.diff(&heads, &changes.new_heads);

                        let mut collected_changes = Vec::new();

                        for patch in patches {
                            // Convert automerge path to autosurgeon path
                            let autosurgeon_path: Vec<Prop<'static>> = patch
                                .path
                                .into_iter()
                                .map(|(_, prop)| prop.into())
                                .collect();

                            collected_changes.push((autosurgeon_path, patch.action));
                        }

                        (changes.new_heads, collected_changes)
                    });

                    info!(?all_changes, "changes observed");

                    // Notify listeners about changes
                    this.notify_listeners(handle.document_id(), all_changes);

                    heads = new_heads;
                }
                Ok(())
            }
            .instrument(span),
        )
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
                while let Some((id, notifications)) = change_rx.recv().await {
                    let listeners_guard = self.listeners.read().await;

                    // Group notifications by listener
                    let mut listener_notifications: std::collections::HashMap<
                        usize,
                        Vec<ChangeNotification>,
                    > = std::collections::HashMap::new();

                    for (listener_idx, listener) in listeners_guard.iter().enumerate() {
                        if listener
                            .filter
                            .doc_id
                            .as_ref()
                            .map(|target| *target == id)
                            .unwrap_or_default()
                        {
                            continue;
                        }
                        let mut relevant_notifications = Vec::new();

                        for notification in &notifications {
                            if path_matches(&listener.filter.path, &notification.path) {
                                relevant_notifications.push(notification.clone());
                            }
                        }

                        if !relevant_notifications.is_empty() {
                            listener_notifications.insert(listener_idx, relevant_notifications);
                        }
                    }

                    // Send batched notifications to each listener
                    for (listener_idx, notifications) in listener_notifications {
                        if let Some(listener) = listeners_guard.get(listener_idx) {
                            (listener.on_change)(notifications);
                        }
                    }
                }
                eyre::Ok(())
            }
            .instrument(tracing::info_span!("change notif switchboard task")),
        )
    }

    /// Notify all relevant listeners about changes
    fn notify_listeners(&self, id: &DocumentId, changes: Vec<(Vec<Prop<'static>>, PatchAction)>) {
        if changes.is_empty() {
            return;
        }

        let timestamp = std::time::Instant::now();
        let notifications: Vec<ChangeNotification> = changes
            .into_iter()
            .map(|(path, action)| ChangeNotification {
                path,
                action,
                timestamp,
            })
            .collect();

        // Send notifications to the worker via channel
        if let Err(e) = self.change_tx.send((id.clone(), notifications)) {
            warn!("Failed to send change notifications: {}", e);
        }
    }
}

/// Check if a change path matches a listener path (including subpaths)
fn path_matches(listener_path: &[Prop<'static>], change_path: &[Prop<'static>]) -> bool {
    if listener_path.len() > change_path.len() {
        return false;
    }

    for (i, listener_prop) in listener_path.iter().enumerate() {
        if !prop_matches(listener_prop, &change_path[i]) {
            return false;
        }
    }
    true
}

/// Check if two properties match (handles different property types)
fn prop_matches(listener_prop: &Prop<'static>, change_prop: &Prop<'static>) -> bool {
    match (listener_prop, change_prop) {
        (Prop::Key(listener_key), Prop::Key(change_key)) => listener_key == change_key,
        (Prop::Index(listener_idx), Prop::Index(change_idx)) => listener_idx == change_idx,
        _ => false,
    }
}
