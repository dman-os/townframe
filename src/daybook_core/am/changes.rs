use crate::interlude::*;
use automerge::PatchAction;
use autosurgeon::Prop;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone)]
pub struct ChangeNotification {
    pub path: Vec<Prop<'static>>,
    pub action: PatchAction,
    pub timestamp: std::time::Instant,
}

struct ChangeListener {
    path: Vec<Prop<'static>>,
    on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
}

pub struct ChangeListenerManager {
    listeners: Arc<RwLock<Vec<ChangeListener>>>,
    change_tx: mpsc::UnboundedSender<Vec<ChangeNotification>>,
}

impl ChangeListenerManager {
    pub fn new() -> Self {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let listeners = Arc::new(RwLock::new(Vec::new()));

        // Start the change notification worker
        Self::start_change_notification_worker(change_rx, listeners.clone());

        Self {
            listeners,
            change_tx,
        }
    }

    /// Start the change notification worker
    fn start_change_notification_worker(
        mut change_rx: mpsc::UnboundedReceiver<Vec<ChangeNotification>>,
        listeners: Arc<RwLock<Vec<ChangeListener>>>,
    ) {
        tokio::spawn(async move {
            while let Some(notifications) = change_rx.recv().await {
                let listeners_guard = listeners.read().await;

                // Group notifications by listener
                let mut listener_notifications: std::collections::HashMap<
                    usize,
                    Vec<ChangeNotification>,
                > = std::collections::HashMap::new();

                for (listener_idx, listener) in listeners_guard.iter().enumerate() {
                    let mut relevant_notifications = Vec::new();

                    for notification in &notifications {
                        if Self::path_matches(&listener.path, &notification.path) {
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
        });
    }

    /// Register a change listener for a specific path
    /// The listener will receive notifications for changes at the path or any subpath
    pub async fn register_change_listener<F>(&self, path: Vec<Prop<'static>>, on_change: F)
    where
        F: Fn(Vec<ChangeNotification>) + Send + Sync + 'static,
    {
        let mut listeners = self.listeners.write().await;
        listeners.push(ChangeListener {
            path,
            on_change: Box::new(on_change),
        });
    }

    /// Check if a change path matches a listener path (including subpaths)
    fn path_matches(listener_path: &[Prop<'static>], change_path: &[Prop<'static>]) -> bool {
        if listener_path.len() > change_path.len() {
            return false;
        }

        for (i, listener_prop) in listener_path.iter().enumerate() {
            if !Self::prop_matches(listener_prop, &change_path[i]) {
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

    /// Notify all relevant listeners about changes
    pub fn notify_listeners(&self, changes: Vec<(Vec<Prop<'static>>, PatchAction)>) {
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
        if let Err(e) = self.change_tx.send(notifications) {
            warn!("Failed to send change notifications: {}", e);
        }
    }
}
