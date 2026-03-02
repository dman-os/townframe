//! FIXME: convert change listenrs to a channel based system to avoid
//! blocking the loop similar to core::repo

use crate::interlude::*;

mod broker;

pub use broker::{DocChangeBrokerHandle, DocChangeBrokerStopToken, HeadListenerRegistration};

use automerge::ChangeHash;
use autosurgeon::Prop;
use samod::{DocHandle, DocumentId};
use std::sync::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ChangeNotification {
    pub patch: Arc<automerge::Patch>,
    pub heads: Arc<[ChangeHash]>,
}

pub struct ChangeFilter {
    pub doc_id: Option<DocIdFilter>,
    pub path: Vec<Prop<'static>>,
}

struct ChangeListener {
    id: Uuid,
    filter: ChangeFilter,
    on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
}

pub struct ChangeListenerManager {
    listeners: Mutex<Vec<ChangeListener>>,
    change_tx: mpsc::UnboundedSender<(DocumentId, Vec<ChangeNotification>)>,
    brokers: DHashMap<
        DocumentId,
        (
            Arc<broker::DocChangeBrokerHandle>,
            Arc<broker::DocChangeBrokerStopToken>,
        ),
    >,
    cancel_token: CancellationToken,
}

pub struct ChangeListenerManagerStopToken {
    pub cancel_token: CancellationToken,
    pub switchboard_handle: Option<JoinHandle<()>>,
    pub manager: Arc<ChangeListenerManager>,
}

impl ChangeListenerManagerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();

        if let Some(handle) = self.switchboard_handle {
            handle.await.wrap_err("switchboard task error")?;
        }
        Ok(())
    }
}

#[non_exhaustive]
pub struct DocIdFilter {
    pub doc_id: DocumentId,
}

#[cfg(feature = "repo")]
impl ChangeListenerManager {
    pub fn boot() -> (Arc<Self>, ChangeListenerManagerStopToken) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();
        let out = Self {
            listeners: default(),
            change_tx,
            brokers: default(),
            cancel_token: cancel_token.clone(),
        };
        let out = Arc::new(out);

        // Start the change notification worker
        let handle = Arc::clone(&out).spawn_switchboard(change_rx);

        (
            Arc::clone(&out),
            ChangeListenerManagerStopToken {
                cancel_token,
                switchboard_handle: Some(handle),
                manager: out,
            },
        )
    }

    fn ensure_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("ChangeListenerManager is stopped");
        }
        Ok(())
    }

    /// NOTE: this only supports linear doc histories and breaks
    /// on branches.
    ///
    /// Start listening for events on the given document
    /// Use the stop token if the Arc is the last one
    pub async fn add_doc(
        &self,
        handle: DocHandle,
    ) -> Res<(
        Arc<broker::DocChangeBrokerHandle>,
        Arc<broker::DocChangeBrokerStopToken>,
    )> {
        self.ensure_live()?;

        let doc_id = handle.document_id().clone();
        if let Some(arc) = self.brokers.get(&doc_id) {
            return Ok(arc.clone());
        }

        let (broker, stop_token) = broker::spawn_doc_listener(
            handle,
            // NOTE: if the changes listener is cancelled,
            // so will all brokers
            self.cancel_token.child_token(),
            self.change_tx.clone(),
        )?;

        let broker = Arc::new(broker);
        let stop_token = Arc::new(stop_token);
        self.brokers
            .insert(doc_id, (Arc::clone(&broker), Arc::clone(&stop_token)));
        Ok((broker, stop_token))
    }

    /// Register a change listener
    /// The listener will receive notifications for changes at the path or any subpath
    pub async fn add_listener(
        self: &Arc<Self>,
        filter: ChangeFilter,
        on_change: Box<dyn Fn(Vec<ChangeNotification>) + Send + Sync + 'static>,
    ) -> Res<ChangeListenerRegistration> {
        self.ensure_live()?;

        let id = Uuid::new_v4();
        self.listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(ChangeListener {
                id,
                filter,
                on_change,
            });
        Ok(ChangeListenerRegistration {
            manager: Arc::downgrade(self),
            id,
        })
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
                let (id, notifications) = tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => {
                        debug!("cancel_token lit");
                        break;
                    },
                    val = change_rx.recv() => {
                        let Some(val) = val else {
                            break;
                        };
                        val
                    }
                };

                listener_notifications.clear();

                let listeners = self.listeners.lock().expect(ERROR_MUTEX);
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
            manager
                .listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

/// Check if a change path matches a listener path (including subpaths)
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
pub fn prop_matches(listener_prop: &Prop<'_>, change_prop: &automerge::Prop) -> bool {
    match (listener_prop, change_prop) {
        (Prop::Key(listener_key), automerge::Prop::Map(change_key)) => listener_key == change_key,
        (Prop::Index(listener_idx), automerge::Prop::Seq(change_idx)) => {
            *listener_idx == (*change_idx as u32)
        }
        _ => false,
    }
}
