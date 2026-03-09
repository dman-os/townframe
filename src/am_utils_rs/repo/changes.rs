use crate::interlude::*;

mod broker;

use automerge::ChangeHash;
use autosurgeon::Prop;
use samod::{DocHandle, DocumentId};
use samod_core::ChangeOrigin;
use std::sync::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum BigRepoChangeNotification {
    DocCreated {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: ChangeOrigin,
    },
    DocImported {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: ChangeOrigin,
    },
    DocChanged {
        doc_id: DocumentId,
        patch: Arc<automerge::Patch>,
        heads: Arc<[ChangeHash]>,
        origin: ChangeOrigin,
    },
}

#[derive(Debug, Clone)]
pub enum BigRepoLocalNotification {
    DocCreated {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    },
    DocImported {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    },
    DocHeadsUpdated {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    },
}

pub struct ChangeFilter {
    pub doc_id: Option<DocIdFilter>,
    pub origin: Option<OriginFilter>,
    pub path: Vec<Prop<'static>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginFilter {
    Local,
    Remote,
    Bootstrap,
}

pub struct LocalFilter {
    pub doc_id: Option<DocIdFilter>,
}

struct ChangeListener {
    id: Uuid,
    filter: ChangeFilter,
    on_change: Box<dyn Fn(Vec<BigRepoChangeNotification>) + Send + Sync + 'static>,
}

struct LocalListener {
    id: Uuid,
    filter: LocalFilter,
    on_change: Box<dyn Fn(Vec<BigRepoLocalNotification>) + Send + Sync + 'static>,
}

pub struct ChangeListenerManager {
    listeners: Arc<Mutex<Vec<ChangeListener>>>,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    local_listeners: Mutex<Vec<LocalListener>>,
    local_tx: mpsc::UnboundedSender<Vec<BigRepoLocalNotification>>,
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
}

impl ChangeListenerManagerStopToken {
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

#[non_exhaustive]
pub struct DocIdFilter {
    pub doc_id: DocumentId,
}

impl ChangeListenerManager {
    pub fn boot() -> (Arc<Self>, ChangeListenerManagerStopToken) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let (local_tx, local_rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();
        let out = Self {
            listeners: Arc::new(default()),
            change_tx,
            local_listeners: default(),
            local_tx,
            brokers: default(),
            cancel_token: cancel_token.clone(),
        };
        let out = Arc::new(out);

        let handle = Arc::clone(&out).spawn_switchboard(change_rx, local_rx);

        (
            out,
            ChangeListenerManagerStopToken {
                cancel_token,
                switchboard_handle: Some(handle),
            },
        )
    }

    fn ensure_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("ChangeListenerManager is stopped");
        }
        Ok(())
    }

    pub async fn add_doc(&self, handle: DocHandle) -> Res<Arc<broker::DocChangeBrokerHandle>> {
        self.ensure_live()?;

        let doc_id = handle.document_id().clone();
        match self.brokers.entry(doc_id) {
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                let (broker, _) = occupied.get();
                Ok(Arc::clone(broker))
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let (broker, stop_token) = broker::spawn_doc_listener(
                    handle,
                    self.cancel_token.child_token(),
                    self.change_tx.clone(),
                    Arc::new({
                        let listeners = Arc::clone(&self.listeners);
                        move |doc_id, origin| {
                            let listeners = listeners.lock().expect(ERROR_MUTEX);
                            listeners.iter().any(|listener| {
                                let doc_ok = listener
                                    .filter
                                    .doc_id
                                    .as_ref()
                                    .map(|target| target.doc_id == *doc_id)
                                    .unwrap_or(true);
                                let origin_ok = listener
                                    .filter
                                    .origin
                                    .as_ref()
                                    .map(|target| origin_matches_filter(origin, *target))
                                    .unwrap_or(true);
                                doc_ok && origin_ok
                            })
                        }
                    }),
                )?;
                let broker = Arc::new(broker);
                let stop_token = Arc::new(stop_token);
                vacant.insert((Arc::clone(&broker), stop_token));
                Ok(broker)
            }
        }
    }

    pub fn notify_doc_created(&self, doc_id: DocumentId, heads: Arc<[ChangeHash]>) -> Res<()> {
        self.ensure_live()?;
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocCreated {
                doc_id,
                heads,
                origin: ChangeOrigin::Local,
            }])?;
        Ok(())
    }

    pub fn notify_doc_imported(&self, doc_id: DocumentId, heads: Arc<[ChangeHash]>) -> Res<()> {
        self.ensure_live()?;
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocImported {
                doc_id,
                heads,
                origin: ChangeOrigin::Local,
            }])?;
        Ok(())
    }

    pub fn notify_local_doc_created(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocCreated { doc_id, heads }])?;
        Ok(())
    }

    pub fn notify_local_doc_imported(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocImported {
                doc_id,
                heads,
            }])?;
        Ok(())
    }

    pub fn notify_local_doc_heads_updated(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocHeadsUpdated {
                doc_id,
                heads,
            }])?;
        Ok(())
    }

    pub async fn add_listener(
        self: &Arc<Self>,
        filter: ChangeFilter,
        on_change: Box<dyn Fn(Vec<BigRepoChangeNotification>) + Send + Sync + 'static>,
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

    pub async fn add_local_listener(
        self: &Arc<Self>,
        filter: LocalFilter,
        on_change: Box<dyn Fn(Vec<BigRepoLocalNotification>) + Send + Sync + 'static>,
    ) -> Res<LocalListenerRegistration> {
        self.ensure_live()?;

        let id = Uuid::new_v4();
        self.local_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(LocalListener {
                id,
                filter,
                on_change,
            });
        Ok(LocalListenerRegistration {
            manager: Arc::downgrade(self),
            id,
        })
    }

    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
        mut local_rx: mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    ) -> JoinHandle<()> {
        let fut = async move {
            let mut listener_notifications: std::collections::HashMap<
                usize,
                Vec<BigRepoChangeNotification>,
            > = std::collections::HashMap::new();
            let mut local_listener_notifications: std::collections::HashMap<
                usize,
                Vec<BigRepoLocalNotification>,
            > = std::collections::HashMap::new();
            loop {
                enum SwitchboardInput {
                    Remote(Vec<BigRepoChangeNotification>),
                    Local(Vec<BigRepoLocalNotification>),
                }
                let input = tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => {
                        debug!("cancel_token lit");
                        break;
                    },
                    val = change_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Remote(val)
                    },
                    val = local_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Local(val)
                    }
                };

                match input {
                    SwitchboardInput::Remote(notifications) => {
                        listener_notifications.clear();
                        let listeners = self.listeners.lock().expect(ERROR_MUTEX);
                        for (listener_idx, listener) in listeners.iter().enumerate() {
                            let mut relevant_notifications = Vec::new();
                            for notification in &notifications {
                                if notification_matches_filter(notification, &listener.filter) {
                                    relevant_notifications.push(notification.clone());
                                }
                            }
                            if !relevant_notifications.is_empty() {
                                listener_notifications.insert(listener_idx, relevant_notifications);
                            }
                        }
                        for (listener_idx, notifications) in listener_notifications.drain() {
                            if let Some(listener) = listeners.get(listener_idx) {
                                (listener.on_change)(notifications);
                            }
                        }
                    }
                    SwitchboardInput::Local(notifications) => {
                        local_listener_notifications.clear();
                        let listeners = self.local_listeners.lock().expect(ERROR_MUTEX);
                        for (listener_idx, listener) in listeners.iter().enumerate() {
                            let mut relevant_notifications = Vec::new();
                            for notification in &notifications {
                                if local_notification_matches_filter(notification, &listener.filter)
                                {
                                    relevant_notifications.push(notification.clone());
                                }
                            }
                            if !relevant_notifications.is_empty() {
                                local_listener_notifications
                                    .insert(listener_idx, relevant_notifications);
                            }
                        }
                        for (listener_idx, notifications) in local_listener_notifications.drain() {
                            if let Some(listener) = listeners.get(listener_idx) {
                                (listener.on_change)(notifications);
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        }
        .instrument(tracing::info_span!("repo change notif switchboard task"));
        tokio::spawn(async { fut.await.unwrap() })
    }
}

fn notification_matches_filter(
    notification: &BigRepoChangeNotification,
    filter: &ChangeFilter,
) -> bool {
    let doc_id = match notification {
        BigRepoChangeNotification::DocCreated { doc_id, .. } => doc_id,
        BigRepoChangeNotification::DocImported { doc_id, .. } => doc_id,
        BigRepoChangeNotification::DocChanged { doc_id, .. } => doc_id,
    };
    if filter
        .doc_id
        .as_ref()
        .map(|target| target.doc_id != *doc_id)
        .unwrap_or_default()
    {
        return false;
    }

    let origin = match notification {
        BigRepoChangeNotification::DocCreated { origin, .. } => origin,
        BigRepoChangeNotification::DocImported { origin, .. } => origin,
        BigRepoChangeNotification::DocChanged { origin, .. } => origin,
    };
    if filter
        .origin
        .as_ref()
        .map(|target| !origin_matches_filter(origin, *target))
        .unwrap_or_default()
    {
        return false;
    }

    if filter.path.is_empty() {
        return true;
    }

    match notification {
        BigRepoChangeNotification::DocChanged { patch, .. } => {
            path_prefix_matches(&filter.path, &patch.path[..])
        }
        BigRepoChangeNotification::DocCreated { .. }
        | BigRepoChangeNotification::DocImported { .. } => false,
    }
}

fn origin_matches_filter(origin: &ChangeOrigin, filter: OriginFilter) -> bool {
    match filter {
        OriginFilter::Local => matches!(origin, ChangeOrigin::Local),
        OriginFilter::Remote => matches!(origin, ChangeOrigin::Remote { .. }),
        OriginFilter::Bootstrap => matches!(origin, ChangeOrigin::Bootstrap),
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

fn local_notification_matches_filter(
    notification: &BigRepoLocalNotification,
    filter: &LocalFilter,
) -> bool {
    let doc_id = match notification {
        BigRepoLocalNotification::DocCreated { doc_id, .. } => doc_id,
        BigRepoLocalNotification::DocImported { doc_id, .. } => doc_id,
        BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } => doc_id,
    };
    !filter
        .doc_id
        .as_ref()
        .map(|target| target.doc_id != *doc_id)
        .unwrap_or_default()
}

pub struct LocalListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for LocalListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .local_listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

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

pub fn prop_matches(listener_prop: &Prop<'_>, change_prop: &automerge::Prop) -> bool {
    match (listener_prop, change_prop) {
        (Prop::Key(listener_key), automerge::Prop::Map(change_key)) => listener_key == change_key,
        (Prop::Index(listener_idx), automerge::Prop::Seq(change_idx)) => {
            *listener_idx == (*change_idx as u32)
        }
        _ => false,
    }
}
