use crate::interlude::*;

use crate::repo::DocumentId;
use automerge::ChangeHash;
use autosurgeon::Prop;
use std::sync::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BigRepoChangeOrigin {
    Local,
    Remote { peer_id: crate::repo::PeerId },
    Bootstrap,
}

#[derive(Debug, Clone)]
pub enum BigRepoChangeNotification {
    DocCreated {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    },
    DocImported {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    },
    DocChanged {
        doc_id: DocumentId,
        patch: Arc<automerge::Patch>,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
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

#[derive(Debug, Clone)]
pub enum BigRepoHeadNotification {
    DocHeadsChanged {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
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

pub struct HeadFilter {
    pub doc_id: Option<DocIdFilter>,
}

struct ChangeListener {
    id: Uuid,
    filter: ChangeFilter,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
}

struct LocalListener {
    id: Uuid,
    filter: LocalFilter,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoLocalNotification>>,
}

pub struct ChangeListenerManager {
    listeners: Arc<Mutex<Vec<ChangeListener>>>,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    head_listeners: Mutex<Vec<HeadListener>>,
    head_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
    local_listeners: Mutex<Vec<LocalListener>>,
    /// used to send local ops to the switchboard
    local_tx: mpsc::UnboundedSender<Vec<BigRepoLocalNotification>>,
    cancel_token: CancellationToken,
}

pub struct ChangeListenerManagerStopToken {
    cancel_token: CancellationToken,
    switchboard_handle: JoinHandle<()>,
}

impl ChangeListenerManagerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.switchboard_handle, Duration::from_secs(5))
            .await
            .map_err(eyre::Report::from)
    }
}

#[non_exhaustive]
pub struct DocIdFilter {
    pub doc_id: DocumentId,
}

impl DocIdFilter {
    pub fn new(doc_id: DocumentId) -> Self {
        Self { doc_id }
    }
}

impl ChangeListenerManager {
    pub fn boot() -> (Arc<Self>, ChangeListenerManagerStopToken) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let (head_tx, head_rx) = mpsc::unbounded_channel();
        let (local_tx, local_rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();
        let out = Self {
            listeners: default(),
            change_tx,
            head_listeners: default(),
            head_tx,
            local_listeners: default(),
            local_tx,
            cancel_token: cancel_token.clone(),
        };
        let out = Arc::new(out);
        let handle = Arc::clone(&out).spawn_switchboard(change_rx, head_rx, local_rx);

        (
            out,
            ChangeListenerManagerStopToken {
                cancel_token,
                switchboard_handle: handle,
            },
        )
    }

    fn ensure_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("ChangeListenerManager is stopped");
        }
        Ok(())
    }

    pub(super) fn notify_doc_created(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocCreated {
                doc_id,
                heads,
                origin: BigRepoChangeOrigin::Local,
            }])?;
        Ok(())
    }

    pub(super) fn notify_doc_imported(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocImported {
                doc_id,
                heads,
                origin: BigRepoChangeOrigin::Local,
            }])?;
        Ok(())
    }

    pub(super) fn notify_doc_changed(
        &self,
        doc_id: DocumentId,
        patch: Arc<automerge::Patch>,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.ensure_live()?;
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocChanged {
                doc_id,
                patch,
                heads,
                origin,
            }])?;
        Ok(())
    }

    pub(super) fn notify_doc_heads_changed(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.ensure_live()?;
        self.head_tx
            .send(vec![BigRepoHeadNotification::DocHeadsChanged {
                doc_id,
                heads,
                origin,
            }])?;
        Ok(())
    }

    pub(super) fn notify_local_doc_created(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocCreated { doc_id, heads }])?;
        Ok(())
    }

    pub(super) fn notify_local_doc_imported(
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

    pub(super) fn notify_local_doc_heads_updated(
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

    pub async fn subscribe_listener(
        self: &Arc<Self>,
        filter: ChangeFilter,
    ) -> Res<(
        ChangeListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        self.ensure_live()?;

        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(ChangeListener {
                id,
                filter,
                change_tx,
            });
        Ok((
            ChangeListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            change_rx,
        ))
    }

    pub async fn subscribe_local_listener(
        self: &Arc<Self>,
        filter: LocalFilter,
    ) -> Res<(
        LocalListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    )> {
        self.ensure_live()?;

        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.local_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(LocalListener {
                id,
                filter,
                change_tx,
            });
        Ok((
            LocalListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            change_rx,
        ))
    }

    pub async fn subscribe_head_listener(
        self: &Arc<Self>,
        filter: HeadFilter,
    ) -> Res<(
        HeadListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoHeadNotification>>,
    )> {
        self.ensure_live()?;
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.head_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(HeadListener {
                id,
                filter,
                change_tx,
            });
        Ok((
            HeadListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            change_rx,
        ))
    }

    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
        mut head_rx: mpsc::UnboundedReceiver<Vec<BigRepoHeadNotification>>,
        mut local_rx: mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
    ) -> JoinHandle<()> {
        let fut = async move {
            loop {
                enum SwitchboardInput {
                    Remote(Vec<BigRepoChangeNotification>),
                    Heads(Vec<BigRepoHeadNotification>),
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
                    val = head_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Heads(val)
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
                        let to_send = {
                            let listeners = self.listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                let mut relevant_notifications = Vec::new();
                                for notification in &notifications {
                                    if notification_matches_filter(notification, &listener.filter) {
                                        relevant_notifications.push(notification.clone());
                                    }
                                }
                                if relevant_notifications.is_empty() {
                                    continue;
                                }
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    relevant_notifications,
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, relevant_notifications) in to_send {
                            if change_tx.send(relevant_notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.listeners.lock().expect(ERROR_MUTEX);
                            listeners
                                .retain(|listener| !failed_listener_ids.contains(&listener.id));
                        }
                    }
                    SwitchboardInput::Heads(notifications) => {
                        let to_send = {
                            let listeners = self.head_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                let mut relevant_notifications = Vec::new();
                                for notification in &notifications {
                                    if head_notification_matches_filter(
                                        notification,
                                        &listener.filter,
                                    ) {
                                        relevant_notifications.push(notification.clone());
                                    }
                                }
                                if relevant_notifications.is_empty() {
                                    continue;
                                }
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    relevant_notifications,
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, relevant_notifications) in to_send {
                            if change_tx.send(relevant_notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.head_listeners.lock().expect(ERROR_MUTEX);
                            listeners
                                .retain(|listener| !failed_listener_ids.contains(&listener.id));
                        }
                    }
                    SwitchboardInput::Local(notifications) => {
                        let to_send = {
                            let listeners = self.local_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                let mut relevant_notifications = Vec::new();
                                for notification in &notifications {
                                    if local_notification_matches_filter(
                                        notification,
                                        &listener.filter,
                                    ) {
                                        relevant_notifications.push(notification.clone());
                                    }
                                }
                                if relevant_notifications.is_empty() {
                                    continue;
                                }
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    relevant_notifications,
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, relevant_notifications) in to_send {
                            if change_tx.send(relevant_notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.local_listeners.lock().expect(ERROR_MUTEX);
                            listeners
                                .retain(|listener| !failed_listener_ids.contains(&listener.id));
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

fn origin_matches_filter(origin: &BigRepoChangeOrigin, filter: OriginFilter) -> bool {
    match filter {
        OriginFilter::Local => matches!(origin, BigRepoChangeOrigin::Local),
        OriginFilter::Remote => matches!(origin, BigRepoChangeOrigin::Remote { .. }),
        OriginFilter::Bootstrap => matches!(origin, BigRepoChangeOrigin::Bootstrap),
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

struct HeadListener {
    id: Uuid,
    filter: HeadFilter,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
}

pub struct HeadListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for HeadListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .head_listeners
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

fn head_notification_matches_filter(
    notification: &BigRepoHeadNotification,
    filter: &HeadFilter,
) -> bool {
    let doc_id = match notification {
        BigRepoHeadNotification::DocHeadsChanged { doc_id, .. } => doc_id,
    };
    !filter
        .doc_id
        .as_ref()
        .map(|target| target.doc_id != *doc_id)
        .unwrap_or_default()
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
