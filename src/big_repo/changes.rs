use crate::interlude::*;

use crate::keyhive_listener::BigRepoKeyhiveListener;
use crate::DocumentId;
use automerge::ChangeHash;
use autosurgeon::Prop;
use beekem::operation::CgkaOperation;
use future_form::Sendable;
use keyhive_core::principal::group::{delegation::Delegation, revocation::Revocation};
use keyhive_core::principal::individual::op::{add_key::AddKeyOp, rotate_key::RotateKeyOp};
use keyhive_crypto::signed::Signed;
use keyhive_crypto::signer::memory::MemorySigner;
use std::sync::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// Keyhive event payload type aliases — concrete for our generics.
pub(crate) type SignedAddKeyOp = Signed<AddKeyOp>;
pub(crate) type SignedRotateKeyOp = Signed<RotateKeyOp>;
pub(crate) type SignedCgkaOp = Signed<CgkaOperation>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BigRepoChangeOrigin {
    Local,
    Remote { peer_id: PeerId },
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

#[expect(clippy::enum_variant_names)]
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
    DocMaterializationPending {
        doc_id: DocumentId,
    },
    DocMaterializationReady {
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

#[derive(Debug, Clone)]
pub enum BigRepoPendingHeadNotification {
    DocPendingHeads {
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    },
}

#[derive(Debug, Clone)]
pub enum BigRepoPrekeyNotification {
    PrekeysExpanded {
        new_prekey: Arc<SignedAddKeyOp>,
    },
    PrekeyRotated {
        rotate_key: Arc<SignedRotateKeyOp>,
    },
}

#[derive(Debug, Clone)]
pub enum BigRepoCgkaNotification {
    CgkaOp {
        data: Arc<SignedCgkaOp>,
    },
}

#[expect(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub enum BigRepoMembershipNotification {
    DelegationReceived {
        data: Arc<Signed<Delegation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>>,
    },
    RevocationReceived {
        data: Arc<Signed<Revocation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>>,
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

pub struct PendingHeadFilter {
    pub doc_id: Option<DocIdFilter>,
}

/// No filter fields yet — all prekey events are delivered to all subscribers.
pub struct PrekeyFilter;

/// No filter fields yet — all cgka events are delivered to all subscribers.
pub struct CgkaFilter;

/// No filter fields yet — all membership events are delivered to all subscribers.
pub struct MembershipFilter;

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

struct PrekeyListener {
    id: Uuid,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoPrekeyNotification>>,
}

struct CgkaListener {
    id: Uuid,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoCgkaNotification>>,
}

struct MembershipListener {
    id: Uuid,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoMembershipNotification>>,
}

pub struct ChangeListenerManager {
    listeners: Arc<Mutex<Vec<ChangeListener>>>,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    head_listeners: Mutex<Vec<HeadListener>>,
    head_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
    pending_head_listeners: Mutex<Vec<PendingHeadListener>>,
    pending_head_tx: mpsc::UnboundedSender<Vec<BigRepoPendingHeadNotification>>,
    local_listeners: Mutex<Vec<LocalListener>>,
    /// used to send local ops to the switchboard
    local_tx: mpsc::UnboundedSender<Vec<BigRepoLocalNotification>>,
    prekey_listeners: Mutex<Vec<PrekeyListener>>,
    prekey_tx: mpsc::UnboundedSender<Vec<BigRepoPrekeyNotification>>,
    cgka_listeners: Mutex<Vec<CgkaListener>>,
    cgka_tx: mpsc::UnboundedSender<Vec<BigRepoCgkaNotification>>,
    membership_listeners: Mutex<Vec<MembershipListener>>,
    membership_tx: mpsc::UnboundedSender<Vec<BigRepoMembershipNotification>>,
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
        let (pending_head_tx, pending_head_rx) = mpsc::unbounded_channel();
        let (prekey_tx, prekey_rx) = mpsc::unbounded_channel();
        let (cgka_tx, cgka_rx) = mpsc::unbounded_channel();
        let (membership_tx, membership_rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();
        let out = Self {
            listeners: default(),
            change_tx,
            head_listeners: default(),
            head_tx,
            pending_head_listeners: default(),
            pending_head_tx,
            local_listeners: default(),
            local_tx,
            prekey_listeners: default(),
            prekey_tx,
            cgka_listeners: default(),
            cgka_tx,
            membership_listeners: default(),
            membership_tx,
            cancel_token: cancel_token.clone(),
        };
        let out = Arc::new(out);
        let handle = Arc::clone(&out).spawn_switchboard(
            change_rx,
            head_rx,
            local_rx,
            pending_head_rx,
            prekey_rx,
            cgka_rx,
            membership_rx,
        );

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

    #[tracing::instrument(skip(self, heads))]
    pub(super) fn notify_doc_created(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        trace!("queue doc created notification");
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocCreated {
                doc_id,
                heads,
                origin: BigRepoChangeOrigin::Local,
            }])?;
        Ok(())
    }

    #[tracing::instrument(skip(self, heads))]
    pub(super) fn notify_doc_imported(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        trace!("queue doc imported notification");
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocImported {
                doc_id,
                heads,
                origin: BigRepoChangeOrigin::Local,
            }])?;
        Ok(())
    }

    #[tracing::instrument(skip(self, heads, patch))]
    pub(super) fn notify_doc_changed(
        &self,
        doc_id: DocumentId,
        patch: Arc<automerge::Patch>,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.ensure_live()?;
        trace!("queue doc changed notification");
        self.change_tx
            .send(vec![BigRepoChangeNotification::DocChanged {
                doc_id,
                patch,
                heads,
                origin,
            }])?;
        Ok(())
    }

    #[tracing::instrument(skip(self, heads))]
    pub(super) fn notify_doc_heads_changed(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.ensure_live()?;
        trace!("queue doc heads notification");
        self.head_tx
            .send(vec![BigRepoHeadNotification::DocHeadsChanged {
                doc_id,
                heads,
                origin,
            }])?;
        Ok(())
    }

    #[tracing::instrument(skip(self, heads))]
    pub(super) fn notify_doc_pending_heads_changed(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        self.ensure_live()?;
        trace!("queue doc pending heads notification");
        self.pending_head_tx
            .send(vec![BigRepoPendingHeadNotification::DocPendingHeads {
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

    pub(super) fn notify_local_doc_materialization_pending(&self, doc_id: DocumentId) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocMaterializationPending {
                doc_id,
            }])?;
        Ok(())
    }

    pub(super) fn notify_local_doc_materialization_ready(
        &self,
        doc_id: DocumentId,
        heads: Arc<[ChangeHash]>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.local_tx
            .send(vec![BigRepoLocalNotification::DocMaterializationReady {
                doc_id,
                heads,
            }])?;
        Ok(())
    }

    pub(super) fn notify_prekeys_expanded(
        &self,
        new_prekey: Arc<SignedAddKeyOp>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.prekey_tx
            .send(vec![BigRepoPrekeyNotification::PrekeysExpanded {
                new_prekey,
            }])?;
        Ok(())
    }

    pub(super) fn notify_prekey_rotated(
        &self,
        rotate_key: Arc<SignedRotateKeyOp>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.prekey_tx
            .send(vec![BigRepoPrekeyNotification::PrekeyRotated {
                rotate_key,
            }])?;
        Ok(())
    }

    pub(super) fn notify_cgka_op(
        &self,
        data: Arc<SignedCgkaOp>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.cgka_tx
            .send(vec![BigRepoCgkaNotification::CgkaOp {
                data,
            }])?;
        Ok(())
    }

    pub(super) fn notify_delegation_received(
        &self,
        data: Arc<Signed<Delegation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.membership_tx
            .send(vec![BigRepoMembershipNotification::DelegationReceived {
                data,
            }])?;
        Ok(())
    }

    pub(super) fn notify_revocation_received(
        &self,
        data: Arc<Signed<Revocation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>>,
    ) -> Res<()> {
        self.ensure_live()?;
        self.membership_tx
            .send(vec![BigRepoMembershipNotification::RevocationReceived {
                data,
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

    pub fn has_change_listener_interest(
        &self,
        doc_id: DocumentId,
        origin: &BigRepoChangeOrigin,
    ) -> bool {
        let listeners = self.listeners.lock().expect(ERROR_MUTEX);
        listeners.iter().any(|listener| {
            listener
                .filter
                .doc_id
                .as_ref()
                .map(|target| target.doc_id == doc_id)
                .unwrap_or(true)
                && listener
                    .filter
                    .origin
                    .as_ref()
                    .map(|target| origin_matches_filter(origin, *target))
                    .unwrap_or(true)
        })
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

    pub async fn subscribe_pending_head_listener(
        self: &Arc<Self>,
        filter: PendingHeadFilter,
    ) -> Res<(
        PendingHeadListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoPendingHeadNotification>>,
    )> {
        self.ensure_live()?;
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.pending_head_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(PendingHeadListener {
                id,
                filter,
                change_tx,
            });
        Ok((
            PendingHeadListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            change_rx,
        ))
    }

    pub async fn subscribe_prekey_listener(
        self: &Arc<Self>,
        _filter: PrekeyFilter,
    ) -> Res<(
        PrekeyListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoPrekeyNotification>>,
    )> {
        self.ensure_live()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.prekey_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(PrekeyListener { id, change_tx: tx });
        Ok((
            PrekeyListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            rx,
        ))
    }

    pub async fn subscribe_cgka_listener(
        self: &Arc<Self>,
        _filter: CgkaFilter,
    ) -> Res<(
        CgkaListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoCgkaNotification>>,
    )> {
        self.ensure_live()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.cgka_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(CgkaListener { id, change_tx: tx });
        Ok((
            CgkaListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            rx,
        ))
    }

    pub async fn subscribe_membership_listener(
        self: &Arc<Self>,
        _filter: MembershipFilter,
    ) -> Res<(
        MembershipListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoMembershipNotification>>,
    )> {
        self.ensure_live()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.membership_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(MembershipListener {
                id,
                change_tx: tx,
            });
        Ok((
            MembershipListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            rx,
        ))
    }

    #[expect(clippy::too_many_arguments)]
    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
        mut head_rx: mpsc::UnboundedReceiver<Vec<BigRepoHeadNotification>>,
        mut local_rx: mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
        mut pending_head_rx: mpsc::UnboundedReceiver<Vec<BigRepoPendingHeadNotification>>,
        mut prekey_rx: mpsc::UnboundedReceiver<Vec<BigRepoPrekeyNotification>>,
        mut cgka_rx: mpsc::UnboundedReceiver<Vec<BigRepoCgkaNotification>>,
        mut membership_rx: mpsc::UnboundedReceiver<Vec<BigRepoMembershipNotification>>,
    ) -> JoinHandle<()> {
        let fut = async move {
            loop {
                enum SwitchboardInput {
                    Remote(Vec<BigRepoChangeNotification>),
                    Heads(Vec<BigRepoHeadNotification>),
                    Local(Vec<BigRepoLocalNotification>),
                    PendingHeads(Vec<BigRepoPendingHeadNotification>),
                    Prekey(Vec<BigRepoPrekeyNotification>),
                    Cgka(Vec<BigRepoCgkaNotification>),
                    Membership(Vec<BigRepoMembershipNotification>),
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
                    },
                    val = pending_head_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::PendingHeads(val)
                    },
                    val = prekey_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Prekey(val)
                    },
                    val = cgka_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Cgka(val)
                    },
                    val = membership_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Membership(val)
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
                    SwitchboardInput::PendingHeads(notifications) => {
                        let to_send = {
                            let listeners = self.pending_head_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                let mut relevant_notifications = Vec::new();
                                for notification in &notifications {
                                    if pending_head_notification_matches_filter(
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
                            let mut listeners = self.pending_head_listeners.lock().expect(ERROR_MUTEX);
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
                    SwitchboardInput::Prekey(notifications) => {
                        let to_send = {
                            let listeners = self.prekey_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    notifications.clone(),
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, notifications) in to_send {
                            if change_tx.send(notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.prekey_listeners.lock().expect(ERROR_MUTEX);
                            listeners
                                .retain(|listener| !failed_listener_ids.contains(&listener.id));
                        }
                    }
                    SwitchboardInput::Cgka(notifications) => {
                        let to_send = {
                            let listeners = self.cgka_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    notifications.clone(),
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, notifications) in to_send {
                            if change_tx.send(notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.cgka_listeners.lock().expect(ERROR_MUTEX);
                            listeners
                                .retain(|listener| !failed_listener_ids.contains(&listener.id));
                        }
                    }
                    SwitchboardInput::Membership(notifications) => {
                        let to_send = {
                            let listeners = self.membership_listeners.lock().expect(ERROR_MUTEX);
                            let mut to_send = Vec::new();
                            for listener in listeners.iter() {
                                to_send.push((
                                    listener.id,
                                    listener.change_tx.clone(),
                                    notifications.clone(),
                                ));
                            }
                            to_send
                        };
                        let mut failed_listener_ids = Vec::new();
                        for (listener_id, change_tx, notifications) in to_send {
                            if change_tx.send(notifications).is_err() {
                                failed_listener_ids.push(listener_id);
                            }
                        }
                        if !failed_listener_ids.is_empty() {
                            let mut listeners = self.membership_listeners.lock().expect(ERROR_MUTEX);
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

struct PendingHeadListener {
    id: Uuid,
    filter: PendingHeadFilter,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoPendingHeadNotification>>,
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

pub struct PendingHeadListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for PendingHeadListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .pending_head_listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

pub struct PrekeyListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for PrekeyListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .prekey_listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

pub struct CgkaListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for CgkaListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .cgka_listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

pub struct MembershipListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for MembershipListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .membership_listeners
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
        BigRepoLocalNotification::DocMaterializationPending { doc_id } => doc_id,
        BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
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

fn pending_head_notification_matches_filter(
    notification: &BigRepoPendingHeadNotification,
    filter: &PendingHeadFilter,
) -> bool {
    let doc_id = match notification {
        BigRepoPendingHeadNotification::DocPendingHeads { doc_id, .. } => doc_id,
    };
    !filter
        .doc_id
        .as_ref()
        .map(|target| target.doc_id != *doc_id)
        .unwrap_or_default()
}

fn prekey_notification_matches_filter(
    _notification: &BigRepoPrekeyNotification,
    _filter: &PrekeyFilter,
) -> bool {
    // No filter fields yet — always deliver.
    true
}

fn cgka_notification_matches_filter(
    _notification: &BigRepoCgkaNotification,
    _filter: &CgkaFilter,
) -> bool {
    // No filter fields yet — always deliver.
    true
}

fn membership_notification_matches_filter(
    _notification: &BigRepoMembershipNotification,
    _filter: &MembershipFilter,
) -> bool {
    // No filter fields yet — always deliver.
    true
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

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};

    fn make_change_fixture() -> (DocumentId, Arc<[ChangeHash]>, Arc<automerge::Patch>) {
        let doc_id = DocumentId::random();
        let mut doc = automerge::Automerge::new();
        let before_heads = doc.get_heads();
        doc.transact(|tx| {
            tx.put(automerge::ROOT, "title", "seed")
                .expect("failed seeding doc");
            eyre::Ok(())
        })
        .expect("failed creating doc change");
        let after_heads = doc.get_heads();
        let patch = doc
            .diff(&before_heads, &after_heads)
            .into_iter()
            .next()
            .expect("expected patch for doc change");
        (doc_id, Arc::from(after_heads), Arc::new(patch))
    }

    async fn recv_batch<T: Send>(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<T>>) -> Vec<T> {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for notification")
            .expect("listener closed unexpectedly")
    }

    #[tokio::test]
    async fn change_listener_drop_unregisters_listener() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (doc_id, heads, patch) = make_change_fixture();
        let (registration, mut rx) = manager
            .subscribe_listener(ChangeFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
                origin: None,
                path: Vec::new(),
            })
            .await?;

        assert!(manager.has_change_listener_interest(doc_id, &BigRepoChangeOrigin::Local));
        drop(registration);
        assert!(!manager.has_change_listener_interest(doc_id, &BigRepoChangeOrigin::Local));

        manager.notify_doc_changed(doc_id, patch, heads, BigRepoChangeOrigin::Local)?;
        let closed = timeout(Duration::from_millis(250), rx.recv())
            .await
            .expect("expected receiver to resolve")
            .is_none();
        assert!(closed, "change listener should be removed on drop");
        Ok(())
    }

    #[tokio::test]
    async fn head_listener_drop_unregisters_listener() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (doc_id, heads, _) = make_change_fixture();
        let (registration, mut rx) = manager
            .subscribe_head_listener(HeadFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
            })
            .await?;

        drop(registration);
        manager.notify_doc_heads_changed(doc_id, heads, BigRepoChangeOrigin::Bootstrap)?;
        let closed = timeout(Duration::from_millis(250), rx.recv())
            .await
            .expect("expected receiver to resolve")
            .is_none();
        assert!(closed, "head listener should be removed on drop");
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_manager_stop_blocks_subscribe_and_notify() -> Res<()> {
        let (manager, stop) = ChangeListenerManager::boot();
        stop.stop().await?;

        let result = manager
            .subscribe_listener(ChangeFilter {
                doc_id: None,
                origin: None,
                path: Vec::new(),
            })
            .await;
        assert!(
            result.is_err(),
            "stopped manager should reject subscriptions"
        );
        let err = result.err().expect("expected subscription error");
        assert!(err.to_string().contains("stopped"));

        let (doc_id, heads, patch) = make_change_fixture();
        let err = manager
            .notify_doc_changed(doc_id, patch, heads, BigRepoChangeOrigin::Local)
            .expect_err("stopped manager should reject notifications");
        assert!(err.to_string().contains("stopped"));
        Ok(())
    }

    #[tokio::test]
    async fn local_listener_receives_local_notifications() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (doc_id, heads, _) = make_change_fixture();
        let (_registration, mut rx) = manager
            .subscribe_local_listener(LocalFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
            })
            .await?;

        manager.notify_local_doc_created(doc_id, Arc::clone(&heads))?;
        let first_batch = recv_batch(&mut rx).await;
        assert!(matches!(
            first_batch.as_slice(),
            [BigRepoLocalNotification::DocCreated { doc_id: seen_doc_id, .. }]
            if *seen_doc_id == doc_id
        ));

        manager.notify_local_doc_imported(doc_id, Arc::clone(&heads))?;
        let second_batch = recv_batch(&mut rx).await;
        assert!(matches!(
            second_batch.as_slice(),
            [BigRepoLocalNotification::DocImported { doc_id: seen_doc_id, .. }]
            if *seen_doc_id == doc_id
        ));

        let heads_for_ready = Arc::clone(&heads);
        manager.notify_local_doc_heads_updated(doc_id, heads)?;
        let third_batch = recv_batch(&mut rx).await;
        assert!(matches!(
            third_batch.as_slice(),
            [BigRepoLocalNotification::DocHeadsUpdated { doc_id: seen_doc_id, .. }]
            if *seen_doc_id == doc_id
        ));
        manager.notify_local_doc_materialization_pending(doc_id)?;
        let fourth_batch = recv_batch(&mut rx).await;
        assert!(matches!(
            fourth_batch.as_slice(),
            [BigRepoLocalNotification::DocMaterializationPending { doc_id: seen_doc_id }]
            if *seen_doc_id == doc_id
        ));
        manager.notify_local_doc_materialization_ready(doc_id, heads_for_ready)?;
        let fifth_batch = recv_batch(&mut rx).await;
        assert!(matches!(
            fifth_batch.as_slice(),
            [BigRepoLocalNotification::DocMaterializationReady { doc_id: seen_doc_id, .. }]
            if *seen_doc_id == doc_id
        ));
        Ok(())
    }

    #[tokio::test]
    async fn head_listener_receives_head_notifications() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (doc_id, heads, _) = make_change_fixture();
        let (_registration, mut rx) = manager
            .subscribe_head_listener(HeadFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
            })
            .await?;

        manager.notify_doc_heads_changed(
            doc_id,
            heads,
            BigRepoChangeOrigin::Remote {
                peer_id: PeerId::new([42_u8; 32]),
            },
        )?;
        let batch = recv_batch(&mut rx).await;
        assert!(matches!(
            batch.as_slice(),
            [BigRepoHeadNotification::DocHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));
        Ok(())
    }

    #[tokio::test]
    async fn change_listener_origin_filters_remote_and_bootstrap() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (doc_id, heads, patch) = make_change_fixture();
        let (remote_registration, mut remote_rx) = manager
            .subscribe_listener(ChangeFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
                origin: Some(OriginFilter::Remote),
                path: Vec::new(),
            })
            .await?;
        let (bootstrap_registration, mut bootstrap_rx) = manager
            .subscribe_listener(ChangeFilter {
                doc_id: Some(DocIdFilter::new(doc_id)),
                origin: Some(OriginFilter::Bootstrap),
                path: Vec::new(),
            })
            .await?;

        manager.notify_doc_changed(
            doc_id,
            Arc::clone(&patch),
            Arc::clone(&heads),
            BigRepoChangeOrigin::Remote {
                peer_id: PeerId::new([11_u8; 32]),
            },
        )?;
        manager.notify_doc_changed(doc_id, patch, heads, BigRepoChangeOrigin::Bootstrap)?;

        let remote_batch = recv_batch(&mut remote_rx).await;
        assert!(matches!(
            remote_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let bootstrap_batch = recv_batch(&mut bootstrap_rx).await;
        assert!(matches!(
            bootstrap_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Bootstrap,
                ..
            }] if *seen_doc_id == doc_id
        ));

        drop(remote_registration);
        drop(bootstrap_registration);
        Ok(())
    }
}
