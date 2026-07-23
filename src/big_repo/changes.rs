//! BigRepo notification types, split into two families:
//!
//! 1. **Document change notifications** — ordinary doc lifecycle events
//!    (DocCreated, DocImported, DocChanged, heads, local materialization).
//! 2. **Domain notifications** — BigRepo-level events about group membership,
//!    document access control, and key rotation. These replace the old raw
//!    Keyhive notification families (PrekeyNotification, CgkaNotification,
//!    MembershipNotification) and expose only BigRepo-owned identifier types.

use crate::interlude::*;

use crate::DocumentId;
use automerge::ChangeHash;
use autosurgeon::Prop;
use std::sync::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

// ═══════════════════════════════════════════════════════════════════════════
// Document-level change notifications (keep as-is)
// ═══════════════════════════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════════════════════════
// Domain notifications — BigRepo-level, no Keyhive internals
// ═══════════════════════════════════════════════════════════════════════════

/// A group identifier, semantically distinct from a document or peer ID.
/// Backed by the same 32-byte representation (Keyhive group public key).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(pub [u8; 32]);

impl GroupId {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GroupId({:02x?}..)", &self.0[..4])
    }
}

/// Access level for a member on a document or group.
/// Re-exported from Keyhive but without exposing the Keyhive crate path.
/// Preserves all four Keyhive access levels distinctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigRepoAccess {
    /// Can retrieve encrypted bytes over the network (relay).
    Relay,
    /// Can read (decrypt) document content.
    Read,
    /// Can edit (append ops to) document content.
    Edit,
    /// Can manage membership (revoke any member, not just causally junior).
    Admin,
}

impl From<keyhive_core::access::Access> for BigRepoAccess {
    fn from(a: keyhive_core::access::Access) -> Self {
        match a {
            keyhive_core::access::Access::Relay => Self::Relay,
            keyhive_core::access::Access::Read => Self::Read,
            keyhive_core::access::Access::Edit => Self::Edit,
            keyhive_core::access::Access::Admin => Self::Admin,
        }
    }
}

/// BigRepo-level domain events that replace the old raw Keyhive
/// notification families.
#[derive(Debug, Clone)]
pub enum BigRepoDomainNotification {
    /// A document was added to a group's governance.
    DocumentAddedToGroup {
        doc_id: DocumentId,
        group_id: GroupId,
    },
    /// A document was removed from a group's governance.
    DocumentRemovedFromGroup {
        doc_id: DocumentId,
        group_id: GroupId,
    },
    /// A member was added to a group.
    MemberAddedToGroup {
        group_id: GroupId,
        member_id: PeerId,
        access: BigRepoAccess,
    },
    /// A member was removed from a group.
    MemberRemovedFromGroup {
        group_id: GroupId,
        member_id: PeerId,
    },
    /// A document's access control entry changed.
    DocumentAccessChanged {
        doc_id: DocumentId,
        member_id: PeerId,
        access: BigRepoAccess,
    },
    /// A member's access to a document was revoked.
    DocumentAccessRevoked {
        doc_id: DocumentId,
        member_id: PeerId,
    },
    /// A document's encryption key was rotated.
    DocumentKeyRotated { doc_id: DocumentId },
}

// ═══════════════════════════════════════════════════════════════════════════
// Filters
// ═══════════════════════════════════════════════════════════════════════════

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

/// Describes which domain events a subscriber is interested in.
/// Currently unfiltered — all domain events are delivered to every subscriber.
pub struct DomainFilter;

// ═══════════════════════════════════════════════════════════════════════════
// Internal listener bookkeeping
// ═══════════════════════════════════════════════════════════════════════════

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

struct DomainListener {
    id: Uuid,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoDomainNotification>>,
}

/// Unified manager for all notification families.
pub struct ChangeListenerManager {
    listeners: Arc<Mutex<Vec<ChangeListener>>>,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    head_listeners: Mutex<Vec<HeadListener>>,
    head_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
    pending_head_listeners: Mutex<Vec<PendingHeadListener>>,
    pending_head_tx: mpsc::UnboundedSender<Vec<BigRepoPendingHeadNotification>>,
    local_listeners: Mutex<Vec<LocalListener>>,
    local_tx: mpsc::UnboundedSender<Vec<BigRepoLocalNotification>>,
    domain_listeners: Mutex<Vec<DomainListener>>,
    domain_tx: mpsc::UnboundedSender<Vec<BigRepoDomainNotification>>,
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

pub struct DomainListenerRegistration {
    manager: std::sync::Weak<ChangeListenerManager>,
    id: Uuid,
}

impl Drop for DomainListenerRegistration {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let id = self.id;
            manager
                .domain_listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ChangeListenerManager implementation
// ═══════════════════════════════════════════════════════════════════════════

impl ChangeListenerManager {
    pub fn boot() -> (Arc<Self>, ChangeListenerManagerStopToken) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let (head_tx, head_rx) = mpsc::unbounded_channel();
        let (local_tx, local_rx) = mpsc::unbounded_channel();
        let (pending_head_tx, pending_head_rx) = mpsc::unbounded_channel();
        let (domain_tx, domain_rx) = mpsc::unbounded_channel();
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
            domain_listeners: default(),
            domain_tx,
            cancel_token: cancel_token.clone(),
        };
        let out = Arc::new(out);
        let handle = Arc::clone(&out).spawn_switchboard(
            change_rx,
            head_rx,
            local_rx,
            pending_head_rx,
            domain_rx,
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

    // ── Document change notify methods ────────────────────────────────────

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

    // ── Domain notification notify methods ────────────────────────────────

    /// Notify that a document was added to a group's governance.
    pub(super) fn notify_document_added_to_group(
        &self,
        doc_id: DocumentId,
        group_id: GroupId,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::DocumentAddedToGroup {
                doc_id,
                group_id,
            }])?;
        Ok(())
    }

    /// Notify that a member was added to a group.
    pub(super) fn notify_document_removed_from_group(
        &self,
        doc_id: DocumentId,
        group_id: GroupId,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::DocumentRemovedFromGroup {
                doc_id,
                group_id,
            }])?;
        Ok(())
    }

    pub(super) fn notify_member_added_to_group(
        &self,
        group_id: GroupId,
        member_id: PeerId,
        access: BigRepoAccess,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::MemberAddedToGroup {
                group_id,
                member_id,
                access,
            }])?;
        Ok(())
    }

    /// Notify that a member was removed from a group.
    pub(super) fn notify_member_removed_from_group(
        &self,
        group_id: GroupId,
        member_id: PeerId,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::MemberRemovedFromGroup {
                group_id,
                member_id,
            }])?;
        Ok(())
    }

    /// Notify that a document's access control entry changed.
    pub(super) fn notify_document_access_changed(
        &self,
        doc_id: DocumentId,
        member_id: PeerId,
        access: BigRepoAccess,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::DocumentAccessChanged {
                doc_id,
                member_id,
                access,
            }])?;
        Ok(())
    }

    /// Notify that a document's encryption key was rotated.
    pub(super) fn notify_document_access_revoked(
        &self,
        doc_id: DocumentId,
        member_id: PeerId,
    ) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::DocumentAccessRevoked {
                doc_id,
                member_id,
            }])?;
        Ok(())
    }

    pub(super) fn notify_document_key_rotated(&self, doc_id: DocumentId) -> Res<()> {
        self.ensure_live()?;
        self.domain_tx
            .send(vec![BigRepoDomainNotification::DocumentKeyRotated {
                doc_id,
            }])?;
        Ok(())
    }

    // ── Subscription methods ──────────────────────────────────────────────

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

    /// Subscribe to domain-level notifications.
    pub async fn subscribe_domain_listener(
        self: &Arc<Self>,
        _filter: DomainFilter,
    ) -> Res<(
        DomainListenerRegistration,
        mpsc::UnboundedReceiver<Vec<BigRepoDomainNotification>>,
    )> {
        self.ensure_live()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        self.domain_listeners
            .lock()
            .expect(ERROR_MUTEX)
            .push(DomainListener { id, change_tx: tx });
        Ok((
            DomainListenerRegistration {
                manager: Arc::downgrade(self),
                id,
            },
            rx,
        ))
    }

    // ── Switchboard ───────────────────────────────────────────────────────

    fn spawn_switchboard(
        self: Arc<Self>,
        mut change_rx: mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
        mut head_rx: mpsc::UnboundedReceiver<Vec<BigRepoHeadNotification>>,
        mut local_rx: mpsc::UnboundedReceiver<Vec<BigRepoLocalNotification>>,
        mut pending_head_rx: mpsc::UnboundedReceiver<Vec<BigRepoPendingHeadNotification>>,
        mut domain_rx: mpsc::UnboundedReceiver<Vec<BigRepoDomainNotification>>,
    ) -> JoinHandle<()> {
        let fut = async move {
            loop {
                enum SwitchboardInput {
                    Remote(Vec<BigRepoChangeNotification>),
                    Heads(Vec<BigRepoHeadNotification>),
                    Local(Vec<BigRepoLocalNotification>),
                    PendingHeads(Vec<BigRepoPendingHeadNotification>),
                    Domain(Vec<BigRepoDomainNotification>),
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
                    val = domain_rx.recv() => {
                        let Some(val) = val else {
                            continue;
                        };
                        SwitchboardInput::Domain(val)
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
                            let mut listeners =
                                self.pending_head_listeners.lock().expect(ERROR_MUTEX);
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
                    SwitchboardInput::Domain(notifications) => {
                        let to_send = {
                            let listeners = self.domain_listeners.lock().expect(ERROR_MUTEX);
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
                            let mut listeners = self.domain_listeners.lock().expect(ERROR_MUTEX);
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

// ═══════════════════════════════════════════════════════════════════════════
// Filter matching helpers
// ═══════════════════════════════════════════════════════════════════════════

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
            patch_matches_path(&filter.path, patch)
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

// ═══════════════════════════════════════════════════════════════════════════
// Path matching utilities (unchanged)
// ═══════════════════════════════════════════════════════════════════════════

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

fn patch_matches_path(listener_path: &[Prop<'_>], patch: &automerge::Patch) -> bool {
    if path_prefix_matches(listener_path, &patch.path[..]) {
        return true;
    }

    let Some((last_listener, listener_prefix)) = listener_path.split_last() else {
        return true;
    };
    listener_prefix.len() == patch.path.len()
        && path_prefix_matches(listener_prefix, &patch.path[..])
        && action_prop_matches(last_listener, &patch.action)
}

fn action_prop_matches(listener_prop: &Prop<'_>, action: &automerge::PatchAction) -> bool {
    match (listener_prop, action) {
        (Prop::Key(listener_key), automerge::PatchAction::PutMap { key, .. })
        | (Prop::Key(listener_key), automerge::PatchAction::DeleteMap { key }) => {
            listener_key == key
        }
        (Prop::Index(listener_idx), automerge::PatchAction::PutSeq { index, .. })
        | (Prop::Index(listener_idx), automerge::PatchAction::Insert { index, .. })
        | (Prop::Index(listener_idx), automerge::PatchAction::DeleteSeq { index, .. }) => {
            *listener_idx == (*index as u32)
        }
        (listener_prop, automerge::PatchAction::Increment { prop, .. })
        | (listener_prop, automerge::PatchAction::Conflict { prop }) => {
            prop_matches(listener_prop, prop)
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

    #[tokio::test]
    async fn domain_listener_receives_domain_notifications() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (_registration, mut rx) = manager.subscribe_domain_listener(DomainFilter).await?;

        let doc_id = DocumentId::random();
        let group_id = GroupId::new([1u8; 32]);
        let member_id = PeerId::new([2u8; 32]);

        manager.notify_document_added_to_group(doc_id, group_id)?;
        let batch1 = recv_batch(&mut rx).await;
        assert!(matches!(
            batch1.as_slice(),
            [BigRepoDomainNotification::DocumentAddedToGroup {
                doc_id: d, group_id: g, ..
            }] if *d == doc_id && *g == group_id
        ));

        manager.notify_member_added_to_group(group_id, member_id, BigRepoAccess::Read)?;
        let batch2 = recv_batch(&mut rx).await;
        assert!(matches!(
            batch2.as_slice(),
            [BigRepoDomainNotification::MemberAddedToGroup {
                group_id: g, member_id: m, access: a, ..
            }] if *g == group_id && *m == member_id && *a == BigRepoAccess::Read
        ));

        manager.notify_member_removed_from_group(group_id, member_id)?;
        let batch3 = recv_batch(&mut rx).await;
        assert!(matches!(
            batch3.as_slice(),
            [BigRepoDomainNotification::MemberRemovedFromGroup {
                group_id: g, member_id: m, ..
            }] if *g == group_id && *m == member_id
        ));

        manager.notify_document_access_changed(doc_id, member_id, BigRepoAccess::Relay)?;
        let batch4 = recv_batch(&mut rx).await;
        assert!(matches!(
            batch4.as_slice(),
            [BigRepoDomainNotification::DocumentAccessChanged {
                doc_id: d, member_id: m, access: a, ..
            }] if *d == doc_id && *m == member_id && *a == BigRepoAccess::Relay
        ));

        manager.notify_document_key_rotated(doc_id)?;
        let batch5 = recv_batch(&mut rx).await;
        assert!(matches!(
            batch5.as_slice(),
            [BigRepoDomainNotification::DocumentKeyRotated {
                doc_id: d, ..
            }] if *d == doc_id
        ));
        Ok(())
    }

    #[tokio::test]
    async fn domain_listener_drop_unregisters() -> Res<()> {
        let (manager, _stop) = ChangeListenerManager::boot();
        let (registration, mut rx) = manager.subscribe_domain_listener(DomainFilter).await?;

        drop(registration);
        manager.notify_document_key_rotated(DocumentId::random())?;
        let closed = timeout(Duration::from_millis(250), rx.recv())
            .await
            .expect("expected receiver to resolve")
            .is_none();
        assert!(closed, "domain listener should be removed on drop");
        Ok(())
    }
}
