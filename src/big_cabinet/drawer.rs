//! DrawerService: drawers as document collections (ADR 014, FDR 001).
//!
//! - `<op>.rs`: one file per operation — its wire types, its implementation, and its tests.
//! - [`access`]: the [`DocAccess`] policy DocsRepo is booted with when drawers are in play — "may
//!   this actor act on doc X" is answered by which drawers hold X and what the actor holds on them.
//! - `state`: the local projection — drawers this node knows, their rosters, child links and the
//!   two local drawers — in SQLite, fed by this service's own writes and by boot rescans.
//! - `ledger`: the durable op ledger a roster-and-Keyhive operation steps through, so a crash
//!   between the two halves resumes rather than leaving a grant without an entry or vice versa.
//!
//! A `DrawerId` is the `DocId` of the doc carrying `org.example.daybook.drawer/main`. The roster
//! (facets on that doc, written through DocsRepo) is what makes inclusion official; the Keyhive
//! groups the descriptor names are what make it effective. The two are deliberately not atomic:
//! adding a doc grants the docs group first and publishes the entry second, removing deletes the
//! entry first and revokes second, so an interrupted operation is visible as "listed but not yet
//! available" or "available but no longer listed" and is finished by the ledger replay at boot.
//!
//! The drawer facets are ordinary signed facets, so an actor holding Write on a drawer doc could
//! publish an entry directly through DocsRepo. That is not a bypass: an entry grants nothing by
//! itself (the Keyhive grant is what the node enforces), it is exactly the "official but
//! unavailable" state ADR 014 names, and the roster's authority is the drawer doc's write
//! authority — which is what that actor held.

pub mod access;
mod add_child;
mod add_doc;
mod create;
mod get;
mod grant;
mod known;
mod ledger;
mod list;
mod list_docs;
mod principal;
mod remove_child;
mod remove_doc;
mod revoke;
mod state;

#[cfg(test)]
mod e2e;

pub use self::access::DrawerAccess;
pub use self::add_child::AddChild;
pub use self::add_doc::AddDoc;
pub use self::create::{CreateDrawer, CreateLayout, DrawerCreated};
pub use self::get::{DrawerView, GetDrawer};
pub use self::grant::GrantDrawer;
pub use self::known::{
    ForgetDrawer, KnownDrawer, ListKnownDrawers, RememberDrawer, SetDrawerPolicy,
};
pub use self::list::{ListDrawers, ListedDrawer};
pub use self::list_docs::{ListDocs, ListedDoc};
pub use self::principal::{InvitePrincipal, Principal, RevokePrincipal};
pub use self::remove_child::RemoveChild;
pub use self::remove_doc::RemoveDoc;
pub use self::revoke::RevokeDrawer;
pub use self::state::{DrawerRecord, LocalDrawers};
pub use big_cabinet_core::drawer::{Drawer, DrawerLayout, LayoutKind};

use self::ledger::Ledger;
use self::state::State;
use crate::authority::{
    Actor, AuthorityProtocol, AuthorityServiceError, CapId, Capability, CheckGrant, GrantAccess,
    MintCapability, ResolveActor, RevokeAccess, Rights,
};
use crate::doc::{CommandId, DocsError, DocsProtocol, WriteMode, WriteReceipt};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{DocId, Uuid7};
use big_cabinet_core::drawer::{
    DRAWER_CHILD_ENTRY_FACET_TAG, DRAWER_DOC_ENTRY_FACET_TAG, DRAWER_FACET_TAG,
    DRAWER_KNOWN_ENTRY_FACET_TAG, DRAWER_ROSTER_FACET_TAG, DrawerAccess as Access, DrawerGroups,
    GroupKey,
};
use big_repo::keyhive_core::access::Access as KeyhiveAccess;
use big_repo::{BigKeyhiveGroup, SharedBigRepo};
use serde::{Deserialize, Serialize};
use service_utils_rs::{
    Client, ClientError, ContextError, Envelope, Handler, Host, Operation, RequestContext,
    ResponseEnvelope, StopToken, XrpcOperation, service_protocol,
};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

pub type DrawerId = DocId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DrawersError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("document repository failed: {0}")]
    Docs(DocsError),
    #[error("document repository transport failed: {0}")]
    DocsUnavailable(String),
    #[error("keyhive failed: {0}")]
    Keyhive(String),
    #[error("drawer state failed: {0}")]
    Storage(String),
    #[error("{0} is not a drawer this node knows")]
    UnknownDrawer(DrawerId),
    #[error("{0} is a self-doc drawer: it holds only itself and cannot take entries or members")]
    SelfDoc(DrawerId),
    #[error("{0} is already a drawer")]
    AlreadyDrawer(DocId),
    #[error("a drawer cannot be its own child")]
    SelfChild,
    #[error("{0} is not an entry of this drawer")]
    NotAnEntry(DocId),
    #[error("the drawer's descriptor states a layout this build does not know: {0}")]
    ForeignLayout(String),
    #[error("command {command} is still in flight; retry once it resolves")]
    CommandInFlight { command: CommandId },
    #[error("command {command} was already used for a different request")]
    CommandReused { command: CommandId },
    #[error("the drawer's roster could not be read: {0}")]
    RosterUnreadable(String),
}

impl DrawersError {
    /// Whether a failed op may have left a side effect behind, so its ledger row stays for a
    /// retry or the next boot to finish; a refusal left nothing and frees its command id.
    pub(crate) fn keeps_ledger_row(&self) -> bool {
        match self {
            Self::Context(_)
            | Self::AuthorityUnavailable(_)
            | Self::DocsUnavailable(_)
            | Self::Keyhive(_)
            | Self::Storage(_)
            | Self::RosterUnreadable(_) => true,
            Self::Docs(error) => matches!(
                error,
                DocsError::Context(_)
                    | DocsError::AuthorityUnavailable(_)
                    | DocsError::Access(_)
                    | DocsError::Repository(_)
                    | DocsError::Pending { .. }
                    | DocsError::CommandInFlight { .. }
            ),
            Self::Denied
            | Self::Authority(_)
            | Self::UnknownDrawer(_)
            | Self::ForeignLayout(_)
            | Self::SelfDoc(_)
            | Self::AlreadyDrawer(_)
            | Self::SelfChild
            | Self::NotAnEntry(_)
            | Self::CommandInFlight { .. }
            | Self::CommandReused { .. } => false,
        }
    }
}

/// Everything DrawerService is booted with.
pub struct DrawerConfig {
    pub big_repo: SharedBigRepo,
    pub authority: Client<AuthorityProtocol>,
    /// DrawerService's own actor capability: it names the objects it mints and checks, and it is
    /// the actor every roster write and every drawer doc creation goes through DocsRepo as.
    pub issuer: CapId,
    pub docs: Client<DocsProtocol>,
    /// The policy DocsRepo was booted with. It was built from the same `sql` before DocsRepo, so
    /// the two services share one projection; boot hands it the docs client it was missing.
    pub access: Arc<DrawerAccess>,
}

/// The service: one per node, shared with [`DrawerAccess`] through [`Core`].
pub struct DrawerService {
    core: Arc<Core>,
}

/// What both the hosted service and the [`DocAccess`] policy need. The docs client is set once
/// DocsRepo exists, which is after the policy was built and before any request reaches either.
pub(crate) struct Core {
    pub(crate) big_repo: SharedBigRepo,
    pub(crate) authority: Client<AuthorityProtocol>,
    pub(crate) issuer: CapId,
    pub(crate) actor: OnceLock<Actor>,
    pub(crate) docs: OnceLock<Client<DocsProtocol>>,
    pub(crate) state: State,
    pub(crate) ledger: Ledger,
}

/// The booted service.
pub struct Drawers {
    pub host: Arc<Host<DrawersHandler>>,
    pub client: Client<DrawersProtocol>,
    core: Arc<Core>,
}

impl Drawers {
    /// The two drawers every doc on this node is entered into (ADR 014 "required local drawers").
    pub fn local(&self) -> LocalDrawers {
        self.core.state.local()
    }

    /// Mint an object capability for `drawer_id` at `access` with `rights` in DrawerService's
    /// namespace after verifying that `capability` resolves to an actor holding at least `access`
    /// on `drawer_id` (ADR 017).
    pub async fn mint_drawer_capability(
        &self,
        context: &RequestContext,
        capability: CapId,
        drawer_id: &DrawerId,
        access: Access,
        rights: Rights,
    ) -> Result<Capability, DrawersError> {
        let actor = self.core.resolve(context, capability).await?;
        self.core
            .authorize(context, &actor, drawer_id, access)
            .await?;
        self.core
            .authority
            .call(
                context.child(),
                MintCapability {
                    issuer: self.core.issuer,
                    object: drawer_object(access, drawer_id),
                    rights,
                },
            )
            .await
            .map_err(authority_error)
    }
}

pub type DrawersHandler = Arc<DrawerService>;

service_protocol! {
    pub enum DrawersProtocol => DrawersMessage {
        unary {
            CreateDrawer(CreateDrawer),
            AddDoc(AddDoc),
            RemoveDoc(RemoveDoc),
            AddChild(AddChild),
            RemoveChild(RemoveChild),
            GetDrawer(GetDrawer),
            ListDocs(ListDocs),
            RememberDrawer(RememberDrawer),
            ForgetDrawer(ForgetDrawer),
            ListKnownDrawers(ListKnownDrawers),
            SetDrawerPolicy(SetDrawerPolicy),
            ListDrawers(ListDrawers),
            GrantDrawer(GrantDrawer),
            RevokeDrawer(RevokeDrawer),
            InvitePrincipal(InvitePrincipal),
            RevokePrincipal(RevokePrincipal)
        }
        stream {}
    }
}

/// The object a standing grant on a drawer names in DrawerService's ledger namespace: the access
/// level is part of the string (`write drawer <id>`), the rights are ADR 017's generic `use`.
/// Levels nest — `admin` covers `write` covers `read` covers `relay` — so a check asks for the
/// strongest level the subject holds and compares.
pub fn drawer_object(access: Access, drawer: &DrawerId) -> String {
    let verb = match access {
        Access::Relay => "relay",
        Access::Read => "read",
        Access::Write => "write",
        Access::Admin => "admin",
    };
    format!("{verb} drawer {drawer}")
}

/// The object an actor needs `use` on to create a collection drawer.
pub const CREATE_DRAWER_OBJECT: &str = "create drawer";

/// The `placement` a doc names to be created into a drawer: `drawer <id>`. DocsRepo does not read
/// it; [`DrawerAccess`] does.
pub fn drawer_placement(drawer: &DrawerId) -> String {
    format!("drawer {drawer}")
}

/// The `placement` DrawerService itself uses for a drawer's own metadata doc: it enters the local
/// drawers like every doc on this node, and nothing else.
pub(crate) const METADATA_PLACEMENT: &str = "drawer metadata";

pub(crate) fn parse_placement(placement: &str) -> Option<DrawerId> {
    placement.strip_prefix("drawer ")?.parse().ok()
}

pub(crate) fn drawer_key() -> FacetKey {
    FacetKey::from_text(DRAWER_FACET_TAG, "main").expect("the drawer key is well formed")
}

pub(crate) fn known_entry_key(drawer_id: &DrawerId) -> FacetKey {
    FacetKey::from_text(DRAWER_KNOWN_ENTRY_FACET_TAG, drawer_id.to_string())
        .expect("a known entry key is well formed")
}

pub(crate) fn roster_key() -> FacetKey {
    FacetKey::from_text(DRAWER_ROSTER_FACET_TAG, "main").expect("the roster key is well formed")
}

pub(crate) fn doc_entry_key(doc: &DocId) -> FacetKey {
    FacetKey::from_text(DRAWER_DOC_ENTRY_FACET_TAG, doc.to_string())
        .expect("a doc entry key is well formed")
}

pub(crate) fn child_entry_key(child: &DrawerId) -> FacetKey {
    FacetKey::from_text(DRAWER_CHILD_ENTRY_FACET_TAG, child.to_string())
        .expect("a child entry key is well formed")
}

/// The target of an entry key: `docEntry/<id>` or `childEntry/<id>` names it in the key id.
pub(crate) fn entry_target(key: &FacetKey) -> Option<DocId> {
    std::str::from_utf8(key.id_bytes()).ok()?.parse().ok()
}

pub(crate) fn keyhive_access(access: Access) -> KeyhiveAccess {
    match access {
        Access::Relay => KeyhiveAccess::Relay,
        Access::Read => KeyhiveAccess::Read,
        Access::Write => KeyhiveAccess::Edit,
        Access::Admin => KeyhiveAccess::Admin,
    }
}

pub(crate) fn group_key(group: &BigKeyhiveGroup) -> GroupKey {
    GroupKey(group.id().to_bytes())
}

pub(crate) fn authority_error(error: ClientError<AuthorityServiceError>) -> DrawersError {
    match error {
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DrawersError::Denied,
        ClientError::Operation(error) => DrawersError::Authority(error),
        ClientError::Transport { message, .. } => DrawersError::AuthorityUnavailable(message),
    }
}

pub(crate) fn docs_error(error: ClientError<DocsError>) -> DrawersError {
    match error {
        ClientError::Operation(DocsError::Context(error)) => error.into(),
        ClientError::Operation(error) => DrawersError::Docs(error),
        ClientError::Transport { message, .. } => DrawersError::DocsUnavailable(message),
    }
}

pub(crate) fn keyhive_error(error: impl std::fmt::Display) -> DrawersError {
    DrawersError::Keyhive(error.to_string())
}

impl Core {
    pub(crate) fn docs(&self) -> &Client<DocsProtocol> {
        self.docs
            .get()
            .expect("DrawerService is booted before any request reaches drawers or their policy")
    }

    /// DrawerService's own actor, resolved at boot.
    pub(crate) fn actor(&self) -> &Actor {
        self.actor
            .get()
            .expect("DrawerService is booted before any request reaches drawers or their policy")
    }

    pub(crate) async fn resolve(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Actor, DrawersError> {
        self.authority
            .call(context.child(), ResolveActor { capability })
            .await
            .map_err(authority_error)
    }

    /// The strongest level `actor` holds on `drawer` directly in this service's ledger namespace.
    /// Four objects, checked strongest first; the ledger answers only standing grants.
    pub(crate) async fn held(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        drawer: &DrawerId,
    ) -> Result<Option<Access>, DrawersError> {
        for access in Access::ALL.into_iter().rev() {
            let rights = self
                .authority
                .call(
                    context.child(),
                    CheckGrant {
                        via: self.issuer,
                        subject: actor,
                        object: drawer_object(access, drawer),
                    },
                )
                .await
                .map_err(authority_error)?;
            if rights.contains(Rights::USE) {
                return Ok(Some(access));
            }
        }
        Ok(None)
    }

    /// Whether `actor` may act on `drawer` at `needed`, by a direct grant or through a parent
    /// drawer whose child entry reaches it: the effective level along a path is the weakest link,
    /// and the strongest path wins (ADR 014 child traversal).
    pub(crate) async fn effective(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        drawer: &DrawerId,
    ) -> Result<Option<Access>, DrawersError> {
        let mut best = self.held(context, actor, drawer).await?;
        if best == Some(Access::Admin) {
            return Ok(best);
        }
        if self.state.parents_of(drawer).await?.is_empty() {
            self.sync_discovered(context).await?;
        }
        // `bound` is the strongest level a grant on that drawer can confer on `drawer` through the
        // entries walked so far. A drawer is re-expanded when a stronger path reaches it, so the
        // strongest path wins rather than the first one found.
        let mut bounds: BTreeMap<DrawerId, Access> =
            BTreeMap::from([(drawer.clone(), Access::Admin)]);
        let mut frontier = vec![(drawer.clone(), Access::Admin)];
        while let Some((child, bound)) = frontier.pop() {
            for (parent, entry) in self.state.parents_of(&child).await? {
                let via = entry.min(bound);
                if bounds.get(&parent).is_some_and(|known| *known >= via) {
                    continue;
                }
                bounds.insert(parent.clone(), via);
                if let Some(held) = self.held(context, actor, &parent).await? {
                    let reached = held.min(via);
                    if best.is_none_or(|best| reached > best) {
                        best = Some(reached);
                    }
                }
                frontier.push((parent, via));
            }
        }
        Ok(best)
    }

    /// The strongest level `actor` reaches on `doc`: through the doc being a drawer itself, or
    /// through any drawer listing it, where an entry caps what the drawer confers on the doc.
    /// This is the doc-access policy's answer, so a caller checked here is checked as DocsRepo
    /// checks it.
    pub(crate) async fn reach(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        doc: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        let mut best = self.reach_projected(context, actor, doc).await?;
        if best != Some(Access::Admin) {
            self.sync_discovered(context).await?;
            if self.state.drawer(doc).await?.is_none() {
                drop(self.ensure_drawer(context, doc).await?);
            }
            best = self.reach_projected(context, actor, doc).await?;
        }
        Ok(best)
    }

    async fn reach_projected(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        doc: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        let mut best = None;
        if self.state.drawer(doc).await?.is_some() {
            best = self.effective(context, actor, doc).await?;
        }
        for (drawer, entry) in self.state.drawers_of(doc).await? {
            if best == Some(Access::Admin) {
                break;
            }
            if let Some(held) = self.effective(context, actor, &drawer).await? {
                let reached = held.min(entry);
                if best.is_none_or(|best| reached > best) {
                    best = Some(reached);
                }
            }
        }
        Ok(best)
    }

    pub(crate) async fn authorize(
        &self,
        context: &RequestContext,
        actor: &Actor,
        drawer: &DrawerId,
        needed: Access,
    ) -> Result<(), DrawersError> {
        match self.effective(context, actor.uuid, drawer).await? {
            Some(held) if held >= needed => Ok(()),
            _ => Err(DrawersError::Denied),
        }
    }

    pub(crate) async fn grant(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
    ) -> Result<(), DrawersError> {
        self.authority
            .call(
                context.child(),
                GrantAccess {
                    via: self.issuer,
                    subject,
                    object,
                    rights: Rights::USE,
                },
            )
            .await
            .map_err(authority_error)?;
        self.state.notify_changed();
        Ok(())
    }

    pub(crate) async fn revoke(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
    ) -> Result<(), DrawersError> {
        self.authority
            .call(
                context.child(),
                RevokeAccess {
                    via: self.issuer,
                    subject,
                    object,
                },
            )
            .await
            .map_err(authority_error)?;
        self.state.notify_changed();
        Ok(())
    }

    /// Write facets on a drawer doc as this service, at current heads. Roster writes are
    /// last-writer-wins by construction: two nodes publishing the same entry converge on one
    /// value, and the entry's presence is what matters.
    pub(crate) async fn write_roster(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
        command: Option<CommandId>,
        set: BTreeMap<FacetKey, serde_json::Value>,
        remove: std::collections::BTreeSet<FacetKey>,
    ) -> Result<WriteReceipt, DrawersError> {
        self.docs()
            .call(
                context.child(),
                crate::doc::WriteFacets {
                    capability: self.issuer,
                    command,
                    doc_id: drawer.clone(),
                    mode: WriteMode::AtCurrent,
                    set,
                    remove,
                },
            )
            .await
            .map_err(docs_error)
    }

    /// Bring the projection of `drawer` back in line with its doc: the roster facets, read as
    /// this service, replace the rows. A self-doc drawer has no roster and nothing to rescan.
    /// This is how a change another node made to a shared roster becomes visible here; the
    /// service's own writes update the rows directly.
    pub(crate) async fn rescan(
        &self,
        context: &RequestContext,
        record: &DrawerRecord,
    ) -> Result<(DrawerRecord, big_cabinet_core::drawer::Drawer), DrawersError> {
        let read = self
            .docs()
            .call(
                context.child(),
                crate::doc::ReadFacets {
                    capability: self.issuer,
                    doc_id: record.drawer_id.clone(),
                    at: crate::doc::At::Current,
                    facets: None,
                    optimistic: false,
                },
            )
            .await
            .map_err(docs_error)?;
        // The descriptor is re-read with the entries: a layout edited on another node — into
        // another known layout, or into one this build does not know — replaces the stored rows
        // with what the facet now states.
        let descriptor_value = match read.facets.get(&drawer_key()) {
            Some(crate::doc::FacetRead::Accepted { value, .. }) => value.clone(),
            Some(crate::doc::FacetRead::Absent { .. }) => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{} carries no drawer descriptor",
                    record.drawer_id
                )));
            }
            Some(other) => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{} descriptor: {other:?}",
                    record.drawer_id
                )));
            }
            None => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{} carries no drawer descriptor",
                    record.drawer_id
                )));
            }
        };
        let descriptor: big_cabinet_core::drawer::Drawer = serde_json::from_value(descriptor_value)
            .map_err(|error| {
                DrawersError::RosterUnreadable(format!("{} descriptor: {error}", record.drawer_id))
            })?;
        let record = crate::drawer::create::record_of(&record.drawer_id, &descriptor);
        let mut docs = BTreeMap::new();
        let mut children = BTreeMap::new();
        for (key, facet) in read.facets {
            let tag = key.tag().as_str();
            if tag != DRAWER_DOC_ENTRY_FACET_TAG && tag != DRAWER_CHILD_ENTRY_FACET_TAG {
                continue;
            }
            let value = match facet {
                crate::doc::FacetRead::Accepted { value, .. } => value,
                crate::doc::FacetRead::Absent { .. } => continue,
                other => {
                    return Err(DrawersError::RosterUnreadable(format!(
                        "{key} on {}: {other:?}",
                        record.drawer_id
                    )));
                }
            };
            let Some(target) = entry_target(&key) else {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{key} on {} does not name a doc",
                    record.drawer_id
                )));
            };
            let unreadable = |error: serde_json::Error| {
                DrawersError::RosterUnreadable(format!("{key} on {}: {error}", record.drawer_id))
            };
            if tag == DRAWER_DOC_ENTRY_FACET_TAG {
                let entry: big_cabinet_core::drawer::DrawerDocEntry =
                    serde_json::from_value(value).map_err(unreadable)?;
                if let Some(access) = entry.access.known() {
                    docs.insert(target, access);
                }
            } else {
                let entry: big_cabinet_core::drawer::DrawerChildEntry =
                    serde_json::from_value(value).map_err(unreadable)?;
                if let Some(access) = entry.access.known() {
                    children.insert(target, access);
                }
            }
        }
        match record.layout_kind() {
            Some(LayoutKind::SingleGroup) => {
                self.state
                    .replace_roster(&record.drawer_id, &docs, &children)
                    .await?;
            }
            Some(LayoutKind::SelfDoc) => {
                // A self-doc drawer has no roster; anything the descriptor once stated is stale.
                self.state
                    .replace_roster(&record.drawer_id, &BTreeMap::new(), &BTreeMap::new())
                    .await?;
            }
            None => {
                // A foreign layout is carried, not operated: its roster projection is left
                // exactly as it was, because this build does not know where its entries live.
            }
        }
        self.state
            .put_drawer(&record.drawer_id, &record.descriptor)
            .await?;
        Ok((record, descriptor))
    }

    /// The descriptor facet as the doc carries it, read as this service at current heads.
    pub(crate) async fn descriptor(
        &self,
        context: &RequestContext,
        drawer_id: &DrawerId,
    ) -> Result<big_cabinet_core::drawer::Drawer, DrawersError> {
        let read = self
            .docs()
            .call(
                context.child(),
                crate::doc::ReadFacets {
                    capability: self.issuer,
                    doc_id: drawer_id.clone(),
                    at: crate::doc::At::Current,
                    facets: Some(std::collections::BTreeSet::from([drawer_key()])),
                    optimistic: false,
                },
            )
            .await
            .map_err(docs_error)?;
        let value = match read.facets.into_iter().next() {
            Some((_, crate::doc::FacetRead::Accepted { value, .. })) => value,
            _ => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{drawer_id} carries no drawer descriptor"
                )));
            }
        };
        serde_json::from_value(value).map_err(|error| {
            DrawersError::RosterUnreadable(format!("{drawer_id} descriptor: {error}"))
        })
    }

    /// Look up `drawer_id` in the local projection, or if it is not yet projected but its doc is
    /// locally available with an accepted `org.example.daybook.drawer/main` facet, project its
    /// descriptor and roster now.
    pub(crate) async fn ensure_drawer(
        &self,
        context: &RequestContext,
        drawer_id: &DrawerId,
    ) -> Result<Option<DrawerRecord>, DrawersError> {
        if let Some(record) = self.state.drawer(drawer_id).await? {
            return Ok(Some(record));
        }
        self.discover_drawer(context, drawer_id).await
    }

    pub(crate) async fn discover_drawer(
        &self,
        context: &RequestContext,
        root_id: &DrawerId,
    ) -> Result<Option<DrawerRecord>, DrawersError> {
        let mut queue = std::collections::VecDeque::from([root_id.clone()]);
        let mut visited = std::collections::BTreeSet::from([root_id.clone()]);
        let mut root_record = None;
        while let Some(drawer_id) = queue.pop_front() {
            let read = match self
                .docs()
                .call(
                    context.child(),
                    crate::doc::ReadFacets {
                        capability: self.issuer,
                        doc_id: drawer_id.clone(),
                        at: crate::doc::At::Current,
                        facets: Some(std::collections::BTreeSet::from([drawer_key()])),
                        optimistic: false,
                    },
                )
                .await
            {
                Ok(read) => read,
                Err(ClientError::Operation(
                    DocsError::DocNotFound | DocsError::Pending { .. },
                )) => continue,
                Err(error) => return Err(docs_error(error)),
            };
            let Some(crate::doc::FacetRead::Accepted { value, .. }) =
                read.facets.get(&drawer_key())
            else {
                continue;
            };
            let Ok(descriptor) =
                serde_json::from_value::<big_cabinet_core::drawer::Drawer>(value.clone())
            else {
                continue;
            };
            let initial = crate::drawer::create::record_of(&drawer_id, &descriptor);
            let (record, _) = self.rescan(context, &initial).await?;
            for (child_id, _) in self.state.children_of(&drawer_id).await? {
                if visited.insert(child_id.clone())
                    && self.state.drawer(&child_id).await?.is_none()
                {
                    queue.push_back(child_id);
                }
            }
            if &drawer_id == root_id {
                root_record = Some(record);
            }
        }
        Ok(root_record)
    }

    /// Project any drawer docs that arrived via sync (referenced by child entries or `facet_set`)
    /// into the local drawer projection without modifying the registry.
    pub(crate) async fn sync_discovered(
        &self,
        context: &RequestContext,
    ) -> Result<(), DrawersError> {
        for candidate in self.state.unprojected_candidates().await? {
            drop(self.discover_drawer(context, &candidate).await?);
        }
        Ok(())
    }

    /// A known single-group drawer's groups, or why it cannot take entries.
    pub(crate) async fn groups(
        &self,
        drawer: &DrawerId,
    ) -> Result<(BigKeyhiveGroup, BigKeyhiveGroup), DrawersError> {
        let record = self
            .ensure_drawer(&RequestContext::root(), drawer)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(drawer.clone()))?;
        let DrawerGroups {
            meta_group,
            docs_group,
        } = record.groups().ok_or_else(|| match record.layout_kind() {
            None => DrawersError::ForeignLayout(
                record
                    .descriptor
                    .layout
                    .tag()
                    .unwrap_or("unknown")
                    .to_owned(),
            ),
            Some(_) => DrawersError::SelfDoc(drawer.clone()),
        })?;
        let group = |key: GroupKey| async move {
            self.big_repo
                .get_group_by_id(key.0)
                .await
                .ok_or_else(|| DrawersError::Keyhive(format!("group {key} is not in keyhive")))
        };
        Ok((group(meta_group).await?, group(docs_group).await?))
    }
}

impl DrawerService {
    /// Finish every operation a crash left between its two halves, from the step it reached.
    /// Replay runs as the service itself: the caller was authorized when the row was begun.
    async fn recover(&self) -> Result<(), DrawersError> {
        let context = RequestContext::root();
        for row in self.core.ledger.unfinished().await? {
            tracing::info!(command = %row.command, op = ?row.op, step = ?row.step, "resuming drawer op");
            match &row.op {
                ledger::Op::AddDoc { .. } => self.core.resume_add_doc(&context, &row).await?,
                ledger::Op::RemoveDoc { .. } => self.core.resume_remove_doc(&context, &row).await?,
                ledger::Op::AddChild { .. } => self.core.resume_add_child(&context, &row).await?,
                ledger::Op::RemoveChild { .. } => {
                    self.core.resume_remove_child(&context, &row).await?
                }
                ledger::Op::Create { .. } => self.core.resume_create(&context, &row).await?,
                ledger::Op::CreateSelfDoc { .. } => {
                    self.core.resume_create_self_doc(&context, &row).await?
                }
                ledger::Op::Invite { .. } | ledger::Op::Revoke { .. } => {
                    self.core.resume_membership(&context, &row).await?
                }
            }
        }
        Ok(())
    }
}

/// Boot DrawerService: resolve its actor, hand the policy its docs client, make sure the two local
/// drawers exist, settle the op ledger, then host the service.
pub async fn boot(config: DrawerConfig) -> Result<(Drawers, StopToken), DrawersError> {
    let core = Arc::clone(&config.access.core);
    assert!(
        Arc::ptr_eq(&core.big_repo, &config.big_repo),
        "DrawerService and its DocAccess policy are built over the same BigRepo"
    );
    assert_eq!(
        core.issuer, config.issuer,
        "DrawerService and its DocAccess policy are the same actor"
    );
    let actor = core
        .authority
        .call(
            RequestContext::root(),
            ResolveActor {
                capability: config.issuer,
            },
        )
        .await
        .map_err(authority_error)?;
    drop(core.actor.set(actor));
    drop(core.docs.set(config.docs));
    let service = Arc::new(DrawerService { core });
    service.recover().await?;
    for record in service.core.state.drawers().await? {
        service
            .core
            .rescan(&RequestContext::root(), &record)
            .await?;
    }
    service.ensure_local_drawers().await?;
    service.core.sync_discovered(&RequestContext::root()).await?;
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::direct(Arc::clone(&host));
    Ok((
        Drawers {
            host,
            client,
            core: Arc::clone(&service.core),
        },
        host_stop,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_round_trips_through_its_text() {
        let drawer: DrawerId = DocId::new([3; 32]);
        assert_eq!(parse_placement(&drawer_placement(&drawer)), Some(drawer));
        assert_eq!(parse_placement(METADATA_PLACEMENT), None);
        assert_eq!(parse_placement("drawer not-a-key"), None);
    }

    #[test]
    fn entry_keys_name_their_target() {
        let doc = DocId::new([9; 32]);
        assert_eq!(entry_target(&doc_entry_key(&doc)), Some(doc.clone()));
        assert_eq!(entry_target(&child_entry_key(&doc)), Some(doc));
        assert_eq!(entry_target(&drawer_key()), None);
    }

    #[test]
    fn objects_spell_the_level() {
        let drawer: DrawerId = DocId::new([1; 32]);
        assert_eq!(
            drawer_object(Access::Write, &drawer),
            format!("write drawer {drawer}")
        );
        assert_eq!(keyhive_access(Access::Relay), KeyhiveAccess::Relay);
        assert_eq!(keyhive_access(Access::Admin), KeyhiveAccess::Admin);
    }
}
