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
mod sqlite {
    include!("drawer/sqlite/gen.rs");
}

#[cfg(test)]
mod e2e;

pub use self::access::DrawerAccess;
pub use self::access::DrawerProvider;
pub use self::access::DrawerWorker;
pub use big_cabinet_core::drawer::Drawer;
pub use big_cabinet_core::drawer::DrawerLayout;
pub use big_cabinet_core::drawer::LayoutKind;

use self::ledger::Ledger;
use self::state::State;
use crate::authority::Actor;
use crate::authority::AuthorityServiceError;
use crate::authority::Authorization;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::DomainAuthority;
use crate::authority::Namespace;
use crate::authority::Rights;
use crate::capability::DomainCapabilities;
use crate::capability::Invocation;
use crate::doc::CommandId;
use crate::doc::DocScope;
use crate::doc::DocsError;
use crate::doc::DocsProtocol;
use crate::doc::MintDocCapability;
use crate::doc::WriteMode;
use crate::doc::WriteReceipt;
use crate::grants::GrantError;
use crate::grants::Grants;
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::DRAWER_CHILD_ENTRY_FACET_TAG;
use big_cabinet_core::drawer::DRAWER_DOC_ENTRY_FACET_TAG;
use big_cabinet_core::drawer::DRAWER_FACET_TAG;
use big_cabinet_core::drawer::DRAWER_KNOWN_ENTRY_FACET_TAG;
use big_cabinet_core::drawer::DRAWER_ROSTER_FACET_TAG;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::DrawerGroups;
use big_cabinet_core::drawer::GroupKey;
use big_repo::BigKeyhiveGroup;
use big_repo::SharedBigRepo;
use big_repo::keyhive_core::access::Access as KeyhiveAccess;
use serde::Serialize;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use std::collections::BTreeMap;
use std::sync::Arc;

pub use big_cabinet_core::drawer::protocol::*;

trait LedgerFailure {
    fn keeps_ledger_row(&self) -> bool;
}

impl LedgerFailure for DrawersError {
    /// Whether a failed op may have left a side effect behind, so its ledger row stays for a
    /// retry or the next boot to finish; a refusal left nothing and frees its command id.
    fn keeps_ledger_row(&self) -> bool {
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

pub struct DrawerConfig<'a> {
    pub worker: &'a DrawerWorker,
}

pub struct DrawerService {
    core: Arc<Core>,
}

pub(crate) struct Projection {
    pub(crate) big_repo: SharedBigRepo,
    pub(crate) issuer: CapId,
    pub(crate) namespace: Namespace,
    pub(crate) capabilities: DomainCapabilities,
    pub(crate) grants: Arc<Grants>,
    pub(crate) actor: Actor,
    pub(crate) state: State,
    pub(crate) facet_set: Arc<crate::index::FacetSetStore>,
    pub(crate) ledger: Ledger,
    pub(crate) commands: tokio::sync::mpsc::Sender<access::PolicyCommand>,
}

pub(crate) struct Core {
    pub(crate) projection: Arc<Projection>,
    pub(crate) docs: Client<DocsProtocol>,
    #[cfg(test)]
    pub(crate) placement_gate: std::sync::Mutex<Option<access::PlacementGate>>,
    /// An armed crash: the next operation that reaches the point fails there, as a process
    /// dying at that boundary would, leaving its ledger row for the next boot.
    #[cfg(test)]
    pub(crate) crash: std::sync::Mutex<Option<CrashPoint>>,
}

/// The boundaries inside a roster operation where a crash leaves one half done.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrashPoint {
    /// An entry's Keyhive grant landed; the roster never said so.
    DocGranted,
    /// An entry's roster write landed; the projection row never followed.
    RosterWritten,
    /// A removal's roster write landed; the Keyhive grant was never revoked.
    RosterRemoved,
    /// An invite's docs membership landed; the meta membership never did.
    DocsMembershipAdded,
}

impl Core {
    /// Fail here if a crash is armed at `point`, consuming it.
    #[cfg(test)]
    pub(crate) fn crash_at(&self, point: CrashPoint) -> Result<(), DrawersError> {
        let mut armed = self.crash.lock().unwrap();
        if *armed == Some(point) {
            armed.take();
            return Err(DrawersError::Storage(format!(
                "injected crash at {point:?}"
            )));
        }
        Ok(())
    }
}

impl std::ops::Deref for Core {
    type Target = Projection;

    fn deref(&self) -> &Self::Target {
        &self.projection
    }
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
        authorization: Authorization,
        drawer_id: &DrawerId,
        access: Access,
        rights: Rights,
    ) -> Result<Capability, DrawersError> {
        self.client
            .call(
                context.child(),
                MintDrawerCapability {
                    authorization,
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), access)]),
                        create: false,
                    },
                    rights,
                },
            )
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => {
                    DrawersError::AuthorityUnavailable(message)
                }
            })
    }
}

impl Handler<MintDrawerCapability> for DrawerService {
    async fn call(
        &self,
        request: Envelope<MintDrawerCapability>,
    ) -> Result<ResponseEnvelope<Capability>, DrawersError> {
        let params = request.params;
        let caller = self
            .core
            .capabilities
            .source::<DrawerScope>(&request.context, params.authorization)
            .await
            .map_err(authority_error)?;
        if !caller.rights.contains(Rights::NARROW) || !params.rights.is_subset_of(caller.rights) {
            return Err(DrawersError::Denied);
        }
        let mut dependencies = Vec::new();
        for (drawer, access) in &params.scope.drawers {
            match &caller.authority {
                DomainAuthority::Identity(actor) => {
                    let (held, proof) = self
                        .core
                        .effective(&request.context, *actor, drawer)
                        .await?
                        .ok_or(DrawersError::Denied)?;
                    if held < *access {
                        return Err(DrawersError::Denied);
                    }
                    dependencies.extend(proof);
                }
                DomainAuthority::Scoped(_) => {
                    self.core
                        .authorize(&request.context, &caller.authority, drawer, *access)
                        .await?;
                }
            }
        }
        if params.scope.create {
            dependencies.extend(
                self.core
                    .authorize_create(&request.context, &caller.authority)
                    .await?,
            );
        }
        Ok(self
            .core
            .capabilities
            .mint(
                &request.context,
                params.authorization.authority,
                &caller,
                params.scope,
                params.rights,
                dependencies,
            )
            .await
            .map_err(authority_error)?
            .into())
    }
}

pub type DrawersHandler = Arc<DrawerService>;

service_utils_rs::bind_service_protocol! {
    pub struct DrawersBinding for DrawersProtocol => DrawersMessage {
        unary {
            MintDrawerCapability(MintDrawerCapability),
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

pub(crate) fn grant_error(error: GrantError) -> DrawersError {
    match error {
        GrantError::Storage(error) => DrawersError::Storage(error.to_string()),
        GrantError::Authority(error) => authority_error(error),
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

impl Projection {
    pub(crate) fn actor(&self) -> &Actor {
        &self.actor
    }
}

impl Core {
    pub(crate) fn docs(&self) -> &Client<DocsProtocol> {
        &self.docs
    }

    pub(crate) async fn invocation(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Invocation<DrawerScope>, DrawersError> {
        self.capabilities
            .resolve(context, capability)
            .await
            .map_err(authority_error)
    }

    /// Run `request` against the docs service under a capability narrowed to `scope` and
    /// signed as `signer`, minted for this call and retracted once it returns. The drawer
    /// service holds doc authority over every drawer it projects, so a handle per request
    /// is how it narrows that authority; retracting it keeps the authority's handle table
    /// from growing with every roster read.
    pub(crate) async fn as_signer<T, F>(
        &self,
        context: &RequestContext,
        signer: CapId,
        scope: DocScope,
        request: impl FnOnce(CapId) -> F,
    ) -> Result<T, DrawersError>
    where
        F: Future<Output = Result<T, DrawersError>>,
    {
        let capability = self
            .docs()
            .call(
                context.child(),
                MintDocCapability {
                    authorization: Authorization {
                        authority: self.issuer,
                        signer,
                    },
                    scope,
                    rights: Rights::USE,
                },
            )
            .await
            .map_err(docs_error)?
            .capability_id;
        let result = request(capability).await;
        self.capabilities
            .retract(context, capability)
            .await
            .map_err(authority_error)?;
        result
    }
}

impl Projection {
    pub(crate) async fn granted(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        bounds: BTreeMap<DrawerId, Access>,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        let candidates: Vec<_> = bounds
            .iter()
            .flat_map(|(drawer, bound)| {
                Access::ALL
                    .into_iter()
                    .map(move |access| (drawer_object(access, drawer), access.min(*bound)))
            })
            .collect();
        let proofs = self
            .grants
            .matching(
                context,
                actor,
                &candidates
                    .iter()
                    .map(|(object, _)| object.clone())
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(grant_error)?;
        assert_eq!(
            proofs.len(),
            candidates.len(),
            "one proof per requested object"
        );
        let mut selected = None;
        for ((_, access), proof) in candidates.into_iter().zip(proofs) {
            if proof.rights.contains(Rights::USE)
                && selected.as_ref().is_none_or(|(held, _)| access > *held)
            {
                selected = Some((access, proof.dependencies));
            }
        }
        Ok(selected)
    }
}

impl Core {
    pub(crate) async fn effective(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        drawer: &DrawerId,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        let mut bounds = self
            .state
            .ancestors(state::ScopeTarget::Drawer(drawer))
            .await?;
        // Self-only (or missing) projection: a synced-in drawer hierarchy may not be
        // projected yet. `sync_discovered` finds nothing when the projection is complete.
        if bounds.len() <= 1 {
            self.sync_discovered(context).await?;
            bounds = self
                .state
                .ancestors(state::ScopeTarget::Drawer(drawer))
                .await?;
        }
        bounds.insert(drawer.clone(), Access::Admin);
        self.granted(context, actor, bounds).await
    }

    pub(crate) async fn reach(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        doc: &DocId,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        let mut bounds = self.state.ancestors(state::ScopeTarget::Doc(doc)).await?;
        if bounds.is_empty() {
            // The doc's drawers may have arrived via sync without a local projection row.
            self.sync_discovered(context).await?;
            if self.state.drawer(doc).await?.is_none() {
                drop(self.ensure_drawer(context, doc).await?);
            }
            bounds = self.state.ancestors(state::ScopeTarget::Doc(doc)).await?;
        }
        self.granted(context, actor, bounds).await
    }

    pub(crate) async fn level(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DrawerScope>,
        drawer: &DrawerId,
    ) -> Result<Option<Access>, DrawersError> {
        match authority {
            DomainAuthority::Identity(actor) => Ok(self
                .effective(context, *actor, drawer)
                .await?
                .map(|(access, _)| access)),
            DomainAuthority::Scoped(scope) => {
                if scope.drawers.get(drawer) == Some(&Access::Admin) {
                    return Ok(Some(Access::Admin));
                }
                if scope.drawers.is_empty() {
                    return Ok(None);
                }
                let mut bounds = self
                    .state
                    .ancestors(state::ScopeTarget::Drawer(drawer))
                    .await?;
                bounds.insert(drawer.clone(), Access::Admin);
                Ok(scope
                    .drawers
                    .iter()
                    .filter_map(|(root, access)| {
                        bounds.get(root).map(|bound| (*access).min(*bound))
                    })
                    .max())
            }
        }
    }

    pub(crate) async fn doc_level(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DrawerScope>,
        doc: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        match authority {
            DomainAuthority::Identity(actor) => Ok(self
                .reach(context, *actor, doc)
                .await?
                .map(|(access, _)| access)),
            DomainAuthority::Scoped(scope) => {
                let bounds = self.state.ancestors(state::ScopeTarget::Doc(doc)).await?;
                Ok(scope
                    .drawers
                    .iter()
                    .filter_map(|(root, access)| {
                        bounds.get(root).map(|bound| (*access).min(*bound))
                    })
                    .max())
            }
        }
    }

    pub(crate) async fn authorize(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DrawerScope>,
        drawer: &DrawerId,
        needed: Access,
    ) -> Result<(), DrawersError> {
        match self.level(context, authority, drawer).await? {
            Some(held) if held >= needed => Ok(()),
            _ => Err(DrawersError::Denied),
        }
    }

    pub(crate) async fn authorize_create(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DrawerScope>,
    ) -> Result<Vec<CapId>, DrawersError> {
        match authority {
            DomainAuthority::Identity(actor) => {
                let proof = self
                    .grants
                    .matching(context, *actor, &[CREATE_DRAWER_OBJECT.into()])
                    .await
                    .map_err(grant_error)?
                    .pop()
                    .expect("one proof for creation");
                if !proof.rights.contains(Rights::USE) {
                    return Err(DrawersError::Denied);
                }
                Ok(proof.dependencies)
            }
            DomainAuthority::Scoped(scope) if scope.create => Ok(Vec::new()),
            DomainAuthority::Scoped(_) => Err(DrawersError::Denied),
        }
    }

    pub(crate) async fn grant(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
        source: Option<CapId>,
    ) -> Result<(), DrawersError> {
        self.grants
            .grant(context, subject, object, Rights::USE, source)
            .await
            .map_err(grant_error)?;
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
        self.as_signer(
            context,
            self.issuer,
            DocScope {
                write: [drawer.clone()].into(),
                ..Default::default()
            },
            |capability| async move {
                self.docs()
                    .call(
                        context.child(),
                        crate::doc::WriteFacets {
                            capability,
                            command,
                            doc_id: drawer.clone(),
                            mode: WriteMode::AtCurrent,
                            set,
                            remove,
                        },
                    )
                    .await
                    .map_err(docs_error)
            },
        )
        .await
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
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    read: [record.drawer_id.clone()].into(),
                    ..Default::default()
                },
                |capability| async move {
                    self.docs()
                        .call(
                            context.child(),
                            crate::doc::ReadFacets {
                                capability,
                                doc_id: record.drawer_id.clone(),
                                at: crate::doc::At::Current,
                                facets: None,
                                optimistic: false,
                            },
                        )
                        .await
                        .map_err(docs_error)
                },
            )
            .await?;
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
            let read = self
                .as_signer(
                    context,
                    self.issuer,
                    DocScope {
                        read: [drawer_id.clone()].into(),
                        ..Default::default()
                    },
                    |capability| {
                        let drawer_id = &drawer_id;
                        async move {
                            match self
                                .docs()
                                .call(
                                    context.child(),
                                    crate::doc::ReadFacets {
                                        capability,
                                        doc_id: drawer_id.clone(),
                                        at: crate::doc::At::Current,
                                        facets: Some(std::collections::BTreeSet::from([
                                            drawer_key(),
                                        ])),
                                        optimistic: false,
                                    },
                                )
                                .await
                            {
                                Ok(read) => Ok(Some(read)),
                                Err(ClientError::Operation(
                                    DocsError::DocNotFound | DocsError::Pending { .. },
                                )) => Ok(None),
                                Err(error) => Err(docs_error(error)),
                            }
                        }
                    },
                )
                .await?;
            let Some(read) = read else {
                continue;
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
                if visited.insert(child_id.clone()) && self.state.drawer(&child_id).await?.is_none()
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
        let announced = self
            .facet_set
            .query(crate::index::FacetSetQuery::Tag(
                big_cabinet_core::FacetTag::new(big_cabinet_core::drawer::DRAWER_FACET_TAG)
                    .expect("the drawer facet tag is a valid tag"),
            ))
            .await
            .map_err(|error| DrawersError::Storage(error.to_string()))?
            .into_iter()
            .map(|record| record.route.doc_id)
            .collect();
        for candidate in self.state.unprojected(&announced).await? {
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

pub async fn boot(config: DrawerConfig<'_>) -> Result<(Drawers, StopToken), DrawersError> {
    let service = Arc::new(DrawerService {
        core: Arc::clone(&config.worker.core),
    });
    service.recover().await?;
    for record in service.core.state.drawers().await? {
        service
            .core
            .rescan(&RequestContext::root(), &record)
            .await?;
    }
    service.ensure_local_drawers().await?;
    service
        .core
        .sync_discovered(&RequestContext::root())
        .await?;
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::direct(DrawersBinding::new(Arc::clone(&host)));
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
