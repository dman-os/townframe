//! DrawerService: drawers as document collections (ADR 019, FDR 001).
//!
//! - `<op>.rs`: one file per operation — its wire types, its implementation, and its tests.
//! - [`access`]: the [`DocAccess`] policy DocsRepo is booted with when drawers are in play — "may
//!   this actor act on doc X" is answered by which drawers hold X and what the actor holds on them.
//! - `state`: the local projection — drawers this node knows, their rosters, child links and the
//!   two local drawers — in SQLite, fed by this service's own writes and by boot rescans.
//! - `ledger`: the durable op ledger a roster-and-Keyhive operation steps through, so a crash
//!   between the two halves resumes rather than leaving a grant without an entry or vice versa.
//! - `keys`: the datum keys of a drawer doc's descriptor, roster and entries, and the group keys
//!   a descriptor names its Keyhive groups by.
//!
//! A `DrawerId` is the `DocId` of the doc carrying `org.example.daybook.drawer/main`. The roster
//! (datums on that doc, written through DocsRepo) is what makes inclusion official; the Keyhive
//! groups the descriptor names are what make it effective. The two are deliberately not atomic:
//! adding a doc grants the docs group first and publishes the entry second, removing deletes the
//! entry first and revokes second, so an interrupted operation is visible as "listed but not yet
//! available" or "available but no longer listed" and is finished by the ledger replay at boot.
//!
//! The drawer datums are ordinary signed datums, so an actor holding Write on a drawer doc could
//! publish an entry directly through DocsRepo. That is not a bypass: an entry grants nothing by
//! itself (the Keyhive grant is what the node enforces), it is exactly the "official but
//! unavailable" state ADR 019 names, and the roster's authority is the drawer doc's write
//! authority — which is what that actor held.

pub mod access;
mod add_child;
mod add_doc;
mod create;
mod discovery;
mod get;
mod grant;
mod keys;
mod known;
mod ledger;
mod list;
mod list_doc_drawers;
mod list_docs;
mod mirror;
mod principal;
mod remove_child;
mod remove_doc;
mod revoke;
mod state;
mod successor;
mod sqlite {
    include!("drawer/sqlite/gen.rs");
}

#[cfg(test)]
mod e2e;

pub use self::access::DrawerAccess;
pub use self::access::DrawerProvider;
pub use self::access::DrawerWorker;
pub use self::discovery::DrawerMetadataObservation;
pub use self::discovery::MetadataRootStatus;
pub use self::mirror::Mirrored;
pub use big_cabinet_core::drawer::Drawer;
pub use big_cabinet_core::drawer::DrawerLayout;
pub use big_cabinet_core::drawer::LayoutKind;

use self::keys::child_entry_key;
use self::keys::doc_entry_key;
use self::keys::drawer_key;
pub(crate) use self::keys::entry_target;
use self::keys::group_key;
use self::keys::known_entry_key;
use self::keys::meta_doc_key;
use self::keys::meta_doc_slot;
use self::keys::roster_key;
use self::keys::successor_key;
#[cfg(test)]
use self::ledger::CrashPoint;
use self::ledger::Ledger;
use self::state::MetaDocRows;
use self::state::State;
use crate::authority::DomainAuthority;
use crate::capability::DomainCapabilities;
use crate::capability::Invocation;
use crate::grants::GrantError;
use crate::grants::Grants;
use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerGroups;
use big_cabinet_core::drawer::DrawerMetaDoc;
use big_cabinet_core::drawer::DrawerSuccessor;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::AddChild;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DocDrawer;
use big_cabinet_core::drawer::protocol::DocDrawers;
use big_cabinet_core::drawer::protocol::DrawerCreated;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerRecord;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawerView;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::DrawersMessage;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::ForgetDrawer;
use big_cabinet_core::drawer::protocol::GetDrawer;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::KnownDrawer;
use big_cabinet_core::drawer::protocol::KnownDrawers;
use big_cabinet_core::drawer::protocol::ListDocDrawers;
use big_cabinet_core::drawer::protocol::ListDocs;
use big_cabinet_core::drawer::protocol::ListDrawers;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::ListedDoc;
use big_cabinet_core::drawer::protocol::ListedDocs;
use big_cabinet_core::drawer::protocol::ListedDrawer;
use big_cabinet_core::drawer::protocol::ListedDrawers;
use big_cabinet_core::drawer::protocol::LocalDrawers;
use big_cabinet_core::drawer::protocol::MetaDocCreated;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::Principal;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cabinet_core::drawer::protocol::RemoveChild;
use big_cabinet_core::drawer::protocol::RemoveDoc;
use big_cabinet_core::drawer::protocol::ResolveDrawerSuccessor;
use big_cabinet_core::drawer::protocol::RevokeDrawer;
use big_cabinet_core::drawer::protocol::RevokePrincipal;
use big_cabinet_core::drawer::protocol::SetDrawerPolicy;
use big_cabinet_core::drawer::protocol::SetDrawerSuccessor;
use big_cabinet_core::drawer::protocol::drawer_object;
#[cfg(test)]
use big_cabinet_core::drawer::protocol::drawer_placement;
use big_repo::BigKeyhiveGroup;
use big_repo::SharedBigRepo;
use big_repo::keyhive_core::access::Access as KeyhiveAccess;
use serde::Serialize;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct DrawerConfig<'a> {
    pub worker: &'a DrawerWorker,
}

#[derive(Clone)]
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
    /// The authority's root identity, the node operator (ADR 033 section 5). It holds no
    /// standing grants and needs none: see [`Projection::granted`].
    pub(crate) operator: Uuid7,
    pub(crate) state: State,
    pub(crate) datum_set: Arc<crate::index::DatumSetStore>,
    pub(crate) ledger: Ledger,
    pub(crate) commands: tokio::sync::mpsc::Sender<access::PolicyCommand>,
}

/// The projection rows a drawer doc's roster datums state.
struct RosterRows {
    docs: BTreeMap<DocId, Access>,
    children: BTreeMap<DrawerId, Access>,
    meta_docs: BTreeMap<String, DrawerMetaDoc>,
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
#[derive(Clone)]
pub struct Drawers {
    pub host: Arc<Host<DrawersHandler>>,
    pub client: Client<DrawersProtocol>,
    core: Arc<Core>,
}

impl Drawers {
    /// The two drawers every doc on this node is entered into (ADR 019 "required local drawers").
    pub fn local(&self) -> LocalDrawers {
        self.core.state.local()
    }

    /// Mint an object capability for `drawer_id` at `access` with `rights` in DrawerService's
    /// namespace after verifying that `capability` resolves to an actor holding at least `access`
    /// on `drawer_id` (ADR 022).
    pub async fn mint_drawer_capability(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        drawer_id: &DrawerId,
        access: Access,
        rights: Rights,
    ) -> Result<Capability, DrawersError> {
        self.client
            .call_with(
                context.child(),
                authorization.metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), access)]),
                        create: false,
                    },
                    rights,
                    subject: None,
                },
            )
            .await
            .map_err(|error| match error {
                ClientError::Method(error) => error,
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
        let authorization = Authorization::from_metadata(&request.metadata)?;
        let caller = self
            .core
            .capabilities
            .source::<DrawerScope>(&request.context, authorization)
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
                &caller,
                params.subject,
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
            CreateMetaDoc(CreateMetaDoc),
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
            SetDrawerSuccessor(SetDrawerSuccessor),
            ResolveDrawerSuccessor(ResolveDrawerSuccessor),
            ListDrawers(ListDrawers),
            ListDocDrawers(ListDocDrawers),
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

pub(crate) fn keyhive_access(access: Access) -> KeyhiveAccess {
    match access {
        Access::Relay => KeyhiveAccess::Relay,
        Access::Read => KeyhiveAccess::Read,
        Access::Write => KeyhiveAccess::Edit,
        Access::Admin => KeyhiveAccess::Admin,
    }
}

pub(crate) fn authority_error(error: ClientError<AuthorityServiceError>) -> DrawersError {
    match error {
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DrawersError::Denied,
        ClientError::Method(error) => DrawersError::Authority(error),
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
        ClientError::Method(DocsError::Context(error)) => error.into(),
        ClientError::Method(error) => DrawersError::Docs(error),
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
    /// signed through the handle `signer`, minted for this call and retracted once it returns.
    /// The drawer service holds doc authority over every drawer it projects, so a handle per
    /// request is how it narrows that authority; retracting it keeps the authority's handle
    /// table from growing with every roster read. The minted handle stays bound to the drawer
    /// service (no introduction), and `signer` is only the handle the docs service signs
    /// through: the drawer service's own identity (`self.issuer`) for its own writes, or the
    /// handle a caller presented when the write is the caller's. `request` receives the
    /// [`Authorization`] to attach to the docs call that signs; a call that signs as the drawer
    /// service needs only its `authority`.
    pub(crate) async fn as_signer<T, F>(
        &self,
        context: &RequestContext,
        signer: CapId,
        scope: DocScope,
        request: impl FnOnce(Authorization) -> F,
    ) -> Result<T, DrawersError>
    where
        F: Future<Output = Result<T, DrawersError>>,
    {
        let capability = self
            .docs()
            .call_with(
                context.child(),
                Authorization::identity(self.issuer).metadata(),
                MintDocCapability {
                    scope,
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .map_err(docs_error)?
            .capability_id;
        let result = request(Authorization {
            authority: capability,
            signer,
        })
        .await;
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
        bounds: BTreeMap<DrawerId, state::Reach>,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        // The operator is the cabinet's superuser: it holds Admin on every drawer this node
        // projects without a standing grant, so what it reaches is what Admin on each bounding
        // drawer reaches (a doc at the level it is entered, a drawer through its links). That is
        // what lets the operator turn its own standing into a handle on a drawer that synced in
        // from a peer, where no local grant exists for anyone. Its proof is the identity
        // capability it presents, which every minted handle already depends on, so there is no
        // grant to cite here.
        if actor == self.operator {
            return Ok(bounds
                .values()
                .filter_map(|bound| bound.apply(Access::Admin))
                .max()
                .map(|access| (access, Vec::new())));
        }
        let candidates: Vec<_> = bounds
            .iter()
            .flat_map(|(drawer, bound)| {
                Access::ALL.into_iter().filter_map(move |access| {
                    bound
                        .apply(access)
                        .map(|reached| (drawer_object(access, drawer), reached))
                })
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
        bounds.insert(drawer.clone(), state::Reach::ordinary(Access::Admin));
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
                bounds.insert(drawer.clone(), state::Reach::ordinary(Access::Admin));
                Ok(scope
                    .drawers
                    .iter()
                    .filter_map(|(root, access)| {
                        bounds.get(root).and_then(|bound| bound.apply(*access))
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
                        bounds.get(root).and_then(|bound| bound.apply(*access))
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
            // The operator creates drawers without a grant, as it reaches every drawer without
            // one (see `Projection::granted`).
            DomainAuthority::Identity(actor) if *actor == self.operator => Ok(Vec::new()),
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

    /// Write datums on a drawer doc as this service, at current heads. Roster writes are
    /// last-writer-wins by construction: two nodes publishing the same entry converge on one
    /// value, and the entry's presence is what matters.
    pub(crate) async fn write_roster(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
        command: Option<CommandId>,
        set: BTreeMap<DatumKey, serde_json::Value>,
        remove: std::collections::BTreeSet<DatumKey>,
    ) -> Result<WriteReceipt, DrawersError> {
        self.as_signer(
            context,
            self.issuer,
            DocScope {
                write: [drawer.clone()].into(),
                ..Default::default()
            },
            |Authorization {
                 authority: capability,
                 ..
             }| async move {
                self.docs()
                    .call(
                        context.child(),
                        big_cabinet_core::doc::WriteDatums {
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

    /// Split a drawer doc's datums into the rows the projection stores: doc entries, child
    /// entries (both only at access levels this build knows), and meta-doc references by kind.
    fn read_roster(
        drawer_id: &DrawerId,
        datums: BTreeMap<DatumKey, big_cabinet_core::doc::DatumRead>,
    ) -> Result<RosterRows, DrawersError> {
        let mut docs = BTreeMap::new();
        let mut children = BTreeMap::new();
        let mut meta_docs = BTreeMap::new();
        for (key, datum) in datums {
            let tag = key.tag().as_str();
            let value = match datum {
                big_cabinet_core::doc::DatumRead::Accepted { value, .. } => value,
                big_cabinet_core::doc::DatumRead::Absent { .. } => continue,
                other => {
                    return Err(DrawersError::RosterUnreadable(format!(
                        "{key} on {drawer_id}: {other:?}"
                    )));
                }
            };
            if tag == DrawerMetaDoc::ID {
                // A slot this build cannot split back into `(kind, name)` — the old
                // `metaDoc/<kind>` shape, or a spelling a writer invented — is skipped rather
                // than fatal: it names no slot this build publishes, so there is nothing to
                // admit or to drop.
                let Some((kind, name)) = meta_doc_slot(&key) else {
                    continue;
                };
                let entry: DrawerMetaDoc = serde_json::from_value(value).map_err(|error| {
                    DrawersError::RosterUnreadable(format!("{key} on {drawer_id}: {error}"))
                })?;
                meta_docs.insert(format!("{kind}/{name}"), entry);
                continue;
            }
            if tag != DrawerDocEntry::ID && tag != DrawerChildEntry::ID {
                continue;
            }
            let Some(target) = entry_target(&key) else {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{key} on {drawer_id} does not name a doc"
                )));
            };
            let unreadable = |error: serde_json::Error| {
                DrawersError::RosterUnreadable(format!("{key} on {drawer_id}: {error}"))
            };
            if tag == DrawerDocEntry::ID {
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
        Ok(RosterRows {
            docs,
            children,
            meta_docs,
        })
    }

    /// Every datum `doc` currently carries, read as this service at current heads.
    pub(crate) async fn read_all_datums(
        &self,
        context: &RequestContext,
        doc: &DocId,
    ) -> Result<BTreeMap<DatumKey, big_cabinet_core::doc::DatumRead>, DrawersError> {
        let read = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    read: [doc.clone()].into(),
                    ..Default::default()
                },
                |Authorization {
                     authority: capability,
                     ..
                 }| async move {
                    self.docs()
                        .call(
                            context.child(),
                            big_cabinet_core::doc::ReadDatums {
                                capability,
                                doc_id: doc.clone(),
                                at: big_cabinet_core::doc::At::Current,
                                datums: None,
                                optimistic: false,
                            },
                        )
                        .await
                        .map_err(docs_error)
                },
            )
            .await?;
        Ok(read.datums)
    }

    async fn admit_meta_docs(
        &self,
        groups: DrawerGroups,
        roots: [(MetaRoot, BTreeMap<String, DrawerMetaDoc>); 2],
        statuses: &mut BTreeMap<MetaRoot, MetadataRootStatus>,
    ) -> Result<MetaDocRows, DrawersError> {
        let mut admitted = MetaDocRows::new();
        for (root, slots) in roots {
            if slots.is_empty() {
                continue;
            }
            let named = match root {
                MetaRoot::Public => groups.meta_group,
                MetaRoot::Sensitive => groups.sensitive_group,
            };
            let Some(group) = self.big_repo.get_group_by_id(named.0).await else {
                statuses.insert(
                    root,
                    MetadataRootStatus::Pending {
                        reason: format!("metadata group {named} is not locally available"),
                    },
                );
                continue;
            };
            for (slot, entry) in slots {
                if self.meta_doc_admitted(&group, &entry.doc_id).await? {
                    admitted.entry(root).or_default().insert(slot, entry.doc_id);
                } else {
                    statuses.insert(
                        root,
                        MetadataRootStatus::Pending {
                            reason: format!("unadmitted metadata slot {slot} on {root:?}"),
                        },
                    );
                }
            }
        }
        Ok(admitted)
    }

    /// Bring the projection of `drawer` back in line with its doc: the roster datums, read as
    /// this service, replace the rows. A self-doc drawer has no roster and nothing to rescan.
    /// This is how a change another node made to a shared roster becomes visible here; the
    /// service's own writes update the rows directly.
    pub(crate) async fn rescan(
        &self,
        context: &RequestContext,
        record: &DrawerRecord,
    ) -> Result<(DrawerRecord, big_cabinet_core::drawer::Drawer), DrawersError> {
        let observation = self.observe_metadata(context, &record.drawer_id).await?;
        Ok((
            crate::drawer::create::record_of(&record.drawer_id, &observation.descriptor),
            observation.descriptor,
        ))
    }

    /// Look up `drawer_id` in the local projection, or if it is not yet projected but its doc is
    /// locally available with an accepted `org.example.daybook.drawer/main` datum, project its
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
                    |Authorization {
                         authority: capability,
                         ..
                     }| {
                        let drawer_id = &drawer_id;
                        async move {
                            match self
                                .docs()
                                .call(
                                    context.child(),
                                    big_cabinet_core::doc::ReadDatums {
                                        capability,
                                        doc_id: drawer_id.clone(),
                                        at: big_cabinet_core::doc::At::Current,
                                        datums: Some(std::collections::BTreeSet::from([
                                            drawer_key(),
                                        ])),
                                        optimistic: false,
                                    },
                                )
                                .await
                            {
                                Ok(read) => Ok(Some(read)),
                                Err(ClientError::Method(
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
            let Some(big_cabinet_core::doc::DatumRead::Accepted { value, .. }) =
                read.datums.get(&drawer_key())
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

    /// Project any drawer docs that arrived via sync (referenced by child entries or `datum_set`)
    /// into the local drawer projection without modifying the registry.
    pub(crate) async fn sync_discovered(
        &self,
        context: &RequestContext,
    ) -> Result<(), DrawersError> {
        let announced = self
            .datum_set
            .query(crate::index::DatumSetQuery::Tag(
                big_cabinet_core::DatumTag::new(Drawer::ID)
                    .expect("the drawer datum tag is a valid tag"),
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

    /// One Keyhive group a layout names, or why it is not in Keyhive.
    pub(crate) async fn group(&self, key: GroupKey) -> Result<BigKeyhiveGroup, DrawersError> {
        self.big_repo
            .get_group_by_id(key.0)
            .await
            .ok_or_else(|| DrawersError::Keyhive(format!("group {key} is not in keyhive")))
    }

    /// A known single-group drawer's three groups — public metadata, docs, sensitive metadata,
    /// in that order — or why it cannot take entries.
    pub(crate) async fn groups(
        &self,
        drawer: &DrawerId,
    ) -> Result<(BigKeyhiveGroup, BigKeyhiveGroup, BigKeyhiveGroup), DrawersError> {
        let record = self
            .ensure_drawer(&RequestContext::root(), drawer)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(drawer.clone()))?;
        let DrawerGroups {
            meta_group,
            docs_group,
            sensitive_group,
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
        Ok((
            self.group(meta_group).await?,
            self.group(docs_group).await?,
            self.group(sensitive_group).await?,
        ))
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
                ledger::Op::CreateMetaDoc { .. } => {
                    self.core.resume_create_meta_doc(&context, &row).await?
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
    let client = Client::new(Transport::direct(DrawersBinding::new(Arc::clone(&host))));
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
