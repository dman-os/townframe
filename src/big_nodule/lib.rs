//! The nodule service: the node-side runtime management surface of ADR 027 §4, §6, and §8,
//! without an executor.
//!
//! Portable desired state lives in two kinds of document (ADR 044), each projected through
//! [`big_cabinet::DatumStoreHandle`]: the node's config drawer document holds the installs (a pin
//! of the published version, and enablement on this node) over
//! [`big_nodule_core::config::NoduleConfig`], and each cartridge's config document in the shared
//! config drawer holds its settings and its grants over
//! [`big_nodule_core::config::CartridgeConfig`].
//! Host-specific state (each nodule's durable local actor, the service-owned drawer grant
//! capabilities and the dispatch handles minted from method grants applied on this node)
//! lives in SQLite.

#![recursion_limit = "256"]

use sqlite_rs as sqlite_rs_alias;

mod sqlite {
    include!("sqlite/gen.rs");
}
mod cartridge_docs;
pub mod egress;
pub use service_client_core_rs::xrpc::XrpcBody;
pub mod executor;
mod requests;
pub use requests::MissingAuthorityLookup;
mod state;
mod store;
pub mod wire;

#[cfg(test)]
mod e2e;

use big_cabinet::DatumStoreHandle;
use big_cabinet::DrawerAccess;
use big_cabinet::Drawers;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::SchemaRepo;
use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet::capability::Invocation;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::Universe;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::AWAIT_MAX_HOLD_MS;
use big_cartridges_core::protocol::AnswerCartridgeGrantRequest;
use big_cartridges_core::protocol::AwaitCartridgeGrantRequest;
use big_cartridges_core::protocol::AwaitCartridgeGrants;
use big_cartridges_core::protocol::CartridgeGrants;
use big_cartridges_core::protocol::CartridgeGrantsAt;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgeInstalls;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_cartridges_core::protocol::ClearCartridgeConfig;
use big_cartridges_core::protocol::FiledGrantRequest;
use big_cartridges_core::protocol::GrantRequestAnswer;
use big_cartridges_core::protocol::GrantRequestState;
use big_cartridges_core::protocol::GrantRequests;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::ListCartridgeGrantRequests;
use big_cartridges_core::protocol::ListCartridgeGrants;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ObserveCartridgeInstalls;
use big_cartridges_core::protocol::RequestCartridgeGrant;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::protocol::UninstallCartridge;
use big_dispatch_core::DispatchProtocol;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::config::config_universe;
use big_nodule_core::protocol::ConfirmNoduleResolution;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::MintNoduleCapability;
use big_nodule_core::protocol::NoduleList;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::NodulesMessage;
use big_nodule_core::protocol::NodulesProtocol;
use big_nodule_core::protocol::SetMethodRoute;
use big_nodule_core::protocol::SetNoduleDevTakeover;
use method_core::EventStream;
use method_core::Method;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use state::Command;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

/// The prefix of every capability object this domain mints; the scope follows it as JSON.
const SCOPE_PREFIX: &str = "NoduleService/scope/v1/";

static CONFIG_UNIVERSE: LazyLock<Universe> = LazyLock::new(config_universe);

/// The schema registration group for the portable nodule config datums stored in the node's
/// config drawer document. The handler artifact references there are `Blob` datums, whose
/// Lexicon the embedder registers beside this group (big_blobs does).
pub fn schema_group() -> SchemaRegistrationGroup<'static> {
    let bundle = Bundle::new(&CONFIG_UNIVERSE, Vec::new());
    let lexicons = bundle.lexicons().expect("nodule config lexicons publish");
    let registrations = lexicons
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new())
}

/// What the embedder hands the service: the authority it provisions actors from, the docs and
/// drawers services it reads config from and applies grants through, the node's config drawer, the
/// registry drawer whose documents installs pin and the shared config drawer (from
/// [`ensure_shared_config_drawer`]) that holds each cartridge's config document, the subsystem actor it provisions nodule actors under and that actor's registry path, the
/// namespace it mints management handles in,
/// the dispatch router it registers each admitted nodule's implemented operations in, the dispatch
/// client and dispatch handle it mints every nodule's own handle from (a
/// handle covering every authority, whose privilege bit only the node's own grants to its core
/// cartridge turn on), the optional executor that hosts their
/// handlers, the node's schema repo it registers each enabled nodule's datum Lexicons in, and its
/// database. Without an executor a handler is never
/// ready: its operations are registered not ready with the nodule's own readiness.
pub struct NodulesConfig {
    pub authority: Client<AuthorityProtocol>,
    pub docs: Client<DocsProtocol>,
    pub drawers: Client<DrawersProtocol>,
    pub config_drawer: DrawerId,
    /// The nodules actor must hold Read on the registry drawer and Admin on the shared config
    /// drawer.
    pub registry_drawer: DrawerId,
    pub shared_config_drawer: DrawerId,
    pub actor: OpenedActor,
    pub actor_path: String,
    pub namespace: Namespace,
    pub dispatch: Arc<big_dispatch::Router>,
    pub dispatch_client: Client<DispatchProtocol>,
    pub dispatch_authority: CapId,
    /// The cartridges service that resolves a pinned version's payloads at install, and the
    /// handle presented to it: a read-only one (no `publish`) the operator introduced to the
    /// nodule service's own actor, whose capability is the `signer`.
    pub cartridges: Client<CartridgesProtocol>,
    pub cartridges_authorization: Authorization,
    /// The blobs service, and the delegator handle the operator minted the nodule service's actor
    /// for every method-level blobs flag (`put`), with `DELEGATE`: each routine whose `#method`
    /// permbind names a method that needs a blobs handle is minted its narrowed handle from it
    /// (ADR 049 §6.1–6.2).
    pub blobs: Client<big_blobs_core::BlobsProtocol>,
    pub blobs_capability: CapId,
    pub executor: Option<Arc<dyn executor::NoduleExecutor>>,
    pub schemas: Arc<SchemaRepo>,
    /// Whether the dev takeover hook is on (`dev.allow_slot_takeover` in `node.json`, ADR 045 §5).
    /// Fixed at boot: with it off, `setNoduleDevTakeover` refuses to set one and a takeover datum
    /// found in the config document is not honored, so the slot is refused.
    pub allow_slot_takeover: bool,
    /// The node's core cartridge (ADR 053), which the node itself installs at boot: a person's
    /// install of any other version of it, and every change to its install, is refused.
    pub core_cartridge: CoreCartridge,
    pub sql: Database,
}

/// The id of the node's core cartridge, the version the node's build carries (ADR 053), the
/// actor its nodule acts as (the `micros` subsystem actor, with the rights to open children), the
/// handles its routines' handles are minted from, and the grants the node itself makes it. The grants are held for the boot, never written to its
/// config document, which is what lets one of them be `privileged`
/// ([`big_cartridges_core::config::GrantBody::privileged`]): a person cannot grant that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreCartridge {
    pub id: CartridgeId,
    pub version: big_cartridges_core::Version,
    pub actor: OpenedActor,
    pub delegators: CoreDelegators,
    pub grants: std::collections::BTreeMap<
        big_cartridges_core::config::GrantKey,
        big_cartridges_core::config::GrantBody,
    >,
}

/// The handles the core cartridge's routines' handles are minted from, which the operator
/// introduces to the `micros` actor. The nodule service mints every other nodule's handles from
/// ones bound to its own actor (`nodules`), but an introduction may only name a descendant of the
/// delegator's subject, and `micros` is a sibling of `nodules`, not a descendant: a `nodules`-bound
/// delegator cannot mint a handle for it. Each has the scope rules of the service's own, `privileged`
/// included for dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreDelegators {
    pub dispatch: CapId,
    pub docs: CapId,
    pub blobs: CapId,
}

/// The hosted surface: it settles who the caller is and what they may touch, then hands the
/// work to the state task that owns the reconciled inventory.
pub struct NoduleService {
    capabilities: DomainCapabilities,
    /// The authority service, for the two methods a cartridge's own actor presents a dispatch
    /// handle to (see [`Self::presenter`]).
    authority: Client<AuthorityProtocol>,
    /// The core cartridge's id: its install is the node's, so the methods that change an install
    /// refuse it (ADR 053 §3 item 5).
    core: CartridgeId,
    state: state::Handle,
}

type Res<T> = std::result::Result<T, NodulesError>;

/// Who presents a call to one of the methods a cartridge makes on its own behalf.
enum Presenter {
    /// The operator, presenting its root capability.
    Operator,
    /// An actor holding a handle bound to it, which for a call through dispatch is a nodule's
    /// actor presenting the handle dispatch minted for it.
    Actor(Uuid7),
}

pub(crate) fn authority_error(error: ClientError<AuthorityServiceError>) -> NodulesError {
    match error {
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => NodulesError::Denied,
        ClientError::Method(error) => NodulesError::Authority(error),
        ClientError::Transport { message, .. } => NodulesError::AuthorityUnavailable(message),
    }
}

pub(crate) fn docs_error(error: ClientError<DocsError>) -> NodulesError {
    match error {
        ClientError::Method(DocsError::Context(error)) => error.into(),
        ClientError::Method(DocsError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Method(DocsError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        ClientError::Method(DocsError::Denied) => NodulesError::Denied,
        ClientError::Method(other) => NodulesError::Storage(other.to_string()),
        ClientError::Transport { message, .. } => NodulesError::Storage(message),
    }
}

/// A failure of the cartridges service while resolving a pinned version that no refusal of the
/// pin explains.
pub(crate) fn cartridges_error(error: ClientError<CartridgesError>) -> NodulesError {
    match error {
        ClientError::Method(CartridgesError::Context(error)) => error.into(),
        ClientError::Method(CartridgesError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Method(CartridgesError::AuthorityUnavailable(message)) => {
            NodulesError::AuthorityUnavailable(message)
        }
        ClientError::Method(CartridgesError::Denied) => NodulesError::Denied,
        ClientError::Method(other) => NodulesError::Storage(other.to_string()),
        ClientError::Transport { message, .. } => NodulesError::Storage(message),
    }
}

pub(crate) fn store_error(error: StoreError) -> NodulesError {
    match error {
        StoreError::Docs(DocsError::Context(error)) => error.into(),
        StoreError::Docs(DocsError::Authority(error)) => NodulesError::Authority(error),
        StoreError::Docs(DocsError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        StoreError::Docs(DocsError::Denied) | StoreError::AccessLost => NodulesError::Denied,
        StoreError::Docs(other) => NodulesError::Storage(other.to_string()),
        StoreError::DocsUnavailable(msg) => NodulesError::Storage(msg),
        StoreError::InvalidEdit(msg) => NodulesError::Rejected(msg),
    }
}

impl NoduleService {
    async fn caller(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<Invocation<NoduleScope>> {
        self.capabilities
            .source::<NoduleScope>(context, authorization)
            .await
            .map_err(authority_error)
    }

    /// Who presents `authorization`. The grant-request methods a cartridge's runtime calls
    /// through dispatch carry the routine's dispatch handle, which this domain does not mint and
    /// so cannot read as a [`NoduleScope`]: the presenter is the actor the handle is bound to,
    /// accepted only when the signer is bound to that same actor (holder binding, ADR 037), the
    /// check dispatch itself ran before admitting the call.
    async fn presenter(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<Presenter> {
        let resolved = self
            .authority
            .call(
                context.child(),
                ResolveAuthority {
                    capability: authorization.authority,
                },
            )
            .await
            .map_err(authority_error)?;
        if resolved.is_operator_root() {
            return Ok(Presenter::Operator);
        }
        let actor = resolved
            .actor
            .map(|actor| actor.actor_id)
            .or(resolved.capability.subject)
            .ok_or(NodulesError::Denied)?;
        let signer = self
            .authority
            .call(
                context.child(),
                InspectCapability {
                    capability: authorization.signer,
                },
            )
            .await
            .map_err(authority_error)?;
        if !signer.rights.contains(Rights::USE) || signer.subject != Some(actor) {
            return Err(NodulesError::Denied);
        }
        Ok(Presenter::Actor(actor))
    }

    /// The cartridge a presenting actor runs for.
    async fn cartridge_of(&self, actor_id: Uuid7) -> Res<CartridgeId> {
        self.state
            .ask(|reply| Command::CartridgeOfActor { actor_id, reply })
            .await
    }

    /// Files a request on behalf of the cartridge the calling actor runs for. The cartridge is
    /// never a field of the request, so one cartridge cannot file for another, and the operator,
    /// who runs for none, cannot file.
    async fn request_grant(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: RequestCartridgeGrant,
    ) -> Res<FiledGrantRequest> {
        let Presenter::Actor(actor_id) = self.presenter(context, authorization).await? else {
            return Err(NodulesError::Denied);
        };
        let cartridge_id = self.cartridge_of(actor_id).await?;
        self.state
            .ask(|reply| Command::FileGrantRequest {
                cartridge_id,
                permbind: params.permbind,
                drawer: params.drawer,
                reply,
            })
            .await
    }

    /// The long poll of ADR 023 over one request: answers at once when it is no longer pending,
    /// otherwise holds until the state actor answers it, the hold expires, or the service stops.
    /// Only the cartridge that filed the request (or the operator) may ask after it.
    async fn await_grant_request(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: AwaitCartridgeGrantRequest,
    ) -> Res<GrantRequestAnswer> {
        let owner = match self.presenter(context, authorization).await? {
            Presenter::Operator => None,
            Presenter::Actor(actor_id) => Some(self.cartridge_of(actor_id).await?),
        };
        let deadline = Instant::now()
            + Duration::from_millis(u64::from(params.hold_ms.min(AWAIT_MAX_HOLD_MS)));
        let mut requests = self.state.requests.clone();
        loop {
            let state = {
                let current = requests.borrow_and_update();
                current
                    .get(&params.request)
                    .filter(|entry| {
                        owner
                            .as_ref()
                            .is_none_or(|own| *own == entry.record.cartridge_id)
                    })
                    .map(|entry| entry.state)
                    .ok_or_else(|| {
                        NodulesError::Rejected(format!(
                            "there is no grant request `{}`",
                            params.request
                        ))
                    })?
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if state != GrantRequestState::Pending || remaining.is_zero() {
                return Ok(GrantRequestAnswer { state });
            }
            // The sender lives as long as the state actor: a closed channel is the service
            // stopping, which ends the hold like an expiry.
            if let Ok(Err(_)) = tokio::time::timeout(remaining, requests.changed()).await {
                return Ok(GrantRequestAnswer { state });
            }
        }
    }

    /// The pending requests, for the operator.
    async fn list_grant_requests(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        _: ListCartridgeGrantRequests,
    ) -> Res<GrantRequests> {
        let caller = self.caller(context, authorization).await?;
        if self.scope_of(&caller)?.is_some() {
            return Err(NodulesError::Denied);
        }
        Ok(GrantRequests {
            requests: self
                .state
                .requests
                .borrow()
                .values()
                .filter(|entry| entry.state == GrantRequestState::Pending)
                .map(|entry| entry.record.clone())
                .collect(),
        })
    }

    async fn answer_grant_request(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: AnswerCartridgeGrantRequest,
    ) -> Res<GrantRequestAnswer> {
        let caller = self.caller(context, authorization).await?;
        if self.scope_of(&caller)?.is_some() {
            return Err(NodulesError::Denied);
        }
        let context = context.clone();
        let state = self
            .state
            .ask(|reply| Command::AnswerGrantRequest {
                context,
                authorization,
                request: params.request,
                answer: params.answer,
                drawer: params.drawer,
                reply,
            })
            .await?;
        Ok(GrantRequestAnswer { state })
    }

    async fn list_cartridge_grants(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: ListCartridgeGrants,
    ) -> Res<CartridgeGrants> {
        if self
            .read_scope(context, authorization, ListCartridgeGrants::XRPC.nsid)
            .await?
            .is_some()
        {
            return Err(NodulesError::Denied);
        }
        let grants = self
            .state
            .ask(|reply| Command::ListGrants {
                cartridge_id: params.cartridge_id,
                reply,
            })
            .await?;
        Ok(CartridgeGrants { grants })
    }

    /// The long poll over one cartridge's grants: the operator's, as the list is.
    async fn await_cartridge_grants(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: AwaitCartridgeGrants,
    ) -> Res<CartridgeGrantsAt> {
        if self
            .read_scope(context, authorization, AwaitCartridgeGrants::XRPC.nsid)
            .await?
            .is_some()
        {
            return Err(NodulesError::Denied);
        }
        let deadline = Instant::now()
            + Duration::from_millis(u64::from(params.hold_ms.min(AWAIT_MAX_HOLD_MS)));
        let mut feed = self.state.grants.clone();
        // The sender lives as long as the state actor: a closed channel is the service stopping,
        // which ends the hold like an expiry.
        let mut closed = false;
        loop {
            {
                let current = feed.borrow_and_update();
                let revision = current.revision(&params.cartridge_id);
                if params.since.as_ref() != Some(&revision) || closed || Instant::now() >= deadline
                {
                    return Ok(CartridgeGrantsAt {
                        grants: current.grants(&params.cartridge_id).to_vec(),
                        revision,
                    });
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            closed = matches!(
                tokio::time::timeout(remaining, feed.changed()).await,
                Ok(Err(_))
            );
        }
    }

    /// What the caller may administer: everything for the operator's root capability, the scope of a
    /// handle this domain minted, nothing for any other identity. Another actor is refused
    /// rather than looked up in a grant table: this domain has no entitlements beyond the
    /// handles it minted.
    fn scope_of<'a>(&self, caller: &'a Invocation<NoduleScope>) -> Res<Option<&'a NoduleScope>> {
        let kind = match &caller.authority {
            DomainAuthority::Operator => "operator",
            DomainAuthority::Identity(_) => "identity",
            DomainAuthority::Scoped(_) => "scoped",
        };
        tracing::info!(
            target: "instr",
            kind,
            handle = ?caller.handle,
            via = ?caller.via,
            signer = ?caller.signer.actor_id,
            rights = ?caller.rights,
            "INSTR nodule scope_of"
        ); // INSTR
        if !caller.rights.contains(Rights::USE) {
            tracing::info!(target: "instr", kind, "INSTR nodule scope_of denied: no USE right"); // INSTR
            return Err(NodulesError::Denied);
        }
        match &caller.authority {
            DomainAuthority::Operator => Ok(None),
            DomainAuthority::Identity(_) => {
                tracing::info!(target: "instr", "INSTR nodule scope_of denied: identity that is not the operator"); // INSTR
                Err(NodulesError::Denied)
            }
            DomainAuthority::Scoped(scope) => Ok(Some(scope)),
        }
    }

    /// What a read of every cartridge's installs or grants may see for `method`: `None` for all
    /// of it, `Some` for a management handle's scope.
    ///
    /// The operator and the handles this domain minted are settled by [`Self::scope_of`]. A
    /// handle this domain did not mint is a nodule's dispatch handle (dispatch admitted the call
    /// before it got here), which is read as everything only when the presenting nodule's
    /// cartridge holds a privileged grant of exactly `method`. A privileged method trusts a
    /// privileged grant for that method and for no other, whichever cartridge holds it: the node
    /// makes that grant to its core cartridge alone ([`CoreCartridge::grants`]), and nothing in
    /// this rule names the core cartridge or the `micros` actor.
    async fn read_scope(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        method: &str,
    ) -> Res<Option<NoduleScope>> {
        match self.caller(context, authorization).await {
            Ok(caller) => return Ok(self.scope_of(&caller)?.cloned()),
            // Not one of this domain's handles. A dead or foreign handle is refused below too,
            // by `presenter`.
            Err(NodulesError::Denied) => {}
            Err(error) => return Err(error),
        }
        let Presenter::Actor(actor_id) = self.presenter(context, authorization).await? else {
            return Err(NodulesError::Denied);
        };
        let cartridge_id = self.cartridge_of(actor_id).await?;
        let grant = GrantKey::Method(method.to_owned());
        // Only a boot grant counts: the runtime reads no privilege out of a config document.
        let privileged = self
            .state
            .grants
            .borrow()
            .grants(&cartridge_id)
            .iter()
            .any(|record| record.this_boot && record.grant == grant && record.value.privileged);
        if privileged {
            Ok(None)
        } else {
            Err(NodulesError::Denied)
        }
    }

    /// Refuses a change to the core cartridge's install, which only the node makes.
    fn refuse_core(&self, cartridge_id: &CartridgeId) -> Res<()> {
        if *cartridge_id == self.core {
            return Err(NodulesError::CoreCartridge(cartridge_id.clone()));
        }
        Ok(())
    }

    fn require_manage(
        &self,
        caller: &Invocation<NoduleScope>,
        cartridge_id: &CartridgeId,
    ) -> Res<()> {
        match self.scope_of(caller)? {
            Some(scope) if !scope.cartridges.contains(cartridge_id) => Err(NodulesError::Denied),
            _ => Ok(()),
        }
    }

    async fn mint(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: MintNoduleCapability,
    ) -> Res<Capability> {
        let caller = self.caller(context, authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(NodulesError::Denied);
        }
        self.capabilities
            .mint(
                context,
                &caller,
                None,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)
    }

    /// Installs the published version `params.cartridge` pins. A scoped handle installs only the
    /// cartridge ids in its `cartridges`, and only with `admit` set, which the state actor
    /// checks once it has read the id out of the pin.
    async fn install(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: InstallCartridge,
    ) -> Res<CartridgeInstallRecord> {
        tracing::info!(
            target: "instr",
            authority = ?authorization.authority,
            signer_cap = ?authorization.signer,
            "INSTR nodule install presented"
        ); // INSTR
        let caller = self
            .caller(context, authorization)
            .await
            .inspect_err(|error| {
                tracing::info!(target: "instr", ?error, "INSTR nodule install: caller refused"); // INSTR
            })?;
        let scope = self.scope_of(&caller)?.cloned();
        tracing::info!(target: "instr", scope = ?scope, "INSTR nodule install scope_of result"); // INSTR
        let context = context.clone();
        self.state
            .ask(|reply| Command::Install {
                context,
                scope,
                pin: params.cartridge,
                reply,
            })
            .await
    }

    async fn set_cartridge_enabled(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: SetCartridgeEnabled,
    ) -> Res<CartridgeInstallRecord> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        self.refuse_core(&params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetEnabled {
                context,
                cartridge_id: params.cartridge_id,
                component: params.component,
                enabled: params.enabled,
                reply,
            })
            .await
    }

    async fn set_cartridge_grant(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: SetCartridgeGrant,
    ) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        self.refuse_core(&params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetGrant {
                context,
                authorization,
                cartridge_id: params.cartridge_id,
                grant: params.grant,
                value: params.value,
                reply,
            })
            .await
    }

    async fn clear_cartridge_config(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: ClearCartridgeConfig,
    ) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        self.refuse_core(&params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::ClearConfig {
                context,
                cartridge_id: params.cartridge_id,
                reply,
            })
            .await
    }

    async fn uninstall_cartridge(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: UninstallCartridge,
    ) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        self.refuse_core(&params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Uninstall {
                context,
                cartridge_id: params.cartridge_id,
                reply,
            })
            .await
    }

    async fn list_installs(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        _: ListCartridgeInstalls,
    ) -> Res<CartridgeInstalls> {
        let scope = self
            .read_scope(context, authorization, ListCartridgeInstalls::XRPC.nsid)
            .await?;
        let context = context.clone();
        let installs = self
            .state
            .ask(|reply| Command::ListInstalls { context, reply })
            .await?;
        Ok(CartridgeInstalls {
            installs: visible_installs(installs, scope.as_ref()),
        })
    }

    /// The install rows now, and again after each change, for the rows the caller may read.
    async fn observe_installs(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<EventStream<CartridgeInstalls, CartridgesError>> {
        let caller = self.caller(context, authorization).await?;
        let scope = self.scope_of(&caller)?.cloned();
        let rows = self.state.installs.clone();
        Ok(Box::pin(futures::stream::unfold(
            (rows, true),
            move |(mut rows, first)| {
                let scope = scope.clone();
                async move {
                    if !first {
                        // The sender lives as long as the state actor, so the feed ends with the
                        // service.
                        rows.changed().await.ok()?;
                    }
                    let installs =
                        visible_installs(rows.borrow_and_update().clone(), scope.as_ref());
                    Some((Ok(CartridgeInstalls { installs }), (rows, false)))
                }
            },
        )))
    }

    async fn confirm_resolution(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: ConfirmNoduleResolution,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::ConfirmResolution {
                context,
                cartridge_id: params.cartridge_id,
                binding: params.binding,
                reply,
            })
            .await
    }

    async fn set_dev_takeover(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: SetNoduleDevTakeover,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        if let Some(by) = &params.by {
            self.require_manage(&caller, by)?;
        }
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetDevTakeover {
                context,
                cartridge_id: params.cartridge_id,
                by: params.by,
                reply,
            })
            .await
    }

    /// Routes are node-wide, so only the operator's root capability may set them: a scoped handle
    /// manages named cartridges and never decides where every caller's data goes.
    async fn set_method_route(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: SetMethodRoute,
    ) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        if self.scope_of(&caller)?.is_some() {
            return Err(NodulesError::Denied);
        }
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetMethodRoute {
                context,
                pattern: params.pattern,
                provider: params.provider,
                reply,
            })
            .await
    }

    async fn get(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: GetNodule,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, authorization).await?;
        self.require_manage(&caller, &params.cartridge_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Get {
                context,
                cartridge_id: params.cartridge_id,
                reply,
            })
            .await
    }

    async fn list(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        _: ListNodules,
    ) -> Res<NoduleList> {
        let caller = self.caller(context, authorization).await?;
        let scope = self.scope_of(&caller)?.cloned();
        let context = context.clone();
        let nodules = self
            .state
            .ask(|reply| Command::List {
                context,
                scope,
                reply,
            })
            .await?;
        Ok(NoduleList { nodules })
    }
}

/// The install rows a caller may read: all of them for the operator, the cartridges named by a
/// scoped handle for that handle.
fn visible_installs(
    installs: Vec<CartridgeInstallRecord>,
    scope: Option<&NoduleScope>,
) -> Vec<CartridgeInstallRecord> {
    installs
        .into_iter()
        .filter(|record| scope.is_none_or(|scope| scope.cartridges.contains(&record.cartridge_id)))
        .collect()
}

macro_rules! handlers {
    ($error:ty; $($request:ident => $method:ident : $output:ty;)*) => {$(
        impl Handler<$request> for NoduleService {
            async fn call(
                &self,
                request: Envelope<$request>,
            ) -> Result<ResponseEnvelope<$output>, $error> {
                let authorization = Authorization::from_metadata(&request.metadata)?;
                Ok(self
                    .$method(&request.context, authorization, request.params)
                    .await?
                    .into())
            }
        }
    )*};
}
handlers! {
    NodulesError;
    MintNoduleCapability => mint: Capability;
    ConfirmNoduleResolution => confirm_resolution: NoduleRecord;
    SetNoduleDevTakeover => set_dev_takeover: NoduleRecord;
    SetMethodRoute => set_method_route: ();
    GetNodule => get: NoduleRecord;
    ListNodules => list: NoduleList;
}
// The cartridge-level methods fail with the cartridges service's error (see
// `big_cartridges_core::protocol`); the service's own errors convert into it.
handlers! {
    CartridgesError;
    InstallCartridge => install: CartridgeInstallRecord;
    SetCartridgeEnabled => set_cartridge_enabled: CartridgeInstallRecord;
    UninstallCartridge => uninstall_cartridge: ();
    SetCartridgeGrant => set_cartridge_grant: ();
    ClearCartridgeConfig => clear_cartridge_config: ();
    ListCartridgeInstalls => list_installs: CartridgeInstalls;
    RequestCartridgeGrant => request_grant: FiledGrantRequest;
    AwaitCartridgeGrantRequest => await_grant_request: GrantRequestAnswer;
    ListCartridgeGrantRequests => list_grant_requests: GrantRequests;
    AnswerCartridgeGrantRequest => answer_grant_request: GrantRequestAnswer;
    ListCartridgeGrants => list_cartridge_grants: CartridgeGrants;
    AwaitCartridgeGrants => await_cartridge_grants: CartridgeGrantsAt;
}

impl Handler<ObserveCartridgeInstalls> for NoduleService {
    async fn call(
        &self,
        request: Envelope<ObserveCartridgeInstalls>,
    ) -> Result<ResponseEnvelope<EventStream<CartridgeInstalls, CartridgesError>>, CartridgesError>
    {
        let authorization = Authorization::from_metadata(&request.metadata)?;
        Ok(self
            .observe_installs(&request.context, authorization)
            .await?
            .into())
    }
}

service_utils_rs::bind_service_protocol! {
    pub struct NodulesBinding for NodulesProtocol => NodulesMessage {
        unary {
            MintNoduleCapability(MintNoduleCapability),
            InstallCartridge(InstallCartridge),
            SetCartridgeEnabled(SetCartridgeEnabled),
            UninstallCartridge(UninstallCartridge),
            SetCartridgeGrant(SetCartridgeGrant),
            ClearCartridgeConfig(ClearCartridgeConfig),
            ListCartridgeInstalls(ListCartridgeInstalls),
            RequestCartridgeGrant(RequestCartridgeGrant),
            AwaitCartridgeGrantRequest(AwaitCartridgeGrantRequest),
            ListCartridgeGrantRequests(ListCartridgeGrantRequests),
            AnswerCartridgeGrantRequest(AnswerCartridgeGrantRequest),
            ListCartridgeGrants(ListCartridgeGrants),
            AwaitCartridgeGrants(AwaitCartridgeGrants),
            ConfirmNoduleResolution(ConfirmNoduleResolution),
            SetNoduleDevTakeover(SetNoduleDevTakeover),
            SetMethodRoute(SetMethodRoute),
            GetNodule(GetNodule),
            ListNodules(ListNodules)
        }
        stream {
            ObserveCartridgeInstalls(ObserveCartridgeInstalls)
        }
    }
}

pub type NodulesHandler = Arc<NoduleService>;

/// A booted nodule service: the host that fronts it and the client its consumers use.
pub struct Nodules {
    pub host: Arc<Host<NodulesHandler>>,
    pub client: Client<NodulesProtocol>,
    /// The drawer the service created (or found) for the cartridge config documents.
    pub shared_config_drawer: DrawerId,
}

/// Boot the nodule service on `config.sql`, load its config datum store on `config.config_drawer`,
/// reconcile its inventory, and host it.
pub async fn boot(config: NodulesConfig) -> Result<(Nodules, StopToken), NodulesError> {
    store::migrate(&config.sql).await?;
    let config_doc = DocId::from(config.config_drawer);
    let doc_cap = config
        .docs
        .call_with(
            RequestContext::root(),
            Authorization::identity(config.actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    read: DocScope::read_all([config_doc.clone()]),
                    write: std::collections::BTreeMap::from([(
                        config_doc.clone(),
                        big_cabinet_core::doc::DatumKinds::All,
                    )]),
                    // The registry's documents are only read, at the heads installs pin. The
                    // shared config drawer holds the cartridge config documents, which are
                    // created, read and written.
                    drawers: BTreeMap::from([
                        (
                            config.registry_drawer.clone(),
                            big_cabinet_core::doc::DrawerReach::all_kinds(Access::Read),
                        ),
                        (
                            config.shared_config_drawer.clone(),
                            big_cabinet_core::doc::DrawerReach::all_kinds(Access::Write),
                        ),
                    ]),
                    ..DocScope::default()
                },
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                subject: None,
            },
        )
        .await
        .map_err(docs_error)?;
    let (datum_store, store_stop) = DatumStoreHandle::<NoduleConfig>::load(
        &RequestContext::root(),
        config.docs.clone(),
        doc_cap.capability_id,
        config_doc,
    )
    .await
    .map_err(store_error)?;
    let authority = config.authority.clone();
    let capabilities =
        DomainCapabilities::new(config.authority.clone(), config.namespace, SCOPE_PREFIX);
    let domain_stop = capabilities.stop_token();
    let (state, state_stop) = match state::State::recover(state::Stores {
        sql: config.sql,
        authority: config.authority,
        drawers: config.drawers,
        datum_store,
        docs: config.docs,
        docs_capability: doc_cap.capability_id,
        cartridges: config.cartridges,
        cartridges_authorization: config.cartridges_authorization,
        blobs: config.blobs,
        blobs_capability: config.blobs_capability,
        shared_config_drawer: config.shared_config_drawer.clone(),
        parent: config.actor,
        parent_path: config.actor_path,
        dispatch: config.dispatch,
        dispatch_client: config.dispatch_client,
        dispatch_authority: config.dispatch_authority,
        executor: config.executor,
        schemas: config.schemas,
        allow_slot_takeover: config.allow_slot_takeover,
        core_cartridge: config.core_cartridge.clone(),
    })
    .await
    {
        Ok(recovered) => recovered,
        Err(error) => {
            store_stop.then(domain_stop).stop().await;
            return Err(error);
        }
    };
    let service = Arc::new(NoduleService {
        capabilities,
        authority,
        core: config.core_cartridge.id,
        state,
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(NodulesBinding::new(Arc::clone(&host))));
    Ok((
        Nodules {
            host,
            client,
            shared_config_drawer: config.shared_config_drawer,
        },
        host_stop
            .then(state_stop)
            .then(store_stop)
            .then(domain_stop),
    ))
}

/// What [`ensure_shared_config_drawer`] needs: the drawers service, the nodules actor (it holds
/// Admin on the drawer afterwards, being its creator), the drawer access policy it is granted
/// creation through, and the node's database.
pub struct SharedConfigDrawerConfig<'a> {
    pub drawers: &'a Drawers,
    pub actor: &'a OpenedActor,
    pub access: &'a DrawerAccess,
    pub sql: Database,
}

/// The shared config drawer's id, created on the first boot and read back from SQLite on every
/// later one (ADR 044 §1). It is the cartridge config documents' home: one document per
/// installed cartridge, holding its settings and grants. It is ADR 038's configuration drawer,
/// local until sister nodes exist; its sync policy is theirs to decide, and nothing here depends
/// on it. The nodules actor creates it, so it holds Admin on it.
///
/// The nodules actor holds the right to create drawers on every boot, not only while it makes this
/// one: it is the delegator every routine's drawer handle is minted under, so a routine whose
/// `uses` takes `create` can be minted it only while the actor holds it (ADR 049 §6.1).
/// Least privilege applies to the routines' handles, which are `uses` ∩ grants, not to the actor
/// that mints them. Granting is idempotent, so a later boot re-asserts it at no cost.
///
/// A crash between the drawer's creation and the stored id replays the creation under its fixed
/// command, which the drawer ledger answers with the same drawer while its row is retained; past
/// that, the first drawer is leaked as an unreferenced empty drawer and a second one made.
pub async fn ensure_shared_config_drawer(
    config: SharedConfigDrawerConfig<'_>,
) -> Result<DrawerId, NodulesError> {
    store::migrate(&config.sql).await?;
    let storage = |error: &dyn std::fmt::Display| NodulesError::Storage(error.to_string());
    let context = RequestContext::root();
    config
        .access
        .grants()
        .grant(
            &context,
            config.actor.actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .map_err(|error| storage(&error))?;
    if let Some(drawer) = store::shared_config_drawer(&config.sql).await? {
        return Ok(drawer);
    }
    let create = config
        .drawers
        .client
        .call_with(
            context.child(),
            Authorization::identity(config.actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::new(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .map_err(|error| storage(&error))?;
    let drawer = config
        .drawers
        .client
        .call(
            context.child(),
            CreateDrawer {
                capability: create.capability_id,
                command: CommandId::try_from("shared config drawer".to_owned())
                    .expect("the shared config drawer command is within bounds"),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .map_err(|error| storage(&error))?
        .drawer_id;
    store::set_shared_config_drawer(&config.sql, &drawer).await?;
    Ok(drawer)
}
