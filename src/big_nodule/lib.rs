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
pub mod executor;
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
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenedActor;
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
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgeInstalls;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_cartridges_core::protocol::ClearCartridgeConfig;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ObserveCartridgeInstalls;
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
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::LazyLock;

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
/// namespace it mints management handles in, the operator whose identity administers nodules,
/// the dispatch router it registers each admitted nodule's implemented operations in, the dispatch
/// client and dispatch handle it mints every nodule's own handle from (a
/// handle covering every authority but no privileged method), the optional executor that hosts their
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
    pub operator: Uuid7,
    pub dispatch: Arc<big_dispatch::Router>,
    pub dispatch_client: Client<DispatchProtocol>,
    pub dispatch_authority: CapId,
    /// The cartridges service that resolves a pinned version's payloads at install, and the
    /// handle presented to it: a read-only one (no `publish`) the operator introduced to the
    /// nodule service's own actor, whose capability is the `signer`.
    pub cartridges: Client<CartridgesProtocol>,
    pub cartridges_authorization: Authorization,
    pub executor: Option<Arc<dyn executor::NoduleExecutor>>,
    pub schemas: Arc<SchemaRepo>,
    /// Whether the dev takeover hook is on (`dev.allow_slot_takeover` in `node.json`, ADR 045 §5).
    /// Fixed at boot: with it off, `setNoduleDevTakeover` refuses to set one and a takeover datum
    /// found in the config document is not honored, so the slot is refused.
    pub allow_slot_takeover: bool,
    pub sql: Database,
}

/// The hosted surface: it settles who the caller is and what they may touch, then hands the
/// work to the state task that owns the reconciled inventory.
pub struct NoduleService {
    capabilities: DomainCapabilities,
    operator: Uuid7,
    state: state::Handle,
}

type Res<T> = std::result::Result<T, NodulesError>;

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

    /// What the caller may administer: everything for the operator's identity, the scope of a
    /// handle this domain minted, nothing for any other identity. Another actor is refused
    /// rather than looked up in a grant table: this domain has no entitlements beyond the
    /// handles it minted.
    fn scope_of<'a>(&self, caller: &'a Invocation<NoduleScope>) -> Res<Option<&'a NoduleScope>> {
        if !caller.rights.contains(Rights::USE) {
            return Err(NodulesError::Denied);
        }
        match &caller.authority {
            DomainAuthority::Identity(actor) if *actor == self.operator => Ok(None),
            DomainAuthority::Identity(_) => Err(NodulesError::Denied),
            DomainAuthority::Scoped(scope) => Ok(Some(scope)),
        }
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
        let caller = self.caller(context, authorization).await?;
        let scope = self.scope_of(&caller)?.cloned();
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
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetGrant {
                context,
                cartridge_id: params.cartridge_id,
                component: params.component,
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
        let caller = self.caller(context, authorization).await?;
        let scope = self.scope_of(&caller)?.cloned();
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

    /// Routes are node-wide, so only the operator's own identity may set them: a scoped handle
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
                    read: BTreeSet::from([config_doc.clone()]),
                    write: std::collections::BTreeMap::from([(
                        config_doc.clone(),
                        big_cabinet_core::doc::DatumKinds::All,
                    )]),
                    // The registry's documents are only read, at the heads installs pin. The
                    // shared config drawer holds the cartridge config documents, which are
                    // created, read and written.
                    drawers: BTreeMap::from([
                        (config.registry_drawer.clone(), Access::Read),
                        (config.shared_config_drawer.clone(), Access::Write),
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
        shared_config_drawer: config.shared_config_drawer,
        parent: config.actor,
        parent_path: config.actor_path,
        dispatch: config.dispatch,
        dispatch_client: config.dispatch_client,
        dispatch_authority: config.dispatch_authority,
        executor: config.executor,
        schemas: config.schemas,
        allow_slot_takeover: config.allow_slot_takeover,
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
        operator: config.operator,
        state,
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(NodulesBinding::new(Arc::clone(&host))));
    Ok((
        Nodules { host, client },
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
/// A crash between the drawer's creation and the stored id replays the creation under its fixed
/// command, which the drawer ledger answers with the same drawer while its row is retained; past
/// that, the first drawer is leaked as an unreferenced empty drawer and a second one made.
pub async fn ensure_shared_config_drawer(
    config: SharedConfigDrawerConfig<'_>,
) -> Result<DrawerId, NodulesError> {
    store::migrate(&config.sql).await?;
    if let Some(drawer) = store::shared_config_drawer(&config.sql).await? {
        return Ok(drawer);
    }
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
    // Creating the one shared config drawer is the only use of this right, so it is revoked once
    // the drawer is recorded rather than left standing (least privilege). Admin on the drawer
    // comes from being its creator and is unaffected.
    config
        .access
        .grants()
        .revoke(
            &context,
            config.actor.actor.actor_id,
            &[CREATE_DRAWER_OBJECT.into()],
        )
        .await
        .map_err(|error| storage(&error))?;
    Ok(drawer)
}
