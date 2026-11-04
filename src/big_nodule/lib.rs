//! The nodule service: the node-side runtime management surface of ADR 027 §4, §6, and §8,
//! without an executor.
//!
//! Portable desired state (installed envelopes, enablement, settings, and local drawer grants)
//! lives in the node's config drawer document and is projected through
//! [`big_cabinet::DatumStoreHandle`] over [`big_nodule_core::config::NoduleConfig`].
//! Host-specific state (each nodule's durable local actor, the service-owned drawer grant
//! capabilities and the dispatch handles minted from operation grants applied on this node)
//! lives in SQLite.

#![recursion_limit = "256"]

use sqlite_rs as sqlite_rs_alias;

mod sqlite {
    include!("sqlite/gen.rs");
}
pub mod executor;
mod state;
mod store;

#[cfg(test)]
mod e2e;

use big_cabinet::DatumStoreHandle;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::SchemaRepo;
use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet::capability::Invocation;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::Universe;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_dispatch_core::DispatchProtocol;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::config::config_universe;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::ConfigureNodule;
use big_nodule_core::protocol::ConfirmNoduleResolution;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::MintNoduleCapability;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::NodulesMessage;
use big_nodule_core::protocol::NodulesProtocol;
use big_nodule_core::protocol::SetNoduleDrawerGrants;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleOperationGrants;
use big_nodule_core::protocol::SetNoduleSubstitution;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::PlugId;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use state::Command;
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
/// drawers services it reads config from and applies grants through, the node's config drawer,
/// the subsystem actor it provisions nodule actors under and that actor's registry path, the
/// namespace it mints management handles in, the operator whose identity administers nodules,
/// the dispatch router it registers each admitted nodule's implemented operations in, the dispatch
/// client and operator-granted dispatch handle it mints every nodule's own handle from (the
/// ceiling of what any nodule may be granted to invoke), the optional executor that hosts their
/// handlers, the node's schema repo it registers each enabled nodule's datum Lexicons in, and its
/// database. Without an executor a handler is never
/// ready: its operations are registered not ready with the nodule's own readiness.
pub struct NodulesConfig {
    pub authority: Client<AuthorityProtocol>,
    pub docs: Client<DocsProtocol>,
    pub drawers: Client<DrawersProtocol>,
    pub config_drawer: DrawerId,
    pub actor: Actor,
    pub actor_path: String,
    pub namespace: Namespace,
    pub operator: Uuid7,
    pub dispatch: Arc<big_dispatch::Router>,
    pub dispatch_client: Client<DispatchProtocol>,
    pub dispatch_authority: CapId,
    pub executor: Option<Arc<dyn executor::NoduleExecutor>>,
    pub schemas: Arc<SchemaRepo>,
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
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => NodulesError::Denied,
        ClientError::Operation(error) => NodulesError::Authority(error),
        ClientError::Transport { message, .. } => NodulesError::AuthorityUnavailable(message),
    }
}

pub(crate) fn docs_error(error: ClientError<DocsError>) -> NodulesError {
    match error {
        ClientError::Operation(DocsError::Context(error)) => error.into(),
        ClientError::Operation(DocsError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Operation(DocsError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        ClientError::Operation(DocsError::Denied) => NodulesError::Denied,
        ClientError::Operation(other) => NodulesError::Storage(other.to_string()),
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

    /// A scoped handle admits only the plug ids in its `plugs`, and only with `admit` set.
    fn require_admit(&self, caller: &Invocation<NoduleScope>, plug_id: &PlugId) -> Res<()> {
        match self.scope_of(caller)? {
            Some(scope) if !(scope.admit && scope.plugs.contains(plug_id)) => {
                Err(NodulesError::Denied)
            }
            _ => Ok(()),
        }
    }

    fn require_manage(&self, caller: &Invocation<NoduleScope>, plug_id: &PlugId) -> Res<()> {
        match self.scope_of(caller)? {
            Some(scope) if !scope.plugs.contains(plug_id) => Err(NodulesError::Denied),
            _ => Ok(()),
        }
    }

    async fn mint(
        &self,
        context: &RequestContext,
        params: MintNoduleCapability,
    ) -> Res<Capability> {
        let caller = self.caller(context, params.authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(NodulesError::Denied);
        }
        self.capabilities
            .mint(
                context,
                params.authorization.authority,
                &caller,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)
    }

    async fn admit(&self, context: &RequestContext, params: AdmitNodule) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_admit(&caller, &params.plug.id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Admit {
                context,
                plug: Box::new(params.plug),
                drawers: params.drawers,
                reply,
            })
            .await
    }

    async fn configure(
        &self,
        context: &RequestContext,
        params: ConfigureNodule,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Configure {
                context,
                plug_id: params.plug_id,
                settings: params.settings,
                reply,
            })
            .await
    }

    async fn set_enabled(
        &self,
        context: &RequestContext,
        params: SetNoduleEnabled,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetEnabled {
                context,
                plug_id: params.plug_id,
                enabled: params.enabled,
                reply,
            })
            .await
    }

    async fn confirm_resolution(
        &self,
        context: &RequestContext,
        params: ConfirmNoduleResolution,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::ConfirmResolution {
                context,
                plug_id: params.plug_id,
                binding: params.binding,
                reply,
            })
            .await
    }

    async fn set_substitution(
        &self,
        context: &RequestContext,
        params: SetNoduleSubstitution,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        if let Some(by) = &params.by {
            self.require_manage(&caller, by)?;
        }
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetSubstitution {
                context,
                plug_id: params.plug_id,
                by: params.by,
                reply,
            })
            .await
    }

    async fn set_drawer_grants(
        &self,
        context: &RequestContext,
        params: SetNoduleDrawerGrants,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetDrawerGrants {
                context,
                plug_id: params.plug_id,
                drawers: params.drawers,
                reply,
            })
            .await
    }

    async fn set_operation_grants(
        &self,
        context: &RequestContext,
        params: SetNoduleOperationGrants,
    ) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::SetOperationGrants {
                context,
                plug_id: params.plug_id,
                operations: params.operations,
                reply,
            })
            .await
    }

    async fn uninstall(&self, context: &RequestContext, params: UninstallNodule) -> Res<()> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Uninstall {
                context,
                plug_id: params.plug_id,
                reply,
            })
            .await
    }

    async fn get(&self, context: &RequestContext, params: GetNodule) -> Res<NoduleRecord> {
        let caller = self.caller(context, params.authorization).await?;
        self.require_manage(&caller, &params.plug_id)?;
        let context = context.clone();
        self.state
            .ask(|reply| Command::Get {
                context,
                plug_id: params.plug_id,
                reply,
            })
            .await
    }

    async fn list(&self, context: &RequestContext, params: ListNodules) -> Res<Vec<NoduleRecord>> {
        let caller = self.caller(context, params.authorization).await?;
        let scope = self.scope_of(&caller)?.cloned();
        let context = context.clone();
        self.state
            .ask(|reply| Command::List {
                context,
                scope,
                reply,
            })
            .await
    }
}

macro_rules! handlers {
    ($($request:ident => $method:ident : $output:ty;)*) => {$(
        impl Handler<$request> for NoduleService {
            async fn call(
                &self,
                request: Envelope<$request>,
            ) -> Res<ResponseEnvelope<$output>> {
                Ok(self.$method(&request.context, request.params).await?.into())
            }
        }
    )*};
}
handlers! {
    MintNoduleCapability => mint: Capability;
    AdmitNodule => admit: NoduleRecord;
    ConfigureNodule => configure: NoduleRecord;
    SetNoduleEnabled => set_enabled: NoduleRecord;
    SetNoduleDrawerGrants => set_drawer_grants: NoduleRecord;
    SetNoduleOperationGrants => set_operation_grants: NoduleRecord;
    ConfirmNoduleResolution => confirm_resolution: NoduleRecord;
    SetNoduleSubstitution => set_substitution: NoduleRecord;
    UninstallNodule => uninstall: ();
    GetNodule => get: NoduleRecord;
    ListNodules => list: Vec<NoduleRecord>;
}

service_utils_rs::bind_service_protocol! {
    pub struct NodulesBinding for NodulesProtocol => NodulesMessage {
        unary {
            MintNoduleCapability(MintNoduleCapability),
            AdmitNodule(AdmitNodule),
            ConfigureNodule(ConfigureNodule),
            SetNoduleEnabled(SetNoduleEnabled),
            SetNoduleDrawerGrants(SetNoduleDrawerGrants),
            SetNoduleOperationGrants(SetNoduleOperationGrants),
            ConfirmNoduleResolution(ConfirmNoduleResolution),
            SetNoduleSubstitution(SetNoduleSubstitution),
            UninstallNodule(UninstallNodule),
            GetNodule(GetNodule),
            ListNodules(ListNodules)
        }
        stream {}
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
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(config.actor.capability),
                scope: DocScope {
                    read: BTreeSet::from([config_doc.clone()]),
                    write: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .map_err(docs_error)?;
    let (datum_store, store_stop) = DatumStoreHandle::<NoduleConfig>::load(
        &RequestContext::root(),
        config.docs,
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
        parent: config.actor,
        parent_path: config.actor_path,
        dispatch: config.dispatch,
        dispatch_client: config.dispatch_client,
        dispatch_authority: config.dispatch_authority,
        executor: config.executor,
        schemas: config.schemas,
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
    let client = Client::direct(NodulesBinding::new(Arc::clone(&host)));
    Ok((
        Nodules { host, client },
        host_stop
            .then(state_stop)
            .then(store_stop)
            .then(domain_stop),
    ))
}
