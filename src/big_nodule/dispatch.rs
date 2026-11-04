//! The dispatch service: the nodule runtime's XRPC reverse proxy (ADR 027 §3, §6), hosted on its
//! own beside the nodule service. It owns an operations namespace in the authority service and
//! knows nothing about nodules: whoever owns providers (the nodule reconciler, native code)
//! registers them in the [`Router`] it is handed.
//!
//! One call: (1) the presented [`Authorization`] is verified and its scope must cover the NSID,
//! (2) the NSID resolves to a provider that is ready, (3) the provider is called under the
//! caller's deadline. Nothing is persisted and the routing lookup touches no database or disk.

mod router;

pub use router::Caller;
pub use router::Provider;
pub use router::ProviderEntry;
pub use router::ProviderError;
pub use router::ProviderKind;
pub use router::ProviderState;
pub use router::Router;

use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet::capability::Invocation;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use big_nodule_core::dispatch::DispatchError;
use big_nodule_core::dispatch::DispatchMessage;
use big_nodule_core::dispatch::DispatchProtocol;
use big_nodule_core::dispatch::DispatchScope;
use big_nodule_core::dispatch::Invoke;
use big_nodule_core::dispatch::InvokeOutput;
use big_nodule_core::dispatch::MintDispatchCapability;
use big_nodule_core::dispatch::NotReadyReason;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// The prefix of every capability object this domain mints; the scope follows it as JSON.
const SCOPE_PREFIX: &str = "DispatchService/scope/v1/";

/// What the embedder hands the service: the authority it verifies and mints capabilities
/// through, the operations namespace it mints in, and the operator whose identity may invoke
/// anything.
pub struct DispatchConfig {
    pub authority: Client<AuthorityProtocol>,
    pub namespace: Namespace,
    pub operator: Uuid7,
}

pub struct DispatchService {
    capabilities: DomainCapabilities,
    operator: Uuid7,
    router: Arc<Router>,
}

type Res<T> = std::result::Result<T, DispatchError>;

fn authority_error(error: ClientError<AuthorityServiceError>) -> DispatchError {
    match error {
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DispatchError::Denied,
        ClientError::Operation(error) => DispatchError::Authority(error),
        ClientError::Transport { message, .. } => DispatchError::AuthorityUnavailable(message),
    }
}

/// Time left until a wire deadline, saturating at zero.
fn until(deadline_unix_ms: u64) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before UNIX epoch");
    Duration::from_millis(deadline_unix_ms).saturating_sub(now)
}

impl DispatchService {
    /// Verifies `authorization` through the authority service, the same path the nodule service
    /// uses. Cost per call: `ResolveAuthority` + `ResolveActor` against the authority's
    /// in-process ledger for an identity; a scoped handle adds one more `ResolveActor` for the
    /// signer it was bound to. Each `ResolveActor` (and an identity's `ResolveAuthority`)
    /// currently reads the actor's registry row on the authority's reader pool.
    async fn caller(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<Invocation<DispatchScope>> {
        let caller = self
            .capabilities
            .source::<DispatchScope>(context, authorization)
            .await
            .map_err(authority_error)?;
        if !caller.rights.contains(Rights::USE) {
            return Err(DispatchError::Denied);
        }
        Ok(caller)
    }

    /// What the caller may invoke: everything for the operator's identity, the scope of a handle
    /// this domain minted, nothing for any other identity (this domain keeps no grant table).
    fn scope_of<'a>(
        &self,
        caller: &'a Invocation<DispatchScope>,
    ) -> Res<Option<&'a DispatchScope>> {
        match &caller.authority {
            DomainAuthority::Identity(actor) if *actor == self.operator => Ok(None),
            DomainAuthority::Identity(_) => Err(DispatchError::Denied),
            DomainAuthority::Scoped(scope) => Ok(Some(scope)),
        }
    }

    async fn invoke(&self, context: &RequestContext, params: Invoke) -> Res<InvokeOutput> {
        let verified = self.caller(context, params.authorization).await?;
        let scope = self.scope_of(&verified)?;
        // Denied is settled before the lookup so an unauthorized caller cannot probe which
        // operations exist.
        if let Some(scope) = scope
            && !scope.permits(&params.operation)
        {
            return Err(DispatchError::Denied);
        }
        let caller = Caller {
            authorization: params.authorization,
            actor: verified.signer.actor_id,
            scope: scope.cloned(),
        };
        let entry = self.router.resolve(&params.operation)?;
        let kind = match &entry.state {
            ProviderState::Ready(kind) => kind,
            ProviderState::NotReady(readiness) => {
                return Err(DispatchError::NotReady {
                    reason: NotReadyReason::Provider(*readiness),
                });
            }
        };
        // The tighter of the caller's request budget and the invocation's own deadline. An
        // elapsed budget still polls the provider once before expiring, which tokio's timeout
        // guarantees.
        let budget = [
            context.remaining_budget(),
            params.context.deadline_unix_ms.map(until),
        ]
        .into_iter()
        .flatten()
        .min();
        let ProviderKind::BuiltIn(provider) = kind;
        let call = provider.call(&caller, &params.context, params.input);
        let result = match budget {
            Some(budget) => tokio::time::timeout(budget, call)
                .await
                .map_err(|_| DispatchError::DeadlineExceeded)?,
            None => call.await,
        };
        match result {
            Ok(output) => Ok(InvokeOutput { output }),
            Err(ProviderError::InvalidInput(reason)) => Err(DispatchError::InvalidInput(reason)),
            Err(ProviderError::Failed { status, body }) => {
                Err(DispatchError::Provider { status, body })
            }
        }
    }

    async fn mint(
        &self,
        context: &RequestContext,
        params: MintDispatchCapability,
    ) -> Res<Capability> {
        let caller = self.caller(context, params.authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(DispatchError::Denied);
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
}

macro_rules! handlers {
    ($($request:ident => $method:ident : $output:ty;)*) => {$(
        impl Handler<$request> for DispatchService {
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
    Invoke => invoke: InvokeOutput;
    MintDispatchCapability => mint: Capability;
}

service_utils_rs::bind_service_protocol! {
    pub struct DispatchBinding for DispatchProtocol => DispatchMessage {
        unary {
            Invoke(Invoke),
            MintDispatchCapability(MintDispatchCapability)
        }
        stream {}
    }
}

pub type DispatchHandler = Arc<DispatchService>;

/// A booted dispatch service: the host that fronts it, the client its callers use, and the
/// router providers register in.
pub struct Dispatch {
    pub host: Arc<Host<DispatchHandler>>,
    pub client: Client<DispatchProtocol>,
    pub router: Arc<Router>,
}

/// Host the dispatch service over an empty routing table. The returned token retires the
/// service's namespace after its host has drained.
pub fn boot(config: DispatchConfig) -> (Dispatch, StopToken) {
    let capabilities = DomainCapabilities::new(config.authority, config.namespace, SCOPE_PREFIX);
    let domain_stop = capabilities.stop_token();
    let router = Arc::new(Router::default());
    let service = Arc::new(DispatchService {
        capabilities,
        operator: config.operator,
        router: Arc::clone(&router),
    });
    let (host, host_stop) = Host::new(service);
    let client = Client::direct(DispatchBinding::new(Arc::clone(&host)));
    (
        Dispatch {
            host,
            client,
            router,
        },
        host_stop.then(domain_stop),
    )
}
