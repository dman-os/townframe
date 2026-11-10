//! The dispatch service: the node's XRPC reverse proxy (ADR 027 §3, §6; ADR 033 §4), hosted on
//! its own beside the services it fronts. It owns an operations namespace in the authority
//! service and knows nothing about nodules or any other provider: whoever owns providers (the
//! nodule reconciler, the node's built-in services through [`ServiceProvider`]) registers them in
//! the [`Router`] it is handed.
//!
//! One call: (1) the caller's [`Authorization`] (the request's `Authorization: Bearer` metadata) is verified and its scope must cover the NSID,
//! (2) the NSID resolves to a provider that is ready, (3) the provider is called under the
//! caller's deadline. Nothing is persisted and the routing lookup touches no database or disk.

mod router;
mod service;
mod xrpc;

pub use router::Caller;
pub use router::Contract;
pub use router::ContractError;
pub use router::LexiconSource;
pub use router::Operation;
pub use router::Provider;
pub use router::ProviderEntry;
pub use router::ProviderError;
pub use router::ProviderKind;
pub use router::ProviderState;
pub use router::Router;
pub use service::ServiceProvider;
pub use xrpc::Anonymous;
pub use xrpc::xrpc_router;

use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet::capability::Invocation;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::dmeta::Uuid7;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchMessage;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::HttpForm;
use big_dispatch_core::Invoke;
use big_dispatch_core::InvokeOutput;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::NotReadyReason;
use method_core::ContextError;
use method_lexicon::MethodContract;
use serde_json::Value;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use std::sync::Arc;
use std::time::Duration;

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
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => DispatchError::Denied,
        ClientError::Method(error) => DispatchError::Authority(error),
        ClientError::Transport { message, .. } => DispatchError::AuthorityUnavailable(message),
    }
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

    async fn invoke(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: Invoke,
    ) -> Res<InvokeOutput> {
        // The caller's request budget. Like gRPC, which answers `DEADLINE_EXCEEDED` for a call whose deadline has already passed
        // without running the handler, an elapsed budget fails here, before admission, lookup
        // and readiness: no admission work is spent on it and the answer reveals nothing about
        // which operations exist.
        let budget = context.remaining_budget();
        if budget == Some(Duration::ZERO) {
            return Err(ContextError::DeadlineExceeded.into());
        }
        let verified = self.caller(context, authorization).await?;
        let scope = self.scope_of(&verified)?;
        // A handle is its holder's: it was minted bound to one actor and only that actor may
        // invoke through it. Presenting another actor's handle under one's own signer is the
        // forgery this refuses, and it is what lets the Wasm egress hand a guest nothing but the
        // handle id: the signer is always the nodule's own actor there.
        if verified
            .bound_signer
            .is_some_and(|holder| holder != verified.signer.actor_id)
        {
            return Err(DispatchError::Denied);
        }
        // Denied is settled before the lookup so an unauthorized caller cannot probe which
        // operations exist.
        if let Some(scope) = scope
            && !scope.permits(&params.operation)
        {
            return Err(DispatchError::Denied);
        }
        let caller = Caller {
            authorization,
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
        let contract = entry.contract.as_deref().map(Contract::compiled);
        let input = admit_input(&params, entry.operation_kind, contract)?;
        let call =
            kind.provider()
                .call(&caller, &params.operation, context, &params.context, input);
        let result = match budget {
            Some(budget) => tokio::time::timeout(budget, call)
                .await
                .map_err(|_| DispatchError::from(ContextError::DeadlineExceeded))?,
            None => call.await,
        };
        match result {
            Ok(output) => {
                // atproto's xrpc-server validates every response against the method's Lexicon
                // (unless `validateResponse` is turned off) and fails the call with
                // `InternalServerError` (500) when the handler's output departs from it: the
                // handler is the one in the wrong, not the caller. Here that is a provider bug,
                // so it is logged at error as well as answered.
                if let Some(contract) = contract
                    && let Err(violations) = contract.check_output(&output)
                {
                    let violations = violations.join("; ");
                    tracing::error!(
                        operation = %params.operation,
                        provider = %entry.provider_id,
                        %violations,
                        "a provider answered off its method's Lexicon"
                    );
                    return Err(DispatchError::InvalidOutput(format!(
                        "{} answered off its Lexicon: {violations}",
                        params.operation
                    )));
                }
                Ok(InvokeOutput { output })
            }
            Err(ProviderError::InvalidInput(reason)) => Err(DispatchError::InvalidInput(reason)),
            Err(ProviderError::Failed { status, body }) => {
                Err(DispatchError::Provider { status, body })
            }
        }
    }

    async fn mint(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: MintDispatchCapability,
    ) -> Res<Capability> {
        let caller = self.caller(context, authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(DispatchError::Denied);
        }
        self.capabilities
            .mint(
                context,
                authorization.authority,
                &caller,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)
    }
}

/// The input a provider is called with. An HTTP call is first held to the method's kind, as
/// atproto holds it (`@atproto/xrpc-server` answers `InvalidRequest`, "Incorrect HTTP method
/// (POST) expected GET", for the wrong verb): a GET names a query and builds its input from the
/// query string through the Lexicon's `parameters` (undeclared names ignored, defaults applied),
/// a POST names a procedure and its body is the input. Then the input must conform to the
/// Lexicon's `input` (a query's `parameters`): atproto's `InvalidRequest`, 400. A method with no
/// Lexicon here (a foreign contract no Lexicon states, a query like `getNodeInfo` that has none)
/// is passed through as given, with a GET's input empty.
fn admit_input(
    params: &Invoke,
    kind: Option<OperationKind>,
    contract: Option<&MethodContract>,
) -> Res<Value> {
    let refuse = |message: String| Err(DispatchError::InvalidInput(message));
    let is_query = kind == Some(OperationKind::Query);
    let input = match &params.context.http {
        Some(HttpForm::Get { .. }) if !is_query => {
            return refuse("Incorrect HTTP method (GET) expected POST".to_owned());
        }
        Some(HttpForm::Post) if is_query => {
            return refuse("Incorrect HTTP method (POST) expected GET".to_owned());
        }
        Some(HttpForm::Get { query }) => {
            let query: Vec<(String, String)> =
                serde_urlencoded::from_str(query).map_err(|error| {
                    DispatchError::InvalidInput(format!("invalid query string: {error}"))
                })?;
            match contract {
                None => Value::Object(Default::default()),
                Some(contract) => {
                    let query: Vec<(&str, &str)> = query
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_str()))
                        .collect();
                    Value::Object(contract.coerce_query(&query).map_err(|errors| {
                        DispatchError::InvalidInput(
                            errors
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("; "),
                        )
                    })?)
                }
            }
        }
        Some(HttpForm::Post) | None => params.input.clone(),
    };
    if let Some(contract) = contract
        && let Err(violations) = contract.check_request(&input)
    {
        return refuse(format!(
            "the input does not match the method's Lexicon: {}",
            violations.join("; ")
        ));
    }
    Ok(input)
}

macro_rules! handlers {
    ($($request:ident => $method:ident : $output:ty;)*) => {$(
        impl Handler<$request> for DispatchService {
            async fn call(
                &self,
                request: Envelope<$request>,
            ) -> Res<ResponseEnvelope<$output>> {
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
    let client = Client::new(Transport::direct(DispatchBinding::new(Arc::clone(&host))));
    (
        Dispatch {
            host,
            client,
            router,
        },
        host_stop.then(domain_stop),
    )
}

#[cfg(test)]
mod e2e;
