//! The one adapter from a hosted service to a built-in provider (ADR 033 §4).
//!
//! A service is reached through `Client<Proto>`, whose calls name the operation by its request
//! type. Dispatch names it by NSID and hands over JSON, so [`ServiceProvider`] holds one typed
//! route per unary operation of every protocol mounted on it: the route decodes the JSON as the
//! operation's request, calls the client and encodes the output. The routes come from
//! [`Protocol::visit_unary`], so they follow the protocol declaration and cannot drift from it.
//!
//! The adapter is not an authorization boundary. Dispatch admission has already run when
//! [`Provider::call`] is entered. The credential dispatch admitted ([`Caller::authorization`])
//! is handed on as the request's `Authorization: Bearer` metadata, the way an XRPC server hands
//! it to any service, and the service checks it itself: an identity (the operator) satisfies
//! both layers, while a dispatch-minted handle is not understood by a service's domain, which
//! admits only its own handles. Resource capabilities (`capability` fields) stay in the input.

use crate::Caller;
use crate::LexiconSource;
use crate::Operation;
use crate::Provider;
use crate::ProviderError;
use crate::ProviderKind;
use crate::ProviderState;
use crate::Router;
use async_trait::async_trait;
use big_cabinet_core::datum::operation::OperationKind;
use big_dispatch_core::InvocationContext;
use futures::FutureExt;
use futures::future::BoxFuture;
use method_core::MethodKind;
use serde_json::Value;
use serde_json::json;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::HttpMetadata;
use service_utils_rs::Protocol;
use service_utils_rs::RequestContext;
use service_utils_rs::UnaryVisitor;
use service_utils_rs::xrpc::DOMAIN_ERROR_STATUS;
use service_utils_rs::xrpc::domain_error_envelope;
use std::collections::BTreeMap;
use std::sync::Arc;

/// HTTP status a failure to reach the service is reported with, as a gateway reports its
/// upstream being unreachable.
const BAD_GATEWAY: u16 = 502;

type RouteFn = dyn Fn(RequestContext, HttpMetadata, Value) -> BoxFuture<'static, Result<Value, ProviderError>>
    + Send
    + Sync;

struct Route {
    kind: OperationKind,
    privileged: bool,
    call: Box<RouteFn>,
}

/// A built-in [`Provider`] serving the unary operations of the protocols mounted on it.
#[derive(Default)]
pub struct ServiceProvider {
    routes: BTreeMap<&'static str, Route>,
}

struct Mount<'a, Proto: irpc::Service> {
    client: &'a Client<Proto>,
    routes: &'a mut BTreeMap<&'static str, Route>,
}

impl<Proto> UnaryVisitor<Proto> for Mount<'_, Proto>
where
    Proto: irpc::Service + Send + Sync + 'static,
{
    fn visit<P>(&mut self)
    where
        P: method_core::Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<irpc::WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: irpc::Channels<
                Proto,
                Tx = service_utils_rs::Reply<P>,
                Rx = irpc::channel::none::NoReceiver,
            >,
    {
        let operation = P::XRPC;
        let kind = match operation.kind {
            MethodKind::Query => OperationKind::Query,
            MethodKind::Procedure => OperationKind::Procedure,
            MethodKind::Subscription => unreachable!("a unary operation is never a subscription"),
        };
        let client = self.client.clone();
        let call: Box<RouteFn> = Box::new(move |context, metadata, input| {
            let client = client.clone();
            async move {
                let params: P = serde_json::from_value(input)
                    .map_err(|error| ProviderError::InvalidInput(error.to_string()))?;
                match client
                    .call_with_metadata(Envelope::with_metadata(context, metadata, params))
                    .await
                {
                    Ok(response) => Ok(serde_json::to_value(response.output)
                        .expect("operation output must serialize")),
                    // The operation's own error travels in the standard XRPC domain envelope,
                    // as it does when the same operation is served over HTTP.
                    Err(ClientError::Method(error)) => Err(ProviderError::Failed {
                        status: DOMAIN_ERROR_STATUS.as_u16(),
                        body: serde_json::to_value(domain_error_envelope(&error))
                            .expect("an error envelope serializes"),
                    }),
                    Err(ClientError::Transport { message, .. }) => Err(ProviderError::Failed {
                        status: BAD_GATEWAY,
                        body: json!({ "error": "BadGateway", "message": message }),
                    }),
                }
            }
            .boxed()
        });
        let previous = self.routes.insert(
            operation.nsid,
            Route {
                kind,
                privileged: operation.privileged,
                call,
            },
        );
        assert!(
            previous.is_none(),
            "operation {} is declared twice in one provider",
            operation.nsid
        );
    }
}

impl ServiceProvider {
    /// Serves every unary operation of `Proto` through `client`. Subscriptions are not routed:
    /// dispatch exchanges one JSON value each way. An NSID served twice is a programming error.
    pub fn serve<Proto>(mut self, client: &Client<Proto>) -> Self
    where
        Proto: Protocol + Send + Sync + 'static,
    {
        Proto::visit_unary(&mut Mount {
            client,
            routes: &mut self.routes,
        });
        self
    }

    /// Registers this provider in `router` as `provider_id`, ready, claiming every operation it
    /// serves with the Lexicon its method publishes. A served operation that has no Lexicon in
    /// [`method_lexicon::builtin_methods`], or one that does not compile, is a programming error
    /// and panics: a built-in that dispatch could not check would be served unchecked.
    pub fn register(self, router: &Router, provider_id: &str) {
        let lexicons: BTreeMap<_, _> = method_lexicon::builtin_methods().collect();
        let operations: Vec<_> = self
            .routes
            .iter()
            .map(|(nsid, route)| {
                let assemble = lexicons.get(nsid).unwrap_or_else(|| {
                    panic!("built-in operation {nsid} is missing from `method_lexicon::builtin_methods`")
                });
                let file = assemble().unwrap_or_else(|error| panic!("{error}"));
                (
                    (*nsid).to_owned(),
                    Operation {
                        kind: Some(route.kind),
                        privileged: route.privileged,
                        lexicon: Some(LexiconSource {
                            file,
                            carried: Vec::new(),
                        }),
                    },
                )
            })
            .collect();
        router
            .register(
                provider_id,
                operations,
                ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(self))),
            )
            .unwrap_or_else(|error| panic!("a built-in's Lexicon must compile: {error}"));
    }
}

#[async_trait]
impl Provider for ServiceProvider {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        request: &RequestContext,
        context: &InvocationContext,
        input: Value,
    ) -> Result<Value, ProviderError> {
        let route = self
            .routes
            .get(operation)
            .expect("the router only resolves NSIDs this provider registered");
        // Dispatch enforces the deadline around this call; the service sees it as its budget so
        // it can stop early, as it would for any caller with one.
        let mut service_request = RequestContext::root();
        if let Some(budget) = request.remaining_budget() {
            service_request = service_request.with_time_budget(budget);
        }
        let metadata = HttpMetadata {
            authorization: Some(caller.authorization.header_value()),
            idempotency_key: context.idempotency_key.clone(),
        };
        (route.call)(service_request, metadata, input).await
    }
}
