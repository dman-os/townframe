use crate::wasi::WasiHttp;
use method_core::ContextError;
use method_core::Unary;
use service_client_core_rs::UnaryTransport;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::ClientError;
use service_core_rs::Envelope;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::xrpc::NODULE_CAPABILITY_HEADER;
use service_core_rs::xrpc::bearer_header_value;
use service_core_rs::xrpc::bearer_token;
use service_core_rs::xrpc::request_context;

/// What a handler knows about the call it is serving, built from the headers the host delivered,
/// and the way it calls other methods.
#[derive(Debug, Clone)]
pub struct Cx {
    pub(crate) request: RequestContext,
    pub(crate) caller: Option<String>,
    pub(crate) nodule: Option<String>,
    pub(crate) idempotency_key: Option<String>,
}

impl Cx {
    /// Parses the delivered headers. A malformed header is the caller's mistake, reported to it
    /// as an `InvalidRequest`.
    pub fn from_headers(headers: &http::HeaderMap) -> Result<Self, String> {
        let (request, metadata) = request_context(headers)?;
        let nodule = headers
            .get(NODULE_CAPABILITY_HEADER)
            .map(|value| {
                value.to_str().map(str::to_owned).map_err(|_| {
                    format!("{NODULE_CAPABILITY_HEADER} must be valid UTF-8 HTTP text")
                })
            })
            .transpose()?;
        Ok(Self {
            request,
            caller: metadata
                .authorization
                .as_deref()
                .and_then(bearer_token)
                .map(str::to_owned),
            nodule,
            idempotency_key: metadata.idempotency_key,
        })
    }

    /// This hop's context: the request id and trace the host delivered, and the budget it
    /// still had then (`remaining_budget` reads the clock now).
    pub fn request(&self) -> &RequestContext {
        &self.request
    }

    /// The capability id the caller presented (`authorization: Bearer <id>`). Present it on the
    /// calls made on the caller's behalf rather than the nodule's own, so the nodule is not a
    /// confused deputy.
    pub fn caller_capability(&self) -> Option<&str> {
        self.caller.as_deref()
    }

    /// The nodule's own dispatch capability (`x-nodule-capability`), for its private state.
    /// Absent when the nodule was granted nothing.
    pub fn nodule_capability(&self) -> Option<&str> {
        self.nodule.as_deref()
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// The context of a call made from here: a new hop of this call's chain (fresh request id,
    /// child span of the same trace) whose budget is what remains now, not at receipt.
    pub fn next_hop(&self) -> RequestContext {
        self.request.next_hop()
    }

    /// The `wasi:http/client` transport to `dispatch.local`, for a call that is not a typed
    /// method (a dynamic NSID, or presenting another capability).
    pub fn transport(&self) -> WasiHttp {
        WasiHttp
    }

    /// Calls `M` through dispatch with the nodule's own capability, as the next hop of this
    /// call. Without a capability of its own (nothing granted) there is no credential to
    /// present, which is [`ContextError::Unauthenticated`].
    pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, ClientError<M::Error>> {
        call_through(WasiHttp, self, input).await
    }
}

/// [`Cx::call`] over any transport, which is how it is tested off wasm.
pub(crate) async fn call_through<H: HttpTransport, M: Unary>(
    transport: H,
    cx: &Cx,
    input: M,
) -> Result<M::Output, ClientError<M::Error>> {
    let Some(capability) = cx.nodule_capability() else {
        return Err(ContextError::Unauthenticated.into());
    };
    let metadata = HttpMetadata {
        authorization: Some(bearer_header_value(capability)),
        idempotency_key: None,
    };
    // `Client::call` would also race the call against `RequestContext::expired`, a tokio timer,
    // and a guest has no tokio runtime to run one: it panics as soon as a budget is set. The
    // spent-budget check is `validate`; the in-flight deadline is the host's, which parses the
    // `x-timeout-ms` this hop writes and cancels the nested call when it runs out.
    let context = cx.next_hop();
    context.validate()?;
    Xrpc(transport)
        .call(Envelope::with_metadata(context, metadata, input))
        .await
        .map(|response| response.output)
}
