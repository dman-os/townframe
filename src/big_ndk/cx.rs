use method_core::ContextError;
use method_core::Unary;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::Xrpc;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_core_rs::ClientError;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::xrpc::NODULE_CAPABILITY_HEADER;
use service_core_rs::xrpc::NODULE_CONFIG_CAPABILITY_HEADER;
use service_core_rs::xrpc::NODULE_CONFIG_DOC_HEADER;
use service_core_rs::xrpc::bearer_header_value;
use service_core_rs::xrpc::bearer_token;
use service_core_rs::xrpc::request_context;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

/// Object-safe form of [`HttpTransport`], whose `impl Future` return cannot be a trait object.
trait LocalHttp {
    fn send(
        &self,
        request: XrpcRequest,
    ) -> Pin<Box<dyn Future<Output = Result<XrpcResponse, HttpTransportError>> + '_>>;
}

impl<H: HttpTransport> LocalHttp for H {
    fn send(
        &self,
        request: XrpcRequest,
    ) -> Pin<Box<dyn Future<Output = Result<XrpcResponse, HttpTransportError>> + '_>> {
        Box::pin(HttpTransport::send(self, request))
    }
}

/// The HTTP transport a [`Cx`] makes its outgoing calls over, chosen when the call is served: a
/// guest serves over [`crate::WasiHttp`] (`serve_wasi`), and a native test harness supplies one
/// that reaches dispatch the way the host's egress does. Cheap to clone, and not `Send`, like
/// the guest futures it carries.
#[derive(Clone)]
pub struct HttpHandle(Rc<dyn LocalHttp>);

impl HttpHandle {
    pub fn new(transport: impl HttpTransport + 'static) -> Self {
        Self(Rc::new(transport))
    }
}

impl HttpTransport for HttpHandle {
    fn send(
        &self,
        request: XrpcRequest,
    ) -> impl Future<Output = Result<XrpcResponse, HttpTransportError>> {
        self.0.send(request)
    }
}

/// What a handler knows about the call it is serving, built from the headers the host delivered,
/// and the way it calls other methods.
#[derive(Clone)]
pub struct Cx {
    pub(crate) request: RequestContext,
    pub(crate) caller: Option<String>,
    pub(crate) nodule: Option<String>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) config_doc: Option<String>,
    pub(crate) config_capability: Option<String>,
    pub(crate) transport: HttpHandle,
}

impl std::fmt::Debug for Cx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Cx")
            .field("request", &self.request)
            .field("caller", &self.caller)
            .field("nodule", &self.nodule)
            .field("idempotency_key", &self.idempotency_key)
            .finish_non_exhaustive()
    }
}

impl Cx {
    /// Parses the delivered headers, with outgoing calls going over `transport`. A malformed
    /// header is the caller's mistake, reported to it as an `InvalidRequest`.
    pub fn from_headers(headers: &http::HeaderMap, transport: HttpHandle) -> Result<Self, String> {
        let (request, metadata) = request_context(headers)?;
        let nodule = headers
            .get(NODULE_CAPABILITY_HEADER)
            .map(|value| {
                value.to_str().map(str::to_owned).map_err(|_| {
                    format!("{NODULE_CAPABILITY_HEADER} must be valid UTF-8 HTTP text")
                })
            })
            .transpose()?;
        let config_doc = headers
            .get(NODULE_CONFIG_DOC_HEADER)
            .map(|value| {
                let value = value.to_str().map_err(|_| {
                    format!("{NODULE_CONFIG_DOC_HEADER} must be valid UTF-8 HTTP text")
                })?;
                Ok::<_, String>(value.to_owned())
            })
            .transpose()?;
        let config_capability = headers
            .get(NODULE_CONFIG_CAPABILITY_HEADER)
            .map(|value| {
                let value = value.to_str().map_err(|_| {
                    format!("{NODULE_CONFIG_CAPABILITY_HEADER} must be valid UTF-8 HTTP text")
                })?;
                Ok::<_, String>(value.to_owned())
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
            config_doc,
            config_capability,
            transport,
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

    /// The cartridge config document delivered for this nodule.
    pub fn config_doc_id(&self) -> Option<&str> {
        self.config_doc.as_deref()
    }

    /// The capability scoped to this nodule's config document and declared settings.
    pub fn config_capability(&self) -> Option<&str> {
        self.config_capability.as_deref()
    }

    /// Reads the cartridge config document through the capability delivered with this request.
    pub async fn read_config(
        &self,
        datums: Option<BTreeSet<String>>,
    ) -> Result<Value, ClientError<Value>> {
        let (Some(doc_id), Some(capability), Some(_nodule)) = (
            &self.config_doc,
            &self.config_capability,
            self.nodule_capability(),
        ) else {
            return Err(ClientError::Method(
                serde_json::json!({ "error": "Denied" }),
            ));
        };
        let mut body = serde_json::json!({
            "capability": capability,
            "docId": doc_id,
            "at": { "at": "current" },
            "optimistic": false,
        });
        if let Some(datums) = datums {
            body["datums"] = serde_json::to_value(datums).expect("a set of strings serializes");
        }
        self.config_call("org.example.daybook.docs.readDatums", body)
            .await
    }

    /// Writes settings datums in the cartridge config document at its current heads.
    pub async fn write_config(
        &self,
        set: BTreeMap<String, Value>,
        remove: BTreeSet<String>,
    ) -> Result<Value, ClientError<Value>> {
        let (Some(doc_id), Some(capability), Some(_nodule)) = (
            &self.config_doc,
            &self.config_capability,
            self.nodule_capability(),
        ) else {
            return Err(ClientError::Method(
                serde_json::json!({ "error": "Denied" }),
            ));
        };
        self.config_call(
            "org.example.daybook.docs.writeDatums",
            serde_json::json!({
                "capability": capability,
                "docId": doc_id,
                "mode": { "mode": "atCurrent" },
                "set": set,
                "remove": remove,
            }),
        )
        .await
    }

    async fn config_call(&self, operation: &str, body: Value) -> Result<Value, ClientError<Value>> {
        let nodule = self
            .nodule_capability()
            .ok_or_else(|| ClientError::Method(serde_json::json!({ "error": "Denied" })))?;
        let context = self.next_hop();
        let mut headers = http::HeaderMap::new();
        service_client_core_rs::xrpc::write_context_headers(
            &mut headers,
            &context,
            &HttpMetadata {
                authorization: Some(bearer_header_value(nodule)),
                idempotency_key: None,
                range: None,
            },
        )
        .expect("context headers are valid HTTP");
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        let response = service_client_core_rs::xrpc::HttpTransport::send(
            &self.transport,
            XrpcRequest {
                method: http::Method::POST,
                path: format!("xrpc/{operation}"),
                headers,
                body: serde_json::to_vec(&body).expect("a JSON value serializes"),
            },
        )
        .await
        .map_err(|error| ClientError::Transport {
            message: error.message,
            outcome_unknown: true,
        })?;
        let value =
            serde_json::from_slice(&response.body).map_err(|error| ClientError::Transport {
                message: format!("invalid JSON response: {error}"),
                outcome_unknown: true,
            })?;
        if response.status.is_success() {
            Ok(value)
        } else {
            Err(ClientError::Method(value))
        }
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// The context of a call made from here: a new hop of this call's chain (fresh request id,
    /// child span of the same trace) whose budget is what remains now, not at receipt.
    pub fn next_hop(&self) -> RequestContext {
        self.request.next_hop()
    }

    /// The transport to `dispatch.local` this call was served with, for a call that is not a
    /// typed method (a dynamic NSID, or presenting another capability).
    pub fn transport(&self) -> HttpHandle {
        self.transport.clone()
    }

    /// Calls `M` through dispatch with the nodule's own capability, as the next hop of this
    /// call. Without a capability of its own (nothing granted) there is no credential to
    /// present, which is [`ContextError::Unauthenticated`].
    pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, ClientError<M::Error>> {
        let Some(capability) = self.nodule_capability() else {
            return Err(ContextError::Unauthenticated.into());
        };
        let metadata = HttpMetadata {
            authorization: Some(bearer_header_value(capability)),
            // The nested call is a different command from the one being served, so it does not
            // inherit the idempotency key: reusing it would make dispatch treat the two as one.
            idempotency_key: None,
            range: None,
        };
        Client::new(Xrpc(self.transport()))
            .call_with(self.next_hop(), metadata, input)
            .await
    }
}
