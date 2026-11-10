//! The Wasm executor for nodule handlers: the [`NoduleExecutor`] of kind `"wasm"`.
//!
//! One wash-runtime [`Host`] runs every nodule handler as a workload of its own: one
//! `wasi:http/handler@0.3.0` component per (plug, revision). A call enters the workload through
//! [`Ingress::dispatch_request`], the in-process entry point of our wash-runtime fork, so
//! nothing here is reachable over the network:
//!
//! - the ingress still binds a loopback listener (the host requires one) but its [`Router`]
//!   refuses every incoming request and declines every local-egress match;
//! - the only egress a workload has is `dispatch.local` (a nodule calling an operation through
//!   dispatch), served by [`Egress`]; [`Sealed::allow_outgoing_request`] refuses every other
//!   authority, because nodule network facilities are not granted.
//!
//! A nodule learns its own dispatch capability from the `x-nodule-capability` header of every
//! request it serves and presents it as `authorization` on its outgoing calls. Identity is not
//! the guest's to claim: [`Egress`] signs each call with the actor of the workload it came from.
//!
//! Handler bytes come from the in-process [`BlobService`]. A handler whose bytes are not in the
//! local store yet is `NotReady(ArtifactsUnavailable)`, and the blob service's availability
//! feed (the same one the blob workers follow) tells this executor when they arrive, which then
//! asks the state actor to `ensure` again. Nothing polls.

// The nested async futures of wash's `Host::workload_start` make the compiler's `Send` proof
// for `ensure` deeper than the default limit (rust-lang/rust#159228).
#![recursion_limit = "256"]

use async_trait::async_trait;
use big_blobs::BlobService;
use big_blobs_core::BlobId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::Readiness;
use big_nodule::executor::HandlerSpec;
use big_nodule::executor::Hosted;
use big_nodule::executor::NoduleExecutor;
use big_nodule::executor::Reensure;
use big_plugs_core::PlugId;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::Limited;
use serde_json::Value;
use serde_json::json;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use utils_rs::prelude::Res;
use utils_rs::prelude::ToEyre;
use wash_runtime::engine::Engine;
use wash_runtime::engine::workload::ResolvedWorkload;
use wash_runtime::host::Host;
use wash_runtime::host::HostApi;
use wash_runtime::host::HostBuilder;
use wash_runtime::host::allowed_hosts::AllowedHost;
use wash_runtime::host::http::HostHandler;
use wash_runtime::host::http::Ingress;
use wash_runtime::host::http::OutgoingHandler;
use wash_runtime::host::http::RequestIoFuture;
use wash_runtime::host::http::RouteError;
use wash_runtime::host::http::Router;
use wash_runtime::host::http::SendFuture;
use wash_runtime::types::Component;
use wash_runtime::types::Workload;
use wash_runtime::types::WorkloadStartRequest;
use wash_runtime::types::WorkloadStopRequest;
use wash_runtime::wit::WitInterface;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;

/// The provider type tag dispatch records for nodules this executor hosts.
const KIND: &str = "wasm";

/// The authority every request is addressed to. The workload is picked by id, not by `Host`
/// (a plug id like `did:key:z6Mk.../photos` is not a valid URI authority), so this is only what the
/// guest sees in its `Host` header.
const AUTHORITY: &str = "nodule.local";

/// Handler components larger than this are not loaded. The whole component is held in memory
/// while it compiles, and a handler is a nodule's logic, not its data (blobs carry data), so a
/// generous 64 MiB bounds a hostile or mistaken envelope without constraining real handlers.
const MAX_HANDLER_OCTETS: u64 = 64 * 1024 * 1024;

/// Message bodies larger than this fail the call, in both directions: a handler's response and a
/// guest's outgoing request into dispatch. Either is a JSON value held in memory (several times
/// its encoded size once parsed) and carried over a service call, so it is capped at 4 MiB;
/// anything bigger belongs in a blob that the operation passes or returns an id for.
const MAX_MESSAGE_OCTETS: usize = 4 * 1024 * 1024;

/// The authority a guest addresses dispatch at. It resolves nowhere: [`Egress`] answers it.
const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// The header every delivered request carries the nodule's own dispatch capability in, as
/// `Capability <id>`. Absent when the nodule is granted no operations.
const CAPABILITY_HEADER: &str = "x-nodule-capability";

/// The status a transport failure (workload gone, body failed, oversize response) is reported
/// under, as an HTTP gateway would.
const BAD_GATEWAY: u16 = 502;

/// Hosts nodule handlers as wash workloads. Built by [`boot`].
pub struct WasmExecutor {
    host: Arc<Host>,
    ingress: Arc<Ingress<Sealed, Egress>>,
    blobs: Arc<BlobService>,
    /// What `ensure` started, by plug. Never locked across an await: `ensure` and `retire` are
    /// only called from the nodule service's state actor, one at a time, so the check, start
    /// and insert in `ensure` cannot interleave with another call for the same plug.
    hosted: Mutex<BTreeMap<PlugId, Running>>,
    /// The signer of each running workload's outgoing calls, shared with [`Egress`].
    signers: Signers,
    /// The plugs waiting on a blob that was not in the store, to be woken on its arrival.
    arrivals: Arc<Mutex<BTreeMap<BlobId, BTreeMap<PlugId, Reensure>>>>,
}

struct Running {
    workload_id: String,
    handler: BlobId,
    provider: Arc<WasmProvider>,
}

/// Starts the wash host and the feed that wakes handlers whose bytes arrive late. Guests' calls
/// to `dispatch.local` are made through `dispatch`. The returned token stops the feed and then
/// the host, and must be stopped after the nodule service that calls the executor and before
/// the dispatch service (and the authority behind it), which in-flight guest calls still use.
pub async fn boot(
    blobs: Arc<BlobService>,
    dispatch: Client<DispatchProtocol>,
) -> Res<(Arc<WasmExecutor>, StopToken)> {
    let signers = Signers::default();
    let ingress = Arc::new(
        Ingress::builder(Sealed, "127.0.0.1:0".parse().expect("literal address"))
            .outgoing_handler(Egress {
                dispatch,
                signers: Arc::clone(&signers),
            })
            .build()
            .await
            .to_eyre()?,
    );
    let host = HostBuilder::new()
        .with_engine(Engine::builder().build().to_eyre()?)
        .with_friendly_name("nodules-wasm")
        .with_http_handler(Arc::clone(&ingress) as Arc<dyn HostHandler>)
        .build()
        .to_eyre()?
        .start()
        .await
        .to_eyre()?;
    let (stop, spawner) = StopToken::new();
    let arrivals = Arc::<Mutex<BTreeMap<_, BTreeMap<_, Reensure>>>>::default();

    // Stops the host once the executor is stopped. A task rather than a hook on the token
    // because `StopToken` joins tasks and a failing host stop must crash the node, not vanish.
    let stopper = spawner.clone();
    let stopping = Arc::clone(&host);
    spawner
        .spawn(async move {
            stopper.cancelled().await;
            stopping.stop().await.expect("stopping the wash host");
        })
        .expect("a fresh stop token accepts tasks");

    // The feed follows blobs announced after `head`, read before any `ensure` can run. A blob
    // announced at or before `head` is already in the store, and `ensure` registers its waiter
    // before it checks the store, so a blob is either seen there or announced after `head` and
    // replayed by the feed, however late this task opens it. Opening at `head` rather than 0
    // keeps boot from replaying the store's whole announcement history.
    let head = blobs.availability_revision().await?;
    let watcher = spawner.clone();
    let watching = Arc::clone(&blobs);
    let waiting = Arc::clone(&arrivals);
    spawner
        .spawn(async move {
            let mut feed = watching
                .open_availability(head)
                .await
                .expect("opening the blob availability feed");
            loop {
                let read = tokio::select! {
                    _ = watcher.cancelled() => return,
                    read = feed.next(RevisionReadLimits::default()) => {
                        read.expect("reading the blob availability feed")
                    }
                };
                let RevisionRead::Entries { entries, .. } = read else {
                    continue;
                };
                let mut waiting = waiting.lock().expect("arrivals lock");
                for entry in entries.into_iter().filter(|entry| entry.value.is_some()) {
                    for (_, reensure) in waiting.remove(&entry.key).into_iter().flatten() {
                        reensure.request();
                    }
                }
            }
        })
        .expect("a fresh stop token accepts tasks");

    Ok((
        Arc::new(WasmExecutor {
            host,
            ingress,
            blobs,
            hosted: Mutex::default(),
            signers,
            arrivals,
        }),
        stop,
    ))
}

impl WasmExecutor {
    /// Whether a workload is running for `plug_id`.
    pub fn hosts(&self, plug_id: &PlugId) -> bool {
        self.hosted
            .lock()
            .expect("hosted lock")
            .contains_key(plug_id)
    }

    /// The signer is forgotten only once the workload is stopped, so its in-flight requests keep
    /// their identity while it drains; [`Egress`] answers any call that still outlives it as a
    /// gone upstream.
    async fn stop_workload(&self, running: &Running) {
        self.host
            .workload_stop(WorkloadStopRequest {
                workload_id: running.workload_id.clone(),
            })
            .await
            .expect("stopping a workload this executor started");
        self.signers
            .lock()
            .expect("signers lock")
            .remove(&running.workload_id);
    }
}

#[async_trait]
impl NoduleExecutor for WasmExecutor {
    fn kind(&self) -> &'static str {
        KIND
    }

    async fn ensure(&self, spec: &HandlerSpec) -> Hosted {
        let workload_id = format!("nodule/{}/{}", spec.plug_id, spec.revision);
        let handler = BlobId::new(spec.handler.digest.0);
        let stale = {
            let mut hosted = self.hosted.lock().expect("hosted lock");
            match hosted.get(&spec.plug_id) {
                Some(running)
                    if running.workload_id == workload_id && running.handler == handler =>
                {
                    // The grants may have changed since the last ensure: the handler is told
                    // the capability that is live now, on its next request.
                    running.provider.set_capability(spec.capability);
                    return Hosted::Ready(Arc::clone(&running.provider) as Arc<dyn Provider>);
                }
                // Another revision (or handler) of the plug: the reconciler retires first, but
                // an executor does not rely on that to keep one workload per plug.
                Some(_) => hosted.remove(&spec.plug_id),
                None => None,
            }
        };
        if let Some(stale) = stale {
            self.stop_workload(&stale).await;
        }

        // Register for the arrival before looking, so a blob announced in between wakes us.
        self.arrivals
            .lock()
            .expect("arrivals lock")
            .entry(handler)
            .or_default()
            .insert(spec.plug_id.clone(), spec.reensure.clone());
        let Some(info) = self.blobs.stat(&handler).await.expect("blob store stat") else {
            return Hosted::NotReady(Readiness::ArtifactsUnavailable);
        };
        if info.length_octets > MAX_HANDLER_OCTETS {
            tracing::warn!(plug = %spec.plug_id, octets = info.length_octets, "handler artifact over the size cap");
            return Hosted::NotReady(Readiness::ExecutorUnavailable);
        }
        let Some(bytes) = self
            .blobs
            .read_all(&handler, MAX_HANDLER_OCTETS)
            .await
            .expect("blob store read")
        else {
            // The blob was complete a moment ago and is not now: retention released it.
            return Hosted::NotReady(Readiness::ArtifactsUnavailable);
        };
        assert_eq!(
            blake3::hash(&bytes).as_bytes(),
            &handler.to_bytes32(),
            "the blob store returned bytes that do not hash to their id"
        );

        // `pool_size: 0` is wash's `Ephemeral` instance policy: a fresh store and instance per
        // request, no warm pool, which is the one-instance-per-dispatch rule. The default
        // `LocalResources` allow no outgoing hosts.
        // Registered before the workload can make a call: [`Egress`] finds its signer by id.
        self.signers
            .lock()
            .expect("signers lock")
            .insert(workload_id.clone(), spec.actor.capability);
        let started = self
            .host
            .workload_start(WorkloadStartRequest {
                workload_id: workload_id.clone(),
                workload: Workload {
                    namespace: spec.plug_id.to_string(),
                    name: spec.revision.to_string(),
                    annotations: Default::default(),
                    service: None,
                    components: vec![Component {
                        name: "handler".into(),
                        bytes: bytes.into(),
                        pool_size: 0,
                        ..Default::default()
                    }],
                    host_interfaces: vec![WitInterface::from("wasi:http/handler@0.3.0")],
                    volumes: vec![],
                },
            })
            .await;
        if let Err(error) = started {
            // The bytes are what the envelope declared but are not a component this host can
            // run (not a component, or imports it does not provide): a property of the
            // artifact, reported as readiness rather than crashing the node.
            self.signers
                .lock()
                .expect("signers lock")
                .remove(&workload_id);
            tracing::warn!(plug = %spec.plug_id, revision = %spec.revision, "handler workload failed to start: {error:#}");
            return Hosted::NotReady(Readiness::ExecutorUnavailable);
        }
        self.arrivals
            .lock()
            .expect("arrivals lock")
            .get_mut(&handler)
            .map(|waiting| waiting.remove(&spec.plug_id));
        let provider = Arc::new(WasmProvider {
            ingress: Arc::clone(&self.ingress),
            workload_id: workload_id.clone(),
            capability: Mutex::new(spec.capability),
        });
        self.hosted.lock().expect("hosted lock").insert(
            spec.plug_id.clone(),
            Running {
                workload_id,
                handler,
                provider: Arc::clone(&provider),
            },
        );
        Hosted::Ready(provider)
    }

    async fn retire(&self, plug_id: &PlugId) {
        let running = self.hosted.lock().expect("hosted lock").remove(plug_id);
        if let Some(running) = running {
            self.stop_workload(&running).await;
        }
        let mut arrivals = self.arrivals.lock().expect("arrivals lock");
        arrivals.retain(|_, waiting| {
            waiting.remove(plug_id);
            !waiting.is_empty()
        });
    }
}

/// Calls one running handler workload over in-process `wasi:http`.
struct WasmProvider {
    ingress: Arc<Ingress<Sealed, Egress>>,
    workload_id: String,
    /// The nodule's own dispatch capability, delivered in [`CAPABILITY_HEADER`]. Replaced in
    /// place when the nodule's grants change.
    capability: Mutex<Option<CapId>>,
}

impl WasmProvider {
    fn set_capability(&self, capability: Option<CapId>) {
        *self.capability.lock().expect("capability lock") = capability;
    }
}

/// The wire form of a capability id, which is how the guest sees it in `authorization`.
fn capability_header(authority: impl serde::Serialize) -> String {
    let Value::String(id) = serde_json::to_value(authority).expect("a capability id serializes")
    else {
        unreachable!("a capability id serializes as a string");
    };
    format!("Capability {id}")
}

fn gateway_error(message: impl std::fmt::Display) -> ProviderError {
    ProviderError::Failed {
        status: BAD_GATEWAY,
        body: json!({ "error": "BadGateway", "message": message.to_string() }),
    }
}

#[async_trait]
impl Provider for WasmProvider {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        context: &InvocationContext,
        input: Value,
    ) -> Result<Value, ProviderError> {
        let mut request = hyper::Request::post(format!("http://{AUTHORITY}/xrpc/{operation}"))
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(
                hyper::header::AUTHORIZATION,
                capability_header(caller.authorization.authority),
            );
        if let Some(capability) = *self.capability.lock().expect("capability lock") {
            request = request.header(CAPABILITY_HEADER, capability_header(capability));
        }
        if let Some(key) = &context.idempotency_key {
            request = request.header("idempotency-key", key);
        }
        if let Some(deadline) = context.deadline_unix_ms {
            request = request.header("x-request-deadline-unix-ms", deadline);
        }
        let body = Full::new(Bytes::from(
            serde_json::to_vec(&input).expect("a JSON value serializes"),
        ))
        .map_err(|never| match never {})
        .boxed_unsync();
        // The operation is an NSID the router resolved, so the URI and headers are valid.
        let request = request.body(body).expect("a well-formed request");

        let response = self
            .ingress
            .dispatch_request(&self.workload_id, request)
            .await
            .map_err(gateway_error)?;
        let status = response.status();
        let body = Limited::new(response.into_body(), MAX_MESSAGE_OCTETS)
            .collect()
            .await
            .map_err(gateway_error)?
            .to_bytes();

        if status.is_success() {
            return if body.is_empty() {
                Ok(Value::Null)
            } else {
                serde_json::from_slice(&body)
                    .map_err(|error| gateway_error(format!("response is not JSON: {error}")))
            };
        }
        let body = serde_json::from_slice::<Value>(&body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
        if status == hyper::StatusCode::BAD_REQUEST {
            // XRPC errors are `{ "error", "message" }`; the message is what a caller can act on.
            return Err(ProviderError::InvalidInput(
                body.get("message")
                    .and_then(Value::as_str)
                    .map_or_else(|| body.to_string(), str::to_owned),
            ));
        }
        Err(ProviderError::Failed {
            status: status.as_u16(),
            body,
        })
    }
}

/// The ingress router: nothing is routable from outside. Calls enter through
/// [`Ingress::dispatch_request`], which consults no router.
struct Sealed;

#[async_trait]
impl Router for Sealed {
    async fn on_workload_resolved(
        &self,
        _resolved_handle: &ResolvedWorkload,
        _component_id: &str,
    ) -> wash_runtime::wasmtime::anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
    ) -> wash_runtime::wasmtime::anyhow::Result<()> {
        Ok(())
    }

    /// Only `dispatch.local` is allowed, and [`Egress`] answers it without any connection.
    /// Nothing else ever is, because nodule network facilities are not granted.
    fn allow_outgoing_request(
        &self,
        _workload_id: &str,
        request: &hyper::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _allowed_hosts: &[AllowedHost],
    ) -> wash_runtime::wasmtime::anyhow::Result<()> {
        if request
            .uri()
            .authority()
            .map(|authority| authority.as_str())
            == Some(DISPATCH_AUTHORITY)
        {
            return Ok(());
        }
        wash_runtime::wasmtime::anyhow::bail!("nodule handlers may only call {DISPATCH_AUTHORITY}")
    }

    fn route_incoming_request(
        &self,
        _req: &hyper::Request<hyper::body::Incoming>,
    ) -> Result<String, RouteError> {
        Err(RouteError::NoWorkloadForHost(String::new()))
    }
}

/// The signer (the nodule actor's own capability) of each running workload's outgoing calls,
/// by workload id. The id is chosen by the host and is not guest-controllable, which is what
/// makes it the caller identity of a nested call.
type Signers = Arc<Mutex<BTreeMap<String, CapId>>>;

/// The `dispatch.local` egress: the only outgoing request a workload can make.
///
/// `POST /xrpc/<nsid>` with `authorization: Capability <id>` is invoked through dispatch with
/// the calling workload's nodule actor as the signer. Dispatch then admits the call as it admits
/// any other: the capability must be live, permit the NSID and be bound to that actor, so a
/// guest that presents another nodule's capability id is denied there. Nothing else the guest
/// says (headers, body) is trusted as identity. `idempotency-key` and
/// `x-request-deadline-unix-ms` pass through as the call's [`InvocationContext`].
#[derive(Clone)]
struct Egress {
    dispatch: Client<DispatchProtocol>,
    signers: Signers,
}

impl Egress {
    /// Reads the guest's request and runs it through dispatch. A malformed request is answered
    /// like an XRPC server would, not refused as a transport failure.
    async fn call(
        &self,
        workload_id: &str,
        request: hyper::Request<WasiBody>,
    ) -> (hyper::StatusCode, Value) {
        let bad = |status: hyper::StatusCode, error: &str, message: &str| {
            (status, json!({ "error": error, "message": message }))
        };
        let (parts, body) = request.into_parts();
        let operation = match parts.uri.path().strip_prefix("/xrpc/") {
            Some(operation) if parts.method == hyper::Method::POST => operation.to_owned(),
            _ => {
                return bad(
                    hyper::StatusCode::NOT_FOUND,
                    "NotFound",
                    "dispatch.local serves POST /xrpc/<nsid> only",
                );
            }
        };
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .map(|value| value.to_str().map(str::to_owned))
        };
        let Some(Ok(authorization)) = header(hyper::header::AUTHORIZATION.as_str()) else {
            return bad(
                hyper::StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "an `authorization: Capability <id>` header is required",
            );
        };
        let Some(authority) = authorization
            .strip_prefix("Capability ")
            .and_then(|id| serde_json::from_value::<CapId>(Value::String(id.to_owned())).ok())
        else {
            return bad(
                hyper::StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "the authorization header is not `Capability <id>`",
            );
        };
        let deadline_unix_ms = match header("x-request-deadline-unix-ms").transpose() {
            Ok(value) => match value.map(|value| value.parse::<u64>()).transpose() {
                Ok(deadline) => deadline,
                Err(_) => {
                    return bad(
                        hyper::StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        "the deadline header is not a millisecond timestamp",
                    );
                }
            },
            Err(_) => {
                return bad(
                    hyper::StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "the deadline header is not text",
                );
            }
        };
        let Ok(idempotency_key) = header("idempotency-key").transpose() else {
            return bad(
                hyper::StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "the idempotency-key header is not text",
            );
        };
        let Ok(body) = Limited::new(body, MAX_MESSAGE_OCTETS).collect().await else {
            return bad(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                "InvalidRequest",
                "the request body failed or is too large",
            );
        };
        let body = body.to_bytes();
        let input = if body.is_empty() {
            Value::Null
        } else if let Ok(input) = serde_json::from_slice(&body) {
            input
        } else {
            return bad(
                hyper::StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "the request body is not JSON",
            );
        };

        // A workload that has been stopped (its nodule disabled, upgraded or retired) while one
        // of its requests was still running has no identity left to call with. That is a
        // lifecycle race, not a broken invariant, so it is answered the way a proxy answers a
        // call from a drained upstream rather than crashing the node.
        let Some(signer) = self
            .signers
            .lock()
            .expect("signers lock")
            .get(workload_id)
            .copied()
        else {
            return bad(
                hyper::StatusCode::SERVICE_UNAVAILABLE,
                "WorkloadStopped",
                "the calling handler has been stopped",
            );
        };
        let result = self
            .dispatch
            .call_with(
                RequestContext::root(),
                Authorization { authority, signer }.metadata(),
                Invoke {
                    operation,
                    input,
                    context: InvocationContext {
                        deadline_unix_ms,
                        idempotency_key,
                    },
                },
            )
            .await;
        match result {
            Ok(output) => (hyper::StatusCode::OK, output.output),
            Err(ClientError::Method(error)) => dispatch_error_response(error),
            Err(ClientError::Transport { message, .. }) => {
                panic!("in-process dispatch transport failed: {message}")
            }
        }
    }
}

/// The HTTP form of a dispatch failure, which is the inverse of [`WasmProvider::call`]'s mapping
/// of a handler's response onto dispatch's errors: denied 403, unknown operation 404, not ready
/// 503, invalid input 400 as an XRPC `{ error, message }`, a provider's own error as it came, a
/// deadline 504. The failures of the services behind dispatch are gateway errors.
fn dispatch_error_response(error: DispatchError) -> (hyper::StatusCode, Value) {
    use hyper::StatusCode;
    let named = |status: StatusCode, name: &str| {
        (
            status,
            json!({ "error": name, "message": error.to_string() }),
        )
    };
    match &error {
        DispatchError::Denied => named(StatusCode::FORBIDDEN, "Denied"),
        DispatchError::UnknownOperation => named(StatusCode::NOT_FOUND, "UnknownOperation"),
        DispatchError::NotReady { .. } => named(StatusCode::SERVICE_UNAVAILABLE, "NotReady"),
        DispatchError::InvalidInput(message) => (
            StatusCode::BAD_REQUEST,
            json!({ "error": "InvalidRequest", "message": message }),
        ),
        DispatchError::DeadlineExceeded => named(StatusCode::GATEWAY_TIMEOUT, "DeadlineExceeded"),
        DispatchError::Provider { status, body } => (
            StatusCode::from_u16(*status).expect("a provider answered a valid status"),
            body.clone(),
        ),
        DispatchError::AuthorityUnavailable(_) => {
            named(StatusCode::SERVICE_UNAVAILABLE, "AuthorityUnavailable")
        }
        DispatchError::Context(_) | DispatchError::Authority(_) => {
            named(StatusCode::BAD_GATEWAY, "BadGateway")
        }
    }
}

impl OutgoingHandler for Egress {
    fn send_request(
        &self,
        workload_id: &str,
        request: hyper::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: RequestIoFuture,
    ) -> SendFuture {
        let egress = self.clone();
        let workload_id = workload_id.to_owned();
        Box::new(async move {
            let (status, body) = egress.call(&workload_id, request).await;
            let body = Full::new(Bytes::from(
                serde_json::to_vec(&body).expect("a JSON value serializes"),
            ))
            .map_err(|never| match never {})
            .boxed_unsync();
            let response = hyper::Response::builder()
                .status(status)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(body)
                .expect("a well-formed response");
            // Nothing is uploaded out of band: the request body was read in full above.
            let io: RequestIoFuture = Box::new(async { Ok(()) });
            Ok((response, io))
        })
    }
}
