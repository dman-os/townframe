//! The Wasm executor for nodule handlers: the [`NoduleExecutor`] of kind `"wasm"`.
//!
//! One wash-runtime [`Host`] runs every nodule handler as a workload of its own: one
//! `wasi:http/handler@0.3.0` component per (plug, revision). A call enters the workload through
//! [`Ingress::dispatch_request`], the in-process entry point of our wash-runtime fork, so
//! nothing here is reachable over the network:
//!
//! - the ingress still binds a loopback listener (the host requires one) but its [`Router`]
//!   refuses every incoming request and declines every local-egress match;
//! - the workloads get no egress at all in this slice. Nodule network facilities are not
//!   granted yet and the `dispatch.local` egress (a nodule calling another nodule's operation)
//!   is slice 2c, which replaces [`Sealed::allow_outgoing_request`] and the egress handler for
//!   that one authority.
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
use big_nodule::dispatch::Caller;
use big_nodule::dispatch::Provider;
use big_nodule::dispatch::ProviderError;
use big_nodule::executor::HandlerSpec;
use big_nodule::executor::Hosted;
use big_nodule::executor::NoduleExecutor;
use big_nodule::executor::Reensure;
use big_nodule_core::dispatch::InvocationContext;
use big_nodule_core::inventory::Readiness;
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
/// (a plug id like `@example/photos` is not a valid URI authority), so this is only what the
/// guest sees in its `Host` header.
const AUTHORITY: &str = "nodule.local";

/// Handler components larger than this are not loaded. The whole component is held in memory
/// while it compiles, and a handler is a nodule's logic, not its data (blobs carry data), so a
/// generous 64 MiB bounds a hostile or mistaken envelope without constraining real handlers.
const MAX_HANDLER_OCTETS: u64 = 64 * 1024 * 1024;

/// Response bodies larger than this fail the call. dispatch's output is a JSON value held in
/// memory (several times its encoded size once parsed) and returned over a service call, so it
/// is capped at 4 MiB; anything bigger belongs in a blob that the operation returns an id for.
const MAX_RESPONSE_OCTETS: usize = 4 * 1024 * 1024;

/// The status a transport failure (workload gone, body failed, oversize response) is reported
/// under, as an HTTP gateway would.
const BAD_GATEWAY: u16 = 502;

/// Hosts nodule handlers as wash workloads. Built by [`boot`].
pub struct WasmExecutor {
    host: Arc<Host>,
    ingress: Arc<Ingress<Sealed, NoEgress>>,
    blobs: Arc<BlobService>,
    /// What `ensure` started, by plug. Never locked across an await: `ensure` and `retire` are
    /// only called from the nodule service's state actor, one at a time, so the check, start
    /// and insert in `ensure` cannot interleave with another call for the same plug.
    hosted: Mutex<BTreeMap<PlugId, Running>>,
    /// The plugs waiting on a blob that was not in the store, to be woken on its arrival.
    arrivals: Arc<Mutex<BTreeMap<BlobId, BTreeMap<PlugId, Reensure>>>>,
}

struct Running {
    workload_id: String,
    handler: BlobId,
    provider: Arc<WasmProvider>,
}

/// Starts the wash host and the feed that wakes handlers whose bytes arrive late. The returned
/// token stops the feed and then the host, and must outlive (be stopped after) the nodule
/// service that calls the executor.
pub async fn boot(blobs: Arc<BlobService>) -> Res<(Arc<WasmExecutor>, StopToken)> {
    let ingress = Arc::new(
        Ingress::builder(Sealed, "127.0.0.1:0".parse().expect("literal address"))
            .outgoing_handler(NoEgress)
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

    async fn stop_workload(&self, running: &Running) {
        self.host
            .workload_stop(WorkloadStopRequest {
                workload_id: running.workload_id.clone(),
            })
            .await
            .expect("stopping a workload this executor started");
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
    ingress: Arc<Ingress<Sealed, NoEgress>>,
    workload_id: String,
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
        let body = Limited::new(response.into_body(), MAX_RESPONSE_OCTETS)
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

    /// Slice 2c opens the `dispatch.local` authority here (and in [`NoEgress`]); nothing else
    /// is ever allowed, because nodule network facilities are not granted.
    fn allow_outgoing_request(
        &self,
        _workload_id: &str,
        _request: &hyper::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _allowed_hosts: &[AllowedHost],
    ) -> wash_runtime::wasmtime::anyhow::Result<()> {
        wash_runtime::wasmtime::anyhow::bail!("nodule handlers have no network egress")
    }

    fn route_incoming_request(
        &self,
        _req: &hyper::Request<hyper::body::Incoming>,
    ) -> Result<String, RouteError> {
        Err(RouteError::NoWorkloadForHost(String::new()))
    }
}

/// The egress handler behind [`Sealed::allow_outgoing_request`]: it should never be reached, and
/// refuses if it is.
struct NoEgress;

impl OutgoingHandler for NoEgress {
    fn send_request(
        &self,
        _workload_id: &str,
        _request: hyper::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: RequestIoFuture,
    ) -> SendFuture {
        Box::new(async { Err(wasmtime_wasi_http::Error::ConnectionRefused) })
    }
}
