//! The Wasm executor for nodule handlers: the [`NoduleExecutor`] of kind `"wasm"`.
//!
//! One wash-runtime [`Host`] runs each declared nodule component as its own workload: one
//! `wasi:http/handler@0.3.0` component per (cartridge, revision, component key). A call enters
//! the selected workload through [`Ingress::dispatch_request`], the in-process entry point of our
//! wash-runtime fork, so
//! nothing here is reachable over the network:
//!
//! - the ingress still binds a loopback listener (the host requires one) but its [`Router`]
//!   refuses every incoming request and declines every local-egress match;
//! - a workload's egress is `dispatch.local` (a nodule calling an operation through dispatch),
//!   served by [`Egress`], and any `http(s)` URL its routine holds a `#net` permbind and grant
//!   for (ADR 049 §2): [`Egress`] puts it through [`egress::authorize_net`] and performs an
//!   allowed one with wash's pooled client, per workload. gRPC never goes out (wash would route
//!   it around [`Egress`]).
//!
//! A nodule learns its own dispatch capability from the `x-nodule-capability` header of every
//! request it serves and presents it as `authorization: Bearer <id>` on its outgoing calls. Identity is not
//! the guest's to claim: [`Egress`] signs each call with the actor of the workload it came from.
//!
//! Handler bytes come from the in-process [`BlobService`]. A handler whose bytes are not in the
//! local store yet is `NotReady(ArtifactsUnavailable)`, and the blob service's availability
//! feed (the same one the blob workers follow) tells this executor when they arrive, which then
//! asks the state actor to `ensure` again. Nothing polls.

// The nested async futures of wash's `Host::workload_start` make the compiler's `Send` proof
// for `ensure` deeper than the default limit (rust-lang/rust#159228).
#![recursion_limit = "256"]

mod materialize;
mod net;
mod stdio;

use async_trait::async_trait;
use big_blobs::BlobService;
use big_blobs::service::mount::WorkingMount;
use big_blobs_core::BlobId;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component as ManifestComponent;
use big_cartridges_core::Digest;
use big_cartridges_core::Revision;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Readiness;
use big_nodule::XrpcBody;
use big_nodule::egress;
use big_nodule::egress::WorkloadCaller;
use big_nodule::executor::HandlerSpec;
use big_nodule::executor::Hosted;
use big_nodule::executor::NoduleExecutor;
use big_nodule::executor::Reensure;
use big_nodule::executor::RoutineCapabilities;
use big_nodule::wire::delivery_headers;
use big_nodule::wire::gateway_error;
use big_nodule::wire::provider_result;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use bytes::Bytes;
use futures::StreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::Limited;
use http_body_util::StreamBody;
use method_core::ByteBody;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_client_native_rs::Client;
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
use wash_runtime::host::http::SendResult;
use wash_runtime::types::Component;
use wash_runtime::types::HostPathVolume;
use wash_runtime::types::LocalResources;
use wash_runtime::types::Volume;
use wash_runtime::types::VolumeMount;
use wash_runtime::types::VolumeType;
use wash_runtime::types::Workload;
use wash_runtime::types::WorkloadStartRequest;
use wash_runtime::types::WorkloadStopRequest;
use wash_runtime::wasmtime::WasmBacktrace;
use wash_runtime::wit::WitInterface;
use wasmtime_wasi_http::Error as WasiHttpError;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;

fn wasi_body(body: XrpcBody) -> (WasiBody, Option<String>, Option<u64>) {
    match body {
        XrpcBody::Json(body) => {
            let length = body.len() as u64;
            let body = Full::new(Bytes::from(body))
                .map_err(|never| match never {})
                .boxed_unsync();
            (body, Some("application/json".to_owned()), Some(length))
        }
        XrpcBody::Bytes(body) => {
            let mime = body.mime.clone();
            let length = body.length;
            let body = StreamBody::new(
                body.map(|chunk| chunk.map(Frame::data).map_err(WasiHttpError::Connect)),
            )
            .boxed_unsync();
            (body, mime, length)
        }
    }
}

fn byte_body(body: WasiBody, mime: Option<String>, length: Option<u64>) -> ByteBody {
    let body = ByteBody::new(
        body.into_data_stream()
            .map(|chunk| chunk.map_err(|error| std::io::Error::other(format!("{error:?}")))),
    );
    let mut body = body;
    if let Some(mime) = mime {
        body = body.with_mime(mime);
    }
    if let Some(length) = length {
        body = body.with_length(length);
    }
    body
}

/// The provider type tag dispatch records for nodules this executor hosts.
const KIND: &str = "wasm";

/// The authority every request is addressed to. The workload is picked by id, not by `Host`
/// (a cartridge id like `did:key:z6Mk.../photos` is not a valid URI authority), so this is only what the
/// guest sees in its `Host` header.
const AUTHORITY: &str = "nodule.local";

/// Handler components larger than this are not loaded. The whole component is held in memory
/// while it compiles, and a handler is a nodule's logic, not its data (blobs carry data), so a
/// generous 64 MiB bounds a hostile or mistaken envelope without constraining real handlers.
const MAX_HANDLER_OCTETS: u64 = 64 * 1024 * 1024;

fn component_digest(component: &ManifestComponent) -> Digest {
    let ManifestComponent::Wasm(component) = component else {
        panic!("a nodule component is not wasm")
    };
    component.digest
}

/// Message bodies larger than this fail the call, in both directions: a handler's response and a
/// guest's outgoing request into dispatch. Either is a JSON value held in memory (several times
/// its encoded size once parsed) and carried over a service call, so it is capped at 4 MiB;
/// anything bigger belongs in a blob that the operation passes or returns an id for.
const MAX_MESSAGE_OCTETS: usize = 4 * 1024 * 1024;

/// Where every workload's working mount appears in the guest, preopened read-write (ADR 051 §2).
/// [`materialize`] answers paths under it.
const WORKING_MOUNT: &str = "/work";

/// The authority a guest addresses dispatch at. It resolves nowhere: [`Egress`] answers it.
const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// Why a guest failed a call, as data. A call that fails this way is answered `502 BadGateway`
/// whose JSON body is `{ "error": "GuestFault", "message", "fault": <this> }`, and the same
/// record is logged at `error` with the cartridge id, so it is in the log file beside the
/// host's own events.
///
/// The stdio tails are node-side only: they are neither serialized into the body nor logged
/// anywhere a caller reads. A guest instance serves many callers, so its recent output can hold
/// another caller's data; they appear in the node's own `error` event and in this struct for
/// in-process consumers, and a deserialized body has them empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestFault {
    pub kind: FaultKind,
    /// One line: the trap's cause, or what the host failed at.
    pub message: String,
    /// The digest of the component that faulted, which is the identity of the debug side file
    /// its frames resolve against.
    pub component_hash: BlobId,
    /// The guest's frames at a trap, innermost first, as raw data. They are `file:line` only
    /// after a lookup against the side file: the shipped component carries no DWARF.
    pub frames: Vec<FaultFrame>,
    /// The last 16 KiB the workload wrote to its stdout and stderr, lossily decoded. A Rust
    /// guest prints its panic message to stderr just before it traps, so it is in
    /// `stderr_tail`. Guest-written: render it as text, never as markup.
    #[serde(skip_serializing, default)]
    pub stdout_tail: String,
    #[serde(skip_serializing, default)]
    pub stderr_tail: String,
}

/// What failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultKind {
    /// The guest trapped: a panic, an `unreachable`, an out-of-bounds access and the like.
    Trap,
    /// The host failed to run the guest (no workload bound, an instance that stopped, a body
    /// that failed to arrive), with nothing of the guest's to show.
    Host,
}

/// One frame of a guest backtrace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultFrame {
    /// The function's index in its core module.
    pub func_index: u32,
    /// The function's name from the component's `name` section; `None` when it has none.
    pub func_name: Option<String>,
    /// The offset of the frame's instruction in the core module's original bytes, the address
    /// DWARF line tables are keyed by.
    pub module_offset: Option<usize>,
}

/// Hosts nodule handlers as wash workloads. Built by [`boot`].
pub struct WasmExecutor {
    host: Arc<Host>,
    ingress: Arc<Ingress<Sealed, Egress>>,
    blobs: Arc<BlobService>,
    /// What `ensure` started, by cartridge. Never locked across an await: `ensure` and `retire` are
    /// only called from the nodule service's state actor, one at a time, so the check, start
    /// and insert in `ensure` cannot interleave with another call for the same cartridge.
    hosted: Mutex<BTreeMap<CartridgeId, Running>>,
    /// The signer of each running workload's outgoing calls, shared with [`Egress`].
    signers: Signers,
    /// Each running workload's working mount, shared with [`Egress`], which materializes blobs
    /// into it.
    mounts: Mounts,
    /// Each running workload's captured stdout and stderr.
    pipes: stdio::Pipes,
    /// The cartridges waiting on a blob that was not in the store, to be woken on its arrival.
    arrivals: Arc<Mutex<BTreeMap<BlobId, BTreeMap<CartridgeId, Reensure>>>>,
}

struct RunningComponent {
    workload_id: String,
    artifact: BlobId,
}

struct Running {
    revision: Revision,
    components: BTreeMap<String, RunningComponent>,
    provider: Arc<WasmProvider>,
}

/// Starts the wash host and the feed that wakes handlers whose bytes arrive late. Guests' calls
/// to `dispatch.local` are made through `dispatch`. The returned token stops the feed and then
/// the host, and must be stopped after the nodule service that calls the executor and before
/// the dispatch service (and the authority behind it), which in-flight guest calls still use.
///
/// `allow_host_loopback` is the node's switch for the machine's own loopback: only when it is
/// on does `NetTarget::HOST_LOOPBACK`, for the ports a cartridge holds a `grant/net` for,
/// reach `127.0.0.1`. A guest's own `127.0.0.1` and `localhost` never do.
pub async fn boot(
    blobs: Arc<BlobService>,
    dispatch: Client<DispatchProtocol>,
    allow_host_loopback: bool,
) -> Res<(Arc<WasmExecutor>, StopToken)> {
    let signers = Signers::default();
    let mounts = Mounts::default();
    let pipes = stdio::Pipes::default();
    let ingress = Arc::new(
        Ingress::builder(Sealed, "127.0.0.1:0".parse().expect("literal address"))
            .outgoing_handler(Egress {
                dispatch,
                signers: Arc::clone(&signers),
                mounts: Arc::clone(&mounts),
                blobs: Arc::clone(&blobs),
                net: Arc::new(net::NetClients::new(allow_host_loopback)),
            })
            .build()
            .await
            .to_eyre()?,
    );
    let host = HostBuilder::new()
        .with_engine(
            Engine::builder()
                .with_guest_stdio(Arc::new(stdio::Stdio(Arc::clone(&pipes))))
                // The one place a workload's host directory may come from: its working mount.
                .with_allowed_host_paths([blobs.working_mounts().root().to_owned()])
                .build()
                .to_eyre()?,
        )
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
    let stopped_mounts = Arc::clone(&mounts);
    spawner
        .spawn(async move {
            stopper.cancelled().await;
            stopping.stop().await.expect("stopping the wash host");
            // The host stopped the workloads that were still running without going through
            // `stop_workload`, so their mounts are removed here.
            let remaining = std::mem::take(&mut *stopped_mounts.lock().expect("mounts lock"));
            for mount in remaining.into_values() {
                mount.remove().await.expect("removing a working mount");
            }
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
            mounts,
            pipes,
            arrivals,
        }),
        stop,
    ))
}

impl WasmExecutor {
    /// The workload each admitted operation is served by: that of the component its routine
    /// names, which routines sharing a component share.
    fn route_table(
        spec: &HandlerSpec,
        components: &BTreeMap<String, RunningComponent>,
    ) -> BTreeMap<String, RoutineTarget> {
        spec.routines
            .iter()
            .map(|(operation, routine)| {
                let component = components
                    .get(&routine.component)
                    .expect("admission resolved every routine component");
                (
                    operation.clone(),
                    RoutineTarget {
                        routine_key: routine.routine_key.clone(),
                        workload_id: component.workload_id.clone(),
                        artifact: component.artifact,
                        // The current nodule guest exposes one `wasi:http/handler#handle` entry
                        // point; every fixture routine names that export. Keep the declared export
                        // beside the route so a future component ABI can call it directly.
                        export: routine.export.clone(),
                    },
                )
            })
            .collect()
    }

    /// The signer is forgotten only once the workload is stopped, so its in-flight requests keep
    /// their identity while it drains; [`Egress`] answers any call that still outlives it as a
    /// gone upstream.
    async fn stop_workload(&self, running: &RunningComponent) {
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
        self.pipes
            .lock()
            .expect("pipes lock")
            .remove(&running.workload_id);
        self.remove_mount(&running.workload_id).await;
    }

    /// Registers a component's signer, stdio pipes and working mount, then starts its workload,
    /// and answers the workload's id. A failed start leaves none of the registrations behind.
    async fn start_component(
        &self,
        spec: &HandlerSpec,
        key: &str,
        component_bytes: Vec<u8>,
    ) -> wash_runtime::wasmtime::anyhow::Result<String> {
        let workload_id = format!("nodule/{}/{}/{}", spec.cartridge_id, spec.revision, key);
        self.signers.lock().expect("signers lock").insert(
            workload_id.clone(),
            WorkloadCaller {
                signer: spec.actor.capability,
                missing_authority: spec.missing_authority.clone(),
            },
        );
        // The host asks for the workload's stdio while starting it, so the pipes exist first.
        self.pipes.lock().expect("pipes lock").insert(
            workload_id.clone(),
            Arc::new(stdio::WorkloadPipes::new(&spec.cartridge_id)),
        );
        // And it opens the workload's volumes then, so the mount exists first too.
        let mount = self
            .blobs
            .working_mounts()
            .create()
            .await
            .expect("creating a working mount");
        let mount_dir = mount
            .dir()
            .to_str()
            .expect("a working mount's path is UTF-8 because wash volumes are strings")
            .to_owned();
        self.mounts
            .lock()
            .expect("mounts lock")
            .insert(workload_id.clone(), mount);
        let started = self
            .host
            .workload_start(WorkloadStartRequest {
                workload_id: workload_id.clone(),
                workload: Workload {
                    namespace: spec.cartridge_id.to_string(),
                    name: format!("{}-{key}", spec.revision),
                    annotations: Default::default(),
                    service: None,
                    components: vec![Component {
                        name: key.to_owned(),
                        bytes: component_bytes.into(),
                        pool_size: 0,
                        local_resources: LocalResources {
                            volume_mounts: vec![VolumeMount {
                                name: "work".to_owned(),
                                mount_path: WORKING_MOUNT.to_owned(),
                                read_only: false,
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    }],
                    host_interfaces: vec![WitInterface::from("wasi:http/handler@0.3.0")],
                    volumes: vec![Volume {
                        name: "work".to_owned(),
                        volume_type: VolumeType::HostPath(HostPathVolume {
                            local_path: mount_dir,
                        }),
                    }],
                },
            })
            .await;
        if let Err(error) = started {
            self.signers
                .lock()
                .expect("signers lock")
                .remove(&workload_id);
            self.pipes.lock().expect("pipes lock").remove(&workload_id);
            self.remove_mount(&workload_id).await;
            return Err(error);
        }
        Ok(workload_id)
    }

    /// Removes a workload's working mount, after its instances are gone or never started. A file
    /// the guest still had open keeps reading until closed.
    async fn remove_mount(&self, workload_id: &str) {
        let mount = self.mounts.lock().expect("mounts lock").remove(workload_id);
        if let Some(mount) = mount {
            mount.remove().await.expect("removing a working mount");
        }
    }
}

#[async_trait]
impl NoduleExecutor for WasmExecutor {
    fn kind(&self) -> &'static str {
        KIND
    }

    async fn ensure(&self, spec: &HandlerSpec) -> Hosted {
        let stale = {
            let mut hosted = self.hosted.lock().expect("hosted lock");
            match hosted.get(&spec.cartridge_id) {
                Some(running)
                    if running.revision == spec.revision
                        && running.components.len() == spec.components.len()
                        && running.components.iter().all(|(key, running)| {
                            spec.components.get(key).is_some_and(|artifact| {
                                running.artifact == BlobId::new(component_digest(artifact).0)
                            })
                        }) =>
                {
                    // Grants may change without recompiling a component: every routine provider
                    // sees the capability that is live now on its next request.
                    running
                        .provider
                        .set_capabilities(spec.routine_capabilities.clone());
                    running.provider.set_config(spec.config_doc.clone());
                    return Hosted::Ready(Arc::clone(&running.provider) as Arc<dyn Provider>);
                }
                Some(_) => hosted.remove(&spec.cartridge_id),
                None => None,
            }
        };
        if let Some(stale) = stale {
            for component in stale.components.values() {
                self.stop_workload(component).await;
            }
        }

        // Register every component before looking for its bytes, so an announcement racing this
        // check wakes the reconcile rather than leaving one routine permanently unavailable.
        let mut bytes = BTreeMap::new();
        for (key, artifact) in &spec.components {
            let blob = BlobId::new(component_digest(artifact).0);
            self.arrivals
                .lock()
                .expect("arrivals lock")
                .entry(blob)
                .or_default()
                .insert(spec.cartridge_id.clone(), spec.reensure.clone());
            let Some(info) = self.blobs.stat(&blob).await.expect("blob store stat") else {
                return Hosted::NotReady(Readiness::ArtifactsUnavailable);
            };
            if info.length_octets > MAX_HANDLER_OCTETS {
                tracing::warn!(
                    cartridge = %spec.cartridge_id,
                    component = key,
                    octets = info.length_octets,
                    "nodule component over the size cap"
                );
                return Hosted::NotReady(Readiness::ExecutorUnavailable);
            }
            let Some(component_bytes) = self
                .blobs
                .read_all(&blob, MAX_HANDLER_OCTETS)
                .await
                .expect("blob store read")
            else {
                return Hosted::NotReady(Readiness::ArtifactsUnavailable);
            };
            assert_eq!(
                blake3::hash(&component_bytes).as_bytes(),
                &blob.to_bytes32(),
                "the blob store returned bytes that do not hash to their id"
            );
            bytes.insert(key.clone(), (blob, component_bytes));
        }

        let mut components = BTreeMap::new();
        for (key, (artifact, component_bytes)) in bytes {
            let workload_id = match self.start_component(spec, &key, component_bytes).await {
                Ok(workload_id) => workload_id,
                Err(error) => {
                    for component in components.values() {
                        self.stop_workload(component).await;
                    }
                    tracing::warn!(
                        cartridge = %spec.cartridge_id,
                        revision = %spec.revision,
                        component = key,
                        "nodule component workload failed to start: {error:#}"
                    );
                    return Hosted::NotReady(Readiness::ExecutorUnavailable);
                }
            };
            self.arrivals
                .lock()
                .expect("arrivals lock")
                .get_mut(&artifact)
                .map(|waiting| waiting.remove(&spec.cartridge_id));
            components.insert(
                key,
                RunningComponent {
                    workload_id,
                    artifact,
                },
            );
        }

        let routines = Self::route_table(spec, &components);
        let provider = Arc::new(WasmProvider {
            cartridge: spec.cartridge_id.clone(),
            pipes: Arc::clone(&self.pipes),
            ingress: Arc::clone(&self.ingress),
            routines,
            routine_capabilities: Mutex::new(spec.routine_capabilities.clone()),
            config: Mutex::new(spec.config_doc.clone()),
        });
        self.hosted.lock().expect("hosted lock").insert(
            spec.cartridge_id.clone(),
            Running {
                revision: spec.revision,
                components,
                provider: Arc::clone(&provider),
            },
        );
        Hosted::Ready(provider)
    }

    fn hosts(&self, cartridge_id: &CartridgeId) -> bool {
        self.hosted
            .lock()
            .expect("hosted lock")
            .contains_key(cartridge_id)
    }

    async fn retire(&self, cartridge_id: &CartridgeId) {
        let running = self
            .hosted
            .lock()
            .expect("hosted lock")
            .remove(cartridge_id);
        if let Some(running) = running {
            for component in running.components.values() {
                self.stop_workload(component).await;
            }
        }
        let mut arrivals = self.arrivals.lock().expect("arrivals lock");
        arrivals.retain(|_, waiting| {
            waiting.remove(cartridge_id);
            !waiting.is_empty()
        });
    }
}

/// Routes an operation to the workload for its routine's component. The current guest ABI exposes
/// one HTTP entry point (`wasi:http/handler#handle`) for all routines; the declared export remains
/// attached to each route so a component-specific ABI can use it without changing dispatch.
struct RoutineTarget {
    workload_id: String,
    /// The component the workload runs, which a fault names.
    artifact: BlobId,
    routine_key: String,
    export: String,
}

/// Calls one running routine component workload over in-process `wasi:http`.
struct WasmProvider {
    cartridge: CartridgeId,
    pipes: stdio::Pipes,
    ingress: Arc<Ingress<Sealed, Egress>>,
    routines: BTreeMap<String, RoutineTarget>,
    routine_capabilities: Mutex<BTreeMap<String, RoutineCapabilities>>,
    config: Mutex<big_cabinet_core::dmeta::DocId>,
}

impl WasmProvider {
    fn set_capabilities(&self, capabilities: BTreeMap<String, RoutineCapabilities>) {
        *self
            .routine_capabilities
            .lock()
            .expect("routine capabilities lock") = capabilities;
    }

    fn set_config(&self, doc_id: big_cabinet_core::dmeta::DocId) {
        *self.config.lock().expect("config lock") = doc_id;
    }

    /// The answer to a call the guest or its host failed. The error is not flattened: a trap
    /// carries a [`WasmBacktrace`] in its chain, whose frames are kept as data in the
    /// [`GuestFault`] the body holds, and the fault is logged at `error`.
    fn fault(
        &self,
        routine: &RoutineTarget,
        error: wash_runtime::wasmtime::anyhow::Error,
    ) -> ProviderError {
        let (kind, frames) = match error.downcast_ref::<WasmBacktrace>() {
            Some(backtrace) => (
                FaultKind::Trap,
                backtrace
                    .frames()
                    .iter()
                    .map(|frame| FaultFrame {
                        func_index: frame.func_index(),
                        func_name: frame.func_name().map(str::to_owned),
                        module_offset: frame.module_offset(),
                    })
                    .collect(),
            ),
            None => (FaultKind::Host, Vec::new()),
        };
        // A call still draining after its workload was stopped has no pipes left, so its tails
        // are empty.
        let (stdout_tail, stderr_tail) = self
            .pipes
            .lock()
            .expect("pipes lock")
            .get(&routine.workload_id)
            .map(|pipes| pipes.tails())
            .unwrap_or_default();
        // The root cause is the trap's own text ("wasm trap: wasm `unreachable` instruction
        // executed"); the outer layers are the host's context and the backtrace.
        let fault = GuestFault {
            kind,
            message: error.root_cause().to_string(),
            component_hash: routine.artifact,
            frames,
            stdout_tail,
            stderr_tail,
        };
        tracing::error!(
            cartridge = %self.cartridge,
            kind = ?fault.kind,
            component_hash = %fault.component_hash,
            frames = ?fault.frames,
            stdout_tail = fault.stdout_tail,
            stderr_tail = fault.stderr_tail,
            "{}",
            fault.message
        );
        ProviderError::Failed {
            status: 502,
            body: json!({
                "error": "GuestFault",
                "message": fault.message,
                "fault": fault,
            }),
        }
    }
}

#[async_trait]
impl Provider for WasmProvider {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        request: &RequestContext,
        context: &InvocationContext,
        input: Value,
        body: Option<Body>,
    ) -> Result<Body, ProviderError> {
        // Byte bodies reach a guest as wasi:http body streams, which this provider does not
        let body = match body {
            None => XrpcBody::json(serde_json::to_vec(&input).expect("a JSON value serializes")),
            Some(Body::Bytes(body)) => XrpcBody::Bytes(body),
            Some(Body::Json(body)) => {
                XrpcBody::json(serde_json::to_vec(&body).expect("a JSON value serializes"))
            }
            #[cfg(unix)]
            Some(Body::Descriptors(_)) => return Err(big_nodule::wire::descriptor_body_refused()),
        };

        // The guest sees the contract every XRPC hop speaks, built by the shared `wire`.
        let config_doc = self.config.lock().expect("config lock").clone();
        let routine = self
            .routines
            .get(operation)
            .expect("dispatch routed an operation without an admitted routine");
        let workload_id = &routine.workload_id;
        assert_eq!(
            routine.export, "handle",
            "the nodule HTTP ABI uses the shared handle export"
        );
        let headers = delivery_headers(
            caller,
            request,
            context,
            &config_doc,
            self.routine_capabilities
                .lock()
                .expect("routine capabilities lock")
                .get(&routine.routine_key)
                .expect("a ready routine has capabilities"),
        );
        let (body, mime, length) = wasi_body(body);
        let path = match &context.http {
            Some(HttpForm::Post { query }) if !query.is_empty() => {
                format!("/xrpc/{operation}?{query}")
            }
            _ => format!("/xrpc/{operation}"),
        };
        let mut http_request = hyper::Request::post(format!("http://{AUTHORITY}{path}"));
        if let Some(mime) = mime {
            http_request = http_request.header(hyper::header::CONTENT_TYPE, mime);
        }
        if let Some(length) = length {
            http_request = http_request.header(hyper::header::CONTENT_LENGTH, length);
        }
        http_request
            .headers_mut()
            .expect("a fresh request builder has headers")
            .extend(headers);
        // The operation is an NSID the router resolved, so the URI and headers are valid.
        let request = http_request.body(body).expect("a well-formed request");

        let response = self
            .ingress
            .dispatch_request(workload_id, request)
            .await
            .map_err(|error| self.fault(routine, error))?;
        let (parts, body) = response.into_parts();
        let status = parts.status;
        let mime = parts
            .headers
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let length = parts
            .headers
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let is_json = mime
            .as_deref()
            .is_some_and(method_core::is_json_content_type);
        if is_json {
            let body = Limited::new(body, MAX_MESSAGE_OCTETS)
                .collect()
                .await
                .map_err(|error| {
                    self.fault(
                        routine,
                        wash_runtime::wasmtime::anyhow::Error::from_boxed(error),
                    )
                })?
                .to_bytes();
            provider_result(status, &body).map(Body::Json)
        } else if status.is_success() {
            Ok(Body::Bytes(byte_body(body, mime, length)))
        } else {
            Err(gateway_error("a failed byte response is not an XRPC error"))
        }
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

    /// Everything but gRPC is let through to [`Egress`], which is where a request is authorized:
    /// a refusal here is an opaque transport error, and a routine needs the `Denied` or
    /// `MissingAuthority` answer. gRPC is refused because wash sends it around [`Egress`] (it
    /// needs HTTP/2), so its destination would never be checked.
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
            != Some(DISPATCH_AUTHORITY)
            && request
                .headers()
                .get(hyper::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("application/grpc"))
        {
            wash_runtime::wasmtime::anyhow::bail!("nodule handlers may not send gRPC requests")
        }
        Ok(())
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
type Signers = Arc<Mutex<BTreeMap<String, WorkloadCaller>>>;

/// Each running workload's working mount, by workload id.
type Mounts = Arc<Mutex<BTreeMap<String, Arc<WorkingMount>>>>;

/// The `dispatch.local` egress of the wash host: adapts a workload's outgoing `wasi:http`
/// request to [`big_nodule::egress::call`], which authorizes it for every executor alike. What
/// is specific to this executor is the signer: the workload's own actor, found by the host's
/// workload id (not guest-controllable) in [`Signers`].
#[derive(Clone)]
struct Egress {
    dispatch: Client<DispatchProtocol>,
    signers: Signers,
    /// The working mount of each workload, which `materialize` places files in.
    mounts: Mounts,
    /// What a workload's `materialize` reads from.
    blobs: Arc<BlobService>,
    /// Performs the requests [`egress::authorize_net`] allowed, refusing the addresses they
    /// resolve to that no `#net` grant could open.
    net: Arc<net::NetClients>,
}

fn answer((status, body): (hyper::StatusCode, XrpcBody)) -> SendResult {
    let (body, mime, length) = wasi_body(body);
    let mut builder = hyper::Response::builder().status(status);
    if let Some(mime) = mime {
        builder = builder.header(hyper::header::CONTENT_TYPE, mime);
    }
    if let Some(length) = length {
        builder = builder.header(hyper::header::CONTENT_LENGTH, length);
    }
    let response = builder.body(body).expect("a well-formed response");

    // Nothing is uploaded out of band: the request body was read in full above.
    let io: RequestIoFuture = Box::new(async { Ok(()) });
    Ok((response, io))
}

impl Egress {
    async fn call(
        &self,
        workload_id: &str,
        request: hyper::Request<WasiBody>,
    ) -> (hyper::StatusCode, XrpcBody) {
        let (parts, body) = request.into_parts();
        let is_json = parts.method == hyper::Method::GET
            || parts
                .headers
                .get(hyper::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(method_core::is_json_content_type);
        let body = if is_json {
            let Ok(body) = Limited::new(body, MAX_MESSAGE_OCTETS).collect().await else {
                return (
                    hyper::StatusCode::PAYLOAD_TOO_LARGE,
                    XrpcBody::json(
                        serde_json::to_vec(&json!({
                            "error": "InvalidRequest",
                            "message": "the request body failed or is too large"
                        }))
                        .expect("a JSON error serializes"),
                    ),
                );
            };
            XrpcBody::Json(body.to_bytes().to_vec())
        } else {
            let mime = parts
                .headers
                .get(hyper::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let length = parts
                .headers
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());
            XrpcBody::Bytes(byte_body(body, mime, length))
        };
        // The one method the executor serves itself instead of dispatching: its destination is
        // this workload's own mount.
        if parts.method == hyper::Method::POST
            && parts.uri.path().strip_prefix("/xrpc/") == Some(materialize::NSID)
        {
            return self.materialize(workload_id, &parts.headers, body).await;
        }
        let caller = self
            .signers
            .lock()
            .expect("signers lock")
            .get(workload_id)
            .cloned();
        egress::call(
            &self.dispatch,
            caller.as_ref(),
            &parts.method,
            parts
                .uri
                .path_and_query()
                .map_or(parts.uri.path(), |target| target.as_str()),
            &parts.headers,
            body,
        )
        .await
    }
}

impl OutgoingHandler for Egress {
    fn send_request(
        &self,
        workload_id: &str,
        mut request: hyper::Request<WasiBody>,
        options: Option<RequestOptions>,
        _fut: RequestIoFuture,
    ) -> SendFuture {
        if request
            .uri()
            .authority()
            .map(|authority| authority.as_str())
            != Some(DISPATCH_AUTHORITY)
        {
            let caller = self
                .signers
                .lock()
                .expect("signers lock")
                .get(workload_id)
                .cloned();
            if let Err(refusal) = egress::authorize_net(
                caller.as_ref(),
                request.method(),
                request.uri(),
                request.headers(),
            ) {
                return Box::new(async move { answer(refusal) });
            }
            egress::strip_node_headers(request.headers_mut());
            let net = Arc::clone(&self.net);
            let workload_id = workload_id.to_owned();
            return Box::new(async move {
                match net.send(&workload_id, request, options).await {
                    Ok(sent) => sent,
                    // A destination in a refused range is answered as an undeclared host is.
                    Err(refused) => {
                        tracing::warn!(%workload_id, %refused, "refused a network destination");
                        answer(egress::net_refused())
                    }
                }
            });
        }
        let egress = self.clone();
        let workload_id = workload_id.to_owned();
        Box::new(async move { answer(egress.call(&workload_id, request).await) })
    }

    fn on_workload_unbind(&self, workload_id: &str) {
        self.net.forget(workload_id);
    }
}
