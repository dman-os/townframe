//! The native nodule executor: serves a [`big_ndk::Nodule`] in-process in place of the wasm
//! component the author ships.
//!
//! It reports provider kind `"wasm"` because it stands in for the wasm nodule the author ships,
//! so grants behave as in production. Everything else on the path is the production one: the
//! delivery request is built by `big_nodule::wire` and served by `big_ndk::serve`, and the
//! handler's outgoing calls go through `big_nodule::egress::call` signed with the nodule's own
//! actor capability, so they are authorized exactly like wasm egress.
//!
//! Handler futures are not `Send` and a [`Provider`] must be, so each hosted nodule owns an OS
//! thread running a current-thread runtime and a `LocalSet`. Calls reach it over a channel and
//! answer through a oneshot.

use async_trait::async_trait;
use big_cabinet_core::authority::CapId;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Digest;
use big_cartridges_core::Revision;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::InvocationContext;
use big_ndk::HttpHandle;
use big_ndk::Nodule;
use big_nodule::egress;
use big_nodule::executor::HandlerSpec;
use big_nodule::executor::Hosted;
use big_nodule::executor::NoduleExecutor;
use big_nodule::wire::delivery_headers;
use big_nodule::wire::gateway_error;
use big_nodule::wire::provider_result;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use method_core::ByteBody;
use serde_json::Value;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_client_native_rs::Client;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::LocalSet;

/// A nodule the harness hosts, type-erased over the author's [`Nodule`].
pub(crate) trait Registered: Send + Sync {
    /// Starts a thread serving the nodule, whose outgoing calls go over `egress`.
    fn start(&self, thread_name: String, egress: Egress) -> Started;
}

/// The registration of one nodule: the instance each request is served from a clone of. The
/// wasm component is stateless between requests (`export_nodule!` builds a fresh `Default` per
/// request), so a clone per request keeps state from leaking between calls here too.
pub(crate) struct Hosting<N>(pub(crate) Mutex<N>);

impl<N: Nodule + Clone + Send + 'static> Registered for Hosting<N> {
    fn start(&self, thread_name: String, egress: Egress) -> Started {
        let template = self.0.lock().expect("nodule template lock").clone();
        let (jobs, mut inbox) = mpsc::unbounded_channel::<Job>();
        let (stop, mut stopped) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("building the nodule thread's runtime");
                let local = LocalSet::new();
                let transport = HttpHandle::new(egress);
                local.block_on(&runtime, async {
                    loop {
                        let job = tokio::select! {
                            biased;
                            _ = &mut stopped => break,
                            job = inbox.recv() => match job {
                                Some(job) => job,
                                // Every sender is gone: nothing can call this nodule any more.
                                None => break,
                            },
                        };
                        let nodule = template.clone();
                        let transport = transport.clone();
                        tokio::task::spawn_local(async move {
                            let response = big_ndk::serve(
                                &nodule,
                                transport,
                                &Method::POST,
                                &job.path,
                                &job.headers,
                                &job.body,
                            )
                            .await;
                            // The caller's deadline or cancellation drops its end of the reply
                            // channel while the handler runs, which is a call nobody awaits any
                            // more, not a lifecycle break.
                            drop(job.reply.send((response.status(), response.into_body())));
                        });
                    }
                });
                // Handlers still running (a call in flight when the nodule was retired) finish
                // before the thread ends, as a stopping wasm workload drains its requests.
                runtime.block_on(local);
            })
            .expect("spawning a nodule thread");
        Started { jobs, stop, thread }
    }
}

/// One request to a hosted nodule: the delivery the wire builds, and where the answer goes.
struct Job {
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
    reply: oneshot::Sender<(StatusCode, Vec<u8>)>,
}

pub(crate) struct Started {
    jobs: mpsc::UnboundedSender<Job>,
    stop: oneshot::Sender<()>,
    thread: JoinHandle<()>,
}

/// The `dispatch.local` egress of one hosted nodule: its calls are signed with the nodule's own
/// actor capability, which comes from the executor's table and never from the handler.
#[derive(Clone)]
pub(crate) struct Egress {
    dispatch: Client<DispatchProtocol>,
    signer: CapId,
}

impl HttpTransport for Egress {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        let (status, body) = egress::call(
            &self.dispatch,
            Some(self.signer),
            &request.method,
            &format!("/{}", request.path),
            &request.headers,
            &request.body,
        )
        .await;
        Ok(XrpcResponse {
            status,
            headers: HeaderMap::new(),
            body: serde_json::to_vec(&body).expect("a JSON value serializes"),
        })
    }
}

struct NativeProvider {
    jobs: mpsc::UnboundedSender<Job>,
    /// The nodule's own dispatch capability, replaced in place when its grants change.
    capability: Mutex<Option<CapId>>,
}

#[async_trait]
impl Provider for NativeProvider {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        request: &RequestContext,
        context: &InvocationContext,
        input: Value,
        body: Option<ByteBody>,
    ) -> Result<Body, ProviderError> {
        if body.is_some() {
            return Err(big_nodule::wire::byte_body_refused());
        }
        let (reply, answer) = oneshot::channel();
        let job = Job {
            path: format!("/xrpc/{operation}"),
            headers: delivery_headers(
                caller,
                request,
                context,
                *self.capability.lock().expect("capability lock"),
            ),
            body: serde_json::to_vec(&input).expect("a JSON value serializes"),
            reply,
        };
        // A call that raced the nodule's retirement reaches a stopped thread. Like the wasm
        // provider's gone workload, that is a gateway failure, not a broken invariant.
        self.jobs
            .send(job)
            .map_err(|_| gateway_error("the nodule has been stopped"))?;
        let (status, body) = answer
            .await
            .map_err(|_| gateway_error("the nodule stopped before it answered"))?;
        provider_result(status, &body).map(Body::Json)
    }
}

struct Running {
    revision: Revision,
    handler: Digest,
    provider: Arc<NativeProvider>,
    stop: oneshot::Sender<()>,
    thread: JoinHandle<()>,
}

impl Running {
    async fn stop(self) {
        let stopped = self.stop.send(());
        let thread = self.thread;
        let joined = tokio::task::spawn_blocking(move || thread.join())
            .await
            .expect("joining a nodule thread");
        if let Err(payload) = joined {
            std::panic::resume_unwind(payload);
        }
        stopped.expect("a nodule thread ended before it was stopped");
    }
}

pub(crate) struct NativeExecutor {
    nodules: BTreeMap<CartridgeId, Box<dyn Registered>>,
    dispatch: Client<DispatchProtocol>,
    hosted: Mutex<BTreeMap<CartridgeId, Running>>,
}

impl NativeExecutor {
    pub(crate) fn new(
        nodules: BTreeMap<CartridgeId, Box<dyn Registered>>,
        dispatch: Client<DispatchProtocol>,
    ) -> Arc<Self> {
        Arc::new(Self {
            nodules,
            dispatch,
            hosted: Mutex::default(),
        })
    }

    /// Stops every nodule still hosted, for the node's shutdown.
    pub(crate) async fn stop_all(&self) {
        let running = std::mem::take(&mut *self.hosted.lock().expect("hosted lock"));
        for (_, running) in running {
            running.stop().await;
        }
    }
}

#[async_trait]
impl NoduleExecutor for NativeExecutor {
    fn kind(&self) -> &'static str {
        // It stands in for the wasm nodule the author ships, so grants behave as in production.
        "wasm"
    }

    async fn ensure(&self, spec: &HandlerSpec) -> Hosted {
        let stale = {
            let mut hosted = self.hosted.lock().expect("hosted lock");
            match hosted.get(&spec.cartridge_id) {
                Some(running)
                    if running.revision == spec.revision
                        && running.handler == spec.handler.digest =>
                {
                    // The grants may have changed since the last ensure: the nodule is told
                    // the capability that is live now, on its next request.
                    *running.provider.capability.lock().expect("capability lock") = spec.capability;
                    return Hosted::Ready(Arc::clone(&running.provider) as Arc<dyn Provider>);
                }
                Some(_) => hosted.remove(&spec.cartridge_id),
                None => None,
            }
        };
        if let Some(stale) = stale {
            stale.stop().await;
        }
        let registered = self.nodules.get(&spec.cartridge_id).unwrap_or_else(|| {
            panic!(
                "cartridge {} was admitted but no native nodule was registered for it",
                spec.cartridge_id
            )
        });
        // The handler artifact's bytes are never read: the nodule is the Rust value the author
        // registered, so only the envelope's declaration that it has a handler matters.
        let started = registered.start(
            format!("nodule/{}", spec.cartridge_id),
            Egress {
                dispatch: self.dispatch.clone(),
                signer: spec.actor.capability,
            },
        );
        let provider = Arc::new(NativeProvider {
            jobs: started.jobs,
            capability: Mutex::new(spec.capability),
        });
        self.hosted.lock().expect("hosted lock").insert(
            spec.cartridge_id.clone(),
            Running {
                revision: spec.revision,
                handler: spec.handler.digest,
                provider: Arc::clone(&provider),
                stop: started.stop,
                thread: started.thread,
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
            running.stop().await;
        }
    }
}
