//! A small reference service for the Cabinet service shape.
//!
//! The operation declaration is intentionally declarative: its XRPC identity and
//! wire semantics are the source of the generated native client and IRPC
//! optimization.  The fake HTTP executor below stands in for the real XRPC over
//! Iroh adapter; it is not a complete HTTP server.

use super::{HttpInput, HttpMethod, HttpRequest, Request, RequestContext, XrpcKind, XrpcOperation};
use irpc::{WithChannels, channel, rpc_requests};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, broadcast, mpsc, watch};

#[cfg(test)]
tokio::task_local! {
    static TEST_TASK_MARKER: u64;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetValueParams;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValueParams {
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStateParams {
    pub after_source_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeEventsParams {
    pub after_source_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueSnapshot {
    pub value: String,
    pub source_revision: u64,
    pub service_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetValueOutput {
    pub snapshot: ValueSnapshot,
    pub observed_context: RequestContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValueOutput {
    pub snapshot: ValueSnapshot,
}

/// A service observation, not a durable semantic event journal.  Consumers
/// that miss it reread the current interpreted state from the source cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleEvent {
    pub source_revision: u64,
    pub service_revision: u64,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatePage {
    pub snapshot: ValueSnapshot,
    pub reset_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SampleError {
    #[error("invalid value: {message}")]
    InvalidValue { message: String },
    #[error("subscription lagged by {skipped}; reread current service state")]
    SubscriptionLagged { skipped: u64 },
    #[error("subscription capacity exhausted (limit {limit})")]
    SubscriptionCapacityExceeded { limit: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError {
    #[error("transport failed: {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
    #[error("operation failed: {0}")]
    Operation(#[from] SampleError),
}

impl ClientError {
    fn transport(error: impl std::fmt::Display, outcome_unknown: bool) -> Self {
        Self::Transport {
            message: error.to_string(),
            outcome_unknown,
        }
    }
}

/// The canonical operation declaration.  It is deliberately declarative for
/// this reference: the same entries generate the XRPC descriptors, the typed
/// client methods, and the optimized IRPC message/client.
macro_rules! declare_sample_service {
    (
        unary {
            $(
                $variant:ident => $method:ident(
                    $params:ty
                ) -> $output:ty = $kind:ident, $http:ident, $input:ident,
                    $nsid:literal, $output_schema:literal, $error_schema:literal;
            )+
        }
        stream {
            $(
                $stream_variant:ident => $stream_method:ident(
                    $stream_params:ty
                ) -> $stream_item:ty = $stream_kind:ident, $stream_http:ident, $stream_input:ident,
                    $stream_nsid:literal, $stream_output_schema:literal, $stream_error_schema:literal;
            )+
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Operation {
            $( $variant, )+
            $( $stream_variant, )+
        }

        #[rpc_requests(message = SampleIrpcMessage)]
        #[derive(Debug, Serialize, Deserialize)]
        pub enum SampleIrpc {
            $(
                #[rpc(tx = channel::oneshot::Sender<Result<$output, SampleError>>)]
                $variant(Request<$params>),
            )+
            $(
                #[rpc(tx = channel::mpsc::Sender<Result<$stream_item, SampleError>>)]
                $stream_variant(Request<$stream_params>),
            )+
        }

        /// Generated from the canonical XRPC declaration.  This is the schema
        /// and adapter seam; a production build would additionally emit and
        /// check Lexicon files from these operation entries.
        pub const XRPC_OPERATIONS: &[XrpcOperation] = &[
            $( XrpcOperation {
                nsid: $nsid,
                kind: XrpcKind::$kind,
                method: HttpMethod::$http,
                input: HttpInput::$input,
                output_schema: $output_schema,
                error_schema: $error_schema,
            }, )+
            $( XrpcOperation {
                nsid: $stream_nsid,
                kind: XrpcKind::$stream_kind,
                method: HttpMethod::$stream_http,
                input: HttpInput::$stream_input,
                output_schema: $stream_output_schema,
                error_schema: $stream_error_schema,
            }, )+
        ];

        impl SampleClient {
            $(
                pub async fn $method(
                    &self,
                    context: RequestContext,
                    params: $params,
                ) -> Result<$output, ClientError> {
                    let request = Request { context, params };
                    match &self.transport {
                        SampleTransport::Direct(service) => service.invoke(Operation::$variant, request).await.map_err(ClientError::Operation),
                        SampleTransport::Irpc(client) => client
                            .rpc(request)
                            .await
                            .map_err(|error| ClientError::transport(error, false))?
                            .map_err(ClientError::Operation),
                        SampleTransport::Xrpc(executor) => {
                            let operation = operation_descriptor(Operation::$variant);
                            let http = encode_http_request(operation, &request)?;
                            let response = executor
                                .execute(http)
                                .await
                                .map_err(|error| {
                                    ClientError::transport(
                                        error,
                                        matches!(operation.kind, XrpcKind::Procedure | XrpcKind::Subscription),
                                    )
                                })?;
                            decode_http_response(response)
                        }
                    }
                }
            )+

            $(
                pub async fn $stream_method(
                    &self,
                    context: RequestContext,
                    params: $stream_params,
                ) -> Result<SampleEventSubscription, ClientError> {
                    let request = Request { context, params };
                    match &self.transport {
                        SampleTransport::Direct(service) => Ok(SampleEventSubscription {
                            inner: SubscriptionTransport::Direct(service.invoke_subscription(request).await),
                        }),
                        SampleTransport::Irpc(client) => client
                            .server_streaming(request, EVENT_REPLAY_WINDOW)
                            .await
                            .map(|receiver| SampleEventSubscription {
                                inner: SubscriptionTransport::Irpc(receiver),
                            })
                            .map_err(|error| ClientError::transport(error, false)),
                        SampleTransport::Xrpc(executor) => {
                            let operation = operation_descriptor(Operation::$stream_variant);
                            let http = encode_http_request(operation, &request)?;
                            let response = executor
                                .execute(http)
                                .await
                                .map_err(|error| {
                                    ClientError::transport(
                                        error,
                                        matches!(operation.kind, XrpcKind::Procedure | XrpcKind::Subscription),
                                    )
                                })?;
                            let stream = response
                                .stream
                                .ok_or_else(|| ClientError::transport("XRPC response was not a stream", false))?;
                            Ok(SampleEventSubscription {
                                inner: SubscriptionTransport::Xrpc(stream),
                            })
                        }
                    }
                }
            )+
        }

        async fn dispatch_irpc(
            service: Arc<SampleService>,
            unary_permits: Arc<Semaphore>,
            stream_permits: Arc<Semaphore>,
            activity: Arc<AdapterActivity>,
            message: SampleIrpcMessage,
            stream_limit: usize,
        ) {
            let is_stream = match &message {
                $(SampleIrpcMessage::$stream_variant(_) => true,)+
                _ => false,
            };
            if is_stream {
                let Some(stream_permit) = stream_permits.try_acquire_owned().ok() else {
                    match message {
                        $( SampleIrpcMessage::$stream_variant(request) => {
                            let WithChannels { tx, .. } = request;
                            let _unused = tx.send(Err(SampleError::SubscriptionCapacityExceeded {
                                limit: stream_limit,
                            })).await;
                        } )+
                        _ => unreachable!(),
                    }
                    return;
                };
                let stream = StreamPump::new(Arc::clone(&activity));
                tokio::spawn(async move {
                    let _stream_permit = stream_permit;
                    let _stream = stream;
                    match message {
                        $( SampleIrpcMessage::$stream_variant(request) => {
                            let WithChannels { inner, tx, .. } = request;
                            let mut subscription = service.invoke_subscription(inner).await;
                            loop {
                                let item = match subscription.recv().await {
                                    Ok(Some(event)) => Ok(event),
                                    Ok(None) => break,
                                    Err(error) => Err(error),
                                };
                                let terminal = item.is_err();
                                if tx.send(item).await.is_err() || terminal { break; }
                            }
                        } )+
                        _ => unreachable!(),
                    }
                });
                return;
            }

            // Admission happens before spawning, so a bounded input queue cannot
            // be drained into an unbounded set of parked Tokio tasks.
            let permit = unary_permits.acquire_owned().await.expect("permit semaphore lives");
            let admitted = AdmittedHandler::new(Arc::clone(&activity));
            let spawned = SpawnedHandler::new(Arc::clone(&activity));
            tokio::spawn(async move {
                let _permit = permit;
                let _admitted = admitted;
                let _spawned = spawned;
                let _active = ActiveHandler::new(activity);
                match message {
                    $( SampleIrpcMessage::$variant(request) => {
                        let WithChannels { inner, tx, .. } = request;
                        let _unused = tx.send(service.invoke(Operation::$variant, inner).await).await;
                    } )+
                    _ => unreachable!(),
                }
            });
        }
    };
}

declare_sample_service! {
    unary {
        GetValue => get_value(GetValueParams) -> GetValueOutput = Query, Get, Query,
            "org.example.sample.getValue", "GetValueOutput", "SampleError";
        PutValue => put_value(PutValueParams) -> PutValueOutput = Procedure, Post, JsonBody,
            "org.example.sample.putValue", "PutValueOutput", "SampleError";
        ReadState => read_state(ReadStateParams) -> StatePage = Query, Get, Query,
            "org.example.sample.readState", "StatePage", "SampleError";
    }
    stream {
        SubscribeEvents => subscribe_events(SubscribeEventsParams) -> SampleEvent = Subscription, Get, Query,
            "org.example.sample.subscribeEvents", "SampleEvent", "SampleError";
    }
}

const EVENT_REPLAY_WINDOW: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceSnapshot {
    value: String,
    revision: u64,
}

#[derive(Debug, Clone)]
struct ServiceState {
    source: SourceSnapshot,
    readable: ValueSnapshot,
    process_observations: VecDeque<SampleEvent>,
}

/// The implementation is separate from source persistence and from its client.
/// A projector task observes both local and externally injected source changes.
pub struct SampleService {
    state: RwLock<ServiceState>,
    source_tx: watch::Sender<SourceSnapshot>,
    source_writer: StdMutex<SourceSnapshot>,
    projection_watermark: watch::Sender<u64>,
    projection_lock: Mutex<()>,
    events: broadcast::Sender<SampleEvent>,
    middleware_log: Mutex<Vec<String>>,
    #[cfg(test)]
    last_handler_task: std::sync::Mutex<Option<u64>>,
    #[cfg(test)]
    read_gate: std::sync::Mutex<Option<Arc<Semaphore>>>,
    #[cfg(test)]
    projector_gate: std::sync::Mutex<Option<Arc<Semaphore>>>,
}

impl SampleService {
    pub fn new(initial_value: impl Into<String>) -> Arc<Self> {
        let source = SourceSnapshot {
            value: initial_value.into(),
            revision: 0,
        };
        let (source_tx, source_rx) = watch::channel(source.clone());
        let (projection_watermark, _) = watch::channel(0);
        let (events, _) = broadcast::channel(EVENT_REPLAY_WINDOW);
        let service = Arc::new(Self {
            state: RwLock::new(ServiceState {
                readable: ValueSnapshot {
                    value: source.value.clone(),
                    source_revision: 0,
                    service_revision: 0,
                },
                source: source.clone(),
                process_observations: VecDeque::new(),
            }),
            source_tx,
            source_writer: StdMutex::new(source.clone()),
            projection_watermark,
            projection_lock: Mutex::new(()),
            events,
            middleware_log: Mutex::new(Vec::new()),
            #[cfg(test)]
            last_handler_task: std::sync::Mutex::new(None),
            #[cfg(test)]
            read_gate: std::sync::Mutex::new(None),
            #[cfg(test)]
            projector_gate: std::sync::Mutex::new(None),
        });
        let worker = Arc::clone(&service);
        tokio::spawn(async move {
            worker.projector(source_rx).await;
        });
        service
    }

    async fn projector(self: Arc<Self>, mut source_rx: watch::Receiver<SourceSnapshot>) {
        loop {
            if source_rx.changed().await.is_err() {
                return;
            }
            #[cfg(test)]
            {
                let gate = self.projector_gate.lock().expect("gate mutex").clone();
                if let Some(gate) = gate
                    && gate.acquire().await.is_err()
                {
                    return;
                }
            }
            let source = source_rx.borrow().clone();
            let _guard = self.projection_lock.lock().await;
            let event = {
                let mut state = self.state.write().await;
                if source.revision <= state.readable.source_revision {
                    continue;
                }
                state.source = source.clone();
                let snapshot = ValueSnapshot {
                    value: source.value.clone(),
                    source_revision: source.revision,
                    service_revision: state.readable.service_revision + 1,
                };
                state.readable = snapshot.clone();
                let event = SampleEvent {
                    source_revision: source.revision,
                    service_revision: snapshot.service_revision,
                    value: snapshot.value.clone(),
                };
                state.process_observations.push_back(event.clone());
                while state.process_observations.len() > EVENT_REPLAY_WINDOW {
                    state.process_observations.pop_front();
                }
                event
            };
            // State and the interpreted watermark precede broadcast. There is
            // no mutation-site event and no durable semantic event journal.
            self.projection_watermark
                .send_replace(event.source_revision);
            drop(self.events.send(event));
        }
    }

    #[cfg(test)]
    fn record_handler_task(&self) {
        *self.last_handler_task.lock().expect("task mutex") =
            TEST_TASK_MARKER.try_with(|marker| *marker).ok();
    }

    #[cfg(test)]
    fn block_projector(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.projector_gate.lock().expect("gate mutex") = Some(Arc::clone(&gate));
        gate
    }

    async fn middleware<T, F>(
        &self,
        operation: Operation,
        context: &RequestContext,
        call: F,
    ) -> Result<T, SampleError>
    where
        F: std::future::Future<Output = Result<T, SampleError>>,
    {
        self.middleware_log
            .lock()
            .await
            .push(format!("{}:context", operation_name(operation)));
        if context.request_id.is_nil() || context.trace_id.is_nil() {
            return Err(SampleError::InvalidValue {
                message: "request context is incomplete".into(),
            });
        }
        self.middleware_log
            .lock()
            .await
            .push(format!("{}:validate", operation_name(operation)));
        let result = call.await;
        self.middleware_log
            .lock()
            .await
            .push(format!("{}:complete", operation_name(operation)));
        result
    }

    async fn invoke<P: Send + 'static, T: Send + 'static>(
        &self,
        operation: Operation,
        request: Request<P>,
    ) -> Result<T, SampleError>
    where
        Self: Dispatch<P, T>,
    {
        <Self as Dispatch<P, T>>::dispatch(self, operation, request).await
    }

    async fn get_value_impl(
        &self,
        request: Request<GetValueParams>,
    ) -> Result<GetValueOutput, SampleError> {
        #[cfg(test)]
        {
            self.record_handler_task();
            let gate = self.read_gate.lock().expect("gate mutex").clone();
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.expect("read gate lives");
            }
        }
        Ok(GetValueOutput {
            snapshot: self.state.read().await.readable.clone(),
            observed_context: request.context,
        })
    }

    async fn put_value_impl(
        &self,
        request: Request<PutValueParams>,
    ) -> Result<PutValueOutput, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        if request.params.value.is_empty() {
            return Err(SampleError::InvalidValue {
                message: "value cannot be empty".into(),
            });
        }
        // One source boundary allocates revisions for every writer. The
        // projector never allocates revisions, so coalescing cannot duplicate
        // or reorder source identities.
        let source = {
            let mut current = self.source_writer.lock().expect("source writer mutex");
            current.revision += 1;
            current.value = request.params.value;
            current.clone()
        };
        // This writes only the source. The independent projector observes it;
        // waiting here is a completion contract, not a direct projector call.
        self.source_tx.send_replace(source.clone());
        self.wait_projected(source.revision).await;
        Ok(PutValueOutput {
            snapshot: self.state.read().await.readable.clone(),
        })
    }

    async fn read_state_impl(
        &self,
        request: Request<ReadStateParams>,
    ) -> Result<StatePage, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        let snapshot = self.state.read().await.readable.clone();
        Ok(StatePage {
            reset_required: request.params.after_source_revision > snapshot.source_revision,
            snapshot,
        })
    }

    async fn wait_projected(&self, source_revision: u64) {
        let mut rx = self.projection_watermark.subscribe();
        while *rx.borrow() < source_revision {
            rx.changed()
                .await
                .expect("projector watermark must remain alive");
        }
    }

    async fn subscribe_impl(&self, request: Request<SubscribeEventsParams>) -> DirectSubscription {
        #[cfg(test)]
        self.record_handler_task();
        let receiver = self.events.subscribe();
        let current = self.state.read().await.readable.clone();
        let initial = (current.source_revision > request.params.after_source_revision).then_some(
            SampleEvent {
                source_revision: current.source_revision,
                service_revision: current.service_revision,
                value: current.value,
            },
        );
        DirectSubscription {
            pending: initial.into_iter().collect(),
            receiver,
            last_source_revision: request.params.after_source_revision,
        }
    }

    pub async fn middleware_log(&self) -> Vec<String> {
        self.middleware_log.lock().await.clone()
    }

    /// External synchronization uses exactly the same source/projector path as
    /// a local procedure; it does not publish directly.
    pub fn inject_source(&self, value: impl Into<String>) -> u64 {
        let value = value.into();
        let source = {
            let mut current = self.source_writer.lock().expect("source writer mutex");
            current.revision += 1;
            current.value = value;
            current.clone()
        };
        let revision = source.revision;
        self.source_tx.send_replace(source);
        revision
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SampleEvent> {
        self.events.subscribe()
    }
}

trait Dispatch<P, T> {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<P>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, SampleError>> + Send + 'a>>;
}

impl Dispatch<GetValueParams, GetValueOutput> for SampleService {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<GetValueParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GetValueOutput, SampleError>> + Send + 'a>,
    > {
        Box::pin(async move {
            debug_assert!(matches!(operation, Operation::GetValue));
            let context = request.context.clone();
            self.middleware(operation, &context, self.get_value_impl(request))
                .await
        })
    }
}
impl Dispatch<PutValueParams, PutValueOutput> for SampleService {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<PutValueParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<PutValueOutput, SampleError>> + Send + 'a>,
    > {
        Box::pin(async move {
            debug_assert!(matches!(operation, Operation::PutValue));
            let context = request.context.clone();
            self.middleware(operation, &context, self.put_value_impl(request))
                .await
        })
    }
}
impl Dispatch<ReadStateParams, StatePage> for SampleService {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<ReadStateParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<StatePage, SampleError>> + Send + 'a>,
    > {
        Box::pin(async move {
            debug_assert!(matches!(operation, Operation::ReadState));
            let context = request.context.clone();
            self.middleware(operation, &context, self.read_state_impl(request))
                .await
        })
    }
}

impl SampleService {
    async fn invoke_subscription(
        &self,
        request: Request<SubscribeEventsParams>,
    ) -> DirectSubscription {
        self.middleware_log
            .lock()
            .await
            .push("subscribeEvents:context".into());
        self.middleware_log
            .lock()
            .await
            .push("subscribeEvents:validate".into());
        let result = self.subscribe_impl(request).await;
        self.middleware_log
            .lock()
            .await
            .push("subscribeEvents:complete".into());
        result
    }
}

fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::GetValue => "getValue",
        Operation::PutValue => "putValue",
        Operation::ReadState => "readState",
        Operation::SubscribeEvents => "subscribeEvents",
    }
}

fn operation_descriptor(operation: Operation) -> XrpcOperation {
    XRPC_OPERATIONS[match operation {
        Operation::GetValue => 0,
        Operation::PutValue => 1,
        Operation::ReadState => 2,
        Operation::SubscribeEvents => 3,
    }]
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Option<serde_json::Value>,
    pub stream: Option<XrpcStream>,
}

pub type XrpcStream = mpsc::Receiver<Result<SampleEvent, SampleError>>;

#[derive(Debug, thiserror::Error)]
#[error("XRPC transport failed: {message}")]
pub struct XrpcTransportError {
    message: String,
}

#[async_trait::async_trait]
pub trait XrpcExecutor: Send + Sync {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, XrpcTransportError>;
}

fn encode_http_request<P: Serialize>(
    operation: XrpcOperation,
    request: &Request<P>,
) -> Result<HttpRequest, ClientError> {
    let params = serde_json::to_value(&request.params)
        .map_err(|error| ClientError::transport(error, false))?;
    Ok(HttpRequest {
        context: request.context.clone(),
        operation,
        query: matches!(operation.input, HttpInput::Query).then_some(params.clone()),
        body: matches!(operation.input, HttpInput::JsonBody).then_some(params),
    })
}

fn decode_http_response<T: DeserializeOwned>(response: HttpResponse) -> Result<T, ClientError> {
    let body = response
        .body
        .ok_or_else(|| ClientError::transport("XRPC response omitted body", false))?;
    if response.status >= 400 {
        let error = serde_json::from_value::<SampleError>(body)
            .map_err(|error| ClientError::transport(error, false))?;
        return Err(ClientError::Operation(error));
    }
    serde_json::from_value(body).map_err(|error| ClientError::transport(error, false))
}

/// Fake local XRPC/HTTP server used only to demonstrate canonical JSON mapping.
pub struct LocalXrpcExecutor {
    service: Arc<SampleService>,
}

impl LocalXrpcExecutor {
    pub fn new(service: Arc<SampleService>) -> Arc<Self> {
        Arc::new(Self { service })
    }
}

#[async_trait::async_trait]
impl XrpcExecutor for LocalXrpcExecutor {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, XrpcTransportError> {
        let operation = request.operation;
        match operation.kind {
            XrpcKind::Query if operation.nsid == "org.example.sample.getValue" => {
                let params: GetValueParams =
                    serde_json::from_value(request.query.unwrap_or(serde_json::Value::Null))
                        .map_err(|error| XrpcTransportError {
                            message: error.to_string(),
                        })?;
                let output = self
                    .service
                    .invoke(
                        Operation::GetValue,
                        Request {
                            context: request.context,
                            params,
                        },
                    )
                    .await;
                Ok(json_response(output))
            }
            XrpcKind::Procedure if operation.nsid == "org.example.sample.putValue" => {
                let params: PutValueParams =
                    serde_json::from_value(request.body.unwrap_or(serde_json::Value::Null))
                        .map_err(|error| XrpcTransportError {
                            message: error.to_string(),
                        })?;
                let output = self
                    .service
                    .invoke(
                        Operation::PutValue,
                        Request {
                            context: request.context,
                            params,
                        },
                    )
                    .await;
                Ok(json_response(output))
            }
            XrpcKind::Query if operation.nsid == "org.example.sample.readState" => {
                let params: ReadStateParams =
                    serde_json::from_value(request.query.unwrap_or(serde_json::Value::Null))
                        .map_err(|error| XrpcTransportError {
                            message: error.to_string(),
                        })?;
                let output = self
                    .service
                    .invoke(
                        Operation::ReadState,
                        Request {
                            context: request.context,
                            params,
                        },
                    )
                    .await;
                Ok(json_response(output))
            }
            _ => Err(XrpcTransportError {
                message: format!("unsupported sample operation {}", operation.nsid),
            }),
        }
    }
}

fn json_response<T: Serialize>(result: Result<T, SampleError>) -> HttpResponse {
    match result {
        Ok(output) => HttpResponse {
            status: 200,
            body: Some(serde_json::to_value(output).expect("sample output serializes")),
            stream: None,
        },
        Err(error) => HttpResponse {
            status: 400,
            body: Some(serde_json::to_value(error).expect("sample error serializes")),
            stream: None,
        },
    }
}

#[derive(Clone)]
enum SampleTransport {
    Direct(Arc<SampleService>),
    Irpc(irpc::Client<SampleIrpc>),
    Xrpc(Arc<dyn XrpcExecutor>),
}

#[derive(Clone)]
pub struct SampleClient {
    transport: SampleTransport,
}

impl SampleClient {
    pub fn direct(service: Arc<SampleService>) -> Self {
        Self {
            transport: SampleTransport::Direct(service),
        }
    }
    pub fn xrpc(executor: Arc<dyn XrpcExecutor>) -> Self {
        Self {
            transport: SampleTransport::Xrpc(executor),
        }
    }
}

pub struct DirectSubscription {
    pending: VecDeque<SampleEvent>,
    receiver: broadcast::Receiver<SampleEvent>,
    last_source_revision: u64,
}

impl DirectSubscription {
    async fn recv(&mut self) -> Result<Option<SampleEvent>, SampleError> {
        loop {
            let event = match self.pending.pop_front() {
                Some(event) => event,
                None => match self.receiver.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        return Err(SampleError::SubscriptionLagged { skipped });
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(None),
                },
            };
            if event.source_revision <= self.last_source_revision {
                continue;
            }
            self.last_source_revision = event.source_revision;
            return Ok(Some(event));
        }
    }
}

enum SubscriptionTransport {
    Direct(DirectSubscription),
    Irpc(channel::mpsc::Receiver<Result<SampleEvent, SampleError>>),
    Xrpc(XrpcStream),
}

pub struct SampleEventSubscription {
    inner: SubscriptionTransport,
}

impl SampleEventSubscription {
    pub async fn recv(&mut self) -> Result<Option<SampleEvent>, ClientError> {
        match &mut self.inner {
            SubscriptionTransport::Direct(subscription) => {
                subscription.recv().await.map_err(ClientError::Operation)
            }
            SubscriptionTransport::Irpc(receiver) => match receiver
                .recv()
                .await
                .map_err(|error| ClientError::transport(error, false))?
            {
                Some(Ok(event)) => Ok(Some(event)),
                Some(Err(error)) => Err(ClientError::Operation(error)),
                None => Ok(None),
            },
            SubscriptionTransport::Xrpc(receiver) => receiver
                .recv()
                .await
                .transpose()
                .map_err(ClientError::Operation),
        }
    }
}

#[derive(Default)]
struct AdapterActivity {
    admitted: AtomicUsize,
    peak_admitted: AtomicUsize,
    spawned: AtomicUsize,
    peak_spawned: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    stream_pumps: AtomicUsize,
    peak_stream_pumps: AtomicUsize,
    changed: Notify,
}

struct AdmittedHandler(Arc<AdapterActivity>);
impl AdmittedHandler {
    fn new(activity: Arc<AdapterActivity>) -> Self {
        let count = activity.admitted.fetch_add(1, Ordering::SeqCst) + 1;
        activity.peak_admitted.fetch_max(count, Ordering::SeqCst);
        activity.changed.notify_waiters();
        Self(activity)
    }
}
impl Drop for AdmittedHandler {
    fn drop(&mut self) {
        self.0.admitted.fetch_sub(1, Ordering::SeqCst);
    }
}
struct SpawnedHandler(Arc<AdapterActivity>);
impl SpawnedHandler {
    fn new(activity: Arc<AdapterActivity>) -> Self {
        let count = activity.spawned.fetch_add(1, Ordering::SeqCst) + 1;
        activity.peak_spawned.fetch_max(count, Ordering::SeqCst);
        Self(activity)
    }
}
impl Drop for SpawnedHandler {
    fn drop(&mut self) {
        self.0.spawned.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ActiveHandler(Arc<AdapterActivity>);
impl ActiveHandler {
    fn new(activity: Arc<AdapterActivity>) -> Self {
        let count = activity.active.fetch_add(1, Ordering::SeqCst) + 1;
        activity.peak.fetch_max(count, Ordering::SeqCst);
        Self(activity)
    }
}
impl Drop for ActiveHandler {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct StreamPump(Arc<AdapterActivity>);
impl StreamPump {
    fn new(activity: Arc<AdapterActivity>) -> Self {
        let count = activity.stream_pumps.fetch_add(1, Ordering::SeqCst) + 1;
        activity
            .peak_stream_pumps
            .fetch_max(count, Ordering::SeqCst);
        activity.changed.notify_waiters();
        Self(activity)
    }
}
impl Drop for StreamPump {
    fn drop(&mut self) {
        self.0.stream_pumps.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct SampleIrpcAdapter {
    dispatch_task: tokio::task::JoinHandle<()>,
    activity: Arc<AdapterActivity>,
    max_handlers: usize,
    max_stream_pumps: usize,
}

impl SampleIrpcAdapter {
    pub fn admitted_handlers(&self) -> usize {
        self.activity.admitted.load(Ordering::SeqCst)
    }
    pub fn spawned_handlers(&self) -> usize {
        self.activity.spawned.load(Ordering::SeqCst)
    }
    pub fn active_handlers(&self) -> usize {
        self.activity.active.load(Ordering::SeqCst)
    }
    pub fn peak_handlers(&self) -> usize {
        self.activity.peak.load(Ordering::SeqCst)
    }
    pub fn peak_stream_pumps(&self) -> usize {
        self.activity.peak_stream_pumps.load(Ordering::SeqCst)
    }
    pub fn stream_pumps(&self) -> usize {
        self.activity.stream_pumps.load(Ordering::SeqCst)
    }
    pub fn max_handlers(&self) -> usize {
        self.max_handlers
    }
    pub fn max_stream_pumps(&self) -> usize {
        self.max_stream_pumps
    }
    #[cfg(test)]
    async fn wait_for_stream_peak(&self, target: usize) {
        loop {
            let notified = self.activity.changed.notified();
            if self.peak_stream_pumps() >= target {
                return;
            }
            notified.await;
        }
    }
}
impl Drop for SampleIrpcAdapter {
    fn drop(&mut self) {
        self.dispatch_task.abort();
    }
}

pub fn spawn_sample_irpc(
    service: Arc<SampleService>,
    queue_capacity: usize,
    max_handlers: usize,
    max_stream_pumps: usize,
) -> (SampleClient, SampleIrpcAdapter) {
    assert!(queue_capacity > 0 && max_handlers > 0 && max_stream_pumps > 0);
    let (sender, mut receiver) = mpsc::channel(queue_capacity);
    let client = SampleClient {
        transport: SampleTransport::Irpc(irpc::Client::<SampleIrpc>::local(sender)),
    };
    let unary_permits = Arc::new(Semaphore::new(max_handlers));
    let stream_permits = Arc::new(Semaphore::new(max_stream_pumps));
    let activity = Arc::new(AdapterActivity::default());
    let task_activity = Arc::clone(&activity);
    let dispatch_task = tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            dispatch_irpc(
                Arc::clone(&service),
                Arc::clone(&unary_permits),
                Arc::clone(&stream_permits),
                Arc::clone(&task_activity),
                message,
                max_stream_pumps,
            )
            .await;
        }
    });
    (
        client,
        SampleIrpcAdapter {
            dispatch_task,
            activity,
            max_handlers,
            max_stream_pumps,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn direct_and_irpc_use_the_same_inline_middleware_without_direct_spawn() {
        let service = SampleService::new("initial");
        let direct = SampleClient::direct(Arc::clone(&service));
        let root = RequestContext::root();
        let child = root.child();
        let direct_result = TEST_TASK_MARKER
            .scope(41, direct.get_value(child.clone(), GetValueParams))
            .await
            .expect("direct query");
        assert_eq!(direct_result.observed_context, child);
        assert_eq!(
            *service.last_handler_task.lock().expect("task mutex"),
            Some(41)
        );
        let direct_log = service.middleware_log().await;
        assert_eq!(
            direct_log,
            vec!["getValue:context", "getValue:validate", "getValue:complete"]
        );

        let (irpc, adapter) = spawn_sample_irpc(Arc::clone(&service), 4, 1, 1);
        let irpc_result = irpc
            .get_value(root.child(), GetValueParams)
            .await
            .expect("irpc query");
        assert_eq!(irpc_result.snapshot.source_revision, 0);
        assert_eq!(adapter.peak_handlers(), 1);
        assert_eq!(*service.last_handler_task.lock().expect("task mutex"), None);
        assert_eq!(
            &service.middleware_log().await[3..],
            &["getValue:context", "getValue:validate", "getValue:complete"]
        );
    }

    #[tokio::test]
    async fn local_and_external_source_changes_share_projector_and_coalesce() {
        let service = SampleService::new("initial");
        let gate = service.block_projector();
        let client = SampleClient::direct(Arc::clone(&service));
        let events = service.subscribe_events();
        let _a = service.inject_source("A");
        let _b = service.inject_source("B");
        let _c = service.inject_source("C");
        gate.add_permits(1);
        tokio::task::yield_now().await;
        let mut events = events;
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("event")
            .expect("broadcast");
        assert_eq!(event.value, "C");
        let state = client
            .read_state(
                RequestContext::root(),
                ReadStateParams {
                    after_source_revision: 0,
                },
            )
            .await
            .expect("state");
        assert_eq!(state.snapshot.value, "C");
        assert_eq!(state.snapshot.source_revision, 3);
    }

    #[tokio::test]
    async fn mutation_waits_for_projector_and_external_changes_are_not_mutation_events() {
        let service = SampleService::new("initial");
        let client = SampleClient::direct(Arc::clone(&service));
        let written = client
            .put_value(
                RequestContext::root(),
                PutValueParams {
                    value: "local".into(),
                },
            )
            .await
            .expect("write");
        assert_eq!(written.snapshot.value, "local");

        let mut events = service.subscribe_events();
        let external_revision = service.inject_source("remote");
        let external_event = events.recv().await.expect("external projection event");
        assert_eq!(external_event.source_revision, external_revision);
        assert_eq!(external_event.value, "remote");
        let page = client
            .read_state(
                RequestContext::root(),
                ReadStateParams {
                    after_source_revision: external_revision,
                },
            )
            .await
            .expect("read");
        assert_eq!(page.snapshot.source_revision, external_revision);
        assert_eq!(page.snapshot.value, "remote");
    }

    #[tokio::test]
    async fn concurrent_source_writers_get_distinct_monotonic_revisions() {
        let service = SampleService::new("initial");
        let client = SampleClient::direct(Arc::clone(&service));
        let (left, right) = tokio::join!(
            client.put_value(
                RequestContext::root(),
                PutValueParams {
                    value: "left".into()
                }
            ),
            client.put_value(
                RequestContext::root(),
                PutValueParams {
                    value: "right".into()
                }
            ),
        );
        left.expect("left write");
        right.expect("right write");
        let state = client
            .read_state(
                RequestContext::root(),
                ReadStateParams {
                    after_source_revision: 0,
                },
            )
            .await
            .expect("final state");
        assert_eq!(state.snapshot.source_revision, 2);
    }

    #[tokio::test]
    async fn xrpc_query_and_procedure_transport_failures_have_different_outcomes() {
        struct FailingExecutor;
        #[async_trait::async_trait]
        impl XrpcExecutor for FailingExecutor {
            async fn execute(
                &self,
                _request: HttpRequest,
            ) -> Result<HttpResponse, XrpcTransportError> {
                Err(XrpcTransportError {
                    message: "connection lost".into(),
                })
            }
        }
        let client = SampleClient::xrpc(Arc::new(FailingExecutor));
        let query = client
            .get_value(RequestContext::root(), GetValueParams)
            .await;
        let procedure = client
            .put_value(RequestContext::root(), PutValueParams { value: "x".into() })
            .await;
        assert!(matches!(
            query,
            Err(ClientError::Transport {
                outcome_unknown: false,
                ..
            })
        ));
        assert!(matches!(
            procedure,
            Err(ClientError::Transport {
                outcome_unknown: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn canonical_xrpc_query_and_procedure_use_json_http_mapping() {
        let service = SampleService::new("initial");
        let xrpc = SampleClient::xrpc(LocalXrpcExecutor::new(Arc::clone(&service)));
        assert_eq!(XRPC_OPERATIONS[0].nsid, "org.example.sample.getValue");
        assert_eq!(XRPC_OPERATIONS[1].input, HttpInput::JsonBody);
        let read = xrpc
            .get_value(RequestContext::root(), GetValueParams)
            .await
            .expect("xrpc query");
        assert_eq!(read.snapshot.value, "initial");
        let write = xrpc
            .put_value(
                RequestContext::root(),
                PutValueParams {
                    value: "next".into(),
                },
            )
            .await
            .expect("xrpc procedure");
        assert_eq!(write.snapshot.value, "next");
    }

    #[tokio::test]
    async fn stream_capacity_is_independent_from_unary_capacity() {
        let service = SampleService::new("initial");
        let (client, adapter) = spawn_sample_irpc(Arc::clone(&service), 8, 1, 1);
        let subscription = client
            .subscribe_events(
                RequestContext::root(),
                SubscribeEventsParams {
                    after_source_revision: 0,
                },
            )
            .await
            .expect("sub");
        adapter.wait_for_stream_peak(1).await;
        let query = client
            .get_value(RequestContext::root(), GetValueParams)
            .await
            .expect("query remains available");
        assert_eq!(query.snapshot.value, "initial");
        assert_eq!(adapter.stream_pumps(), 1);
        let second = client
            .subscribe_events(
                RequestContext::root(),
                SubscribeEventsParams {
                    after_source_revision: 0,
                },
            )
            .await
            .expect("second sub request");
        let mut second = second;
        assert!(matches!(
            second.recv().await,
            Err(ClientError::Operation(
                SampleError::SubscriptionCapacityExceeded { limit: 1 }
            ))
        ));
        drop(subscription);
    }
}
