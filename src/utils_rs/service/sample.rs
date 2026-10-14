//! A small reference service for the Cabinet service shape.
//!
//! The operation declaration is intentionally declarative: its XRPC identity and
//! wire semantics are the source of the generated native client and IRPC
//! optimization.  The fake HTTP executor below stands in for the real XRPC over
//! Iroh adapter; it is not a complete HTTP server.

use super::{
    CancellationReceiver, ContextError, HttpInput, HttpRequest, OwnedIrpcAdapter, Request,
    RequestContext, XrpcKind, XrpcOperation, XrpcTransportError,
};
use crate::declare_typed_service;
use irpc::channel;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, watch};

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
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("invalid value: {message}")]
    InvalidValue { message: String },
    #[error("subscription lagged by {skipped}; reread current service state")]
    SubscriptionLagged { skipped: u64 },
    #[error("subscription capacity exhausted (limit {limit})")]
    SubscriptionCapacityExceeded { limit: usize },
}

pub type ClientError = super::ClientError<SampleError>;

fn subscription_capacity_error(limit: usize) -> SampleError {
    SampleError::SubscriptionCapacityExceeded { limit }
}

declare_typed_service! {
    service = SampleService;
    operation = Operation;
    descriptors = XRPC_OPERATIONS;
    error = SampleError;
    client = SampleClient;
    transport = SampleTransport;
    irpc = SampleIrpc;
    irpc_message = SampleIrpcMessage;
    serde_module = sample_typed_serde;
    subscription = SampleEventSubscription;
    direct_subscription = DirectSubscription;
    subscription_transport = SubscriptionTransport;
    executor = XrpcExecutor;
    request_encoder = encode_http_request;
    response_decoder = decode_http_response;
    stream_decoder = decode_http_stream_response;
    event_window = EVENT_REPLAY_WINDOW;
    dispatch = dispatch_irpc;
    dispatch_loop = dispatch_irpc_loop;
    stream_capacity_error = subscription_capacity_error;
    unary {
        GetValue => get_value / get_value_with_metadata(GetValueParams) -> GetValueOutput = Query, Get, Query,
            "org.example.sample.getValue", "GetValueOutput", "SampleError";
        PutValue => put_value / put_value_with_metadata(PutValueParams) -> PutValueOutput = Procedure, Post, JsonBody,
            "org.example.sample.putValue", "PutValueOutput", "SampleError";
        ReadState => read_state / read_state_with_metadata(ReadStateParams) -> StatePage = Query, Get, Query,
            "org.example.sample.readState", "StatePage", "SampleError";
    }
    stream {
        SubscribeEvents => subscribe_events / subscribe_events_with_metadata(SubscribeEventsParams) -> SampleEvent = Subscription, Get, Query,
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
    shutdown_tx: watch::Sender<bool>,
    projector_task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    projection_watermark: watch::Sender<u64>,
    projection_lock: Mutex<()>,
    events: broadcast::Sender<SampleEvent>,
    middleware_log: Mutex<Vec<String>>,
    #[cfg(test)]
    last_handler_task: std::sync::Mutex<Option<u64>>,
    #[cfg(test)]
    read_gate: std::sync::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    #[cfg(test)]
    projector_gate: std::sync::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}

impl SampleService {
    pub fn new(initial_value: impl Into<String>) -> Arc<Self> {
        let source = SourceSnapshot {
            value: initial_value.into(),
            revision: 0,
        };
        let (source_tx, source_rx) = watch::channel(source.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
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
            shutdown_tx,
            projector_task: StdMutex::new(None),
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
        let projector_task = tokio::spawn(async move {
            worker.projector(source_rx, shutdown_rx).await;
        });
        *service.projector_task.lock().expect("projector task mutex") = Some(projector_task);
        service
    }

    async fn projector(
        self: Arc<Self>,
        mut source_rx: watch::Receiver<SourceSnapshot>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                changed = source_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                    continue;
                }
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
    fn block_projector(&self) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
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
        context.validate().map_err(SampleError::Context)?;
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

    /// Stops and joins the owned projector task. Callers that retain a service
    /// beyond a runtime shutdown use this to make task completion observable.
    pub async fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
        let task = self
            .projector_task
            .lock()
            .expect("projector task mutex")
            .take();
        if let Some(task) = task {
            task.await.expect("projector task must stop cleanly");
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

impl super::UnaryServiceDispatch<Operation, GetValueParams, GetValueOutput, SampleError>
    for SampleService
{
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<GetValueParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GetValueOutput, SampleError>> + Send + 'a>,
    > {
        <Self as Dispatch<GetValueParams, GetValueOutput>>::dispatch(self, operation, request)
    }
}

impl super::UnaryServiceDispatch<Operation, PutValueParams, PutValueOutput, SampleError>
    for SampleService
{
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<PutValueParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<PutValueOutput, SampleError>> + Send + 'a>,
    > {
        <Self as Dispatch<PutValueParams, PutValueOutput>>::dispatch(self, operation, request)
    }
}

impl super::UnaryServiceDispatch<Operation, ReadStateParams, StatePage, SampleError>
    for SampleService
{
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<ReadStateParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<StatePage, SampleError>> + Send + 'a>,
    > {
        <Self as Dispatch<ReadStateParams, StatePage>>::dispatch(self, operation, request)
    }
}

impl super::StreamServiceDispatch<Operation, SubscribeEventsParams, DirectSubscription, SampleError>
    for SampleService
{
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<SubscribeEventsParams>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DirectSubscription, SampleError>> + Send + 'a>,
    > {
        Box::pin(async move {
            debug_assert!(matches!(operation, Operation::SubscribeEvents));
            let context = request.context.clone();
            let subscription =
                async move { Ok::<_, SampleError>(self.subscribe_impl(request).await) };
            self.middleware(Operation::SubscribeEvents, &context, subscription)
                .await
        })
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

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Option<serde_json::Value>,
    pub stream: Option<XrpcStream>,
}

pub type XrpcStream = mpsc::Receiver<Result<SampleEvent, SampleError>>;

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
        metadata: request.metadata.clone(),
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

fn decode_http_stream_response(response: HttpResponse) -> Result<XrpcStream, ClientError> {
    if response.status >= 400 {
        let body = response
            .body
            .ok_or_else(|| ClientError::transport("XRPC response omitted body", false))?;
        let error = serde_json::from_value::<SampleError>(body)
            .map_err(|error| ClientError::transport(error, false))?;
        return Err(ClientError::Operation(error));
    }
    response
        .stream
        .ok_or_else(|| ClientError::transport("XRPC response was not a stream", false))
}

/// Fake local XRPC/HTTP server used only to demonstrate canonical JSON mapping.
pub struct LocalXrpcExecutor {
    service: Arc<SampleService>,
    stream_tasks: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl LocalXrpcExecutor {
    pub fn new(service: Arc<SampleService>) -> Arc<Self> {
        Arc::new(Self {
            service,
            stream_tasks: StdMutex::new(Vec::new()),
        })
    }

    pub async fn shutdown(&self) {
        let tasks = std::mem::take(&mut *self.stream_tasks.lock().expect("stream task mutex"));
        for task in tasks {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("XRPC stream task failed: {error}"),
            }
        }
    }
}

impl Drop for LocalXrpcExecutor {
    fn drop(&mut self) {
        let tasks = self.stream_tasks.get_mut().expect("stream task mutex");
        for task in tasks.drain(..) {
            task.abort();
        }
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
                        Request::with_metadata(
                            request.context.for_receiver(),
                            request.metadata.clone(),
                            params,
                        ),
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
                        Request::with_metadata(
                            request.context.for_receiver(),
                            request.metadata.clone(),
                            params,
                        ),
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
                        Request::with_metadata(
                            request.context.for_receiver(),
                            request.metadata.clone(),
                            params,
                        ),
                    )
                    .await;
                Ok(json_response(output))
            }
            XrpcKind::Subscription if operation.nsid == "org.example.sample.subscribeEvents" => {
                let params: SubscribeEventsParams =
                    serde_json::from_value(request.query.unwrap_or(serde_json::Value::Null))
                        .map_err(|error| XrpcTransportError {
                            message: error.to_string(),
                        })?;
                let mut subscription = match <SampleService as super::StreamServiceDispatch<
                    Operation,
                    SubscribeEventsParams,
                    DirectSubscription,
                    SampleError,
                >>::dispatch(
                    self.service.as_ref(),
                    Operation::SubscribeEvents,
                    Request::with_metadata(
                        request.context.for_receiver(),
                        request.metadata,
                        params,
                    ),
                )
                .await
                {
                    Ok(subscription) => subscription,
                    Err(error) => return Ok(json_response::<()>(Err(error))),
                };
                let (sender, receiver) = mpsc::channel(EVENT_REPLAY_WINDOW);
                let task = tokio::spawn(async move {
                    loop {
                        let item = match subscription.recv().await {
                            Ok(Some(event)) => Ok(event),
                            Ok(None) => break,
                            Err(error) => Err(error),
                        };
                        let terminal = item.is_err();
                        if sender.send(item).await.is_err() || terminal {
                            break;
                        }
                    }
                });
                self.stream_tasks
                    .lock()
                    .expect("stream task mutex")
                    .push(task);
                Ok(HttpResponse {
                    status: 200,
                    body: None,
                    stream: Some(receiver),
                })
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
    Irpc {
        receiver: channel::mpsc::Receiver<Result<SampleEvent, SampleError>>,
        cancellation: Option<crate::service::CancellationRequestGuard>,
    },
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
            SubscriptionTransport::Irpc {
                receiver,
                cancellation: _cancellation,
            } => {
                match receiver
                    .recv()
                    .await
                    .map_err(|error| ClientError::transport(error, false))?
                {
                    Some(Ok(event)) => Ok(Some(event)),
                    Some(Err(error)) => Err(ClientError::Operation(error)),
                    None => Ok(None),
                }
            }
            SubscriptionTransport::Xrpc(receiver) => receiver
                .recv()
                .await
                .transpose()
                .map_err(ClientError::Operation),
        }
    }
}

pub struct SampleIrpcAdapter {
    dispatch_task: tokio::task::JoinHandle<()>,
    owned: Arc<OwnedIrpcAdapter>,
}

impl SampleIrpcAdapter {
    pub fn admitted_handlers(&self) -> usize {
        self.owned.activity().admitted_count()
    }
    pub fn spawned_handlers(&self) -> usize {
        self.owned.activity().spawned_count()
    }
    pub fn active_handlers(&self) -> usize {
        self.owned.activity().active_count()
    }
    pub fn peak_handlers(&self) -> usize {
        self.owned.activity().peak_count()
    }
    pub fn peak_stream_pumps(&self) -> usize {
        self.owned.activity().peak_stream_pump_count()
    }
    pub fn stream_pumps(&self) -> usize {
        self.owned.activity().stream_pump_count()
    }
    pub fn max_handlers(&self) -> usize {
        self.owned.max_handlers()
    }
    pub fn max_stream_pumps(&self) -> usize {
        self.owned.max_stream_pumps()
    }
    #[cfg(test)]
    async fn wait_for_stream_peak(&self, target: usize) {
        loop {
            if self.peak_stream_pumps() >= target {
                return;
            }
            self.owned.activity().wait_for_change().await;
        }
    }

    /// Stops the dispatcher and all admitted child handlers, joining every
    /// owned task so no request can outlive the transport adapter.
    pub async fn shutdown(&mut self) {
        self.dispatch_task.abort();
        match (&mut self.dispatch_task).await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => panic!("IRPC dispatcher failed: {error}"),
        }
        self.owned.shutdown_children().await;
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
    spawn_sample_irpc_with_owned(
        service,
        queue_capacity,
        OwnedIrpcAdapter::new(max_handlers, max_stream_pumps),
    )
}

/// Spawns the native Sample adapter with a transport-scoped cancellation
/// receiver. Cancellation commands are applied only to requests admitted by
/// this adapter; shutdown still joins the receiver and all operation tasks.
pub fn spawn_sample_irpc_with_cancellation(
    service: Arc<SampleService>,
    queue_capacity: usize,
    max_handlers: usize,
    max_stream_pumps: usize,
    cancellation: CancellationReceiver,
) -> (SampleClient, SampleIrpcAdapter) {
    spawn_sample_irpc_with_owned(
        service,
        queue_capacity,
        OwnedIrpcAdapter::new_with_cancellation(max_handlers, max_stream_pumps, cancellation),
    )
}

fn spawn_sample_irpc_with_owned(
    service: Arc<SampleService>,
    queue_capacity: usize,
    owned: Arc<OwnedIrpcAdapter>,
) -> (SampleClient, SampleIrpcAdapter) {
    assert!(queue_capacity > 0);
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let client = SampleClient {
        transport: SampleTransport::Irpc(irpc::Client::<SampleIrpc>::local(sender)),
    };
    let task_owned = Arc::clone(&owned);
    let dispatch_task = tokio::spawn(dispatch_irpc_loop(service, task_owned, receiver));
    (
        client,
        SampleIrpcAdapter {
            dispatch_task,
            owned,
        },
    )
}

#[cfg(test)]
mod tests;
