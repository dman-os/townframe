extern crate renamed_utils;

use renamed_utils::declare_typed_service;
#[allow(unused_imports)]
use renamed_utils::service::__serde as serde;
use renamed_utils::service::__tokio::sync::mpsc;
use renamed_utils::service::{
    ClientError, ContextError, HttpMetadata, HttpRequest, OwnedIrpcAdapter, Request,
    RequestContext, StreamServiceDispatch, UnaryServiceDispatch, XrpcOperation, XrpcTransportError,
};
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(
    Clone,
    Debug,
    renamed_utils::service::__serde::Serialize,
    renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
struct FixtureQuery {
    value: String,
}

#[derive(
    Clone,
    Debug,
    renamed_utils::service::__serde::Serialize,
    renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
struct FixtureProcedure {
    value: String,
}

#[derive(
    Clone,
    Debug,
    renamed_utils::service::__serde::Serialize,
    renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
struct FixtureSubscriptionParams {
    after: u64,
}

#[derive(
    Clone,
    Debug,
    renamed_utils::service::__serde::Serialize,
    renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
struct FixtureOutput {
    value: String,
}

#[derive(
    Clone,
    Debug,
    renamed_utils::service::__serde::Serialize,
    renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
struct FixtureEvent {
    value: String,
}

#[derive(
    Debug, renamed_utils::service::__serde::Serialize, renamed_utils::service::__serde::Deserialize,
)]
#[serde(crate = "renamed_utils::service::__serde")]
enum FixtureError {
    Context(ContextError),
    Failed,
    Capacity(usize),
}

impl From<ContextError> for FixtureError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => write!(formatter, "fixture context: {error}"),
            Self::Failed => formatter.write_str("fixture failure"),
            Self::Capacity(limit) => write!(formatter, "fixture stream capacity: {limit}"),
        }
    }
}

impl std::error::Error for FixtureError {}

struct FixtureService;

impl UnaryServiceDispatch<FixtureOperation, FixtureQuery, FixtureOutput, FixtureError>
    for FixtureService
{
    fn dispatch<'a>(
        &'a self,
        _operation: FixtureOperation,
        request: Request<FixtureQuery>,
    ) -> Pin<Box<dyn Future<Output = Result<FixtureOutput, FixtureError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(FixtureOutput {
                value: request.params.value,
            })
        })
    }
}

impl UnaryServiceDispatch<FixtureOperation, FixtureProcedure, FixtureOutput, FixtureError>
    for FixtureService
{
    fn dispatch<'a>(
        &'a self,
        _operation: FixtureOperation,
        request: Request<FixtureProcedure>,
    ) -> Pin<Box<dyn Future<Output = Result<FixtureOutput, FixtureError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(FixtureOutput {
                value: request.params.value,
            })
        })
    }
}

struct FixtureDirectSubscription {
    receiver: mpsc::Receiver<Result<FixtureEvent, FixtureError>>,
}

impl FixtureDirectSubscription {
    async fn recv(&mut self) -> Result<Option<FixtureEvent>, FixtureError> {
        self.receiver.recv().await.transpose()
    }
}

impl
    StreamServiceDispatch<
        FixtureOperation,
        FixtureSubscriptionParams,
        FixtureDirectSubscription,
        FixtureError,
    > for FixtureService
{
    fn dispatch<'a>(
        &'a self,
        _operation: FixtureOperation,
        request: Request<FixtureSubscriptionParams>,
    ) -> Pin<Box<dyn Future<Output = Result<FixtureDirectSubscription, FixtureError>> + Send + 'a>>
    {
        Box::pin(async move {
            let (sender, receiver) = mpsc::channel(1);
            sender
                .send(Ok(FixtureEvent {
                    value: request.params.after.to_string(),
                }))
                .await
                .expect("fixture receiver remains open");
            Ok(FixtureDirectSubscription { receiver })
        })
    }
}

enum FixtureSubscriptionTransport {
    Direct(FixtureDirectSubscription),
    Irpc {
        receiver: irpc::channel::mpsc::Receiver<Result<FixtureEvent, FixtureError>>,
        cancellation: Option<renamed_utils::service::CancellationRequestGuard>,
    },
    Xrpc(mpsc::Receiver<Result<FixtureEvent, FixtureError>>),
}

struct FixtureSubscription {
    inner: FixtureSubscriptionTransport,
}

impl FixtureSubscription {
    async fn recv(&mut self) -> Result<Option<FixtureEvent>, ClientError<FixtureError>> {
        match &mut self.inner {
            FixtureSubscriptionTransport::Direct(subscription) => {
                subscription.recv().await.map_err(ClientError::Operation)
            }
            FixtureSubscriptionTransport::Irpc {
                receiver,
                cancellation: _cancellation,
            } => receiver
                .recv()
                .await
                .map_err(|error| ClientError::transport(error, true))?
                .transpose()
                .map_err(ClientError::Operation),
            FixtureSubscriptionTransport::Xrpc(receiver) => receiver
                .recv()
                .await
                .transpose()
                .map_err(ClientError::Operation),
        }
    }
}

struct FixtureResponse;
struct FixtureExecutorImpl;

trait FixtureExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        _request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FixtureResponse, XrpcTransportError>> + Send + 'a>>;
}

impl FixtureExecutor for FixtureExecutorImpl {
    fn execute<'a>(
        &'a self,
        _request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FixtureResponse, XrpcTransportError>> + Send + 'a>>
    {
        Box::pin(async {
            Err(XrpcTransportError {
                message: "fixture fake transport".into(),
            })
        })
    }
}

fn encode_fixture<P>(
    operation: XrpcOperation,
    request: &Request<P>,
) -> Result<HttpRequest, ClientError<FixtureError>> {
    Ok(HttpRequest {
        context: request.context.clone(),
        metadata: request.metadata.clone(),
        operation,
        query: None,
        body: None,
    })
}

fn decode_fixture<T>(_response: FixtureResponse) -> Result<T, ClientError<FixtureError>> {
    Err(ClientError::transport("fixture decoder", false))
}

fn decode_fixture_stream(
    _response: FixtureResponse,
) -> Result<mpsc::Receiver<Result<FixtureEvent, FixtureError>>, ClientError<FixtureError>> {
    Err(ClientError::transport("fixture stream decoder", true))
}

fn fixture_capacity(limit: usize) -> FixtureError {
    FixtureError::Capacity(limit)
}

declare_typed_service! {
    service = FixtureService;
    operation = FixtureOperation;
    descriptors = FIXTURE_OPERATIONS;
    error = FixtureError;
    client = FixtureClient;
    transport = FixtureTransport;
    irpc = FixtureIrpc;
    irpc_message = FixtureIrpcMessage;
    serde_module = fixture_typed_serde;
    subscription = FixtureSubscription;
    direct_subscription = FixtureDirectSubscription;
    subscription_transport = FixtureSubscriptionTransport;
    executor = FixtureExecutor;
    request_encoder = encode_fixture;
    response_decoder = decode_fixture;
    stream_decoder = decode_fixture_stream;
    event_window = 1;
    dispatch = dispatch_fixture_irpc;
    dispatch_loop = dispatch_fixture_irpc_loop;
    stream_capacity_error = fixture_capacity;
    unary {
        Query => query / query_with_metadata(FixtureQuery) -> FixtureOutput = Query, Get, Query,
            "org.example.fixture.query", "FixtureOutput", "FixtureError";
        Procedure => procedure / procedure_with_metadata(FixtureProcedure) -> FixtureOutput = Procedure, Post, JsonBody,
            "org.example.fixture.procedure", "FixtureOutput", "FixtureError";
    }
    stream {
        Subscribe => subscribe / subscribe_with_metadata(FixtureSubscriptionParams) -> FixtureEvent = Subscription, Get, Query,
            "org.example.fixture.subscribe", "FixtureEvent", "FixtureError";
    }
}

async fn exercise_public_api() {
    let direct = FixtureClient::direct(Arc::new(FixtureService));
    let output = direct
        .query(
            RequestContext::root(),
            FixtureQuery {
                value: "direct-query".into(),
            },
        )
        .await
        .expect("direct query dispatch");
    assert_eq!(output.value, "direct-query");

    let output = direct
        .procedure_with_metadata(Request::with_metadata(
            RequestContext::root(),
            HttpMetadata::default(),
            FixtureProcedure {
                value: "direct-procedure".into(),
            },
        ))
        .await
        .expect("direct procedure dispatch");
    assert_eq!(output.value, "direct-procedure");

    let mut subscription = direct
        .subscribe(
            RequestContext::root(),
            FixtureSubscriptionParams { after: 7 },
        )
        .await
        .expect("direct subscription dispatch");
    assert_eq!(subscription.recv().await.unwrap().unwrap().value, "7");

    let (sender, receiver) = mpsc::channel(8);
    let irpc_client = FixtureClient::irpc(irpc::Client::local(sender));
    let adapter = OwnedIrpcAdapter::new(2, 1);
    let dispatch_task = renamed_utils::service::__tokio::spawn(dispatch_fixture_irpc_loop(
        Arc::new(FixtureService),
        Arc::clone(&adapter),
        receiver,
    ));
    let output = irpc_client
        .query(
            RequestContext::root(),
            FixtureQuery {
                value: "irpc-query".into(),
            },
        )
        .await
        .expect("IRPC query dispatch");
    assert_eq!(output.value, "irpc-query");
    drop(irpc_client);
    dispatch_task.await.expect("IRPC dispatch loop");
    assert_eq!(adapter.child_count(), 0);

    let xrpc = FixtureClient::xrpc(Arc::new(FixtureExecutorImpl));
    assert!(matches!(
        xrpc.query(
            RequestContext::root(),
            FixtureQuery {
                value: "xrpc-query".into(),
            },
        )
        .await,
        Err(ClientError::Transport { .. })
    ));
}

fn main() {
    let runtime = renamed_utils::service::__tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime");
    runtime.block_on(exercise_public_api());
}
