use crate::Admission;
use crate::Client;
use crate::ClientError;
use crate::ContextError;
use crate::Envelope;
use crate::EventStream;
use crate::Handler;
use crate::Host;
use crate::IrpcAdapter;
use crate::Operation;
use crate::RequestContext;
use crate::ResponseEnvelope;
use crate::ResponseMetadata;
use crate::Route;
use crate::StopToken;
use crate::Subscription;
use crate::TraceContext;
use crate::TransportContext;
use crate::XrpcClient;
use crate::XrpcOperation;
use crate::XrpcRouter;
use crate::sample::GetValue;
use crate::sample::SampleProtocol;
use futures::StreamExt;
use futures::stream;
use opentelemetry::trace::SpanContext;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::trace::SpanExporter;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
struct Probe {
    nested: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Observation {
    trace: TraceContext,
    transport: String,
}

impl Operation for Probe {
    type Output = Observation;
    type Error = ContextError;
    const XRPC: XrpcOperation = XrpcOperation::query("org.example.probe.get");
}

#[derive(Debug, Serialize, Deserialize)]
struct Feed {
    ready: bool,
}

impl Operation for Feed {
    type Output = EventStream<u64, ContextError>;
    type Error = ContextError;
    const XRPC: XrpcOperation = XrpcOperation::subscription("org.example.probe.feed");
}

impl Subscription for Feed {
    type Event = u64;
}

crate::service_protocol! {
    enum Protocol => Message {
        unary { Probe(Probe) }
        stream { Feed(Feed) }
    }
}

struct ProbeService {
    dependency: Client<SampleProtocol>,
    polls: Arc<Mutex<Vec<TraceContext>>>,
    polled: Arc<Notify>,
    dropped: Arc<Notify>,
}

fn response_metadata() -> ResponseMetadata {
    ResponseMetadata {
        etag: Some("\"revision-1\"".into()),
        location: Some("/xrpc/org.example.probe.get".into()),
        cache_control: Some("private, max-age=60".into()),
        retry_after: Some("Wed, 21 Oct 2026 07:28:00 GMT".into()),
        warnings: vec![
            "199 probe \"first warning\"".into(),
            "299 probe \"second warning\"".into(),
        ],
    }
}

impl Handler<Probe> for ProbeService {
    async fn call(
        &self,
        request: Envelope<Probe>,
    ) -> Result<ResponseEnvelope<Observation>, ContextError> {
        if request.params.nested {
            self.dependency
                .call(request.context.child(), GetValue {})
                .await
                .unwrap();
        }
        tracing::info!("probe handler");
        let transport = match request.transport() {
            TransportContext::Direct => "direct".into(),
            TransportContext::Irpc => "irpc".into(),
            TransportContext::Http => "http".into(),
            TransportContext::Iroh { peer } => peer.clone(),
            TransportContext::Unknown => panic!("transport was not established"),
        };
        Ok(ResponseEnvelope {
            output: Observation {
                trace: TraceContext::current(),
                transport,
            },
            metadata: response_metadata(),
        })
    }
}

impl Handler<Feed> for ProbeService {
    async fn call(
        &self,
        request: Envelope<Feed>,
    ) -> Result<ResponseEnvelope<EventStream<u64, ContextError>>, ContextError> {
        struct OnDrop(Arc<Notify>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                tracing::info!("feed dropped");
                self.0.notify_one();
            }
        }
        let guard = OnDrop(Arc::clone(&self.dropped));
        let polls = Arc::clone(&self.polls);
        let polled = Arc::clone(&self.polled);
        let stream = stream::poll_fn(move |_| {
            let _guard = &guard;
            let mut polls = polls.lock().unwrap();
            polls.push(TraceContext::current());
            polled.notify_one();
            if request.params.ready {
                Poll::Ready(Some(Ok(polls.len() as u64)))
            } else {
                Poll::Pending
            }
        });
        Ok(ResponseEnvelope {
            output: Box::pin(stream),
            metadata: response_metadata(),
        })
    }
}

struct Runtime {
    service: Arc<ProbeService>,
    host: Arc<Host<Arc<ProbeService>>>,
    direct: Client<Protocol>,
    irpc: Client<Protocol>,
    stop: StopToken,
}

impl Runtime {
    fn boot() -> Self {
        let (sample, dependency_stop) = crate::sample::boot("initial");
        let service = Arc::new(ProbeService {
            dependency: Client::direct(sample.host),
            polls: Arc::new(Mutex::new(Vec::new())),
            polled: Arc::new(Notify::new()),
            dropped: Arc::new(Notify::new()),
        });
        let (host, host_stop) = Host::new(Arc::clone(&service));
        let direct = Client::direct(Arc::clone(&host)).with_event_buffer(1);
        let (irpc, irpc_stop) =
            IrpcAdapter::local::<_, Protocol>(Arc::clone(&host), Admission::new(2, 2), 2);
        Self {
            service,
            host,
            direct,
            irpc: Client::irpc(irpc).with_event_buffer(1),
            stop: irpc_stop.then(host_stop).then(dependency_stop),
        }
    }

    async fn http(
        &self,
    ) -> (
        Client<Protocol>,
        tokio::task::JoinHandle<std::io::Result<()>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let mut router = XrpcRouter::new(4096);
        Protocol::mount(&mut router, Arc::clone(&self.host)).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let (task, shutdown) = crate::xrpc::serve(listener, router.into_router())
            .await
            .unwrap();
        (
            Client::xrpc(XrpcClient::new(base, 4096).unwrap()),
            task,
            shutdown,
        )
    }
}

#[derive(Clone, Debug, Default)]
struct Recording(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for Recording {
    async fn export(&self, batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

fn span_context(trace: &TraceContext) -> SpanContext {
    trace.parent().unwrap().span().span_context().clone()
}

#[tokio::test]
async fn response_metadata_and_established_identity_cross_native_and_http_transports() {
    let runtime = Runtime::boot();
    let (http, task, shutdown) = runtime.http().await;
    for (name, client) in [
        ("direct", &runtime.direct),
        ("irpc", &runtime.irpc),
        ("http", &http),
    ] {
        let mut wire = serde_json::to_value(Envelope::new(
            RequestContext::root(),
            Probe { nested: false },
        ))
        .unwrap();
        wire["transport"] = serde_json::json!({ "Iroh": { "peer": "forged" } });
        let request: Envelope<Probe> = serde_json::from_value(wire).unwrap();
        let response = client.call_with_metadata(request).await.unwrap();
        assert_eq!(response.metadata, response_metadata(), "{name}");
        assert_eq!(response.output.transport, name);
    }
    for client in [&runtime.direct, &runtime.irpc] {
        let response = client
            .subscribe_with_metadata(Envelope::new(RequestContext::root(), Feed { ready: false }))
            .await
            .unwrap();
        assert_eq!(response.metadata, response_metadata());
        drop(response.output);
        runtime.service.dropped.notified().await;
    }
    shutdown.send(()).unwrap();
    task.await.unwrap().unwrap();
    runtime.stop.stop().await;
}

#[tokio::test]
async fn spans_have_real_parentage_across_direct_irpc_http_and_nested_calls() {
    let recording = Recording::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(recording.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("service-tests")));
    let (caller, observations) = async {
        let runtime = Runtime::boot();
        let (http, task, shutdown) = runtime.http().await;
        let caller = tracing::info_span!("caller");
        let caller_context = caller.context().span().span_context().clone();
        let observations = async {
            let mut observations = Vec::new();
            for client in [&runtime.direct, &runtime.irpc, &http] {
                observations.push(
                    client
                        .call(RequestContext::root(), Probe { nested: true })
                        .await
                        .unwrap(),
                );
            }
            observations
        }
        .instrument(caller)
        .await;
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
        runtime.stop.stop().await;
        (caller_context, observations)
    }
    .with_subscriber(subscriber)
    .await;
    provider.force_flush().unwrap();
    let spans = recording.0.lock().unwrap();
    let calls: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "service.call")
        .collect();
    assert_eq!(calls.len(), 6, "{calls:#?}");
    for observation in observations {
        let observed = span_context(&observation.trace);
        let parent = calls
            .iter()
            .find(|span| {
                (span.span_context.trace_id(), span.span_context.span_id())
                    == (observed.trace_id(), observed.span_id())
            })
            .unwrap();
        assert_eq!(
            (parent.span_context.trace_id(), parent.parent_span_id),
            (caller.trace_id(), caller.span_id())
        );
        assert!(
            parent
                .events
                .events
                .iter()
                .any(|event| event.name == "probe handler")
        );
        let children: Vec<_> = calls
            .iter()
            .filter(|span| span.parent_span_id == observed.span_id())
            .collect();
        assert_eq!(children.len(), 1, "{calls:#?}");
        assert_eq!(children[0].span_context.trace_id(), caller.trace_id());
    }
}

#[tokio::test]
async fn propagated_w3c_parents_and_tracestate_are_used_without_requiring_telemetry() {
    let parent = TraceContext::from_headers(
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        Some("vendor=value"),
    );
    let recording = Recording::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(recording.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("service-tests")));
    let expected = span_context(&parent);
    async {
        let runtime = Runtime::boot();
        let (http, task, shutdown) = runtime.http().await;
        for client in [&runtime.direct, &runtime.irpc, &http] {
            let mut context = RequestContext::root();
            context.trace = parent.clone();
            let response = client.call(context, Probe { nested: false }).await.unwrap();
            assert_eq!(response.trace.tracestate(), Some("vendor=value"));
            assert_eq!(
                span_context(&response.trace).trace_id(),
                expected.trace_id()
            );
        }
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
        runtime.stop.stop().await;
    }
    .with_subscriber(subscriber)
    .await;
    provider.force_flush().unwrap();
    for span in recording
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|span| span.name == "service.call")
    {
        assert_eq!(
            (span.span_context.trace_id(), span.parent_span_id),
            (expected.trace_id(), expected.span_id())
        );
    }
    for dispatch in [
        tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
        tracing::Dispatch::new(tracing_subscriber::registry()),
    ] {
        async {
            let runtime = Runtime::boot();
            let mut context = RequestContext::root();
            context.trace = parent.clone();
            runtime
                .direct
                .call(context, Probe { nested: false })
                .await
                .unwrap();
            runtime.stop.stop().await;
        }
        .with_subscriber(dispatch)
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn idle_and_backpressured_streams_keep_the_request_span_and_deadline() {
    let recording = Recording::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(recording.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("service-tests")));
    async {
        for ready in [false, true] {
            for direct in [true, false] {
                let runtime = Runtime::boot();
                let client = if direct {
                    &runtime.direct
                } else {
                    &runtime.irpc
                };
                let mut events = client
                    .subscribe(
                        RequestContext::root().with_time_budget(Duration::from_secs(5)),
                        Feed { ready },
                    )
                    .await
                    .unwrap();
                let polls_needed = if ready { 2 } else { 1 };
                while runtime.service.polls.lock().unwrap().len() < polls_needed {
                    runtime.service.polled.notified().await;
                }
                tokio::time::advance(Duration::from_secs(5)).await;
                runtime.service.dropped.notified().await;
                let polls = runtime.service.polls.lock().unwrap().clone();
                assert!(polls.iter().all(|trace| trace == &polls[0]));
                assert!(span_context(&polls[0]).is_valid());
                assert_eq!(
                    events.recv().await,
                    Err(ClientError::Operation(ContextError::DeadlineExceeded))
                );
                assert!(events.next().await.is_none());
                runtime.stop.stop().await;
            }
        }
    }
    .with_subscriber(subscriber)
    .await;
    provider.force_flush().unwrap();
    let spans = recording.0.lock().unwrap();
    let calls: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "service.call")
        .collect();
    assert_eq!(calls.len(), 4);
    assert!(calls.iter().all(|span| {
        span.events
            .events
            .iter()
            .any(|event| event.name == "feed dropped")
    }));
}

#[tokio::test(start_paused = true)]
async fn deadlines_cover_client_queueing_and_subscription_establishment() {
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let client = Client::<Protocol>::irpc(irpc::Client::local(sender));
    let pending_context = RequestContext::root();
    let pending = tokio::spawn({
        let client = client.clone();
        let context = pending_context.clone();
        async move { client.call(context, Probe { nested: false }).await }
    });
    tokio::task::yield_now().await;
    let context = RequestContext::root().with_time_budget(Duration::from_secs(5));
    assert_eq!(
        client.call(context, Probe { nested: false }).await,
        Err(ClientError::Operation(ContextError::DeadlineExceeded))
    );
    let context = RequestContext::root().with_time_budget(Duration::from_secs(5));
    assert!(matches!(
        client.subscribe(context, Feed { ready: false }).await,
        Err(ClientError::Operation(ContextError::DeadlineExceeded))
    ));
    pending_context.cancel();
    assert_eq!(
        pending.await.unwrap(),
        Err(ClientError::Operation(ContextError::Cancelled))
    );
}

#[cfg(feature = "iroh-http")]
#[tokio::test]
async fn iroh_identity_comes_from_the_connection_and_response_metadata_survives() {
    async fn endpoint() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap()
    }
    let runtime = Runtime::boot();
    let server = endpoint().await;
    let client_endpoint = endpoint().await;
    let peer = client_endpoint.id().to_string();
    let mut router = XrpcRouter::new(4096);
    Protocol::mount(&mut router, Arc::clone(&runtime.host)).unwrap();
    let server_router = iroh::protocol::Router::builder(server.clone())
        .accept(
            crate::xrpc::IROH_XRPC_HTTP_ALPN,
            crate::xrpc::iroh_protocol_handler(router.into_router(), 2, 2),
        )
        .spawn();
    let client =
        crate::xrpc::IrohXrpcClient::new(client_endpoint.clone(), server.addr(), 4096).unwrap();
    let response = client
        .execute_with_metadata::<Probe, Observation, ContextError>(
            Probe::XRPC,
            RequestContext::root(),
            &crate::HttpMetadata::default(),
            &Probe { nested: false },
        )
        .await
        .unwrap();
    assert_eq!(response.output.transport, peer);
    assert_eq!(response.metadata, response_metadata());

    let connection = client_endpoint
        .connect(server.addr(), crate::xrpc::IROH_XRPC_HTTP_ALPN)
        .await
        .unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(b"GET /xrpc/org.example.probe.get?nested=false HTTP/1.1\r\nHost: peer\r\nConnection: close\r\nX-Peer-Id: forged\r\nX-Transport-Peer: forged\r\n\r\n").await.unwrap();
    send.finish().unwrap();
    let response = recv.read_to_end(8192).await.unwrap();
    let response = std::str::from_utf8(&response).unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "response: {response:?}; connection: {:?}",
        connection.close_reason()
    );
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let observation: Observation = serde_json::from_str(body).unwrap();
    assert_eq!(observation.transport, peer);
    server_router.shutdown().await.unwrap();
    client_endpoint.close().await;
    server.close().await;
    runtime.stop.stop().await;
}
