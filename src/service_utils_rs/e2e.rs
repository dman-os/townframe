//! Contracts that hold across transports, exercised against the sample
//! service. Each test boots the whole stack — service, host, IRPC adapter, and
//! an HTTP server — and talks to it through `Client<SampleProtocol>` the way a
//! consumer would, so a regression in any adapter shows up as a parity break.

use crate::{
    Admission, Client, IrpcAdapter, Route, StopToken, XrpcClient, XrpcRouter,
    sample::{Sample, SampleProtocol, boot},
    xrpc,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use url::Url;

mod irpc;
mod parity;
mod xrpc_wire;

const IRPC_QUEUE: usize = 8;
const HTTP_BODY_LIMIT: usize = 64 * 1024;

/// Every transport in front of one booted sample. `shutdown` stops in reverse
/// construction order: HTTP server, IRPC adapter, then host and service.
struct Stack {
    sample: Sample,
    admission: Arc<Admission>,
    direct: Client<SampleProtocol>,
    irpc: Client<SampleProtocol>,
    xrpc: Client<SampleProtocol>,
    http_base: Url,
    stop: StopToken,
    irpc_stop: StopToken,
    http_task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    http_shutdown: oneshot::Sender<()>,
}

impl Stack {
    async fn boot(max_handlers: usize, max_stream_pumps: usize) -> Self {
        let (sample, stop) = boot("initial");
        let admission = Admission::new(max_handlers, max_stream_pumps);
        let direct = Client::direct(Arc::clone(&sample.host));
        let (irpc_client, irpc_stop) = IrpcAdapter::local::<_, SampleProtocol>(
            Arc::clone(&sample.host),
            Arc::clone(&admission),
            IRPC_QUEUE,
        );
        let irpc = Client::irpc(irpc_client);

        let mut router = XrpcRouter::new(HTTP_BODY_LIMIT);
        SampleProtocol::mount(&mut router, Arc::clone(&sample.host)).expect("mount sample");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (http_task, http_shutdown) = xrpc::serve(listener, router.into_router())
            .await
            .expect("start HTTP server");
        let http_base = Url::parse(&format!("http://{address}/")).expect("base URL");
        let xrpc =
            Client::xrpc(XrpcClient::new(http_base.clone(), HTTP_BODY_LIMIT).expect("HTTP client"));

        Self {
            sample,
            admission,
            direct,
            irpc,
            xrpc,
            http_base,
            stop,
            irpc_stop,
            http_task,
            http_shutdown,
        }
    }

    fn transports(&self) -> [(&'static str, &Client<SampleProtocol>); 3] {
        [
            ("direct", &self.direct),
            ("irpc", &self.irpc),
            ("xrpc", &self.xrpc),
        ]
    }

    async fn shutdown(self) {
        self.http_shutdown
            .send(())
            .expect("HTTP server is still serving");
        self.http_task
            .await
            .expect("HTTP server task join")
            .expect("HTTP server exit");
        self.irpc_stop.stop().await;
        self.stop.stop().await;
    }
}

/// Blocks until the admission activity counters satisfy `done`.
async fn wait_for_activity(admission: &Admission, done: impl Fn(&crate::TaskActivity) -> bool) {
    let activity = admission.activity();
    while !done(&activity) {
        activity.wait_for_change().await;
    }
}
