//! The serving half: [`ReplayService`] answers the three methods of [`ReplayProtocol`] and the
//! three of its sibling [`SyncRpcProtocol`] from a part store, and [`boot`] hosts both the way
//! `big_blobs` hosts its service. It sits on `service_utils_rs`'s host and builds for the browser too.
//!
//! `Instant` is `web_time`'s, as `big_automata` takes: std's on native targets and a clock a
//! browser has.

use std::sync::Arc;
use std::sync::Mutex;
use web_time::Instant;

use big_automata::PeerKey;
use big_automata::long_poll::RuntimeTimer;
use big_automata::part_store::PartStoreServe;
use big_automata::replay::responder::Responder;
use big_automata::replay::server::ReplayServer;
use big_automata::replay::wire::CloseSession;
use big_automata::replay::wire::PageSession;
use big_automata::replay::wire::ReplayMessage;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::replay::wire::SessionError;
use big_automata::replay::wire::SessionPage;
use big_automata::replay::wire::SessionUpdated;
use big_automata::replay::wire::UpdateSession;
use big_automata::sync::rpc::SyncRpcMessage;
use big_automata::sync::rpc::SyncRpcProtocol;
use big_automata::sync::rpc::bucket::BucketsError;
use big_automata::sync::rpc::bucket::GetChangedBucketsRequest;
use big_automata::sync::rpc::bucket::GetChangedBucketsResponse;
use big_automata::sync::rpc::bucket::LeafBucketsRequest;
use big_automata::sync::rpc::bucket::LeafBucketsResponse;
use big_automata::sync::rpc::responder::RpcResponder;
use big_automata::sync::rpc::summary::PartSummariesError;
use big_automata::sync::rpc::summary::PartSummariesRequest;
use big_automata::sync::rpc::summary::PartSummariesResponse;
use method_core::ContextError;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::CallOrigin;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use service_utils_rs::TransportContext;

/// The embedder's mapping from a request to the principal it acts as.
type PrincipalOf = dyn Fn(&RequestContext, &TransportContext) -> Result<PeerKey, ContextError>
    + Send
    + Sync
    + 'static;

/// Serves replay sessions and the summary and bucket calls out of a part store.
///
/// The session table is one [`ReplayServer`] behind a [`std::sync::Mutex`] that no call holds
/// across an await: admission and the page read run on the store first, and the lock is taken
/// only for the synchronous `update`, `resolve_round` and `close`.
///
/// Expired sessions are swept at every session update, before the update is counted against
/// the per-client session cap: a client that reconnects without closing its old session (its
/// transport died) never hits the cap on sessions that already expired, and no timer task is
/// needed. Memory is bounded by the cap times the clients that updated within the TTL.
pub struct ReplayService<S> {
    store: S,
    /// The summary and bucket calls: stateless, so one answers every call.
    rpc: RpcResponder<S>,
    server: Mutex<ReplayServer>,
    /// The principal a request acts as. The iroh endpoint that carried it is not that: one
    /// endpoint may act as several principals, so the embedder decides the mapping from the
    /// [`TransportContext`] and the request's own context.
    principal_of: Arc<PrincipalOf>,
}

impl<S> ReplayService<S>
where
    S: PartStoreServe + Clone,
{
    fn principal(
        &self,
        context: &RequestContext,
        transport: &TransportContext,
    ) -> Result<PeerKey, ContextError> {
        (self.principal_of)(context, transport)
    }

    /// A responder for one call. It subscribes to the store's watch and starts with empty
    /// buffers, so a call pays for the page it builds and nothing carried over from another.
    fn responder(&self) -> Responder<S, RuntimeTimer> {
        Responder::new(self.store.clone(), RuntimeTimer)
    }

    fn server(&self) -> std::sync::MutexGuard<'_, ReplayServer> {
        self.server.lock().expect("the session table lock poisoned")
    }
}

// A store failure has no wire error to answer with (no protocol has such a variant), so it
// crashes the request: the memory store cannot fail, and a durable one's failure is not an
// outcome the client could act on.
const STORE_FAILED: &str = "the part store failed a read";

impl<S> Handler<UpdateSession> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<UpdateSession>,
    ) -> Result<ResponseEnvelope<SessionUpdated>, SessionError> {
        let principal = self.principal(&request.context, request.transport())?;
        let mut responder = self.responder();
        let admits = responder
            .admission(&principal, &request.params)
            .await
            .expect(STORE_FAILED);
        let now = Instant::now();
        let mut server = self.server();
        server.sweep(now);
        let updated = server.update(&principal, &request.params, now, admits)?;
        Ok(updated.into())
    }
}

impl<S> Handler<PageSession> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<PageSession>,
    ) -> Result<ResponseEnvelope<SessionPage>, SessionError> {
        let principal = self.principal(&request.context, request.transport())?;
        let mut responder = self.responder();
        let now = Instant::now();
        let page = responder
            .page(&principal, |round| {
                self.server()
                    .resolve_round(&principal, &request.params, now, round)
            })
            .await
            .expect(STORE_FAILED)?
            .clone();
        Ok(page.into())
    }
}

impl<S> Handler<CloseSession> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<CloseSession>,
    ) -> Result<ResponseEnvelope<()>, SessionError> {
        let principal = self.principal(&request.context, request.transport())?;
        self.server().close(&principal, &request.params);
        Ok(().into())
    }
}

impl<S> Handler<PartSummariesRequest> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<PartSummariesRequest>,
    ) -> Result<ResponseEnvelope<PartSummariesResponse>, PartSummariesError> {
        let principal = self.principal(&request.context, request.transport())?;
        let answer = self
            .rpc
            .part_summaries(&principal, request.params)
            .await
            .expect(STORE_FAILED)?;
        Ok(answer.into())
    }
}

impl<S> Handler<GetChangedBucketsRequest> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<GetChangedBucketsRequest>,
    ) -> Result<ResponseEnvelope<GetChangedBucketsResponse>, BucketsError> {
        let principal = self.principal(&request.context, request.transport())?;
        let answer = self
            .rpc
            .changed_buckets(&principal, request.params)
            .await
            .expect(STORE_FAILED)?;
        Ok(answer.into())
    }
}

impl<S> Handler<LeafBucketsRequest> for ReplayService<S>
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    async fn call(
        &self,
        request: Envelope<LeafBucketsRequest>,
    ) -> Result<ResponseEnvelope<LeafBucketsResponse>, BucketsError> {
        let principal = self.principal(&request.context, request.transport())?;
        let answer = self
            .rpc
            .leaf_buckets(&principal, request.params)
            .await
            .expect(STORE_FAILED)?;
        Ok(answer.into())
    }
}

service_utils_rs::bind_service_protocol! {
    pub struct SyncRpcBinding for SyncRpcProtocol => SyncRpcMessage {
        unary {
            PartSummaries(PartSummariesRequest),
            ChangedBuckets(GetChangedBucketsRequest),
            LeafBuckets(LeafBucketsRequest)
        }
        stream {}
    }
}

service_utils_rs::bind_service_protocol! {
    pub struct ReplayBinding for ReplayProtocol => ReplayMessage {
        unary {
            UpdateSession(UpdateSession),
            PageSession(PageSession),
            CloseSession(CloseSession)
        }
        stream {}
    }
}

/// A booted service: the implementation, the host that fronts it, and clients that call it in
/// process as the local node, one per protocol.
pub struct Replay<S> {
    pub service: Arc<ReplayService<S>>,
    pub host: Arc<Host<Arc<ReplayService<S>>>>,
    pub client: Client<ReplayProtocol>,
    pub rpc_client: Client<SyncRpcProtocol>,
}

/// Hosts the replay and sync rpc protocols over `store`. `principal_of` maps each request to the principal whose
/// access the store reads by; it is the embedder's, because what an authenticated endpoint may
/// act as is decided above this crate.
pub fn boot<S>(
    store: S,
    principal_of: impl Fn(&RequestContext, &TransportContext) -> Result<PeerKey, ContextError>
    + Send
    + Sync
    + 'static,
) -> (Replay<S>, StopToken)
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    let service = Arc::new(ReplayService {
        rpc: RpcResponder::new(store.clone()),
        store,
        server: Mutex::default(),
        principal_of: Arc::new(principal_of),
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(
        ReplayBinding::new(Arc::clone(&host)),
        CallOrigin::LocalNode,
    ));
    let rpc_client = Client::new(Transport::direct(
        SyncRpcBinding::new(Arc::clone(&host)),
        CallOrigin::LocalNode,
    ));
    (
        Replay {
            service,
            host,
            client,
            rpc_client,
        },
        host_stop,
    )
}
