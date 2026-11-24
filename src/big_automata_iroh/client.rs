//! The calling half: [`PeerRpcClient`] is `BigSyncRpcClient` over typed clients of a host that
//! serves both protocols. It builds for the browser (`wasm32-unknown-unknown`): it names no
//! runtime, and its transport is whatever the embedder hands it, [`IrohHttp`] in the browser
//! (relay-only there) and natively too.

use big_automata::replay::wire::CloseSession;
use big_automata::replay::wire::PageSession;
use big_automata::replay::wire::SessionError;
use big_automata::replay::wire::SessionPage;
use big_automata::replay::wire::SessionUpdated;
use big_automata::replay::wire::UpdateSession;
use big_automata::sync::rpc::BigSyncRpcClient;
use big_automata::sync::rpc::BigSyncRpcResult;
use big_automata::sync::rpc::RpcError;
use big_automata::sync::rpc::bucket::BucketsError;
use big_automata::sync::rpc::bucket::GetChangedBucketsRequest;
use big_automata::sync::rpc::bucket::GetChangedBucketsResponse;
use big_automata::sync::rpc::bucket::LeafBucketsRequest;
use big_automata::sync::rpc::bucket::LeafBucketsResponse;
use big_automata::sync::rpc::summary::PartSummariesError;
use big_automata::sync::rpc::summary::PartSummariesRequest;
use big_automata::sync::rpc::summary::PartSummariesResponse;
use method_core::Unary;
use service_client_core_rs::Client;
use service_client_core_rs::UnaryTransport;
use service_client_iroh_rs::IrohHttp;
use service_core_rs::ClientError;
use service_core_rs::RequestContext;

/// `BigSyncRpcClient` over transports to a host that serves both protocols: the replay methods
/// are the replay protocol's calls, the others the sync rpc protocol's. Each call is a root
/// request context; the transport states who is calling.
///
/// `R` carries the replay protocol's three methods and `S` the sync rpc protocol's three. They
/// are one transport over a wire that serves both ([`over_iroh`](Self::over_iroh)), or two
/// bound ones (natively, a host's in-process transports).
pub struct PeerRpcClient<R, S> {
    replay: Client<R>,
    rpc: Client<S>,
}

impl<R, S> PeerRpcClient<R, S> {
    pub fn new(replay: Client<R>, rpc: Client<S>) -> Self {
        Self { replay, rpc }
    }

    /// The outcome of one call: a method's own error is the call's answer, a transport failure
    /// is the caller's `RpcError`.
    async fn call<T, P>(
        client: &Client<T>,
        params: P,
    ) -> BigSyncRpcResult<Result<P::Output, P::Error>>
    where
        P: Unary,
        T: UnaryTransport<P>,
    {
        match client.call(RequestContext::root(), params).await {
            Ok(output) => Ok(Ok(output)),
            Err(ClientError::Method(error)) => Ok(Err(error)),
            Err(ClientError::Transport { message, .. }) => Err(RpcError::TransportError(message)),
        }
    }
}

impl PeerRpcClient<IrohHttp, IrohHttp> {
    /// Both protocols over the one retained connection of `wire`: the browser's client, and a
    /// native one's too.
    pub fn over_iroh(wire: IrohHttp) -> Self {
        Self::new(Client::new(wire.clone()), Client::new(wire))
    }
}

impl<R, S> BigSyncRpcClient for PeerRpcClient<R, S>
where
    R: UnaryTransport<UpdateSession> + UnaryTransport<PageSession> + UnaryTransport<CloseSession>,
    S: UnaryTransport<PartSummariesRequest>
        + UnaryTransport<GetChangedBucketsRequest>
        + UnaryTransport<LeafBucketsRequest>,
{
    async fn part_summaries(
        &self,
        req: PartSummariesRequest,
    ) -> BigSyncRpcResult<Result<PartSummariesResponse, PartSummariesError>> {
        Self::call(&self.rpc, req).await
    }

    async fn update_replay_session(
        &self,
        req: UpdateSession,
    ) -> BigSyncRpcResult<Result<SessionUpdated, SessionError>> {
        Self::call(&self.replay, req).await
    }

    async fn page_replay_session(
        &self,
        req: PageSession,
    ) -> BigSyncRpcResult<Result<SessionPage, SessionError>> {
        Self::call(&self.replay, req).await
    }

    async fn close_replay_session(
        &self,
        req: CloseSession,
    ) -> BigSyncRpcResult<Result<(), SessionError>> {
        Self::call(&self.replay, req).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> BigSyncRpcResult<Result<GetChangedBucketsResponse, BucketsError>> {
        Self::call(&self.rpc, req).await
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> BigSyncRpcResult<Result<LeafBucketsResponse, BucketsError>> {
        Self::call(&self.rpc, req).await
    }
}
