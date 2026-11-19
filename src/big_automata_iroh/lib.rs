//! The big_automata replay protocol bound onto the service stack: [`ReplayService`] answers the
//! three methods of [`ReplayProtocol`] from a part store, and [`boot`] hosts it the way
//! `big_blobs` hosts its service.
//!
//! The protocol's laws live in `big_automata::replay`; this crate adds no protocol semantics.
//! It supplies what the sans-IO half cannot: a lock around the session table, a tokio timer for
//! held rounds, and the embedder's mapping from a request to the principal whose access the
//! store decides by.
//!
//! `Instant` is std's: `big_automata` takes its `web_time::Instant`, which is std's on every
//! native target.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use big_automata::PeerKey;
use big_automata::part_store::PartStoreServe;
use big_automata::replay::responder::Responder;
use big_automata::replay::responder::Timer;
use big_automata::replay::server::ReplayServer;
use big_automata::replay::wire::CloseSession;
use big_automata::replay::wire::PageSession;
use big_automata::replay::wire::ReplayMessage;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::replay::wire::SessionError;
use big_automata::replay::wire::SessionPage;
use big_automata::replay::wire::SessionUpdated;
use big_automata::replay::wire::UpdateSession;
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

#[cfg(test)]
mod tests;

/// The tokio sleep a held round waits on.
struct TokioTimer;

impl Timer for TokioTimer {
    fn sleep(&self, duration: std::time::Duration) -> impl Future<Output = ()> + Send + use<> {
        tokio::time::sleep(duration)
    }
}

/// The embedder's mapping from a request to the principal it acts as.
type PrincipalOf = dyn Fn(&RequestContext, &TransportContext) -> Result<PeerKey, SessionError>
    + Send
    + Sync
    + 'static;

/// Serves replay sessions out of a part store.
///
/// The session table is one [`ReplayServer`] behind a [`std::sync::Mutex`] that no call holds
/// across an await: admission and the page read run on the store first, and the lock is taken
/// only for the synchronous `update`, `resolve_round` and `close`.
///
/// FIXME: nothing calls [`ReplayServer::sweep`] yet, so a forgotten session lives until the
/// process ends. TTL sweeping needs a driver (a periodic task on the service's `StopToken`).
pub struct ReplayService<S> {
    store: S,
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
    ) -> Result<PeerKey, SessionError> {
        (self.principal_of)(context, transport)
    }

    /// A responder for one call. It subscribes to the store's watch and starts with empty
    /// buffers, so a call pays for the page it builds and nothing carried over from another.
    fn responder(&self) -> Responder<S, TokioTimer> {
        Responder::new(self.store.clone(), TokioTimer)
    }

    fn server(&self) -> std::sync::MutexGuard<'_, ReplayServer> {
        self.server.lock().expect("the session table lock poisoned")
    }
}

// A store failure has no `SessionError` to answer with (the wire has no such variant), so it
// crashes the request: the memory store cannot fail, and a durable one's failure is not a
// session outcome the client could act on.
const STORE_FAILED: &str = "the part store failed a replay read";

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
        let updated = self
            .server()
            .update(&principal, &request.params, Instant::now(), admits)?;
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

/// A booted replay service: the implementation, the host that fronts it, and a client that
/// calls it in process as the local node.
pub struct Replay<S> {
    pub service: Arc<ReplayService<S>>,
    pub host: Arc<Host<Arc<ReplayService<S>>>>,
    pub client: Client<ReplayProtocol>,
}

/// Hosts a replay service over `store`. `principal_of` maps each request to the principal whose
/// access the store reads by; it is the embedder's, because what an authenticated endpoint may
/// act as is decided above this crate.
pub fn boot<S>(
    store: S,
    principal_of: impl Fn(&RequestContext, &TransportContext) -> Result<PeerKey, SessionError>
    + Send
    + Sync
    + 'static,
) -> (Replay<S>, StopToken)
where
    S: PartStoreServe + Clone + Send + Sync + 'static,
{
    let service = Arc::new(ReplayService {
        store,
        server: Mutex::default(),
        principal_of: Arc::new(principal_of),
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(
        ReplayBinding::new(Arc::clone(&host)),
        CallOrigin::LocalNode,
    ));
    (
        Replay {
            service,
            host,
            client,
        },
        host_stop,
    )
}
