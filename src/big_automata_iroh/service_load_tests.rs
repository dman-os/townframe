//! The service under held rounds: a round the server holds waits on its store for up to
//! `MAX_HOLD`, and neither the connection nor the other calls of the service may wait with it.
//! Replaces the old `a_cancelled_held_page_request_does_not_wedge_the_dispatch_loop` and its
//! `MAX_INFLIGHT_RPC_HANDLERS` bound.
//!
//! What bounds handler concurrency now is the router the embedder mounts the service on
//! (`XrpcRouter::with_limits`), not this crate: [`serve`](crate::serve) hosts handlers and
//! mounts nothing. The tests mount their own router, as the embedder does.
//!
//! "The server is holding the round" is observed through the store: a round's responder
//! subscribes to the store's watch when its handler starts, so a [`Watched`] store counts the
//! handlers that have started. A request that must not have started is judged by that count
//! after the other side had its chance, which can only pass vacuously, never fail spuriously.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use big_automata::BuckId;
use big_automata::BuckLevel;
use big_automata::ObjKey;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::Generation;
use big_automata::part_store::LeafPage;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::ObjState;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::PartSummary;
use big_automata::part_store::bucket_summary::BucketSummary;
use big_automata::replay::server::MAX_HOLD;
use big_automata::replay::wire::SessionPage;
use big_automata::rev::Limits;
use big_automata::rev::Rev;
use big_automata::rev::RevWatch;
use utils_rs::prelude::Res;

use super::*;

/// A [`MemoryPartStore`] that counts the watches taken on it: one per started handler that
/// builds a responder (an update, a round).
#[derive(Clone, Default)]
struct Watched {
    inner: MemoryPartStore,
    watches: Arc<AtomicUsize>,
}

impl PartStoreServe for Watched {
    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        self.inner.latest_rev()
    }

    fn watch(&self) -> RevWatch {
        self.watches.fetch_add(1, Ordering::SeqCst);
        self.inner.watch()
    }

    fn generation(&self) -> impl Future<Output = Res<Generation>> + Send {
        self.inner.generation()
    }

    fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) -> impl Future<Output = Res<()>> + Send {
        self.inner
            .read_authorized(principal, routes, after, until, limits, page)
    }

    fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> impl Future<Output = Res<Vec<Option<PartSummary>>>> + Send {
        self.inner.part_summaries(principal, parts)
    }

    fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        self.inner.readable_obj_states(principal, objs)
    }

    fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<BucketSummary>>>> + Send {
        self.inner
            .bucket_list(principal, part, from, to_level, since, limit)
    }

    fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
        byte_budget: usize,
    ) -> impl Future<Output = Res<Option<Vec<LeafPage>>>> + Send {
        self.inner
            .bucket_leaves(principal, part, bucks, since, limit, byte_budget)
    }
}

/// One service over iroh, with the one connection its client keeps.
struct Served {
    store: Watched,
    client: Arc<CoreClient<IrohHttp>>,
    router: iroh::protocol::Router,
    endpoints: [iroh::Endpoint; 2],
    stop: StopToken,
}

/// Serve a store with parts 1 and 2 readable and one object in part 1, on a router that admits
/// `handlers` handlers at once, to a client holding one connection (the server admits no
/// second one).
async fn serve(handlers: usize) -> Served {
    let server = endpoint().await;
    let client_endpoint = endpoint().await;
    let allowed = EndpointId::from_bytes(*client_endpoint.id().as_bytes());
    let store = Watched::default();
    let (replay, stop) = boot(store.clone(), principal_of(allowed));
    commit(
        &store.inner,
        vec![grant(&[(1, 0), (2, 0)]), set(&[(1, 10)]), add(&[(1, 1)])],
    )
    .await;

    let mut router = XrpcRouter::with_limits(1 << 20, handlers);
    <ReplayBinding<_> as ServerBinding<ReplayProtocol>>::mount(
        &mut router,
        ReplayBinding::new(Arc::clone(&replay.host)),
    )
    .expect("mount the replay protocol");
    <SyncRpcBinding<_> as ServerBinding<SyncRpcProtocol>>::mount(
        &mut router,
        SyncRpcBinding::new(Arc::clone(&replay.host)),
    )
    .expect("mount the sync rpc protocol");
    let server_router = iroh::protocol::Router::builder(server.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            // streams beyond the handler limit, so only the handler pool is what limits
            iroh_protocol_handler(router.into_router(), 1, 16),
        )
        .spawn();
    let client = Arc::new(CoreClient::new(IrohHttp::new(
        client_endpoint.clone(),
        server.addr(),
        1 << 20,
    )));
    Served {
        store,
        client,
        router: server_router,
        endpoints: [client_endpoint, server],
        stop,
    }
}

impl Served {
    /// A session wanting part `index`, open at the server, and the round that asks for it from
    /// the store's current head and so can only leave on its hold.
    async fn held_round(&self, session: u128, index: u8, hold_ms: u32) -> PageSession {
        let mut puller = Puller::new(session, &[part_route(index)]);
        puller.settle(&*self.client).await;
        let head = self.store.inner.latest_rev().await.unwrap();
        puller.cursors.insert(part_route(index), head);
        puller
            .machine
            .round(|route| puller.cursors.get(route).copied(), 16, hold_ms)
            .expect("the target is acknowledged")
    }

    /// Run `round` as its own task: a request future polled by the test would not be sent.
    fn start(
        &self,
        round: PageSession,
    ) -> tokio::task::JoinHandle<Result<SessionPage, SessionError>> {
        let client = Arc::clone(&self.client);
        tokio::spawn(async move { answer(client.call(RequestContext::root(), round).await) })
    }

    /// Wait until the handlers of `count` calls have started since `before`.
    async fn handlers_started(&self, before: usize, count: usize) {
        while self.store.watches.load(Ordering::SeqCst) < before + count {
            tokio::task::yield_now().await;
        }
    }

    async fn shut_down(self) {
        self.router.shutdown().await.expect("router shutdown");
        for endpoint in self.endpoints {
            endpoint.close().await;
        }
        self.stop.stop().await;
    }
}

/// One held round does not hold the connection: while it waits, every other call of both
/// protocols on the same connection is answered, and the held round is still waiting after each
/// (asked after, so an answer that only came once the hold ended would show as a finished
/// round). The round then leaves with the write it was waiting for.
#[tokio::test]
async fn a_held_round_does_not_block_other_requests_on_its_connection() {
    let served = serve(64).await;
    let round = served
        .held_round(1, 1, u32::try_from(MAX_HOLD.as_millis()).unwrap())
        .await;
    let before = served.store.watches.load(Ordering::SeqCst);
    let held = served.start(round);
    served.handlers_started(before, 1).await;

    let client = &*served.client;
    // the replay protocol: open a session, page it, close it
    let mut other = Puller::new(2, &[part_route(2)]);
    other.settle(client).await;
    assert!(!held.is_finished(), "an update waited for the held round");
    other.catch_up(client, 16).await;
    assert!(!held.is_finished(), "a round waited for the held round");
    answer(
        client
            .call(
                RequestContext::root(),
                CloseSession {
                    session_id: other.machine.session_id(),
                },
            )
            .await,
    )
    .expect("close");
    assert!(!held.is_finished(), "a close waited for the held round");
    // the other protocol
    let summaries = client
        .call(
            RequestContext::root(),
            PartSummariesRequest {
                parts: [(part(1), 0)].into(),
            },
        )
        .await
        .expect("summaries");
    assert!(summaries.contains_key(&part(1)));
    assert!(!held.is_finished(), "a summary waited for the held round");

    // a write to the part it holds releases it, with that write
    let rev = commit(&served.store.inner, vec![set(&[(2, 20)]), add(&[(2, 1)])]).await;
    let page = held.await.expect("the task").expect("the round");
    assert_eq!(page.events, [changed(rev, 2, 20)]);
    served.shut_down().await;
}

/// Handler concurrency is bounded by the router the service is mounted on, and held rounds count
/// against it: with two handlers, two held rounds take both, a third call is refused (the
/// router sheds load with a 429 rather than queueing), and a call is admitted again as soon as
/// a held round leaves.
///
/// A held round is a plain procedure (`PageSession` is not `long_poll`), so it takes its slot
/// from the same pool as every other call: a third *ordinary* call would be refused here too.
#[tokio::test]
async fn handler_concurrency_is_bounded_by_the_router_and_held_rounds_count() {
    let served = serve(2).await;
    let hold = u32::try_from(MAX_HOLD.as_millis()).unwrap();
    // sessions open before any round is held: opening one needs a handler too
    let rounds = [
        served.held_round(1, 1, hold).await,
        served.held_round(2, 2, hold).await,
        served.held_round(3, 1, hold).await,
    ];
    let before = served.store.watches.load(Ordering::SeqCst);
    let [first, second, third] = rounds;
    let (first, second) = (served.start(first), served.start(second));
    served.handlers_started(before, 2).await;

    // a third call is shed at once with the router's "full" answer, not queued behind the holds
    // and not started
    let refused = served
        .client
        .call(RequestContext::root(), third.clone())
        .await
        .expect_err("a third handler was admitted on a router of two");
    match refused {
        ClientError::Transport { message, .. } => {
            assert!(message.contains("admission is currently full"), "{message}")
        }
        other => panic!("not an admission refusal: {other}"),
    }
    assert_eq!(
        served.store.watches.load(Ordering::SeqCst),
        before + 2,
        "a third handler started beside two held rounds on a router of two"
    );

    // the part of the first round is written: it leaves, and its slot admits the third call
    commit(&served.store.inner, vec![set(&[(1, 11)])]).await;
    first.await.expect("the task").expect("the round");
    // the third was built at the head before that write, so it has rows and leaves at once
    let page = answer(served.client.call(RequestContext::root(), third).await)
        .expect("a freed slot admits the call");
    assert!(!page.events.is_empty());
    // the second still holds part 2, which nothing wrote
    assert!(!second.is_finished());
    second.abort();
    served.shut_down().await;
}
