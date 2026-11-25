//! The replay protocol through the hosted service: a typed client drives a `ReplayClient`
//! machine against a `ReplayService` over a `MemoryPartStore`, in process and over iroh.

use std::collections::BTreeMap;

use big_automata::PeerKey;
use big_automata::part_store::laws::add;
use big_automata::part_store::laws::grant;
use big_automata::part_store::laws::obj;
use big_automata::part_store::laws::part;
use big_automata::part_store::laws::payload;
use big_automata::part_store::laws::principal;
use big_automata::part_store::laws::revoke;
use big_automata::part_store::laws::set;
use big_automata::part_store::laws::stage;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::part_store::oracle::PartWrite;
use big_automata::replay::client::ReplayClient;
use big_automata::replay::client::RoundOutcome;
use big_automata::replay::client::TargetOutcome;
use big_automata::replay::client::UpdateOutcome;
use big_automata::replay::wire::CloseSession;
use big_automata::replay::wire::PageEvent;
use big_automata::replay::wire::PageSession;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::replay::wire::Route;
use big_automata::replay::wire::SessionError;
use big_automata::replay::wire::TargetId;
use big_automata::replay::wire::UpdateSession;
use service_client_core_rs::Client as CoreClient;
use service_client_core_rs::UnaryTransport;
use service_client_iroh_rs::IROH_XRPC_HTTP_ALPN;
use service_client_iroh_rs::IrohHttp;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::CallOrigin;
use service_utils_rs::ClientError;
use service_utils_rs::EndpointId;
use service_utils_rs::RequestContext;
use service_utils_rs::ServerBinding;
use service_utils_rs::TransportContext;
use service_utils_rs::xrpc::XrpcRouter;
use service_utils_rs::xrpc::iroh_protocol_handler;
use utils_rs::prelude::Uuid;

use super::*;

/// The one principal every test client acts as, so the grants and revokes below are its own.
fn client_principal() -> PeerKey {
    principal(0)
}

/// The embedder's mapping under test: the local node and the one endpoint the test lets in act as
/// [`client_principal`]; any other endpoint is refused before the store is read.
fn principal_of(
    allowed: EndpointId,
) -> impl Fn(&RequestContext, &TransportContext) -> Result<PeerKey, SessionError> {
    move |_, transport| match transport {
        TransportContext::Direct {
            origin: CallOrigin::LocalNode,
        } => Ok(client_principal()),
        TransportContext::Direct {
            origin: CallOrigin::Endpoint(endpoint),
        }
        | TransportContext::Iroh { endpoint }
            if *endpoint == allowed =>
        {
            Ok(client_principal())
        }
        _ => Err(method_core::ContextError::Unauthenticated.into()),
    }
}

fn booted(allowed: EndpointId) -> (MemoryPartStore, Replay<MemoryPartStore>, StopToken) {
    let store = MemoryPartStore::default();
    let (replay, stop) = boot(store.clone(), principal_of(allowed));
    (store, replay, stop)
}

/// A stand-in endpoint no test client claims to be.
fn stranger() -> EndpointId {
    EndpointId::from_bytes([9; 32])
}

async fn commit(store: &MemoryPartStore, writes: Vec<PartWrite>) -> u64 {
    let mut tx = store.begin();
    for write in writes {
        stage(store, &mut tx, write).await.unwrap();
    }
    tx.commit().expect("the writes change a row")
}

fn part_route(index: u8) -> Route {
    Route::Part {
        part_key: part(index),
    }
}

/// What a method answered, with a transport failure (which no test provokes) crashing the test.
fn answer<T>(result: Result<T, ClientError<SessionError>>) -> Result<T, SessionError> {
    match result {
        Ok(value) => Ok(value),
        Err(ClientError::Method(error)) => Err(error),
        Err(error @ ClientError::Transport { .. }) => panic!("the transport failed: {error}"),
    }
}

/// One replay client's side of a peer: the machine, the cursors it owns (the machine keeps
/// none, law 9) and every row it has received.
struct Puller {
    machine: ReplayClient,
    cursors: BTreeMap<Route, u64>,
    received: Vec<PageEvent>,
}

impl Puller {
    fn new(session: u128, wanted: &[Route]) -> Self {
        let mut machine = ReplayClient::new(Uuid::from_u128(session));
        for route in wanted {
            machine.want(route.clone());
        }
        Self {
            machine,
            cursors: default_cursors(wanted),
            received: Vec::new(),
        }
    }

    /// Send updates until the machine has none to send.
    async fn settle<T>(&mut self, client: &CoreClient<T>)
    where
        T: UnaryTransport<UpdateSession>,
    {
        while let Some(update) = self.machine.next_update().cloned() {
            let reply = answer(client.call(RequestContext::root(), update.clone()).await);
            let outcome = self
                .machine
                .update_answered(&update, &reply)
                .expect("the server answers correctly");
            assert!(
                matches!(outcome, UpdateOutcome::Applied { .. }),
                "{outcome:?}"
            );
        }
    }

    /// One round over every acknowledged target from its cursor, `limit` rows at most. Applies
    /// the answer: received rows and advanced cursors. `None` when no target is acknowledged.
    async fn round<T>(
        &mut self,
        client: &CoreClient<T>,
        limit: u32,
    ) -> Option<Vec<(TargetId, Route, TargetOutcome)>>
    where
        T: UnaryTransport<PageSession>,
    {
        let cursors = &self.cursors;
        let request = self
            .machine
            .round(|route| cursors.get(route).copied(), limit, 0)?;
        let reply = answer(client.call(RequestContext::root(), request.clone()).await);
        let mut outcomes = Vec::new();
        let round = self
            .machine
            .round_answered(&request, &reply, &mut outcomes)
            .expect("the server answers correctly");
        assert_eq!(round, RoundOutcome::Answered);
        let page = reply.expect("an answered round carries its page");
        self.received.extend(page.events);
        for (_, route, outcome) in &outcomes {
            if let TargetOutcome::Advance { resume, .. } = outcome {
                self.cursors.insert(route.clone(), *resume);
            }
        }
        Some(outcomes)
    }

    /// Pages until every target reports drained; returns how many rounds that took.
    async fn catch_up<T>(&mut self, client: &CoreClient<T>, limit: u32) -> usize
    where
        T: UnaryTransport<PageSession>,
    {
        let mut rounds = 0;
        loop {
            let outcomes = self.round(client, limit).await.expect("a target is held");
            rounds += 1;
            assert!(
                outcomes
                    .iter()
                    .all(|(_, _, outcome)| matches!(outcome, TargetOutcome::Advance { .. })),
                "{outcomes:?}"
            );
            if outcomes.iter().all(|(_, _, outcome)| {
                matches!(outcome, TargetOutcome::Advance { drained: true, .. })
            }) {
                return rounds;
            }
            assert!(rounds < 16, "the pull does not converge");
        }
    }
}

fn default_cursors(wanted: &[Route]) -> BTreeMap<Route, u64> {
    wanted.iter().map(|route| (route.clone(), 0)).collect()
}

fn changed(rev: u64, index: u8, value: u8) -> PageEvent {
    PageEvent::Changed {
        rev,
        obj_key: obj(index),
        targets: vec![0],
        payload: payload(value),
    }
}

/// The store's two commits every pull test reads back: object 1 at one rev, object 2 at the next,
/// both members of part 1, which the client's principal may read.
async fn seed(store: &MemoryPartStore) -> (u64, u64) {
    let first = commit(
        store,
        vec![grant(&[(1, 0)]), set(&[(1, 10)]), add(&[(1, 1)])],
    )
    .await;
    let second = commit(store, vec![set(&[(2, 20)]), add(&[(2, 1)])]).await;
    (first, second)
}

#[tokio::test]
async fn a_client_machine_pulls_a_part_from_the_hosted_service() {
    let (store, replay, stop) = booted(stranger());
    let (first, second) = seed(&store).await;

    let mut puller = Puller::new(1, &[part_route(1)]);
    puller.settle(&replay.client).await;
    // One row's worth per round, and the two commits are two revs: the pull takes two rounds
    // and one more to see it is caught up.
    let rounds = puller.catch_up(&replay.client, 1).await;
    assert!(rounds >= 2, "{rounds}");

    assert_eq!(
        puller.received,
        [changed(first, 1, 10), changed(second, 2, 20)]
    );
    assert_eq!(puller.cursors[&part_route(1)], second);

    // A write after the pull is the next round's row, from the cursor the client kept.
    let third = commit(&store, vec![set(&[(2, 21)])]).await;
    puller.received.clear();
    puller.catch_up(&replay.client, 8).await;
    assert_eq!(puller.received, [changed(third, 2, 21)]);

    answer(
        replay
            .client
            .call(
                RequestContext::root(),
                CloseSession {
                    session_id: puller.machine.session_id(),
                },
            )
            .await,
    )
    .expect("close");
    stop.stop().await;
}

#[tokio::test]
async fn an_unreadable_part_is_refused_at_the_update_and_a_revoke_refuses_it_at_the_round() {
    let (store, replay, stop) = booted(stranger());
    seed(&store).await;

    // Part 2 is known to no one: admission refuses it without saying why.
    let mut puller = Puller::new(2, &[part_route(1), part_route(2)]);
    let update = puller.machine.next_update().cloned().expect("an update");
    let updated = answer(
        replay
            .client
            .call(RequestContext::root(), update.clone())
            .await,
    )
    .expect("update");
    assert_eq!(updated.refused.len(), 1);
    assert_eq!(
        puller
            .machine
            .update_answered(&update, &Ok(updated))
            .unwrap(),
        UpdateOutcome::Applied { blocked: 1 }
    );
    puller.catch_up(&replay.client, 8).await;
    assert_eq!(puller.received.len(), 2);

    // Access is decided at the round, not at the update: the revoke between rounds makes the
    // held target Refused, and the round carries no row.
    commit(&store, vec![revoke(&[(1, 0)])]).await;
    puller.received.clear();
    let outcomes = puller
        .round(&replay.client, 8)
        .await
        .expect("a target is held");
    assert_eq!(
        outcomes
            .iter()
            .map(|(_, route, outcome)| (route.clone(), *outcome))
            .collect::<Vec<_>>(),
        [(part_route(1), TargetOutcome::Refused)]
    );
    assert!(puller.received.is_empty());
    stop.stop().await;
}

#[tokio::test]
async fn a_session_the_server_does_not_hold_is_unknown() {
    let (_, replay, stop) = booted(stranger());
    let session_id = Uuid::from_u128(3);

    let page = replay
        .client
        .call(
            RequestContext::root(),
            PageSession {
                session_id,
                targets: vec![(0, 0)],
                limit: 8,
                hold_ms: 0,
            },
        )
        .await;
    assert_eq!(answer(page), Err(SessionError::UnknownSession));

    // Only generation 0 opens a session (law 1).
    let update = replay
        .client
        .call(
            RequestContext::root(),
            UpdateSession {
                session_id,
                generation: 1,
                additions: vec![(0, part_route(1))],
                removals: Vec::new(),
            },
        )
        .await;
    assert_eq!(answer(update), Err(SessionError::UnknownSession));

    // Closing a session the server does not hold already has the outcome asked for.
    answer(
        replay
            .client
            .call(RequestContext::root(), CloseSession { session_id })
            .await,
    )
    .expect("close");
    stop.stop().await;
}

#[tokio::test]
async fn the_principal_seam_decides_before_the_store_is_read() {
    let (_, replay, stop) = booted(stranger());
    // The same service, called as an endpoint the embedder does not admit.
    let outsider = Client::<ReplayProtocol>::new(Transport::direct(
        ReplayBinding::new(Arc::clone(&replay.host)),
        CallOrigin::Endpoint(EndpointId::from_bytes([1; 32])),
    ));
    let result = outsider
        .call(
            RequestContext::root(),
            UpdateSession {
                session_id: Uuid::from_u128(4),
                generation: 0,
                additions: Vec::new(),
                removals: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        answer(result),
        Err(SessionError::Context(
            method_core::ContextError::Unauthenticated
        ))
    );
    stop.stop().await;
}

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

/// The same pull over iroh: the embedder's mapping sees the endpoint the connection
/// authenticated (and refuses every other), and rows round-trip the XRPC wire.
#[tokio::test]
async fn a_client_machine_pulls_a_part_over_iroh() {
    let server = endpoint().await;
    let client_endpoint = endpoint().await;
    let allowed = EndpointId::from_bytes(*client_endpoint.id().as_bytes());
    let (store, replay, stop) = booted(allowed);
    let (first, second) = seed(&store).await;

    let mut router = XrpcRouter::new(1 << 20);
    <ReplayBinding<_> as ServerBinding<ReplayProtocol>>::mount(
        &mut router,
        ReplayBinding::new(Arc::clone(&replay.host)),
    )
    .expect("mount the replay protocol");
    let server_router = iroh::protocol::Router::builder(server.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            // Two connections: the pulling client keeps its own open while the stranger dials in.
            iroh_protocol_handler(router.into_router(), 2, 8),
        )
        .spawn();
    let client = CoreClient::new(IrohHttp::new(
        client_endpoint.clone(),
        server.addr(),
        1 << 20,
    ));

    let mut puller = Puller::new(5, &[part_route(1)]);
    puller.settle(&client).await;
    puller.catch_up(&client, 1).await;
    assert_eq!(
        puller.received,
        [changed(first, 1, 10), changed(second, 2, 20)]
    );

    // Another endpoint reaches the same service and is turned away by the seam.
    let other = endpoint().await;
    let refused = CoreClient::new(IrohHttp::new(other.clone(), server.addr(), 1 << 20))
        .call(
            RequestContext::root(),
            UpdateSession {
                session_id: Uuid::from_u128(6),
                generation: 0,
                additions: Vec::new(),
                removals: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        answer(refused),
        Err(SessionError::Context(
            method_core::ContextError::Unauthenticated
        ))
    );

    server_router.shutdown().await.expect("router shutdown");
    other.close().await;
    client_endpoint.close().await;
    server.close().await;
    stop.stop().await;
}
