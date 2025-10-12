mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use autosurgeon::{Hydrate, Reconcile};
}

mod gen;

use crate::interlude::*;

use axum::Router;
use samod::{DocumentId, PeerId};
use tower_http::ServiceBuilderExt;
use utils_rs::am::changes::ChangeFilter;

fn main() -> Res<()> {
    utils_rs::setup_tracing()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err(ERROR_TOKIO)?;
    rt.block_on(app_main())
}

async fn app_main() -> Res<()> {
    let config = Config {};
    let cx = Ctx::new(config).await?;

    let app = Router::new()
        .route("/", axum::routing::any(connect))
        .route("/doc_id", axum::routing::get(get_doc_id))
        .with_state(cx)
        // tracing layer
        .layer(
            tower::ServiceBuilder::new()
                .sensitive_headers(vec![http::header::AUTHORIZATION, http::header::COOKIE])
                .layer(
                    tower_http::trace::TraceLayer::new_for_http()
                        .on_response(
                            tower_http::trace::DefaultOnResponse::new()
                                .level(tracing::Level::INFO)
                                .latency_unit(tower_http::LatencyUnit::Micros),
                        )
                        .on_failure(
                            tower_http::trace::DefaultOnFailure::new()
                                .level(tracing::Level::ERROR)
                                .latency_unit(tower_http::LatencyUnit::Micros),
                        )
                        .make_span_with(
                            tower_http::trace::DefaultMakeSpan::new().include_headers(true),
                        ),
                ),
        );

    let addr = std::net::SocketAddr::from((
        std::net::Ipv4Addr::UNSPECIFIED,
        utils_rs::get_env_var("PORT")
            .and_then(|str| {
                str.parse()
                    .map_err(|err| ferr!("error parsing port env var ({str}): {err}"))
            })
            .unwrap_or(8090),
    ));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::info!(%addr, "server going online");
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;

    Ok(())
}

struct Config {}

struct Ctx {
    config: Config,
    acx: utils_rs::am::AmCtx,
    peer_docs: Arc<DHashMap<PeerId, DocumentId>>,
    doc_peers: Arc<DHashMap<DocumentId, PeerId>>,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn new(config: Config) -> Res<SharedCtx> {
        let peer_docs: Arc<DHashMap<PeerId, DocumentId>> = default();
        let doc_peers: Arc<DHashMap<DocumentId, PeerId>> = default();
        let (doc_tx, mut doc_rx) = tokio::sync::mpsc::unbounded_channel::<DocumentId>();

        let acx = utils_rs::am::AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "daybook_sync".to_string(),
                storage_dir: "/tmp/samod-sync".into(),
            },
            // we only announce docs to peers that forwarded them in the first place
            // FIXME: this gets run at startup for all doc/peers leading to the first
            // peer getting access to all docs. put it behind a kv store
            Some({
                let peer_docs = peer_docs.clone();
                let doc_peers = doc_peers.clone();
                move |doc_id: DocumentId, peer_id| {
                    info!(%doc_id, %peer_id, ?peer_docs, ?doc_peers, "announcing");

                    doc_tx
                        .send(doc_id.clone())
                        .expect_or_log("doc channel closed");

                    if let Some(doc_of_peer) = peer_docs.get(&peer_id) {
                        return *doc_of_peer.value() == doc_id;
                    }
                    if let Some(peer_of_doc) = doc_peers.get(&doc_id) {
                        if *peer_of_doc.value() == peer_id {
                            unimplemented!("this can't happen");
                        }
                    }
                    peer_docs.insert(peer_id.clone(), doc_id.clone());
                    doc_peers.insert(doc_id, peer_id);
                    false
                }
            }),
        )
        .await?;

        let cx = Arc::new(Self {
            acx,
            config,
            peer_docs,
            doc_peers,
        });
        tokio::spawn({
            let cx = cx.clone();
            async move {
                let mut doc_change_worker = vec![];
                let mut listen_list = std::collections::HashSet::new();
                while let Some(doc_id) = doc_rx.recv().await {
                    if listen_list.contains(&doc_id) {
                        continue;
                    }
                    let Some(handle) = cx
                        .acx
                        .find_doc(doc_id.clone())
                        .await
                        .expect_or_log("repo stopped")
                    else {
                        warn!("announced doc_id not found in repo: {doc_id}");
                        continue;
                    };
                    let change_worker = cx.acx.change_manager().clone().spawn_doc_listener(handle);
                    listen_list.insert(doc_id);
                    doc_change_worker.push(change_worker);
                }
            }
            .instrument(tracing::info_span!("start doc listener task"))
        });
        cx.acx
            .change_manager()
            .add_listener(
                ChangeFilter {
                    path: vec![],
                    doc_id: None,
                },
                |changes| {
                    info!(?changes, "XXX change");
                },
            )
            .await;
        Ok(cx)
    }
}

#[tracing::instrument(skip(cx), ret)]
async fn connect(
    axum::extract::State(cx): axum::extract::State<SharedCtx>,
    ws: axum::extract::WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(cx, socket))
}

#[tracing::instrument(skip(cx), ret)]
async fn handle_socket(cx: SharedCtx, socket: axum::extract::ws::WebSocket) {
    let _handle = tokio::spawn(async move {
        let fin = cx.acx.repo().accept_axum(socket).await;
        info!(?fin, "connection finsihed");
    });
}

#[derive(Deserialize, Debug)]
struct GetDocIdQuery {
    peer_id: String,
}

#[tracing::instrument(skip(cx), ret)]
async fn get_doc_id(
    cx: axum::extract::State<SharedCtx>,
    query: axum::extract::Query<GetDocIdQuery>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let peer_id = PeerId::from_string(query.peer_id.clone());
    let peer_docs = cx.peer_docs.get(&peer_id);
    if let Some(doc_id) = peer_docs {
        format!("{}", *doc_id.value()).into_response()
    } else {
        axum::http::StatusCode::NOT_FOUND.into_response()
    }
}
