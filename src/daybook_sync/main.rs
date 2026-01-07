#[allow(unused)]
mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use autosurgeon::{Hydrate, Reconcile};

    pub(crate) use crate::Ctx;
    pub use generational_box::{GenerationalBox, Storage};
}

mod gen;

use std::collections::HashSet;

use crate::interlude::*;

use axum::Router;
use generational_box::SyncStorage;
use samod::{DocumentId, PeerId};
use tokio::{sync::mpsc, task::JoinHandle};
use tower_http::ServiceBuilderExt;
use utils_rs::am::changes::ChangeFilter;

fn main() -> Res<()> {
    utils_rs::setup_tracing()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error building tokio rt")?;
    rt.block_on(app_main())
}

async fn app_main() -> Res<()> {
    let config = Config {};
    let cx = Ctx::init(config).await?;

    let app = Router::new()
        .route("/", axum::routing::any(connect))
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
    _config: Config,
    acx: utils_rs::am::AmCtx,
    _peer_docs: Arc<DHashMap<PeerId, HashSet<DocumentId>>>,
    _doc_peers: Arc<DHashMap<DocumentId, PeerId>>,
    _gen_store: generational_box::Owner<SyncStorage>,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn init(config: Config) -> Res<SharedCtx> {
        let peer_docs: Arc<DHashMap<PeerId, HashSet<DocumentId>>> = default();
        let doc_peers: Arc<DHashMap<DocumentId, PeerId>> = default();

        let (doc_setup_req_tx, mut doc_setup_req_rx) = mpsc::unbounded_channel::<DocSetupRequest>();
        let announcer_tx = doc_setup_req_tx.clone();
        let (acx, _acx_stop) = utils_rs::am::AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "daybook_sync".to_string(),
                storage: utils_rs::am::StorageConfig::Disk {
                    path: "/tmp/samod-sync".into(),
                },
            },
            // we only announce docs to peers that forwarded them in the first place
            // FIXME: this gets run at startup for all doc/peers leading to the first
            // peer getting access to all docs. put it behind a kv store
            Some({
                let peer_docs = peer_docs.clone();
                let doc_peers = doc_peers.clone();
                let doc_setup_req_tx = announcer_tx.clone();
                move |doc_id: DocumentId, peer_id| {
                    if let Some(peer_of_doc) = doc_peers.get(&doc_id) {
                        if *peer_of_doc.value() != peer_id {
                            return false;
                        }
                    }
                    doc_setup_req_tx
                        .send(DocSetupRequest::new(peer_id.clone(), doc_id.clone()))
                        .expect(ERROR_CHANNEL);
                    peer_docs
                        .entry(peer_id.clone())
                        .or_default()
                        .insert(doc_id.clone());
                    doc_peers.insert(doc_id, peer_id);
                    true
                }
            }),
        )
        .await?;

        use generational_box::AnyStorage;
        let cx = Arc::new(Self {
            acx,
            _config: config,
            _peer_docs: peer_docs,
            _doc_peers: doc_peers,
            _gen_store: SyncStorage::owner(),
        });
        let doc_setup_worker = spawn_doc_setup_worker(cx.clone());
        tokio::spawn(async move {
            while let Some(req) = doc_setup_req_rx.recv().await {
                doc_setup_worker.request_tx.send(req).expect(ERROR_CHANNEL)
            }
        });
        cx.acx
            .change_manager()
            .add_listener(
                ChangeFilter {
                    path: vec![],
                    doc_id: None,
                },
                {
                    // let cx = cx.clone();
                    Box::new(move |changes| {
                        info!(?changes, "XXX change");
                    })
                },
            )
            .await;
        Ok(cx)
    }
}

#[derive(Debug)]
struct DocSetupRequest {
    _peer_id: PeerId,
    doc_id: DocumentId,
    last_attempt_backoff_ms: u64,
}

impl DocSetupRequest {
    fn new(peer_id: PeerId, doc_id: DocumentId) -> Self {
        Self {
            _peer_id: peer_id,
            doc_id,
            last_attempt_backoff_ms: 1000,
        }
    }
}

struct DocSetupWorker {
    request_tx: mpsc::UnboundedSender<DocSetupRequest>,
    _handle: JoinHandle<()>,
}

fn spawn_doc_setup_worker(cx: SharedCtx) -> DocSetupWorker {
    let (request_tx, mut request_rx) = mpsc::unbounded_channel::<DocSetupRequest>();
    let fut = {
        let request_tx = request_tx.clone();
        async move {
            let mut listen_list = std::collections::HashSet::new();
            // let mut drawer_change_workers = std::collections::HashMap::new();

            // with exponential backoff
            let retry = |task: DocSetupRequest| {
                tokio::spawn({
                    let doc_id_tx = request_tx.clone();
                    async move {
                        let new_backoff =
                            utils_rs::backoff(task.last_attempt_backoff_ms, 60 * 1000).await;
                        doc_id_tx
                            .send(DocSetupRequest {
                                last_attempt_backoff_ms: new_backoff,
                                ..task
                            })
                            .expect(ERROR_CHANNEL);
                    }
                });
            };

            while let Some(request) = request_rx.recv().await {
                if listen_list.contains(&request.doc_id) {
                    continue;
                }
                match cx.acx.find_doc(&request.doc_id).await {
                    Err(err) => {
                        warn!(?request, "error looking up doc_id: {err}");
                        retry(request);
                        continue;
                    }
                    Ok(None) => {
                        warn!(?request, "doc not found in repo during setup");
                        retry(request);
                        continue;
                    }
                    Ok(Some(_val)) => {
                        // noop
                    }
                };

                // inspect schema and attach schema-specific listeners
                // Try to hydrate the $schema property via AmCtx helper

                let schema_opt = cx
                    .acx
                    .hydrate_path::<String>(
                        &request.doc_id,
                        automerge::ROOT,
                        vec!["$schema".into()],
                    )
                    .await
                    .wrap_err("error hydrating added docment")?;

                if let Some(schema) = schema_opt {
                    if schema == "daybook.drawer" {
                        // TODO: spawn doc changes worker
                    }
                }

                listen_list.insert(request.doc_id);
            }
            eyre::Ok(())
        }
    };

    let handle = tokio::spawn({
        async move {
            fut.await.unwrap_or_log();
        }
    });
    DocSetupWorker {
        request_tx,
        _handle: handle,
    }
}

#[tracing::instrument(skip(cx))]
async fn connect(
    axum::extract::State(cx): axum::extract::State<SharedCtx>,
    ws: axum::extract::WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(cx, socket))
}

#[tracing::instrument(skip(cx))]
async fn handle_socket(cx: SharedCtx, socket: axum::extract::ws::WebSocket) {
    let _handle = tokio::spawn(async move {
        let fin = cx.acx.repo().accept_axum(socket).await;
        info!(?fin, "connection finsihed");
    });
}
