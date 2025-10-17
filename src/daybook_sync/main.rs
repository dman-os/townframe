#[allow(unused)]
mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use autosurgeon::{Hydrate, Reconcile};

    pub(crate) use crate::Ctx;
    pub use generational_box::{GenerationalBox, Storage};
}

mod gen;
mod restate;

use std::collections::HashSet;

use crate::interlude::*;

use axum::Router;
use generational_box::SyncStorage;
use samod::{DocumentId, PeerId};
use tokio::{sync::mpsc, task::JoinHandle};
use tower_http::ServiceBuilderExt;
use utils_rs::am::changes::{ChangeFilter, ChangeNotification};

fn main() -> Res<()> {
    utils_rs::setup_tracing()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err(ERROR_TOKIO)?;
    rt.block_on(app_main())
}

async fn app_main() -> Res<()> {
    let config = Config {
        restate_base_url: "http://localhost:9080".parse().expect("error parsing url"),
    };
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

struct Config {
    restate_base_url: url::Url,
}

struct Ctx {
    config: Config,
    acx: utils_rs::am::AmCtx,
    rcx: restate::RestateCtx,
    _peer_docs: Arc<DHashMap<PeerId, HashSet<DocumentId>>>,
    _doc_peers: Arc<DHashMap<DocumentId, PeerId>>,
    _gen_store: generational_box::Owner<SyncStorage>,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn init(config: Config) -> Res<SharedCtx> {
        let peer_docs: Arc<DHashMap<PeerId, HashSet<DocumentId>>> = default();
        let doc_peers: Arc<DHashMap<DocumentId, PeerId>> = default();
        let (doc_id_tx, doc_id_rx) = mpsc::unbounded_channel::<DocumentId>();
        let announcer_tx = doc_id_tx.clone();

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
                let doc_id_tx = announcer_tx.clone();
                move |doc_id: DocumentId, peer_id| {
                    doc_id_tx.send(doc_id.clone()).expect("doc channel closed");

                    if let Some(peer_of_doc) = doc_peers.get(&doc_id) {
                        if *peer_of_doc.value() != peer_id {
                            return false;
                        }
                    }
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
            rcx: restate::RestateCtx::new()?,
            acx,
            config,
            _peer_docs: peer_docs,
            _doc_peers: doc_peers,
            _gen_store: SyncStorage::owner(),
        });
        let _doc_setup_worker = spawn_doc_setup_worker(cx.clone(), doc_id_rx, doc_id_tx);
        cx.acx
            .change_manager()
            .add_listener(
                ChangeFilter {
                    path: vec![],
                    doc_id: None,
                },
                {
                    // let cx = cx.clone();
                    move |changes| {
                        info!(?changes, "XXX change");
                    }
                },
            )
            .await;
        Ok(cx)
    }
}

fn spawn_doc_setup_worker(
    cx: SharedCtx,
    doc_id_rx: mpsc::UnboundedReceiver<DocumentId>,
    doc_id_tx: mpsc::UnboundedSender<DocumentId>,
) -> JoinHandle<()> {
    async fn inner(
        cx: SharedCtx,
        mut doc_id_rx: mpsc::UnboundedReceiver<DocumentId>,
        doc_id_tx: mpsc::UnboundedSender<DocumentId>,
    ) -> Res<()> {
        let mut doc_change_worker = vec![];
        let mut listen_list = std::collections::HashSet::new();

        let drawer_changes_driver = spawn_drawer_changes_driver(cx.clone());

        while let Some(doc_id) = doc_id_rx.recv().await {
            if listen_list.contains(&doc_id) {
                continue;
            }
            let handle = match cx.acx.find_doc(&doc_id).await {
                Err(err) => {
                    warn!("error looking up doc_id: {err}");
                    continue;
                }
                Ok(None) => {
                    warn!(?doc_id, "doc not found in repo during setup, trying again");
                    tokio::task::spawn({
                        let doc_id_tx = doc_id_tx.clone();
                        let doc_id = doc_id.clone();
                        async move {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            // requeue for later handling
                            doc_id_tx.send(doc_id).expect("failed to requeue doc_id");
                        }
                    });
                    continue;
                }
                Ok(Some(val)) => val,
            };

            // inspect schema and attach schema-specific listeners
            // Try to hydrate the $schema property via AmCtx helper
            let schema_opt = cx
                .acx
                .hydrate_path::<String>(&doc_id, automerge::ROOT, vec!["$schema".into()])
                .await
                .wrap_err("error hydrating added docment")?;

            if let Some(schema) = schema_opt {
                if schema == "daybook.drawer" {
                    cx.acx
                        .change_manager()
                        .add_listener(
                            ChangeFilter {
                                doc_id: Some(doc_id.clone()),
                                path: vec!["docs".into()],
                            },
                            {
                                let notif_tx = drawer_changes_driver.notif_tx.clone();
                                let doc_id = doc_id.clone();
                                move |notifs| {
                                    let notif_tx = notif_tx.clone();
                                    let doc_id = doc_id.clone();
                                    tokio::task::spawn(async move {
                                        let permit =
                                            notif_tx.reserve().await.expect("channel error");
                                        permit.send((doc_id, notifs));
                                    });
                                }
                            },
                        )
                        .await;

                    let change_worker = cx
                        .acx
                        .change_manager()
                        .clone()
                        .spawn_doc_listener(handle.clone());
                    doc_change_worker.push(change_worker);
                }
            }

            listen_list.insert(doc_id);
        }
        Ok(())
    }

    tokio::spawn({
        let cx = cx.clone();
        async move {
            inner(cx, doc_id_rx, doc_id_tx).await.unwrap_or_log();
        }
    })
}

struct DrawerChangesDriver {
    // drawer_doc_id, notifs
    notif_tx: mpsc::Sender<(DocumentId, Vec<ChangeNotification>)>,
    _handle: JoinHandle<()>,
}

fn spawn_drawer_changes_driver(cx: SharedCtx) -> DrawerChangesDriver {
    let (notif_tx, mut notif_rx) = mpsc::channel(16);

    // #region event handlers
    let on_new_doc = {
        let notif_tx = notif_tx.clone();
        let cx = cx.clone();
        move |drawer_doc_id: DocumentId, notif: ChangeNotification| {
            let automerge::PatchAction::PutMap {
                key: new_doc_id,
                value: (val, obj_id),
                ..
            } = &notif.patch.action
            else {
                panic!("unexpected");
            };
            let Some(automerge::ObjType::List) = val.to_objtype() else {
                panic!("schema violation");
            };

            let notif_tx = notif_tx.clone();
            let cx = cx.clone();
            let new_doc_id = new_doc_id.clone();
            let obj_id = obj_id.clone();

            tokio::task::spawn(async move {
                let heads = cx
                    .acx
                    .hydrate_path_at_head::<Vec<String>>(
                        &drawer_doc_id,
                        &notif.heads,
                        obj_id,
                        vec![],
                    )
                    .await
                    .expect("error hydrating at head")
                    .expect("schema violation");

                if let Err(err) = restate::start_doc_pipeline(
                    &cx,
                    &r#gen::doc::DocAddedEvent {
                        id: new_doc_id.clone(),
                        heads,
                    },
                )
                .await
                {
                    error!(?err, ?new_doc_id, "error starting doc pipeline");
                    if let restate::RestateError::RequestError { status, .. } = &err {
                        if status.is_client_error() {
                            panic!("client error on restate client {err:?}");
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    notif_tx
                        .send((drawer_doc_id, vec![notif]))
                        .await
                        .expect("channel error")
                }
            })
        }
    };
    // #endregion

    let handle = tokio::spawn({
        async move {
            while let Some((drawer_doc_id, notifs)) = notif_rx.recv().await {
                for notif in notifs {
                    match &notif.patch.action {
                        automerge::PatchAction::PutMap { key, .. } => {
                            info!(%key, "drawer: doc added/updated");
                            on_new_doc(drawer_doc_id.clone(), notif);
                        }
                        automerge::PatchAction::DeleteMap { key } => {
                            info!(%key, "drawer: doc removed");
                        }
                        _ => {}
                    }
                }
            }
        }
    });
    DrawerChangesDriver {
        notif_tx,
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
