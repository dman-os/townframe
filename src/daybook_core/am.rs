use std::any::Any;

use crate::interlude::*;

use automerge::Automerge;
use autosurgeon::{Hydrate, HydrateError, Prop, Reconcile, ReconcileError};
use tokio::sync::{mpsc, oneshot};

pub mod changes;

pub struct AmCtx {
    repo: samod::Repo,
    peer_id: samod::PeerId,
    doc_handle: tokio::sync::OnceCell<samod::DocHandle>,
    change_manager: changes::ChangeListenerManager,
}

impl AmCtx {
    pub async fn new(config: crate::AmConfig) -> Res<Self> {
        let peer_id = samod::PeerId::from_string(config.peer_id);

        // Ensure the storage directory exists
        std::fs::create_dir_all(&config.storage_dir)
            .wrap_err_with(|| format!("Failed to create storage directory: {}", config.storage_dir.display()))?;

        let repo = samod::Repo::build_tokio()
            .with_peer_id(peer_id.clone())
            .with_storage(samod::storage::TokioFilesystemStorage::new(
                config.storage_dir.to_string_lossy().as_ref(),
            ))
            .load()
            .await;

        let change_manager = changes::ChangeListenerManager::new();

        Ok(Self {
            doc_handle: default(),
            repo,
            peer_id,
            change_manager,
        })
    }

    /// Initialize the automerge document based on globals, and start connector lazily.
    pub async fn init_from_globals(&self, cx: SharedCtx) -> Res<()> {
        // Start the connector in background but do not block app startup
        self.spawn_connector();

        // Try to recover existing doc_id from local globals kv
        let init_state = crate::globals::get_init_state(&cx).await?;
        let handle = if let crate::globals::InitState::Created { doc_id } = init_state {
            match self.repo.find(doc_id).await? {
                Some(handle) => handle,
                None => {
                    warn!("doc not found locally for stored doc_id; creating new local document");
                    let doc = version_updates::version_latest()?;
                    let doc = Automerge::load(&doc).wrap_err("error loading version_latest")?;
                    let handle = self.repo.create(doc).await?;
                    // Update init state to new id so future runs recover
                    let new_state = crate::globals::InitState::Created {
                        doc_id: handle.document_id().clone(),
                    };
                    crate::globals::set_init_state(&cx, &new_state).await?;
                    handle
                }
            }
        } else {
            // First run: create a new document and persist its id
            let doc = version_updates::version_latest()?;
            let doc = Automerge::load(&doc).wrap_err("error loading version_latest")?;
            let handle = self.repo.create(doc).await?;
            let state = crate::globals::InitState::Created {
                doc_id: handle.document_id().clone(),
            };
            crate::globals::set_init_state(&cx, &state).await?;
            handle
        };

        let Ok(()) = self.doc_handle.set(handle) else {
            eyre::bail!("doc_handle already set");
        };
        Self::change_worker(cx);
        Ok(())
    }

    fn change_worker(cx: SharedCtx) {
        tokio::spawn(async move {
            let handle = cx.acx.doc_handle.get().unwrap();
            let mut heads = handle.with_document(|doc| doc.get_heads());
            use futures::StreamExt;

            while let Some(changes) = handle.changes().next().await {
                let (new_heads, all_changes) = handle.with_document(|doc| {
                    let patches = doc.diff(
                        &heads,
                        &changes.new_heads,
                        automerge::patches::TextRepresentation::String(doc.text_encoding()),
                    );

                    let mut collected_changes = Vec::new();

                    for patch in patches {
                        // Convert automerge path to autosurgeon path
                        let autosurgeon_path: Vec<Prop<'static>> = patch
                            .path
                            .into_iter()
                            .map(|(_, prop)| prop.into())
                            .collect();

                        collected_changes.push((autosurgeon_path, patch.action));
                    }

                    (changes.new_heads, collected_changes)
                });

                // Notify listeners about changes
                cx.acx.change_manager.notify_listeners(all_changes);

                heads = new_heads;
            }
        });
    }

    fn spawn_connector(&self) {
        let repo = self.repo.clone();
        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                attempt += 1;
                match tokio_tungstenite::connect_async("ws://0.0.0.0:8090").await {
                    Ok((conn, resp)) => {
                        if resp.status().as_u16() != 101 {
                            error!(?resp, "bad response connecting to server");
                            continue;
                        }
                        let fin = repo
                            .connect_tungstenite(conn, samod::ConnDirection::Outgoing)
                            .await;
                        error!(?fin, "connection closed");
                    }
                    Err(err) => {
                        error!("error connecting to sync server {err}");
                        continue;
                    }
                }
            }
        });
    }

    fn doc_handle(&self) -> &samod::DocHandle {
        self.doc_handle.get().expect("am not initialized")
    }

    pub async fn reconcile_prop<'a, D, P>(
        &self,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &D,
    ) -> Res<()>
    where
        D: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'a>> + Send + Sync + 'static,
    {
        tokio::task::block_in_place(move || {
            self.doc_handle().with_document(move |doc| {
                doc.transact(move |tx| {
                    autosurgeon::reconcile_prop(tx, obj_id, prop_name, update)?;
                    eyre::Ok(())
                })
            })
        })
        .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(())
    }

    pub async fn hydrate_path<D: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<D>> {
        tokio::task::block_in_place(move || {
            self.doc_handle().with_document(move |doc| {
                let value: Option<D> = autosurgeon::hydrate_path(doc, &obj_id, path)?;
                eyre::Ok(value)
            })
        })
    }

    /// Get access to the change listener manager
    pub fn change_manager(&self) -> &changes::ChangeListenerManager {
        &self.change_manager
    }
}

async fn init_am_doc() -> Res<automerge::AutoCommit> {
    use automerge::ReadDoc;
    let doc = automerge::AutoCommit::new();

    let version = match doc.get(automerge::ROOT, "version") {
        Ok(None) => None,
        Ok(Some((
            automerge::Value::Scalar(Cow::Owned(automerge::ScalarValue::Str(vers))),
            _op_id,
        ))) => Some(vers),
        Ok(Some((no_match, id))) => {
            return Err(ferr!(
                "error reading version from doc: unexpected value {no_match} at {id}"
            ))
        }
        Err(err) => return Err(ferr!("error reading version from doc: {err}")),
    };
    let doc = match version.as_deref() {
        Some("0") => doc,
        None => {
            let save = version_updates::version_latest()?;
            automerge::AutoCommit::load(&save[..]).wrap_err("error loading version_latest")?
        }
        ver => return Err(ferr!("unsupported document version {ver:?}")),
    };
    Ok(doc)
}

mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    use crate::docs::DocsAm;
    use crate::tables::TablesAm;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        reconcile_prop(&mut doc, ROOT, DocsAm::PROP, DocsAm::default())?;
        reconcile_prop(&mut doc, ROOT, TablesAm::PROP, TablesAm::default())?;
        Ok(doc.save_nocompress())
    }
}

pub fn am_worker(mut doc: automerge::AutoCommit) -> AmHandle {
    let (msg_tx, mut msg_rx) = mpsc::channel::<AmMsg>(32);
    let (term_signal_tx, mut term_signal_rx) = tokio::sync::watch::channel(());

    // let mut doc = automerge::AutoCommit::new();
    tokio::task::spawn(async move {
        loop {
            let msg = tokio::select! {
                _ = term_signal_rx.wait_for(|()| true) => {
                    trace!("term_signal_tx was lit, shutting down event loop");
                    break
                }
                Some(msg) = msg_rx.recv() => { msg }
            };
            match msg {
                AmMsg::Reconcile {
                    cb,
                    response_channel,
                } => response_channel
                    .send(cb(&mut doc))
                    .expect_or_log(ERROR_CHANNEL),
                AmMsg::Hydrate {
                    cb,
                    response_channel,
                } => response_channel
                    .send(cb(&mut doc))
                    .expect_or_log(ERROR_CHANNEL),
            }
        }
    });
    AmHandle {
        msg_tx,
        term_signal_tx,
    }
}

#[derive(Clone)]
pub struct AmHandle {
    msg_tx: mpsc::Sender<AmMsg>,
    term_signal_tx: tokio::sync::watch::Sender<()>,
}

impl AmHandle {
    pub async fn reconcile_prop<D, P>(
        &self,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: Arc<D>,
    ) -> Res<()>
    where
        D: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'static>>,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = AmMsg::Reconcile {
            cb: {
                let prop_name = prop_name.into();
                Box::new(move |doc| {
                    autosurgeon::reconcile_prop(doc, obj_id, prop_name, update.as_ref())
                })
            },
            response_channel: tx,
        };
        self.msg_tx.send(msg).await.expect_or_log(ERROR_CHANNEL);
        rx.await
            .expect_or_log(ERROR_CHANNEL)
            .map_err(|err| ferr!("error reonciling update: {err}"))
    }

    pub async fn hydrate_path<D: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<Box<D>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = AmMsg::Hydrate {
            cb: Box::new(move |doc| {
                let value: Option<D> = autosurgeon::hydrate_path(doc, &obj_id, path)?;
                Ok(value.map(|val| Box::new(val) as Box<dyn Any + Send + Sync + 'static>))
            }),
            response_channel: tx,
        };
        self.msg_tx.send(msg).await.expect_or_log(ERROR_CHANNEL);
        rx.await
            .expect_or_log(ERROR_CHANNEL)
            .map(|opt| opt.map(|any| any.downcast::<D>().expect_or_log("downcast error")))
            .map_err(|err| ferr!("error hydrating value: {err}"))
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
enum AmMsg {
    Reconcile {
        #[educe(Debug(ignore))]
        cb: Box<
            // FIXME: this is super limited since we will only be able to work with AutoCommit
            // and we cant make this generic
            dyn FnOnce(&mut automerge::AutoCommit) -> Result<(), ReconcileError> + Send + 'static,
        >,
        #[educe(Debug(ignore))]
        response_channel: oneshot::Sender<Result<(), ReconcileError>>,
    },
    Hydrate {
        #[educe(Debug(ignore))]
        cb: Box<
            dyn FnOnce(
                    &mut automerge::AutoCommit,
                )
                    -> Result<Option<Box<dyn Any + Send + Sync + 'static>>, HydrateError>
                + Send
                + 'static,
        >,
        #[educe(Debug(ignore))]
        response_channel:
            oneshot::Sender<Result<Option<Box<dyn Any + Send + Sync + 'static>>, HydrateError>>,
    },
}

pub mod autosurgeon_date {
    use automerge::ObjId;
    use autosurgeon::{Hydrate, HydrateError, ReadDoc, Reconciler};

    use crate::interlude::*;

    pub fn reconcile<R: Reconciler>(
        ts: &OffsetDateTime,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.timestamp(ts.unix_timestamp())
    }

    // There's no type in autosurgeon with impl for
    // hydrate_timestamp so we do our own
    struct Wrapper(i64);
    impl Hydrate for Wrapper {
        fn hydrate_timestamp(ts: i64) -> Result<Self, HydrateError> {
            Ok(Self(ts))
        }
    }

    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<OffsetDateTime, HydrateError> {
        let Wrapper(inner) = Wrapper::hydrate(doc, obj, prop)?;

        OffsetDateTime::from_unix_timestamp(inner).map_err(|err| {
            HydrateError::unexpected(
                "an valid unix timestamp",
                format!("error parsing timestamp int {err}"),
            )
        })
    }
}

pub mod automerge_skip {
    use automerge::ObjId;
    use autosurgeon::{HydrateError, ReadDoc, Reconciler};

    pub fn reconcile<T: Default, R: Reconciler>(
        _value: &T,
        _reconciler: R,
    ) -> Result<(), R::Error> {
        // Skip reconciliation - this field is not stored in the CRDT
        Ok(())
    }

    pub fn hydrate<'a, D: ReadDoc, T: Default>(
        _doc: &D,
        _obj: &ObjId,
        _prop: autosurgeon::Prop<'a>,
    ) -> Result<T, HydrateError> {
        // Return default value - this field is not stored in the CRDT
        Ok(T::default())
    }
}
