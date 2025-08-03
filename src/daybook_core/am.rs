use std::any::Any;

use crate::interlude::*;

use automerge::AutoCommit;
use autosurgeon::{Hydrate, HydrateError, Prop, Reconcile, ReconcileError};
use tokio::sync::{mpsc, oneshot};

pub struct AmCtx {
    doc: Arc<tokio::sync::RwLock<AutoCommit>>,
}
impl AmCtx {
    pub async fn load() -> Res<Self> {
        let doc = init_am_doc().await?;
        let doc = Arc::new(tokio::sync::RwLock::new(doc));
        Ok(Self { doc })
    }

    pub async fn reconcile_prop<'a, D, P>(
        &self,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &D,
    ) -> Res<()>
    where
        D: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'a>>,
    {
        let mut doc = self.doc.write().await;
        autosurgeon::reconcile_prop(&mut *doc, obj_id, prop_name, update)?;
        Ok(())
    }

    pub async fn hydrate_path<D: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<D>> {
        let doc = self.doc.read().await;
        let value: Option<D> = autosurgeon::hydrate_path(&*doc, &obj_id, path)?;
        return Ok(value);
    }
}

async fn init_am_doc() -> Res<automerge::AutoCommit> {
    use automerge::ReadDoc;
    let doc = automerge::AutoCommit::new();

    // TODO: load from disk
    // TODO: sync with network
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

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        reconcile_prop(&mut doc, ROOT, DocsAm::PROP, DocsAm::default())?;
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
                    .expect_or_log("channel error"),
                AmMsg::Hydrate {
                    cb,
                    response_channel,
                } => response_channel
                    .send(cb(&mut doc))
                    .expect_or_log("channel error"),
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
        self.msg_tx.send(msg).await.expect_or_log("channel error");
        rx.await
            .expect_or_log("channel error")
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
        self.msg_tx.send(msg).await.expect_or_log("channel error");
        rx.await
            .expect_or_log("channel error")
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

fn test() -> Res<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async { eyre::Ok(()) })?;
    Ok(())
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
