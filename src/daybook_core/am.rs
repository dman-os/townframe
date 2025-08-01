use crate::interlude::*;

use autosurgeon::{Hydrate, HydrateError, Reconcile, ReconcileError};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct AmWorker {
    msg_tx: mpsc::Sender<AmMsg>,
    term_signal_tx: tokio::sync::watch::Sender<()>,
}

impl AmWorker {
    pub async fn update_doc<D: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        update: Arc<D>,
    ) -> Res<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = AmMsg::Reconcile {
            cb: Box::new(move |doc| autosurgeon::reconcile(doc, update.as_ref())),
            response_channel: tx,
        };
        self.msg_tx.send(msg).await.expect_or_log("channel error");
        rx.await
            .expect_or_log("channel error")
            .map_err(|err| ferr!("error reonciling update: {err}"))
    }

    pub async fn get_doc<D: Hydrate + Reconcile + Send + Sync + 'static>(&self) -> Res<Arc<D>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = AmMsg::Hydrate {
            cb: Box::new(move |doc| {
                let value: D = autosurgeon::hydrate(doc)?;
                Ok(Arc::new(value))
            }),
            response_channel: tx,
        };
        self.msg_tx.send(msg).await.expect_or_log("channel error");
        rx.await
            .expect_or_log("channel error")
            .map(|any| Arc::downcast(any).expect_or_log("downcast error"))
            .map_err(|err| ferr!("error hydrating value: {err}"))
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
enum AmMsg {
    Reconcile {
        #[educe(Debug(ignore))]
        cb: Box<
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
                    -> Result<Arc<dyn std::any::Any + Send + Sync + 'static>, HydrateError>
                + Send
                + 'static,
        >,
        #[educe(Debug(ignore))]
        response_channel:
            oneshot::Sender<Result<Arc<dyn std::any::Any + Send + Sync + 'static>, HydrateError>>,
    },
}

pub fn am_worker() -> AmWorker {
    let (msg_tx, mut msg_rx) = mpsc::channel::<AmMsg>(32);
    let (term_signal_tx, mut term_signal_rx) = tokio::sync::watch::channel(());

    let mut doc = automerge::AutoCommit::new();
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
    AmWorker {
        msg_tx,
        term_signal_tx,
    }
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
