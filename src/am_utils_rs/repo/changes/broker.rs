use crate::interlude::*;

use crate::repo::changes::{BigRepoChangeNotification, BigRepoHeadNotification, DocIdFilter};

use samod::{DocHandle, DocumentId};
use samod_core::ChangeOrigin;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub(super) struct DocChangeBrokerHandle {
    doc_id: DocumentId,
    msg_tx: mpsc::Sender<BrokerMsg>,
}

type HasCandidateListener = Arc<dyn Fn(&DocumentId, &ChangeOrigin) -> bool + Send + Sync + 'static>;

enum BrokerMsg {
    EnsureReady {
        resp: tokio::sync::oneshot::Sender<()>,
    },
}

impl DocChangeBrokerHandle {
    pub async fn ensure_ready(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.msg_tx
            .send(BrokerMsg::EnsureReady { resp: tx })
            .await
            .expect(ERROR_ACTOR);
        rx.await.expect(ERROR_CHANNEL);
    }
}

pub(super) struct DocChangeBrokerStopToken {
    pub join_handle: JoinHandle<()>,
    pub cancel_token: CancellationToken,
}

impl DocChangeBrokerHandle {
    pub fn filter(&self) -> DocIdFilter {
        DocIdFilter {
            doc_id: self.doc_id.clone(),
        }
    }
}

pub fn spawn_doc_listener(
    handle: DocHandle,
    cancel_token: CancellationToken,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    head_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
    has_candidate_listener: HasCandidateListener,
) -> Res<(DocChangeBrokerHandle, DocChangeBrokerStopToken)> {
    let doc_id = handle.document_id().clone();

    let (msg_tx, mut msg_rx) = mpsc::channel(8);

    let fut = {
        let span = tracing::info_span!("repo doc listener task", ?doc_id);
        let cancel_token = cancel_token.clone();
        async move {
            debug!("listening on doc");

            let mut worker = DocChangeBroker {
                handle: handle.clone(),
                change_tx,
                head_tx,
                cancel_token,
                has_candidate_listener,
            };

            let mut doc_change_stream = handle.changes();
            loop {
                tokio::select! {
                    biased;
                    _ = worker.cancel_token.cancelled() => {
                        debug!("cancel token lit");
                        break;
                    },
                    val = msg_rx.recv() => {
                        let Some(msg) = val else {
                            break;
                        };
                        worker.handle_msg(msg).await?;
                    },
                    val = doc_change_stream.next() => {
                        let Some(changes) = val else {
                            break;
                        };
                        worker.handle_changes(changes).await?;
                    }
                }
            }
            eyre::Ok(())
        }
        .instrument(span)
    };
    let join_handle = tokio::spawn(async move { fut.await.unwrap() });

    Ok((
        DocChangeBrokerHandle {
            doc_id: doc_id.clone(),
            msg_tx,
        },
        DocChangeBrokerStopToken {
            join_handle,
            cancel_token,
        },
    ))
}

struct DocChangeBroker {
    handle: DocHandle,
    change_tx: mpsc::UnboundedSender<Vec<BigRepoChangeNotification>>,
    head_tx: mpsc::UnboundedSender<Vec<BigRepoHeadNotification>>,
    cancel_token: CancellationToken,
    has_candidate_listener: HasCandidateListener,
}

impl DocChangeBroker {
    async fn handle_msg(&mut self, msg: BrokerMsg) -> Res<()> {
        match msg {
            BrokerMsg::EnsureReady { resp } => {
                resp.send(()).expect(ERROR_CHANNEL);
            }
        }
        Ok(())
    }
    async fn handle_changes(&mut self, changes: samod_core::DocumentChanged) -> Res<()> {
        if matches!(changes.origin, ChangeOrigin::Bootstrap) {
            return Ok(());
        }
        let doc_id = self.handle.document_id().clone();
        let new_heads: Arc<[automerge::ChangeHash]> = Arc::from(&changes.new_heads[..]);
        self.head_tx
            .send(vec![BigRepoHeadNotification::DocHeadsChanged {
                doc_id: doc_id.clone(),
                heads: Arc::clone(&new_heads),
                origin: changes.origin.clone(),
            }])
            .or_else(|err| {
                if self.cancel_token.is_cancelled() {
                    debug!(
                        ?err,
                        ?doc_id,
                        "head_tx closed during broker shutdown; dropping late head notification"
                    );
                    Ok(())
                } else {
                    Err(eyre::eyre!("channel error: closed?: {err:?}"))
                }
            })?;
        if !(self.has_candidate_listener)(self.handle.document_id(), &changes.origin) {
            return Ok(());
        }
        let (_new_heads, all_changes) = self.handle.with_document(|doc| {
            let patches = doc.diff(&changes.old_heads[..], &changes.new_heads[..]);
            let collected_changes = patches
                .into_iter()
                .map(|patch| {
                    let patch = Arc::new(patch);
                    BigRepoChangeNotification::DocChanged {
                        doc_id: doc_id.clone(),
                        patch,
                        heads: Arc::clone(&new_heads),
                        origin: changes.origin.clone(),
                    }
                })
                .collect::<Vec<_>>();

            (new_heads, collected_changes)
        });

        if !all_changes.is_empty() {
            self.change_tx.send(all_changes).or_else(|err| {
                if self.cancel_token.is_cancelled() {
                    debug!(
                        ?err,
                        ?doc_id,
                        "change_tx closed during broker shutdown; dropping late change notification"
                    );
                    Ok(())
                } else {
                    Err(eyre::eyre!("channel error: closed?: {err:?}"))
                }
            })?;
        }

        Ok(())
    }
}
