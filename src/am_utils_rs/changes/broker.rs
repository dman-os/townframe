use crate::interlude::*;

use crate::changes::{ChangeNotification, DocIdFilter};

use automerge::ChangeHash;
use samod::{DocHandle, DocumentId};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use std::sync::Mutex;

pub struct DocChangeBrokerHandle {
    doc_id: DocumentId,
    msg_tx: mpsc::Sender<BrokerMsg>,
}

pub struct HeadListenerRegistration {
    change_rx: mpsc::Receiver<Arc<[ChangeHash]>>,
    list: std::sync::Weak<Mutex<Vec<HeadListener>>>,
    id: Uuid,
}

impl HeadListenerRegistration {
    pub fn change_rx(&mut self) -> &mut mpsc::Receiver<Arc<[ChangeHash]>> {
        &mut self.change_rx
    }
}

impl Drop for HeadListenerRegistration {
    fn drop(&mut self) {
        if let Some(listeners) = self.list.upgrade() {
            let id = self.id;
            listeners
                .lock()
                .expect(ERROR_MUTEX)
                .retain(|listener| listener.id != id);
        }
    }
}

struct HeadListener {
    id: Uuid,
    change_tx: mpsc::Sender<Arc<[ChangeHash]>>,
}

enum BrokerMsg {
    AddHeadListener {
        resp: tokio::sync::oneshot::Sender<HeadListenerRegistration>,
    },
}

impl DocChangeBrokerHandle {
    pub async fn get_head_listener(&self) -> Res<HeadListenerRegistration> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.msg_tx
            .send(BrokerMsg::AddHeadListener { resp: tx })
            .await
            .ok()
            .ok_or_eyre(ERROR_ACTOR)?;
        rx.await.wrap_err(ERROR_CHANNEL)
    }
}

pub struct DocChangeBrokerStopToken {
    pub join_handle: JoinHandle<()>,
    pub cancel_token: CancellationToken,
}

impl DocChangeBrokerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.join_handle.await.wrap_err("tokio task error")?;
        Ok(())
    }
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
    change_tx: mpsc::UnboundedSender<(DocumentId, Vec<ChangeNotification>)>,
) -> Res<(DocChangeBrokerHandle, DocChangeBrokerStopToken)> {
    let doc_id = handle.document_id().clone();

    let (msg_tx, mut msg_rx) = mpsc::channel(8);

    let fut = {
        let span = tracing::info_span!("doc listener task", ?doc_id);
        let cancel_token = cancel_token.clone();
        async move {
            debug!("listening on doc");

            let heads = handle.with_document(|doc| doc.get_heads());

            let mut worker = DocChangeBroker {
                heads: heads.into(),
                heads_listeners: default(),
                handle: handle.clone(),
                change_tx,
            };

            let mut doc_change_stream = handle.changes();
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
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
    let join_handle = tokio::spawn(async { fut.await.unwrap() });

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
    // stack: Vec<ChangeHash>,
    // actor_ids: HashSet<ActorId>,
    // collected_patches: Vec<(Vec<automerge::Patch>, Arc<[ActorId]>)>,
    heads: Arc<[ChangeHash]>,
    heads_listeners: Arc<Mutex<Vec<HeadListener>>>,
    handle: DocHandle,
    change_tx: mpsc::UnboundedSender<(DocumentId, Vec<ChangeNotification>)>,
}

impl DocChangeBroker {
    async fn handle_msg(&mut self, msg: BrokerMsg) -> Res<()> {
        match msg {
            BrokerMsg::AddHeadListener { resp } => {
                let (change_tx, change_rx) = mpsc::channel(16);
                let registration = HeadListenerRegistration {
                    id: Uuid::new_v4(),
                    list: Arc::downgrade(&self.heads_listeners),
                    change_rx,
                };
                self.heads_listeners
                    .lock()
                    .expect(ERROR_MUTEX)
                    .push(HeadListener {
                        id: registration.id.clone(),
                        change_tx,
                    });
                resp.send(registration)
                    .inspect_err(|_| error!(ERROR_CALLER))
                    .ok();
            }
        }
        Ok(())
    }
    async fn handle_changes(&mut self, changes: samod_core::DocumentChanged) -> Res<()> {
        let new_heads: Arc<[ChangeHash]> = Arc::from(&changes.new_heads[..]);
        for listener in self.heads_listeners.lock().expect(ERROR_MUTEX).iter() {
            match listener.change_tx.try_send(Arc::clone(&new_heads)) {
                Ok(()) => {}
                Err(err) => match err {
                    mpsc::error::TrySendError::Full(_) => {
                        panic!("HeadListenerRegistration is full, yo");
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        panic!("HeadListenerRegistration dropepd without cleanup");
                    }
                },
            }
        }
        let (new_heads, all_changes) = self.handle.with_document(|doc| {
            let patches = doc.diff(&self.heads, &changes.new_heads[..]);
            let collected_changes = patches
                .into_iter()
                .map(|patch| {
                    let patch = Arc::new(patch);
                    ChangeNotification {
                        patch,
                        heads: Arc::clone(&new_heads),
                    }
                })
                .collect::<Vec<_>>();

            (new_heads, collected_changes)
        });

        trace!(?all_changes, "XXX changes observed");

        // Notify listeners about changes
        if !all_changes.is_empty() {
            if let Err(err) = self
                .change_tx
                .send((self.handle.document_id().clone(), all_changes))
            {
                warn!("failed to send change notifications: {err}");
            }
        }

        self.heads = new_heads;

        Ok(())
    }
}
