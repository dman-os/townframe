use crate::interlude::*;
use crate::partition::PartitionStore;
use crate::repo::{BigRepoChangeOrigin, DocumentId};

use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::BTreeSet;
use tokio::sync::{mpsc, oneshot};

use super::changes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NoopConnection;

impl
    subduction_core::connection::Connection<
        future_form::Sendable,
        subduction_core::connection::message::SyncMessage,
    > for NoopConnection
{
    type DisconnectionError = std::convert::Infallible;
    type SendError = std::convert::Infallible;
    type RecvError = std::convert::Infallible;

    fn disconnect(&self) -> futures::future::BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(
        &self,
        _message: &subduction_core::connection::message::SyncMessage,
    ) -> futures::future::BoxFuture<'_, Result<(), Self::SendError>> {
        async { Ok(()) }.boxed()
    }

    fn recv(
        &self,
    ) -> futures::future::BoxFuture<
        '_,
        Result<subduction_core::connection::message::SyncMessage, Self::RecvError>,
    > {
        async { std::future::pending().await }.boxed()
    }
}

enum RuntimeMsg {
    IngestFull {
        doc_id: DocumentId,
        doc_save: Vec<u8>,
        done: oneshot::Sender<Res<()>>,
    },
    LoadDoc {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<automerge::Automerge>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    },
}

pub(super) struct BigRepoRuntimeHandle {
    msg_tx: mpsc::UnboundedSender<RuntimeMsg>,
}

impl BigRepoRuntimeHandle {
    pub(super) async fn ingest_full(&self, doc_id: DocumentId, doc_save: Vec<u8>) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::IngestFull {
                doc_id,
                doc_save,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!("big repo runtime channel closed"))?;
        done_rx
            .await
            .map_err(|_| eyre::eyre!("big repo runtime dropped ingest response"))?
    }

    pub(super) async fn load_doc(&self, doc_id: DocumentId) -> Res<Option<automerge::Automerge>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::LoadDoc {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!("big repo runtime channel closed"))?;
        done_rx
            .await
            .map_err(|_| eyre::eyre!("big repo runtime dropped load-doc response"))?
    }

    pub(super) async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!("big repo runtime channel closed"))?;
        done_rx
            .await
            .map_err(|_| eyre::eyre!("big repo runtime dropped commit response"))?
    }
}

struct BigRepoRuntimeWorker {
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
}

impl BigRepoRuntimeWorker {
    async fn handle_commit_delta(
        &self,
        doc_id: DocumentId,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let item_payload = serde_json::json!({
            "heads": crate::serialize_commit_heads(&heads),
            "change_count_hint": 1_u64,
        });
        self.partition_store
            .record_member_item_change(&doc_id.to_string(), &item_payload)
            .await?;

        let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads);
        self.change_manager.notify_doc_heads_changed(
            doc_id,
            Arc::clone(&heads_arc),
            origin.clone(),
        )?;
        self.change_manager
            .notify_local_doc_heads_updated(doc_id, Arc::clone(&heads_arc))?;
        for patch in patches {
            self.change_manager.notify_doc_changed(
                doc_id,
                Arc::new(patch),
                Arc::clone(&heads_arc),
                origin.clone(),
            )?;
        }

        Ok(())
    }
}

pub(super) fn spawn_big_repo_runtime<S>(
    join_set: Arc<utils_rs::AbortableJoinSet>,
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<BigRepoRuntimeHandle>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
{
    use subduction_core::{policy::open::OpenPolicy, subduction::builder::SubductionBuilder};
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let policy = Arc::new(OpenPolicy);
    let (subduction, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(signer)
        .storage(storage, policy)
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<future_form::Sendable, NoopConnection>();

    join_set
        .spawn(async move {
            listener.await.unwrap();
        })
        .expect("failed spawning subduction listener");
    join_set
        .spawn(async move {
            manager.await.unwrap();
        })
        .expect("failed spawning subduction manager");

    let worker = BigRepoRuntimeWorker {
        partition_store,
        change_manager,
    };
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<RuntimeMsg>();
    join_set
        .spawn(async move {
            while let Some(msg) = msg_rx.recv().await {
                match msg {
                    RuntimeMsg::IngestFull {
                        doc_id,
                        doc_save,
                        done,
                    } => {
                        let out = async {
                            let doc = automerge::Automerge::load(&doc_save).map_err(|err| {
                                ferr!("failed loading automerge save for ingest: {err}")
                            })?;
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            let ingested = automerge_sedimentree::ingest::ingest_automerge(
                                &doc,
                                sedimentree_id,
                            )
                            .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
                            subduction
                                .add_sedimentree(
                                    sedimentree_id,
                                    ingested.sedimentree,
                                    ingested.blobs,
                                )
                                .await
                                .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
                            eyre::Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::LoadDoc { doc_id, done } => {
                        let out = async {
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            let Some(blobs) =
                                subduction.get_blobs(sedimentree_id).await.map_err(|err| {
                                    ferr!("failed reading blobs from subduction storage: {err}")
                                })?
                            else {
                                return Ok(None);
                            };
                            let doc = reconstruct_automerge_from_blobs(blobs)?;
                            eyre::Ok(Some(doc))
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::CommitDelta {
                        doc_id,
                        commits,
                        heads,
                        patches,
                        origin,
                        done,
                    } => {
                        let out = async {
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            for (head, parents, bytes) in commits {
                                subduction
                                    .add_commit(sedimentree_id, head, parents, Blob::new(bytes))
                                    .await
                                    .map_err(|err| ferr!("failed add_commit: {err}"))?;
                            }
                            worker
                                .handle_commit_delta(doc_id, heads, patches, origin)
                                .await
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                }
            }
        })
        .expect("failed spawning big repo runtime task");

    Ok(BigRepoRuntimeHandle { msg_tx })
}

fn reconstruct_automerge_from_blobs<I>(blobs: I) -> Res<automerge::Automerge>
where
    I: IntoIterator<Item = Blob>,
{
    let mut doc = automerge::Automerge::new();
    let mut pending = blobs
        .into_iter()
        .map(|blob| blob.as_slice().to_vec())
        .collect::<Vec<_>>();
    while !pending.is_empty() {
        let mut next = Vec::new();
        let mut progressed = false;
        for blob in pending {
            match doc.load_incremental(&blob) {
                Ok(_) => progressed = true,
                Err(_) => next.push(blob),
            }
        }
        if !progressed {
            eyre::bail!("failed reconstructing automerge doc from subduction blobs");
        }
        pending = next;
    }
    Ok(doc)
}
