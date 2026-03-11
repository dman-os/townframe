use crate::interlude::*;

use crate::sync::{CursorIndex, PartitionId, PeerKey};
use sqlx::Row;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub struct SyncStoreHandle {
    tx: mpsc::UnboundedSender<StoreMsg>,
}

impl Clone for SyncStoreHandle {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

pub struct SyncStoreStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl SyncStoreStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1))
            .await
            .wrap_err("failed stopping sync store")
    }
}

#[derive(Debug, Clone)]
pub struct PartitionSyncCursorState {
    pub member_cursor: Option<CursorIndex>,
    pub doc_cursor: Option<CursorIndex>,
}

enum StoreMsg {
    RegisterPeer {
        peer: PeerKey,
        resp: oneshot::Sender<Res<()>>,
    },
    UnregisterPeer {
        peer: PeerKey,
        resp: oneshot::Sender<Res<()>>,
    },
    IsPeerRegistered {
        peer: PeerKey,
        resp: oneshot::Sender<Res<bool>>,
    },
    GetPartitionCursor {
        peer: PeerKey,
        partition_id: PartitionId,
        resp: oneshot::Sender<Res<PartitionSyncCursorState>>,
    },
    SetPartitionCursor {
        peer: PeerKey,
        partition_id: PartitionId,
        member_cursor: Option<CursorIndex>,
        doc_cursor: Option<CursorIndex>,
        resp: oneshot::Sender<Res<()>>,
    },
    UpsertUnresolvedDoc {
        peer: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
        cursor: CursorIndex,
        reason: String,
        resp: oneshot::Sender<Res<()>>,
    },
    ResolveUnresolvedDoc {
        peer: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
        resp: oneshot::Sender<Res<()>>,
    },
}

impl SyncStoreHandle {
    pub async fn register_peer(&self, peer: PeerKey) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::RegisterPeer {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn unregister_peer(&self, peer: PeerKey) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::UnregisterPeer {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn is_peer_registered(&self, peer: PeerKey) -> Res<bool> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::IsPeerRegistered {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn get_partition_cursor(
        &self,
        peer: PeerKey,
        partition_id: PartitionId,
    ) -> Res<PartitionSyncCursorState> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::GetPartitionCursor {
                peer,
                partition_id,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn set_partition_cursor(
        &self,
        peer: PeerKey,
        partition_id: PartitionId,
        member_cursor: Option<CursorIndex>,
        doc_cursor: Option<CursorIndex>,
    ) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::SetPartitionCursor {
                peer,
                partition_id,
                member_cursor,
                doc_cursor,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn upsert_unresolved_doc(
        &self,
        peer: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
        cursor: CursorIndex,
        reason: String,
    ) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::UpsertUnresolvedDoc {
                peer,
                partition_id,
                doc_id,
                cursor,
                reason,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn resolve_unresolved_doc(
        &self,
        peer: PeerKey,
        partition_id: PartitionId,
        doc_id: String,
    ) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::ResolveUnresolvedDoc {
                peer,
                partition_id,
                doc_id,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }
}

pub async fn spawn_sync_store(
    state_pool: sqlx::SqlitePool,
) -> Res<(SyncStoreHandle, SyncStoreStopToken)> {
    ensure_schema(&state_pool).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let cancel_token = CancellationToken::new();
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        handle_msg(&state_pool, msg).await;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });
    Ok((
        SyncStoreHandle { tx },
        SyncStoreStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

async fn ensure_schema(pool: &sqlx::SqlitePool) -> Res<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_known_peers(
            peer_key TEXT PRIMARY KEY
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_partition_cursors(
            peer_key TEXT NOT NULL,
            partition_id TEXT NOT NULL,
            member_cursor INTEGER NULL,
            doc_cursor INTEGER NULL,
            PRIMARY KEY(peer_key, partition_id)
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_unresolved_docs(
            peer_key TEXT NOT NULL,
            partition_id TEXT NOT NULL,
            doc_id TEXT NOT NULL,
            first_cursor INTEGER NOT NULL,
            latest_cursor INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            reason TEXT NOT NULL,
            updated_at_unix_ms INTEGER NOT NULL,
            PRIMARY KEY(peer_key, partition_id, doc_id)
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn handle_msg(pool: &sqlx::SqlitePool, msg: StoreMsg) {
    match msg {
        StoreMsg::RegisterPeer { peer, resp } => {
            let out = async {
                sqlx::query(
                    "INSERT INTO sync_known_peers(peer_key) VALUES(?) ON CONFLICT(peer_key) DO NOTHING",
                )
                .bind(peer)
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::UnregisterPeer { peer, resp } => {
            let out = async {
                sqlx::query("DELETE FROM sync_known_peers WHERE peer_key = ?")
                    .bind(&peer)
                    .execute(pool)
                    .await?;
                // Keep cursor rows to support resume after disconnect/reconnect.
                // Peer registration still gates RPC access via ensure_known_peer.
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::IsPeerRegistered { peer, resp } => {
            let out = async {
                let exists: i64 = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sync_known_peers WHERE peer_key = ?)",
                )
                .bind(peer)
                .fetch_one(pool)
                .await?;
                Ok(exists == 1)
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::GetPartitionCursor {
            peer,
            partition_id,
            resp,
        } => {
            let out = async {
                let row = sqlx::query(
                    "SELECT member_cursor, doc_cursor FROM sync_partition_cursors WHERE peer_key = ? AND partition_id = ?",
                )
                .bind(peer)
                .bind(partition_id)
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(PartitionSyncCursorState {
                        member_cursor: None,
                        doc_cursor: None,
                    });
                };
                let member_cursor = row
                    .try_get::<Option<i64>, _>("member_cursor")?
                    .map(|val| val.max(0) as u64);
                let doc_cursor = row
                    .try_get::<Option<i64>, _>("doc_cursor")?
                    .map(|val| val.max(0) as u64);
                Ok(PartitionSyncCursorState {
                    member_cursor,
                    doc_cursor,
                })
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::SetPartitionCursor {
            peer,
            partition_id,
            member_cursor,
            doc_cursor,
            resp,
        } => {
            let out = async {
                sqlx::query(
                    r#"
                    INSERT INTO sync_partition_cursors(peer_key, partition_id, member_cursor, doc_cursor)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(peer_key, partition_id) DO UPDATE SET
                        member_cursor = excluded.member_cursor,
                        doc_cursor = excluded.doc_cursor
                    "#,
                )
                .bind(peer)
                .bind(partition_id)
                .bind(member_cursor.map(|val| val as i64))
                .bind(doc_cursor.map(|val| val as i64))
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::UpsertUnresolvedDoc {
            peer,
            partition_id,
            doc_id,
            cursor,
            reason,
            resp,
        } => {
            let out = async {
                sqlx::query(
                    r#"
                    INSERT INTO sync_unresolved_docs(
                        peer_key, partition_id, doc_id, first_cursor, latest_cursor, attempts, reason, updated_at_unix_ms
                    ) VALUES(?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(peer_key, partition_id, doc_id) DO UPDATE SET
                        latest_cursor = excluded.latest_cursor,
                        attempts = attempts + 1,
                        reason = excluded.reason,
                        updated_at_unix_ms = excluded.updated_at_unix_ms
                    "#,
                )
                .bind(peer)
                .bind(partition_id)
                .bind(doc_id)
                .bind(cursor as i64)
                .bind(cursor as i64)
                .bind(reason)
                .bind(Timestamp::now().as_millisecond())
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::ResolveUnresolvedDoc {
            peer,
            partition_id,
            doc_id,
            resp,
        } => {
            let out = async {
                sqlx::query(
                    "DELETE FROM sync_unresolved_docs WHERE peer_key = ? AND partition_id = ? AND doc_id = ?",
                )
                .bind(peer)
                .bind(partition_id)
                .bind(doc_id)
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unregister_peer_preserves_partition_cursors() -> Res<()> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
        let (store, stop) = spawn_sync_store(pool).await?;
        let peer: PeerKey = "peer-a".into();
        let partition: PartitionId = "part-a".into();

        store.register_peer(peer.clone()).await?;
        store
            .set_partition_cursor(
                peer.clone(),
                partition.clone(),
                Some(10),
                Some(20),
            )
            .await?;
        store.unregister_peer(peer.clone()).await?;
        store.register_peer(peer.clone()).await?;

        let cursor = store.get_partition_cursor(peer, partition).await?;
        assert_eq!(cursor.member_cursor, Some(10));
        assert_eq!(cursor.doc_cursor, Some(20));

        stop.stop().await?;
        Ok(())
    }
}
