use crate::interlude::*;

use crate::sync::protocol::{CursorIndex, PartitionId, PeerKey};

use iroh::EndpointId;
use sqlx::Row;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
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
    AllowPeer {
        peer: PeerKey,
        endpoint_id: Option<EndpointId>,
        resp: oneshot::Sender<Res<()>>,
    },
    RevokePeer {
        peer: PeerKey,
        resp: oneshot::Sender<Res<()>>,
    },
    IsPeerAllowed {
        peer: PeerKey,
        resp: oneshot::Sender<Res<bool>>,
    },
    IsEndpointAllowed {
        endpoint_id: EndpointId,
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
}

impl SyncStoreHandle {
    pub async fn allow_peer(&self, peer: PeerKey, endpoint_id: Option<EndpointId>) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::AllowPeer {
                peer,
                endpoint_id,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn revoke_peer(&self, peer: PeerKey) -> Res<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::RevokePeer {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn is_peer_allowed(&self, peer: PeerKey) -> Res<bool> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::IsPeerAllowed {
                peer,
                resp: resp_tx,
            })
            .wrap_err("sync store closed")?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?
    }

    pub async fn is_endpoint_allowed(&self, endpoint_id: EndpointId) -> Res<bool> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(StoreMsg::IsEndpointAllowed {
                endpoint_id,
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
        CREATE TABLE IF NOT EXISTS sync_allowed_peers(
            peer_key TEXT PRIMARY KEY,
            endpoint_id TEXT NULL
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
    Ok(())
}

async fn handle_msg(pool: &sqlx::SqlitePool, msg: StoreMsg) {
    match msg {
        StoreMsg::AllowPeer {
            peer,
            endpoint_id,
            resp,
        } => {
            let out = async {
                sqlx::query(
                    "INSERT INTO sync_allowed_peers(peer_key, endpoint_id) VALUES(?, ?) ON CONFLICT(peer_key) DO UPDATE SET endpoint_id = excluded.endpoint_id",
                )
                .bind(peer)
                .bind(endpoint_id.map(|id| id.to_string()))
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::RevokePeer { peer, resp } => {
            let out = async {
                sqlx::query("DELETE FROM sync_allowed_peers WHERE peer_key = ?")
                    .bind(&peer)
                    .execute(pool)
                    .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::IsPeerAllowed { peer, resp } => {
            let out = async {
                let exists: i64 = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sync_allowed_peers WHERE peer_key = ?)",
                )
                .bind(peer)
                .fetch_one(pool)
                .await?;
                Ok(exists == 1)
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::IsEndpointAllowed { endpoint_id, resp } => {
            let out = async {
                let exists: i64 = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sync_allowed_peers WHERE endpoint_id = ?)",
                )
                .bind(endpoint_id.to_string())
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

        store.allow_peer(peer.clone(), None).await?;
        store
            .set_partition_cursor(peer.clone(), partition.clone(), Some(10), Some(20))
            .await?;
        store.revoke_peer(peer.clone()).await?;
        store.allow_peer(peer.clone(), None).await?;

        let cursor = store.get_partition_cursor(peer, partition).await?;
        assert_eq!(cursor.member_cursor, Some(10));
        assert_eq!(cursor.doc_cursor, Some(20));

        stop.stop().await?;
        Ok(())
    }
}
