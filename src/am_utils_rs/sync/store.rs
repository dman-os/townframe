use crate::interlude::*;

use crate::sync::{OpaqueCursor, PartitionId, PeerKey};
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
    pub member_cursor: Option<OpaqueCursor>,
    pub doc_cursor: Option<OpaqueCursor>,
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
        member_cursor: Option<OpaqueCursor>,
        doc_cursor: Option<OpaqueCursor>,
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
        member_cursor: Option<OpaqueCursor>,
        doc_cursor: Option<OpaqueCursor>,
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
    cancel_token: CancellationToken,
) -> Res<(SyncStoreHandle, SyncStoreStopToken)> {
    ensure_schema(&state_pool).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
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
            member_cursor TEXT NULL,
            doc_cursor TEXT NULL,
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
                let mut tx = pool.begin().await?;
                sqlx::query("DELETE FROM sync_known_peers WHERE peer_key = ?")
                    .bind(&peer)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM sync_partition_cursors WHERE peer_key = ?")
                    .bind(&peer)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        StoreMsg::IsPeerRegistered { peer, resp } => {
            let out = async {
                let exists: i64 =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sync_known_peers WHERE peer_key = ?)")
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
                Ok(PartitionSyncCursorState {
                    member_cursor: row.try_get("member_cursor")?,
                    doc_cursor: row.try_get("doc_cursor")?,
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
                .bind(member_cursor)
                .bind(doc_cursor)
                .execute(pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
    }
}
