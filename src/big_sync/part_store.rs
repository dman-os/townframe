use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, PeerPartCursors};
use big_sync_core::{ObjId, PartId, PeerId};

use irpc::{channel::oneshot, WithChannels};
use sqlx::Row;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct SqlitePartStoreHandle {
    client: irpc::Client<SqlPartStoreProtocol>,
}

impl std::ops::Deref for SqlitePartStoreHandle {
    type Target = irpc::Client<SqlPartStoreProtocol>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub struct SqlPartStoreStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl SqlPartStoreStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(1))
            .await
            .wrap_err("failed stopping sync store")
    }
}

#[irpc::rpc_requests(message = Msg)]
#[derive(Debug, Serialize, Deserialize)]
enum SqlPartStoreProtocol {
    #[rpc(tx = oneshot::Sender<Result<PeerPartCursors, utils_rs::SerReport>>)]
    #[wrap(GetPartitionCursor)]
    GetPartitionCursor {
        peer_id: PeerId,
        partition_id: PartId,
    },
    #[rpc(tx = oneshot::Sender<Result<(), utils_rs::SerReport>>)]
    #[wrap(SetPartitionCursor)]
    SetPartitionCursor {
        peer_id: PeerId,
        partition_id: PartId,
        cursors: PeerPartCursors,
    },
}

pub async fn spawn_sync_store(
    read_pool: sqlx::SqlitePool,
    write_pool: sqlx::SqlitePool,
) -> Res<(SqlitePartStoreHandle, SqlPartStoreStopToken)> {
    ensure_schema(&write_pool).await?;
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
        SqlitePartStoreHandle {
            client: irpc::Client::local(tx),
        },
        SqlPartStoreStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

async fn ensure_schema(pool: &sqlx::SqlitePool) -> Res<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_allowed_peers(
            peer_id TEXT PRIMARY KEY
        ) STRICT
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_partition_cursors(
            peer_id TEXT NOT NULL,
            partition_id TEXT NOT NULL,
            member_cursor INTEGER NULL,
            item_cursor INTEGER NULL,
            PRIMARY KEY(peer_id, partition_id)
        ) STRICT
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn handle_msg(read_pool: sqlx::SqlitePool, write_pool: sqlx::SqlitePool, msg: Msg) {
    match msg {
        StoreMsg::GetPartitionCursor {
            peer_id: peer,
            partition_id,
            resp,
        } => {}
        StoreMsg::SetPartitionCursor {
            peer_id: peer,
            partition_id,
            member_cursor,
            item_cursor,
            resp,
        } => {
            let out = async {
                sqlx::query(
                    r#"
                    INSERT INTO sync_partition_cursors(peer_id, partition_id, member_cursor, item_cursor)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(peer_id, partition_id) DO UPDATE SET
                        member_cursor = excluded.member_cursor,
                        item_cursor = excluded.item_cursor
                    "#,
                )
                .bind(&peer[..])
                .bind(&partition_id[..])
                .bind(member_cursor.map(|val| val as i64))
                .bind(item_cursor.map(|val| val as i64))
                .execute(write_pool)
                .await?;
                Ok(())
            }
            .await;
            resp.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        Msg::GetPartitionCursor(msg) => {
            let WithChannels { inner, tx, .. } = msg;
            let GetPartitionCursor {
                peer_id,
                partition_id,
            } = inner;
            let out = async {
                let row = sqlx::query(
                    "SELECT member_cursor, item_cursor FROM sync_partition_cursors WHERE peer_id = ? AND partition_id = ?",
                )
                .bind(&peer[..])
                .bind(&partition_id[..])
                .fetch_optional(read_pool)
                .await?;
                let Some(row) = row else {
                    return Ok(PeerPartCursors {
                        member_cursor: None,
                        obj_cursor: None,
                    });
                };
                let member_cursor = row
                    .try_get::<Option<i64>, _>("member_cursor")?
                    .map(|val| val.max(0) as u64);
                let item_cursor = row
                    .try_get::<Option<i64>, _>("item_cursor")?
                    .map(|val| val.max(0) as u64);
                Ok(PeerPartCursors {
                    member_cursor,
                    obj_cursor: item_cursor,
                })
            }
            .await;
            tx.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        }
        Msg::SetPartitionCursor(_) => todo!(),
    }
}
