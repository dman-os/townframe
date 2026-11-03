use super::*;
use crate::receive::ReceiveJournal;
use crate::receive::ReceiveProgress;
use crate::receive::ReceiveRoute;
use crate::receive::ReceivedReplay;
use crate::receive::ReplayReceipt;
use sqlite_rs::QueryExecutor;

async fn progress(
    db: &impl QueryExecutor,
    scope: i64,
    peer: PeerKey,
    route: &ReceiveRoute,
) -> Res<ReceiveProgress> {
    let row = db.query(
        "SELECT received\n     , applied\n  FROM sync_receive_frontiers\n WHERE scope_id = ? AND peer = ? AND backend = ? AND target = ?",
        (scope, SqliteCore::peer_blob(peer), route.backend.clone(), serde_json::to_vec(&route.target)?),
    ).await?.optional::<(i64, i64)>()?;
    Ok(row
        .map(|(received, applied)| ReceiveProgress {
            received: SqliteCore::u64_from_db(received),
            applied: SqliteCore::u64_from_db(applied),
        })
        .unwrap_or_default())
}

async fn pending(db: &impl QueryExecutor, scope: i64) -> Res<Option<ReplayReceipt>> {
    let row = db
        .query(
            "SELECT id\n     , replay\n  FROM sync_receive_receipts\n WHERE scope_id = ?",
            (scope,),
        )
        .await?
        .optional::<(i64, Vec<u8>)>()?;
    row.map(|(id, bytes)| {
        Ok(ReplayReceipt {
            id: u64::try_from(id).expect(ERROR_IMPOSSIBLE),
            replay: serde_json::from_slice(&bytes)?,
        })
    })
    .transpose()
}

#[async_trait]
impl ReceiveJournal for SqlitePartStore {
    async fn progress(&self, peer: PeerKey, route: ReceiveRoute) -> Res<ReceiveProgress> {
        progress(&self.core.sql, self.core.scope_id, peer, &route).await
    }

    async fn pending(&self) -> Res<Option<ReplayReceipt>> {
        pending(&self.core.sql, self.core.scope_id).await
    }

    async fn accept(&self, replay: ReceivedReplay) -> Res<Option<ReplayReceipt>> {
        let tx = self.core.sql.begin_write().await?;
        let scope = self.core.scope_id;
        eyre::ensure!(
            pending(&tx, scope).await?.is_none(),
            "unfinished replay receipt must drain first"
        );
        let bounds = replay.bounds()?;
        for (route, predecessor, _) in &bounds {
            if progress(&tx, scope, replay.peer_id.clone(), route)
                .await?
                .received
                != *predecessor
            {
                return Ok(None);
            }
        }
        let inserted = tx
            .execute(
                "INSERT INTO sync_receive_receipts (scope_id, replay) VALUES (?, ?)",
                (scope, serde_json::to_vec(&replay)?),
            )
            .await?;
        for (route, _, resume) in bounds {
            tx.execute(
                "INSERT INTO sync_receive_frontiers (scope_id, peer, backend, target, received, applied)\nVALUES (?, ?, ?, ?, ?, 0)\nON CONFLICT(scope_id, peer, backend, target) DO UPDATE SET received = excluded.received",
                (scope, SqliteCore::peer_blob(replay.peer_id.clone()), route.backend.clone(), serde_json::to_vec(&route.target)?, SqliteCore::db_from_u64(resume)),
            ).await?;
        }
        tx.commit().await?;
        Ok(Some(ReplayReceipt {
            id: u64::try_from(inserted.last_insert_rowid).expect(ERROR_IMPOSSIBLE),
            replay,
        }))
    }

    async fn applied(&self, receipt_id: u64) -> Res<()> {
        let tx = self.core.sql.begin_write().await?;
        let scope = self.core.scope_id;
        let receipt = pending(&tx, scope)
            .await?
            .ok_or_else(|| ferr!("no pending replay receipt"))?;
        eyre::ensure!(
            receipt.id == receipt_id,
            "replay receipt acknowledgment mismatch"
        );
        for (route, predecessor, resume) in receipt.replay.bounds()? {
            let current = progress(&tx, scope, receipt.replay.peer_id.clone(), route).await?;
            eyre::ensure!(
                current.applied == predecessor && current.received == resume,
                "replay acknowledgment chain mismatch"
            );
            tx.execute(
                "UPDATE sync_receive_frontiers\n   SET applied = received\n WHERE scope_id = ? AND peer = ? AND backend = ? AND target = ?",
                (scope, SqliteCore::peer_blob(receipt.replay.peer_id.clone()), route.backend.clone(), serde_json::to_vec(&route.target)?),
            ).await?;
        }
        tx.execute(
            "DELETE FROM sync_receive_receipts WHERE scope_id = ? AND id = ?",
            (scope, i64::try_from(receipt_id)?),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
