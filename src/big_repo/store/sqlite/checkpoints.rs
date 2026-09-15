use super::*;

impl SqliteBigRepoStore {
    pub(crate) async fn causal_checkpoint_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT seq
                 FROM cursors
                WHERE reader = ?1",
            self.cursor_reader("causal_checkpoint")
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn advance_causal_checkpoint_cursor(&self, cursor: u64) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "UPDATE cursors
                SET seq = MAX(seq, ?1)
              WHERE reader = ?2",
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            self.cursor_reader("causal_checkpoint")
        )
        .execute(&mut *tx)
        .await?;
        self.advance_keyhive_admission_reader_in_tx(
            &mut tx,
            crate::store::sqlite::KEYHIVE_ADMISSION_READER_CAUSAL_CHECKPOINT,
            cursor,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn get_sync_commit_watermark(
        &self,
        doc_id: crate::DocumentId,
        big_sync_txid: u64,
    ) -> Res<Option<i64>> {
        let row_id: Option<i64> = sqlx::query_scalar!(
            "SELECT latest_commit_row_id
                 FROM big_repo_sync_commits_watermark
                WHERE scope_id = ?1
                  AND doc_id = ?2
                  AND big_sync_txid = ?3",
            self.scope().id(),
            doc_id.as_bytes().as_slice(),
            i64::try_from(big_sync_txid).expect(ERROR_IMPOSSIBLE)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(row_id)
    }
}
