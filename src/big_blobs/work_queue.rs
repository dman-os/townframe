use big_cabinet_core::DocId;
use sqlite_rs::Database;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::Res;

mod sqlite;

#[derive(Clone)]
pub(crate) struct WorkQueue {
    sql: Database,
    pub changed: Arc<Notify>,
}

impl WorkQueue {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(
            sqlite::queries::MIGRATION_TABLE,
            sqlite::queries::MIGRATIONS,
        )
        .await?;
        Ok(Self {
            sql,
            changed: Arc::new(Notify::new()),
        })
    }

    pub async fn progress(&self, source: &str) -> Res<u64> {
        Ok(sqlite::queries::progress(&self.sql, source.to_owned())
            .await?
            .into_iter()
            .next()
            .unwrap_or(0) as u64)
    }

    pub async fn ingest(
        &self,
        source: &str,
        revision: u64,
        documents: BTreeSet<DocId>,
        rearm_waiting: bool,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let previous = sqlite::queries::progress(&tx, source.to_owned())
            .await?
            .into_iter()
            .next()
            .unwrap_or(0) as u64;
        if revision <= previous {
            tx.rollback().await?;
            return Ok(());
        }
        tracing::info!(
            source,
            revision,
            documents = documents.len(),
            rearm_waiting,
            "INSTR work queue ingest"
        ); // INSTR
        for document in documents {
            sqlite::queries::enqueue(&tx, document.to_string()).await?;
        }
        if rearm_waiting {
            sqlite::queries::rearm_waiting(&tx).await?;
        }
        sqlite::queries::upsert_progress(&tx, source.to_owned(), i64::try_from(revision)?).await?;
        tx.commit().await?;
        self.changed.notify_one();
        Ok(())
    }

    /// Re-dirty the parked or in-flight rows among `documents` after their content
    /// materialized locally, and advance the `source` cursor in the same transaction.
    ///
    /// Only rows that are `waiting` (parked because the document could not be read yet) or
    /// `dirty` (a pass may be reading it right now) are touched; a document that was never
    /// enqueued is not blob work. Bumping the generation of a dirty row makes the in-flight
    /// pass's `finish` a no-op, so content that lands between that pass's read and its
    /// `finish` cannot be parked over: the row stays dirty and is read again.
    pub async fn wake_materialized(
        &self,
        source: &str,
        revision: u64,
        documents: BTreeSet<DocId>,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let previous = sqlite::queries::progress(&tx, source.to_owned())
            .await?
            .into_iter()
            .next()
            .unwrap_or(0) as u64;
        if revision <= previous {
            tx.rollback().await?;
            return Ok(());
        }
        for document in documents {
            sqlite::queries::wake_materialized(&tx, document.to_string()).await?;
        }
        sqlite::queries::upsert_progress(&tx, source.to_owned(), i64::try_from(revision)?).await?;
        tx.commit().await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn enqueue(&self, document: &DocId) -> Res<()> {
        sqlite::queries::enqueue(&self.sql, document.to_string()).await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn rearm(&self) -> Res<()> {
        sqlite::queries::rearm(&self.sql).await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn next(&self) -> Res<Option<(DocId, u64)>> {
        sqlite::queries::next(&self.sql)
            .await?
            .into_iter()
            .next()
            .map(|row| Ok((row.doc_id.parse()?, row.generation as u64)))
            .transpose()
    }

    pub async fn finish(&self, doc: &DocId, generation: u64, waiting: bool) -> Res<()> {
        sqlite::queries::finish(
            &self.sql,
            waiting,
            doc.to_string(),
            i64::try_from(generation)?,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql() -> Database {
        sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap()
    }

    #[tokio::test]
    async fn stale_inputs_and_inflight_completion_cannot_erase_new_work() -> Res<()> {
        let queue = WorkQueue::new(sql()).await?;
        let doc = DocId::new([7; 32]);
        queue
            .ingest("documents", 2, BTreeSet::from([doc.clone()]), false)
            .await?;
        let (_, generation) = queue.next().await?.unwrap();
        queue.ingest("documents", 1, BTreeSet::new(), false).await?;
        assert_eq!(queue.progress("documents").await?, 2);
        queue
            .ingest("documents", 3, BTreeSet::from([doc.clone()]), false)
            .await?;
        queue.finish(&doc, generation, false).await?;
        let (_, newer) = queue.next().await?.unwrap();
        assert!(newer > generation);
        queue.finish(&doc, newer, false).await?;
        assert!(queue.next().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn parked_documents_survive_restart_and_bytes_rearm_them() -> Res<()> {
        let sql = sql();
        let doc = DocId::new([9; 32]);
        let queue = WorkQueue::new(sql.clone()).await?;
        queue
            .ingest("documents", 1, BTreeSet::from([doc.clone()]), false)
            .await?;
        let (_, generation) = queue.next().await?.unwrap();
        queue.finish(&doc, generation, true).await?;
        drop(queue);
        let queue = WorkQueue::new(sql).await?;
        assert!(queue.next().await?.is_none());
        queue
            .ingest("availability", 8, BTreeSet::new(), true)
            .await?;
        assert_eq!(queue.next().await?.unwrap().0, doc);
        assert_eq!(queue.progress("documents").await?, 1);
        assert_eq!(queue.progress("availability").await?, 8);
        Ok(())
    }
}
