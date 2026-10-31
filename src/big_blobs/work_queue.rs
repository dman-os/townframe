use big_cabinet_core::DocId;
use sqlite_rs::Database;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::Res;

#[derive(Clone)]
pub(crate) struct WorkQueue {
    sql: Database,
    pub changed: Arc<Notify>,
}

impl WorkQueue {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(
            "big_blobs_work_migrations",
            &[sqlite_rs::Migration {
                version: 1,
                name: "durable_blob_work",
                sql: "CREATE TABLE big_blobs_work (
                       doc_id TEXT PRIMARY KEY NOT NULL
                     , generation INTEGER NOT NULL
                     , dirty INTEGER NOT NULL
                     , waiting INTEGER NOT NULL
                  ) STRICT;
                  CREATE INDEX big_blobs_work_dirty ON big_blobs_work (dirty, doc_id);
                  CREATE TABLE big_blobs_progress (
                       source TEXT PRIMARY KEY NOT NULL
                     , revision INTEGER NOT NULL
                  ) STRICT;",
            }],
        )
        .await?;
        Ok(Self {
            sql,
            changed: Arc::new(Notify::new()),
        })
    }

    pub async fn progress(&self, source: &str) -> Res<u64> {
        Ok(self
            .sql
            .query(
                "SELECT revision FROM big_blobs_progress WHERE source = ?",
                (source,),
            )
            .await?
            .all::<u64>()?
            .into_iter()
            .next()
            .unwrap_or(0))
    }

    pub async fn ingest(
        &self,
        source: &str,
        revision: u64,
        documents: BTreeSet<DocId>,
        rearm_waiting: bool,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let previous = tx
            .query(
                "SELECT revision FROM big_blobs_progress WHERE source = ?",
                (source,),
            )
            .await?
            .all::<u64>()?
            .into_iter()
            .next()
            .unwrap_or(0);
        if revision <= previous {
            tx.rollback().await?;
            return Ok(());
        }
        for document in documents {
            tx.execute("INSERT INTO big_blobs_work (doc_id, generation, dirty, waiting) VALUES (?, 1, 1, 0)
                        ON CONFLICT (doc_id) DO UPDATE SET generation = generation + 1, dirty = 1", (document.to_string(),)).await?;
        }
        if rearm_waiting {
            tx.execute("UPDATE big_blobs_work SET generation = generation + 1, dirty = 1 WHERE waiting = 1", ()).await?;
        }
        tx.execute(
            "INSERT INTO big_blobs_progress (source, revision) VALUES (?, ?)
                    ON CONFLICT (source) DO UPDATE SET revision = excluded.revision",
            (source, i64::try_from(revision)?),
        )
        .await?;
        tx.commit().await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn enqueue(&self, document: &DocId) -> Res<()> {
        self.sql.execute("INSERT INTO big_blobs_work (doc_id, generation, dirty, waiting) VALUES (?, 1, 1, 0)
                          ON CONFLICT (doc_id) DO UPDATE SET generation = generation + 1, dirty = 1", (document.to_string(),)).await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn rearm(&self) -> Res<()> {
        self.sql
            .execute(
                "UPDATE big_blobs_work SET generation = generation + 1, dirty = 1",
                (),
            )
            .await?;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn next(&self) -> Res<Option<(DocId, u64)>> {
        self.sql.query("SELECT doc_id, generation FROM big_blobs_work WHERE dirty = 1 ORDER BY doc_id LIMIT 1", ()).await?
            .all::<(String, u64)>()?.into_iter().next()
            .map(|(doc, generation)| Ok((doc.parse()?, generation))).transpose()
    }

    pub async fn finish(&self, doc: &DocId, generation: u64, waiting: bool) -> Res<()> {
        self.sql.execute("UPDATE big_blobs_work SET dirty = 0, waiting = ? WHERE doc_id = ? AND generation = ?", (waiting, doc.to_string(), i64::try_from(generation)?)).await?;
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
