use crate::interlude::*;

use samod::DocumentId;
use tokio::sync::broadcast;

mod partition;

#[derive(Debug, Clone)]
pub struct BigRepoConfig {
    pub sqlite_url: String,
    pub subscription_capacity: usize,
}

impl BigRepoConfig {
    pub fn new(sqlite_url: impl Into<String>) -> Self {
        Self {
            sqlite_url: sqlite_url.into(),
            subscription_capacity: crate::sync::DEFAULT_SUBSCRIPTION_CAPACITY,
        }
    }
}

pub struct BigRepo {
    repo: samod::Repo,
    state_pool: sqlx::SqlitePool,
    partition_events_tx: broadcast::Sender<crate::sync::PartitionEvent>,
}

impl std::fmt::Debug for BigRepo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BigRepo").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct BigDocHandle {
    repo: Arc<BigRepo>,
    inner: samod::DocHandle,
}

impl std::fmt::Debug for BigDocHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BigDocHandle")
            .field("document_id", self.document_id())
            .finish()
    }
}

impl BigRepo {
    pub async fn boot(repo: samod::Repo, config: BigRepoConfig) -> Res<Arc<Self>> {
        let state_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&config.sqlite_url)
            .await
            .wrap_err("failed connecting big repo sqlite")?;
        let (partition_events_tx, _) = broadcast::channel(config.subscription_capacity.max(1));

        let out = Arc::new(Self {
            repo,
            state_pool,
            partition_events_tx,
        });
        out.ensure_schema().await?;
        Ok(out)
    }

    pub fn samod_repo(&self) -> &samod::Repo {
        &self.repo
    }

    pub fn state_pool(&self) -> &sqlx::SqlitePool {
        &self.state_pool
    }

    pub async fn create_doc(
        self: &Arc<Self>,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let handle = self
            .repo
            .create(initial_content)
            .await
            .map_err(|err| ferr!("failed creating doc: {err}"))?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            inner: handle,
        })
    }

    pub async fn import_doc(
        self: &Arc<Self>,
        document_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<BigDocHandle> {
        let handle = self
            .repo
            .import(document_id, initial_content)
            .await
            .map_err(|err| ferr!("failed importing doc: {err}"))?;
        Ok(BigDocHandle {
            repo: Arc::clone(self),
            inner: handle,
        })
    }

    pub async fn find_doc(self: &Arc<Self>, document_id: &DocumentId) -> Res<Option<BigDocHandle>> {
        let handle = self
            .repo
            .find(document_id.clone())
            .await
            .map_err(|err| ferr!("failed finding doc: {err}"))?;
        Ok(handle.map(|inner| BigDocHandle {
            repo: Arc::clone(self),
            inner,
        }))
    }

    async fn on_doc_heads_changed(
        &self,
        doc_id: &DocumentId,
        heads: Vec<automerge::ChangeHash>,
    ) -> Res<()> {
        self.record_doc_heads_change(doc_id, heads).await
    }
}

impl BigDocHandle {
    pub fn document_id(&self) -> &DocumentId {
        self.inner.document_id()
    }

    pub fn raw_handle(&self) -> &samod::DocHandle {
        &self.inner
    }

    pub async fn with_document<F, R>(&self, operation: F) -> Res<R>
    where
        F: FnOnce(&mut automerge::Automerge) -> R,
    {
        let before_heads = self.inner.with_document(|doc| doc.get_heads());
        let out = self.inner.with_document(operation);
        let after_heads = self.inner.with_document(|doc| doc.get_heads());
        if before_heads != after_heads {
            self.repo
                .on_doc_heads_changed(self.document_id(), after_heads)
                .await?;
        }
        Ok(out)
    }
}
