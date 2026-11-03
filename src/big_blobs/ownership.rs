use crate::encrypt::CipherBlobProvider;
use crate::pair_roots::PairRoots;
pub(crate) mod sqlite;
use crate::service::ImportedBlob;
use big_blobs_core::BlobId;
use futures::future::BoxFuture;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_blobs::HashAndFormat;
use iroh_blobs::api::Store;
use iroh_blobs::api::TempTag;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use sqlite_rs::Tx;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre;

type Operation = Box<dyn for<'a> FnOnce(&'a mut State) -> BoxFuture<'a, ()> + Send>;

#[derive(Clone)]
pub(crate) struct ByteOwnership {
    sender: mpsc::Sender<Operation>,
}

struct State {
    sql: Database,
    store: Store,
    provider: Arc<CipherBlobProvider>,
    roots: PairRoots,
    tags: Vec<TempTag>,
}

pub(crate) struct OwnershipTransaction {
    tx: Tx,
    store: Store,

    tags: Vec<TempTag>,
}

pub(crate) struct OwnershipPublication<'a> {
    state: &'a mut State,
}

impl OwnershipPublication<'_> {
    #[cfg(test)]
    pub(crate) fn store(&self) -> &Store {
        &self.state.store
    }

    #[cfg(test)]
    pub(crate) fn provider(&self) -> &CipherBlobProvider {
        &self.state.provider
    }

    pub(crate) async fn claim(&mut self, owner: &str, ids: &[BlobId]) -> Res<()> {
        let owner = owner.to_owned();
        let ids = ids.to_vec();
        self.state
            .transaction(move |tx| Box::pin(async move { tx.claim(&owner, &ids).await }))
            .await
    }

    pub(crate) async fn release(&mut self, owner: &str) -> Res<()> {
        let owner = owner.to_owned();
        self.state
            .transaction(move |tx| Box::pin(async move { tx.release(&owner).await }))
            .await
    }
}

impl ByteOwnership {
    pub(crate) async fn boot(
        sql: Database,
        store: Store,
        provider: Arc<CipherBlobProvider>,
    ) -> Res<(Self, StopToken)> {
        let mut state = State::new(sql, store, provider).await?;
        let (sender, mut receiver) = mpsc::channel::<Operation>(128);
        let (stop, tasks) = StopToken::new();
        let cancellation = tasks.clone();
        tasks
            .spawn(async move {
                loop {
                    let operation = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            receiver.close();
                            break;
                        }
                        operation = receiver.recv() => operation,
                    };
                    let Some(operation) = operation else { break };
                    operation(&mut state).await;
                }
                while let Some(operation) = receiver.recv().await {
                    operation(&mut state).await;
                }
                state
                    .recover()
                    .await
                    .expect("byte ownership shutdown recovery failed");
            })
            .expect("byte ownership owner starts before shutdown");
        Ok((Self { sender }, stop))
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl for<'a> FnOnce(&'a mut State) -> BoxFuture<'a, Res<T>> + Send + 'static,
    ) -> Res<T> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Box::new(move |state| {
                Box::pin(async move {
                    let result = operation(state).await;
                    if let Err(result) = reply.send(result) {
                        result.expect("cancelled byte ownership operation failed");
                    }
                })
            }))
            .await
            .unwrap_or_else(|_| panic!("byte ownership owner stopped before its clients"));
        response
            .await
            .expect("byte ownership owner dropped its response")
    }

    pub(crate) async fn publish<T: Send + 'static>(
        &self,
        operation: impl for<'a, 'b> FnOnce(&'a mut OwnershipPublication<'b>) -> BoxFuture<'a, Res<T>>
        + Send
        + 'static,
    ) -> Res<T> {
        self.run(move |state| {
            Box::pin(async move { operation(&mut OwnershipPublication { state }).await })
        })
        .await
    }

    pub(crate) async fn transaction<T: Send + 'static>(
        &self,
        operation: impl for<'a> FnOnce(&'a mut OwnershipTransaction) -> BoxFuture<'a, Res<T>>
        + Send
        + 'static,
    ) -> Res<T> {
        self.run(move |state| Box::pin(async move { state.transaction(operation).await }))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn claim(&self, owner: &str, ids: &[BlobId]) -> Res<()> {
        let owner = owner.to_owned();
        let ids = ids.to_vec();
        self.transaction(move |tx| Box::pin(async move { tx.claim(&owner, &ids).await }))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn replace(&self, owner: &str, ids: &[BlobId]) -> Res<()> {
        let owner = owner.to_owned();
        let ids = ids.to_vec();
        self.transaction(move |tx| Box::pin(async move { tx.replace(&owner, &ids).await }))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn release(&self, owner: &str) -> Res<()> {
        let owner = owner.to_owned();
        self.transaction(move |tx| Box::pin(async move { tx.release(&owner).await }))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn set_owner(
        &self,
        owner: &str,
        id: &BlobId,
        active: bool,
        tags: Vec<TempTag>,
    ) -> Res<()> {
        let owner = owner.to_owned();
        let id = *id;
        self.transaction(move |tx| {
            Box::pin(async move {
                tx.tags.extend(tags);
                OwnershipTransaction::validate(&owner)?;
                if active {
                    tx.claim(&owner, &[id]).await?;
                } else {
                    let removed = sqlite::queries::delete_owner_digest(
                        tx.sql(),
                        owner.clone(),
                        id.to_string(),
                    )
                    .await?;
                    if removed.rows_affected != 0 {
                        tx.dirty(&id).await?;
                    }
                }
                Ok(())
            })
        })
        .await
    }

    pub(crate) async fn stage(&self, imported: ImportedBlob, tag: TempTag) -> Res<()> {
        self.transaction(move |tx| {
            Box::pin(async move {
                tx.tags.push(tag);
                let owner = imported.owner();
                sqlite::queries::insert_upload_receipt(
                    tx.sql(),
                    owner.clone(),
                    imported.info.blob_id.to_string(),
                )
                .await?;

                tx.claim(&owner, &[imported.info.blob_id]).await
            })
        })
        .await
    }

    pub(crate) async fn adopt(&self, imported: &ImportedBlob, document_owner: &str) -> Res<()> {
        let staging_owner = imported.owner();
        let document_owner = document_owner.to_owned();
        self.transaction(move |tx| {
            Box::pin(async move {
                OwnershipTransaction::validate(&document_owner)?;
                eyre::ensure!(
                    document_owner.starts_with("document:"),
                    "staging adoption requires a document owner"
                );
                let retained = sqlite::queries::has_staged_document(
                    tx.sql(),
                    staging_owner.clone(),
                    document_owner.clone(),
                )
                .await?;
                eyre::ensure!(
                    !retained.is_empty(),
                    "document ownership must be committed before staging adoption"
                );
                tx.release_staging(&staging_owner).await
            })
        })
        .await
    }

    pub(crate) async fn release_import(&self, imported: &ImportedBlob) -> Res<()> {
        let owner = imported.owner();
        self.transaction(move |tx| Box::pin(async move { tx.release_staging(&owner).await }))
            .await
    }

    pub(crate) async fn contains(&self, id: &BlobId) -> Res<bool> {
        let id = *id;
        self.run(move |state| Box::pin(async move { state.contains(&id).await }))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn recover(&self) -> Res<()> {
        self.run(move |state| Box::pin(async move { state.recover().await }))
            .await
    }

    pub(crate) async fn resolve_pair(&self, id: &BlobId) -> Res<()> {
        let id = *id;
        self.transaction(move |tx| {
            Box::pin(async move {
                tx.dependency(&id).await?;
                tx.dirty(&id).await?;
                crate::pair_roots::sqlite::queries::delete_root(
                    tx.sql(),
                    Hash::from(id.to_bytes32()).to_hex().to_string(),
                )
                .await?;
                Ok(())
            })
        })
        .await
    }
}

impl State {
    async fn new(sql: Database, store: Store, provider: Arc<CipherBlobProvider>) -> Res<Self> {
        let roots = PairRoots::boot(sql.clone()).await?;
        sql.apply_migrations(
            "big_blobs_byte_ownership_migrations",
            sqlite::queries::MIGRATIONS,
        )
        .await?;
        let roots = PairRoots::boot(sql.clone()).await?;
        let mut ownership = Self {
            sql,
            store,
            provider,
            tags: Vec::new(),
            roots,
        };
        ownership.recover().await?;
        Ok(ownership)
    }

    async fn begin(&self) -> Res<OwnershipTransaction> {
        Ok(OwnershipTransaction {
            tx: self.sql.begin_write().await?,
            store: self.store.clone(),
            tags: Vec::new(),

        })
    }

    async fn transaction<T>(
        &mut self,
        operation: impl for<'a> FnOnce(&'a mut OwnershipTransaction) -> BoxFuture<'a, Res<T>>,
    ) -> Res<T> {
        let mut transaction = self.begin().await?;
        let result = operation(&mut transaction).await?;
        let OwnershipTransaction { tx, tags, .. } = transaction;
        self.tags.extend(tags);
        tx.commit().await?;
        self.apply_pending().await?;
        self.tags.clear();
        Ok(result)
    }

    async fn contains(&self, id: &BlobId) -> Res<bool> {
        Ok(!sqlite::queries::contains(&self.sql, id.to_string())
            .await?
            .is_empty())
    }

    async fn recover(&mut self) -> Res<()> {
        self.transaction(|tx| {
            Box::pin(async move {
                sqlite::queries::mark_effects(tx.sql()).await?;

                Ok(())
            })
        })
        .await
    }

    async fn apply_pending(&self) -> Res<()> {

        let pending = sqlite::queries::pending_effects(&self.sql).await?;
        let mut effects = Vec::with_capacity(pending.len());
        for digest in pending {
            let id: BlobId = digest.parse()?;
            let retained = !sqlite::queries::retained(&self.sql, digest.clone())
                .await?
                .is_empty();
            let pinned = !sqlite::queries::pinned(&self.sql, digest.clone())
                .await?
                .is_empty();
            let hash = Hash::from(id.to_bytes32());
            if retained {
                self.store
                    .tags()
                    .set(format!("owned:{hash}"), HashAndFormat::raw(hash))
                    .await?;
            }
            if pinned {
                self.store
                    .tags()
                    .set(format!("pin:{hash}"), HashAndFormat::raw(hash))
                    .await?;
            }
            let managed_pin = !sqlite::queries::managed_pin(&self.sql, digest.clone())
                .await?
                .is_empty();
            effects.push((digest, hash, retained, pinned, managed_pin));
        }
        for (_, hash, retained, pinned, managed_pin) in &effects {
            if *managed_pin && !pinned {
                self.store.tags().delete(format!("pin:{hash}")).await?;
            }
            if !retained {
                self.store.tags().delete(format!("owned:{hash}")).await?;
            }
        }
        for (digest, hash, retained, pinned, _) in effects {
            let tx = self.sql.begin_write().await?;
            let dependencies = sqlite::queries::dependencies(&tx, digest.clone()).await?;
            let preclaim = self.roots.is_claimed(hash).await?;
            let pin = self
                .store
                .tags()
                .get(format!("pin:{hash}"))
                .await?
                .is_some();
            if !retained && !preclaim && !pin && !dependencies.is_empty() {
                crate::encrypt::drop_pair_tags(&self.store, hash).await?;
                self.provider.remove(&hash);
            }
            if !retained && !preclaim && !pin {
                sqlite::queries::delete_dependency(&tx, digest.clone()).await?;

            }
            if !pinned {
                sqlite::queries::delete_pin_root(&tx, digest.clone()).await?;
            }
            sqlite::queries::delete_effect(&tx, digest.clone()).await?;
            tx.commit().await?;
        }
        Ok(())
    }
}

impl OwnershipTransaction {
    pub(crate) fn sql(&self) -> &Tx {
        &self.tx
    }

    fn validate(owner: &str) -> Res<()> {
        let (namespace, key) = owner
            .split_once(':')
            .ok_or_else(|| eyre::eyre!("byte owner requires a namespace"))?;
        eyre::ensure!(
            matches!(
                namespace,
                "upload"
                    | "document"
                    | "writer"
                    | "mirror"
                    | "quarantine"
                    | "in-flight"
                    | "inventory"
            ) && !key.is_empty(),
            "invalid byte owner namespace or empty owner key"
        );
        Ok(())
    }

    pub(crate) async fn contains(&self, id: &BlobId) -> Res<bool> {
        Ok(!sqlite::queries::contains(&self.tx, id.to_string())
            .await?
            .is_empty())
    }

    async fn dependency(&mut self, id: &BlobId) -> Res<()> {
        let hash = Hash::from(id.to_bytes32());
        if let Some(plain) = self.store.tags().get(format!("pt:{hash}")).await? {
            eyre::ensure!(
                plain.format == BlobFormat::Raw,
                "non-raw plaintext dependency"
            );
            let plaintext = BlobId::new(*plain.hash.as_bytes()).to_string();
            let previous = sqlite::queries::dependencies(&self.tx, id.to_string()).await?;
            eyre::ensure!(
                previous.first().is_none_or(|value| value == &plaintext),
                "virtual plaintext dependency changed"
            );
            sqlite::queries::insert_dependency(&self.tx, id.to_string(), plaintext).await?;
        }
        Ok(())
    }

    async fn dirty(&self, id: &BlobId) -> Res<()> {
        sqlite::queries::insert_effect(&self.tx, id.to_string()).await?;
        sqlite::queries::insert_effect_plaintext(&self.tx, id.to_string()).await?;

        Ok(())
    }

    pub(crate) async fn claim(&mut self, owner: &str, ids: &[BlobId]) -> Res<()> {
        Self::validate(owner)?;
        for id in ids {
            let hash = Hash::from(id.to_bytes32());
            self.tags
                .push(self.store.tags().temp_tag(HashAndFormat::raw(hash)).await?);
            self.dependency(id).await?;
            let dependencies = sqlite::queries::dependencies(&self.tx, id.to_string()).await?;

            if let Some(plaintext) = dependencies.first() {
                let plain: BlobId = plaintext.parse()?;
                let hash = Hash::from(plain.to_bytes32());
                self.tags
                    .push(self.store.tags().temp_tag(HashAndFormat::raw(hash)).await?);
                eyre::ensure!(
                    self.store.blobs().sync_reader(hash).await?.is_some(),
                    "virtual byte claim has no stored plaintext"
                );
            } else {
                eyre::ensure!(
                    self.store.blobs().sync_reader(hash).await?.is_some(),
                    "byte claim requires complete stored bytes"
                );
            }
            if !self.contains(id).await? {
                eyre::ensure!(
                    matches!(
                        self.store.blobs().status(hash).await?,
                        iroh_blobs::api::proto::BlobStatus::Complete { .. }
                    ),
                    "byte claim requires complete stored bytes"
                );
            }
            sqlite::queries::insert_owner(&self.tx, owner.to_owned(), id.to_string()).await?;

            if owner.starts_with("document:retention:") || owner.starts_with("writer:retention:") {

                    sqlite::queries::insert_pin_root(&self.tx, id.to_string()).await?;
            }
            self.dirty(id).await?;
        }
        Ok(())
    }

    async fn release_staging(&self, owner: &str) -> Res<()> {
        let released = sqlite::queries::release_staging(&self.tx, owner.to_owned()).await?;
        if released.rows_affected != 0 {
            self.release(owner).await?;
        }
        Ok(())
    }

    pub(crate) async fn release(&self, owner: &str) -> Res<()> {
        Self::validate(owner)?;
        let previous = sqlite::queries::owner_digests(&self.tx, owner.to_owned()).await?;
        for id in previous {
            self.dirty(&id.parse()?).await?;
        }
        sqlite::queries::delete_owner(&self.tx, owner.to_owned()).await?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn replace(&mut self, owner: &str, ids: &[BlobId]) -> Res<()> {
        self.claim(owner, ids).await?;
        let retained: std::collections::BTreeSet<_> = ids.iter().copied().collect();
        let previous = sqlite::queries::owner_digests(&self.tx, owner.to_owned()).await?;
        for digest in previous {
            let id: BlobId = digest.parse()?;
            if !retained.contains(&id) {
                sqlite::queries::delete_owner_digest(
                    &self.tx,
                    owner.to_owned(),
                    digest.clone(),
                )
                .await?;
                self.dirty(&id).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn reconcile(
        &mut self,
        prefix: &str,
        id: &BlobId,
        owners: &[String],
    ) -> Res<()> {
        Self::validate(prefix)?;
        eyre::ensure!(
            prefix.ends_with(':'),
            "reconciliation prefix must end at an owner boundary"
        );

        let existing = sqlite::queries::reconcile_owners(
            &self.tx,
            id.to_string(),
            prefix.to_owned(),
        )
        .await?;
        for owner in owners {
            eyre::ensure!(
                owner.starts_with(prefix),
                "owner outside reconciliation namespace"
            );
            self.claim(owner, &[*id]).await?;
        }
        for owner in existing {
            if !owners.contains(&owner) {
                sqlite::queries::delete_owner_digest(
                    &self.tx,
                    owner.clone(),
                    id.to_string(),
                )
                .await?;

                self.dirty(id).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
