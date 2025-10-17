use crate::interlude::*;

pub mod changes;
pub mod codecs;

use automerge::{Automerge, ChangeHash};
use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconcile, Reconciler};
use samod::{DocHandle, DocumentId};

use changes::ChangeListenerManager;

/// Configuration for Automerge storage
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage directory for Automerge documents
    pub storage_dir: PathBuf,
    /// Peer ID for this client
    pub peer_id: String,
}

pub struct AmCtx {
    repo: samod::Repo,
    // peer_id: samod::PeerId,
    // doc_handle: tokio::sync::OnceCell<DocHandle>,
    change_manager: Arc<ChangeListenerManager>,
    handle_cache: Arc<DHashMap<DocumentId, DocHandle>>,
}

impl AmCtx {
    pub async fn boot<A: samod::AnnouncePolicy>(
        config: Config,
        announce_policy: Option<A>,
    ) -> Res<Self> {
        let peer_id = samod::PeerId::from_string(config.peer_id);

        // Ensure the storage directory exists
        std::fs::create_dir_all(&config.storage_dir).wrap_err_with(|| {
            format!(
                "Failed to create storage directory: {}",
                config.storage_dir.display()
            )
        })?;

        let repo = samod::Repo::build_tokio()
            .with_peer_id(peer_id.clone())
            .with_storage(samod::storage::TokioFilesystemStorage::new(
                config.storage_dir.to_string_lossy().as_ref(),
            ));
        let repo = if let Some(policy) = announce_policy {
            repo.with_announce_policy(policy).load().await
        } else {
            repo.load().await
        };

        let change_manager = ChangeListenerManager::boot();
        let out = Self {
            repo,
            // peer_id,
            change_manager,
            handle_cache: default(),
        };

        Ok(out)
    }

    /// Maintains connection to the sync server
    pub fn spawn_connector(&self, addr: std::borrow::Cow<'static, str>) {
        let repo = self.repo.clone();
        tokio::spawn(
            async move {
                let mut attempt = 0u32;
                loop {
                    if attempt > 0 {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    attempt += 1;
                    match tokio_tungstenite::connect_async(&addr[..]).await {
                        Ok((conn, resp)) => {
                            if resp.status().as_u16() != 101 {
                                error!(?resp, "bad response connecting to server");
                                continue;
                            }
                            let fin = repo
                                .connect_tungstenite(conn, samod::ConnDirection::Outgoing)
                                .await;
                            warn!(?fin, "connection closed");
                        }
                        Err(err) => {
                            warn!(?attempt, "error connecting to sync server {err}");
                            continue;
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("sync server connector task")),
        );
    }

    pub async fn reconcile_prop<'a, T, P>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &T,
    ) -> Res<()>
    where
        T: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'a>> + Send + Sync + 'static,
    {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                doc.transact(move |tx| {
                    autosurgeon::reconcile_prop(tx, obj_id, prop_name, update)
                        .wrap_err("error reconciling")?;
                    eyre::Ok(())
                })
            })
        })
        .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(())
    }

    // FIXME: this actually returns an error on failing to find it
    pub async fn hydrate_path<T: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<T>> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                let value: Option<T> =
                    autosurgeon::hydrate_path(doc, &obj_id, path).wrap_err("error hydrating")?;
                eyre::Ok(value)
            })
        })
    }

    pub async fn hydrate_path_at_head<T: autosurgeon::Hydrate>(
        &self,
        doc_id: &DocumentId,
        head: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Result<Option<T>, HydrateAtHeadError> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                let cur = doc.get_heads();
                let cur_formatted = serialize_commit_heads(&cur);
                let cur_roundtrip = parse_commit_heads(&cur_formatted).unwrap();
                let value = doc.hydrate(None);
                info!(?head, ?cur, ?cur_formatted, ?cur_roundtrip, ?value, "XXX");
                let version = match doc.fork_at(head) {
                    Err(automerge::AutomergeError::InvalidHash(hash)) => {
                        return Err(HydrateAtHeadError::HashNotFound(hash))
                    }
                    val => val.wrap_err("error forking doc at change")?,
                };
                let value: Option<T> = autosurgeon::hydrate_path(&version, &obj_id, path)
                    .wrap_err("error hydrating")?;
                Ok(value)
            })
        })
    }

    pub async fn add_doc(&self, doc: Automerge) -> Res<DocHandle> {
        let handle = self.repo.create(doc).await?;
        self.handle_cache
            .insert(handle.document_id().clone(), handle.clone());
        Ok(handle)
    }

    pub async fn find_doc(&self, doc_id: &DocumentId) -> Res<Option<DocHandle>> {
        if let Some(handle) = self.handle_cache.get(doc_id) {
            return Ok(Some(handle.clone()));
        }
        let Some(handle) = self.repo.find(doc_id.clone()).await? else {
            return Ok(None);
        };
        self.handle_cache.insert(doc_id.clone(), handle.clone());
        Ok(Some(handle))
    }

    pub fn repo(&self) -> &samod::Repo {
        &self.repo
    }

    pub fn change_manager(&self) -> &Arc<ChangeListenerManager> {
        &self.change_manager
    }
}

#[derive(Debug, displaydoc::Display, thiserror::Error)]
pub enum HydrateAtHeadError {
    /// hash not found {0:?}
    HashNotFound(ChangeHash),
    /// {0}
    Other(#[from] eyre::Report),
}

#[cfg(feature = "hash")]
pub fn parse_commit_heads<S: AsRef<str>>(heads: &[S]) -> Res<Arc<[ChangeHash]>> {
    heads
        .iter()
        .map(|commit| {
            crate::hash::decode_base32_multibase(commit.as_ref())
                .and_then(|bytes| bytes.as_slice().try_into().wrap_err("invalid change hash"))
        })
        .collect()
}

#[cfg(feature = "hash")]
pub fn serialize_commit_heads(heads: &[ChangeHash]) -> Vec<String> {
    heads
        .iter()
        .map(|commit| crate::hash::encode_base32_multibase(commit.0))
        .collect()
}
