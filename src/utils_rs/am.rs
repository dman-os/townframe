use crate::interlude::*;

#[cfg(feature = "automerge-repo")]
pub mod changes;
pub mod codecs;

use automerge::{Automerge, ChangeHash};
use autosurgeon::{Hydrate, Prop, Reconcile};

pub use codecs::AutosurgeonJson;

#[cfg(feature = "automerge-repo")]
use samod::{DocHandle, DocumentId};

#[cfg(feature = "automerge-repo")]
use changes::ChangeListenerManager;

/// Configuration for Automerge storage
#[cfg(feature = "automerge-repo")]
#[derive(Debug, Clone)]
pub struct Config {
    /// Peer ID for this client
    pub peer_id: String,
    /// Storage directory for Automerge documents
    pub storage: StorageConfig,
}

#[cfg(feature = "automerge-repo")]
#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

#[cfg(feature = "automerge-repo")]
#[derive(Clone)]
pub struct AmCtx {
    repo: samod::Repo,
    // peer_id: samod::PeerId,
    // doc_handle: tokio::sync::OnceCell<DocHandle>,
    change_manager: Arc<ChangeListenerManager>,
    handle_cache: Arc<DHashMap<DocumentId, DocHandle>>,
}

#[cfg(feature = "automerge-repo")]
impl AmCtx {
    pub async fn boot<A: samod::AnnouncePolicy>(
        config: Config,
        announce_policy: Option<A>,
    ) -> Res<Self> {
        let peer_id = samod::PeerId::from_string(config.peer_id);

        let repo = samod::Repo::build_tokio().with_peer_id(peer_id.clone());

        let repo = match config.storage {
            StorageConfig::Disk { path } => {
                std::fs::create_dir_all(&path).wrap_err_with(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
                let repo = repo.with_storage(samod::storage::TokioFilesystemStorage::new(
                    path.to_string_lossy().as_ref(),
                ));
                if let Some(policy) = announce_policy {
                    repo.with_announce_policy(policy).load().await
                } else {
                    repo.load().await
                }
            }
            StorageConfig::Memory => {
                let repo = repo.with_storage(samod::storage::InMemoryStorage::new());
                if let Some(policy) = announce_policy {
                    repo.with_announce_policy(policy).load().await
                } else {
                    repo.load().await
                }
            }
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

    pub fn spawn_mpsc_connector(
        &self,
        rx_from_peer: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
        tx_to_peer: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
        direction: samod::ConnDirection,
    ) {
        use futures::StreamExt;
        let repo = self.repo.clone();
        tokio::spawn(
            async move {
                let fin_reason = repo
                    .connect(
                        rx_from_peer.map(Ok::<_, std::convert::Infallible>),
                        tx_to_peer,
                        direction,
                    )
                    .await;
                info!(?fin_reason, "sync server connector task finished");
            }
            .instrument(tracing::info_span!("mpsc sync server connector task")),
        );
    }
    /// Maintains connection to the sync server
    pub fn spawn_ws_connector(&self, addr: std::borrow::Cow<'static, str>) {
        let repo = self.repo.clone();
        tokio::spawn(
            async move {
                let mut attempt = 0u32;
                loop {
                    if attempt > 0 {
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
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
            .instrument(tracing::info_span!("websocket sync server connector task")),
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
                // If path is empty and obj_id is root, use hydrate instead of hydrate_path
                if path.is_empty() && obj_id == automerge::ROOT {
                    let value: T = autosurgeon::hydrate(doc).wrap_err("error hydrating")?;
                    eyre::Ok(Some(value))
                } else {
                    match autosurgeon::hydrate_path(doc, &obj_id, path) {
                        Ok(Some(value)) => eyre::Ok(Some(value)),
                        Ok(None) => Err(ferr!("path not found in document")),
                        Err(e) => Err(ferr!("error hydrating: {e:?}")),
                    }
                }
            })
        })
    }

    pub async fn reconcile_path<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
    ) -> Res<()> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                doc.transact(move |tx| {
                    use automerge::transaction::Transactable;
                    use automerge::ReadDoc;

                    // Navigate to the parent of the final path element
                    let mut current_obj = obj_id;
                    let (final_prop, path_prefix) = if path.is_empty() {
                        return Err(ferr!("path cannot be empty"));
                    } else if path.len() == 1 {
                        (path[0].clone(), vec![])
                    } else {
                        let last_idx = path.len() - 1;
                        let final_prop = path[last_idx].clone();
                        let prefix = path[..last_idx].to_vec();
                        (final_prop, prefix)
                    };

                    // Navigate through the prefix path
                    for prop in &path_prefix {
                        match prop {
                            Prop::Key(key) => match tx.get(&current_obj, key.clone()) {
                                Ok(Some((automerge::Value::Object(_), id))) => {
                                    current_obj = id;
                                }
                                _ => {
                                    let new_obj = tx
                                        .put_object(
                                            &current_obj,
                                            key.clone(),
                                            automerge::ObjType::Map,
                                        )
                                        .wrap_err("error creating map object")?;
                                    current_obj = new_obj;
                                }
                            },
                            Prop::Index(idx) => {
                                let idx_usize = *idx as usize;
                                let len = tx.length(&current_obj) as usize;
                                if idx_usize >= len {
                                    for i in len..=idx_usize {
                                        tx.insert(&current_obj, i, automerge::ScalarValue::Null)
                                            .wrap_err("error extending sequence")?;
                                    }
                                }
                                match tx.get(&current_obj, idx_usize) {
                                    Ok(Some((automerge::Value::Object(_), id))) => {
                                        current_obj = id;
                                    }
                                    _ => {
                                        if idx_usize < len {
                                            tx.delete(&current_obj, idx_usize)
                                                .wrap_err("error deleting existing item")?;
                                        }
                                        let new_obj = tx
                                            .insert_object(
                                                &current_obj,
                                                idx_usize,
                                                automerge::ObjType::Map,
                                            )
                                            .wrap_err("error creating map object")?;
                                        current_obj = new_obj;
                                    }
                                }
                            }
                        }
                    }

                    // Reconcile at the final prop using reconcile_prop
                    autosurgeon::reconcile_prop(tx, current_obj, final_prop, &value)
                        .wrap_err("error reconciling")?;
                    eyre::Ok(())
                })
            })
        })
        .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(())
    }

    pub async fn reconcile_path_at_heads<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
    ) -> Res<()> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        let heads = heads.to_vec();
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                // Start transaction at the specified heads
                let mut tx = doc.transaction_at(automerge::PatchLog::null(), &heads);

                use automerge::transaction::Transactable;
                use automerge::ReadDoc;

                // Navigate to the parent of the final path element
                let mut current_obj = obj_id;
                let (final_prop, path_prefix) = if path.is_empty() {
                    return Err(ferr!("path cannot be empty"));
                } else if path.len() == 1 {
                    (path[0].clone(), vec![])
                } else {
                    let last_idx = path.len() - 1;
                    let final_prop = path[last_idx].clone();
                    let prefix = path[..last_idx].to_vec();
                    (final_prop, prefix)
                };

                // Navigate through the prefix path
                for prop in &path_prefix {
                    match prop {
                        Prop::Key(key) => match tx.get(&current_obj, key.clone()) {
                            Ok(Some((automerge::Value::Object(_), id))) => {
                                current_obj = id;
                            }
                            _ => {
                                let new_obj = tx
                                    .put_object(&current_obj, key.clone(), automerge::ObjType::Map)
                                    .wrap_err("error creating map object")?;
                                current_obj = new_obj;
                            }
                        },
                        Prop::Index(idx) => {
                            let idx_usize = *idx as usize;
                            let len = tx.length(&current_obj) as usize;
                            if idx_usize >= len {
                                for i in len..=idx_usize {
                                    tx.insert(&current_obj, i, automerge::ScalarValue::Null)
                                        .wrap_err("error extending sequence")?;
                                }
                            }
                            match tx.get(&current_obj, idx_usize) {
                                Ok(Some((automerge::Value::Object(_), id))) => {
                                    current_obj = id;
                                }
                                _ => {
                                    if idx_usize < len {
                                        tx.delete(&current_obj, idx_usize)
                                            .wrap_err("error deleting existing item")?;
                                    }
                                    let new_obj = tx
                                        .insert_object(
                                            &current_obj,
                                            idx_usize,
                                            automerge::ObjType::Map,
                                        )
                                        .wrap_err("error creating map object")?;
                                    current_obj = new_obj;
                                }
                            }
                        }
                    }
                }

                // Reconcile at the final prop using reconcile_prop
                autosurgeon::reconcile_prop(&mut tx, current_obj, final_prop, &value)
                    .wrap_err("error reconciling")?;

                // Commit the transaction
                tx.commit();
                eyre::Ok(())
            })
        })
        .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(())
    }

    pub async fn hydrate_path_at_heads<T: autosurgeon::Hydrate>(
        &self,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Result<Option<T>, HydrateAtHeadError> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                let version = match doc.fork_at(heads) {
                    Err(automerge::AutomergeError::InvalidHash(hash)) => {
                        return Err(HydrateAtHeadError::HashNotFound(hash))
                    }
                    val => val.wrap_err("error forking doc at change")?,
                };
                // If path is empty and obj_id is root, use hydrate instead of hydrate_path
                let value: Option<T> = if path.is_empty() && obj_id == automerge::ROOT {
                    Some(autosurgeon::hydrate(&version).wrap_err("error hydrating")?)
                } else {
                    match autosurgeon::hydrate_path(&version, &obj_id, path) {
                        Ok(Some(v)) => Some(v),
                        Ok(None) => None,
                        Err(e) => {
                            return Err(HydrateAtHeadError::Other(ferr!("error hydrating: {e:?}")))
                        }
                    }
                };
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
