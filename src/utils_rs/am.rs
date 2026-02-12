use crate::interlude::*;

#[cfg(feature = "automerge-repo")]
pub mod changes;
pub mod codecs;

#[cfg(feature = "automerge-repo")]
use automerge::Automerge;
use automerge::ChangeHash;
#[cfg(feature = "automerge-repo")]
use autosurgeon::{Hydrate, Prop, Reconcile};

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
pub struct AmCtxStopToken {
    pub repo: samod::Repo,
    pub change_manager_stop_token: crate::am::changes::ChangeListenerManagerStopToken,
}

#[cfg(feature = "automerge-repo")]
impl AmCtxStopToken {
    pub async fn stop(self) -> Res<()> {
        self.change_manager_stop_token.stop().await?;
        self.repo.stop().await;
        Ok(())
    }
}
#[cfg(feature = "automerge-repo")]
pub struct RepoConnection {
    pub peer_info: samod::PeerInfo,
    pub join_handle: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "automerge-repo")]
impl AmCtx {
    pub async fn boot<A: samod::AnnouncePolicy>(
        config: Config,
        announce_policy: Option<A>,
    ) -> Res<(Self, AmCtxStopToken)> {
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

        let (change_manager, change_manager_stop_token) = ChangeListenerManager::boot();
        let out = Self {
            repo: repo.clone(),
            // peer_id,
            change_manager,
            handle_cache: default(),
        };

        Ok((
            out,
            AmCtxStopToken {
                repo,
                change_manager_stop_token,
            },
        ))
    }

    pub async fn spawn_connection_mpsc(
        &self,
        rx_from_peer: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
        tx_to_peer: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
        direction: samod::ConnDirection,
    ) -> Res<RepoConnection> {
        use futures::StreamExt;
        let repo = self.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                rx_from_peer.map(Ok::<_, std::convert::Infallible>),
                tx_to_peer,
                direction,
            )
        })
        .wrap_err("failed to establish connection")?;
        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| ferr!("failed on handshake: {err:?}"))?;
        let join_handle = tokio::spawn(
            async move {
                let fin_reason = conn.finished().await;
                info!(?fin_reason, "sync server connector task finished");
            }
            .instrument(tracing::info_span!("mpsc sync server connector task")),
        );

        Ok(RepoConnection {
            peer_info,
            join_handle,
        })
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
                            let fin =
                                repo.connect_tungstenite(conn, samod::ConnDirection::Outgoing);
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
    ) -> Res<Option<ChangeHash>>
    where
        T: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'a>> + Send + Sync + 'static,
    {
        self.reconcile_prop_with_actor(doc_id, obj_id, prop_name, update, None)
            .await
    }

    pub async fn reconcile_prop_with_actor<'a, T, P>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        prop_name: P,
        update: &T,
        actor_id: Option<automerge::ActorId>,
    ) -> Res<Option<ChangeHash>>
    where
        T: Hydrate + Reconcile + Send + Sync + 'static,
        P: Into<Prop<'a>> + Send + Sync + 'static,
    {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        let res = handle
            .with_document(|doc| {
                if let Some(actor) = &actor_id {
                    doc.set_actor(actor.clone());
                }
                doc.transact(|tx| {
                    autosurgeon::reconcile_prop(tx, obj_id, prop_name, update)
                        .wrap_err("error reconciling")?;
                    eyre::Ok(())
                })
            })
            .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(res.hash)
    }

    // FIXME: this actually returns an error on failing to find it
    pub async fn hydrate_path<T: Hydrate + Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Res<Option<(T, Arc<[automerge::ChangeHash]>)>> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        handle.with_document(|doc| {
            let heads: Arc<[automerge::ChangeHash]> = Arc::from(doc.get_heads());
            // If path is empty and obj_id is root, use hydrate instead of hydrate_path
            if path.is_empty() && obj_id == automerge::ROOT {
                let value: T = autosurgeon::hydrate(doc).wrap_err("error hydrating")?;
                eyre::Ok(Some((value, heads)))
            } else {
                match autosurgeon::hydrate_path(doc, &obj_id, path.clone()) {
                    Ok(Some(value)) => eyre::Ok(Some((value, heads))),
                    Ok(None) => eyre::Ok(None),
                    Err(err) => Err(ferr!("error hydrating: {err:?}")),
                }
            }
        })
    }

    pub async fn reconcile_path<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
    ) -> Res<Option<ChangeHash>> {
        self.reconcile_path_with_actor(doc_id, obj_id, path, value, None)
            .await
    }

    pub async fn reconcile_path_with_actor<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
        actor_id: Option<automerge::ActorId>,
    ) -> Res<Option<ChangeHash>> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        let res = handle
            .with_document(|doc| {
                if let Some(actor) = &actor_id {
                    doc.set_actor(actor.clone());
                }
                doc.transact(|tx| {
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
                                let len = tx.length(&current_obj);
                                if idx_usize >= len {
                                    for idx in len..=idx_usize {
                                        tx.insert(&current_obj, idx, automerge::ScalarValue::Null)
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
                    autosurgeon::reconcile_prop(tx, current_obj, final_prop, value)
                        .wrap_err("error reconciling")?;
                    eyre::Ok(())
                })
            })
            .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(res.hash)
    }

    pub async fn reconcile_path_at_heads<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
    ) -> Res<Option<ChangeHash>> {
        self.reconcile_path_at_heads_with_actor(doc_id, heads, obj_id, path, value, None)
            .await
    }

    pub async fn reconcile_path_at_heads_with_actor<T: Reconcile + Send + Sync + 'static>(
        &self,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
        value: &T,
        actor_id: Option<automerge::ActorId>,
    ) -> Res<Option<ChangeHash>> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        let heads = heads.to_vec();
        let hash = handle
            .with_document(|doc| {
                if let Some(actor) = &actor_id {
                    doc.set_actor(actor.clone());
                }
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
                            let len = tx.length(&current_obj);
                            if idx_usize >= len {
                                for idx in len..=idx_usize {
                                    tx.insert(&current_obj, idx, automerge::ScalarValue::Null)
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
                autosurgeon::reconcile_prop(&mut tx, current_obj, final_prop, value)
                    .wrap_err("error reconciling")?;

                // Commit the transaction
                let (hash, _log) = tx.commit();
                eyre::Ok(hash)
            })
            .map_err(|err| ferr!("error on samod txn: {err:?}"))?;
        Ok(hash)
    }

    pub async fn hydrate_path_at_heads<T: autosurgeon::Hydrate>(
        &self,
        doc_id: &DocumentId,
        heads: &[automerge::ChangeHash],
        obj_id: automerge::ObjId,
        path: Vec<Prop<'static>>,
    ) -> Result<Option<(T, Arc<[automerge::ChangeHash]>)>, HydrateAtHeadError> {
        let handle = self.find_doc(doc_id).await?.ok_or_eyre("doc not found")?;
        handle.with_document(|doc| {
            let version = match doc.fork_at(heads) {
                Err(automerge::AutomergeError::InvalidHash(hash)) => {
                    return Err(HydrateAtHeadError::HashNotFound(hash))
                }
                val => val.wrap_err("error forking doc at change")?,
            };
            let heads: Arc<[automerge::ChangeHash]> = Arc::from(version.get_heads());
            // If path is empty and obj_id is root, use hydrate instead of hydrate_path
            let value: Option<T> = if path.is_empty() && obj_id == automerge::ROOT {
                Some(autosurgeon::hydrate(&version).wrap_err("error hydrating")?)
            } else {
                match autosurgeon::hydrate_path(&version, &obj_id, path) {
                    Ok(Some(val)) => Some(val),
                    Ok(None) => None,
                    Err(err) => {
                        return Err(HydrateAtHeadError::Other(ferr!("error hydrating: {err:?}")))
                    }
                }
            };
            Ok(value.map(|item| (item, heads)))
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

    // FIXME: hide samod from AmCtx consumers
    #[deprecated]
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

pub fn parse_commit_heads<S: AsRef<str>>(heads: &[S]) -> Res<Arc<[ChangeHash]>> {
    heads
        .iter()
        .map(|commit| {
            let mut buf = [0u8; 32];
            crate::hash::decode_base58_multibase_onto(commit.as_ref(), &mut buf)?;
            eyre::Ok(automerge::ChangeHash(buf))
        })
        .collect()
}

pub fn serialize_commit_heads(heads: &[ChangeHash]) -> Vec<String> {
    heads
        .iter()
        .map(|commit| crate::hash::encode_base58_multibase(commit.0))
        .collect()
}

pub fn get_actor_id_from_patch(patch: &automerge::Patch) -> Option<automerge::ActorId> {
    if let automerge::ObjId::Id(_, actor_id, _) = &patch.obj {
        Some(actor_id.clone())
    } else {
        None
    }
}

#[test]
fn play() -> Res<()> {
    use automerge::transaction::Transactable;
    use automerge::ReadDoc;

    let mut doc = automerge::AutoCommit::new();
    let map = doc.put_object(automerge::ROOT, "map", automerge::ObjType::Map)?;
    let obj1 = doc.put_object(map.clone(), "foo", automerge::ObjType::Map)?;
    doc.put(obj1.clone(), "key1", 1)?;
    doc.commit();
    let commit1 = doc.get_heads();
    doc.put(obj1.clone(), "key2", 2)?;
    doc.put(obj1.clone(), "key3", 3)?;
    let obj2 = doc.put_object(map.clone(), "bar", automerge::ObjType::Map)?;
    doc.put(obj2.clone(), "key1", 1)?;
    doc.commit();
    let commit2 = doc.get_heads();

    let _patches = doc.diff(&commit1, &commit2);

    let _obj1 = doc.put_object(map.clone(), "foo", automerge::ObjType::Map)?;
    doc.commit();
    let commit3 = doc.get_heads();
    let patches = doc.diff(&commit2, &commit3);
    let json = doc.hydrate(automerge::ROOT, None)?;

    println!("{patches:#?} {json:#?}");

    Ok(())
}
