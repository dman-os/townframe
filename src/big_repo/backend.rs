use crate::interlude::*;

use crate::rpc::FullDoc;

#[derive(Clone)]
pub struct BigRepoSyncBackend {
    repo: std::sync::Weak<crate::BigRepo>,
    repo_rpc_endpoint: iroh::Endpoint,
    remote_repo_clients:
        Arc<surelock::mutex::Mutex<std::collections::HashMap<PeerId, Arc<RepoRpcClient>>>>,
}

#[derive(Clone)]
struct RepoRpcClient {
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
}

impl RepoRpcClient {
    fn new(endpoint: iroh::Endpoint, endpoint_addr: iroh::EndpointAddr) -> Self {
        Self {
            endpoint,
            endpoint_addr,
        }
    }

    async fn get_docs_full(&self, doc_ids: Vec<String>) -> Res<Vec<FullDoc>> {
        let client = irpc_iroh::client::<crate::rpc::RepoSyncRpc>(
            self.endpoint.clone(),
            self.endpoint_addr.clone(),
            crate::rpc::REPO_SYNC_ALPN,
        );
        let response = client
            .rpc(crate::rpc::GetDocsFullRpcReq {
                req: crate::rpc::GetDocsFullRequest { doc_ids },
            })
            .await
            .wrap_err("GetDocsFull rpc failure")?
            .wrap_err("GetDocsFull rejected")?;
        Ok(response.docs)
    }
}

impl BigRepoSyncBackend {
    pub async fn boot(
        repo: std::sync::Weak<crate::BigRepo>,
        endpoint: iroh::Endpoint,
    ) -> Res<Self> {
        Ok(Self {
            repo,
            repo_rpc_endpoint: endpoint,
            remote_repo_clients: surelock::mutex::Mutex::new(default()).into(),
        })
    }

    pub fn register_remote_peer(&self, peer_id: PeerId, endpoint_addr: iroh::EndpointAddr) {
        surelock::key::lock_scope(|key| {
            let (mut remote_repo_clients, _key) = key.lock(&self.remote_repo_clients);
            if endpoint_addr.addrs.is_empty()
                && remote_repo_clients
                    .get(&peer_id)
                    .is_some_and(|existing| !existing.endpoint_addr.addrs.is_empty())
            {
                return;
            }
            let client = Arc::new(RepoRpcClient::new(
                self.repo_rpc_endpoint.clone(),
                endpoint_addr,
            ));
            remote_repo_clients.insert(peer_id, client);
        })
    }

    pub fn unregister_remote_peer(&self, peer_id: PeerId) {
        surelock::key::lock_scope(|key| {
            let (mut remote_repo_clients, _key) = key.lock(&self.remote_repo_clients);
            remote_repo_clients.remove(&peer_id);
        })
    }

    fn remote_repo_client(&self, peer_id: PeerId) -> Res<Arc<RepoRpcClient>> {
        surelock::key::lock_scope(|key| {
            let (remote_repo_clients, _key) = key.lock(&self.remote_repo_clients);
            remote_repo_clients
                .get(&peer_id)
                .cloned()
                .ok_or_else(|| ferr!("missing repo rpc client for peer {peer_id:?}"))
        })
    }
}

#[async_trait::async_trait]
impl big_sync::SyncBackend for BigRepoSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: big_sync_core::ObjId,
        remote_payload: Option<big_sync::ObjPayload>,
    ) -> Res<big_sync::SyncTaskRunOutcome> {
        let repo: Arc<crate::BigRepo> = self
            .repo
            .upgrade()
            .ok_or_else(|| eyre::eyre!("big repo dropped while sync backend was active"))?;

        let doc_id: crate::DocumentId = obj_id.into();
        let local_heads = super::partition_doc_heads_payload(&repo.big_sync_store, doc_id).await?;
        if local_heads.is_none() {
            // Doc not yet synced locally. Pull from peer via Subduction sync.
            // Subduction syncs from an empty tree — the peer sends everything it has.
            repo.runtime
                .sync_doc_with_peer(doc_id, peer_id, Some(Duration::from_secs(10)))
                .await
                .map_err(|e| match e {
                    crate::SyncDocError::NotFound => eyre::eyre!("remote doc was not found"),
                    crate::SyncDocError::Unauthorized => eyre::eyre!("remote doc access denied"),
                    _ => eyre::eyre!("{e}"),
                })?;
            return Ok(big_sync::SyncTaskRunOutcome::Completion(
                big_sync_core::SyncTaskCompletion {
                    obj_id,
                    deets: big_sync_core::SyncCompletionDeets::AddedMember,
                },
            ));
        }
        let local_heads = local_heads.expect("checked above");
        if let Some(remote_payload) = &remote_payload {
            let remote_heads = super::doc_heads_from_payload(remote_payload.clone());
            if local_heads.as_ref() == remote_heads.as_ref() {
                return Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    },
                ));
            }
        }
        match repo
            .runtime
            .sync_doc_with_peer(doc_id, peer_id, Some(Duration::from_secs(10)))
            .await
        {
            Ok(()) => {
                let current_doc = repo
                    .get_doc(&doc_id)
                    .await?
                    .ok_or_else(|| eyre::eyre!("local doc missing after successful sync"))?;
                let heads = current_doc.with_document_read(|doc| doc.get_heads()).await;
                let deets = if remote_payload.is_none() && heads.as_slice() == local_heads.as_ref()
                {
                    big_sync_core::SyncCompletionDeets::Noop
                } else {
                    big_sync_core::SyncCompletionDeets::ChangedObject
                };
                Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion { obj_id, deets },
                ))
            }
            Err(crate::SyncDocError::Other(inner)) => Err(inner),
            Err(crate::SyncDocError::IoError(inner)) => {
                Err(inner).wrap_err("i/o error syncing doc")
            }
            Err(crate::SyncDocError::TransportError) => {
                eyre::bail!("transport error syncing doc")
            }
            Err(crate::SyncDocError::NotFound) => {
                eyre::bail!("remote doc was not found")
            }
            Err(crate::SyncDocError::Unauthorized) => {
                eyre::bail!("remote doc access denied")
            }
            Err(crate::SyncDocError::PendingKeys) => {
                eyre::bail!("remote doc keys are pending (keyhive sync incomplete)")
            }
        }
    }
}
