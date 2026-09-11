use crate::interlude::*;

#[derive(Clone)]
pub struct BigRepoSyncBackend {
    repo: std::sync::Weak<crate::BigRepo>,
}

impl BigRepoSyncBackend {
    pub async fn boot(repo: std::sync::Weak<crate::BigRepo>) -> Res<Self> {
        Ok(Self { repo })
    }
}

#[async_trait::async_trait]
impl big_sync::SyncBackend for BigRepoSyncBackend {
    /// Part membership is exclusively owned by runtime2 reconciliation workers.
    /// Sync replay acknowledges removals without mutating that projection.
    async fn remove_obj_from_parts(&self, _obj_id: ObjId, _parts: Vec<PartId>) -> Res<()> {
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(%peer_id, %obj_id, remote_payload_present = remote_payload.is_some()),
    )]
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: big_sync_core::ObjId,
        // Part hints are deliberately ignored: big_repo part membership is
        // owned by the runtime2 workers (group-part reconciliation, frontier
        // publishing), never by the sync path.
        _parts: Vec<big_sync_core::PartId>,
        remote_payload: Option<big_sync::ObjPayload>,
    ) -> Res<big_sync::SyncTaskRunOutcome> {
        let repo: Arc<crate::BigRepo> = self
            .repo
            .upgrade()
            .ok_or_else(|| eyre::eyre!("big repo dropped while sync backend was active"))?;
        let doc_id: crate::DocumentId = obj_id;

        let has_local_doc_state = repo.runtime.has_local_doc_state(doc_id).await?;
        tracing::debug!(
            remote_peer_id = %peer_id,
            %doc_id,
            has_local_doc_state,
            remote_payload = remote_payload.is_some(),
            "big repo sync_obj",
        );
        // Equal advertised heads only prove logical convergence. A partially
        // materialized sedimentree can still be missing the blobs needed to
        // reconstruct those heads, so it must run the backend sync.
        let local_heads = repo.doc_payload_heads(doc_id).await?;
        if let Some(remote_payload) = &remote_payload
            && let Some(local_heads) = &local_heads
            && repo.doc_head_state(doc_id).await?.state
                == crate::runtime2::MaterializationState::Materialized
        {
            let remote_heads = super::doc_heads_from_payload(remote_payload);
            if local_heads.as_ref() == remote_heads.as_ref() {
                return Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    },
                ));
            }
        }
        let timeout = repo.sync_policy().backend_doc_sync_timeout;
        let receipt = match tokio::time::timeout(
            timeout,
            repo.runtime.sync_doc_with_peer_receipt(doc_id, peer_id),
        )
        .await
        {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(crate::SyncDocError::Other(inner))) => return Err(inner),
            Ok(Err(crate::SyncDocError::IoError(inner))) => {
                return Err(inner).wrap_err("i/o error syncing doc");
            }
            Ok(Err(crate::SyncDocError::TransportError)) => {
                eyre::bail!("transport error syncing doc");
            }
            Ok(Err(crate::SyncDocError::NotFound)) => {
                eyre::bail!("remote doc was not found");
            }
            Ok(Err(crate::SyncDocError::Unauthorized)) => {
                #[derive(Debug, Clone, Copy, PartialEq, Eq)]
                enum LocalAuthorization {
                    Unknown,
                    Authorized,
                    NotAuthorized,
                }

                let local_key =
                    ed25519_dalek::VerifyingKey::from_bytes(repo.local_peer_id().as_bytes())
                        .map_err(|_| eyre::eyre!("local peer id is not a verifying key"))?;
                let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
                    .map_err(|_| eyre::eyre!("document id is not a verifying key"))?;
                let local = keyhive_core::principal::identifier::Identifier::from(local_key);
                let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
                let kh_document = keyhive_core::principal::document::id::DocumentId::from(document);
                let before = if repo
                    .keyhive()
                    .clone_keyhive()
                    .get_document(kh_document)
                    .await
                    .is_none()
                {
                    LocalAuthorization::Unknown
                } else if repo
                    .keyhive()
                    .agent_access_on(&local, document)
                    .await
                    .is_some()
                {
                    LocalAuthorization::Authorized
                } else {
                    LocalAuthorization::NotAuthorized
                };

                // A remote rejection races Keyhive propagation. Reconcile the
                // membership view before deciding whether this object is truly
                // revoked: an object can be advertised locally while its grant
                // is still only in the remote admission log.
                repo.sync_keyhive_with_peer(peer_id)
                    .await
                    .wrap_err("keyhive reconciliation after remote Unauthorized failed")?;
                let after = if repo
                    .keyhive()
                    .clone_keyhive()
                    .get_document(kh_document)
                    .await
                    .is_none()
                {
                    LocalAuthorization::Unknown
                } else if repo
                    .keyhive()
                    .agent_access_on(&local, document)
                    .await
                    .is_some()
                {
                    LocalAuthorization::Authorized
                } else {
                    LocalAuthorization::NotAuthorized
                };

                tracing::debug!(
                    %peer_id,
                    %doc_id,
                    ?before,
                    ?after,
                    "classified remote Unauthorized after Keyhive reconciliation",
                );

                if after == LocalAuthorization::Authorized {
                    // The serving peer rejected a document that this peer is
                    // still authorized to fetch. Do not acknowledge the cursor;
                    // this is an inconsistency that needs a later retry and
                    // must remain visible in diagnostics.
                    eyre::bail!(
                        "remote peer {peer_id} rejected document {doc_id} while local Keyhive still grants access"
                    );
                }
                if after == LocalAuthorization::Unknown {
                    // The local Keyhive still does not know the document after
                    // reconciling with the serving peer. This is admission lag or
                    // a broken admission pipeline, not proof of revocation.
                    return Ok(big_sync::SyncTaskRunOutcome::Stale);
                }

                // The local round confirmed that a previously authorized
                // document is no longer authorized. Re-grants publish a fresh
                // frontier object through the existing Keyhive admission path,
                // so this obsolete cursor may be settled without turning
                // revocation into a retry storm.
                return Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    },
                ));
            }
            Ok(Err(crate::SyncDocError::Policy(error))) => {
                eyre::bail!(
                    "doc sync with peer {peer_id} was rejected by the local policy: {error}"
                );
            }
            Err(_) => {
                eyre::bail!("timed out syncing doc");
            }
        };
        debug!(peer_id = %peer_id, obj_id = %obj_id, ?receipt.outcome, "big sync document receipt");
        let heads = repo
            .doc_payload_heads(doc_id)
            .await?
            .ok_or_eyre("local doc payload missing after successful sync")?;
        debug!(
            head_count = heads.len(),
            "loaded persisted heads after document sync"
        );
        let deets = if local_heads
            .as_ref()
            .map(|prev| prev.as_ref() == heads.as_ref())
            .unwrap_or_default()
        {
            big_sync_core::SyncCompletionDeets::Noop
        } else {
            big_sync_core::SyncCompletionDeets::ChangedObject
        };
        debug!(?deets, "document sync backend completed");
        Ok(big_sync::SyncTaskRunOutcome::Completion(
            big_sync_core::SyncTaskCompletion { obj_id, deets },
        ))
    }
}
