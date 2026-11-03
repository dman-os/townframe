use super::*;
use big_sync::backend::contract::SyncBackendHarness;
use big_sync::backend::contract::SyncBackendOutcome;
use big_sync::backend::contract::SyncBackendScenario;
use big_sync::backend::contract::assert_sync_backend_scenarios;

struct BlobSyncBackendContractHarness {
    backend: Arc<dyn SyncBackend>,
    store: Arc<dyn HostPartStore>,
}

#[async_trait]
impl SyncBackendHarness for BlobSyncBackendContractHarness {
    fn backend(&self) -> &dyn SyncBackend {
        self.backend.as_ref()
    }

    fn store(&self) -> &dyn HostPartStore {
        self.store.as_ref()
    }
}

fn blob_sync_backend_cases(
    noop_blob_id: BlobId,
    noop_missing_remote_blob_id: BlobId,
    changed_blob_id: BlobId,
    changed_empty_hints_blob_id: BlobId,
    changed_multi_hints_blob_id: BlobId,
    added_blob_id: BlobId,
) -> Vec<SyncBackendScenario> {
    let parts = test_parts();
    let extra_part = extra_part();
    let noop_payload = serde_json::json!({"kind": "noop"});
    let old_payload = serde_json::json!({"kind": "old"});
    let new_payload = serde_json::json!({"kind": "new"});
    vec![
        SyncBackendScenario::noop(
            "noop_when_membership_and_payload_match",
            PeerKey::new([2; 32]),
            ObjKey::from(noop_blob_id),
            noop_payload.clone(),
            parts.clone(),
        ),
        SyncBackendScenario {
            name: "noop_when_remote_payload_is_missing_and_blob_exists",
            peer_id: PeerKey::new([2; 32]),
            obj_id: ObjKey::from(noop_missing_remote_blob_id),
            initial_payload: Some(noop_payload.clone()),
            initial_parts: parts.clone(),
            remote_payload: None,
            expected_outcome: SyncBackendOutcome::Completion(
                big_sync_core::SyncCompletionDeets::Noop,
            ),
            expected_parts: parts.clone(),
        },
        SyncBackendScenario::changed_object(
            "changed_object_applies_remote_payload",
            PeerKey::new([2; 32]),
            ObjKey::from(changed_blob_id),
            old_payload.clone(),
            new_payload.clone(),
            parts.clone(),
        ),
        SyncBackendScenario::changed_object(
            "changed_object_with_empty_part_hints",
            PeerKey::new([2; 32]),
            ObjKey::from(changed_empty_hints_blob_id),
            old_payload.clone(),
            new_payload.clone(),
            vec![],
        ),
        SyncBackendScenario::changed_object(
            "changed_object_with_multiple_part_hints",
            PeerKey::new([2; 32]),
            ObjKey::from(changed_multi_hints_blob_id),
            old_payload.clone(),
            new_payload.clone(),
            vec![parts[0].clone(), extra_part],
        ),
        SyncBackendScenario::added_member(
            "added_member_materializes_missing_blob",
            PeerKey::new([2; 32]),
            ObjKey::from(added_blob_id),
            new_payload.clone(),
            parts.clone(),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_sync_backend_contract() -> Res<()> {
    let (backend, part_store, blobs_repo, _temp_root) = build_blob_backend().await?;
    let noop_blob_id = blobs_repo.put(b"blob-sync-contract-noop").await?;
    let noop_missing_remote_blob_id = blobs_repo
        .put(b"blob-sync-contract-noop-missing-remote")
        .await?;
    let changed_blob_id = blobs_repo.put(b"blob-sync-contract-changed").await?;
    let changed_empty_hints_blob_id = blobs_repo
        .put(b"blob-sync-contract-changed-empty-hints")
        .await?;
    let changed_multi_hints_blob_id = blobs_repo
        .put(b"blob-sync-contract-changed-multi-hints")
        .await?;
    let added_blob_id = blobs_repo.put(b"blob-sync-contract-added").await?;
    let harness = BlobSyncBackendContractHarness {
        backend,
        store: part_store,
    };
    assert_sync_backend_scenarios(
        &harness,
        &blob_sync_backend_cases(
            noop_blob_id,
            noop_missing_remote_blob_id,
            changed_blob_id,
            changed_empty_hints_blob_id,
            changed_multi_hints_blob_id,
            added_blob_id,
        ),
    )
    .await
}
