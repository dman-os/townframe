use super::*;
use big_blobs::BlobService;
use big_blobs_core::BlobsError;
use big_blobs_core::PutBlob;
use big_blobs_core::ReadBlob;
use big_blobs_core::StatBlob;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::RetractCapability;
use service_utils_rs::ClientError;

async fn blob_cap(node: &BigNode, object: String, dependencies: Vec<CapId>) -> CapId {
    node.authority
        .client
        .call(
            RequestContext::root(),
            MintCapability {
                issuer: node.blobs_namespace.capability,
                parent: None,
                dependencies,
                object,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

#[tokio::test]
async fn blob_capabilities_bound_ranges_and_revocation() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(&secrets, &runtime, sql, StorageConfig::Memory))
        .await
        .unwrap();
    let put = blob_cap(&node, BlobService::put_object().into(), Vec::new()).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"bounded blob bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let read = blob_cap(&node, BlobService::read_object(&info.blob_id), vec![put]).await;
    let denied = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            StatBlob {
                capability: put,
                blob_id: info.blob_id,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, ClientError::Operation(BlobsError::Denied)));
    let range = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            ReadBlob {
                capability: read,
                blob_id: info.blob_id,
                offset: 8,
                length: 4,
            },
        )
        .await
        .unwrap();
    assert_eq!(range.bytes, b"blob");
    assert_eq!(range.length_octets, 18);
    let invalid = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            ReadBlob {
                capability: read,
                blob_id: info.blob_id,
                offset: 0,
                length: big_blobs_core::MAX_READ_BYTES + 1,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid,
        ClientError::Operation(BlobsError::InvalidInput { .. })
    ));
    node.authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: node.blobs_namespace.capability,
                capability: put,
            },
        )
        .await
        .unwrap();
    let denied = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            StatBlob {
                capability: read,
                blob_id: info.blob_id,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, ClientError::Operation(BlobsError::Denied)));
    shutdown(node, stop).await;
}

#[tokio::test]
async fn filesystem_store_has_one_owner_and_reopens_with_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.database(
        directory.path().join("cabinet.sqlite"),
        &[],
        "_sqlite_rs_migrations",
    );
    let mut cfg = config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    let put = blob_cap(&node, BlobService::put_object().into(), Vec::new()).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"persistent bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let duplicate = big_blobs::boot(big_blobs::Config {
        storage: big_blobs::Storage::Filesystem(directory.path().join("blobs")),
        sql: sql.clone(),
        authority: node.authority.client.clone(),
        namespace: node.blobs_namespace,
    })
    .await;
    assert!(
        duplicate
            .err()
            .unwrap()
            .to_string()
            .contains("already has an owner")
    );
    shutdown(node, stop).await;
    let mut cfg = config(
        &secrets,
        &runtime,
        sql,
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    assert_eq!(
        node.blobs.service.stat(&info.blob_id).await.unwrap(),
        Some(info)
    );
    shutdown(node, stop).await;
}
