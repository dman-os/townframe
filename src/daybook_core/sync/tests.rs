use super::*;
mod ladder;
mod stress;

use crate::blobs::{BlobId, BlobsRepo};
use crate::drawer::DrawerRepo;
use crate::index::DocBlobsIndexRepo;
use crate::local_state::SqliteLocalStateRepo;
use crate::plugs::PlugsRepo;
use crate::progress::ProgressRepo;
use crate::repo::{RepoCtx, RepoOpenOptions};
use crate::repos::{Repo, SubscribeOpts};
use daybook_types::doc::{
    AddDocArgs, BlobPin, DocId, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};

struct SyncTestNode {
    ctx: Arc<RepoCtx>,
    rt: Arc<crate::rt::Rt>,
    drawer: Arc<DrawerRepo>,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Arc<ProgressRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sync_repo: Arc<IrohSyncRepo>,
    sync_stop: IrohSyncRepoStopToken,
    rt_stop: crate::rt::RtStopToken,
    drawer_stop: crate::repos::RepoStopToken,
    plugs_stop: crate::repos::RepoStopToken,
    config_stop: crate::repos::RepoStopToken,
    dispatch_stop: crate::repos::RepoStopToken,
    init_stop: crate::repos::RepoStopToken,
    progress_stop: crate::repos::RepoStopToken,
    sqlite_local_state_stop: crate::repos::RepoStopToken,
}

impl SyncTestNode {
    async fn stop(self) -> Res<()> {
        let SyncTestNode {
            ctx,
            rt: _rt,
            blobs_repo: _blobs_repo,
            drawer: _drawer,
            doc_blobs_index_repo: _doc_blobs_index_repo,
            progress_repo: _progress_repo,
            plugs_repo: _plugs_repo,
            sync_repo,
            sync_stop,
            rt_stop,
            progress_stop,
            drawer_stop,
            plugs_stop,
            config_stop,
            dispatch_stop,
            init_stop,
            sqlite_local_state_stop,
        } = self;
        drop(sync_repo);
        sync_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(60)),
            sync_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting sync stop"))??;
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            rt_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting rt stop"))??;
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            progress_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting progress stop"))??;
        dispatch_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            dispatch_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting dispatch stop"))??;
        init_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            init_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting init stop"))??;
        sqlite_local_state_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            sqlite_local_state_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting sqlite local state stop"))??;
        config_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            config_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting config stop"))??;
        drawer_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            drawer_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting drawer stop"))??;
        plugs_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            plugs_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting plugs stop"))??;
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            ctx.shutdown(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting ctx shutdown"))??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_between_copied_repos() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let mut created_doc_ids = Vec::new();
    for _ in 0..3 {
        let new_doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        created_doc_ids.push(new_doc_id);
    }

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20)).await?;
    for doc_id in &created_doc_ids {
        wait_for_doc_presence_with_activity(&node_b, doc_id, Duration::from_secs(60)).await?;
    }

    let ids_a = list_doc_ids(&node_a.drawer).await?;
    let ids_b = list_doc_ids(&node_b.drawer).await?;
    assert_eq!(
        ids_a, ids_b,
        "replica doc sets are not equal after full sync"
    );

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_live_sync_bidirectional_after_clone() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20)).await?;

    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;

    let doc_on_b = node_b
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_b.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_a, &doc_on_b, Duration::from_secs(60)).await?;

    wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

    let ids_a = list_doc_ids(&node_a.drawer).await?;
    let ids_b = list_doc_ids(&node_b.drawer).await?;
    assert_eq!(ids_a, ids_b, "live sync did not converge to equal doc sets");

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_live_sync_propagates_repeated_doc_updates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let seed_node = open_sync_node(&repo_a_path).await?;
    let ticket = seed_node.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket, &repo_b_path).await?;
    seed_node.stop().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20)).await?;

    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;

    for idx in 0..6 {
        let branch = daybook_types::doc::BranchPathBuf::from("main");
        let Some((_doc, heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await?
        else {
            eyre::bail!("missing source doc after initial sync: {doc_id}");
        };
        let mut facets_set = std::collections::HashMap::new();
        facets_set.insert(
            FacetKey::from(WellKnownFacetTag::TitleGeneric),
            FacetRaw::from(WellKnownFacet::TitleGeneric(format!("repeat-{idx}"))),
        );
        node_a
            .drawer
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: doc_id.clone(),
                    facets_set,
                    facets_remove: vec![],
                    user_path: Some(daybook_types::doc::UserPathBuf::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                },
                &branch,
                Some(heads),
            )
            .await?;
    }

    wait_for_doc_head_parity(
        &node_a,
        &node_b,
        &doc_id,
        &daybook_types::doc::BranchPathBuf::from("main"),
        Duration::from_secs(30),
    )
    .await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cloned_repo_registers_core_docs_partition_on_open() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let created_doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_a, &created_doc_id, Duration::from_secs(30)).await?;
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;
    node_a.stop().await?;

    let node_b = open_sync_node(&repo_b_path).await?;
    let core_partition_id = node_b.sync_repo.authority.core_docs_part_id();
    let partitions = node_b
        .ctx
        .part_store
        .summarize_parts(HashSet::from([core_partition_id]))
        .await??;
    let core_partition = partitions.get(&core_partition_id);
    assert!(
        core_partition.is_some(),
        "cloned repo should register core docs partition on open: {partitions:?}"
    );
    let core_partition = core_partition.expect("checked above");
    assert!(
        core_partition.member_count >= 2,
        "core docs partition should include drawer/app docs after sync boot: {core_partition:?}"
    );

    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_ticket_in_tests_omits_relay_addresses() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_path = temp_root.path().join("repo-a");
    tokio::fs::create_dir_all(&repo_path).await?;

    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node = open_sync_node(&repo_path).await?;
    let ticket = node.sync_repo.get_clone_ticket_url().await?;
    let info = crate::sync::resolve_clone_info_from_url(&ticket).await?;
    assert!(!info.repo_name.is_empty());
    node.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_clone_sync_batch_100_docs_with_blobs() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20)).await?;

    let mut args_batch = Vec::new();
    for idx in 0..100usize {
        let payload = format!("blob-payload-{idx:03}").into_bytes();
        let hash = node_a.blobs_repo.put(&payload).await?;
        let hash = crate::blobs::blob_id_to_digest_str(hash);
        args_batch.push(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: hash.clone(),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                })),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        });
    }
    let created = node_a.drawer.batch_add(args_batch).await?;
    assert_eq!(created.len(), 100);

    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(30)).await?;

    let ids_a = list_doc_ids(&node_a.drawer).await?;
    let ids_b = list_doc_ids(&node_b.drawer).await?;
    assert_eq!(
        ids_a, ids_b,
        "doc sets are not equal after 100-doc clone sync"
    );

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_blob_sync_validates_bytes() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(60)).await?;

    let mut blob_payloads = Vec::new();
    let mut args_batch = Vec::new();
    for idx in 0..8usize {
        let payload = format!("blob-bytes-validation-{idx:03}").into_bytes();
        let hash = node_a.blobs_repo.put(&payload).await?;
        blob_payloads.push((hash, payload));
        args_batch.push(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: blob_payloads.last().expect("just pushed").1.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(hash),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                })),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        });
    }
    node_a.drawer.batch_add(args_batch).await?;

    for (hash, expected) in &blob_payloads {
        let got = wait_for_blob_bytes(&node_b.blobs_repo, *hash, Duration::from_secs(60)).await?;
        assert_eq!(
            &got, expected,
            "blob content mismatch after sync for hash={hash}"
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_blob_pin_sync_replicates_and_fetches_blobs() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(60)).await?;

    let payload_1 = b"blob-pin-sync-payload-1".to_vec();
    let payload_2 = b"blob-pin-sync-payload-2".to_vec();
    let blob_id_1 = node_a.blobs_repo.put(&payload_1).await?;
    let blob_id_2 = node_a.blobs_repo.put(&payload_2).await?;
    let hash_1 = blob_id_1.to_string();
    let hash_2 = blob_id_2.to_string();

    let key_pin_1 = FacetKey {
        tag: WellKnownFacetTag::BlobPin.into(),
        id: hash_1.clone(),
    };
    let key_pin_2 = FacetKey {
        tag: WellKnownFacetTag::BlobPin.into(),
        id: hash_2.clone(),
    };

    let doc_id = node_a
        .drawer
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [
                (
                    key_pin_1.clone(),
                    FacetRaw::from(WellKnownFacet::BlobPin(BlobPin {
                        length_octets: payload_1.len() as u64,
                    })),
                ),
                (
                    key_pin_2.clone(),
                    FacetRaw::from(WellKnownFacet::BlobPin(BlobPin {
                        length_octets: payload_2.len() as u64,
                    })),
                ),
            ]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    wait_for_drawer_doc_parity(
        &node_a,
        &node_b,
        &doc_id,
        daybook_types::doc::BranchPath::new("main"),
        Duration::from_secs(60),
    )
    .await?;

    // 1. Verify doc with BlobPin facets exists in node_b.drawer
    let doc_b = node_b
        .drawer
        .get_doc_with_facets_at_branch(&doc_id, daybook_types::doc::BranchPath::new("main"), None)
        .await?
        .expect("doc should exist on node_b");
    assert!(doc_b.facets.contains_key(&key_pin_1));
    assert!(doc_b.facets.contains_key(&key_pin_2));

    // 2. Verify node_b's DocBlobsIndexRepo has indexed the hashes in SQLite doc_blob_refs
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let hashes = node_b
            .doc_blobs_index_repo
            .list_hashes_for_doc(&doc_id)
            .await?;
        if hashes.contains(&hash_1) && hashes.contains(&hash_2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let hashes_b = node_b
        .doc_blobs_index_repo
        .list_hashes_for_doc(&doc_id)
        .await?;
    assert!(hashes_b.contains(&hash_1));
    assert!(hashes_b.contains(&hash_2));

    let blob_refs_b = node_b
        .doc_blobs_index_repo
        .list_blob_refs_for_doc(&doc_id)
        .await?;
    assert_eq!(blob_refs_b.len(), 2);
    assert!(
        blob_refs_b
            .iter()
            .any(|r| r.blob_hash == hash_1 && r.length_octets == payload_1.len() as u64)
    );
    assert!(
        blob_refs_b
            .iter()
            .any(|r| r.blob_hash == hash_2 && r.length_octets == payload_2.len() as u64)
    );

    // 3. Verify node_b.blobs_repo.get_bytes(blob_id) successfully fetches the blob bytes from node_a
    let bytes_1 =
        wait_for_blob_bytes(&node_b.blobs_repo, blob_id_1, Duration::from_secs(60)).await?;
    assert_eq!(bytes_1, payload_1);
    let bytes_1_direct = node_b.blobs_repo.get_bytes(blob_id_1).await?;
    assert_eq!(bytes_1_direct, payload_1);

    let bytes_2 =
        wait_for_blob_bytes(&node_b.blobs_repo, blob_id_2, Duration::from_secs(60)).await?;
    assert_eq!(bytes_2, payload_2);
    let bytes_2_direct = node_b.blobs_repo.get_bytes(blob_id_2).await?;
    assert_eq!(bytes_2_direct, payload_2);

    // 4. Remove a BlobPin facet on node_a, verify propagation to node_b
    node_a
        .drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
                facets_set: default(),
                facets_remove: vec![key_pin_2.clone()],
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            },
            daybook_types::doc::BranchPath::new("main"),
            None,
        )
        .await?;

    wait_for_doc_head_parity(
        &node_a,
        &node_b,
        &doc_id,
        &daybook_types::doc::BranchPathBuf::from("main"),
        Duration::from_secs(60),
    )
    .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let hashes = node_b
            .doc_blobs_index_repo
            .list_hashes_for_doc(&doc_id)
            .await?;
        if hashes.len() == 1 && hashes.contains(&hash_1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let hashes_after = node_b
        .doc_blobs_index_repo
        .list_hashes_for_doc(&doc_id)
        .await?;
    assert_eq!(hashes_after, vec![hash_1.clone()]);

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_after_bootstrap_clone_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let mut created_doc_ids = Vec::new();
    for _ in 0..8 {
        let new_doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        created_doc_ids.push(new_doc_id);
    }

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;

    let node_b = open_sync_node(&repo_b_path).await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(30)).await?;

    for doc_id in &created_doc_ids {
        wait_for_doc_presence_with_activity(&node_b, doc_id, Duration::from_secs(60)).await?;
    }

    wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(30)).await?;

    let ids_a = list_doc_ids(&node_a.drawer).await?;
    let ids_b = list_doc_ids(&node_b.drawer).await?;
    assert_eq!(
        ids_a, ids_b,
        "sync after bootstrap clone did not converge to equal doc sets"
    );

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

async fn init_and_copy_repo_pair(
    repo_a_path: &std::path::Path,
    repo_b_path: &std::path::Path,
) -> Res<()> {
    tokio::fs::create_dir_all(repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    let source_repo_id = rtx.repo_id.clone();
    let source_app_doc_id = rtx.doc_app.document_id();
    let source_drawer_doc_id = rtx.doc_drawer.document_id();
    rtx.shutdown().await?;

    let seed_node = open_sync_node(repo_a_path).await?;
    let result = async {
        let ticket = seed_node.sync_repo.get_clone_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&ticket, repo_b_path).await?;

        let ctx = RepoCtx::open(
            repo_b_path,
            RepoOpenOptions::default(),
            "test-device".into(),
        )
        .await?;
        if ctx.repo_id != source_repo_id {
            eyre::bail!(
                "init repo_id mismatch after clone (source={}, cloned={})",
                source_repo_id,
                ctx.repo_id
            );
        }
        if ctx.doc_app.document_id() != source_app_doc_id {
            eyre::bail!(
                "init app doc mismatch after clone (source={}, cloned={})",
                source_app_doc_id,
                ctx.doc_app.document_id()
            );
        }
        if ctx.doc_drawer.document_id() != source_drawer_doc_id {
            eyre::bail!(
                "init drawer doc mismatch after clone (source={}, cloned={})",
                source_drawer_doc_id,
                ctx.doc_drawer.document_id()
            );
        }
        ctx.shutdown().await
    }
    .await;
    seed_node.stop().await?;
    result
}

async fn bootstrap_clone_repo_from_url_for_tests(
    source_url: &str,
    destination: &std::path::Path,
) -> Res<()> {
    crate::sync::clone_repo_init_from_url(
        source_url,
        destination,
        crate::sync::CloneRepoInitOptions {
            timeout: Duration::from_secs(30),
            repo_options: RepoOpenOptions {
                sync_max_task_backoff: Some(Duration::from_millis(500)),
            },
        },
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct SyncNodeOptions {
    #[expect(dead_code)]
    enable_switch: bool,
}

impl Default for SyncNodeOptions {
    fn default() -> Self {
        Self {
            enable_switch: true,
        }
    }
}

async fn open_sync_node(repo_root: &std::path::Path) -> Res<SyncTestNode> {
    open_sync_node_with_options(repo_root, SyncNodeOptions::default()).await
}

async fn open_sync_node_with_options(
    repo_root: &std::path::Path,
    options: SyncNodeOptions,
) -> Res<SyncTestNode> {
    info!(repo_root = %repo_root.display(), ?options, "opening sync test node");
    let rtx = RepoCtx::open(
        repo_root,
        RepoOpenOptions {
            sync_max_task_backoff: Some(Duration::from_millis(500)),
        },
        "test-device".into(),
    )
    .await?;
    let blobs_repo =
        BlobsRepo::new(rtx.layout.blobs_root.clone(), rtx.local_user_path.clone()).await?;
    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        Arc::clone(&rtx.big_repo),
        Arc::clone(&blobs_repo),
        rtx.doc_app.document_id(),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
    )
    .await?;
    let (drawer_repo, drawer_stop) = DrawerRepo::load(
        Arc::clone(&rtx.big_repo),
        Arc::clone(&rtx.part_store),
        rtx.doc_drawer.document_id(),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        rtx.sql.clone(),
        rtx.layout.repo_root.join("local_state"),
        Arc::new(surelock::mutex::Mutex::new(
            utils_rs::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(surelock::mutex::Mutex::new(
            utils_rs::lru::KeyedLruPool::new(1000),
        )),
        Some(Arc::clone(&plugs_repo)),
    )
    .await?;
    let (config_repo, config_stop) = crate::config::ConfigRepo::load(
        Arc::clone(&rtx.big_repo),
        rtx.doc_app.document_id(),
        Arc::clone(&plugs_repo),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        rtx.sql.clone(),
    )
    .await?;
    let (dispatch_repo, dispatch_stop) = crate::rt::dispatch::DispatchRepo::load(
        Arc::clone(&rtx.big_repo),
        rtx.doc_app.document_id(),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        rtx.sql.clone(),
    )
    .await?;
    let (progress_repo, progress_stop) = ProgressRepo::boot(rtx.sql.clone()).await?;
    let (init_repo, init_stop) = crate::rt::init::InitRepo::load(
        Arc::clone(&rtx.big_repo),
        rtx.doc_app.document_id(),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        rtx.sql.clone(),
        Arc::clone(&progress_repo),
        None,
    )
    .await?;
    let (sqlite_local_state_repo, sqlite_local_state_stop) =
        SqliteLocalStateRepo::boot(rtx.layout.repo_root.join("local_state")).await?;

    let (rt, rt_stop) = crate::rt::Rt::boot(
        crate::rt::RtConfig {
            device_id: "test-device".to_string(),
            startup_progress_task_id: None,
        },
        Arc::clone(&rtx),
        Arc::clone(&drawer_repo),
        Arc::clone(&plugs_repo),
        Arc::clone(&dispatch_repo),
        Arc::clone(&progress_repo),
        Arc::clone(&blobs_repo),
        Arc::clone(&config_repo),
        Arc::clone(&init_repo),
        Arc::clone(&sqlite_local_state_repo),
    )
    .await?;

    let (sync_repo, sync_stop) = IrohSyncRepo::boot(
        Arc::clone(&rtx),
        Arc::clone(&config_repo),
        Arc::clone(&blobs_repo),
        Arc::clone(&rt.doc_blobs_index_repo),
        Some(Arc::clone(&progress_repo)),
    )
    .await?;

    Ok(SyncTestNode {
        ctx: rtx,
        drawer: Arc::clone(&rt.drawer),
        blobs_repo: Arc::clone(&rt.blobs_repo),
        doc_blobs_index_repo: Arc::clone(&rt.doc_blobs_index_repo),
        progress_repo: Arc::clone(&rt.progress_repo),
        plugs_repo: Arc::clone(&rt.plugs_repo),
        rt,
        rt_stop,
        sync_repo,
        sync_stop,
        drawer_stop,
        plugs_stop,
        config_stop,
        dispatch_stop,
        init_stop,
        progress_stop,
        sqlite_local_state_stop,
    })
}

async fn list_doc_ids(drawer: &DrawerRepo) -> Res<HashSet<String>> {
    let (_, ids) = drawer.list_just_ids().await?;
    Ok(ids.into_iter().collect())
}

#[tracing::instrument(skip_all)]
async fn wait_for_doc_presence_with_activity(
    node: &SyncTestNode,
    doc_id: &DocId,
    absolute_timeout: Duration,
) -> Res<()> {
    let last_activity = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
    let last_activity_for_wait = Arc::clone(&last_activity);
    let drawer_listener = node.drawer.subscribe(SubscribeOpts::new(1024));
    let sync_listener = node.sync_repo.subscribe(SubscribeOpts::new(2048));
    let progress_listener = node.progress_repo.subscribe(SubscribeOpts::new(4096));
    let mut loop_count = 0u64;
    tokio::time::timeout(absolute_timeout, async {
        loop {
            loop_count += 1;
            debug!(%doc_id, loop_count, "presence loop: before drawer read");
            let found = node
                .drawer
                .get_doc_with_facets_at_branch(doc_id, daybook_types::doc::BranchPath::new("main"), None)
                .await?
                .is_some();
            debug!(%doc_id, loop_count, found, "presence loop: after drawer read");
            if found {
                break;
            }
            tokio::select! {
                val = drawer_listener.recv_lossy_async() => {
                    let evt = val.map_err(|_| eyre::eyre!("drawer listener closed while waiting for doc presence"))?;
                    match evt.as_ref() {
                        crate::drawer::DrawerEvent::DocAdded { id, .. }
                        | crate::drawer::DrawerEvent::DocUpdated { id, .. }
                        | crate::drawer::DrawerEvent::DocDeleted { id, .. } if id == doc_id => {
                            *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                        }
                        crate::drawer::DrawerEvent::DocAdded { .. }
                        | crate::drawer::DrawerEvent::DocUpdated { .. }
                        | crate::drawer::DrawerEvent::DocDeleted { .. } => {
                            *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                        }
                    }
                }
                val = sync_listener.recv_async() => {
                    match val {
                        Ok(_) => {
                            *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                        }
                        Err(crate::repos::RecvError::Closed) => eyre::bail!("sync listener closed while waiting for doc presence"),
                        Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                            eyre::bail!("sync listener dropped events while waiting for doc presence: dropped_count={dropped_count}");
                        }
                    }
                }
                val = progress_listener.recv_async() => {
                    match val {
                        Ok(_) => {
                            *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                        }
                        Err(crate::repos::RecvError::Closed) => eyre::bail!("progress listener closed while waiting for doc presence"),
                        Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                            eyre::bail!("progress listener dropped events while waiting for doc presence: dropped_count={dropped_count}");
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(150)) => {}
            }
        }
        eyre::Ok(())
    })
    .await
    .map_err(|_| {
        let since_last_activity = std::time::Instant::now()
            .saturating_duration_since(*last_activity.lock().expect(ERROR_MUTEX));
        eyre::eyre!(
            "timed out waiting for document presence: doc_id={doc_id} absolute_timeout={:?} (last_activity_ago={:?})",
            absolute_timeout,
            since_last_activity,
        )
    })??;
    Ok(())
}

async fn wait_for_sync_convergence(
    source: &SyncTestNode,
    target: &SyncTestNode,
    endpoint_id: EndpointId,
    timeout: Duration,
) -> Res<()> {
    let required_partitions = source
        .sync_repo
        .peer_partition_ids("", true)
        .into_keys()
        .collect::<Vec<_>>();
    let peer_id = PeerId::new(*endpoint_id.as_bytes());
    // Keyhive convergence is driven by the production notification
    // subscription. The test waits for the observable BigSync and drawer
    // results instead of reaching through the daybook API into BigRepo to
    // force an internal sync round.
    info!(
        source = %source.sync_repo.router.endpoint().id(),
        target = %target.sync_repo.router.endpoint().id(),
        peer_id = %peer_id,
        partition_count = required_partitions.len(),
        "waiting for notification-driven sync convergence"
    );
    tokio::try_join!(
        target.sync_repo.wait_for_full_sync(
            std::slice::from_ref(&peer_id),
            &required_partitions,
            timeout
        ),
        wait_for_doc_set_parity(&source.drawer, &target.drawer, timeout),
    )?;
    info!(
        source = %source.sync_repo.router.endpoint().id(),
        target = %target.sync_repo.router.endpoint().id(),
        peer_id = %peer_id,
        "notification-driven sync convergence reached"
    );
    Ok(())
}
#[tokio::test(flavor = "multi_thread")]
async fn wait_for_full_sync_succeeds_after_event_was_already_emitted() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr_ba = node_b.sync_repo.connect_url(&ticket_a).await?;

    wait_for_sync_convergence(
        &node_a,
        &node_b,
        endpoint_addr_ba.id,
        Duration::from_secs(20),
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let required_partitions = node_b
        .sync_repo
        .peer_partition_ids("", true)
        .into_keys()
        .collect::<Vec<_>>();
    let peer_id = PeerId::new(*endpoint_addr_ba.id.as_bytes());
    node_b
        .sync_repo
        .wait_for_full_sync(
            std::slice::from_ref(&peer_id),
            &required_partitions,
            Duration::from_secs(5),
        )
        .await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

async fn wait_for_doc_set_parity(
    left: &DrawerRepo,
    right: &DrawerRepo,
    timeout: Duration,
) -> Res<()> {
    let mut last_left = HashSet::<String>::new();
    let mut last_right = HashSet::<String>::new();
    let timeout_outcome = tokio::time::timeout(timeout, async {
        let mut last_heartbeat = std::time::Instant::now();
        loop {
            let lset = list_doc_ids(left).await?;
            let rset = list_doc_ids(right).await?;
            last_left = lset.clone();
            last_right = rset.clone();
            if lset == rset {
                debug!(count = lset.len(), "drawer doc-set parity reached");
                break;
            }
            let now = std::time::Instant::now();
            if now.duration_since(last_heartbeat) >= Duration::from_secs(2) {
                last_heartbeat = now;
                let missing_on_right = lset.difference(&rset).take(8).cloned().collect::<Vec<_>>();
                let missing_on_left = rset.difference(&lset).take(8).cloned().collect::<Vec<_>>();
                debug!(
                    left_count = lset.len(),
                    right_count = rset.len(),
                    missing_on_right = ?missing_on_right,
                    missing_on_left = ?missing_on_left,
                    "waiting for drawer doc-set parity"
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        eyre::Ok(())
    })
    .await;
    match timeout_outcome {
        Ok(out) => out?,
        Err(_) => {
            let missing_on_right = last_left
                .difference(&last_right)
                .take(12)
                .cloned()
                .collect::<Vec<_>>();
            let missing_on_left = last_right
                .difference(&last_left)
                .take(12)
                .cloned()
                .collect::<Vec<_>>();
            eyre::bail!(
                "timed out waiting for drawer doc-set parity: left_count={} right_count={} missing_on_right={missing_on_right:?} missing_on_left={missing_on_left:?}",
                last_left.len(),
                last_right.len()
            );
        }
    }
    Ok(())
}

async fn wait_for_drawer_doc_parity(
    left: &SyncTestNode,
    right: &SyncTestNode,
    doc_id: &DocId,
    branch: &daybook_types::doc::BranchPath,
    timeout: Duration,
) -> Res<()> {
    wait_for_doc_presence_with_activity(right, doc_id, timeout).await?;
    wait_for_doc_head_parity(left, right, doc_id, branch, timeout).await
}

async fn wait_for_doc_head_parity(
    left: &SyncTestNode,
    right: &SyncTestNode,
    doc_id: &String,
    branch: &daybook_types::doc::BranchPath,
    timeout: Duration,
) -> Res<()> {
    let mut last_left = None::<Vec<String>>;
    let mut last_right = None::<Vec<String>>;
    let mut last_left_facet_keys = None::<Vec<String>>;
    let mut last_right_facet_keys = None::<Vec<String>>;
    let mut last_left_facet_values = None::<String>;
    let mut last_right_facet_values = None::<String>;
    let mut last_runtime = None::<String>;
    let mut last_sync_diagnostics = None::<String>;
    tokio::time::timeout(timeout, async {
        let mut last_heartbeat = std::time::Instant::now();
        loop {
            let (_left_facets, left_facet_keys, left_facet_values, left_heads) = left
                .drawer
                .get_with_heads(doc_id, branch, None)
                .await?
                .map(|(doc, heads)| {
                    let mut keys = doc.facets.keys().map(ToString::to_string).collect::<Vec<_>>();
                    keys.sort_unstable();
                    let debug_val = format!("{doc:?}");
                    (doc, keys, debug_val, heads)
                })
                .ok_or_else(|| eyre::eyre!("left missing doc heads for {doc_id}"))?;
            let (_right_facets, right_facet_keys, right_facet_values, right_heads) = right
                .drawer
                .get_with_heads(doc_id, branch, None)
                .await?
                .map(|(doc, heads)| {
                    let mut keys = doc.facets.keys().map(ToString::to_string).collect::<Vec<_>>();
                    keys.sort_unstable();
                    let debug_val = format!("{doc:?}");
                    (doc, keys, debug_val, heads)
                })
                .ok_or_else(|| eyre::eyre!("right missing doc heads for {doc_id}"))?;
            last_left_facet_keys = Some(left_facet_keys.clone());
            last_right_facet_keys = Some(right_facet_keys.clone());
            last_left_facet_values = Some(left_facet_values.clone());
            last_right_facet_values = Some(right_facet_values.clone());
            let mut left_heads = left_heads.iter().map(ToString::to_string).collect::<Vec<_>>();
            left_heads.sort_unstable();
            let mut right_heads = right_heads.iter().map(ToString::to_string).collect::<Vec<_>>();
            right_heads.sort_unstable();
            last_left = Some(left_heads);
            last_right = Some(right_heads);
            if last_left == last_right && left_facet_keys == right_facet_keys && left_facet_values == right_facet_values {
                break eyre::Ok(());
            }
            let now = std::time::Instant::now();
            if now.duration_since(last_heartbeat) >= Duration::from_secs(2) {
                last_heartbeat = now;
                let runtime_doc_id = doc_id.parse::<big_repo::DocumentId>().ok();
                let left_state = match runtime_doc_id {
                    Some(id) => left.ctx.big_repo.doc_head_state(id).await.ok(),
                    None => None,
                };
                let right_state = match runtime_doc_id {
                    Some(id) => right.ctx.big_repo.doc_head_state(id).await.ok(),
                    None => None,
                };
                let left_diagnostics = match runtime_doc_id {
                    Some(id) => left.ctx.big_repo.document_sync_diagnostics(id).await.ok(),
                    None => None,
                };
                let right_diagnostics = match runtime_doc_id {
                    Some(id) => right.ctx.big_repo.document_sync_diagnostics(id).await.ok(),
                    None => None,
                };
                last_runtime = Some(format!("left={left_state:?} right={right_state:?}"));
                last_sync_diagnostics = Some(format!(
                    "left={left_diagnostics:?} right={right_diagnostics:?}"
                ));
                tracing::debug!(
                    doc_id,
                    branch = %branch,
                    left_heads = ?last_left,
                    right_heads = ?last_right,
                    runtime = ?last_runtime,
                    sync_diagnostics = ?last_sync_diagnostics,
                    "waiting for document head parity"
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| {
        eyre::eyre!(
            "timed out waiting for doc head parity: doc_id={} branch={} left={:?} right={:?} left_facet_keys={:?} right_facet_keys={:?} left_doc={:?} right_doc={:?} runtime={:?} sync_diagnostics={:?}",
            doc_id,
            branch,
            last_left,
            last_right,
            last_left_facet_keys,
            last_right_facet_keys,
            last_left_facet_values,
            last_right_facet_values,
            last_runtime,
            last_sync_diagnostics
        )
    })??;
    Ok(())
}

async fn wait_for_blob_bytes(
    blobs_repo: &BlobsRepo,
    blob_id: BlobId,
    timeout: Duration,
) -> Res<Vec<u8>> {
    let timeout = utils_rs::scale_timeout(timeout);
    tokio::time::timeout(timeout, async {
        loop {
            let path = match blobs_repo.get_path(blob_id).await {
                Ok(path) => path,
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("Blob not found:")
                        || msg.contains("Referenced blob source missing for hash")
                    {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    return Err(err);
                }
            };
            if tokio::fs::try_exists(&path).await? {
                return tokio::fs::read(path).await.map_err(Into::into);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("timed out waiting for blob bytes: {blob_id}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_for_blob_bytes_retries_until_blob_arrives() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let blobs_repo = BlobsRepo::new(
        temp_root.path().join("blobs"),
        "/u/stress-test/dev-local".into(),
    )
    .await?;
    let payload = b"delayed-blob-arrival".to_vec();
    let expected_hash = crate::blobs::BlobId::new(*blake3::hash(&payload).as_bytes());

    let repo_bg = Arc::clone(&blobs_repo);
    let payload_bg = payload.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        repo_bg.put(&payload_bg).await.expect("put should succeed");
    });

    let got = wait_for_blob_bytes(
        &blobs_repo,
        expected_hash,
        utils_rs::scale_timeout(Duration::from_secs(10)),
    )
    .await?;
    assert_eq!(got, payload);

    blobs_repo.shutdown().await?;
    Ok(())
}
