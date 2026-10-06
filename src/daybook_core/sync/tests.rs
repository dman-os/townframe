use super::*;
mod ladder;
mod stress;

use crate::blobs::{BlobId, BlobsRepo};
use crate::drawer::{DrawerRepo, types::DrawerError};
use crate::local_state::SqliteLocalStateRepo;
use crate::plugs::PlugsRepo;
use crate::progress::ProgressRepo;
use crate::repo::{RepoCtx, RepoOpenOptions};
use crate::repos::{Repo, SubscribeOpts};
use daybook_types::doc::{
    AddDocArgs, BlobPin, BranchPath, BranchPathBuf, DocId, DocPatch, FacetKey, FacetRaw,
    WellKnownFacet, WellKnownFacetTag,
};

async fn facet_set_hash_rows(
    sql: &sqlx::SqlitePool,
    doc_id: &DocId,
    facet_tag: &str,
) -> Res<Vec<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT facet_id
          FROM facet_set_doc_facets
         WHERE document_id = ?1
           AND facet_tag = ?2
        "#,
    )
    .bind(doc_id)
    .bind(facet_tag)
    .fetch_all(sql)
    .await?)
}

struct SyncTestNode {
    ctx: Arc<RepoCtx>,
    rt: Arc<crate::rt::Rt>,
    drawer: Arc<DrawerRepo>,
    blobs_repo: Arc<BlobsRepo>,
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
            utils_rs::scale_timeout(Duration::from_secs(60)),
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
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;
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

/// The teardown order the blob-inventory writer's correctness depends on: it
/// writes into the store the blob worker and the RPC server serve from, so it
/// stops only after both are down.
///
/// A stop-order inversion is invisible in any single run — every child here stops
/// successfully in any order, and the damage is a rare teardown race — so the
/// token records the order and this pins it. The drain deadlines are deliberately
/// untouched: an ordering assertion is the state-based substitute for a
/// wall-clock condition, which this campaign ruled out of tests.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_stops_the_inventory_writer_after_the_workers_that_serve_from_it() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_path = temp_root.path().join("repo-a");
    tokio::fs::create_dir_all(&repo_path).await?;
    let rtx = RepoCtx::init(
        &repo_path,
        RepoOpenOptions::default(),
        "test-device".to_string(),
        "test-device".to_string(),
    )
    .await?;
    rtx.shutdown().await?;

    // The production blob workers must run for this test to mean anything: it
    // asserts the stop order of `blob_sync_worker_stop` and
    // `blob_inventory_permission_stop`, and a node whose workers never spawned
    // has no such tasks to order.
    let node = open_sync_node(&repo_path).await?;
    // The record is read through a handle taken before the stop, which consumes
    // the token.
    let shutdown_order = node.sync_stop.shutdown_order();
    node.stop().await?;

    assert_eq!(
        shutdown_order.recorded(),
        vec![
            "big_sync_worker_stop",
            "big_sync_rpc_stop",
            "blob_sync_worker_stop",
            "big_repo_rpc_stop_token",
            "blob_inventory_permission_stop",
        ],
        "the inventory writer serves the store the blob worker and the RPC server serve \
         from, so it must stop after both"
    );
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
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

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

    wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, None).await?;

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
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

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
        .summarize_parts(HashSet::from([core_partition_id.clone()]))
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
async fn long_af_test_iroh_clone_sync_batch_100_docs_with_blobs() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    // TEMP-HUNT: with DAYB_KEYHIVE_DIAG set, leak the repo dirs on failure so
    // the failing state can be opened post-mortem.
    let run = async {
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node_without_blob_workers(&repo_a_path).await?;
        let node_b = open_sync_node_without_blob_workers(&repo_b_path).await?;
        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

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

        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "doc sets are not equal after 100-doc clone sync"
        );

        node_b.stop().await?;
        node_a.stop().await?;
        eyre::Ok(())
    };
    // TEMP-HUNT: catch panics too — a panic would otherwise unwind past the
    // leak and the repos would be cleaned up.
    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(run)).await;
    if outcome.is_err() || matches!(&outcome, Ok(Err(_))) {
        let keep = temp_root.keep();
        tracing::warn!(path = %keep.display(), "HUNT-HACK: failing repos preserved on disk");
        eyre::bail!(
            "clone sync test failed; repos preserved at {}",
            keep.display()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_clone_bootstrap_syncs_blob_scope() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    // Blob docs exist in the seed BEFORE the clone so the bootstrap phase must
    // serve blob-scope sync requests (the seed's blob worker probes the
    // bootstrap node's RPC registry for the blob scope (repo::BLOB_SCOPE_KEY).
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

    // The seed runs the production blob workers: the inventory pins a peer
    // pulls exist only because those machines derived them, so a seed with them
    // off advertises nothing and the blob scope has nothing to serve.
    let node_a = open_sync_node(&repo_a_path).await?;
    let mut blob_payloads = Vec::new();
    let mut args_batch = Vec::new();
    for idx in 0..3usize {
        let payload = format!("clone-bootstrap-blob-{idx:03}").into_bytes();
        let hash = node_a.blobs_repo.put(&payload).await?;
        blob_payloads.push((hash.clone(), payload));
        args_batch.push(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: blob_payloads.last().expect("just pushed").1.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
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

    // A blob scope exists only once the pin worker has routed each doc's Blob
    // facet into the plaintext inventory: before that the clone's blob-scope
    // subscription has nothing to pull, so waiting here is a precondition of
    // this test's own premise rather than a workaround.
    let docs_inventory_doc_id = node_a
        .drawer
        .resolve_doc_id_for_branch_doc_id(node_a.ctx.docs_inventory_doc_id.clone())
        .await?;
    wait_for_inventory_pin_ids(&node_a, &docs_inventory_doc_id, Duration::from_secs(90)).await?;

    // Clone to repo_b through the bootstrap path.
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;

    // Open node_b as a full node and converge.
    let node_b = open_sync_node(&repo_b_path).await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

    // The blob bytes must actually be present in node_b's blob store — doc-set
    // equality alone would not catch a blob-scope sync gap. Presence is checked
    // before any byte is read: `BlobsRepo::get_path` materializes a missing blob
    // from the active peers, so a read would fetch it on demand and hold however
    // (or whether) the blob scope delivered it.
    for (hash, expected) in &blob_payloads {
        wait_for_blob_replicated(&node_b.blobs_repo, hash.clone(), Duration::from_secs(60)).await?;
        let got = node_b.blobs_repo.get_bytes(hash.clone()).await?;
        assert_eq!(
            &got, expected,
            "blob content mismatch after clone bootstrap for hash={hash}"
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

/// A clone pulls the encrypted-representation inventory's blob scope, so the
/// ciphertext digests a relay is meant to retain (ADR 003 §13) reach it.
///
/// The origin runs its blob workers, because the pins exist only because the
/// production machines derived them. The plaintext is the control: it arrives
/// through the plaintext inventories just as it does today, while the ciphertext
/// is named by nothing in the documents - its inventory part is its only route.
///
/// Presence is checked by `has_blob_on_disk` and never by a byte read, because
/// `BlobsRepo::get_path` materializes a missing blob from the active peers: a
/// read here would fetch it on demand and hold however the blob was meant to
/// arrive, which is exactly what an ablation of the part advertisement did.
#[tokio::test(flavor = "multi_thread")]
async fn iroh_clone_bootstrap_syncs_encrypted_representation_inventory() -> Res<()> {
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
    let encryption_inventory_doc_id = node_a
        .drawer
        .resolve_doc_id_for_branch_doc_id(node_a.ctx.encryption_inventory_doc_id.clone())
        .await?;

    // One locally-stored plaintext with a `Blob` facet: the encryption worker
    // installs the representation, the pin worker routes its digest into the
    // encrypted-representation inventory.
    let payload = b"clone-bootstrap-ciphertext-000".to_vec();
    let plaintext = node_a.blobs_repo.put(&payload).await?;
    let doc_id = node_a
        .drawer
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(plaintext.clone()),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{plaintext}")]),
                })),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    let ciphertext_pins = match wait_for_inventory_pin_ids(
        &node_a,
        &encryption_inventory_doc_id,
        Duration::from_secs(90),
    )
    .await
    {
        Ok(pins) => pins,
        Err(err) => {
            // Localise before failing: the encryption write and the pin routing
            // are separate stages, and which one broke decides whose contract it
            // is. Report both, and whether the inventory document was found at
            // all (the resolution falls back to the branch doc id's own
            // spelling, which names a document the drawer may not have).
            let content = node_a
                .drawer
                .get_doc_with_facets_at_branch(
                    &doc_id,
                    daybook_types::doc::BranchPath::new("main"),
                    None,
                )
                .await?;
            let cipher_facets = content
                .as_ref()
                .map(|doc| {
                    doc.facets
                        .keys()
                        .filter(|key| key.tag == WellKnownFacetTag::CipherBlob.into())
                        .map(|key| key.id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let inventory = node_a
                .drawer
                .get_doc_with_facets_at_branch(
                    &encryption_inventory_doc_id,
                    daybook_types::doc::BranchPath::new("main"),
                    None,
                )
                .await?;
            let pin_count = inventory
                .as_ref()
                .map(|doc| {
                    doc.facets
                        .keys()
                        .filter(|key| key.tag == WellKnownFacetTag::BlobPin.into())
                        .count()
                })
                .unwrap_or(0);
            eyre::bail!(
                "{err:#} [localisation] inventory doc {} with {pin_count} BlobPin facet(s); content doc {} with cipherBlob facets {cipher_facets:?}",
                if inventory.is_some() {
                    "found"
                } else {
                    "NOT found"
                },
                if content.is_some() {
                    "found"
                } else {
                    "NOT found"
                },
            );
        }
    };
    let ciphertext = ciphertext_pins
        .iter()
        .map(|pin| {
            crate::blobs::digest_str_to_blob_id_lenient(pin)
                .ok_or_else(|| eyre::eyre!("inventory pin {pin} is not a blob digest"))
        })
        .collect::<Res<Vec<_>>>()?;
    assert!(
        !ciphertext.is_empty() && !ciphertext.contains(&plaintext),
        "the inventory must name ciphertext, not the plaintext: {ciphertext:?}"
    );

    // Nothing in the document references the ciphertext: its only route to the
    // mirror is the inventory's blob part. The document's `Blob` url names the
    // plaintext, and the `?via=` url added at the commit point names the facet
    // key (also spelled with the plaintext digest).
    let doc = node_a
        .drawer
        .get_doc_with_facets_at_branch(&doc_id, daybook_types::doc::BranchPath::new("main"), None)
        .await?
        .ok_or_else(|| eyre::eyre!("the document with the Blob facet must exist"))?;
    let mut doc_urls = Vec::new();
    for (key, raw) in &doc.facets {
        if key.tag == WellKnownFacetTag::Blob.into()
            && let Ok(WellKnownFacet::Blob(blob)) =
                WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)
            && let Some(urls) = blob.urls
        {
            doc_urls.extend(urls);
        }
    }
    for c in &ciphertext {
        let spelled = c.to_string();
        assert!(
            !doc_urls.iter().any(|url| url.contains(&spelled)),
            "ciphertext {c} must not be reachable by a document URL: {doc_urls:?}"
        );
    }

    // Clone to repo_b through the bootstrap path.
    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;

    let node_b = open_sync_node(&repo_b_path).await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    // Both sides must name the same blob parts: the clone adopts the origin's
    // inventory ids from the shared config doc.
    assert_eq!(
        &node_b.ctx.encryption_inventory_doc_id, &node_a.ctx.encryption_inventory_doc_id,
        "the clone must name the origin's encrypted-representation inventory"
    );
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

    // TEMP-INSTRUMENTATION: localising why the blob scope does not replicate.
    {
        for (name, part) in [
            (
                "core",
                crate::blobs::blob_inventory_part_id(&node_a.ctx.core_inventory_doc_id),
            ),
            (
                "docs",
                crate::blobs::blob_inventory_part_id(&node_a.ctx.docs_inventory_doc_id),
            ),
            (
                "encryption",
                crate::blobs::blob_inventory_part_id(&node_a.ctx.encryption_inventory_doc_id),
            ),
        ] {
            eprintln!(
                "PROBE part {name} members: origin={} mirror={}",
                node_a
                    .ctx
                    .blob_part_store
                    .member_count(part.clone())
                    .await?,
                node_b.ctx.blob_part_store.member_count(part).await?,
            );
        }
        eprintln!(
            "PROBE plaintext obj exists: origin={} parts={:?} | mirror={} parts={:?}",
            node_a
                .ctx
                .blob_part_store
                .obj_exists(plaintext.clone().into())
                .await?,
            node_a
                .ctx
                .blob_part_store
                .obj_parts(plaintext.clone().into())
                .await?,
            node_b
                .ctx
                .blob_part_store
                .obj_exists(plaintext.clone().into())
                .await?,
            node_b
                .ctx
                .blob_part_store
                .obj_parts(plaintext.clone().into())
                .await?,
        );
        for c in &ciphertext {
            eprintln!(
                "PROBE ciphertext obj {c}: origin exists={} parts={:?} | mirror exists={} parts={:?}",
                node_a
                    .ctx
                    .blob_part_store
                    .obj_exists(c.clone().into())
                    .await?,
                node_a
                    .ctx
                    .blob_part_store
                    .obj_parts(c.clone().into())
                    .await?,
                node_b
                    .ctx
                    .blob_part_store
                    .obj_exists(c.clone().into())
                    .await?,
                node_b
                    .ctx
                    .blob_part_store
                    .obj_parts(c.clone().into())
                    .await?,
            );
        }
    }

    // Presence first, before any byte is read: reading a missing blob fetches
    // it from the active peers, so it would prove nothing about the sync plane.
    wait_for_blob_replicated(
        &node_b.blobs_repo,
        plaintext.clone(),
        Duration::from_secs(60),
    )
    .await?;
    for c in &ciphertext {
        wait_for_blob_replicated(&node_b.blobs_repo, c.clone(), Duration::from_secs(60)).await?;
    }

    // Only now are bytes read: the plaintext is intact, and what the
    // encrypted-representation inventory named is real ciphertext.
    let got_plaintext = node_b.blobs_repo.get_bytes(plaintext).await?;
    assert_eq!(got_plaintext, payload, "the plaintext must be intact");
    for c in &ciphertext {
        let got = node_b.blobs_repo.get_bytes(c.clone()).await?;
        assert!(!got.is_empty(), "ciphertext {c} must have bytes");
        assert_ne!(
            got, payload,
            "ciphertext {c} must be ciphertext, not the plaintext bytes"
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

/// Wait until `blob_id` is on disk, without reading it.
///
/// A byte read cannot be the check: `BlobsRepo::get_path` materializes a missing
/// blob from the active peers, so a read would fetch it here and pass no matter
/// how (or whether) the sync plane delivered it.
async fn wait_for_blob_replicated(
    blobs_repo: &BlobsRepo,
    blob_id: BlobId,
    timeout: Duration,
) -> Res<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if blobs_repo.has_blob_on_disk(blob_id.clone()).await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!("timed out waiting for {blob_id} to arrive by sync, not on demand");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `BlobPin` facet ids recorded on an inventory document, once they appear.
async fn wait_for_inventory_pin_ids(
    node: &SyncTestNode,
    inventory_doc_id: &DocId,
    timeout: Duration,
) -> Res<Vec<String>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let pins = inventory_pin_ids(&node.drawer, inventory_doc_id).await?;
        if !pins.is_empty() {
            return Ok(pins);
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for pins in inventory {inventory_doc_id}: the origin's blob workers must derive them"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `BlobPin` facet ids on an inventory document.
async fn inventory_pin_ids(drawer: &DrawerRepo, inventory_doc_id: &DocId) -> Res<Vec<String>> {
    let Some(doc) = drawer
        .get_doc_with_facets_at_branch(
            inventory_doc_id,
            daybook_types::doc::BranchPath::new("main"),
            None,
        )
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(doc
        .facets
        .keys()
        .filter(|key| key.tag == WellKnownFacetTag::BlobPin.into())
        .map(|key| key.id.clone())
        .collect())
}

/// Every inventory in the repo is a blob scope, so the sync plane must
/// advertise each one: a part that is never advertised is never pulled, and a
/// peer that cannot pull the encrypted-representation inventory never learns
/// the ciphertext digests it is meant to retain (ADR 003 §13).
#[tokio::test(flavor = "multi_thread")]
async fn peer_partition_ids_advertise_every_blob_inventory() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_path = temp_root.path().join("repo");
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
    let advertised = node.sync_repo.peer_partition_ids("", true);
    let inventories = [
        node.ctx.core_inventory_doc_id.clone(),
        node.ctx.docs_inventory_doc_id.clone(),
        node.ctx.encryption_inventory_doc_id.clone(),
    ];
    for inventory in &inventories {
        let part = crate::blobs::blob_inventory_part_id(inventory);
        assert!(
            advertised.contains_key(&part),
            "inventory {inventory} must be advertised as a blob part"
        );
        assert!(
            node.sync_repo.is_blob_part(&part),
            "inventory {inventory} must classify as a blob part"
        );
    }

    node.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_blob_sync_validates_bytes() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    // TEMP-HUNT: with DAYB_KEYHIVE_DIAG set, leak the repo dirs on failure so
    // the failing state can be opened post-mortem.
    let run = async {
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        // The seed runs the production blob workers: the pins a peer pulls
        // exist only because those machines derived them.
        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;
        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;
        eyre::Ok((node_a, node_b))
    };
    let Ok((node_a, node_b)) = run.await else {
        let keep = temp_root.keep();
        tracing::warn!(path = %keep.display(), "HUNT-HACK: failing repos preserved on disk");
        eyre::bail!(
            "clone sync test failed; repos preserved at {}",
            keep.display()
        );
    };

    let mut blob_payloads = Vec::new();
    let mut args_batch = Vec::new();
    for idx in 0..8usize {
        let payload = format!("blob-bytes-validation-{idx:03}").into_bytes();
        let hash = node_a.blobs_repo.put(&payload).await?;
        blob_payloads.push((hash.clone(), payload));
        args_batch.push(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: blob_payloads.last().expect("just pushed").1.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
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

    // Presence before any read: `get_path` materializes a missing blob from the
    // active peers, so a bare read would pass whether or not the blob scope
    // delivered the bytes.
    for (hash, expected) in &blob_payloads {
        wait_for_blob_replicated(&node_b.blobs_repo, hash.clone(), Duration::from_secs(60)).await?;
        let got = node_b.blobs_repo.get_bytes(hash.clone()).await?;
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
    // TEMP-HUNT: with DAYB_KEYHIVE_DIAG set, leak the repo dirs on failure so
    // the failing state can be opened post-mortem.
    let run = async {
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node_without_blob_workers(&repo_a_path).await?;
        let node_b = open_sync_node_without_blob_workers(&repo_b_path).await?;
        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;
        eyre::Ok((node_a, node_b))
    };
    let Ok((node_a, node_b)) = run.await else {
        let keep = temp_root.keep();
        tracing::warn!(path = %keep.display(), "HUNT-HACK: failing repos preserved on disk");
        eyre::bail!(
            "clone sync test failed; repos preserved at {}",
            keep.display()
        );
    };

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

    // 2. Verify node_b's facet-set index has settled the doc's BlobPin
    // facet routes (the projection the retired doc-blobs index derived from).
    let facet_set_sql = node_b.rt.doc_facet_set_index_repo.sql().clone();
    let blob_pin_tag = daybook_types::doc::WellKnownFacetTag::BlobPin.as_str();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let hashes = facet_set_hash_rows(&facet_set_sql.read_pool, &doc_id, blob_pin_tag).await?;
        if hashes.contains(&hash_1) && hashes.contains(&hash_2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let hashes_b = facet_set_hash_rows(&facet_set_sql.read_pool, &doc_id, blob_pin_tag).await?;
    assert!(hashes_b.contains(&hash_1));
    assert!(hashes_b.contains(&hash_2));

    // 3. Verify node_b.blobs_repo.get_bytes(blob_id) successfully fetches the blob bytes from node_a
    let bytes_1 = wait_for_blob_bytes(&node_b.blobs_repo, blob_id_1.clone(), None).await?;
    assert_eq!(bytes_1, payload_1);
    let bytes_1_direct = node_b.blobs_repo.get_bytes(blob_id_1).await?;
    assert_eq!(bytes_1_direct, payload_1);

    let bytes_2 = wait_for_blob_bytes(&node_b.blobs_repo, blob_id_2.clone(), None).await?;
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
        let hashes = facet_set_hash_rows(&facet_set_sql.read_pool, &doc_id, blob_pin_tag).await?;
        if hashes.len() == 1 && hashes.contains(&hash_1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let hashes_after = facet_set_hash_rows(&facet_set_sql.read_pool, &doc_id, blob_pin_tag).await?;
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
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id).await?;

    for doc_id in &created_doc_ids {
        wait_for_doc_presence_with_activity(&node_b, doc_id, Duration::from_secs(60)).await?;
    }

    wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, None).await?;

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

/// Open a sync test node over a prepared repo on the production boot path: the
/// blob workers run, as they do on every node the CLI and FFI boot, so a test
/// node whose inventories a peer pulls derives and serves exactly what
/// production does.
async fn open_sync_node(repo_root: &std::path::Path) -> Res<SyncTestNode> {
    let rtx = RepoCtx::open(
        repo_root,
        RepoOpenOptions {
            sync_max_task_backoff: Some(Duration::from_millis(500)),
        },
        "test-device".into(),
    )
    .await?;
    open_sync_node_over_ctx(rtx).await
}

/// Open a sync test node with the production blob workers deliberately left
/// off. Two tests call it, for different reasons:
///
/// - `iroh_blob_pin_sync_replicates_and_fetches_blobs`: its subject is the
///   on-demand fetch, and running the pin and inventory workers pins the
///   referenced blobs into the blob scope so they arrive by sync instead,
///   which is precisely the assertion that test makes.
/// - `long_af_test_iroh_clone_sync_batch_100_docs_with_blobs`: with the workers
///   running each of its hundred attached blobs gets an encrypted
///   representation plus derived pins, and the test runs 4-6x longer (146 s
///   pass, 141 s assertion failure, >240 s timeout observed) - tracked as a
///   finding about the production shape under load, not hidden test cost.
async fn open_sync_node_without_blob_workers(repo_root: &std::path::Path) -> Res<SyncTestNode> {
    let rtx = RepoCtx::open(
        repo_root,
        RepoOpenOptions {
            sync_max_task_backoff: Some(Duration::from_millis(500)),
        },
        "test-device".into(),
    )
    .await?;
    boot_sync_node(rtx, false).await
}

/// [`open_sync_node`] over an already-opened repo context: a told node's test-built ctx
/// (see `init_told_sync_node`) takes the same boot path as a plain one.
async fn open_sync_node_over_ctx(rtx: Arc<RepoCtx>) -> Res<SyncTestNode> {
    boot_sync_node(rtx, true).await
}

/// The sync-test node boot body, and the single place `blob_workers` is read.
async fn boot_sync_node(rtx: Arc<RepoCtx>, blob_workers: bool) -> Res<SyncTestNode> {
    info!(repo_root = %rtx.layout.repo_root.display(), "opening sync test node");
    let blobs_repo =
        BlobsRepo::new(rtx.layout.blobs_root.clone(), rtx.local_user_path.clone()).await?;
    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        Arc::clone(&rtx.big_repo),
        Arc::clone(&blobs_repo),
        rtx.doc_config.document_id(),
        daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        Arc::clone(&rtx.sqlite_local_state_repo),
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
            spawn_blob_workers: blob_workers,
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
        Some(Arc::clone(&progress_repo)),
    )
    .await?;

    Ok(SyncTestNode {
        ctx: rtx,
        drawer: Arc::clone(&rt.drawer),
        blobs_repo: Arc::clone(&rt.blobs_repo),
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
                        | crate::drawer::DrawerEvent::DocDeleted { id, .. } if id == doc_id => {
                            *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                        }
                        crate::drawer::DrawerEvent::DocAdded { .. }
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
                        Err(crate::repos::RecvError::Closed) => {
                            eyre::bail!("sync listener closed while waiting for doc presence");
                        }
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
                        Err(crate::repos::RecvError::Closed) => {
                            eyre::bail!("progress listener closed while waiting for doc presence");
                        }
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

/// Dump every (peer, part) a node's sync workers still consider unsettled, with the
/// term that holds each one back, so a wait that refuses to settle names its own cause.
async fn dump_sync_state(node: &SyncTestNode, label: &str) {
    for (worker_name, worker) in [
        ("docs", &node.sync_repo.big_sync_worker),
        ("blobs", &node.sync_repo.blob_sync_worker),
    ] {
        let Ok(snapshot) = worker.snapshot().await else {
            warn!(%label, worker = worker_name, "sync state dump: snapshot failed");
            continue;
        };
        let unsettled = snapshot
            .peer_part_sync_flags
            .iter()
            .filter(|(_, _, pending, multi_strat, replay_done, cursor_active, unanswered)| {
                *pending || *multi_strat || !*replay_done || *cursor_active || *unanswered
            })
            .map(
                |(peer, part, pending, multi_strat, replay_done, cursor_active, unanswered)| {
                    format!(
                        "peer={peer} part={part} pending={pending} multi_strat={multi_strat} replay_done={replay_done} cursor_active={cursor_active} unanswered={unanswered}"
                    )
                },
            )
            .collect::<Vec<_>>();
        warn!(
            %label,
            worker = worker_name,
            local_peer_id = %node.sync_repo.router.endpoint().id(),
            waiters = ?snapshot.full_sync_waiters,
            unsettled = ?unsettled,
            "sync state dump"
        );
    }
}

async fn wait_for_sync_convergence(
    source: &SyncTestNode,
    target: &SyncTestNode,
    endpoint_id: EndpointId,
) -> Res<()> {
    let required_partitions = source
        .sync_repo
        .peer_partition_ids("", true)
        .into_keys()
        .collect::<Vec<_>>();
    let peer_id = PeerKey::new(*endpoint_id.as_bytes());
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
    // A stall here dumps what refused to settle: the (peer, part) pairs still outstanding and
    // which of the four `peer_part_is_fully_synced` terms held them back. The cadence is
    // diagnosis only: nextest owns the test's timeout, so this loop exits when the wait does.
    let wait = async {
        tokio::try_join!(
            target.sync_repo.wait_for_full_sync(
                std::slice::from_ref(&peer_id),
                &required_partitions,
                None,
            ),
            wait_for_doc_set_parity(&source.drawer, &target.drawer, None),
        )
    };
    tokio::pin!(wait);
    let mut next_dump = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        tokio::select! {
            result = &mut wait => {
                result?;
                break;
            }
            _ = tokio::time::sleep_until(next_dump) => {
                dump_sync_state(source, "source").await;
                dump_sync_state(target, "target").await;
                next_dump += Duration::from_secs(45);
            }
        }
    }
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

    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr_ba.id).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let required_partitions = node_b
        .sync_repo
        .peer_partition_ids("", true)
        .into_keys()
        .collect::<Vec<_>>();
    let peer_id = PeerKey::new(*endpoint_addr_ba.id.as_bytes());
    node_b
        .sync_repo
        .wait_for_full_sync(std::slice::from_ref(&peer_id), &required_partitions, None)
        .await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

/// A peer can hold a branch's revoked access while its own entry still lists the
/// branch: the two halves of a replicated branch delete travel on different
/// channels, and the network delivers them independently.
///
/// The revocation arrives on the keyhive channel. A keyhive exchange's durable
/// incorporation is awaited *inside* the protocol's message handler, in the
/// connection task, so the peer's graph advances without its hub processing
/// anything. The tombstone that drops the branch from the entry travels on the doc
/// channel and is applied by the hub. Holding the hub's events separates the two
/// halves without touching either transport: the keyhive half is complete and the
/// doc half has not run.
///
/// What the peer does with the branch in that state is the contract this pins. The
/// write must be refused as an unknown branch, never as a local access refusal:
/// "the branch was deleted" and "you lost permission on a live branch" need
/// different handling, and the doc worker's own message cannot tell them apart.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_holds_its_revoked_branch_until_the_delete_lands_on_the_doc_channel() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr_ba = node_b.sync_repo.connect_url(&ticket_a).await?;
    let peer_b = PeerKey::new(*node_b.sync_repo.router.endpoint().id().as_bytes());
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr_ba.id).await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = node_a
        .drawer
        .add(AddDocArgs {
            branch_path: BranchPathBuf::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Initial".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    let main_heads = node_a
        .drawer
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing doc branches after add")?
        .branches
        .get("main")
        .ok_or_eyre("missing main branch")?
        .clone();
    let branch = BranchPathBuf::from("/stress/1");
    node_a
        .drawer
        .create_branch_at_heads_from_branch(
            &doc_id,
            &branch,
            BranchPath::new("main"),
            &main_heads,
            None,
        )
        .await?;

    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr_ba.id).await?;

    // Positive control: the peer holds the replicated branch while it is reachable,
    // so the assertions below cannot pass vacuously.
    let held = node_b
        .drawer
        .get_entry(&doc_id)
        .await?
        .ok_or_eyre("peer never received the doc entry")?;
    assert!(
        held.branches.contains_key("/stress/1"),
        "peer must hold the replicated branch before the delete"
    );

    // Hold the peer's events: the doc channel's half of the delete cannot be
    // applied while held. Commands are still served, so the keyhive half below
    // still runs.
    let hold = node_b.ctx.big_repo.hold_hub_events().await?;

    assert!(node_a.drawer.delete_branch(&doc_id, &branch, None).await?);

    // Drive the keyhive half from the node that is NOT held, and await it: the
    // initiator's completion is resolved by its own hub, and the exchange cannot
    // complete until the responder has durably incorporated it, so a returned
    // round means the peer's graph holds the revocation.
    node_a
        .ctx
        .big_repo
        .sync_keyhive_with_peer(peer_b.clone())
        .await?;

    let entry = node_b
        .drawer
        .get_entry(&doc_id)
        .await?
        .ok_or_eyre("peer doc entry vanished")?;
    let branch_doc_id = entry
        .branches
        .get("/stress/1")
        .ok_or_eyre(
            "the tombstone travels on the doc channel and its apply is held, so the peer's \
             entry must still list the branch",
        )?
        .branch_doc_id
        .clone();
    assert!(
        !node_b.drawer.branch_doc_reachable(&branch_doc_id).await?,
        "the revocation arrived on the keyhive channel, so the peer must no longer reach the \
         branch doc"
    );
    let listed = node_b
        .drawer
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("peer doc branches vanished")?;
    assert!(
        !listed.branches.contains_key("/stress/1"),
        "a branch this peer cannot reach must not be presented by the resolved listing"
    );

    // The write is the contract: unknown branch, not a local access refusal.
    let err = node_b
        .drawer
        .update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("revoked-peer".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            &branch,
            None,
        )
        .await
        .expect_err("a write to a branch whose branch doc is unreachable must be refused");
    assert!(
        matches!(err, DrawerError::BranchNotFound { .. }),
        "expected BranchNotFound, got {err:?}"
    );
    assert!(
        !err.to_string().contains("local access is not writable"),
        "the refusal must not report a permission problem on a live branch: {err}"
    );

    // Release the held events before stopping, so no held work outlives the test.
    hold.resume().await?;
    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

async fn wait_for_doc_set_parity(
    left: &DrawerRepo,
    right: &DrawerRepo,
    timeout: Option<Duration>,
) -> Res<()> {
    let mut last_left = HashSet::<String>::new();
    let mut last_right = HashSet::<String>::new();
    let poll_fut = async {
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
    };
    if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, poll_fut).await {
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
    } else {
        poll_fut.await?;
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
            let (left_doc, left_facet_keys, left_facet_values, left_heads) = left
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
            let (right_doc, right_facet_keys, right_facet_values, right_heads) = right
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
            if last_left == last_right
                && left_facet_keys == right_facet_keys
                && left_doc.facets == right_doc.facets
            {
                break eyre::Ok(());
            }
            let now = std::time::Instant::now();
            if now.duration_since(last_heartbeat) >= Duration::from_secs(2) {
                last_heartbeat = now;
                let runtime_doc_id = doc_id.parse::<big_repo::DocumentId>().ok();
                let left_state = match runtime_doc_id.clone() {
                    Some(id) => left.ctx.big_repo.doc_head_state(id).await.ok(),
                    None => None,
                };
                let right_state = match runtime_doc_id.clone() {
                    Some(id) => right.ctx.big_repo.doc_head_state(id).await.ok(),
                    None => None,
                };
                let left_diagnostics = match runtime_doc_id.clone() {
                    Some(id) => left.ctx.big_repo.document_sync_diagnostics(id).await.ok(),
                    None => None,
                };
                let right_diagnostics = match runtime_doc_id.clone() {
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
    timeout: Option<Duration>,
) -> Res<Vec<u8>> {
    let deadline = timeout
        .map(utils_rs::scale_timeout)
        .map(|t| tokio::time::Instant::now() + t);
    let mut last_log = tokio::time::Instant::now();
    loop {
        let path = match blobs_repo.get_path(blob_id.clone()).await {
            Ok(path) => path,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("Blob not found:")
                    || msg.contains("Referenced blob source missing for hash")
                {
                    if last_log.elapsed() >= Duration::from_secs(5) {
                        warn!("wait_for_blob_bytes waiting for blob={blob_id} path resolution...");
                        last_log = tokio::time::Instant::now();
                    }
                    if let Some(d) = deadline
                        && tokio::time::Instant::now() >= d
                    {
                        eyre::bail!("timed out waiting for blob bytes: {blob_id}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(err);
            }
        };
        if tokio::fs::try_exists(&path).await? {
            return tokio::fs::read(path).await.map_err(Into::into);
        }
        if last_log.elapsed() >= Duration::from_secs(5) {
            warn!("wait_for_blob_bytes waiting for blob={blob_id} file presence on disk...");
            last_log = tokio::time::Instant::now();
        }
        if let Some(d) = deadline
            && tokio::time::Instant::now() >= d
        {
            eyre::bail!("timed out waiting for blob bytes: {blob_id}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
    let put_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        repo_bg.put(&payload_bg).await
    });

    let got =
        wait_for_blob_bytes(&blobs_repo, expected_hash, Some(Duration::from_secs(10))).await?;
    assert_eq!(got, payload);
    put_task
        .await
        .expect("delayed blob put task should complete")?;

    blobs_repo.shutdown().await?;
    Ok(())
}

/// Node_b's seam for the "told, not cloned" shape: a fresh, fully independent
/// repo that never cloned anything (its keyhive holds only its own graph), and
/// whose inventories are the ones the config doc names.
///
/// The telling is exactly the write every repo init and clone adoption makes:
/// `ConfigRepo::set_blob_inventories` on the repo's config field
/// (`AppBlobInventories`, config.rs), and the local init_state row for the
/// inventories is dropped to the triple-of-`None`s the clone path itself
/// writes (`sync/bootstrap.rs`) — the shape `load_core_docs`' config fallback
/// exists for. The ctx is then booted through `finish_clone_init`, the same
/// loader the clone path uses when the inventoried ids live in config, so the
/// told ids genuinely arrive via the config field rather than being injected.
///
/// The plain disk-open path cannot serve this shape: it additionally runs
/// `grant_docs_admin` against the told (foreign) inventory documents, which
/// fails on a keyhive that has never pulled them — only a pulled (clone- or
/// grant-fed) repo can. `finish_clone_init` boots the same ctx shape without
/// that requirement, and that is the one legitimate pre-existing seam in the
/// tree; no new config plumbing was added.
async fn init_told_sync_node(
    repo_root: &std::path::Path,
    told: crate::config::AppBlobInventories,
    blob_workers: bool,
) -> Res<SyncTestNode> {
    let device_name = "test-device".to_string();
    // 1. A fresh independent repo: own identity, own core docs, own keyhive.
    //    Its own init dance also mints its own inventories; the tell below
    //    re-points the node's inventories at the told ones, and the local
    //    leftovers stay as inert drawer-only docs nothing watches.
    let rtx = RepoCtx::init(
        repo_root,
        RepoOpenOptions::default(),
        "told-node".to_string(),
        device_name.clone(),
    )
    .await?;
    let layout = rtx.layout.clone();
    let doc_app_id = rtx.doc_app.document_id();
    let doc_drawer_id = rtx.doc_drawer.document_id();
    let doc_config_id = rtx.doc_config.document_id();
    let local_device_name = rtx.local_device_name.clone();
    let local_user_path = rtx.local_user_path.clone();
    let local_peer_key = std::sync::Arc::<str>::clone(&rtx.local_peer_key);
    let local_actor_id = rtx.local_actor_id.clone();
    let repo_id = rtx.repo_id.clone();
    let checkout_id = rtx.checkout_id.clone();
    let repo_name = rtx.repo_name.clone();

    // 2. The tell, on the node's own config doc, via the production mutator.
    let result: Res<()> = async {
        let blobs_repo = BlobsRepo::new(
            layout.blobs_root.clone(),
            daybook_types::doc::UserPathBuf::from(local_user_path.clone()),
        )
        .await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            Arc::clone(&rtx.big_repo),
            Arc::clone(&blobs_repo),
            doc_config_id.clone(),
            daybook_types::doc::UserPathBuf::from(local_user_path.clone()),
            Arc::clone(&rtx.sqlite_local_state_repo),
        )
        .await?;
        let (config_repo, config_stop) = crate::config::ConfigRepo::load(
            Arc::clone(&rtx.big_repo),
            doc_app_id.clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPathBuf::from(local_user_path.clone()),
            rtx.sql.clone(),
        )
        .await?;
        config_repo.set_blob_inventories(told.clone()).await?;
        assert_eq!(
            config_repo.get_blob_inventories().await,
            Some(told.clone()),
            "the tell must land in the repo's config doc",
        );
        crate::repo::globals::set_init_state(
            &rtx.sql,
            &crate::repo::globals::InitState::Created {
                doc_id_app: doc_app_id.clone(),
                doc_id_drawer: doc_drawer_id.clone(),
                doc_id_config: Some(doc_config_id.clone()),
                core_inventory_doc_id: None,
                docs_inventory_doc_id: None,
                encryption_inventory_doc_id: told.encryption_inventory_doc_id.clone(),
            },
        )
        .await?;
        config_stop.stop().await?;
        plugs_stop.stop().await?;
        blobs_repo.shutdown().await?;
        eyre::Ok(())
    }
    .await;
    rtx.shutdown().await?;
    result?;

    // 3. The told boot: the ctx the clone machinery itself delivers once the
    // inventoried ids live in config.
    let result: Res<SyncTestNode> = async {
        let lock_guard = crate::repo::RepoLockGuard::acquire(layout.lock_path.clone()).await?;
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::file(layout.sqlite_path.clone()))
            .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            crate::local_state::SqliteLocalStateRepo::boot(layout.repo_root.join("local_state"))
                .await?;
        let secret_store = secrets_rs::SecretStore::boot().await?;
        let identity = crate::secrets::load_identity(&secret_store, &checkout_id.clone())
            .await?
            .ok_or_else(|| eyre::eyre!("told node identity missing from the secret store"))?;
        let (big_repo, big_repo_stop) = big_repo::BigRepo::boot(big_repo::Config {
            node_identity_seed: identity.iroh_secret_key.to_bytes(),
            storage: big_repo::StorageConfig::Disk {
                path: layout.big_repo_root.clone(),
            },
            scope_key: Arc::from("daybook-core"),
            hidden_parts: default(),
            automerge_frontier_group_scope: default(),
            causal_checkpoint_group_scope: default(),
            group_part_group_scope: default(),
        })
        .await?;
        let parts = crate::repo::RepoCtxParts {
            layout: layout.clone(),
            lock_guard,
            options: RepoOpenOptions {
                sync_max_task_backoff: Some(Duration::from_millis(500)),
            },
            sql: sql.clone(),
            sqlite_local_state_repo: Arc::clone(&sqlite_local_state_repo),
            sqlite_local_state_stop: std::sync::Mutex::new(Some(sqlite_local_state_stop)),
            part_store: big_repo.shared_part_store(),
            blob_part_store: crate::repo::open_blob_part_store(big_repo.sql_ctx()).await?,
            blob_presence_store: crate::repo::open_blob_presence_part_store(big_repo.sql_ctx())
                .await?,
            frontier_part_store: big_repo.frontier_part_store(),
            derived_part_store: big_repo.derived_part_store(),
            big_repo: Arc::clone(&big_repo),
            big_repo_stop: std::sync::Mutex::new(Some(big_repo_stop)),
            local_peer_key,
            local_actor_id,
            local_user_path: local_user_path.clone(),
            local_device_name,
            repo_id,
            checkout_id,
            repo_name,
            iroh_public_key: identity.iroh_public_key.to_string(),
            iroh_secret_key: identity.iroh_secret_key.clone(),
            secret_store,
        };
        let rtx = crate::repo::finish_clone_init(parts).await?;
        assert_eq!(
            rtx.encryption_inventory_doc_id, told.encryption_inventory_doc_id,
            "the told boot must resolve the encryption inventory from the config doc",
        );
        assert_eq!(
            rtx.core_inventory_doc_id, told.core_inventory_doc_id,
            "the told boot must resolve the core inventory from the config doc",
        );
        boot_sync_node(rtx, blob_workers).await
    }
    .await;
    result
}

/// The access rows of one blob-inventory part, read from the repository's own
/// blob part store (scope-keyed; the shape the permission writer's tests use).
async fn inventory_part_rows(
    sql: &SqlCtx,
    scope: &str,
    part: &PartKey,
) -> Res<Vec<(PeerKey, String)>> {
    let scope_id =
        big_sync::sqlite_core::SqliteCore::ensure_scope_id(&sql.write_pool, &Arc::from(scope))
            .await?;
    let rows = sqlx::query(
        r#"
        SELECT s.principal_id AS principal_id
             , s.access_level AS access_level
          FROM big_sync_syncable s
          JOIN big_sync_parts p ON p.part_ref = s.part_ref
         WHERE p.scope_id = ?1
           AND p.part_id = ?2
        "#,
    )
    .bind(scope_id)
    .bind(big_sync::sqlite_core::SqliteCore::part_blob(part.clone()))
    .fetch_all(&sql.read_pool)
    .await?;
    use sqlx::Row as _;
    rows.into_iter()
        .map(|row| {
            let principal: Vec<u8> = row.get("principal_id");
            let level: i64 = row.get("access_level");
            let principal =
                PeerKey::new(<[u8; 32]>::try_from(principal.as_slice()).map_err(|_| {
                    eyre::eyre!(
                        "a principal id is {} bytes wide, expected 32",
                        principal.len()
                    )
                })?);
            Ok((principal, level.to_string()))
        })
        .collect()
}

/// Bounded wait for the peer's keyhive agent on a node (the grant needs it).
async fn wait_for_peer_agent(
    node: &SyncTestNode,
    peer_id: PeerKey,
    timeout: Duration,
) -> Res<big_repo::BigKeyhiveAgent> {
    let deadline = utils_rs::scale_timeout(timeout);
    let deadline = tokio::time::Instant::now() + deadline;
    loop {
        if let Some(agent) = node
            .ctx
            .big_repo
            .keyhive_agent_for_peer(peer_id.clone())
            .await?
        {
            return Ok(agent);
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for the peer's keyhive agent on node {}; \
                 the contact-card exchange never named it",
                node.sync_repo.router.endpoint().id(),
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drive one keyhive exchange from `node` toward `peer`, retrying while the
/// connection has not landed yet (the established pattern of
/// `sync/tests/stress.rs`, which connects and then exchanges in both
/// directions).
async fn drive_keyhive_exchange(
    node: &SyncTestNode,
    peer_id: PeerKey,
    timeout: Duration,
) -> Res<()> {
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(timeout);
    loop {
        match node
            .ctx
            .big_repo
            .sync_keyhive_with_peer(peer_id.clone())
            .await
        {
            Ok(()) => return Ok(()),
            Err(_) if tokio::time::Instant::now() >= deadline => {
                eyre::bail!(
                    "timed out driving the keyhive exchange from {} toward {peer_id}",
                    node.sync_repo.router.endpoint().id(),
                );
            }
            Err(_) => {
                // The connection may still be landing on the far side; retry
                // within the bounded window.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// The "told, not cloned" arc: a node holds the inventories of documents it is
/// told about through config, without ever cloning the origin's graph, and the
/// ciphertext the encrypted-representation inventory names is unreachable to
/// it until the origin grants it the inventory document (ADR 003 §13).
///
/// node_a runs its production blob workers: the encryption worker installs the
/// representation of one locally-stored plaintext, and the pin worker derives
/// the C pin into the encryption inventory — the only place the ciphertext is
/// named by anything (the doc names its own `Blob` url with the plaintext).
///
/// node_b is told the inventories through the config field only
/// (`init_told_sync_node`): it never cloned, its keyhive has never seen the
/// origin's graph, and its told parts start empty and unmembered.
///
/// Both arcs live here, ordered:
/// 1. *Ablation, before any grant*, bounded: within a window where the machine
///    is only pacing its retries, the ciphertext never lands on disk and the
///    machine reports the refused part (`PeerPartUnanswered` — the flags
///    record of the UnknownPart verdict the serving side answers because the
///    origin's part rows, written from the inventory document's closure by the
///    permission writer, do not admit node_b) and the full-sync waiter stays
///    blocked on the part.
/// 2. *The grant*, after the bounded window: a Read grant on the told
///    encryption inventory document only (the grant shape big_repo's own
///    cross-repo tests use) reaches node_a's permission writer, whose part row
///    now admits node_b; the part's next retry resolves, its members arrive,
///    and the ciphertext bytes land — while the still-ungranted core/docs
///    parts of the same subscription batch stay refused: no members, no
///    bytes, `unanswered` still true. The peer summary answer is per part
///    (`big_sync/rpc.rs` refuses named parts beside the readable summaries in
///    the same answer), and the decision side marks exactly the refused parts
///    unanswered and retries exactly those, so a partially granted batch
///    serves every granted part of it.
///
/// Ablation story: each half fails alone. If the serving side stopped folding
/// denied parts into the refused set — or the permission writer stopped
/// seeding/deltaing rows — the ciphertext would land before the grant and the
/// no-bytes/member-count assertions of the ablation half fail. If the
/// pending-with-backoff loop died (or the writer dropped a grant), the part
/// never resolves and the post-grant convergence wait fails. Removing either
/// half cannot pass the other. The discriminator for the per-part answer is
/// the post-grant half's ungranted-parts assertions: under the old
/// all-or-nothing batch answer the ciphertext cannot land before every told
/// part is granted, so granting only the encryption inventory must hang.
///
/// Presence is asserted with the same disk-based helpers as the clone test,
/// never through `get_bytes` — a missing blob read materializes from the
/// active peers and would prove nothing. The one byte read below happens only
/// after presence was already established, and is sanity, not proof.
#[tokio::test(flavor = "multi_thread")]
async fn told_not_cloned_inventory_part_is_refused_until_the_inventory_document_is_granted()
-> Res<()> {
    use big_repo::keyhive_core::access::Access;

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
    let encryption_inventory_doc_id = node_a
        .drawer
        .resolve_doc_id_for_branch_doc_id(node_a.ctx.encryption_inventory_doc_id.clone())
        .await?;

    // One locally-stored plaintext with a `Blob` facet: the encryption worker
    // installs the representation, the pin worker routes its digest into the
    // encrypted-representation inventory.
    let payload = b"told-not-cloned-plaintext".to_vec();
    let plaintext = node_a.blobs_repo.put(&payload).await?;
    let _doc_id = node_a
        .drawer
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(plaintext.clone()),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{plaintext}")]),
                })),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    let ciphertext_pins = wait_for_inventory_pin_ids(
        &node_a,
        &encryption_inventory_doc_id,
        utils_rs::scale_timeout(Duration::from_secs(90)),
    )
    .await?;
    let ciphertext = ciphertext_pins
        .iter()
        .map(|pin| {
            crate::blobs::digest_str_to_blob_id_lenient(pin)
                .ok_or_else(|| eyre::eyre!("inventory pin {pin} is not a blob digest"))
        })
        .collect::<Res<Vec<_>>>()?;
    assert!(
        !ciphertext.is_empty() && !ciphertext.contains(&plaintext),
        "the inventory must name ciphertext, not the plaintext: {ciphertext:?}"
    );

    // The tell: node_b's independent repo names node_a's inventories through
    // config and never clones anything. It boots WITHOUT the blob workers by
    // design: a told config's inventories are the origin's document ids, which
    // no local drawer entry registers here, so the drawer-dialect pin
    // worker's boot ensurer refuses that spawn on a told node the same way it
    // does on the relay test's nodes.
    let node_b = init_told_sync_node(
        &repo_b_path,
        crate::config::AppBlobInventories {
            core_inventory_doc_id: node_a.ctx.core_inventory_doc_id.clone(),
            docs_inventory_doc_id: node_a.ctx.docs_inventory_doc_id.clone(),
            encryption_inventory_doc_id: node_a.ctx.encryption_inventory_doc_id.clone(),
        },
        false,
    )
    .await?;
    assert_eq!(
        &node_b.ctx.encryption_inventory_doc_id, &node_a.ctx.encryption_inventory_doc_id,
        "the told config must name the origin's inventories without any clone"
    );
    assert_ne!(
        node_b.ctx.repo_id, node_a.ctx.repo_id,
        "the two nodes are independent repos"
    );
    let encryption_part =
        crate::blobs::blob_inventory_part_id(&node_b.ctx.encryption_inventory_doc_id);
    info!(
        told_core_part = %crate::blobs::blob_inventory_part_id(&node_a.ctx.core_inventory_doc_id),
        told_docs_part = %crate::blobs::blob_inventory_part_id(&node_a.ctx.docs_inventory_doc_id),
        told_encryption_part = %encryption_part,
        "told part ids"
    );
    assert!(
        node_b.sync_repo.is_blob_part(&encryption_part),
        "the told encryption inventory must classify as a blob part"
    );
    assert!(
        node_b
            .sync_repo
            .peer_partition_ids("", true)
            .contains_key(&encryption_part),
        "the told encryption inventory must be advertised on connect"
    );

    let peer_a = PeerKey::new(*node_a.sync_repo.router.endpoint().id().as_bytes());
    let peer_b = PeerKey::new(*node_b.sync_repo.router.endpoint().id().as_bytes());

    let node_a_ticket = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&node_a_ticket).await?;
    assert_eq!(
        endpoint_addr.id,
        node_a.sync_repo.router.endpoint().id(),
        "node_b must have connected to node_a"
    );
    // The contact-card exchange the grant below needs; no grant exists yet, so
    // nothing of node_a's graph is readable to node_b here.
    drive_keyhive_exchange(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_b, peer_a.clone(), Duration::from_secs(30)).await?;

    // Full sync on the told part only: the never-granted core/docs parts must
    // stay out of the required set, because a told-and-ungranted part is what
    // this test pends forever.
    let sync_repo = Arc::clone(&node_b.sync_repo);
    let waited_peers = [peer_a.clone()];
    let waited_parts = [encryption_part.clone()];
    let synced = sync_repo.wait_for_full_sync(&waited_peers, &waited_parts, None);
    tokio::pin!(synced);

    // Ablation: the refusal is the machine's state, and it is bounded — no
    // ciphertext may land while the part is ungranted.
    info!(
        encryption_part = %encryption_part,
        "ablation: window begins; no grant yet"
    );
    let ablation_window = utils_rs::scale_timeout(Duration::from_secs(12));
    let deadline = tokio::time::Instant::now() + ablation_window;
    let mut saw_unanswered = false;
    let mut saw_blocked_waiter = false;
    while tokio::time::Instant::now() < deadline {
        for cipher in &ciphertext {
            assert!(
                !node_b.blobs_repo.has_blob_on_disk(cipher.clone()).await?,
                "ciphertext {cipher} must not land before the inventory document is granted",
            );
        }
        assert_eq!(
            node_b
                .ctx
                .blob_part_store
                .member_count(encryption_part.clone())
                .await?,
            0,
            "the refused part must stay unmembered before the grant",
        );
        let snapshot = node_b.sync_repo.blob_sync_worker.snapshot().await?;
        saw_unanswered |= snapshot.peer_part_sync_flags.iter().any(
            |(peer, part, _pending, _multi, _replay, _cursor, unanswered)| {
                peer == &peer_a && part == &encryption_part && *unanswered
            },
        );
        saw_blocked_waiter |= snapshot
            .full_sync_waiters
            .values()
            .flatten()
            .any(|(peer, part)| peer == &peer_a && part == &encryption_part);
        tokio::select! {
            _ = &mut synced => {
                eyre::bail!(
                    "the told part must not fully sync before the grant: the ablation \
                     window caught a successful pull of an ungranted part"
                );
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    if !saw_unanswered {
        dump_sync_state(
            &node_b,
            "told node: no refusal observed in the ablation window",
        )
        .await;
        eyre::bail!(
            "the machine must report the told part unanswered within {ablation_window:?} \
             (the PeerPartUnanswered flag)",
        );
    }
    assert!(
        saw_blocked_waiter,
        "the full-sync waiter must be blocked on the refused part while unanswered"
    );

    // The grant: a Read grant on the encryption inventory document ONLY, to
    // node_b's agent — the shape part rows derive from (the closure the
    // permission writer mirrors). Only the encryption inventory, because the
    // old answer folded every unreadable part of a summary batch into one
    // `UnkownParts` error and retried the whole pending batch: a partially
    // granted batch starved the granted part forever. The per-part answer
    // (`big_sync/rpc.rs`) makes exactly this partial grant converge, which is
    // the discriminator the assertions below lean on.
    let agent_b = wait_for_peer_agent(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    info!("granting the origin's encryption inventory document to node_b's agent");
    node_a
        .ctx
        .big_repo
        .grant_doc_access(
            encryption_inventory_doc_id
                .parse::<big_repo::DocumentId>()
                .map_err(|_| eyre::eyre!("inventory doc id is not a document id"))?,
            agent_b.clone(),
            Access::Read,
        )
        .await?;
    // Pull the grants' keyhive events on node_b's own connection so both
    // sides' part rows agree.
    drive_keyhive_exchange(&node_b, peer_a.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    // The permission writer must fold the grant into the origin's part row
    // before the part's next retry can resolve.
    let row_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let rows = inventory_part_rows(
            &node_a.ctx.big_repo.sql_ctx(),
            "daybook-blobs",
            &encryption_part,
        )
        .await?;
        if rows.iter().any(|principal| principal.0 == peer_b) {
            break;
        }
        info!(
            rows = ?rows.iter().map(|(k, v)| (k.to_string(), format!("{v:?}"))).collect::<Vec<_>>(),
            "origin inventory part rows after the grant"
        );
        if tokio::time::Instant::now() >= row_deadline {
            eyre::bail!("node_b never appeared in the origin's inventory part row after the grant");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let granted = tokio::time::timeout(
        utils_rs::scale_timeout(Duration::from_secs(90)),
        &mut synced,
    )
    .await;
    if granted.is_err() {
        dump_sync_state(&node_b, "told node: granted part never fully synced").await;
        dump_sync_state(&node_a, "origin: granted part never fully synced").await;
    }
    granted.map_err(|_| eyre::eyre!("timed out waiting for the granted part to fully sync"))??;

    // Membership convergence: the part's members (the ciphertext objects the
    // pin worker pinned) arrive at node_b.
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(30));
    let origin_count = loop {
        let origin_count = node_a
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        let told_count = node_b
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        if origin_count > 0 && told_count == origin_count {
            break origin_count;
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for the part's members to arrive: origin={origin_count} \
                 told={told_count}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    for cipher in &ciphertext {
        assert!(
            node_b
                .ctx
                .blob_part_store
                .obj_exists(cipher.clone().into())
                .await?,
            "the part's membership must name ciphertext object {cipher}"
        );
        assert!(
            node_b
                .ctx
                .blob_part_store
                .obj_parts(cipher.clone().into())
                .await?
                .contains(&encryption_part),
            "ciphertext object {cipher} must be a member of the told part"
        );
    }

    // Presence, before any byte is read: the bytes have now landed by sync.
    for cipher in &ciphertext {
        wait_for_blob_replicated(&node_b.blobs_repo, cipher.clone(), Duration::from_secs(60))
            .await?;
    }
    // Only now are bytes read — post-presence sanity, not the proof.
    for cipher in &ciphertext {
        let got = node_b.blobs_repo.get_bytes(cipher.clone()).await?;
        assert!(!got.is_empty(), "ciphertext {cipher} must have bytes");
        assert_ne!(
            got, payload,
            "ciphertext {cipher} must be ciphertext, not the plaintext bytes"
        );
    }
    tracing::debug!(
        member_count = origin_count,
        ciphertext = ?ciphertext,
        "granted arc converged"
    );

    // The discriminator: two of the three told parts were never granted, and
    // they must still be refused in the same machine that just landed the
    // granted one. Under the old all-or-nothing summary answer, granting only
    // the encryption inventory hung it forever — this is the starvation being
    // closed. Bounded on both sides: the ungranted parts' bytes may not land
    // within the window and their `unanswered` flags must keep saying so.
    let ungranted_parts = [
        crate::blobs::blob_inventory_part_id(&node_a.ctx.core_inventory_doc_id),
        crate::blobs::blob_inventory_part_id(&node_a.ctx.docs_inventory_doc_id),
    ];
    let still_window = utils_rs::scale_timeout(Duration::from_secs(12));
    let still_deadline = tokio::time::Instant::now() + still_window;
    loop {
        let snapshot = node_b.sync_repo.blob_sync_worker.snapshot().await?;
        let for_a_and_ungranted: Vec<_> = snapshot
            .peer_part_sync_flags
            .iter()
            .filter(
                |(peer, part, _pending, _multi, _replay, _cursor, _unanswered)| {
                    peer == &peer_a && ungranted_parts.contains(part)
                },
            )
            .collect();
        // Held to the FULL window: the refusal is only proven if every
        // ungranted part is still unanswered AFTER the deadline, not merely
        // once a momentary snapshot says so.
        let all_refused = for_a_and_ungranted.len() == ungranted_parts.len()
            && for_a_and_ungranted
                .iter()
                .all(|(.., unanswered)| *unanswered);
        if tokio::time::Instant::now() >= still_deadline {
            dump_sync_state(&node_b, "told node: ungranted parts lost their refusal").await;
            if !all_refused {
                eyre::bail!(
                    "the ungranted told parts must stay refused (answered=false, \
                     unanswered=true) beside the granted one; saw {}/{}/{all_refused}, \
                     snapshot flags: {:?}",
                    for_a_and_ungranted.len(),
                    ungranted_parts.len(),
                    snapshot.peer_part_sync_flags
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for part in &ungranted_parts {
        assert_eq!(
            node_b
                .ctx
                .blob_part_store
                .member_count(part.clone())
                .await?,
            0,
            "the ungranted part {part} must stay unmembered after a partial grant"
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

/// The relay-retention leg of the cipherBlob design (ADR 003 §19, on the
/// fetch plane ADR 013 §4 gave relays): a node told the inventories and
/// granted Read access on the inventory documents ONLY — the digest-naming
/// metadata documents, never a content document, never any group that grants
/// the key document —
///
/// 1. replays the part store derived from the inventory documents (the part
///    identities come from the config-told ids; the relay never projects
///    anything of its own),
/// 2. pulls the ciphertext bytes and stores them,
/// 3. holds no plaintext and no key document,
/// 4. its keyhive never received the key document at all (the typed
///    `DocLookup::Missing` check below), and
/// 5. still re-serves the bytes to a third node after the origin is stopped:
///    retention is exactly "the bytes are useful without the keys".
///
/// The lever is the access-row grant shape of
/// `told_not_cloned_inventory_part_is_refused_until_the_inventory_document_is_granted`
/// applied to the relay: the permission writer copies the inventory document's
/// closure verbatim (ADR 013 §4 keeps relays out of private documents) and the
/// serving side's fetch predicate is `is_fetcher` (`>= Access::Relay`), so a
/// Read-granted inventory row retains exactly like a Relay-granted one — Read
/// is what enables the relay's validation/access on the metadata document; it
/// never decrypts content and never derives pins.
///
/// The drawer-dialect machines (the pin worker authoring BlobPin facets into
/// inventory documents, the encryption worker) are the ORIGIN-side authoring
/// path, so the relay and the consumer boot WITHOUT the blob workers — that
/// is the design, not a dodge: the pin worker's boot ensurer refuses a spawn
/// whose configured inventories are not local drawer branches (asserted as
/// the policy leg of this test), and the silent resolver fallback that once
/// let such a spawn through to a mid-run "headless patch" crash is gone.
#[tokio::test(flavor = "multi_thread")]
async fn relay_granted_only_the_encrypted_inventory_cannot_decrypt_but_retains_and_re_serves_ciphertext()
-> Res<()> {
    use big_repo::keyhive_core::access::Access;

    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b-relay");
    let repo_c_path = temp_root.path().join("repo-c-consumer");

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

    // The origin runs its blob workers: the inventory pins exist only because
    // the production machines derived them.
    let node_a = open_sync_node(&repo_a_path).await?;
    let encryption_inventory_doc_id = node_a
        .drawer
        .resolve_doc_id_for_branch_doc_id(node_a.ctx.encryption_inventory_doc_id.clone())
        .await?;

    // One locally-stored plaintext with a `Blob` facet: the encryption worker
    // installs the representation and derives the inventory pin for it.
    let payload = b"relay-retention-ciphertext-000".to_vec();
    let plaintext = node_a.blobs_repo.put(&payload).await?;
    let doc_id = node_a
        .drawer
        .add(AddDocArgs {
            branch_path: BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(plaintext.clone()),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{plaintext}")]),
                })),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    let ciphertext_pins = wait_for_inventory_pin_ids(
        &node_a,
        &encryption_inventory_doc_id,
        utils_rs::scale_timeout(Duration::from_secs(90)),
    )
    .await?;
    let ciphertext: Vec<BlobId> = ciphertext_pins
        .iter()
        .map(|pin| {
            crate::blobs::digest_str_to_blob_id_lenient(pin)
                .ok_or_else(|| eyre::eyre!("inventory pin {pin} is not a blob digest"))
        })
        .collect::<Res<Vec<_>>>()?;
    assert!(
        !ciphertext.is_empty() && !ciphertext.contains(&plaintext),
        "the inventory must name ciphertext, not the plaintext: {ciphertext:?}"
    );

    // A relay that reads the content document holds this exact facet state
    // (ADR 003 §19: the cipherBlob rides with the Blob facet); the key
    // document it names is where the relay's entitlements stop.
    let origin_doc = node_a
        .drawer
        .get_doc_with_facets_at_branch(&doc_id, BranchPath::new("main"), None)
        .await?
        .ok_or_else(|| eyre::eyre!("the content doc with the Blob facet must exist"))?;
    let cipher = match origin_doc
        .facets
        .iter()
        .find(|(key, _)| key.tag == WellKnownFacetTag::CipherBlob.into())
        .map(|(_, raw)| raw.clone())
    {
        Some(raw) => match WellKnownFacet::from_json(raw, WellKnownFacetTag::CipherBlob)? {
            WellKnownFacet::CipherBlob(cipher) => cipher,
            other => eyre::bail!("expected a cipherBlob facet, got {:?}", other.tag()),
        },
        None => eyre::bail!("the origin content doc must carry a cipherBlob facet"),
    };
    assert!(
        !cipher.key_ref_heads.0.is_empty(),
        "a cross-document keyRef must pin the key state it meant (ADR 003 §19)"
    );
    let key_ref = daybook_types::url::parse_facet_ref(&cipher.key_ref)?;
    eyre::ensure!(
        key_ref.doc_id != daybook_types::url::FACET_SELF_DOC_ID,
        "the key must live in another document (ADR 003 §6, §19)"
    );
    // The key document is staged (`add_temporary`) and committed (`commit_temporary`,
    // the docs.map registration) only after the cipherBlob facet that names it:
    // the pin the inventory carries is derived from the facet, not from the
    // commit, so the key doc's drawer registration can postdate the pins this
    // wait already observed.
    let key_branch_doc_id = {
        let deadline =
            tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(30));
        loop {
            let branch = node_a
                .drawer
                .get_branch_ref(&key_ref.doc_id, &BranchPathBuf::from("main"))
                .await?
                .map(|row| row.branch_doc_id);
            if branch.is_some() {
                break branch.expect("checked above");
            }
            if tokio::time::Instant::now() >= deadline {
                eyre::bail!(
                    "the origin key document ({}) never registered its main branch",
                    key_ref.doc_id
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    // The tell: the relay names node_a's inventories through config and never
    // clones anything. The inventory documents themselves are granted to it
    // with Read (the sibling told test's provisioning route: metadata access
    // for validation — the relay replays the part store derived from them and
    // never reads a private document); the key document is granted to nobody
    // new in this test and is never a grant destination for the relay. The
    // relay boots WITHOUT the blob workers by design: the drawer-dialect pin
    // machines are the origin-side authoring path, and its told inventories
    // have no local drawer branch, so the pin worker's boot ensurer refuses
    // the spawn — asserted as the policy leg below.
    let node_b = init_told_sync_node(
        &repo_b_path,
        crate::config::AppBlobInventories {
            core_inventory_doc_id: node_a.ctx.core_inventory_doc_id.clone(),
            docs_inventory_doc_id: node_a.ctx.docs_inventory_doc_id.clone(),
            encryption_inventory_doc_id: node_a.ctx.encryption_inventory_doc_id.clone(),
        },
        false,
    )
    .await?;
    assert_eq!(
        &node_b.ctx.encryption_inventory_doc_id, &node_a.ctx.encryption_inventory_doc_id,
        "the told config must name the origin's inventories without any clone"
    );
    assert_ne!(
        node_b.ctx.repo_id, node_a.ctx.repo_id,
        "the relay is an independent repo"
    );

    // The policy leg: the drawer-dialect pin worker is the origin-side
    // authoring path, so it must not run on a told/relay config — its
    // configured inventories are the origin's document ids and no local
    // drawer entry registers them here. The boot ensurer turns that from the
    // resolver's old silent fallback (a foreign id spelled straight through
    // until the first nonempty inventory diff crashed mid-run on the headless
    // patch) into a loud boot failure naming the broken config. Asserted on
    // the relay's told config: every slot points at node_a's inventories, so
    // the refused spawn names one of the ids the config cannot serve.
    let pin_worker_spawn = crate::blobs::spawn_blob_pin_worker(crate::blobs::BlobPinWorkerArgs {
        drawer_repo: Arc::clone(&node_b.drawer),
        sql: node_b.ctx.sql.clone(),
        core_inventory_doc_id: node_b.ctx.core_inventory_doc_id.clone(),
        docs_inventory_doc_id: node_b.ctx.docs_inventory_doc_id.clone(),
        encryption_inventory_doc_id: node_b.ctx.encryption_inventory_doc_id.clone(),
        blobs_repo: Arc::clone(&node_b.blobs_repo),
        facet_set_store: node_b.rt.doc_facet_set_index_repo.revision_store(),
        plugs_repo: Arc::clone(&node_b.plugs_repo),
        parent_cancel_token: tokio_util::sync::CancellationToken::new(),
    })
    .await;
    let refusal = match pin_worker_spawn {
        Ok(_) => panic!(
            "the pin worker must not spawn on a relay-told config whose inventories \
             have no local drawer branch",
        ),
        Err(err) => format!("{err:#}"),
    };
    assert!(
        refusal.contains("not a local drawer branch")
            && [
                &node_a.ctx.core_inventory_doc_id,
                &node_a.ctx.docs_inventory_doc_id,
                &node_a.ctx.encryption_inventory_doc_id,
            ]
            .iter()
            .any(|told_inventory| refusal.contains(told_inventory.to_string().as_str())),
        "the boot ensurer must name the told config's unreachable inventory: {refusal}"
    );

    let encryption_part =
        crate::blobs::blob_inventory_part_id(&node_b.ctx.encryption_inventory_doc_id);
    assert!(
        node_b.sync_repo.is_blob_part(&encryption_part),
        "the told encryption inventory must classify as a blob part"
    );
    assert!(
        node_b
            .sync_repo
            .peer_partition_ids("", true)
            .contains_key(&encryption_part),
        "the told encryption inventory must be advertised on connect"
    );

    let peer_a = PeerKey::new(*node_a.sync_repo.router.endpoint().id().as_bytes());
    let peer_b = PeerKey::new(*node_b.sync_repo.router.endpoint().id().as_bytes());

    let node_a_ticket = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&node_a_ticket).await?;
    assert_eq!(
        endpoint_addr.id,
        node_a.sync_repo.router.endpoint().id(),
        "the relay must have connected to node_a"
    );
    drive_keyhive_exchange(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_b, peer_a.clone(), Duration::from_secs(30)).await?;

    let agent_b = wait_for_peer_agent(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    node_a
        .ctx
        .big_repo
        .grant_doc_access(
            encryption_inventory_doc_id
                .parse::<big_repo::DocumentId>()
                .map_err(|_| eyre::eyre!("inventory doc id is not a document id"))?,
            agent_b,
            Access::Read,
        )
        .await?;
    drive_keyhive_exchange(&node_b, peer_a.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    wait_for_part_row_including(
        &node_a,
        &encryption_part,
        &peer_b,
        "the relay must be admitted to the origin's inventory part row after the Read grant",
        Duration::from_secs(30),
    )
    .await?;

    node_b
        .sync_repo
        .wait_for_full_sync(
            std::slice::from_ref(&peer_a),
            std::slice::from_ref(&encryption_part),
            None,
        )
        .await?;

    // Membership convergence: the part's members — the ciphertext objects the
    // pin worker pinned, and nothing else — arrive at the relay.
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(30));
    let origin_count = loop {
        let origin_count = node_a
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        let told_count = node_b
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        if origin_count > 0 && told_count == origin_count {
            break origin_count;
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for the part's members to arrive at the relay: \
                 origin={origin_count} relay={told_count}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(
        origin_count,
        ciphertext.len() as u64,
        "the encryption part must carry exactly the ciphertext objects"
    );
    for c in &ciphertext {
        assert!(
            node_b
                .ctx
                .blob_part_store
                .obj_exists(c.clone().into())
                .await?,
            "the part's membership must name ciphertext object {c}"
        );
    }
    // No plaintext: the plaintext belongs to the ungranted parts, and the
    // granted part carries only the ciphertext. The disk check is the
    // end-user form of the claim; the part form is the structural one.
    assert!(
        !node_b
            .ctx
            .blob_part_store
            .obj_exists(plaintext.clone().into())
            .await?,
        "the plaintext must not be a member of any part the relay holds"
    );
    assert!(
        !node_b
            .blobs_repo
            .has_blob_on_disk(plaintext.clone())
            .await?,
        "the relay must hold no plaintext bytes"
    );

    // Presence, before any byte is read — the bytes must have landed by sync
    // (an on-demand read would fetch from the active peers and prove nothing).
    for c in &ciphertext {
        wait_for_blob_replicated(
            &node_b.blobs_repo,
            c.clone(),
            utils_rs::scale_timeout(Duration::from_secs(60)),
        )
        .await?;
    }
    for c in &ciphertext {
        let got = node_b.blobs_repo.get_bytes(c.clone()).await?;
        assert!(!got.is_empty(), "ciphertext {c} must have bytes");
        assert_ne!(
            got, payload,
            "ciphertext {c} must be ciphertext, not the plaintext bytes"
        );
    }

    // The typed leg of the keyless claim: the relay's keyhive never received
    // the key document, so there is nothing to read it against (no grant, no
    // clone, no group membership).
    let key_doc_on_relay = node_b.ctx.big_repo.get_doc(&key_branch_doc_id).await?;
    assert!(
        matches!(key_doc_on_relay, big_repo::DocLookup::Missing),
        "the relay must never have received the key document (ADR 003 §19)"
    );

    // Retention: a third node, told the same inventories, receives the
    // ciphertext from the relay ALONE. The origin learns the third node's
    // Keyhive agent in-process through its contact card (the way `big_repo`'s
    // relay-topology tests propagate identity through a relay), but repo_c is
    // never connected to node_a — and node_a is stopped before repo_c's pull,
    // so no byte can reach it from anywhere but the relay's retained copy.
    // Like the relay itself, repo_c boots without the blob workers by design:
    // the drawer-dialect pin machines are the origin-side authoring path, and
    // its told inventories have no local drawer branch, so the pin worker's
    // boot ensurer refuses the spawn (the policy leg above asserts the relay
    // side of this).
    let node_c = init_told_sync_node(
        &repo_c_path,
        crate::config::AppBlobInventories {
            core_inventory_doc_id: node_a.ctx.core_inventory_doc_id.clone(),
            docs_inventory_doc_id: node_a.ctx.docs_inventory_doc_id.clone(),
            encryption_inventory_doc_id: node_a.ctx.encryption_inventory_doc_id.clone(),
        },
        false,
    )
    .await?;
    let peer_c = PeerKey::new(*node_c.sync_repo.router.endpoint().id().as_bytes());
    let agent_c = node_a
        .ctx
        .big_repo
        .receive_keyhive_contact_card(&node_c.ctx.big_repo.local_keyhive_contact_card())
        .await?;
    node_a
        .ctx
        .big_repo
        .grant_doc_access(
            encryption_inventory_doc_id
                .parse::<big_repo::DocumentId>()
                .map_err(|_| eyre::eyre!("inventory doc id is not a document id"))?,
            agent_c,
            Access::Read,
        )
        .await?;
    // The grant delta reaches the relay before it is asked to serve: the
    // permission writer mirrors the document's readers the relay can see.
    drive_keyhive_exchange(&node_b, peer_a.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_a, peer_b.clone(), Duration::from_secs(30)).await?;
    wait_for_part_row_including(
        &node_b,
        &encryption_part,
        &peer_c,
        "the relay's part row must admit the third node — the grant is visible \
         to the relay through the document",
        Duration::from_secs(30),
    )
    .await?;

    // The origin goes away; the retained bytes are all that is left.
    node_a.stop().await?;

    let node_b_ticket = node_b.sync_repo.get_clone_ticket_url().await?;
    let consumer_addr = node_c.sync_repo.connect_url(&node_b_ticket).await?;
    assert_eq!(
        consumer_addr.id,
        node_b.sync_repo.router.endpoint().id(),
        "the consumer must have connected to the relay"
    );
    drive_keyhive_exchange(&node_c, peer_b.clone(), Duration::from_secs(30)).await?;
    drive_keyhive_exchange(&node_b, peer_c.clone(), Duration::from_secs(30)).await?;

    node_c
        .sync_repo
        .wait_for_full_sync(
            std::slice::from_ref(&peer_b),
            std::slice::from_ref(&encryption_part),
            None,
        )
        .await?;
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(30));
    loop {
        let relay_count = node_b
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        let consumer_count = node_c
            .ctx
            .blob_part_store
            .member_count(encryption_part.clone())
            .await?;
        if relay_count > 0 && consumer_count == relay_count {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out waiting for the part's members to arrive from the relay: \
                 relay={relay_count} consumer={consumer_count}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for c in &ciphertext {
        wait_for_blob_replicated(
            &node_c.blobs_repo,
            c.clone(),
            utils_rs::scale_timeout(Duration::from_secs(60)),
        )
        .await?;
        let got = node_c.blobs_repo.get_bytes(c.clone()).await?;
        assert!(
            !got.is_empty(),
            "the relay-served ciphertext {c} must have bytes"
        );
        assert_ne!(
            got, payload,
            "the relay-served ciphertext {c} must still be ciphertext, not the plaintext"
        );
    }
    assert!(
        !node_c
            .blobs_repo
            .has_blob_on_disk(plaintext.clone())
            .await?,
        "the consumer must hold no plaintext, either"
    );

    node_c.stop().await?;
    node_b.stop().await?;
    Ok(())
}

/// Wait until `part`'s rows on a node's own serving store admit `peer` — the
/// same question the told-node test asks the origin after a grant, with the
/// node parameterised so a serving relay's own rows can be polled too.
async fn wait_for_part_row_including(
    node: &SyncTestNode,
    part: &PartKey,
    peer: &PeerKey,
    what: &str,
    timeout: Duration,
) -> Res<()> {
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(timeout);
    loop {
        let rows = inventory_part_rows(&node.ctx.big_repo.sql_ctx(), "daybook-blobs", part).await?;
        if rows.iter().any(|principal| principal.0 == *peer) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            eyre::bail!(
                "timed out: {what}; rows = {:?}",
                rows.iter().map(|(k, _)| k.to_string()).collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
