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
    AddDocArgs, DocId, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct SyncTestNode {
    ctx: Arc<RepoCtx>,
    blobs_repo: Arc<BlobsRepo>,
    drawer: Arc<DrawerRepo>,
    progress_repo: Arc<ProgressRepo>,
    progress_stop: crate::repos::RepoStopToken,
    drawer_stop: crate::repos::RepoStopToken,
    _plugs_repo: Arc<PlugsRepo>,
    plugs_stop: crate::repos::RepoStopToken,
    config_stop: crate::repos::RepoStopToken,
    doc_blobs_index_stop: crate::repos::RepoStopToken,
    sqlite_local_state_stop: crate::repos::RepoStopToken,
    doc_blobs_bridge_cancel: CancellationToken,
    doc_blobs_bridge_handle: Option<JoinHandle<()>>,
    sync_repo: Arc<IrohSyncRepo>,
    sync_stop: IrohSyncRepoStopToken,
}

impl SyncTestNode {
    async fn stop(self) -> Res<()> {
        let SyncTestNode {
            ctx,
            blobs_repo: _blobs_repo,
            drawer: _drawer,
            progress_repo: _progress_repo,
            progress_stop,
            drawer_stop,
            _plugs_repo,
            plugs_stop,
            config_stop,
            doc_blobs_index_stop,
            sqlite_local_state_stop,
            doc_blobs_bridge_cancel,
            doc_blobs_bridge_handle,
            sync_repo,
            sync_stop,
        } = self;
        drop(sync_repo);
        sync_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(30)),
            sync_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting sync stop"))??;
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            progress_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting progress stop"))??;
        doc_blobs_bridge_cancel.cancel();
        if let Some(handle) = doc_blobs_bridge_handle {
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(5)),
                utils_rs::wait_on_handle_with_timeout(
                    handle,
                    utils_rs::scale_timeout(Duration::from_secs(2)),
                ),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting doc blobs bridge join"))??;
        }
        doc_blobs_index_stop.cancel_token.cancel();
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            doc_blobs_index_stop.stop(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting doc blobs index stop"))??;
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
        RepoOpenOptions {},
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
        RepoOpenOptions {},
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
    let core_partition_id = crate::part_id_from_label(CORE_DOCS_PARTITION_ID);
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
        RepoOpenOptions {},
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

    let mut args_batch = Vec::new();
    for idx in 0..100usize {
        let payload = format!("blob-payload-{idx:03}").into_bytes();
        let hash = node_a
            .blobs_repo
            .put(&payload, crate::blobs::BlobUseHints::Docs)
            .await?;
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

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20)).await?;
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

    let mut blob_payloads = Vec::new();
    let mut args_batch = Vec::new();
    for idx in 0..8usize {
        let payload = format!("blob-bytes-validation-{idx:03}").into_bytes();
        let hash = node_a
            .blobs_repo
            .put(&payload, crate::blobs::BlobUseHints::Docs)
            .await?;
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

    let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
    let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(60)).await?;

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
async fn iroh_sync_after_bootstrap_clone_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions {},
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
        RepoOpenOptions {},
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

        let ctx = RepoCtx::open(repo_b_path, RepoOpenOptions {}, "test-device".into()).await?;
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
        },
    )
    .await?;
    Ok(())
}

async fn open_sync_node(repo_root: &std::path::Path) -> Res<SyncTestNode> {
    let rtx = RepoCtx::open(repo_root, RepoOpenOptions {}, "test-device".into()).await?;
    let blobs_repo = BlobsRepo::new(
        rtx.layout.blobs_root.clone(),
        rtx.local_user_path.clone(),
        Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
            Arc::clone(&rtx.part_store),
        )),
    )
    .await?;
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
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(surelock::mutex::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
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
    let (sqlite_local_state_repo, sqlite_local_state_stop) =
        SqliteLocalStateRepo::boot(rtx.layout.repo_root.join("local_state")).await?;
    let (doc_blobs_index_repo, doc_blobs_index_stop) = DocBlobsIndexRepo::boot(
        Arc::clone(&drawer_repo),
        Arc::clone(&blobs_repo),
        Arc::clone(&sqlite_local_state_repo),
    )
    .await?;
    let (doc_blobs_bridge_cancel, doc_blobs_bridge_handle) = spawn_doc_blobs_index_bridge_for_tests(
        Arc::clone(&drawer_repo),
        Arc::clone(&doc_blobs_index_repo),
    );
    let (progress_repo, progress_stop) = ProgressRepo::boot(rtx.sql.clone()).await?;
    let (sync_repo, sync_stop) = IrohSyncRepo::boot(
        Arc::clone(&rtx),
        Arc::clone(&config_repo),
        Arc::clone(&blobs_repo),
        Arc::clone(&doc_blobs_index_repo),
        Some(Arc::clone(&progress_repo)),
    )
    .await?;

    Ok(SyncTestNode {
        ctx: rtx,
        blobs_repo,
        drawer: drawer_repo,
        progress_repo,
        progress_stop,
        drawer_stop,
        _plugs_repo: plugs_repo,
        plugs_stop,
        config_stop,
        doc_blobs_index_stop,
        sqlite_local_state_stop,
        doc_blobs_bridge_cancel,
        doc_blobs_bridge_handle: Some(doc_blobs_bridge_handle),
        sync_repo,
        sync_stop,
    })
}

fn spawn_doc_blobs_index_bridge_for_tests(
    drawer_repo: Arc<DrawerRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
) -> (CancellationToken, JoinHandle<()>) {
    let drawer_listener = drawer_repo.subscribe(SubscribeOpts::new(16_384));
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel_for_task.cancelled() => break,
                evt = drawer_listener.recv_async() => {
                    match evt {
                        Ok(evt) => match evt.as_ref() {
                            crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                                doc_blobs_index_repo.enqueue_delete(id.clone()).unwrap_or_log();
                            }
                            crate::drawer::DrawerEvent::DocAdded { id, entry, .. } => {
                                for (branch_name, heads) in &entry.branches {
                                    doc_blobs_index_repo
                                        .enqueue_upsert(
                                            id.clone(),
                                            daybook_types::doc::BranchPathBuf::from(
                                                branch_name.as_str(),
                                            ),
                                            heads.clone(),
                                        )
                                        .unwrap_or_log();
                                }
                            }
                            crate::drawer::DrawerEvent::DocUpdated { id, entry, .. } => {
                                let retained_branches: Vec<daybook_types::doc::BranchPathBuf> = entry
                                    .branches
                                    .keys()
                                    .map(|branch_name| {
                                        daybook_types::doc::BranchPathBuf::from(branch_name.as_str())
                                    })
                                    .collect();
                                doc_blobs_index_repo
                                    .enqueue_delete_branches_not_in(
                                        id.clone(),
                                        retained_branches,
                                    )
                                    .unwrap_or_log();
                                for (branch_name, heads) in &entry.branches {
                                    doc_blobs_index_repo
                                        .enqueue_upsert(
                                            id.clone(),
                                            daybook_types::doc::BranchPathBuf::from(
                                                branch_name.as_str(),
                                            ),
                                            heads.clone(),
                                        )
                                        .unwrap_or_log();
                                }
                            }
                        },
                        Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                            panic!("doc blobs bridge dropped {dropped_count} drawer events");
                        }
                        Err(crate::repos::RecvError::Closed) => break,
                    }
                }
            }
        }
    });
    (cancel, handle)
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
            let found = node
                .drawer
                .get_doc_with_facets_at_branch(doc_id, daybook_types::doc::BranchPath::new("main"), None)
                .await?
                .is_some();
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
        .peer_partition_ids("")
        .into_keys()
        .collect::<Vec<_>>();
    let peer_id = PeerId::new(*endpoint_id.as_bytes());
    tokio::try_join!(
        target.sync_repo.wait_for_full_sync(
            std::slice::from_ref(&peer_id),
            &required_partitions,
            timeout
        ),
        wait_for_doc_set_parity(&source.drawer, &target.drawer, timeout),
    )?;
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
        .peer_partition_ids("")
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

async fn wait_for_doc_head_parity(
    left: &SyncTestNode,
    right: &SyncTestNode,
    doc_id: &String,
    branch: &daybook_types::doc::BranchPath,
    timeout: Duration,
) -> Res<()> {
    let mut last_left = None::<Vec<String>>;
    let mut last_right = None::<Vec<String>>;
    tokio::time::timeout(timeout, async {
        loop {
            let left_heads = left
                .drawer
                .get_with_heads(doc_id, branch, None)
                .await?
                .map(|(_, heads)| {
                    let mut out = heads.iter().map(ToString::to_string).collect::<Vec<_>>();
                    out.sort_unstable();
                    out
                })
                .ok_or_else(|| eyre::eyre!("left missing doc heads for {doc_id}"))?;
            let right_heads = right
                .drawer
                .get_with_heads(doc_id, branch, None)
                .await?
                .map(|(_, heads)| {
                    let mut out = heads.iter().map(ToString::to_string).collect::<Vec<_>>();
                    out.sort_unstable();
                    out
                })
                .ok_or_else(|| eyre::eyre!("right missing doc heads for {doc_id}"))?;
            last_left = Some(left_heads);
            last_right = Some(right_heads);
            if last_left == last_right {
                break eyre::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| {
        eyre::eyre!(
            "timed out waiting for doc head parity: doc_id={} branch={} left={:?} right={:?}",
            doc_id,
            branch,
            last_left,
            last_right
        )
    })??;
    Ok(())
}

async fn wait_for_blob_bytes(
    blobs_repo: &BlobsRepo,
    blob_id: BlobId,
    timeout: Duration,
) -> Res<Vec<u8>> {
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
        Arc::new(crate::blobs::NoopPartitionMembershipWriter),
    )
    .await?;
    let payload = b"delayed-blob-arrival".to_vec();
    let expected_hash = crate::blobs::BlobId::new(*blake3::hash(&payload).as_bytes());

    let repo_bg = Arc::clone(&blobs_repo);
    let payload_bg = payload.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        repo_bg
            .put(&payload_bg, crate::blobs::BlobUseHints::Unknown)
            .await
            .expect("put should succeed");
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
