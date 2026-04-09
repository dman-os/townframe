use crate::interlude::*;

use crate::drawer::{
    cache::FacetCacheState,
    facet_recovery,
    lru::KeyedLruPool,
    types::{DocEntry, DrawerEvent, StoredBranchRef, UpdateDocArgsV2},
    DrawerRepo,
};
use crate::repos::Repo;

use daybook_types::doc::{AddDocArgs, ChangeHashSet, DocId, DocPatch, FacetKey};

use std::str::FromStr;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use daybook_types::doc::{Body, UserPath, WellKnownFacet, WellKnownFacetTag};
use daybook_types::url::build_facet_ref;

async fn get_dmeta_on_main(repo: &DrawerRepo, doc_id: &DocId) -> Res<daybook_types::doc::Dmeta> {
    let dmeta_key = FacetKey::from(WellKnownFacetTag::Dmeta);
    let doc = repo
        .get_doc_with_facets_at_branch(doc_id, &"main".into(), Some(vec![dmeta_key.clone()]))
        .await?
        .ok_or_eyre("doc not found when loading dmeta")?;
    let dmeta = doc
        .facets
        .get(&dmeta_key)
        .ok_or_eyre("dmeta facet missing")?;
    let dmeta = match serde_json::from_value::<WellKnownFacet>(dmeta.clone())? {
        WellKnownFacet::Dmeta(dmeta) => dmeta,
        other => eyre::bail!("expected dmeta facet, got {:?}", other.tag()),
    };
    Ok(dmeta)
}

fn local_branch(name: &str) -> daybook_types::doc::BranchPath {
    daybook_types::doc::BranchPath::from(format!("/test-device/{name}"))
}

async fn new_meta_db_pool() -> Res<sqlx::SqlitePool> {
    Ok(crate::app::SqlCtx::new("sqlite::memory:").await?.db_pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_smoke() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    // 1. Add doc
    let facet_title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title_key.clone(),
                WellKnownFacet::TitleGeneric("Initial".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    // 2. List docs
    let list = repo.list().await?;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].doc_id, doc_id);
    assert!(list[0].branches.contains_key("main"));

    // 3. Get doc
    let doc = repo
        .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc.facets.get(&facet_title_key).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Initial".into()))
    );

    // 4. Update doc
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title_key.clone(),
                WellKnownFacet::TitleGeneric("Updated".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        "main".into(),
        None,
    )
    .await?;

    let doc = repo
        .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc.facets.get(&facet_title_key).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Updated".into()))
    );

    // 5. Delete doc
    assert!(repo.del(&doc_id).await?);
    let list = repo.list().await?;
    assert_eq!(list.len(), 0);

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partitions_track_non_tmp_branches() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-partitions".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let partition_id = repo.replicated_partition_id();
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);

    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Initial".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    assert_eq!(big_repo.partition_member_count(&partition_id).await?, 1);

    let main_heads = repo
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing doc branches after add")?
        .branches
        .get("main")
        .ok_or_eyre("missing main branch")?
        .clone();

    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &daybook_types::doc::BranchPath::from("/tmp/job-1"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Tmp branch update".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        daybook_types::doc::BranchPath::from("/tmp/job-1"),
        Some(main_heads.clone()),
    )
    .await?;
    assert_eq!(big_repo.partition_member_count(&partition_id).await?, 1);

    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Replicated branch update".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(main_heads),
    )
    .await?;
    assert_eq!(big_repo.partition_member_count(&partition_id).await?, 2);

    assert!(
        repo.delete_branch(&doc_id, &local_branch("branch-a"), None)
            .await?
    );
    assert_eq!(big_repo.partition_member_count(&partition_id).await?, 1);

    assert!(repo.del(&doc_id).await?);
    assert_eq!(big_repo.partition_member_count(&partition_id).await?, 0);

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_batch_add_smoke() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-batch-add".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let created_ids = repo
        .batch_add(vec![
            AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("First".into()).into(),
                )]
                .into(),
                user_path: None,
            },
            AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Second".into()).into(),
                )]
                .into(),
                user_path: None,
            },
        ])
        .await?;

    assert_eq!(created_ids.len(), 2);
    assert_ne!(created_ids[0], created_ids[1]);

    let list = repo.list().await?;
    assert_eq!(list.len(), 2);
    let listed_ids: HashSet<DocId> = list.into_iter().map(|item| item.doc_id).collect();
    assert!(listed_ids.contains(&created_ids[0]));
    assert!(listed_ids.contains(&created_ids[1]));

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_batch_add_emits_single_list_changed() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-batch-add-events".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let listener = repo.subscribe(crate::repos::SubscribeOpts::new(64));
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let created_ids = repo
        .batch_add(vec![
            AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Alpha".into()).into(),
                )]
                .into(),
                user_path: None,
            },
            AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Beta".into()).into(),
                )]
                .into(),
                user_path: None,
            },
            AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Gamma".into()).into(),
                )]
                .into(),
                user_path: None,
            },
        ])
        .await?;

    let mut events = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            listener.recv_lossy_async(),
        )
        .await
        .wrap_err("timeout waiting for drawer event")?
        .map_err(|_| eyre::eyre!("listener closed"))?;
        events.push(event);
    }

    let mut added_ids = HashSet::new();
    let mut doc_added_heads = Vec::new();
    for event in events {
        match &*event {
            DrawerEvent::DocAdded {
                id, drawer_heads, ..
            } => {
                added_ids.insert(id.clone());
                doc_added_heads.push(drawer_heads.clone());
            }
            other => eyre::bail!("unexpected event: {other:?}"),
        }
    }

    assert_eq!(added_ids.len(), created_ids.len());
    for created_id in created_ids {
        assert!(added_ids.contains(&created_id));
    }
    assert_eq!(doc_added_heads.len(), 3);
    assert!(doc_added_heads.windows(2).all(|pair| pair[0] == pair[1]));

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_merge() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-merge".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let facet_note = FacetKey::from(WellKnownFacetTag::Note);

    // 1. Add doc on main
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Base".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let main_heads = entry.branches.get("main").unwrap().clone();

    // 2. Update branch-a
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("A".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(main_heads.clone()),
    )
    .await?;

    // 3. Update branch-b
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-b"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(facet_note.clone(), WellKnownFacet::Note("B".into()).into())].into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-b"),
        Some(main_heads.clone()),
    )
    .await?;

    // 4. Merge branch-a to main
    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let a_heads = entry
        .branches
        .get(&*local_branch("branch-a").to_string())
        .unwrap()
        .clone();
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-a"),
        &a_heads,
        None,
    )
    .await?;

    // 5. Merge branch-b to main
    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let b_heads = entry
        .branches
        .get(&*local_branch("branch-b").to_string())
        .unwrap()
        .clone();
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-b"),
        &b_heads,
        None,
    )
    .await?;

    // 6. Verify merge
    let doc = repo
        .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc.facets.get(&facet_title).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("A".into()))
    );
    assert_eq!(
        doc.facets.get(&facet_note).unwrap(),
        &serde_json::Value::from(WellKnownFacet::Note("B".into()))
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resolve_handle_for_heads_does_not_match_foreign_doc_heads() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-resolve-handle-heads".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let doc_a = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note("A".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    let doc_b = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note("B".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let doc_a_heads = repo
        .get_doc_branches(&doc_a)
        .await?
        .ok_or_eyre("doc_a missing")?
        .branches
        .get("main")
        .cloned()
        .ok_or_eyre("doc_a main missing")?;

    let resolved = repo
        .resolve_handle_for_branch_heads(&doc_b, &"main".into(), &doc_a_heads)
        .await?;
    assert!(
        resolved.is_none(),
        "foreign heads must not resolve to another doc handle"
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_branch_at_stale_main_heads_after_intervening_merges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-stale-heads-after-merges".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let facet_note = FacetKey::from(WellKnownFacetTag::Note);

    for round in 0..32 {
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_note.clone(),
                    WellKnownFacet::Note(format!("init-{round}").into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let base_heads = repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("missing doc branches after add")?
            .branches
            .get("main")
            .ok_or_eyre("missing main branch after add")?
            .clone();

        let branch_a = daybook_types::doc::BranchPath::from(format!("/tmp/stale-a-{round}"));
        repo.create_branch_at_heads_from_branch(
            &doc_id,
            &branch_a,
            &"main".into(),
            &base_heads,
            None,
        )
        .await?;
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric(format!("A-{round}")).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            branch_a.clone(),
            Some(base_heads.clone()),
        )
        .await?;
        let a_heads = repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("missing doc branches after branch-a update")?
            .branches
            .get(&branch_a.to_string())
            .ok_or_eyre("missing branch-a state after update")?
            .clone();
        repo.merge_from_heads(&doc_id, &"main".into(), &branch_a, &a_heads, None)
            .await?;
        let heads_after_a = repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("missing doc branches after merge a")?
            .branches
            .get("main")
            .ok_or_eyre("missing main after merge a")?
            .clone();

        let branch_b = daybook_types::doc::BranchPath::from(format!("/tmp/stale-b-{round}"));
        repo.create_branch_at_heads_from_branch(
            &doc_id,
            &branch_b,
            &"main".into(),
            &heads_after_a,
            None,
        )
        .await?;
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_note.clone(),
                    WellKnownFacet::Note(format!("B-{round}").into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            branch_b.clone(),
            Some(heads_after_a.clone()),
        )
        .await?;
        let b_heads = repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("missing doc branches after branch-b update")?
            .branches
            .get(&branch_b.to_string())
            .ok_or_eyre("missing branch-b state after update")?
            .clone();
        repo.merge_from_heads(&doc_id, &"main".into(), &branch_b, &b_heads, None)
            .await?;

        // Recreate the stale-heads path: materialize a new branch from an older main head set
        // after main has already advanced through an intervening merge.
        let stale_branch = daybook_types::doc::BranchPath::from(format!("/tmp/stale-c-{round}"));
        repo.create_branch_at_heads_from_branch(
            &doc_id,
            &stale_branch,
            &"main".into(),
            &heads_after_a,
            None,
        )
        .await?;
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric(format!("C-{round}")).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            stale_branch.clone(),
            Some(heads_after_a.clone()),
        )
        .await?;
        let stale_heads = repo
            .get_doc_branches(&doc_id)
            .await?
            .ok_or_eyre("missing doc branches after stale branch update")?
            .branches
            .get(&stale_branch.to_string())
            .ok_or_eyre("missing stale branch state after update")?
            .clone();
        repo.merge_from_heads(&doc_id, &"main".into(), &stale_branch, &stale_heads, None)
            .await?;

        assert!(
            repo.del(&doc_id).await?,
            "doc should be removable after round"
        );
    }

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[test]
fn test_raw_automerge_fork_at_stale_heads_after_merges() -> Res<()> {
    let mut main = automerge::Automerge::new();
    {
        let mut tx = main.transaction();
        tx.put(automerge::ROOT, "note", "init")?;
        tx.commit();
    }
    let h0 = main.get_heads();

    let mut branch_a = main.fork_at(&h0)?;
    {
        let mut tx = branch_a.transaction();
        tx.put(automerge::ROOT, "title", "A")?;
        tx.commit();
    }
    main.merge(&mut branch_a)?;
    let h1 = main.get_heads();
    assert_ne!(h1, h0, "main should advance after merge a");

    let mut branch_b = main.fork_at(&h1)?;
    {
        let mut tx = branch_b.transaction();
        tx.put(automerge::ROOT, "note", "B")?;
        tx.commit();
    }
    main.merge(&mut branch_b)?;
    let h2 = main.get_heads();
    assert_ne!(h2, h1, "main should advance after merge b");

    let mut stale = main.fork_at(&h1)?;
    {
        let mut tx = stale.transaction();
        tx.put(automerge::ROOT, "title", "C")?;
        tx.commit();
    }
    let stale_heads = stale.get_heads();
    assert!(
        !stale_heads.is_empty(),
        "fork_at on older known heads should produce a valid branch"
    );
    Ok(())
}

#[test]
fn test_raw_automerge_merge_and_log_then_commit_preserves_stale_forkability() -> Res<()> {
    let mut main = automerge::Automerge::new();
    {
        let mut tx = main.transaction();
        tx.put(automerge::ROOT, "note", "init")?;
        tx.commit();
    }
    let h0 = main.get_heads();

    let mut branch_a = main.fork_at(&h0)?;
    {
        let mut tx = branch_a.transaction();
        tx.put(automerge::ROOT, "title", "A")?;
        tx.commit();
    }
    {
        let mut patch_log = automerge::PatchLog::active();
        main.merge_and_log_patches(&mut branch_a, &mut patch_log)?;
        let _patches = main.make_patches(&mut patch_log);
        let mut tx = main.transaction();
        tx.put(automerge::ROOT, "meta_a", "ok")?;
        tx.commit();
    }
    let h1 = main.get_heads();

    let mut branch_b = main.fork_at(&h1)?;
    {
        let mut tx = branch_b.transaction();
        tx.put(automerge::ROOT, "note", "B")?;
        tx.commit();
    }
    {
        let mut patch_log = automerge::PatchLog::active();
        main.merge_and_log_patches(&mut branch_b, &mut patch_log)?;
        let _patches = main.make_patches(&mut patch_log);
        let mut tx = main.transaction();
        tx.put(automerge::ROOT, "meta_b", "ok")?;
        tx.commit();
    }

    let stale = main.fork_at(&h1)?;
    assert!(
        !stale.get_heads().is_empty(),
        "stale fork_at should keep working after merge_and_log_patches + follow-up commit"
    );
    Ok(())
}

#[test]
fn test_raw_automerge_merge_and_log_make_patches_without_followup_commit_stale_forkability(
) -> Res<()> {
    let mut main = automerge::Automerge::new();
    {
        let mut tx = main.transaction();
        tx.put(automerge::ROOT, "note", "init")?;
        tx.commit();
    }
    let h0 = main.get_heads();

    let mut branch_a = main.fork_at(&h0)?;
    {
        let mut tx = branch_a.transaction();
        tx.put(automerge::ROOT, "title", "A")?;
        tx.commit();
    }
    {
        let mut patch_log = automerge::PatchLog::active();
        main.merge_and_log_patches(&mut branch_a, &mut patch_log)?;
        let _patches = main.make_patches(&mut patch_log);
    }
    let h1 = main.get_heads();

    let mut branch_b = main.fork_at(&h1)?;
    {
        let mut tx = branch_b.transaction();
        tx.put(automerge::ROOT, "note", "B")?;
        tx.commit();
    }
    {
        let mut patch_log = automerge::PatchLog::active();
        main.merge_and_log_patches(&mut branch_b, &mut patch_log)?;
        let _patches = main.make_patches(&mut patch_log);
    }

    let stale = main.fork_at(&h1)?;
    assert!(!stale.get_heads().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bigrepo_raw_automerge_stale_heads_after_merges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-bigrepo-stale-heads".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let main_handle = big_repo.create_doc(automerge::Automerge::new()).await?;
    main_handle
        .with_document_local(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "note", "init")?;
            tx.commit();
            eyre::Ok(())
        })
        .await??;
    let h0 = main_handle
        .with_document_local(|doc| doc.get_heads())
        .await?;

    let branch_a_doc = main_handle
        .with_document_local(|doc| doc.fork_at(&h0))
        .await??;
    let branch_a_handle = big_repo.create_doc(branch_a_doc).await?;
    branch_a_handle
        .with_document_local(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "title", "A")?;
            tx.commit();
            eyre::Ok(())
        })
        .await??;
    let mut a_snapshot = branch_a_handle
        .with_document_local(|doc| doc.clone())
        .await?;
    main_handle
        .with_document_local(|doc| doc.merge(&mut a_snapshot))
        .await??;
    let h1 = main_handle
        .with_document_local(|doc| doc.get_heads())
        .await?;

    let branch_b_doc = main_handle
        .with_document_local(|doc| doc.fork_at(&h1))
        .await??;
    let branch_b_handle = big_repo.create_doc(branch_b_doc).await?;
    branch_b_handle
        .with_document_local(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "note", "B")?;
            tx.commit();
            eyre::Ok(())
        })
        .await??;
    let mut b_snapshot = branch_b_handle
        .with_document_local(|doc| doc.clone())
        .await?;
    main_handle
        .with_document_local(|doc| doc.merge(&mut b_snapshot))
        .await??;
    let h2 = main_handle
        .with_document_local(|doc| doc.get_heads())
        .await?;
    assert_ne!(h2, h1, "main should advance after second merge");

    let stale = main_handle
        .with_document_local(|doc| doc.fork_at(&h1))
        .await??;
    assert!(
        !stale.get_heads().is_empty(),
        "fork_at on older known heads should work through BigRepo handle path"
    );

    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_sync_smoke() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (client_acx, client_acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "client".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;
    let (server_acx, server_acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "server".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    // Connect repos
    {
        #[allow(deprecated)]
        fn repos(big_repo: &SharedBigRepo) -> &samod::Repo {
            big_repo.samod_repo()
        }
        crate::tincans::connect_repos(repos(&client_acx), repos(&server_acx));
        repos(&client_acx).when_connected("server".into()).await?;
        repos(&server_acx).when_connected("client".into()).await?;
    }

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = client_acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));

    let (client_repo, client_stop) = DrawerRepo::load(
        Arc::clone(&client_acx),
        drawer_doc_id.clone(),
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::clone(&entry_pool),
        Arc::clone(&doc_pool),
        None,
    )
    .await?;
    let (server_repo, server_stop) = DrawerRepo::load(
        Arc::clone(&server_acx),
        drawer_doc_id.clone(),
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::clone(&entry_pool),
        Arc::clone(&doc_pool),
        None,
    )
    .await?;

    // Ensure baseline drawer sync converges before asserting incremental replication.
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let client_heads = client_repo
                .drawer_am_handle
                .with_document(|doc| doc.get_heads());
            let server_heads = server_repo
                .drawer_am_handle
                .with_document(|doc| doc.get_heads());
            if client_heads == server_heads {
                break Ok::<(), eyre::Report>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .wrap_err("timeout waiting for drawer baseline sync")??;

    // 1. Client adds a doc
    let new_doc_id = client_repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note("Hello".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    // 2. Server should eventually observe the replicated doc
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let list = server_repo.list().await?;
            if list.iter().any(|entry| entry.doc_id == new_doc_id) {
                break Ok::<(), eyre::Report>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .wrap_err("timeout waiting for DocAdded")??;

    // 3. Server should be able to list the doc.
    // Note: this test validates drawer-map replication only. Content-doc sync is
    // covered by sync tests that exercise full document transfer semantics.
    let list = server_repo.list().await?;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].doc_id, new_doc_id);

    client_acx_stop.stop().await?;
    server_acx_stop.stop().await?;
    client_stop.stop().await?;
    server_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_additional_apis() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-apis".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);

    // 1. Add doc
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Base".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    // 2. Test get_doc_branches
    let branches = repo.get_doc_branches(&doc_id).await?.unwrap();
    assert!(branches.branches.contains_key("main"));

    // 3. Test get_with_heads
    let (doc, heads) = repo
        .get_with_heads(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc.facets.get(&facet_title).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Base".into()))
    );

    // 4. Test get_if_latest
    let doc_latest = repo
        .get_if_latest(&doc_id, &"main".into(), &heads, None)
        .await?
        .unwrap();
    assert_eq!(
        doc_latest.facets.get(&facet_title).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Base".into()))
    );

    let wrong_heads = ChangeHashSet(Arc::from([automerge::ChangeHash([0u8; 32])]));
    assert!(repo
        .get_if_latest(&doc_id, &"main".into(), &wrong_heads, None)
        .await?
        .is_none());

    // 5. Test update_batch
    repo.update_batch(vec![UpdateDocArgsV2 {
        branch_path: "main".into(),
        heads: None,
        patch: DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Batch Updated".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
    }])
    .await
    .map_err(|e| eyre::eyre!("batch update failed: {:?}", e))?;

    let doc_updated = repo
        .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc_updated.facets.get(&facet_title).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Batch Updated".into()))
    );

    // 6. Test merge_from_branch
    let latest_main_heads = repo
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing branches")?
        .branches
        .get("main")
        .cloned()
        .ok_or_eyre("missing main")?;
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &latest_main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Branch A".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(latest_main_heads),
    )
    .await?;

    repo.merge_from_branch(&doc_id, &"main".into(), &local_branch("branch-a"), None)
        .await?;
    let doc_merged = repo
        .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
        .await?
        .unwrap();
    assert_eq!(
        doc_merged.facets.get(&facet_title).unwrap(),
        &serde_json::Value::from(WellKnownFacet::TitleGeneric("Branch A".into()))
    );

    // 7. Test delete_branch
    assert!(
        repo.delete_branch(&doc_id, &local_branch("branch-a"), None)
            .await?
    );
    let branches_after_del = repo.get_doc_branches(&doc_id).await?.unwrap();
    assert!(!branches_after_del
        .branches
        .contains_key(&*local_branch("branch-a").to_string()));
    let entry_after_del = repo
        .get_entry(&doc_id)
        .await?
        .ok_or_eyre("entry missing after delete_branch")?;
    let branch_a_deleted = entry_after_del
        .branches_deleted
        .get(&local_branch("branch-a").to_string())
        .ok_or_eyre("missing branch-a tombstone after delete_branch")?;
    let latest_tombstone = branch_a_deleted
        .last()
        .ok_or_eyre("missing latest branch-a tombstone")?;
    assert!(
        !latest_tombstone.branch_doc_id.is_empty(),
        "branch tombstone should retain deleted branch doc id"
    );
    assert!(
        !latest_tombstone.branch_heads.0.is_empty(),
        "branch tombstone should retain deleted branch heads"
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_metadata_maintenance() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-meta".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let facet_note = FacetKey::from(WellKnownFacetTag::Note);
    let user_path = UserPath::from("/duser-wip-testmeta1/ddev-wip-iroh-testmeta1/plug1/routine1");

    // 1. Test 'add' metadata
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Initial".into()).into(),
            )]
            .into(),
            user_path: Some(user_path.clone()),
        })
        .await?;

    let dmeta_after_add = get_dmeta_on_main(&repo, &doc_id).await?;
    let main_branch_doc_id = repo
        .get_branch_state(&doc_id, &"main".into())
        .await?
        .ok_or_eyre("missing main branch state")?
        .branch_doc_id;
    assert!(
        dmeta_after_add.actors.contains_key(
            &repo
                .content_actor_id(Some(&user_path), &main_branch_doc_id)
                .to_string()
        ),
        "user should be recorded on add dmeta"
    );
    assert!(
        dmeta_after_add.facets.contains_key(&facet_title),
        "dmeta facet metadata should exist for title"
    );

    // Check Dmeta in content doc
    let am_id = DocumentId::from_str(
        &repo
            .get_branch_state(&doc_id, &"main".into())
            .await?
            .ok_or_eyre("missing main branch state")?
            .branch_doc_id,
    )?;
    let handle = big_repo.find_doc_handle(&am_id).await?.unwrap();
    handle.with_document(|doc| -> Res<()> {
        let heads = facet_recovery::recover_facet_heads(doc, &facet_title)?;
        assert_eq!(heads.len(), 1, "should have 1 head for title");
        Ok(())
    })?;

    // 2. Test 'update' metadata and user attribution
    let user_path2 = UserPath::from("/duser-wip-testmeta2/ddev-wip-iroh-testmeta2/plug2/routine2");
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_note.clone(),
                WellKnownFacet::Note("New Note".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: Some(user_path2.clone()),
        },
        "main".into(),
        None,
    )
    .await?;

    let dmeta_after_update = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(
        dmeta_after_update.actors.contains_key(
            &repo
                .content_actor_id(Some(&user_path2), &main_branch_doc_id)
                .to_string()
        ),
        "updated user should be recorded on dmeta"
    );
    assert!(
        dmeta_after_update.facets.contains_key(&facet_note),
        "dmeta facet metadata should exist for note"
    );
    assert!(
        dmeta_after_update
            .facets
            .get(&facet_note)
            .is_some_and(|meta| meta.deleted_at.is_empty()),
        "newly active facet metadata should not be tombstoned",
    );

    // 2b. Remove facet: dmeta should tombstone, not delete metadata entry.
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: HashMap::new(),
            facets_remove: vec![facet_note.clone()],
            user_path: None,
        },
        "main".into(),
        None,
    )
    .await?;
    let dmeta_after_remove = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(
        dmeta_after_remove.facets.contains_key(&facet_note),
        "removed facet metadata should remain in dmeta as a tombstone",
    );
    assert!(
        dmeta_after_remove
            .facets
            .get(&facet_note)
            .is_some_and(|meta| !meta.deleted_at.is_empty()),
        "removed facet metadata should record deleted_at",
    );

    // 2c. Re-add facet: tombstone should clear.
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_note.clone(),
                WellKnownFacet::Note("Re-added Note".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        "main".into(),
        None,
    )
    .await?;
    let dmeta_after_readd = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(
        dmeta_after_readd
            .facets
            .get(&facet_note)
            .is_some_and(|meta| meta.deleted_at.is_empty()),
        "re-added facet metadata should clear deleted_at tombstones",
    );
    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();

    // 3. Test 'merge' metadata maintenance
    let main_heads = entry.branches.get("main").unwrap().clone();
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Branch Title".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(main_heads),
    )
    .await?;

    let entry_before_merge = repo.get_doc_branches(&doc_id).await?.unwrap();
    let a_heads = entry_before_merge
        .branches
        .get(&*local_branch("branch-a").to_string())
        .unwrap()
        .clone();

    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-a"),
        &a_heads,
        Some(user_path.clone()),
    )
    .await?;

    let dmeta_after_merge = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(
        dmeta_after_merge.facets.contains_key(&facet_title),
        "title metadata should exist after merge"
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

fn latest_change_actor(handle: &samod::DocHandle) -> Res<automerge::ActorId> {
    handle.with_document(|doc| {
        let heads = doc.get_heads();
        let Some(latest_head) = heads.first() else {
            eyre::bail!("doc has no heads");
        };
        let change = doc
            .get_change_by_hash(latest_head)
            .ok_or_eyre("latest head change not found")?;
        Ok(change.actor_id().clone())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_at_heads_uses_patch_user_path_actor() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-update-actor".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;
    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("before".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let user_path = UserPath::from("/duser-wip-testactor/ddev-wip-iroh-testactor/plug/routine");
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note("updated".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: Some(user_path.clone()),
        },
        "main".into(),
        None,
    )
    .await?;

    let expected_actor = repo.content_actor_id(
        Some(&user_path),
        &repo
            .get_branch_state(&doc_id, &"main".into())
            .await?
            .ok_or_eyre("missing main branch state")?
            .branch_doc_id,
    );
    let handle = big_repo
        .find_doc_handle(&DocumentId::from_str(
            &repo
                .get_branch_state(&doc_id, &"main".into())
                .await?
                .ok_or_eyre("missing main branch state")?
                .branch_doc_id,
        )?)
        .await?
        .ok_or_eyre("doc not found")?;
    let latest_actor = latest_change_actor(&handle)?;
    assert_eq!(latest_actor, expected_actor);

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_merge_from_heads_uses_user_path_actor() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-merge-actor".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;
    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("base".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let main_heads = repo
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing entry")?
        .branches
        .get("main")
        .cloned()
        .ok_or_eyre("missing main branch")?;
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("branch-change".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(main_heads),
    )
    .await?;

    let branch_heads = repo
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing entry after branch update")?
        .branches
        .get(&*local_branch("branch-a").to_string())
        .cloned()
        .ok_or_eyre("missing branch-a")?;
    let merge_user_path =
        UserPath::from("/duser-wip-testmerge/ddev-wip-iroh-testmerge/plug/routine");
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-a"),
        &branch_heads,
        Some(merge_user_path.clone()),
    )
    .await?;

    let expected_actor = repo.content_actor_id(
        Some(&merge_user_path),
        &repo
            .get_branch_state(&doc_id, &"main".into())
            .await?
            .ok_or_eyre("missing main branch state")?
            .branch_doc_id,
    );
    let handle = big_repo
        .find_doc_handle(&DocumentId::from_str(
            &repo
                .get_branch_state(&doc_id, &"main".into())
                .await?
                .ok_or_eyre("missing main branch state")?
                .branch_doc_id,
        )?)
        .await?
        .ok_or_eyre("doc not found")?;
    let latest_actor = latest_change_actor(&handle)?;
    assert_eq!(latest_actor, expected_actor);

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_facet_keys_touched_by_local_actor_includes_user_path_scoped_actor() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-local-facet-touch".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;
    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("base".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let user_path = UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest/plug/routine");
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                note_key.clone(),
                WellKnownFacet::Note("from-user-path-actor".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: Some(user_path),
        },
        "main".into(),
        None,
    )
    .await?;

    let main_heads = repo
        .get_doc_branches(&doc_id)
        .await?
        .ok_or_eyre("missing entry")?
        .branches
        .get("main")
        .cloned()
        .ok_or_eyre("missing main branch")?;
    let touched = repo
        .facet_keys_touched_by_local_actor(
            &doc_id,
            &"main".into(),
            &main_heads,
            std::slice::from_ref(&note_key),
        )
        .await?;
    assert!(
        touched.contains(&note_key),
        "expected Note facet change from user_path-scoped actor to count as local"
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[test]
fn test_facet_cache_admission_requires_second_put() {
    let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(10_000)));
    let mut cache = FacetCacheState::new(pool);
    let doc_id = "doc-1".to_string();
    let facet_uuid = Uuid::new_v4();
    let heads = ChangeHashSet(Vec::new().into());
    let value = Arc::new(serde_json::json!({"mime":"text/plain","content":"hello"}));

    cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
    let miss = cache.get_if_heads_match(&doc_id, &facet_uuid, &heads);
    assert!(miss.is_none(), "first write should stay probationary");

    cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
    let hit = cache.get_if_heads_match(&doc_id, &facet_uuid, &heads);
    assert!(hit.is_some(), "second write should be admitted");
}

#[test]
fn test_facet_cache_miss_on_heads_change() {
    let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(10_000)));
    let mut cache = FacetCacheState::new(pool);
    let doc_id = "doc-2".to_string();
    let facet_uuid = Uuid::new_v4();
    let heads_a = ChangeHashSet(Vec::new().into());
    let heads_b = ChangeHashSet(Arc::from([automerge::ChangeHash([1u8; 32])]));
    let value = Arc::new(serde_json::json!({"content":"a"}));

    cache.put(&doc_id, facet_uuid, heads_a.clone(), Arc::clone(&value));
    cache.put(&doc_id, facet_uuid, heads_a.clone(), Arc::clone(&value));
    assert!(cache
        .get_if_heads_match(&doc_id, &facet_uuid, &heads_a)
        .is_some());
    assert!(cache
        .get_if_heads_match(&doc_id, &facet_uuid, &heads_b)
        .is_none());
}

#[test]
fn test_facet_cache_large_one_hit_entries_do_not_pollute() {
    let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1024)));
    let mut cache = FacetCacheState::new(pool);
    let doc_id = "doc-3".to_string();
    let heads = ChangeHashSet(Vec::new().into());

    for index in 0..20u32 {
        let facet_uuid = Uuid::new_v4();
        let payload = "x".repeat(4 * 1024);
        let value = Arc::new(serde_json::json!({"idx": index, "payload": payload}));
        cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
        assert!(
            cache
                .get_if_heads_match(&doc_id, &facet_uuid, &heads)
                .is_none(),
            "probationary one-hit values should not be admitted"
        );
    }
    assert!(
        cache.entries.is_empty(),
        "no one-hit payload should be cached"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_updated_at_merge() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-updated-at".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);

    // 1. Add doc on main
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Base".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let main_heads = entry.branches.get("main").unwrap().clone();

    // 2. Concurrent updates to the same facet on branch-a and branch-b
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("A".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(main_heads.clone()),
    )
    .await?;

    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-b"),
        &"main".into(),
        &main_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("B".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-b"),
        Some(main_heads.clone()),
    )
    .await?;

    let expected_a_raw = repo
        .get_doc_with_facets_at_branch(
            &doc_id,
            &local_branch("branch-a"),
            Some(vec![facet_title.clone()]),
        )
        .await?
        .ok_or_eyre("missing branch-a doc")?
        .facets
        .get(&facet_title)
        .cloned()
        .ok_or_eyre("missing title facet on branch-a")?;
    let expected_b_raw = repo
        .get_doc_with_facets_at_branch(
            &doc_id,
            &local_branch("branch-b"),
            Some(vec![facet_title.clone()]),
        )
        .await?
        .ok_or_eyre("missing branch-b doc")?
        .facets
        .get(&facet_title)
        .cloned()
        .ok_or_eyre("missing title facet on branch-b")?;

    // 3. Merge both into main
    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let a_heads = entry
        .branches
        .get(&*local_branch("branch-a").to_string())
        .unwrap()
        .clone();
    let b_heads = entry
        .branches
        .get(&*local_branch("branch-b").to_string())
        .unwrap()
        .clone();

    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-a"),
        &a_heads,
        None,
    )
    .await?;
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-b"),
        &b_heads,
        None,
    )
    .await?;

    // 4. Verify updatedAt list in content doc and that recovered heads
    // can resolve the corresponding facet payload versions.
    let am_id = DocumentId::from_str(
        &repo
            .get_branch_state(&doc_id, &"main".into())
            .await?
            .ok_or_eyre("missing main branch state")?
            .branch_doc_id,
    )?;
    let handle = big_repo.find_doc_handle(&am_id).await?.unwrap();
    let recovered_heads = handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
        let heads = facet_recovery::recover_facet_heads(doc, &facet_title)?;

        // On a concurrent update where both sides clear and insert,
        // Automerge list merge will result in both inserted elements being present.
        assert_eq!(
            heads.len(),
            2,
            "updatedAt should have 2 elements after concurrent merge"
        );

        Ok(heads)
    })?;

    let mut recovered_values = std::collections::HashSet::new();
    for head in &recovered_heads {
        let single_head = daybook_types::doc::ChangeHashSet([*head].into());
        let at_doc = repo
            .get_doc_with_facets_at_branch_heads(
                &doc_id,
                &"main".into(),
                &single_head,
                Some(vec![facet_title.clone()]),
            )
            .await?
            .ok_or_eyre("doc missing at recovered facet head")?;
        let raw = at_doc
            .facets
            .get(&facet_title)
            .ok_or_eyre("title facet missing at recovered head")?;
        recovered_values.insert(serde_json::to_string(raw)?);
    }
    let expected_values = std::collections::HashSet::from([
        serde_json::to_string(&expected_a_raw)?,
        serde_json::to_string(&expected_b_raw)?,
    ]);
    assert_eq!(
        recovered_values, expected_values,
        "recovered facet heads should resolve concurrent facet versions",
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_facet_blame_maintenance() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-blame".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let facet_note = FacetKey::from(WellKnownFacetTag::Note);

    // 1. Add doc
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("Initial".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let entry = repo.get_doc_branches(&doc_id).await?.unwrap();
    let initial_heads = entry.branches.get("main").unwrap().clone();
    let dmeta_initial = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(dmeta_initial.facets.contains_key(&facet_title));

    // 2. Update facet on branch-a
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-a"),
        &"main".into(),
        &initial_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("A".into()).into(),
            )]
            .into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-a"),
        Some(initial_heads.clone()),
    )
    .await?;

    let entry_a = repo.get_doc_branches(&doc_id).await?.unwrap();
    let a_heads = entry_a
        .branches
        .get(&*local_branch("branch-a").to_string())
        .unwrap()
        .clone();
    assert_ne!(a_heads, initial_heads, "branch-a should advance");

    // 3. Update different facet on branch-b
    repo.create_branch_at_heads_from_branch(
        &doc_id,
        &local_branch("branch-b"),
        &"main".into(),
        &initial_heads,
        None,
    )
    .await?;
    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(facet_note.clone(), WellKnownFacet::Note("B".into()).into())].into(),
            facets_remove: vec![],
            user_path: None,
        },
        local_branch("branch-b"),
        Some(initial_heads.clone()),
    )
    .await?;

    let entry_b = repo.get_doc_branches(&doc_id).await?.unwrap();
    let b_heads = entry_b
        .branches
        .get(&*local_branch("branch-b").to_string())
        .unwrap()
        .clone();
    assert_ne!(b_heads, initial_heads, "branch-b should advance");

    // 4. Merge branch-a to main
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-a"),
        &a_heads,
        None,
    )
    .await?;
    let entry_merged_a = repo.get_doc_branches(&doc_id).await?.unwrap();
    let main_heads_a = entry_merged_a.branches.get("main").unwrap().clone();
    assert_ne!(
        main_heads_a, initial_heads,
        "main should advance after merge A"
    );
    let dmeta_after_a = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(dmeta_after_a.facets.contains_key(&facet_title));

    // 5. Merge branch-b to main
    repo.merge_from_heads(
        &doc_id,
        &"main".into(),
        &local_branch("branch-b"),
        &b_heads,
        None,
    )
    .await?;
    let entry_merged_b = repo.get_doc_branches(&doc_id).await?.unwrap();
    let main_heads_b = entry_merged_b.branches.get("main").unwrap().clone();
    assert_ne!(
        main_heads_b, main_heads_a,
        "main should advance after merge B"
    );
    let dmeta_after_b = get_dmeta_on_main(&repo, &doc_id).await?;
    assert!(dmeta_after_b.facets.contains_key(&facet_note));
    assert!(dmeta_after_b.facets.contains_key(&facet_title));

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_listener_is_scoped_to_drawer_doc() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-scope".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let make_drawer_doc = || async {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        eyre::Ok(handle.document_id().clone())
    };

    let drawer_doc_id_a = make_drawer_doc().await?;
    let drawer_doc_id_b = make_drawer_doc().await?;

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));

    let (repo_a, stop_a) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id_a,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::clone(&entry_pool),
        Arc::clone(&doc_pool),
        None,
    )
    .await?;
    let (repo_b, stop_b) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id_b,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::clone(&entry_pool),
        Arc::clone(&doc_pool),
        None,
    )
    .await?;

    let listener_b = repo_b.subscribe(crate::repos::SubscribeOpts::new(128));

    let facet_note = FacetKey::from(WellKnownFacetTag::Note);
    let _doc_id_a = repo_a
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                facet_note.clone(),
                WellKnownFacet::Note("hello-from-a".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let maybe_event = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        listener_b.recv_lossy_async(),
    )
    .await;
    assert!(
        maybe_event.is_err(),
        "repo_b received an event from foreign drawer doc"
    );

    stop_a.stop().await?;
    stop_b.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_content_update_does_not_emit_drawer_membership_events() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-changed-facets".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let facet_note = FacetKey::from(WellKnownFacetTag::Note);
    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [
                (
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("initial".into()).into(),
                ),
                (
                    facet_note.clone(),
                    WellKnownFacet::Note("initial".into()).into(),
                ),
            ]
            .into(),
            user_path: None,
        })
        .await?;

    let listener = repo.subscribe(crate::repos::SubscribeOpts::new(256));

    repo.update_at_heads(
        DocPatch {
            id: doc_id.clone(),
            facets_set: [(
                facet_title.clone(),
                WellKnownFacet::TitleGeneric("updated".into()).into(),
            )]
            .into(),
            facets_remove: vec![facet_note.clone()],
            user_path: None,
        },
        "main".into(),
        None,
    )
    .await?;

    let recv = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        listener.recv_lossy_async(),
    )
    .await;
    assert!(
        recv.is_err(),
        "content-only update should not emit drawer membership events"
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_diff_events_delete_origin_uses_map_deleted_tombstone() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-delete-origin".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let doc_id = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("delete me".into()).into(),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    let before_delete = repo.get_drawer_heads();
    repo.del(&doc_id).await?;
    let after_delete = repo.get_drawer_heads();

    let events = repo
        .diff_events(before_delete, Some(after_delete))
        .await
        .wrap_err("diff_events failed after delete")?;
    let delete_event = events
        .into_iter()
        .find(|event| matches!(event, DrawerEvent::DocDeleted { id, .. } if id == &doc_id))
        .ok_or_eyre("missing DocDeleted event in diff replay")?;
    let DrawerEvent::DocDeleted {
        origin,
        deleted_facet_keys,
        ..
    } = delete_event
    else {
        unreachable!("guard above ensures DocDeleted");
    };
    assert!(
        matches!(origin, crate::event_origin::SwitchEventOrigin::Local { .. }),
        "replayed delete should infer local origin from docs.map_deleted tombstone",
    );
    assert!(
        deleted_facet_keys
            .iter()
            .any(|key| key.tag == WellKnownFacetTag::TitleGeneric.into()),
        "replayed delete should include deleted facet keys",
    );

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_rejects_unknown_facet_tag() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-unknown-tag".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let (repo, stop_token) = DrawerRepo::load(
        big_repo,
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let unknown_facet_key = FacetKey::from("org.test.unknown/main");
    let add_result = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(unknown_facet_key, serde_json::json!({"hello":"world"}))].into(),
            user_path: None,
        })
        .await;
    assert!(add_result.is_err());
    assert!(add_result
        .unwrap_err()
        .to_string()
        .contains("no registered manifest"));

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_rejects_self_reference_without_target_facet() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-self-ref".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let (repo, stop_token) = DrawerRepo::load(
        big_repo,
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let blob_facet_key = FacetKey::from(WellKnownFacetTag::Blob);
    let image_metadata_facet_key = FacetKey::from(WellKnownFacetTag::ImageMetadata);
    let facet_ref = build_facet_ref(daybook_types::url::FACET_SELF_DOC_ID, &blob_facet_key)?;
    let image_metadata_facet = WellKnownFacet::ImageMetadata(daybook_types::doc::ImageMetadata {
        facet_ref,
        ref_heads: ChangeHashSet(Arc::new([])),
        mime: "image/jpeg".into(),
        width_px: 1,
        height_px: 1,
    });

    let add_result = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [(image_metadata_facet_key, image_metadata_facet.into())].into(),
            user_path: None,
        })
        .await;
    assert!(add_result.is_err());
    assert!(add_result
        .unwrap_err()
        .to_string()
        .contains("self-reference target"));

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_accepts_body_self_reference_with_empty_fragment_for_present_target() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: "test-v2-body-empty-fragment-self".into(),
        storage: am_utils_rs::repo::StorageConfig::Memory,
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let (repo, stop_token) = DrawerRepo::load(
        big_repo,
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        None,
    )
    .await?;

    let note_facet_key = FacetKey::from(WellKnownFacetTag::Note);
    let body_facet_key = FacetKey::from(WellKnownFacetTag::Body);
    let note_ref = format!(
        "{}#",
        build_facet_ref(daybook_types::url::FACET_SELF_DOC_ID, &note_facet_key)?
    )
    .parse()?;

    let add_result = repo
        .add(AddDocArgs {
            branch_path: "main".into(),
            facets: [
                (
                    note_facet_key.clone(),
                    WellKnownFacet::Note("hello".into()).into(),
                ),
                (
                    body_facet_key,
                    WellKnownFacet::Body(Body {
                        order: vec![note_ref],
                    })
                    .into(),
                ),
            ]
            .into(),
            user_path: None,
        })
        .await;

    assert!(add_result.is_ok(), "{add_result:?}");

    stop_token.stop().await?;
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn perf_samod_disk_add_like_drawer_baseline() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_dir = tempfile::tempdir()?;
    let storage_path = temp_dir.path().join("samod-amctx-disk");
    std::fs::create_dir_all(&storage_path)?;

    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: format!("perf-drawer-raw-amctx-{}", Uuid::new_v4()),
        storage: am_utils_rs::repo::StorageConfig::Disk {
            path: storage_path.clone(),
            big_repo_sqlite_url: None,
        },
    })
    .await?;

    let local_actor_id = automerge::ActorId::random();
    let mut aggregate_doc = automerge::Automerge::new();
    {
        let mut tx = aggregate_doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        let docs_obj = tx.put_object(automerge::ROOT, "docs", automerge::ObjType::Map)?;
        tx.put_object(&docs_obj, "map", automerge::ObjType::Map)?;
        tx.commit();
    }
    let aggregate_handle = big_repo.add_doc(aggregate_doc).await?;

    let total_docs: u64 = 80;
    let started_at = std::time::Instant::now();

    for ii in 0..total_docs {
        let mut content_doc = automerge::Automerge::new();
        content_doc.set_actor(automerge::ActorId::random());
        let content_handle = big_repo.add_doc(content_doc).await?;
        let content_doc_id = content_handle.document_id().to_string();

        let _content_heads = content_handle.with_document(|doc| -> Res<ChangeHashSet> {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
            tx.put(automerge::ROOT, "id", &content_doc_id)?;
            let facets_obj = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
            for jj in 0..4_u64 {
                let facet_obj = tx.put_object(
                    &facets_obj,
                    format!("org.test.perf/{jj:02}"),
                    automerge::ObjType::Map,
                )?;
                tx.put(&facet_obj, "title", format!("doc-{ii}-facet-{jj}"))?;
                tx.put(&facet_obj, "num", (ii * 10 + jj) as i64)?;
                let tags_obj = tx.put_object(&facet_obj, "tags", automerge::ObjType::List)?;
                tx.insert(&tags_obj, 0, "alpha")?;
                tx.insert(&tags_obj, 1, "beta")?;
                tx.insert(&tags_obj, 2, "gamma")?;
            }
            let (heads, _) = tx.commit();
            let head = heads.expect("content doc commit failed");
            Ok(ChangeHashSet(Arc::from([head])))
        })?;

        aggregate_handle.with_document(|doc| -> Res<()> {
            let mut tx = doc.transaction();
            let docs_obj = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => eyre::bail!("aggregate docs map missing"),
            };
            let map_obj = match tx.get(&docs_obj, "map")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => eyre::bail!("aggregate docs.map missing"),
            };
            let entry = DocEntry {
                branches: [(
                    "main".to_string(),
                    StoredBranchRef {
                        branch_doc_id: content_doc_id.to_string(),
                    },
                )]
                .into(),
                branches_deleted: HashMap::new(),
                vtag: VersionTag::mint(local_actor_id.clone()),
                previous_version_heads: None,
            };
            autosurgeon::reconcile_prop(&mut tx, &map_obj, &*content_doc_id, &entry)?;
            tx.commit();
            Ok(())
        })?;
    }

    let elapsed = started_at.elapsed();
    let docs_per_sec = total_docs as f64 / elapsed.as_secs_f64();
    eprintln!(
        "samod+automerge via SharedBigRepo baseline: added {} docs in {:?} ({:.2} docs/sec)",
        total_docs, elapsed, docs_per_sec
    );
    assert!(docs_per_sec > 0.0);
    acx_stop.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn perf_drawer_add_disk_baseline() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let storage_path = std::env::temp_dir()
        .join("drawer-perf")
        .join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&storage_path)?;

    let (big_repo, acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
        peer_id: format!("perf-drawer-add-{}", Uuid::new_v4()),
        storage: am_utils_rs::repo::StorageConfig::Disk {
            path: storage_path.clone(),
            big_repo_sqlite_url: None,
        },
    })
    .await?;

    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(4000)));
    let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(4000)));
    let (repo, stop_token) = DrawerRepo::load(
        Arc::clone(&big_repo),
        drawer_doc_id,
        daybook_types::doc::UserPath::from("/duser-wip-localtest/ddev-wip-iroh-localtest"),
        new_meta_db_pool().await?,
        std::env::temp_dir().join(Uuid::new_v4().to_string()),
        entry_pool,
        doc_pool,
        None,
    )
    .await?;

    let total_docs: u64 = 80;
    let started_at = std::time::Instant::now();
    for ii in 0..total_docs {
        let _doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [
                    (
                        FacetKey::from(WellKnownFacetTag::TitleGeneric),
                        WellKnownFacet::TitleGeneric(format!("doc-{ii}")).into(),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::Note),
                        WellKnownFacet::Note(format!("note-{ii}").into()).into(),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::LabelGeneric),
                        WellKnownFacet::LabelGeneric(format!("label-{ii}")).into(),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::PathGeneric),
                        WellKnownFacet::PathGeneric(format!("/tmp/doc-{ii}")).into(),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;
    }

    let elapsed = started_at.elapsed();
    let docs_per_sec = total_docs as f64 / elapsed.as_secs_f64();
    eprintln!(
        "drawer add baseline: added {} docs in {:?} ({:.2} docs/sec)",
        total_docs, elapsed, docs_per_sec
    );
    assert!(docs_per_sec > 0.0);
    stop_token.stop().await?;
    acx_stop.stop().await?;
    let _ = std::fs::remove_dir_all(&storage_path);
    Ok(())
}
