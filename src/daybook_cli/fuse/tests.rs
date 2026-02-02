use crate::interlude::*;

use daybook_core::drawer::DrawerRepo;
use daybook_types::doc::{Doc, DocId};
use std::fs;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Handle;

async fn setup_test_repo() -> Res<(Arc<DrawerRepo>, Handle, tempfile::TempDir)> {
    // Create a temporary directory for test storage
    let temp_dir = tempfile::tempdir()?;
    let samod_dir = temp_dir.path().join("samod");
    let blobs_dir = temp_dir.path().join("blobs");
    let db_path = temp_dir.path().join("test.db");

    std::fs::create_dir_all(&samod_dir)?;
    std::fs::create_dir_all(&blobs_dir)?;

    // Create test config with temp directories
    let config = crate::context::Config {
        am: utils_rs::am::Config {
            storage: utils_rs::am::StorageConfig::Disk { path: samod_dir },
            peer_id: "daybook_test_client".to_string(),
        },
        sql: crate::context::SqlConfig {
            database_url: format!("sqlite://{}", db_path.display()),
        },
        blobs_root: blobs_dir,
    };

    let ctx = crate::context::Ctx::init(config).await?;
    let drawer_doc_id = ctx.doc_drawer().document_id().clone();
    let (repo, _repo_stop) = DrawerRepo::load(
        ctx.acx.clone(),
        drawer_doc_id,
        ctx.local_actor_id.clone(),
        Arc::new(std::sync::Mutex::new(daybook_core::drawer::lru::KeyedLruPool::new(1000))),
        Arc::new(std::sync::Mutex::new(daybook_core::drawer::lru::KeyedLruPool::new(1000))),
    )
    .await?;
    let rt_handle = Handle::current();
    Ok((repo, rt_handle, temp_dir))
}

async fn mount_filesystem(
    repo: Arc<DrawerRepo>,
    rt_handle: Handle,
    mountpoint: &Path,
) -> Res<fuser::BackgroundSession> {
    let mountpoint_str = mountpoint.to_string_lossy().to_string();
    let options = vec![
        fuser::MountOption::FSName("daybook-test".to_string()),
        fuser::MountOption::AutoUnmount,
    ];

    let fs = crate::fuse::filesystem::DaybookAsyncFS::new(repo, rt_handle.clone()).await?;

    // Use spawn_mount2 which returns a BackgroundSession that auto-unmounts on drop
    let session = fuser::spawn_mount2(fs, &mountpoint_str, &options)?;

    // Give the filesystem a moment to mount
    thread::sleep(Duration::from_millis(500));

    Ok(session)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mount_and_list_files() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    // Create a test document
    let doc = daybook_types::doc::AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::TitleGeneric),
            daybook_types::doc::WellKnownFacet::TitleGeneric("Test Doc".to_string()).into(),
        )]
        .into(),
        user_path: None,
    };
    let doc_id = repo.add(doc).await?;

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");

    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo, rt_handle, &mountpoint).await?;

    // Wait for filesystem to sync
    thread::sleep(Duration::from_millis(200));

    // List files
    let entries: Vec<String> = fs::read_dir(&mountpoint)?
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| name.ends_with(".json"))
        .collect();

    // Should contain the doc file
    let expected_filename = format!("{}.json", doc_id);
    assert!(
        entries.contains(&expected_filename),
        "Expected file {} not found in {:?}",
        expected_filename,
        entries
    );

    // Cleanup - unmount happens automatically with AutoUnmount
    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_file() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    // Create a test document
    let doc = daybook_types::doc::AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::TitleGeneric),
            daybook_types::doc::WellKnownFacet::TitleGeneric("Read Test Doc".to_string()).into(),
        )]
        .into(),
        user_path: None,
    };
    let doc_id = repo.add(doc.clone()).await?;

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");
    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo, rt_handle, &mountpoint).await?;

    // Wait for filesystem to sync
    thread::sleep(Duration::from_millis(200));

    // Read the file
    let file_path = mountpoint.join(format!("{}.json", doc_id));
    let content = fs::read_to_string(&file_path)?;

    println!("{content}");
    // Verify it's valid JSON and contains the doc data
    let parsed_doc: Doc = serde_json::from_str(&content)?;
    assert_eq!(parsed_doc.id, doc_id); // Use the ID returned by repo.add()
    // assert_eq!(parsed_doc.content, doc.content);

    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_file() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    // Create a test document
    let original_doc = daybook_types::doc::AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::TitleGeneric),
            daybook_types::doc::WellKnownFacet::TitleGeneric("Original Title".to_string()).into(),
        )]
        .into(),
        user_path: None,
    };
    let doc_id = repo.add(original_doc).await?;

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");
    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo.clone(), rt_handle, &mountpoint).await?;

    // Wait for filesystem to sync
    thread::sleep(Duration::from_millis(200));

    // Read the original file
    let file_path = mountpoint.join(format!("{}.json", doc_id));
    let original_content = fs::read_to_string(&file_path)?;

    // Modify the JSON
    let mut modified_doc: Doc = serde_json::from_str(&original_content)?;
    modified_doc.content = DocContent::Text("Modified content".to_string());
    modified_doc.props = [(
        DocPropKey::WellKnown(WellKnownDocPropKeys::TitleGeneric),
        DocProp::TitleGeneric("Modified Title".to_string()),
    )]
    .into();
    let modified_json = serde_json::to_string_pretty(&modified_doc)?;

    // Write it back
    fs::write(&file_path, modified_json)?;

    // Give it a moment to process
    thread::sleep(Duration::from_millis(200));

    // Verify the change was persisted
    let updated_doc = repo.get_doc_with_facets_at_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), None).await?;
    let updated_doc = updated_doc.expect("Document should exist");

    // assert_eq!(
    //     updated_doc.content,
    //     DocContent::Text("Modified content".to_string())
    // );
    assert_eq!(
        updated_doc.facets.get(&daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::TitleGeneric)).unwrap(),
        &serde_json::to_value(daybook_types::doc::WellKnownFacet::TitleGeneric("Modified Title".to_string())).unwrap()
    );

    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_invalid_json() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    // Create a test document
    let doc = daybook_types::doc::AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: default(),
        user_path: None,
    };
    let doc_id = repo.add(doc).await?;

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");
    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo.clone(), rt_handle, &mountpoint).await?;

    // Wait for filesystem to sync
    thread::sleep(Duration::from_millis(200));

    let file_path = mountpoint.join(format!("{}.json", doc_id));

    // Try to write invalid JSON
    let invalid_json = "{ invalid json }";
    let write_result = fs::write(&file_path, invalid_json);

    // The write itself should succeed (it's just writing bytes)
    write_result?;

    // But when we close the file (release), it should reject invalid JSON
    // We can verify by checking that the original content is still there
    thread::sleep(Duration::from_millis(200));

    // Re-read the file - it should still have the original content
    let content = fs::read_to_string(&file_path)?;
    let _parsed_doc: Doc = serde_json::from_str(&content)?;
    // assert_eq!(
    //     parsed_doc.content,
    //     DocContent::Text("Original content".to_string())
    // );

    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nonexistent_file() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");
    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo, rt_handle, &mountpoint).await?;

    // Try to read a non-existent file
    let nonexistent_id: DocId = Uuid::new_v4().to_string();
    let file_path = mountpoint.join(format!("{}.json", nonexistent_id));

    let read_result = fs::read_to_string(&file_path);
    assert!(
        read_result.is_err(),
        "Reading non-existent file should fail"
    );

    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_files() -> Res<()> {
    let (repo, rt_handle, _temp_storage) = setup_test_repo().await?;

    // Create multiple test documents
    let mut doc_ids = Vec::new();
    for i in 0..5 {
        let doc = daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::TitleGeneric),
                daybook_types::doc::WellKnownFacet::TitleGeneric(format!("Doc {}", i)).into(),
            )]
            .into(),
            user_path: None,
        };
        let doc_id = repo.add(doc).await?;
        doc_ids.push(doc_id);
    }

    let temp_dir = TempDir::new()?;
    let mountpoint = temp_dir.path().join("mount");
    fs::create_dir(&mountpoint)?;

    let _mount_handle = mount_filesystem(repo, rt_handle, &mountpoint).await?;

    // Wait for filesystem to sync
    thread::sleep(Duration::from_millis(200));

    // List all files
    let entries: Vec<String> = fs::read_dir(&mountpoint)?
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| name.ends_with(".json"))
        .collect();

    // Should contain at least our 5 doc files (might have more from other tests)
    assert!(
        entries.len() >= 5,
        "Should have at least 5 document files, got {}",
        entries.len()
    );

    for doc_id in &doc_ids {
        let expected_filename = format!("{}.json", doc_id);
        assert!(
            entries.contains(&expected_filename),
            "Expected file {} not found",
            expected_filename
        );
    }

    // Unmount the filesystem (drops BackgroundSession which auto-unmounts)
    drop(_mount_handle);
    thread::sleep(Duration::from_millis(200));

    Ok(())
}
