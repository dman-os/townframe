use super::*;

#[tokio::test]
async fn reopened_database_preserves_quarantine_and_source_tombstones() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let inventory = DocId::new([83; 32]);
    let digest = BlobId::new([84; 32]);
    let peer = PeerKey::new([85; 32]);
    let pins = BTreeMap::from([(digest, 42)]);
    for pin_first in [false, true] {
        let path = directory
            .path()
            .join(format!("admission-{pin_first}.sqlite"));
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let (admission, stop) =
            Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
        admission.select(&inventory, "root", true).await?;
        admission.authorize(&inventory, true).await?;
        if pin_first {
            admission.pins(&inventory, 2, &pins, true).await?;
        } else {
            admission
                .observe(&inventory, digest, &peer, 10, true, None)
                .await?;
        }
        stop.stop().await;
        drop(admission);
        drop(runtime);
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let (admission, stop) =
            Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
        admission.recover().await?;
        if pin_first {
            admission
                .observe(&inventory, digest, &peer, 30, true, None)
                .await?;
        } else {
            admission.pins(&inventory, 1, &pins, true).await?;
        }
        assert!(admission.begin(&inventory, digest).await?.is_none());
        admission.authorize(&inventory, true).await?;
        let attempt = admission.begin(&inventory, digest).await?.unwrap();
        assert!(admission.complete(&attempt, true).await?);
        admission
            .observe(&inventory, digest, &peer, 40, false, None)
            .await?;
        stop.stop().await;
        drop(admission);
        drop(runtime);
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let (admission, stop) =
            Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
        admission.recover().await?;
        admission.authorize(&inventory, true).await?;
        admission
            .observe(&inventory, digest, &peer, 10, true, None)
            .await?;
        let state = admission.state(&inventory, digest).await?.unwrap();
        assert!(!state.observed);
        assert!(!state.advertisable());
        assert!(!state.retains_bytes());
        stop.stop().await;
        drop(admission);
        drop(runtime);
    }
    Ok(())
}

#[tokio::test]
async fn complete_pin_retry_at_the_same_revision_replaces_incomplete_state_after_restart() -> Res<()>
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("admission-pin-retry.sqlite");
    let inventory = DocId::new([92; 32]);
    let digest = BlobId::new([93; 32]);
    let peer = PeerKey::new([94; 32]);
    let pins = BTreeMap::from([(digest, 42)]);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) =
        Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
    admission.select(&inventory, "root", true).await?;
    admission.authorize(&inventory, true).await?;
    admission.pins(&inventory, 1, &pins, true).await?;
    admission
        .observe(&inventory, digest, &peer, 10, true, None)
        .await?;
    let attempt = admission.begin(&inventory, digest).await?.unwrap();
    assert!(admission.complete(&attempt, true).await?);
    let before = admission.state(&inventory, digest).await?.unwrap();
    admission
        .pins(&inventory, 2, &BTreeMap::new(), false)
        .await?;
    assert_eq!(admission.state(&inventory, digest).await?, Some(before));
    stop.stop().await;
    drop(admission);
    drop(runtime);

    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) =
        Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
    admission.recover().await?;
    admission.authorize(&inventory, true).await?;
    assert!(admission.state(&inventory, digest).await?.unwrap().pinned);
    admission
        .pins(&inventory, 2, &BTreeMap::new(), true)
        .await?;
    let unpinned = admission.state(&inventory, digest).await?.unwrap();
    assert!(!unpinned.pinned, "complete retry must apply pin removals");
    assert!(!unpinned.fetch_eligible());
    assert!(!unpinned.advertisable());
    assert!(unpinned.retains_bytes());
    for (revision, complete) in [(1, true), (2, false), (2, true)] {
        admission
            .pins(&inventory, revision, &pins, complete)
            .await?;
        assert_eq!(
            admission.state(&inventory, digest).await?,
            Some(unpinned.clone())
        );
    }
    admission.pins(&inventory, 3, &pins, true).await?;
    assert!(
        admission
            .state(&inventory, digest)
            .await?
            .unwrap()
            .advertisable()
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn publication_is_ordered_with_invalidation_and_survives_caller_cancellation() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) = Admission::boot(runtime.ephemeral(&[])?).await?;
    let inventory = DocId::new([88; 32]);
    let digest = BlobId::new([89; 32]);
    admission
        .produce(&inventory, digest, "writer", true)
        .await?;
    admission.authorize(&inventory, true).await?;
    admission
        .pins(&inventory, 1, &BTreeMap::from([(digest, 1)]), true)
        .await?;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    let publisher = admission.clone();
    let target = inventory.clone();
    let publication = tokio::spawn(async move {
        publisher
            .publish(&target, digest, move |state, _| {
                Box::pin(async move {
                    assert!(state.advertisable());
                    started.send(()).unwrap();
                    resume.await.unwrap();
                    Ok(true)
                })
            })
            .await
    });
    entered.await?;
    publication.abort();
    release.send(()).unwrap();
    admission.authorize(&inventory, false).await?;
    admission
        .publish(&inventory, digest, |state, _| {
            Box::pin(async move {
                assert!(!state.advertisable());
                Ok(true)
            })
        })
        .await?;
    assert!(publication.await.unwrap_err().is_cancelled());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn final_publication_proof_rearms_a_pair_when_cached_bytes_disappear() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) = Admission::boot(runtime.ephemeral(&[])?).await?;
    let inventory = DocId::new([95; 32]);
    let digest = BlobId::new([96; 32]);
    let peer = PeerKey::new([97; 32]);
    admission.select(&inventory, "root", true).await?;
    admission.authorize(&inventory, true).await?;
    admission
        .pins(&inventory, 1, &BTreeMap::from([(digest, 42)]), true)
        .await?;
    admission
        .observe(&inventory, digest, &peer, 1, true, None)
        .await?;
    admission.available(&inventory, digest, true).await?;
    assert!(admission.begin(&inventory, digest).await?.is_none());
    admission
        .publish(&inventory, digest, |state, _| {
            Box::pin(async move {
                assert!(state.bytes);
                Ok(false)
            })
        })
        .await?;
    let state = admission.state(&inventory, digest).await?.unwrap();
    assert!(!state.bytes);
    assert!(admission.dirty().await?.is_empty());
    assert!(admission.begin(&inventory, digest).await?.is_some());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_accepted_operations_before_database_reopen() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("admission-drain.sqlite");
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) =
        Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
    let inventory = DocId::new([90; 32]);
    let digest = BlobId::new([91; 32]);
    admission
        .produce(&inventory, digest, "writer", true)
        .await?;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    let publisher = admission.clone();
    let target = inventory.clone();
    let publication = tokio::spawn(async move {
        publisher
            .publish(&target, digest, move |_, _| {
                Box::pin(async move {
                    started.send(()).unwrap();
                    resume.await.unwrap();
                    Ok(true)
                })
            })
            .await
    });
    entered.await?;
    let mut selection = Box::pin(admission.select(&inventory, "accepted-before-shutdown", true));
    assert!(futures::poll!(&mut selection).is_pending());
    let mut stopping = Box::pin(stop.stop());
    assert!(futures::poll!(&mut stopping).is_pending());
    release.send(()).unwrap();
    stopping.await;
    selection.await?;
    publication.await??;
    drop(admission);
    drop(runtime);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (admission, stop) =
        Admission::boot(runtime.database(&path, &[], "fixture_migrations")).await?;
    assert!(admission.state(&inventory, digest).await?.unwrap().selected);
    stop.stop().await;
    Ok(())
}
