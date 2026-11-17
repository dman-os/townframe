use super::*;

pub async fn conformance(writer: &mut dyn Writer) -> Res<()> {
    let store = writer.reader();
    let mut revisions = store.subscribe();
    let all = (Bound::Unbounded, Bound::Unbounded);
    let keys: Vec<Arc<[u8]>> = vec![b"a".as_slice().into(), b"b".as_slice().into()];
    let initial = store.read(&keys, all.clone(), 4).await?;
    assert!(initial.entries.is_empty());
    assert_eq!(initial.values, vec![None, None]);
    revisions.borrow_and_update();
    let mutations = vec![
        Mutation {
            key: Arc::clone(&keys[0]),
            value: Some(b"first".as_slice().into()),
        },
        Mutation {
            key: Arc::clone(&keys[1]),
            value: Some(b"second".as_slice().into()),
        },
        Mutation {
            key: Arc::clone(&keys[0]),
            value: None,
        },
        Mutation {
            key: Arc::clone(&keys[0]),
            value: Some(b"last".as_slice().into()),
        },
    ];
    let revision = writer.commit(initial.revision, mutations.clone()).await?;
    assert_eq!(revision, initial.revision + 1);
    assert!(revisions.has_changed()?);
    assert_eq!(*revisions.borrow_and_update(), revision);
    let read = store.read(&keys, all.clone(), 1).await?;
    assert_eq!(read.revision, revision);
    assert_eq!(
        read.values,
        vec![
            Some(b"last".as_slice().into()),
            Some(b"second".as_slice().into())
        ]
    );
    assert_eq!(read.entries.len(), 1);
    assert_eq!(read.entries[0].key, keys[0]);
    assert_eq!(read.entries[0].value.as_ref(), b"last");
    assert!(matches!(writer.commit(initial.revision, mutations).await,
        Err(CommitError::Conflict { actual_revision }) if actual_revision == revision));
    assert!(!revisions.has_changed()?);
    let unchanged = store.read(&keys, all.clone(), 4).await?;
    assert_eq!(unchanged.values, read.values);
    assert_eq!(unchanged.revision, revision);
    let page = store
        .read(
            &[],
            (
                Bound::Excluded(Arc::clone(&keys[0])),
                Bound::Included(Arc::clone(&keys[1])),
            ),
            4,
        )
        .await?;
    assert_eq!(
        page.entries,
        vec![Entry {
            key: Arc::clone(&keys[1]),
            value: b"second".as_slice().into()
        }]
    );
    writer
        .commit(
            revision,
            keys.iter()
                .map(|key| Mutation {
                    key: Arc::clone(key),
                    value: None,
                })
                .collect(),
        )
        .await?;
    let empty = store.read(&keys, all, 4).await?;
    assert!(empty.entries.is_empty());
    assert_eq!(empty.values, vec![None, None]);
    Ok(())
}

#[test]
fn memory_conformance_and_exclusive_ownership() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut writer = store.writer()?;
        assert!(store.clone().writer().is_err());
        conformance(&mut writer).await?;
        drop(writer);
        let reopened = store.writer()?;
        assert_eq!(
            reopened
                .reader()
                .read(&[], (Bound::Unbounded, Bound::Unbounded), 1)
                .await?
                .revision,
            2
        );
        Ok(())
    })
}

#[test]
fn ordered_numeric_scans_and_zero_limit() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut writer = store.writer()?;
        let numbers = [65536u64, 256, 255, 1];
        let mutations = numbers
            .into_iter()
            .map(|number| Mutation {
                key: number.to_be_bytes().into(),
                value: Some(number.to_be_bytes().into()),
            })
            .collect();
        writer.commit(0, mutations).await?;
        let range = (
            Bound::Included(Arc::from(1u64.to_be_bytes())),
            Bound::Excluded(Arc::from(65536u64.to_be_bytes())),
        );
        assert!(store.read(&[], range.clone(), 0).await?.entries.is_empty());
        let page = store.read(&[], range, 10).await?;
        let actual: Vec<_> = page
            .entries
            .iter()
            .map(|entry| u64::from_be_bytes(entry.key.as_ref().try_into().unwrap()))
            .collect();
        assert_eq!(actual, vec![1, 255, 256]);
        Ok(())
    })
}

#[test]
fn readers_never_observe_half_a_batch() -> Res<()> {
    let store = MemoryStore::default();
    let mut writer = store.writer()?;
    let barrier = std::sync::Barrier::new(2);
    let keys: Vec<Arc<[u8]>> = vec![b"entry".as_slice().into(), b"frontier".as_slice().into()];
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            futures::executor::block_on(async {
                for revision in 0..128u64 {
                    writer
                        .commit(
                            revision,
                            keys.iter()
                                .map(|key| Mutation {
                                    key: Arc::clone(key),
                                    value: Some((revision + 1).to_be_bytes().into()),
                                })
                                .collect(),
                        )
                        .await
                        .unwrap();
                }
            });
        });
        barrier.wait();
        futures::executor::block_on(async {
            for _ in 0..128 {
                let read = store
                    .read(&keys, (Bound::Unbounded, Bound::Unbounded), 2)
                    .await
                    .unwrap();
                assert_eq!(read.values[0], read.values[1]);
                if let Some(value) = &read.values[0] {
                    assert_eq!(
                        u64::from_be_bytes(value.as_ref().try_into().unwrap()),
                        read.revision
                    );
                    assert_eq!(read.entries.len(), 2);
                } else {
                    assert_eq!(read.revision, 0);
                    assert!(read.entries.is_empty());
                }
            }
        });
    });
    Ok(())
}
