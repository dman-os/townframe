use super::*;
use big_cabinet_core::drawer::MetaRoot;
use futures::FutureExt;
use std::collections::BTreeSet;

fn doc(value: u8) -> DocId {
    DocId::new([value; 32])
}

fn selection(root: MetaRoot, name: &str) -> InventorySelection {
    InventorySelection {
        root,
        name: name.to_owned(),
    }
}

fn ciphertext() -> InventorySelection {
    selection(MetaRoot::Public, "ciphertext")
}

fn plaintext() -> InventorySelection {
    selection(MetaRoot::Sensitive, "plaintext")
}

fn snapshot(inventory: u8, children: &[u8]) -> DrawerSnapshot {
    DrawerSnapshot {
        self_doc: false,
        roots: BTreeSet::from([MetaRoot::Public, MetaRoot::Sensitive]),
        inventories: BTreeMap::from([
            ((MetaRoot::Public, "ciphertext".to_owned()), doc(inventory)),
            (
                (MetaRoot::Sensitive, "plaintext".to_owned()),
                doc(inventory + 1),
            ),
        ]),
        children: children.iter().copied().map(doc).collect(),
    }
}

async fn fixture() -> Res<(Admission, service_utils_rs::StopToken, Database)> {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    let (admission, stop) = Admission::boot(sql.clone()).await?;
    Ok((admission, stop, sql))
}

async fn drain(admission: &Admission, snapshots: &BTreeMap<DocId, DrawerSnapshot>) -> Res<usize> {
    let mut count = 0;
    while let Some((drawer, generation)) = admission.discovery_work().await? {
        admission
            .observe_drawer(&drawer, generation, Some(snapshots[&drawer].clone()))
            .await?;
        count += 1;
    }
    Ok(count)
}

async fn claims(sql: &Database) -> Res<BTreeMap<DocId, u64>> {
    sql.query(
        "SELECT inventory_id
              , COUNT(*)
           FROM big_blobs_discovery_claims
          GROUP BY inventory_id
          ORDER BY inventory_id",
        (),
    )
    .await?
    .all::<(String, u64)>()?
    .into_iter()
    .map(|(id, count)| Ok((id.parse()?, count)))
    .collect()
}

async fn candidate(admission: &Admission, inventory: u8) -> Res<()> {
    admission.authorize(&doc(inventory), true).await?;
    admission
        .pins(
            &doc(inventory),
            1,
            &BTreeMap::from([(BlobId::new([90; 32]), 42)]),
            true,
        )
        .await?;
    admission
        .observe(
            &doc(inventory),
            BlobId::new([90; 32]),
            &PeerKey::new([91; 32]),
            1,
            true,
            None,
        )
        .await
}

async fn publish_pairs(admission: &Admission) -> Res<()> {
    for (inventory, digest, _) in admission.dirty().await? {
        admission
            .publish(&inventory, digest, |pair, _| {
                Box::pin(async move { Ok(pair.bytes) })
            })
            .await?;
    }
    Ok(())
}

async fn empty_inventory(admission: &Admission, inventory: u8) -> Res<()> {
    admission.authorize(&doc(inventory), true).await?;
    admission
        .pins(&doc(inventory), 1, &BTreeMap::new(), true)
        .await
}

#[tokio::test]
async fn cycles_are_finite_and_disconnected_cycles_lose_all_discovery_claims() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[1, 2])),
        (doc(2), snapshot(22, &[1])),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    assert_eq!(drain(&admission, &snapshots).await?, 2);
    assert_eq!(
        admission
            .discovery_documents()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([doc(1), doc(2)])
    );
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(20), 1), (doc(22), 1)])
    );
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    assert!(admission.discovery_work().await?.is_none());
    admission.select_root(&doc(1), None).await?;
    assert!(admission.discovery_documents().await?.is_empty());
    assert!(claims(&sql).await?.is_empty());
    assert!(admission.discovery_work().await?.is_none());
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::UnselectedRoot]
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn diamond_removal_fences_only_the_last_selected_path() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let mut snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[2, 3])),
        (doc(2), snapshot(22, &[4])),
        (doc(3), snapshot(24, &[4])),
        (doc(4), snapshot(26, &[])),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    candidate(&admission, 26).await?;
    let digest = BlobId::new([90; 32]);
    let attempt = admission.begin(&doc(26), digest).await?.unwrap();
    let before = admission.state(&doc(26), digest).await?.unwrap();
    snapshots.insert(doc(1), snapshot(20, &[3]));
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(admission.state(&doc(26), digest).await?, Some(before));
    assert_eq!(claims(&sql).await?.get(&doc(26)), Some(&1));
    snapshots.insert(doc(3), snapshot(24, &[]));
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert!(!claims(&sql).await?.contains_key(&doc(26)));
    assert!(!admission.complete(&attempt, true).await?);
    assert!(!admission.state(&doc(26), digest).await?.unwrap().selected);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn overlapping_roots_and_inherited_overrides_keep_independent_policies() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[2])),
        (doc(2), snapshot(22, &[3])),
        (doc(3), snapshot(24, &[2])),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission.select_root(&doc(2), Some(plaintext())).await?;
    assert_eq!(drain(&admission, &snapshots).await?, 3);
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([
            (doc(20), 1),
            (doc(22), 1),
            (doc(23), 1),
            (doc(24), 1),
            (doc(25), 1)
        ])
    );
    candidate(&admission, 24).await?;
    let attempt = admission
        .begin(&doc(24), BlobId::new([90; 32]))
        .await?
        .unwrap();
    admission
        .override_drawer(&doc(2), Some(plaintext()))
        .await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(20), 1), (doc(23), 2), (doc(25), 2)])
    );
    assert!(!admission.complete(&attempt, true).await?);
    admission.override_drawer(&doc(2), None).await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(claims(&sql).await?.get(&doc(24)), Some(&1));
    assert_eq!(claims(&sql).await?.get(&doc(25)), Some(&1));
    admission.select_root(&doc(1), None).await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(23), 1), (doc(25), 1)])
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn replacement_is_atomic_and_preserves_manual_and_writer_claims() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let mut snapshots = BTreeMap::from([(doc(1), snapshot(20, &[]))]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    candidate(&admission, 20).await?;
    candidate(&admission, 22).await?;
    let digest = BlobId::new([90; 32]);
    admission.select(&doc(20), "manual", true).await?;
    admission.produce(&doc(20), digest, "writer", true).await?;
    admission.available(&doc(20), digest, false).await?;
    let attempt = admission.begin(&doc(20), digest).await?.unwrap();
    snapshots.insert(doc(1), snapshot(22, &[]));
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(22), 1)]));
    assert!(!admission.complete(&attempt, true).await?);
    admission.available(&doc(20), digest, true).await?;
    let old = admission.state(&doc(20), digest).await?.unwrap();
    assert!(old.selected && old.writer && old.bytes);
    assert!(
        admission
            .state(&doc(22), digest)
            .await?
            .unwrap()
            .fetch_eligible()
    );
    assert_eq!(
        admission
            .inventory(&crate::workers::inventory_part(&doc(22)))
            .await?,
        Some(doc(22))
    );
    let before = admission.state(&doc(22), digest).await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission.override_drawer(&doc(1), None).await?;
    assert!(admission.discovery_work().await?.is_none());
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(admission.state(&doc(22), digest).await?, before);
    admission.select_root(&doc(1), None).await?;
    admission.select(&doc(20), "manual", false).await?;
    let old = admission.state(&doc(20), digest).await?.unwrap();
    assert!(!old.selected && old.writer && old.retains_bytes());
    assert!(old.advertisable());
    assert!(!admission.state(&doc(22), digest).await?.unwrap().selected);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn unavailable_and_partial_metadata_preserve_edges_and_only_replace_authoritative_roots()
-> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let mut snapshots = BTreeMap::from([(doc(1), snapshot(20, &[2])), (doc(2), snapshot(22, &[]))]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission.select_root(&doc(2), Some(plaintext())).await?;
    drain(&admission, &snapshots).await?;
    for inventory in [20, 22, 23] {
        empty_inventory(&admission, inventory).await?;
    }
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    assert!(admission.coverage(&doc(2)).await?.is_complete());
    let before = claims(&sql).await?;
    admission.rearm_discovery().await?;
    while let Some((drawer, generation)) = admission.discovery_work().await? {
        admission.observe_drawer(&drawer, generation, None).await?;
        admission
            .observe_drawer(&drawer, generation, Some(snapshot(30, &[])))
            .await?;
    }
    assert_eq!(claims(&sql).await?, before);
    assert_eq!(admission.discovery_documents().await?, vec![doc(1), doc(2)]);
    let pending = admission.coverage(&doc(1)).await?;
    assert_eq!(pending.gaps.len(), 2);
    for drawer in [doc(1), doc(2)] {
        assert!(pending.gaps.contains(&CoverageGap::Metadata {
            drawer,
            selection: ciphertext()
        }));
    }
    assert_eq!(
        admission.coverage(&doc(2)).await?.gaps,
        vec![CoverageGap::Metadata {
            drawer: doc(2),
            selection: plaintext()
        }]
    );
    assert!(admission.discovery_work().await?.is_none());
    admission.rearm_discovery().await?;
    assert_eq!(drain(&admission, &snapshots).await?, 2);
    assert_eq!(claims(&sql).await?, before);
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    assert!(admission.coverage(&doc(2)).await?.is_complete());
    let mut partial = snapshot(24, &[]);
    partial.roots = BTreeSet::from([MetaRoot::Public]);
    snapshots.insert(doc(2), partial);
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(20), 1), (doc(23), 1), (doc(24), 1)])
    );
    assert!(
        admission
            .coverage(&doc(2))
            .await?
            .gaps
            .contains(&CoverageGap::Metadata {
                drawer: doc(2),
                selection: plaintext()
            })
    );
    snapshots
        .get_mut(&doc(2))
        .unwrap()
        .roots
        .insert(MetaRoot::Sensitive);
    snapshots.get_mut(&doc(2)).unwrap().inventories.clear();
    admission.rearm_discovery().await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(20), 1)]));
    assert!(
        admission
            .coverage(&doc(2))
            .await?
            .gaps
            .contains(&CoverageGap::InventoryMissing {
                drawer: doc(2),
                selection: plaintext()
            })
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn stale_generations_cannot_replace_graph_and_pending_work_does_not_hotloop() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    let (drawer, old) = admission.discovery_work().await?.unwrap();
    admission.select_root(&doc(1), Some(plaintext())).await?;
    admission
        .observe_drawer(&drawer, old, Some(snapshot(20, &[2])))
        .await?;
    assert!(claims(&sql).await?.is_empty());
    let (_, current) = admission.discovery_work().await?.unwrap();
    assert!(current > old);
    admission.rearm_discovery().await?;
    let (_, rearmed) = admission.discovery_work().await?.unwrap();
    assert!(rearmed > current);
    admission
        .observe_drawer(&drawer, current, Some(snapshot(20, &[2])))
        .await?;
    assert!(claims(&sql).await?.is_empty());
    admission.observe_drawer(&drawer, rearmed, None).await?;
    assert!(admission.discovery_work().await?.is_none());
    admission.select_root(&drawer, Some(plaintext())).await?;
    assert!(admission.discovery_work().await?.is_none());
    admission.rearm_discovery().await?;
    admission.rearm_discovery().await?;
    let (_, next) = admission.discovery_work().await?.unwrap();
    assert_eq!(next, rearmed + 1);
    admission
        .observe_drawer(&drawer, next, Some(snapshot(20, &[])))
        .await?;
    assert!(admission.discovery_work().await?.is_none());
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(21), 1)]));
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn reopen_recovers_graph_claims_overrides_and_in_flight_work_without_a_scan() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("discovery.sqlite");
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.database(&path, &[], "fixture_migrations");
    let (admission, stop) = Admission::boot(sql.clone()).await?;
    let snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[2, 3])),
        (doc(2), snapshot(22, &[1])),
        (doc(3), snapshot(24, &[])),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission
        .override_drawer(&doc(2), Some(plaintext()))
        .await?;
    drain(&admission, &snapshots).await?;
    let before = claims(&sql).await?;
    let mut changes = admission.subscribe_changes();
    admission.rearm_discovery().await?;
    assert!(changes.has_changed()?);
    changes.borrow_and_update();
    admission.rearm_discovery().await?;
    assert!(!changes.has_changed()?);
    let (drawer, generation) = admission.discovery_work().await?.unwrap();
    let (settled, settled_generation) = admission.discovery_work().await?.unwrap();
    admission
        .observe_drawer(
            &settled,
            settled_generation,
            Some(snapshots[&settled].clone()),
        )
        .await?;
    stop.stop().await;
    drop(admission);
    drop(sql);
    drop(runtime);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.database(&path, &[], "fixture_migrations");
    let (admission, stop) = Admission::boot(sql.clone()).await?;
    assert!(*admission.subscribe_changes().borrow() > 0);
    assert_eq!(claims(&sql).await?, before);
    assert!(!admission.coverage(&doc(1)).await?.is_complete());
    admission
        .observe_drawer(&drawer, generation, Some(snapshot(30, &[])))
        .await?;
    assert_eq!(claims(&sql).await?, before);
    assert_eq!(drain(&admission, &snapshots).await?, 3);
    assert_eq!(claims(&sql).await?, before);
    assert_eq!(
        admission
            .inventory(&crate::workers::inventory_part(&doc(23)))
            .await?,
        Some(doc(23))
    );
    admission.select_root(&doc(1), None).await?;
    assert!(claims(&sql).await?.is_empty());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn coverage_is_inventory_specific_and_never_caches_authority_or_byte_success() -> Res<()> {
    let (admission, stop, _) = fixture().await?;
    let snapshots = BTreeMap::from([(doc(1), snapshot(20, &[2])), (doc(2), snapshot(22, &[]))]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    assert!(!admission.coverage(&doc(1)).await?.is_complete());
    drain(&admission, &snapshots).await?;
    let initial = admission.coverage(&doc(1)).await?;
    for inventory in [doc(20), doc(22)] {
        assert!(initial.gaps.contains(&CoverageGap::InventoryUnreadable {
            inventory: inventory.clone()
        }));
        assert!(
            initial
                .gaps
                .contains(&CoverageGap::PinsIncomplete { inventory })
        );
    }
    empty_inventory(&admission, 20).await?;
    empty_inventory(&admission, 22).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    let digest = BlobId::new([90; 32]);
    candidate(&admission, 24).await?;
    admission
        .pins(&doc(22), 2, &BTreeMap::from([(digest, 42)]), true)
        .await?;
    admission.available(&doc(22), digest, true).await?;
    publish_pairs(&admission).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::ObjectMissing {
            inventory: doc(22),
            digest
        }]
    );
    admission.produce(&doc(22), digest, "writer", true).await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.produce(&doc(22), digest, "writer", false).await?;
    publish_pairs(&admission).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::ObjectMissing {
            inventory: doc(22),
            digest
        }]
    );
    admission
        .observe(&doc(22), digest, &PeerKey::new([91; 32]), 1, true, None)
        .await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.authorize(&doc(22), false).await?;
    publish_pairs(&admission).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::InventoryUnreadable { inventory: doc(22) }]
    );
    admission.authorize(&doc(22), true).await?;
    admission.available(&doc(22), digest, false).await?;
    publish_pairs(&admission).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::BytesMissing {
            inventory: doc(22),
            digest
        }]
    );
    admission.available(&doc(22), digest, true).await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.pins(&doc(22), 3, &BTreeMap::new(), false).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::PinsIncomplete { inventory: doc(22) }]
    );
    admission.pins(&doc(22), 3, &BTreeMap::new(), true).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::PublicationPending {
            inventory: doc(22),
            digest
        }]
    );
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn self_doc_children_are_terminal_and_a_self_doc_root_is_not_empty_success() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let mut terminal = snapshot(22, &[3]);
    terminal.self_doc = true;
    let snapshots = BTreeMap::from([(doc(1), snapshot(20, &[2])), (doc(2), terminal)]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(admission.discovery_documents().await?, vec![doc(1), doc(2)]);
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(20), 1)]));
    assert_eq!(admission.selected_inventories().await?, vec![doc(20)]);
    assert_eq!(
        admission
            .inventory(&crate::workers::inventory_part(&doc(22)))
            .await?,
        None
    );
    candidate(&admission, 20).await?;
    publish_pairs(&admission).await?;
    let digest = BlobId::new([90; 32]);
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::BytesMissing {
            inventory: doc(20),
            digest
        }]
    );
    admission.available(&doc(20), digest, true).await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission
        .invalidate_discovery(BTreeSet::from([doc(2)]))
        .await?;
    let (drawer, generation) = admission.discovery_work().await?.unwrap();
    assert_eq!(drawer, doc(2));
    admission.observe_drawer(&drawer, generation, None).await?;
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(20), 1)]));
    assert_eq!(admission.selected_inventories().await?, vec![doc(20)]);
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::Metadata {
            drawer: doc(2),
            selection: ciphertext()
        }]
    );
    assert!(admission.discovery_work().await?.is_none());
    admission
        .invalidate_discovery(BTreeSet::from([doc(2)]))
        .await?;
    assert_eq!(drain(&admission, &snapshots).await?, 1);
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.select_root(&doc(2), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    let unsupported = admission.coverage(&doc(2)).await?;
    assert!(!unsupported.is_complete());
    assert_eq!(unsupported.gaps, vec![CoverageGap::UnsupportedSelfDoc]);
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.select_root(&doc(1), None).await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(admission.discovery_documents().await?, vec![doc(2)]);
    assert!(claims(&sql).await?.is_empty());
    assert!(admission.selected_inventories().await?.is_empty());
    assert_eq!(
        admission.coverage(&doc(2)).await?.gaps,
        vec![CoverageGap::UnsupportedSelfDoc]
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn self_doc_children_use_each_immediate_grouped_parents_selected_inventory() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[3])),
        (doc(2), snapshot(22, &[3])),
        (
            doc(3),
            DrawerSnapshot {
                self_doc: true,
                roots: BTreeSet::new(),
                inventories: BTreeMap::new(),
                children: BTreeSet::new(),
            },
        ),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission.select_root(&doc(2), Some(plaintext())).await?;
    admission
        .override_drawer(
            &doc(3),
            Some(selection(MetaRoot::Public, "no-child-inventory")),
        )
        .await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(20), 1), (doc(23), 1)])
    );
    let digest = BlobId::new([90; 32]);
    candidate(&admission, 20).await?;
    candidate(&admission, 23).await?;
    admission.available(&doc(20), digest, true).await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    assert_eq!(
        admission.coverage(&doc(2)).await?.gaps,
        vec![CoverageGap::BytesMissing {
            inventory: doc(23),
            digest
        }]
    );
    admission.available(&doc(23), digest, true).await?;
    publish_pairs(&admission).await?;
    assert!(admission.coverage(&doc(2)).await?.is_complete());
    admission.authorize(&doc(20), false).await?;
    publish_pairs(&admission).await?;
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::InventoryUnreadable { inventory: doc(20) }]
    );
    assert!(admission.coverage(&doc(2)).await?.is_complete());
    admission.select_root(&doc(1), None).await?;
    drain(&admission, &snapshots).await?;
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(23), 1)]));
    assert_eq!(admission.selected_inventories().await?, vec![doc(23)]);
    assert_eq!(admission.discovery_documents().await?, vec![doc(2), doc(3)]);
    assert!(admission.coverage(&doc(2)).await?.is_complete());
    admission.select_root(&doc(2), None).await?;
    assert!(claims(&sql).await?.is_empty());
    assert!(admission.discovery_documents().await?.is_empty());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn coverage_waits_for_publication_effects_and_wakes_after_atomic_settlement() -> Res<()> {
    for completed_transfer in [false, true] {
        let (admission, stop, _) = fixture().await?;
        admission.select_root(&doc(1), Some(ciphertext())).await?;
        drain(&admission, &BTreeMap::from([(doc(1), snapshot(20, &[]))])).await?;
        candidate(&admission, 20).await?;
        publish_pairs(&admission).await?;
        let digest = BlobId::new([90; 32]);
        let mut changes = admission.subscribe_changes();
        let before = *changes.borrow();
        if completed_transfer {
            let attempt = admission.begin(&doc(20), digest).await?.unwrap();
            assert!(!changes.has_changed()?);
            assert!(admission.complete(&attempt, true).await?);
        } else {
            admission.available(&doc(20), digest, true).await?;
        }
        changes.changed().await?;
        assert_eq!(*changes.borrow_and_update(), before + 1);
        let pending = vec![CoverageGap::PublicationPending {
            inventory: doc(20),
            digest,
        }];
        assert_eq!(admission.coverage(&doc(1)).await?.gaps, pending);
        assert!(admission.changed.notified().now_or_never().is_some());
        assert!(admission.changed.notified().now_or_never().is_none());
        let state = admission
            .run(|state| Box::pin(async move { Ok(state) }))
            .await?;
        let parts: big_repo::SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
        let part = crate::workers::inventory_part(&doc(20));
        parts.ensure_part(part.clone()).await?;
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let publisher = admission.clone();
        let effects = Arc::clone(&parts);
        let target = part.clone();
        let publication = tokio::spawn(async move {
            publisher
                .publish(&doc(20), digest, move |pair, length| {
                    Box::pin(async move {
                        assert!(pair.advertisable());
                        assert_eq!(length, Some(42));
                        started.send(()).unwrap();
                        resume.await?;
                        effects
                            .set_obj_payload(digest.into(), serde_json::json!({"lengthOctets": 42}))
                            .await?;
                        effects
                            .add_obj_to_parts(digest.into(), vec![target])
                            .await?;
                        Ok(true)
                    })
                })
                .await
        });
        entered.await?;
        assert_eq!(state.coverage(&doc(1)).await?.gaps, pending);
        assert_eq!(parts.member_count(part.clone()).await?, 0);
        assert!(parts.obj_payload(digest.into()).await?.is_none());
        assert!(!changes.has_changed()?);
        let mut wake = Box::pin(changes.changed());
        assert!(futures::poll!(&mut wake).is_pending());
        let root = doc(1);
        let mut coverage = Box::pin(admission.coverage(&root));
        assert!(futures::poll!(&mut coverage).is_pending());
        release.send(()).unwrap();
        wake.await?;
        assert!(state.coverage(&root).await?.is_complete());
        assert_eq!(parts.member_count(part.clone()).await?, 1);
        assert!(admission.changed.notified().now_or_never().is_none());
        publication.await??;
        assert!(coverage.await?.is_complete());
        assert_eq!(*changes.borrow_and_update(), before + 2);
        assert!(admission.dirty().await?.is_empty());
        assert_eq!(parts.member_count(part).await?, 1);
        assert_eq!(
            parts.obj_payload(digest.into()).await?,
            Some(serde_json::json!({"lengthOctets": 42}))
        );
        admission.coverage(&root).await?;
        admission.available(&doc(20), digest, true).await?;
        assert!(!changes.has_changed()?);
        stop.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn pair_fact_commits_broadcast_but_reads_and_duplicate_receipts_are_silent() -> Res<()> {
    let (admission, stop, _) = fixture().await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &BTreeMap::from([(doc(1), snapshot(20, &[]))])).await?;
    let digest = BlobId::new([90; 32]);
    let peer = PeerKey::new([91; 32]);
    let pins = BTreeMap::from([(digest, 42)]);
    let mut changes = admission.subscribe_changes();
    let mut subscriptions = admission.subscribe_changes();
    macro_rules! committed {
        ($operation:expr) => {{
            let revision = *changes.borrow();
            $operation.await?;
            changes.changed().await?;
            subscriptions.changed().await?;
            assert_eq!(*changes.borrow_and_update(), revision + 1);
            assert_eq!(*subscriptions.borrow_and_update(), revision + 1);
        }};
    }
    committed!(admission.authorize(&doc(20), true));
    committed!(admission.pins(&doc(20), 1, &pins, true));
    committed!(admission.observe(&doc(20), digest, &peer, 1, true, None));
    committed!(admission.available(&doc(20), digest, true));
    committed!(publish_pairs(&admission));
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    admission.authorize(&doc(20), true).await?;
    admission.pins(&doc(20), 1, &pins, true).await?;
    admission
        .observe(&doc(20), digest, &peer, 1, false, None)
        .await?;
    admission.available(&doc(20), digest, true).await?;
    admission.produce(&doc(20), digest, "writer", false).await?;
    admission.state(&doc(20), digest).await?;
    admission.length(&doc(20), digest).await?;
    admission.dirty().await?;
    admission.coverage(&doc(1)).await?;
    assert!(!changes.has_changed()? && !subscriptions.has_changed()?);
    committed!(admission.produce(&doc(20), digest, "writer", true));
    admission.produce(&doc(20), digest, "writer", true).await?;
    assert!(!changes.has_changed()? && !subscriptions.has_changed()?);
    committed!(admission.produce(&doc(20), digest, "writer", false));
    committed!(admission.authorize(&doc(20), false));
    assert!(!admission.coverage(&doc(1)).await?.is_complete());
    committed!(admission.authorize(&doc(20), true));
    committed!(admission.available(&doc(20), digest, false));
    let attempt = admission.begin(&doc(20), digest).await?.unwrap();
    assert!(!changes.has_changed()? && !subscriptions.has_changed()?);
    committed!(admission.complete(&attempt, true));
    assert!(!admission.complete(&attempt, true).await?);
    admission.fail(&attempt).await?;
    assert!(!changes.has_changed()? && !subscriptions.has_changed()?);
    committed!(publish_pairs(&admission));
    let generation = admission.state(&doc(20), digest).await?.unwrap().generation;
    admission.settle(&doc(20), digest, generation).await?;
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    assert!(!changes.has_changed()? && !subscriptions.has_changed()?);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn pair_fact_rollbacks_do_not_broadcast_or_acknowledge_publication() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &BTreeMap::from([(doc(1), snapshot(20, &[]))])).await?;
    candidate(&admission, 20).await?;
    let digest = BlobId::new([90; 32]);
    admission.available(&doc(20), digest, true).await?;
    publish_pairs(&admission).await?;
    let before = admission.state(&doc(20), digest).await?;
    let mut changes = admission.subscribe_changes();
    assert!(admission.changed.notified().now_or_never().is_some());
    sql.execute_batch(
        "CREATE TRIGGER reject_pair_mutation BEFORE UPDATE ON big_blobs_admission_pairs
         BEGIN SELECT RAISE(ABORT, 'pair mutation rejected'); END;",
    )
    .await?;
    for operation in [
        "pins",
        "authorization",
        "source",
        "producer",
        "availability",
    ] {
        let result = match operation {
            "pins" => {
                admission
                    .pins(&doc(20), 2, &BTreeMap::from([(digest, 43)]), true)
                    .await
            }
            "authorization" => admission.authorize(&doc(20), false).await,
            "source" => {
                admission
                    .observe(&doc(20), digest, &PeerKey::new([91; 32]), 2, false, None)
                    .await
            }
            "producer" => admission.produce(&doc(20), digest, "writer", true).await,
            "availability" => admission.available(&doc(20), digest, false).await,
            _ => unreachable!(),
        };
        let error = result.unwrap_err();
        assert!(
            format!("{error:?}").contains("pair mutation rejected"),
            "{operation}: {error:?}"
        );
        assert_eq!(admission.state(&doc(20), digest).await?, before);
        assert_eq!(admission.length(&doc(20), digest).await?, Some(42));
        assert!(admission.dirty().await?.is_empty());
        assert!(admission.coverage(&doc(1)).await?.is_complete());
        assert!(!changes.has_changed()?, "{operation} rolled back");
        assert!(admission.changed.notified().now_or_never().is_none());
    }
    assert_eq!(
        sql.query(
            "SELECT pin_revision
              , pins_complete
           FROM big_blobs_inventory_registry
          WHERE inventory_id = ?",
            (doc(20).to_string(),),
        )
        .await?
        .all::<(u64, bool)>()?,
        vec![(1, true)]
    );
    assert_eq!(
        sql.query(
            "SELECT revision
              , present
           FROM big_blobs_source_observations
          WHERE inventory_id = ? AND digest = ?",
            (doc(20).to_string(), digest.to_string()),
        )
        .await?
        .all::<(u64, bool)>()?,
        vec![(1, true)]
    );
    sql.execute_batch("DROP TRIGGER reject_pair_mutation;")
        .await?;
    admission.available(&doc(20), digest, false).await?;
    changes.changed().await?;
    let revision = *changes.borrow_and_update();
    assert!(admission.changed.notified().now_or_never().is_some());
    sql.execute_batch(
        "CREATE TRIGGER reject_pair_settlement BEFORE UPDATE OF dirty ON big_blobs_admission_pairs
         WHEN NEW.dirty = 0
         BEGIN SELECT RAISE(ABORT, 'pair settlement rejected'); END;",
    )
    .await?;
    let error = admission
        .publish(&doc(20), digest, |_, _| Box::pin(async { Ok(true) }))
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("pair settlement rejected"));
    assert!(!admission.state(&doc(20), digest).await?.unwrap().bytes);
    assert_eq!(admission.dirty().await?.len(), 1);
    assert!(
        admission
            .coverage(&doc(1))
            .await?
            .gaps
            .contains(&CoverageGap::PublicationPending {
                inventory: doc(20),
                digest,
            })
    );
    assert!(!changes.has_changed()?);
    assert!(admission.changed.notified().now_or_never().is_none());
    sql.execute_batch("DROP TRIGGER reject_pair_settlement;")
        .await?;
    admission
        .publish(&doc(20), digest, |_, _| Box::pin(async { Ok(true) }))
        .await?;
    changes.changed().await?;
    assert_eq!(*changes.borrow_and_update(), revision + 1);
    assert!(admission.dirty().await?.is_empty());
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    assert!(admission.changed.notified().now_or_never().is_none());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn failed_publication_stays_pending_and_final_availability_settles_cleanly() -> Res<()> {
    let (admission, stop, _) = fixture().await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &BTreeMap::from([(doc(1), snapshot(20, &[]))])).await?;
    candidate(&admission, 20).await?;
    let digest = BlobId::new([90; 32]);
    admission.available(&doc(20), digest, true).await?;
    let pending = vec![CoverageGap::PublicationPending {
        inventory: doc(20),
        digest,
    }];
    let mut changes = admission.subscribe_changes();
    let before = *changes.borrow();
    let failed = admission
        .publish(&doc(20), digest, |_, _| {
            Box::pin(async { Err(utils_rs::prelude::eyre::eyre!("publication effect failed")) })
        })
        .await;
    assert_eq!(failed.unwrap_err().to_string(), "publication effect failed");
    assert_eq!(admission.coverage(&doc(1)).await?.gaps, pending);
    assert_eq!(admission.dirty().await?.len(), 1);
    assert!(!changes.has_changed()?);
    admission
        .publish(&doc(20), digest, |_, _| Box::pin(async { Ok(false) }))
        .await?;
    changes.changed().await?;
    assert_eq!(*changes.borrow_and_update(), before + 1);
    assert!(admission.dirty().await?.is_empty());
    assert!(!admission.state(&doc(20), digest).await?.unwrap().bytes);
    assert_eq!(
        admission.coverage(&doc(1)).await?.gaps,
        vec![CoverageGap::BytesMissing {
            inventory: doc(20),
            digest,
        }]
    );
    let attempt = admission.begin(&doc(20), digest).await?.unwrap();
    assert!(admission.complete(&attempt, true).await?);
    changes.borrow_and_update();
    assert_eq!(admission.coverage(&doc(1)).await?.gaps, pending);
    publish_pairs(&admission).await?;
    changes.changed().await?;
    assert_eq!(*changes.borrow_and_update(), before + 3);
    assert!(admission.coverage(&doc(1)).await?.is_complete());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn discovery_revisions_broadcast_commits_without_consuming_pair_notifications() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let mut discovery = admission.subscribe_changes();
    let mut subscriptions = admission.subscribe_changes();
    admission.changed.notify_one();
    assert!(!discovery.has_changed()? && !subscriptions.has_changed()?);
    assert!(admission.changed.notified().now_or_never().is_some());
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    assert_eq!(*discovery.borrow_and_update(), 1);
    assert_eq!(*subscriptions.borrow_and_update(), 1);
    assert_eq!(admission.discovery_documents().await?, vec![doc(1)]);
    assert!(admission.changed.notified().now_or_never().is_some());
    assert!(admission.changed.notified().now_or_never().is_none());
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    let (drawer, generation) = admission.discovery_work().await?.unwrap();
    assert!(!discovery.has_changed()? && !subscriptions.has_changed()?);
    admission
        .observe_drawer(&drawer, generation, Some(snapshot(20, &[])))
        .await?;
    assert_eq!(*discovery.borrow_and_update(), 2);
    assert_eq!(*subscriptions.borrow_and_update(), 2);
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(20), 1)]));
    admission
        .override_drawer(&doc(1), Some(plaintext()))
        .await?;
    assert_eq!(*discovery.borrow_and_update(), 3);
    assert_eq!(*subscriptions.borrow_and_update(), 3);
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(21), 1)]));
    admission
        .override_drawer(&doc(1), Some(plaintext()))
        .await?;
    assert!(!discovery.has_changed()? && !subscriptions.has_changed()?);
    admission.select_root(&doc(1), None).await?;
    assert_eq!(*discovery.borrow_and_update(), 4);
    assert_eq!(*subscriptions.borrow_and_update(), 4);
    assert!(claims(&sql).await?.is_empty());
    assert!(admission.discovery_documents().await?.is_empty());
    admission.select(&doc(40), "manual", true).await?;
    assert_eq!(*discovery.borrow_and_update(), 5);
    assert_eq!(*subscriptions.borrow_and_update(), 5);
    assert_eq!(admission.selected_inventories().await?, vec![doc(40)]);
    admission.select(&doc(40), "manual", true).await?;
    admission.coverage(&doc(1)).await?;
    admission.discovery_documents().await?;
    admission.inventories().await?;
    assert!(admission.discovery_work().await?.is_none());
    assert!(!discovery.has_changed()? && !subscriptions.has_changed()?);
    admission.select(&doc(40), "manual", false).await?;
    assert_eq!(*discovery.borrow_and_update(), 6);
    assert_eq!(*subscriptions.borrow_and_update(), 6);
    assert!(admission.selected_inventories().await?.is_empty());
    assert!(admission.inventories().await?.contains(&doc(40)));
    admission.select(&doc(40), "manual", false).await?;
    assert!(!discovery.has_changed()? && !subscriptions.has_changed()?);
    let late_subscriber = admission.subscribe_changes();
    assert_eq!(*late_subscriber.borrow(), 6);
    assert!(!late_subscriber.has_changed()?);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn targeted_changes_fence_only_the_affected_drawer_and_reads_do_not_notify() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let snapshots = BTreeMap::from([
        (doc(1), snapshot(20, &[2])),
        (doc(2), snapshot(22, &[])),
        (doc(3), snapshot(24, &[])),
        (doc(4), snapshot(26, &[])),
    ]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    admission.select_root(&doc(3), Some(ciphertext())).await?;
    admission.select_root(&doc(4), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    admission.select_root(&doc(4), None).await?;
    admission.select(&doc(40), "manual", true).await?;
    admission
        .produce(&doc(42), BlobId::new([90; 32]), "writer", true)
        .await?;
    assert_eq!(
        admission
            .selected_inventories()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([doc(20), doc(22), doc(24), doc(40)])
    );
    let before = claims(&sql).await?;
    let mut changes = admission.subscribe_changes();
    let mut other = admission.subscribe_changes();
    admission.coverage(&doc(1)).await?;
    admission.discovery_documents().await?;
    assert!(admission.discovery_work().await?.is_none());
    admission
        .invalidate_discovery(BTreeSet::from([doc(4), doc(9)]))
        .await?;
    assert!(!changes.has_changed()? && !other.has_changed()?);
    assert_eq!(
        sql.query(
            "SELECT dirty FROM big_blobs_discovery_documents WHERE drawer_id = ?",
            (doc(4).to_string(),),
        )
        .await?
        .all::<bool>()?,
        vec![false]
    );
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM big_blobs_discovery_documents WHERE drawer_id = ?",
            (doc(9).to_string(),),
        )
        .await?
        .all::<u64>()?,
        vec![0]
    );
    admission
        .invalidate_discovery(BTreeSet::from([doc(2)]))
        .await?;
    assert!(changes.has_changed()? && other.has_changed()?);
    assert_eq!(
        sql.query(
            "SELECT dirty
                  , waiting
                  , attempt
               FROM big_blobs_discovery_documents
              WHERE drawer_id = ?",
            (doc(2).to_string(),),
        )
        .await?
        .all::<(bool, bool, Option<u64>)>()?,
        vec![(true, true, None)]
    );
    assert_eq!(claims(&sql).await?, before);
    changes.borrow_and_update();
    other.borrow_and_update();
    admission
        .invalidate_discovery(BTreeSet::from([doc(2)]))
        .await?;
    assert!(!changes.has_changed()?);
    let (drawer, stale) = admission.discovery_work().await?.unwrap();
    assert_eq!(drawer, doc(2));
    admission
        .invalidate_discovery(BTreeSet::from([doc(2)]))
        .await?;
    admission
        .observe_drawer(&drawer, stale, Some(snapshot(30, &[4])))
        .await?;
    assert_eq!(claims(&sql).await?, before);
    let (drawer, current) = admission.discovery_work().await?.unwrap();
    assert_eq!(drawer, doc(2));
    assert!(current > stale);
    admission
        .observe_drawer(&drawer, current, Some(snapshots[&drawer].clone()))
        .await?;
    assert!(admission.discovery_work().await?.is_none());
    assert_eq!(claims(&sql).await?, before);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn identical_snapshots_do_not_rewrite_claims_edges_or_pair_generations() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    let snapshots = BTreeMap::from([(doc(1), snapshot(20, &[2])), (doc(2), snapshot(22, &[]))]);
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    drain(&admission, &snapshots).await?;
    candidate(&admission, 22).await?;
    let digest = BlobId::new([90; 32]);
    let attempt = admission.begin(&doc(22), digest).await?.unwrap();
    let before = admission.state(&doc(22), digest).await?;
    for (table, operations) in [
        (
            "big_blobs_mirror_claims",
            &["INSERT", "UPDATE", "DELETE"][..],
        ),
        (
            "big_blobs_discovery_claims",
            &["INSERT", "UPDATE", "DELETE"][..],
        ),
        (
            "big_blobs_discovery_reach",
            &["INSERT", "UPDATE", "DELETE"][..],
        ),
        (
            "big_blobs_discovery_edges",
            &["INSERT", "UPDATE", "DELETE"][..],
        ),
        ("big_blobs_discovery_slots", &["UPDATE", "DELETE"][..]),
    ] {
        for operation in operations {
            sql.execute_batch(&format!(
                "CREATE TRIGGER reject_noop_{table}_{operation} BEFORE {operation} ON {table}
                 BEGIN SELECT RAISE(ABORT, 'unchanged discovery fact rewritten'); END;"
            ))
            .await?;
        }
    }
    admission.rearm_discovery().await?;
    assert_eq!(drain(&admission, &snapshots).await?, 2);
    assert_eq!(admission.state(&doc(22), digest).await?, before);
    assert_eq!(
        claims(&sql).await?,
        BTreeMap::from([(doc(20), 1), (doc(22), 1)])
    );
    assert!(admission.complete(&attempt, true).await?);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn graph_and_claim_failure_rolls_back_the_snapshot_and_work_receipt_together() -> Res<()> {
    let (admission, stop, sql) = fixture().await?;
    admission.select_root(&doc(1), Some(ciphertext())).await?;
    let (drawer, generation) = admission.discovery_work().await?.unwrap();
    let mut changes = admission.subscribe_changes();
    sql.execute_batch(
        "CREATE TRIGGER reject_discovery BEFORE INSERT ON big_blobs_mirror_claims
         BEGIN SELECT RAISE(ABORT, 'discovery fixture failure'); END;",
    )
    .await?;
    let observed = snapshot(20, &[2]);
    let error = admission
        .observe_drawer(&drawer, generation, Some(observed.clone()))
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("discovery fixture failure"));
    assert!(!changes.has_changed()?);
    assert!(claims(&sql).await?.is_empty());
    assert_eq!(admission.discovery_documents().await?, vec![doc(1)]);
    assert_eq!(
        sql.query("SELECT COUNT(*) FROM big_blobs_discovery_edges", ())
            .await?
            .all::<u64>()?,
        vec![0]
    );
    sql.execute_batch("DROP TRIGGER reject_discovery;").await?;
    admission
        .observe_drawer(&drawer, generation, Some(observed))
        .await?;
    assert!(changes.has_changed()?);
    changes.borrow_and_update();
    assert_eq!(claims(&sql).await?, BTreeMap::from([(doc(20), 1)]));
    assert_eq!(admission.discovery_documents().await?.len(), 2);
    stop.stop().await;
    Ok(())
}
