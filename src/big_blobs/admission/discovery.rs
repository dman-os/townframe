use super::*;
mod sqlite;
use big_cabinet_core::drawer::MetaRoot;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct InventorySelection {
    pub root: MetaRoot,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrawerSnapshot {
    pub self_doc: bool,
    pub roots: BTreeSet<MetaRoot>,
    pub inventories: BTreeMap<(MetaRoot, String), DocId>,
    pub children: BTreeSet<DocId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageGap {
    UnselectedRoot,
    UnsupportedSelfDoc,
    Metadata {
        drawer: DocId,
        selection: InventorySelection,
    },
    InventoryMissing {
        drawer: DocId,
        selection: InventorySelection,
    },
    InventoryUnreadable {
        inventory: DocId,
    },
    PinsIncomplete {
        inventory: DocId,
    },
    ObjectMissing {
        inventory: DocId,
        digest: BlobId,
    },
    BytesMissing {
        inventory: DocId,
        digest: BlobId,
    },
    PublicationPending {
        inventory: DocId,
        digest: BlobId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    pub gaps: Vec<CoverageGap>,
}

impl Coverage {
    pub fn is_complete(&self) -> bool {
        self.gaps.is_empty()
    }
}

impl State {
    pub(super) async fn initialize_discovery(&self) -> Res<()> {
        self.sql
            .apply_migrations(
                "big_blobs_discovery_migrations",
                sqlite::queries::MIGRATIONS,
            )
            .await?;
        self.rearm_discovery().await
    }

    pub(super) async fn discovery_policy(
        &self,
        drawer: &DocId,
        policy: &str,
        selection: Option<InventorySelection>,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let changed = if let Some(selection) = selection {
            sqlite::queries::set_policy(
                &tx,
                drawer.to_string(),
                policy.to_owned(),
                selection.root.as_str().to_owned(),
                selection.name,
            )
            .await?
            .rows_affected
        } else {
            sqlite::queries::delete_policy(&tx, drawer.to_string(), policy.to_owned())
                .await?
                .rows_affected
        };
        if changed == 0 {
            tx.rollback().await?;
            return Ok(());
        }
        sqlite::queries::ensure_document(&tx, drawer.to_string()).await?;
        Self::reconcile_discovery(&tx).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn reconcile_discovery(tx: &Tx) -> Res<()> {
        sqlite::queries::clear_next_reach(tx).await?;
        sqlite::queries::populate_next_reach(tx).await?;
        sqlite::queries::mark_reach_changed(tx).await?;
        sqlite::queries::delete_stale_reach(tx).await?;
        sqlite::queries::insert_new_reach(tx).await?;
        sqlite::queries::clear_next_claims(tx).await?;
        sqlite::queries::populate_next_claims(tx).await?;
        sqlite::queries::mark_claim_pairs(tx).await?;
        sqlite::queries::delete_mirror_claims(tx).await?;
        sqlite::queries::insert_mirror_claims(tx).await?;
        sqlite::queries::delete_stale_claims(tx).await?;
        sqlite::queries::insert_new_claims(tx).await?;
        sqlite::queries::clear_next_claims_final(tx).await?;
        for inventory in sqlite::queries::claim_inventories(tx).await? {
            let parsed: DocId = inventory.parse()?;
            let part = crate::workers::inventory_part(&parsed);
            sqlite::queries::register_inventory(tx, inventory, part.as_bytes().to_vec()).await?;
        }
        Ok(())
    }

    pub(super) async fn discovery_work(&self) -> Res<Option<(DocId, u64)>> {
        let tx = self.sql.begin_write().await?;
        let work = sqlite::queries::work(&tx).await?.into_iter().next();
        if let Some(row) = &work {
            sqlite::queries::mark_work(&tx, row.drawer_id.clone()).await?;
        }
        tx.commit().await?;
        work.map(|row| Ok((row.drawer_id.parse()?, row.generation as u64)))
            .transpose()
    }

    pub(super) async fn discovery_documents(&self) -> Res<Vec<DocId>> {
        sqlite::queries::documents(&self.sql)
            .await?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub(super) async fn selected_inventories(&self) -> Res<Vec<DocId>> {
        sqlite::queries::selected_inventories(&self.sql)
            .await?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub(super) async fn invalidate_discovery(&self, documents: BTreeSet<DocId>) -> Res<()> {
        let documents = documents
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let changed = sqlite::queries::invalidate(&self.sql, serde_json::to_string(&documents)?)
            .await?
            .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub(super) async fn rearm_discovery(&self) -> Res<()> {
        let changed = sqlite::queries::rearm(&self.sql).await?.rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub(super) async fn observe_drawer(
        &self,
        drawer: &DocId,
        generation: u64,
        snapshot: Option<DrawerSnapshot>,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let changed = sqlite::queries::observe(
            &tx,
            snapshot.is_none(),
            drawer.to_string(),
            i64::try_from(generation)?,
            i64::try_from(generation)?,
        )
        .await?
        .rows_affected;
        if changed == 0 {
            tx.rollback().await?;
            return Ok(());
        }
        if let Some(snapshot) = snapshot {
            Self::store_drawer_snapshot(&tx, drawer, snapshot).await?;
            Self::reconcile_discovery(&tx).await?;
        }
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn store_drawer_snapshot(tx: &Tx, drawer: &DocId, snapshot: DrawerSnapshot) -> Res<()> {
        sqlite::queries::mark_observed(tx, snapshot.self_doc, drawer.to_string()).await?;
        let children = if snapshot.self_doc {
            Vec::new()
        } else {
            snapshot.children.iter().map(ToString::to_string).collect()
        };
        let roots = snapshot
            .roots
            .iter()
            .copied()
            .map(MetaRoot::as_str)
            .collect::<Vec<_>>();
        sqlite::queries::remove_children(tx, drawer.to_string(), serde_json::to_string(&children)?)
            .await?;
        sqlite::queries::remove_roots(tx, drawer.to_string(), serde_json::to_string(&roots)?)
            .await?;
        if snapshot.self_doc {
            sqlite::queries::delete_slots(tx, drawer.to_string()).await?;
            return Ok(());
        }
        for child in snapshot.children {
            let child = child.to_string();
            sqlite::queries::ensure_child(tx, child.clone()).await?;
            sqlite::queries::insert_edge(tx, drawer.to_string(), child).await?;
        }
        for root in snapshot.roots {
            let root_name = root.as_str().to_owned();
            sqlite::queries::upsert_root(tx, drawer.to_string(), root_name.clone()).await?;
            let names = snapshot
                .inventories
                .keys()
                .filter(|(slot_root, _)| *slot_root == root)
                .map(|(_, name)| name)
                .collect::<Vec<_>>();
            sqlite::queries::delete_old_slots(
                tx,
                drawer.to_string(),
                root_name.clone(),
                serde_json::to_string(&names)?,
            )
            .await?;
            for ((slot_root, name), inventory) in &snapshot.inventories {
                if *slot_root == root {
                    sqlite::queries::upsert_slot(
                        tx,
                        drawer.to_string(),
                        root_name.clone(),
                        name.clone(),
                        inventory.to_string(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn coverage(&self, root: &DocId) -> Res<Coverage> {
        let tx = self.sql.begin_read().await?;
        let mut gaps = Vec::new();
        if sqlite::queries::root_selected(&tx, root.to_string())
            .await?
            .is_empty()
        {
            gaps.push(CoverageGap::UnselectedRoot);
        }
        Self::discovery_metadata_gaps(&tx, root, &mut gaps).await?;
        Self::discovery_inventory_gaps(&tx, root, &mut gaps).await?;
        tx.rollback().await?;
        Ok(Coverage { gaps })
    }

    async fn discovery_metadata_gaps(
        tx: &sqlite_rs::ReadTx,
        root: &DocId,
        gaps: &mut Vec<CoverageGap>,
    ) -> Res<()> {
        for row in sqlite::queries::metadata_gaps(tx, root.to_string()).await? {
            let drawer: DocId = row.drawer_id.parse()?;
            let selection = InventorySelection {
                root: row.meta_root.parse()?,
                name: row.name,
            };
            if row.self_doc && drawer == *root {
                gaps.push(CoverageGap::UnsupportedSelfDoc);
            }
            if !row.observed || row.waiting || (!row.self_doc && !row.authoritative) {
                gaps.push(CoverageGap::Metadata {
                    drawer: drawer.clone(),
                    selection: selection.clone(),
                });
            }
            if !row.self_doc && row.inventory_id.is_none() {
                gaps.push(CoverageGap::InventoryMissing { drawer, selection });
            }
        }
        Ok(())
    }

    async fn discovery_inventory_gaps(
        tx: &sqlite_rs::ReadTx,
        root: &DocId,
        gaps: &mut Vec<CoverageGap>,
    ) -> Res<()> {
        for row in sqlite::queries::inventory_gaps(tx, root.to_string()).await? {
            let inventory: DocId = row.inventory_id.parse()?;
            if !row.readable {
                gaps.push(CoverageGap::InventoryUnreadable {
                    inventory: inventory.clone(),
                });
            }
            if !row.pins_complete {
                gaps.push(CoverageGap::PinsIncomplete { inventory });
            }
        }
        for row in sqlite::queries::coverage_pairs(tx, root.to_string()).await? {
            let inventory: DocId = row.inventory_id.parse()?;
            let digest: BlobId = row.digest.parse()?;
            if row.dirty {
                gaps.push(CoverageGap::PublicationPending {
                    inventory: inventory.clone(),
                    digest,
                });
            }
            if !row.pinned {
                continue;
            }
            if !row.observed {
                gaps.push(CoverageGap::ObjectMissing {
                    inventory: inventory.clone(),
                    digest,
                });
            }
            if !row.bytes {
                gaps.push(CoverageGap::BytesMissing { inventory, digest });
            }
        }
        Ok(())
    }
}
