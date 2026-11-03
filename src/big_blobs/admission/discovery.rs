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
            tx.execute(
                "INSERT INTO big_blobs_discovery_policies (drawer_id, policy, meta_root, name)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (drawer_id, policy) DO UPDATE
                     SET meta_root = excluded.meta_root
                       , name = excluded.name
                   WHERE meta_root != excluded.meta_root OR name != excluded.name",
                (
                    drawer.to_string(),
                    policy,
                    selection.root.as_str(),
                    selection.name,
                ),
            )
            .await?
            .rows_affected
        } else {
            tx.execute(
                "DELETE FROM big_blobs_discovery_policies WHERE drawer_id = ? AND policy = ?",
                (drawer.to_string(), policy),
            )
            .await?
            .rows_affected
        };
        if changed == 0 {
            tx.rollback().await?;
            return Ok(());
        }
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_discovery_documents (drawer_id) VALUES (?)",
            (drawer.to_string(),),
        )
        .await?;
        Self::reconcile_discovery(&tx).await?;
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }

    async fn reconcile_discovery(tx: &Tx) -> Res<()> {
        tx.execute_batch(
            "CREATE TEMP TABLE big_blobs_next_reach (
                 root_id TEXT NOT NULL
               , drawer_id TEXT NOT NULL
               , meta_root TEXT NOT NULL
               , name TEXT NOT NULL
               , PRIMARY KEY (root_id, drawer_id, meta_root, name)
             );
             INSERT INTO big_blobs_next_reach
             WITH RECURSIVE reach (root_id, drawer_id, meta_root, name) AS (
                 SELECT p.drawer_id
                      , p.drawer_id
                      , COALESCE(o.meta_root, p.meta_root)
                      , COALESCE(o.name, p.name)
                   FROM big_blobs_discovery_policies p
                   LEFT JOIN big_blobs_discovery_policies o
                     ON o.drawer_id = p.drawer_id AND o.policy = 'override'
                  WHERE p.policy = 'root'
                 UNION
                 SELECT r.root_id
                      , e.child_id
                      , COALESCE(o.meta_root, r.meta_root)
                      , COALESCE(o.name, r.name)
                   FROM reach r
                   JOIN big_blobs_discovery_documents d ON d.drawer_id = r.drawer_id
                   JOIN big_blobs_discovery_edges e ON e.parent_id = r.drawer_id
                   LEFT JOIN big_blobs_discovery_policies o
                     ON o.drawer_id = e.child_id AND o.policy = 'override'
                  WHERE d.self_doc = 0
             )
             SELECT * FROM reach;
             UPDATE big_blobs_discovery_documents
                SET generation = generation + 1
                  , attempt = NULL
                  , waiting = 1
                  , dirty = EXISTS (SELECT 1 FROM big_blobs_next_reach n
                                     WHERE n.drawer_id = big_blobs_discovery_documents.drawer_id)
              WHERE drawer_id IN (
                 SELECT n.drawer_id FROM big_blobs_next_reach n
                  WHERE NOT EXISTS (SELECT 1 FROM big_blobs_discovery_reach r
                                     WHERE (r.root_id, r.drawer_id, r.meta_root, r.name)
                                         = (n.root_id, n.drawer_id, n.meta_root, n.name))
                 UNION
                 SELECT r.drawer_id FROM big_blobs_discovery_reach r
                  WHERE NOT EXISTS (SELECT 1 FROM big_blobs_next_reach n
                                     WHERE (r.root_id, r.drawer_id, r.meta_root, r.name)
                                         = (n.root_id, n.drawer_id, n.meta_root, n.name))
             );",
        )
        .await?;
        Self::reconcile_discovery_claims(tx).await?;
        tx.execute_batch(
            "DELETE FROM big_blobs_discovery_reach
              WHERE (root_id, drawer_id, meta_root, name) NOT IN (SELECT * FROM big_blobs_next_reach);
             INSERT INTO big_blobs_discovery_reach
             SELECT * FROM big_blobs_next_reach
             EXCEPT SELECT * FROM big_blobs_discovery_reach;
             DROP TABLE big_blobs_next_reach;",
        ).await?;
        Ok(())
    }

    async fn reconcile_discovery_claims(tx: &Tx) -> Res<()> {
        tx.execute_batch(
            "CREATE TEMP TABLE big_blobs_next_claims AS
             SELECT s.inventory_id
                  , 'discovery/' || r.root_id || '/' || r.drawer_id || '/' || r.meta_root
                    || '/' || hex(CAST(r.name AS BLOB)) || '/' || s.inventory_id AS owner
                  , r.root_id
                  , r.drawer_id
                  , r.meta_root
                  , r.name
               FROM big_blobs_next_reach r
               JOIN big_blobs_discovery_documents d ON d.drawer_id = r.drawer_id
               JOIN big_blobs_discovery_slots s
                 ON (s.drawer_id, s.meta_root, s.name) = (r.drawer_id, r.meta_root, r.name)
              WHERE d.self_doc = 0;
             CREATE UNIQUE INDEX big_blobs_next_claims_key ON big_blobs_next_claims (inventory_id, owner);
             UPDATE big_blobs_admission_pairs
                SET generation = generation + 1
                  , attempt = NULL
                  , dirty = 1
              WHERE inventory_id IN (
                 SELECT inventory_id FROM big_blobs_discovery_claims
                  WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_next_claims)
                 UNION
                 SELECT inventory_id FROM big_blobs_next_claims
                  WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_discovery_claims)
             );
             DELETE FROM big_blobs_mirror_claims
              WHERE (inventory_id, owner) IN (
                 SELECT inventory_id, owner FROM big_blobs_discovery_claims
                 EXCEPT
                 SELECT inventory_id, owner FROM big_blobs_next_claims
             );
             INSERT INTO big_blobs_mirror_claims (inventory_id, owner)
             SELECT inventory_id, owner FROM big_blobs_next_claims
             EXCEPT SELECT inventory_id, owner FROM big_blobs_discovery_claims;
             DELETE FROM big_blobs_discovery_claims
              WHERE (inventory_id, owner) NOT IN (SELECT inventory_id, owner FROM big_blobs_next_claims);
             INSERT INTO big_blobs_discovery_claims
             SELECT * FROM big_blobs_next_claims
             EXCEPT SELECT * FROM big_blobs_discovery_claims;",
        ).await?;
        let inventories = tx
            .query(
                "SELECT DISTINCT c.inventory_id
               FROM big_blobs_next_claims c
               LEFT JOIN big_blobs_inventory_registry r USING (inventory_id)
              WHERE r.inventory_id IS NULL",
                (),
            )
            .await?
            .all::<String>()?;
        for inventory in inventories {
            let part = crate::workers::inventory_part(&inventory.parse()?);
            tx.execute(
                "INSERT INTO big_blobs_inventory_registry (inventory_id, part_id) VALUES (?, ?)",
                (inventory, part.as_bytes().to_vec()),
            )
            .await?;
        }
        tx.execute_batch("DROP TABLE big_blobs_next_claims;")
            .await?;
        Ok(())
    }

    pub(super) async fn discovery_work(&self) -> Res<Option<(DocId, u64)>> {
        let tx = self.sql.begin_write().await?;
        let work = tx.query(
            "SELECT d.drawer_id
                  , d.generation
               FROM big_blobs_discovery_documents d
              WHERE d.dirty = 1
                AND EXISTS (SELECT 1 FROM big_blobs_discovery_reach r WHERE r.drawer_id = d.drawer_id)
              ORDER BY d.drawer_id
              LIMIT 1",
            (),
        ).await?.all::<(String, u64)>()?.into_iter().next();
        if let Some((drawer, _)) = &work {
            tx.execute(
                "UPDATE big_blobs_discovery_documents SET dirty = 0, attempt = generation WHERE drawer_id = ?",
                (drawer.clone(),),
            ).await?;
        }
        tx.commit().await?;
        work.map(|(drawer, generation)| Ok((drawer.parse()?, generation)))
            .transpose()
    }

    pub(super) async fn discovery_documents(&self) -> Res<Vec<DocId>> {
        self.sql
            .query(
                "SELECT DISTINCT drawer_id FROM big_blobs_discovery_reach ORDER BY drawer_id",
                (),
            )
            .await?
            .all::<String>()?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub(super) async fn selected_inventories(&self) -> Res<Vec<DocId>> {
        self.sql
            .query(
                "SELECT DISTINCT inventory_id FROM big_blobs_mirror_claims ORDER BY inventory_id",
                (),
            )
            .await?
            .all::<String>()?
            .into_iter()
            .map(|id| Ok(id.parse()?))
            .collect()
    }

    pub(super) async fn invalidate_discovery(&self, documents: BTreeSet<DocId>) -> Res<()> {
        let documents = documents
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let changed = self
            .sql
            .execute(
                "UPDATE big_blobs_discovery_documents
                SET generation = generation + 1
                  , dirty = 1
                  , waiting = 1
                  , attempt = NULL
              WHERE dirty = 0
                AND drawer_id IN (SELECT value FROM json_each(?))
                AND EXISTS (SELECT 1 FROM big_blobs_discovery_reach r
                             WHERE r.drawer_id = big_blobs_discovery_documents.drawer_id)",
                (serde_json::to_string(&documents)?,),
            )
            .await?
            .rows_affected;
        if changed > 0 {
            self.changes.send_modify(|revision| *revision += 1);
            self.changed.notify_one();
        }
        Ok(())
    }

    pub(super) async fn rearm_discovery(&self) -> Res<()> {
        let changed = self
            .sql
            .execute(
                "UPDATE big_blobs_discovery_documents
                SET generation = generation + 1
                  , dirty = 1
                  , waiting = 1
                  , attempt = NULL
              WHERE dirty = 0
                AND EXISTS (SELECT 1 FROM big_blobs_discovery_reach r
                             WHERE r.drawer_id = big_blobs_discovery_documents.drawer_id)",
                (),
            )
            .await?
            .rows_affected;
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
        let changed = tx
            .execute(
                "UPDATE big_blobs_discovery_documents
                SET attempt = NULL
                  , waiting = ?
              WHERE drawer_id = ? AND generation = ? AND attempt = ?",
                (
                    snapshot.is_none(),
                    drawer.to_string(),
                    i64::try_from(generation)?,
                    i64::try_from(generation)?,
                ),
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
        tx.execute(
            "UPDATE big_blobs_discovery_documents SET observed = 1, self_doc = ? WHERE drawer_id = ?",
            (snapshot.self_doc, drawer.to_string()),
        ).await?;
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
        tx.execute(
            "DELETE FROM big_blobs_discovery_edges
              WHERE parent_id = ? AND child_id NOT IN (SELECT value FROM json_each(?))",
            (drawer.to_string(), serde_json::to_string(&children)?),
        )
        .await?;
        tx.execute(
            "UPDATE big_blobs_discovery_roots SET authoritative = 0
              WHERE drawer_id = ? AND authoritative = 1
                AND meta_root NOT IN (SELECT value FROM json_each(?))",
            (drawer.to_string(), serde_json::to_string(&roots)?),
        )
        .await?;
        if snapshot.self_doc {
            tx.execute(
                "DELETE FROM big_blobs_discovery_slots WHERE drawer_id = ?",
                (drawer.to_string(),),
            )
            .await?;
            return Ok(());
        }
        for child in snapshot.children {
            tx.execute(
                "INSERT OR IGNORE INTO big_blobs_discovery_documents (drawer_id) VALUES (?)",
                (child.to_string(),),
            )
            .await?;
            tx.execute(
                "INSERT INTO big_blobs_discovery_edges (parent_id, child_id)
                 SELECT ?, ? WHERE NOT EXISTS (
                     SELECT 1 FROM big_blobs_discovery_edges WHERE parent_id = ? AND child_id = ?
                 )",
                (
                    drawer.to_string(),
                    child.to_string(),
                    drawer.to_string(),
                    child.to_string(),
                ),
            )
            .await?;
        }
        for root in snapshot.roots {
            tx.execute(
                "INSERT INTO big_blobs_discovery_roots (drawer_id, meta_root, authoritative) VALUES (?, ?, 1)
                 ON CONFLICT (drawer_id, meta_root) DO UPDATE SET authoritative = 1
                   WHERE authoritative != 1",
                (drawer.to_string(), root.as_str()),
            ).await?;
            let names = snapshot
                .inventories
                .keys()
                .filter(|(slot_root, _)| *slot_root == root)
                .map(|(_, name)| name)
                .collect::<Vec<_>>();
            tx.execute(
                "DELETE FROM big_blobs_discovery_slots
                  WHERE drawer_id = ? AND meta_root = ?
                    AND name NOT IN (SELECT value FROM json_each(?))",
                (
                    drawer.to_string(),
                    root.as_str(),
                    serde_json::to_string(&names)?,
                ),
            )
            .await?;
            for ((slot_root, name), inventory) in &snapshot.inventories {
                if *slot_root == root {
                    tx.execute(
                        "INSERT INTO big_blobs_discovery_slots (drawer_id, meta_root, name, inventory_id)
                          VALUES (?, ?, ?, ?)
                          ON CONFLICT (drawer_id, meta_root, name) DO UPDATE
                              SET inventory_id = excluded.inventory_id
                            WHERE inventory_id != excluded.inventory_id",
                        (drawer.to_string(), root.as_str(), name.clone(), inventory.to_string()),
                    ).await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn coverage(&self, root: &DocId) -> Res<Coverage> {
        let tx = self.sql.begin_read().await?;
        let mut gaps = Vec::new();
        let selected = tx.query(
            "SELECT 1 FROM big_blobs_discovery_policies WHERE drawer_id = ? AND policy = 'root'",
            (root.to_string(),),
        ).await?.all::<i64>()?;
        if selected.is_empty() {
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
        let rows = tx
            .query(
                "SELECT r.drawer_id
                  , r.meta_root
                  , r.name
                  , d.observed
                  , d.waiting
                  , d.self_doc
                  , COALESCE(m.authoritative, 0) AS authoritative
                  , s.inventory_id
               FROM big_blobs_discovery_reach r
               JOIN big_blobs_discovery_documents d USING (drawer_id)
               LEFT JOIN big_blobs_discovery_roots m USING (drawer_id, meta_root)
               LEFT JOIN big_blobs_discovery_slots s USING (drawer_id, meta_root, name)
              WHERE r.root_id = ?
              ORDER BY r.drawer_id, r.meta_root, r.name",
                (root.to_string(),),
            )
            .await?;
        for row in rows.rows {
            let drawer: DocId = row.get::<String>("drawer_id")?.parse()?;
            let selection = InventorySelection {
                root: row.get::<String>("meta_root")?.parse()?,
                name: row.get("name")?,
            };
            let self_doc = row.get::<bool>("self_doc")?;
            if self_doc && drawer == *root {
                gaps.push(CoverageGap::UnsupportedSelfDoc);
            }
            if !row.get::<bool>("observed")?
                || row.get::<bool>("waiting")?
                || (!self_doc && !row.get::<bool>("authoritative")?)
            {
                gaps.push(CoverageGap::Metadata {
                    drawer: drawer.clone(),
                    selection: selection.clone(),
                });
            }
            if !self_doc && row.get::<Option<String>>("inventory_id")?.is_none() {
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
        let inventories = tx
            .query(
                "SELECT DISTINCT r.inventory_id
                  , r.readable
                  , r.pins_complete
               FROM big_blobs_discovery_claims c
               JOIN big_blobs_inventory_registry r USING (inventory_id)
              WHERE c.root_id = ?
              ORDER BY r.inventory_id",
                (root.to_string(),),
            )
            .await?
            .all::<(String, bool, bool)>()?;
        for (inventory, readable, complete) in inventories {
            let inventory: DocId = inventory.parse()?;
            if !readable {
                gaps.push(CoverageGap::InventoryUnreadable {
                    inventory: inventory.clone(),
                });
            }
            if !complete {
                gaps.push(CoverageGap::PinsIncomplete { inventory });
            }
        }
        let rows = tx
            .query(
                "SELECT p.inventory_id
                   , p.digest
                   , p.bytes
                   , p.dirty
                   , p.length_octets IS NOT NULL AS pinned
                   , (EXISTS (SELECT 1 FROM big_blobs_source_observations o
                                WHERE o.inventory_id = p.inventory_id AND o.digest = p.digest
                                  AND o.present = 1)
                       OR EXISTS (SELECT 1 FROM big_blobs_producer_claims w
                                   WHERE w.inventory_id = p.inventory_id AND w.digest = p.digest)) AS observed
                FROM big_blobs_admission_pairs p
               WHERE (p.length_octets IS NOT NULL OR p.dirty = 1)
                 AND EXISTS (SELECT 1 FROM big_blobs_discovery_claims c
                              WHERE c.root_id = ? AND c.inventory_id = p.inventory_id)
               ORDER BY p.inventory_id, p.digest",
                (root.to_string(),),
            )
            .await?;
        for row in rows.rows {
            let inventory: DocId = row.get::<String>("inventory_id")?.parse()?;
            let digest = row.get::<String>("digest")?.parse()?;
            if row.get::<bool>("dirty")? {
                gaps.push(CoverageGap::PublicationPending {
                    inventory: inventory.clone(),
                    digest,
                });
            }
            if !row.get::<bool>("pinned")? {
                continue;
            }
            if !row.get::<bool>("observed")? {
                gaps.push(CoverageGap::ObjectMissing {
                    inventory: inventory.clone(),
                    digest,
                });
            }
            if !row.get::<bool>("bytes")? {
                gaps.push(CoverageGap::BytesMissing { inventory, digest });
            }
        }
        Ok(())
    }
}
