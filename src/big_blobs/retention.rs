use crate::documents::RetainedRepresentation;
use crate::ownership::OwnershipTransaction;
use crate::service::BlobService;
use crate::workers::MetadataCapabilities;
use big_blobs_core::BlobId;
use big_blobs_core::BlobPin;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerMetaDoc;
use big_cabinet_core::drawer::MetaRoot;
use big_repo::SharedPartStore;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre;

pub(crate) struct Retention {
    sql: Database,
    service: Arc<BlobService>,
    docs: Client<DocsProtocol>,
    metadata: MetadataCapabilities,
    parts: SharedPartStore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WriterTarget {
    drawer: DocId,
    root: MetaRoot,
    name: String,
    inventory: DocId,
}

impl Retention {
    pub(crate) async fn migrate(sql: &Database) -> Res<()> {
        sql.apply_migrations("big_blobs_retention_migrations", &[
            sqlite_rs::Migration {
                version: 1,
                name: "representation_retention",
                sql: "CREATE TABLE big_blobs_retention (
                       doc_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , plaintext TEXT
                     , length_octets INTEGER NOT NULL
                     , PRIMARY KEY (doc_id, digest)
                  ) STRICT;
                  CREATE INDEX big_blobs_retention_digest ON big_blobs_retention (digest);
                  CREATE TABLE big_blobs_retention_effects (digest TEXT PRIMARY KEY NOT NULL) STRICT;",
            },
            sqlite_rs::Migration {
                version: 2,
                name: "root_aware_writer_targets",
                sql: "ALTER TABLE big_blobs_retention ADD COLUMN root TEXT CHECK (root IN ('public', 'sensitive'));
                  UPDATE big_blobs_retention SET root = 'public' WHERE plaintext IS NOT NULL;
                  CREATE TABLE big_blobs_writer_targets (
                       doc_id TEXT NOT NULL
                     , drawer_id TEXT NOT NULL
                     , root TEXT NOT NULL CHECK (root IN ('public', 'sensitive'))
                     , name TEXT NOT NULL
                     , inventory_id TEXT NOT NULL
                     , PRIMARY KEY (doc_id, drawer_id, root, name)
                  ) STRICT;
                  CREATE INDEX big_blobs_writer_targets_inventory ON big_blobs_writer_targets (inventory_id);
                  CREATE TABLE big_blobs_writer_claims (
                       doc_id TEXT NOT NULL
                     , drawer_id TEXT NOT NULL
                     , root TEXT NOT NULL CHECK (root IN ('public', 'sensitive'))
                     , name TEXT NOT NULL
                     , inventory_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , length_octets INTEGER NOT NULL
                     , PRIMARY KEY (doc_id, drawer_id, root, name, inventory_id, digest)
                  ) STRICT;
                  CREATE INDEX big_blobs_writer_claims_pair ON big_blobs_writer_claims (inventory_id, digest);
                  CREATE INDEX big_blobs_writer_claims_digest ON big_blobs_writer_claims (digest);
                  CREATE TABLE big_blobs_writer_effects (
                       inventory_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , PRIMARY KEY (inventory_id, digest)
                  ) STRICT;",
            },
        ]).await?;
        Ok(())
    }

    pub async fn new(
        sql: Database,
        service: Arc<BlobService>,
        docs: Client<DocsProtocol>,
        metadata: MetadataCapabilities,
        parts: SharedPartStore,
    ) -> Res<Self> {
        Self::migrate(&sql).await?;
        let retention = Self {
            sql,
            service,
            docs,
            metadata,
            parts,
        };
        let service = Arc::clone(&retention.service);
        retention
            .service
            .ownership
            .transaction(move |ownership| {
                Box::pin(async move {
                    ownership
                        .sql()
                        .execute(
                            "INSERT OR IGNORE INTO big_blobs_retention_effects (digest)
             SELECT digest FROM big_blobs_retention
             UNION SELECT digest FROM big_blobs_writer_claims
             UNION SELECT digest FROM big_blobs_writer_effects",
                            (),
                        )
                        .await?;
                    let digests = ownership
                        .sql()
                        .query("SELECT digest FROM big_blobs_retention_effects", ())
                        .await?
                        .all::<String>()?
                        .into_iter()
                        .collect();
                    Self::reconcile(&service, ownership, digests).await
                })
            })
            .await?;
        Ok(retention)
    }

    async fn reconcile(
        service: &BlobService,
        ownership: &mut OwnershipTransaction,
        digests: BTreeSet<String>,
    ) -> Res<()> {
        for digest in digests {
            let id: BlobId = digest.parse()?;
            let available = ownership.contains(&id).await? || service.stat(&id).await?.is_some();
            let documents = ownership
                .sql()
                .query(
                    "SELECT doc_id FROM big_blobs_retention WHERE digest = ?",
                    (digest.as_str(),),
                )
                .await?
                .all::<String>()?;
            let claims = ownership
                .sql()
                .query(
                    "SELECT doc_id
                      , drawer_id
                      , root
                      , name
                      , inventory_id
                   FROM big_blobs_writer_claims
                  WHERE digest = ?",
                    (digest.as_str(),),
                )
                .await?
                .all::<(String, String, String, String, String)>()?;
            let effects = ownership
                .sql()
                .query(
                    "SELECT inventory_id FROM big_blobs_writer_effects WHERE digest = ?",
                    (digest.as_str(),),
                )
                .await?
                .all::<String>()?;
            let mut document_owners = Vec::new();
            let mut writer_owners = Vec::new();
            if available {
                for document in documents {
                    document_owners.push(format!("document:retention:{document}"));
                }
                for claim in claims {
                    writer_owners.push(format!(
                        "writer:retention:claim:{}",
                        serde_json::to_string(&claim)?
                    ));
                }
                for inventory in effects {
                    writer_owners.push(format!("writer:retention:effect:{inventory}"));
                }
            }
            let document = ("document:retention:", document_owners);
            let writer = ("writer:retention:", writer_owners);
            let ordered = if document.1.is_empty() {
                [writer, document]
            } else {
                [document, writer]
            };
            for (prefix, owners) in ordered {
                ownership.reconcile(prefix, &id, &owners).await?;
            }
        }
        Ok(())
    }

    async fn read(&mut self, doc: &DocId, datums: BTreeSet<DatumKey>) -> Res<Option<DatumsRead>> {
        match self
            .metadata
            .call(doc, false, |capability| {
                self.docs.call(
                    RequestContext::root(),
                    ReadDatums {
                        capability,
                        doc_id: doc.clone(),
                        at: At::Current,
                        datums: Some(datums),
                        optimistic: false,
                    },
                )
            })
            .await
        {
            Ok(read) => Ok(Some(read)),
            Err(ClientError::Operation(
                DocsError::Denied
                | DocsError::DocNotFound
                | DocsError::Pending { .. }
                | DocsError::UnknownHeads { .. }
                | DocsError::AuthorityUnavailable(_)
                | DocsError::Access(_),
            )) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn candidates(&self, doc: &DocId) -> Res<Vec<WriterTarget>> {
        let rows = self.sql.query(
            "WITH containing(drawer_id) AS (
                 SELECT drawer_id FROM big_cabinet_drawer_docs WHERE doc_id = ?1
                 UNION
                 SELECT links.parent_id
                   FROM big_cabinet_drawer_children AS links
                   JOIN big_cabinet_drawers AS child ON child.drawer_id = links.child_id
                  WHERE links.child_id = ?1
                    AND json_extract(child.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
             )
             SELECT meta.drawer_id
                  , meta.root
                  , meta.name
                  , meta.doc_id
               FROM containing
               JOIN big_cabinet_drawer_meta_docs AS meta USING (drawer_id)
              WHERE meta.kind = ?2
             UNION
             SELECT drawer_id
                  , root
                  , name
                  , inventory_id
               FROM big_blobs_writer_targets
              WHERE doc_id = ?1",
            (doc.to_string(), crate::workers::INVENTORY_KIND),
        ).await?.all::<(String, String, String, String)>()?;
        rows.into_iter()
            .map(|(drawer, root, name, inventory)| {
                Ok(WriterTarget {
                    drawer: drawer.parse()?,
                    root: root.parse()?,
                    name,
                    inventory: inventory.parse()?,
                })
            })
            .collect()
    }

    async fn refresh_targets(&mut self, doc: &DocId) -> Res<()> {
        let mut snapshots = BTreeMap::new();
        let targets = self.candidates(doc).await?;
        let member_keys = [DrawerDocEntry::ID, DrawerChildEntry::ID].map(|tag| {
            DatumKey::from_text(tag, doc.to_string()).expect("a doc id is a datum slot")
        });
        let descriptor_key = DatumKey::from_text(Drawer::ID, "main")?;
        for target in &targets {
            if !snapshots.contains_key(&target.drawer) {
                let mut keys = Self::reference_keys(&targets, &target.drawer, MetaRoot::Public)?;
                keys.extend(member_keys.clone());
                keys.insert(descriptor_key.clone());
                snapshots.insert(
                    target.drawer.clone(),
                    self.read(&target.drawer, keys).await?,
                );
            }
            let Some(public) = &snapshots[&target.drawer] else {
                continue;
            };
            let Some(DatumRead::Accepted { value, .. }) = public.datums.get(&descriptor_key) else {
                continue;
            };
            let drawer: Drawer = serde_json::from_value(value.clone())?;
            if drawer.groups().is_none() {
                continue;
            }
            if member_keys.iter().any(|key| {
                matches!(
                    public.datums.get(key),
                    Some(
                        DatumRead::Pending { .. }
                            | DatumRead::Undecodable { .. }
                            | DatumRead::Rejected { .. }
                    )
                )
            }) {
                continue;
            }
            if !member_keys
                .iter()
                .any(|key| matches!(public.datums.get(key), Some(DatumRead::Accepted { .. })))
            {
                if member_keys.iter().all(|key| matches!(public.datums.get(key), Some(DatumRead::Absent { .. })))
                    && member_keys.iter().any(|key| matches!(public.datums.get(key), Some(DatumRead::Absent { version }) if !version.heads.is_empty())) {
                    self.update_target(doc, target, None).await?;
                }
                continue;
            }
            let root_doc = match target.root {
                MetaRoot::Public => target.drawer.clone(),
                MetaRoot::Sensitive => drawer
                    .sensitive_root()
                    .expect("a grouped drawer has a sensitive root"),
            };
            if !snapshots.contains_key(&root_doc) {
                let keys = Self::reference_keys(&targets, &target.drawer, target.root)?;
                snapshots.insert(root_doc.clone(), self.read(&root_doc, keys).await?);
            }
            let Some(root) = &snapshots[&root_doc] else {
                continue;
            };
            let key = DatumKey::from_text(
                DrawerMetaDoc::ID,
                format!("{}/{}", crate::workers::INVENTORY_KIND, target.name),
            )?;
            match root.datums.get(&key) {
                Some(DatumRead::Accepted { value, .. }) => {
                    let reference: DrawerMetaDoc = serde_json::from_value(value.clone())?;
                    let admitted = self.sql.query(
                        "SELECT 1 FROM big_cabinet_drawer_meta_docs
                          WHERE drawer_id = ? AND root = ? AND kind = ? AND name = ? AND doc_id = ?",
                        (target.drawer.to_string(), target.root.as_str(), crate::workers::INVENTORY_KIND, target.name.as_str(), reference.doc_id.to_string()),
                    ).await?.all::<i64>()?;
                    if !admitted.is_empty() && reference.doc_id == target.inventory {
                        self.update_target(doc, target, Some(&target.inventory))
                            .await?;
                    }
                }
                Some(DatumRead::Absent { version }) if !version.heads.is_empty() => {
                    self.update_target(doc, target, None).await?
                }
                None | Some(_) => (),
            }
        }
        Ok(())
    }

    fn reference_keys(
        targets: &[WriterTarget],
        drawer: &DocId,
        root: MetaRoot,
    ) -> Res<BTreeSet<DatumKey>> {
        targets
            .iter()
            .filter(|target| target.drawer == *drawer && target.root == root)
            .map(|target| {
                Ok(DatumKey::from_text(
                    DrawerMetaDoc::ID,
                    format!("{}/{}", crate::workers::INVENTORY_KIND, target.name),
                )?)
            })
            .collect()
    }

    async fn update_target(
        &self,
        doc: &DocId,
        target: &WriterTarget,
        inventory: Option<&DocId>,
    ) -> Res<()> {
        let doc = doc.clone();
        let target = target.clone();
        let inventory = inventory.cloned();
        let service = Arc::clone(&self.service);
        self.service.ownership.transaction(move |ownership| Box::pin(async move {
        let inventory = inventory.as_ref();
        let tx = ownership.sql();
        let params = (
            doc.to_string(),
            target.drawer.to_string(),
            target.root.as_str(),
            target.name.as_str(),
        );
        let previous = tx
            .query(
                "SELECT inventory_id FROM big_blobs_writer_targets
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4",
                params.clone(),
            )
            .await?
            .all::<String>()?;
        let claims = tx
            .query(
                "SELECT digest
                  , length_octets
               FROM big_blobs_writer_claims
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4
              ORDER BY digest",
                params.clone(),
            )
            .await?
            .all::<(String, u64)>()?;
        let desired = if inventory.is_some() {
            tx.query(
                "SELECT digest
                           , length_octets
                        FROM big_blobs_retention
                       WHERE doc_id = ?1 AND root = ?2
                       ORDER BY digest",
                (doc.to_string(), target.root.as_str()),
            )
            .await?
            .all::<(String, u64)>()?
        } else {
            Vec::new()
        };
        if previous.first() == inventory.map(ToString::to_string).as_ref() && claims == desired {
            return Ok(());
        }
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_retention_effects (digest)
             SELECT digest FROM big_blobs_writer_claims
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4",
            params.clone(),
        )
        .await?;
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_writer_effects (inventory_id, digest)
             SELECT inventory_id
                  , digest
               FROM big_blobs_writer_claims
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4",
            params.clone(),
        )
        .await?;
        tx.execute("DELETE FROM big_blobs_writer_claims WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4", params.clone()).await?;
        tx.execute("DELETE FROM big_blobs_writer_targets WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4", params.clone()).await?;
        if let Some(inventory) = inventory {
            tx.execute("INSERT INTO big_blobs_writer_targets (doc_id, drawer_id, root, name, inventory_id) VALUES (?, ?, ?, ?, ?)",
                (params.0.as_str(), params.1.as_str(), params.2, params.3, inventory.to_string())).await?;
            tx.execute(
                "INSERT INTO big_blobs_writer_claims (doc_id, drawer_id, root, name, inventory_id, digest, length_octets)
                 SELECT ?1
                      , ?2
                      , ?3
                      , ?4
                      , ?5
                      , digest
                      , length_octets
                   FROM big_blobs_retention
                  WHERE doc_id = ?1 AND root = ?3",
                (params.0.as_str(), params.1.as_str(), params.2, params.3, inventory.to_string()),
            ).await?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_retention_effects (digest)
             SELECT digest FROM big_blobs_writer_claims
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4",
            params.clone(),
        )
        .await?;
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_writer_effects (inventory_id, digest)
             SELECT inventory_id
                  , digest
               FROM big_blobs_writer_claims
              WHERE doc_id = ?1 AND drawer_id = ?2 AND root = ?3 AND name = ?4",
            params,
        )
        .await?;
        let digests = claims
            .into_iter()
            .chain(desired)
            .map(|(digest, _)| digest)
            .collect();
        Self::reconcile(&service, ownership, digests).await
        })).await
    }

    pub(crate) async fn is_inventory(&self, doc: &DocId) -> Res<bool> {
        Ok(!self
            .sql
            .query(
                "SELECT 1 FROM big_cabinet_drawer_meta_docs WHERE doc_id = ?1 AND kind = ?2
             UNION ALL
             SELECT 1 FROM big_blobs_writer_targets WHERE inventory_id = ?1
             LIMIT 1",
                (doc.to_string(), crate::workers::INVENTORY_KIND),
            )
            .await?
            .all::<i64>()?
            .is_empty())
    }

    pub async fn replace(
        &mut self,
        doc: &DocId,
        pairs: &[RetainedRepresentation],
        pins: BTreeMap<BlobId, u64>,
        preserve: bool,
    ) -> Res<()> {
        let mut representations = Vec::new();
        for pair in pairs {
            let Some(plain) = self.service.stat(&pair.plaintext).await? else {
                return Err(eyre::eyre!(
                    "a retained representation has no plaintext bytes"
                ));
            };
            representations.push((
                pair.ciphertext,
                Some(pair.plaintext.to_string()),
                pair.length_octets,
                MetaRoot::Public,
            ));
            representations.push((
                pair.plaintext,
                None,
                plain.length_octets,
                MetaRoot::Sensitive,
            ));
        }
        let doc_id = doc.to_string();
        let service = Arc::clone(&self.service);
        self.service.ownership.transaction(move |ownership| Box::pin(async move {
        let tx = ownership.sql();
        let mut digests: BTreeSet<String> = tx
            .query(
                "SELECT digest FROM big_blobs_retention WHERE doc_id = ?",
                (doc_id.as_str(),),
            )
            .await?
            .all::<String>()?
            .into_iter()
            .collect();
        tx.execute("INSERT OR IGNORE INTO big_blobs_retention_effects (digest) SELECT digest FROM big_blobs_retention WHERE doc_id = ?", (doc_id.as_str(),)).await?;
        if !preserve {
            tx.execute(
                "DELETE FROM big_blobs_retention WHERE doc_id = ?",
                (doc_id.as_str(),),
            )
            .await?;
        }
        for (digest, plaintext, length, root) in representations {
            tx.execute(
                "INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets, root) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (doc_id, digest) DO UPDATE SET plaintext = excluded.plaintext, length_octets = excluded.length_octets, root = excluded.root",
                (doc_id.as_str(), digest.to_string(), plaintext, i64::try_from(length)?, root.as_str()),
            ).await?;
        }
        for (digest, length) in pins {
            tx.execute("INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets) VALUES (?, ?, NULL, ?)
                        ON CONFLICT (doc_id, digest) DO NOTHING", (doc_id.as_str(), digest.to_string(), i64::try_from(length)?)).await?;
        }
        tx.execute("INSERT OR IGNORE INTO big_blobs_retention_effects (digest) SELECT digest FROM big_blobs_retention WHERE doc_id = ?", (doc_id.as_str(),)).await?;
        digests.extend(
            tx.query(
                "SELECT digest FROM big_blobs_retention WHERE doc_id = ?",
                (doc_id,),
            )
            .await?
            .all::<String>()?,
        );
        Self::reconcile(&service, ownership, digests).await
        })).await?;
        self.refresh_targets(doc).await?;
        Ok(())
    }

    async fn write_pin(
        &mut self,
        inventory: &DocId,
        id: &BlobId,
        length: Option<u64>,
    ) -> Res<bool> {
        let key = DatumKey::from_text(BlobPin::ID, id.digest_str())?;
        let Some(read) = self.read(inventory, BTreeSet::from([key.clone()])).await? else {
            return Ok(false);
        };
        let current = match read.datums.get(&key) {
            Some(DatumRead::Accepted { value, .. }) => Some(value),
            Some(
                DatumRead::Pending { .. }
                | DatumRead::Undecodable { .. }
                | DatumRead::Rejected { .. },
            ) => return Ok(false),
            _ => None,
        };
        let value = length
            .map(|length| {
                let mut pin = match current {
                    Some(value) => serde_json::from_value::<BlobPin>(value.clone())?,
                    None => BlobPin {
                        length_octets: length,
                        extra: Default::default(),
                    },
                };
                pin.length_octets = length;
                serde_json::to_value(pin)
            })
            .transpose()?;
        let (set, remove) = if current == value.as_ref() {
            (BTreeMap::new(), BTreeSet::new())
        } else {
            match value {
                Some(value) => (BTreeMap::from([(key, value)]), BTreeSet::new()),
                None => (BTreeMap::new(), BTreeSet::from([key])),
            }
        };
        match self
            .metadata
            .call(inventory, true, |capability| {
                self.docs.call(
                    RequestContext::root(),
                    WriteDatums {
                        capability,
                        command: None,
                        doc_id: inventory.clone(),
                        mode: WriteMode::IfDocUnchanged { heads: read.heads },
                        set,
                        remove,
                    },
                )
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(ClientError::Operation(
                DocsError::Conflict { .. }
                | DocsError::Denied
                | DocsError::DocNotFound
                | DocsError::Pending { .. }
                | DocsError::UnknownHeads { .. }
                | DocsError::AuthorityUnavailable(_)
                | DocsError::Access(_),
            )) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn flush_writers(&mut self) -> Res<BTreeSet<DocId>> {
        let effects = self.sql.query("SELECT inventory_id, digest FROM big_blobs_writer_effects ORDER BY inventory_id, digest", ()).await?.all::<(String, String)>()?;
        let mut changed = BTreeSet::new();
        for (inventory, digest) in effects {
            let inventory_id: DocId = inventory.parse()?;
            let id: BlobId = digest.parse()?;
            let lengths = self.sql.query(
                "SELECT DISTINCT length_octets FROM big_blobs_writer_claims WHERE inventory_id = ? AND digest = ?",
                (inventory.as_str(), digest.as_str()),
            ).await?.all::<u64>()?;
            eyre::ensure!(
                lengths.len() <= 1,
                "writer claims disagree on object length"
            );
            let length = lengths.first().copied();
            if !self.write_pin(&inventory_id, &id, length).await? {
                continue;
            }
            let part = crate::workers::inventory_part(&inventory_id);
            self.parts.ensure_part(part.clone()).await?;
            match length {
                Some(length) => {
                    self.parts
                        .set_obj_payload(id.into(), serde_json::json!({"lengthOctets": length}))
                        .await?;
                    self.parts.add_obj_to_parts(id.into(), vec![part]).await?;
                }
                None => self.parts.remove_obj_from_part(id.into(), part).await?,
            }
            let service = Arc::clone(&self.service);
            self.service
                .ownership
                .transaction(move |ownership| {
                    Box::pin(async move {
                        let tx = ownership.sql();
                        tx.execute(
                            "INSERT OR IGNORE INTO big_blobs_retention_effects (digest) VALUES (?)",
                            (digest.as_str(),),
                        )
                        .await?;
                        tx.execute(
                "DELETE FROM big_blobs_writer_effects WHERE inventory_id = ? AND digest = ?",
                (inventory, digest.clone()),
            )
            .await?;
                        Self::reconcile(&service, ownership, BTreeSet::from([digest])).await
                    })
                })
                .await?;
            changed.insert(inventory_id);
        }
        Ok(changed)
    }

    pub async fn flush(&mut self) -> Res<BTreeSet<DocId>> {
        let changed = self.flush_writers().await?;
        let effects = self
            .sql
            .query(
                "SELECT digest FROM big_blobs_retention_effects ORDER BY digest",
                (),
            )
            .await?
            .all::<String>()?;
        for digest in effects {
            let id: BlobId = digest.parse()?;
            let service = Arc::clone(&self.service);
            let pending = digest.clone();
            self.service
                .ownership
                .transaction(move |ownership| {
                    Box::pin(async move {
                        Self::reconcile(&service, ownership, BTreeSet::from([pending])).await
                    })
                })
                .await?;
            if self.service.ownership.contains(&id).await?
                && self.service.stat(&id).await?.is_some()
            {
                self.service.announce(id).await?;
            }
            let service = Arc::clone(&self.service);
            self.service
                .ownership
                .transaction(move |ownership| {
                    Box::pin(async move {
                        ownership
                            .sql()
                            .execute(
                                "DELETE FROM big_blobs_retention_effects WHERE digest = ?",
                                (digest.clone(),),
                            )
                            .await?;
                        Self::reconcile(&service, ownership, BTreeSet::from([digest])).await
                    })
                })
                .await?;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests;
