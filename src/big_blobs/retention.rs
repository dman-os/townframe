use crate::documents::RetainedRepresentation;
use crate::service::BlobService;
use big_blobs_core::BlobId;
use big_blobs_core::BlobPin;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use service_utils_rs::Client;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use utils_rs::prelude::Res;

pub(crate) struct Retention {
    sql: Database,
    service: Arc<BlobService>,
    docs: Client<DocsProtocol>,
    capability: CapId,
    /// The node-local inventory, used for docs no drawer claims yet. Every other pin goes
    /// to the inventory of the drawer that contains the claiming doc (ADR 026 §5).
    fallback_inventory: DocId,
}

impl Retention {
    pub async fn new(
        sql: Database,
        service: Arc<BlobService>,
        docs: Client<DocsProtocol>,
        capability: CapId,
        inventory: DocId,
    ) -> Res<Self> {
        sql.apply_migrations("big_blobs_retention_migrations", &[sqlite_rs::Migration {
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
        }]).await?;
        Ok(Self {
            sql,
            service,
            docs,
            capability,
            fallback_inventory: inventory,
        })
    }

    /// The `blobInventory` meta docs of every drawer that directly contains `doc`
    /// (ADR 026 §5: an inventory pins its own drawer's docs only). Empty when the doc is
    /// in no drawer with an inventory; callers fall back to the node-local inventory.
    pub(crate) async fn inventories_for(&self, doc: &DocId) -> Res<Vec<DocId>> {
        let rows = self
            .sql
            .query(
                "SELECT meta.doc_id
                   FROM big_cabinet_drawer_docs AS docs
                   JOIN big_cabinet_drawer_meta_docs AS meta
                     ON meta.drawer_id = docs.drawer_id
                  WHERE docs.doc_id = ? AND meta.kind = ?",
                (doc.to_string(), crate::workers::INVENTORY_KIND),
            )
            .await?
            .all::<String>()?;
        rows.into_iter()
            .map(|row| row.parse().map_err(Into::into))
            .collect()
    }

    pub(crate) async fn is_inventory(&self, doc: &DocId) -> Res<bool> {
        Ok(!self
            .sql
            .query(
                "SELECT 1 FROM big_cabinet_drawer_meta_docs WHERE doc_id = ? AND kind = ? LIMIT 1",
                (doc.to_string(), crate::workers::INVENTORY_KIND),
            )
            .await?
            .all::<i64>()?
            .is_empty())
    }

    async fn all_inventories(&self) -> Res<Vec<DocId>> {
        let mut inventories = self
            .sql
            .query(
                "SELECT doc_id FROM big_cabinet_drawer_meta_docs WHERE kind = ?",
                (crate::workers::INVENTORY_KIND,),
            )
            .await?
            .all::<String>()?;
        inventories.push(self.fallback_inventory.to_string());
        inventories.sort();
        inventories.dedup();
        inventories
            .into_iter()
            .map(|row| row.parse().map_err(Into::into))
            .collect()
    }

    pub async fn replace(
        &self,
        doc: &DocId,
        pairs: &[RetainedRepresentation],
        pins: BTreeMap<BlobId, u64>,
        preserve: bool,
    ) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let doc = doc.to_string();
        tx.execute("INSERT OR IGNORE INTO big_blobs_retention_effects (digest) SELECT digest FROM big_blobs_retention WHERE doc_id = ?", (doc.as_str(),)).await?;
        if !preserve {
            tx.execute(
                "DELETE FROM big_blobs_retention WHERE doc_id = ?",
                (doc.as_str(),),
            )
            .await?;
        }
        for pair in pairs {
            tx.execute("INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets) VALUES (?, ?, ?, ?)
                        ON CONFLICT (doc_id, digest) DO UPDATE SET plaintext = excluded.plaintext, length_octets = excluded.length_octets",
                (doc.as_str(), pair.ciphertext.to_string(), pair.plaintext.to_string(), i64::try_from(pair.length_octets)?)).await?;
        }
        // Callers pass pins only for inventory docs (ADR 026 §5: pins live in
        // inventories); a pin row retains its digest without authoring anything.
        for (digest, length) in pins {
            tx.execute("INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets) VALUES (?, ?, NULL, ?)
                        ON CONFLICT (doc_id, digest) DO NOTHING", (doc.as_str(), digest.to_string(), i64::try_from(length)?)).await?;
        }
        tx.execute("INSERT OR IGNORE INTO big_blobs_retention_effects (digest) SELECT digest FROM big_blobs_retention WHERE doc_id = ?", (doc,)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Set (or, with `None`, remove) the pin for `id` in one inventory doc. A conflicting
    /// or not-yet-decodable inventory leaves the effect queued: `flush` is called on every
    /// work cycle, so the write is retried without a dedicated rearm.
    async fn write_pin(&self, inventory: &DocId, id: &BlobId, length: Option<u64>) -> Res<()> {
        let context = RequestContext::root();
        let read = self
            .docs
            .call(
                context.child(),
                ReadDatums {
                    capability: self.capability,
                    doc_id: inventory.clone(),
                    at: At::Current,
                    datums: None,
                    optimistic: false,
                },
            )
            .await?;
        let key = DatumKey::from_text(BlobPin::ID, id.digest_str())?;
        let current = match read.datums.get(&key) {
            Some(DatumRead::Accepted { value, .. }) => Some(value),
            Some(DatumRead::Pending { .. } | DatumRead::Undecodable { .. }) => return Ok(()),
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
        if current == value.as_ref() {
            return Ok(());
        }
        let (set, remove) = match value {
            Some(value) => (BTreeMap::from([(key, value)]), BTreeSet::new()),
            None => (BTreeMap::new(), BTreeSet::from([key])),
        };
        let request = WriteDatums {
            capability: self.capability,
            command: None,
            doc_id: inventory.clone(),
            mode: WriteMode::IfDocUnchanged { heads: read.heads },
            set,
            remove,
        };
        match self.docs.call(context, request).await {
            Ok(_) => Ok(()),
            Err(service_utils_rs::ClientError::Operation(
                big_cabinet_core::doc::DocsError::Conflict { .. },
            )) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn flush(&self) -> Res<()> {
        loop {
            let Some(digest) = self
                .sql
                .query(
                    "SELECT digest FROM big_blobs_retention_effects ORDER BY digest LIMIT 1",
                    (),
                )
                .await?
                .all::<String>()?
                .into_iter()
                .next()
            else {
                return Ok(());
            };
            let id: BlobId = digest.parse()?;
            let rows = self
                .sql
                .query(
                    "SELECT doc_id, plaintext, length_octets FROM big_blobs_retention WHERE digest = ?",
                    (digest.as_str(),),
                )
                .await?
                .all::<(String, Option<String>, u64)>()?;
            // Each owned representation is pinned in the inventory of every drawer that
            // contains its claiming doc, or the node-local inventory when none does
            // (ADR 026 §5). Pin-only rows never author pins: pins live in inventories,
            // and the inventory's own worker maintains them there.
            let mut desired: BTreeMap<DocId, u64> = BTreeMap::new();
            for (doc, plaintext, length) in &rows {
                if plaintext.is_none() {
                    continue;
                }
                let doc: DocId = doc.parse()?;
                let mut targets = self.inventories_for(&doc).await?;
                if targets.is_empty() {
                    targets.push(self.fallback_inventory.clone());
                }
                for target in targets {
                    desired
                        .entry(target)
                        .and_modify(|current| *current = (*current).max(*length))
                        .or_insert(*length);
                }
            }
            if rows.is_empty() {
                for inventory in self.all_inventories().await? {
                    self.write_pin(&inventory, &id, None).await?;
                }
            } else {
                for (inventory, length) in &desired {
                    self.write_pin(inventory, &id, Some(*length)).await?;
                }
            }
            let hash = iroh_blobs::Hash::from(id.to_bytes32());
            if rows.is_empty() {
                crate::encrypt::drop_pair_tags(self.service.store(), hash).await?;
                self.service
                    .store()
                    .tags()
                    .delete(format!("pin:{hash}"))
                    .await?;
                self.service.roots.clear(hash).await?;
                self.service.provider.remove(&hash);
            } else {
                self.service
                    .store()
                    .tags()
                    .set(format!("pin:{hash}"), iroh_blobs::HashAndFormat::raw(hash))
                    .await?;
                if self.service.stat(&id).await?.is_some() {
                    self.service.announce(id).await?;
                }
                self.service.release_upload(&id).await?;
                for (_, plaintext, _) in &rows {
                    if let Some(plaintext) = plaintext {
                        self.service.release_upload(&plaintext.parse()?).await?;
                    }
                }
            }
            self.sql
                .execute(
                    "DELETE FROM big_blobs_retention_effects WHERE digest = ?",
                    (digest,),
                )
                .await?;
        }
    }
}
