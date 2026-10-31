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
    inventory: DocId,
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
            inventory,
        })
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
        if doc != self.inventory.to_string() {
            for (digest, length) in pins {
                tx.execute("INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets) VALUES (?, ?, NULL, ?)
                            ON CONFLICT (doc_id, digest) DO NOTHING", (doc.as_str(), digest.to_string(), i64::try_from(length)?)).await?;
            }
        }
        tx.execute("INSERT OR IGNORE INTO big_blobs_retention_effects (digest) SELECT digest FROM big_blobs_retention WHERE doc_id = ?", (doc,)).await?;
        tx.commit().await?;
        Ok(())
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
                    "SELECT plaintext, length_octets FROM big_blobs_retention WHERE digest = ?",
                    (digest.as_str(),),
                )
                .await?
                .all::<(Option<String>, u64)>()?;
            let owned = rows.iter().find(|(plaintext, _)| plaintext.is_some());
            let desired = owned.map(|(_, length)| *length);
            let context = RequestContext::root();
            let read = self
                .docs
                .call(
                    context.child(),
                    ReadDatums {
                        capability: self.capability,
                        doc_id: self.inventory.clone(),
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
            let value = desired
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
            if current != value.as_ref() {
                let (set, remove) = match value {
                    Some(value) => (BTreeMap::from([(key, value)]), BTreeSet::new()),
                    None => (BTreeMap::new(), BTreeSet::from([key])),
                };
                let request = WriteDatums {
                    capability: self.capability,
                    command: None,
                    doc_id: self.inventory.clone(),
                    mode: WriteMode::IfDocUnchanged { heads: read.heads },
                    set,
                    remove,
                };
                match self.docs.call(context, request).await {
                    Ok(_) => (),
                    Err(service_utils_rs::ClientError::Operation(
                        big_cabinet_core::doc::DocsError::Conflict { .. },
                    )) => return Ok(()),
                    Err(error) => return Err(error.into()),
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
                for (plaintext, _) in &rows {
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
