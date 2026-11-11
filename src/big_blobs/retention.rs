use crate::documents::RetainedRepresentation;
mod sqlite;
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
use futures::future::BoxFuture;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use utils_rs::prelude::Res;
use utils_rs::prelude::eyre;

/// Whether this node can commit to an inventory doc: its own keyhive standing on it, the
/// predicate big_repo's commit gate enforces. The metadata capability cannot answer this: it is
/// minted by the operator, a cabinet superuser on every drawer the node knows, including a
/// drawer synced in from a peer where the node holds only keyhive Read.
pub(crate) type InventoryWriteAccess =
    Arc<dyn Fn(DocId) -> BoxFuture<'static, Res<bool>> + Send + Sync>;

pub(crate) struct Retention {
    sql: Database,
    service: Arc<BlobService>,
    docs: Client<DocsProtocol>,
    metadata: MetadataCapabilities,
    writable: InventoryWriteAccess,
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
        sql.apply_migrations(
            "big_blobs_retention_migrations",
            sqlite::queries::MIGRATIONS,
        )
        .await?;
        Ok(())
    }

    pub async fn new(
        sql: Database,
        service: Arc<BlobService>,
        docs: Client<DocsProtocol>,
        metadata: MetadataCapabilities,
        writable: InventoryWriteAccess,
        parts: SharedPartStore,
    ) -> Res<Self> {
        Self::migrate(&sql).await?;
        let retention = Self {
            sql,
            service,
            docs,
            metadata,
            writable,
            parts,
        };
        let service = Arc::clone(&retention.service);
        retention
            .service
            .ownership
            .transaction(move |ownership| {
                Box::pin(async move {
                    sqlite::queries::refresh_effects(ownership.sql()).await?;
                    let digests = sqlite::queries::effect_digests(ownership.sql())
                        .await?
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

            let documents =
                sqlite::queries::retention_docs_by_digest(ownership.sql(), digest.clone()).await?;
            let claims = sqlite::queries::writer_claims_by_digest(ownership.sql(), digest.clone())
                .await?
                .into_iter()
                .map(|row| {
                    (
                        row.doc_id,
                        row.drawer_id,
                        row.root,
                        row.name,
                        row.inventory_id,
                    )
                })
                .collect::<Vec<_>>();
            let effects =
                sqlite::queries::writer_effects_by_digest(ownership.sql(), digest).await?;
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
            Err(ClientError::Method(
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
        let rows = sqlite::queries::candidate_targets(
            &self.sql,
            doc.to_string(),
            crate::workers::INVENTORY_KIND.to_owned(),
        )
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(WriterTarget {
                    drawer: row.drawer_id.parse()?,
                    root: row.root.parse()?,
                    name: row.name,
                    inventory: row.doc_id.parse()?,
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
            // A doc is a candidate for the inventory of every drawer that contains it, and a
            // drawer synced in from a peer may grant this node only Read: its inventory is that
            // peer's to pin into, not ours. Such a target is left exactly as it stands, like a
            // denied read below: never newly claimed, and claims made while the node could
            // write are kept until it can write again (ADR 026 §5: an unreadable or unwritable
            // inventory must not shrink retention). Asking first keeps retention from staging
            // pins that keyhive refuses at commit; a grant arriving later rearms the work queue
            // through the permissions ingress, which reruns this.
            if !(self.writable)(target.inventory.clone()).await? {
                continue;
            }
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
                    let admitted = sqlite::queries::admitted_meta_doc(
                        &self.sql,
                        target.drawer.to_string(),
                        target.root.as_str().to_owned(),
                        crate::workers::INVENTORY_KIND.to_owned(),
                        target.name.clone(),
                        reference.doc_id.to_string(),
                    )
                    .await?;
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
        self.service
            .ownership
            .transaction(move |ownership| {
                Box::pin(async move {
                    let inventory = inventory.as_ref();
                    let tx = ownership.sql();
                    let params = (
                        doc.to_string(),
                        target.drawer.to_string(),
                        target.root.as_str(),
                        target.name.as_str(),
                    );
                    let previous = sqlite::queries::writer_target_inventory(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;
                    let claims = sqlite::queries::target_claims(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?
                    .into_iter()
                    .map(|row| (row.digest, row.length_octets as u64))
                    .collect::<Vec<_>>();
                    let desired = if inventory.is_some() {
                        sqlite::queries::target_retention(
                            tx,
                            doc.to_string(),
                            target.root.as_str().to_owned(),
                        )
                        .await?
                        .into_iter()
                        .map(|row| (row.digest, row.length_octets as u64))
                        .collect()
                    } else {
                        Vec::new()
                    };
                    if previous.as_ref() == inventory.map(ToString::to_string).as_ref()
                        && claims == desired
                    {
                        return Ok(());
                    }
                    sqlite::queries::retention_effects_from_claims(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;
                    sqlite::queries::writer_effects_from_claims(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;
                    sqlite::queries::delete_claims(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;
                    sqlite::queries::delete_targets(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;

                    if let Some(inventory) = inventory {
                        sqlite::queries::insert_target(
                            tx,
                            params.0.clone(),
                            params.1.clone(),
                            params.2.to_owned(),
                            params.3.to_owned(),
                            inventory.to_string(),
                        )
                        .await?;
                        sqlite::queries::insert_claims_from_retention(
                            tx,
                            params.0.clone(),
                            params.1.clone(),
                            params.2.to_owned(),
                            params.3.to_owned(),
                            inventory.to_string(),
                        )
                        .await?;
                    }
                    sqlite::queries::retention_effects_from_claims(
                        tx,
                        params.0.clone(),
                        params.1.clone(),
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;
                    sqlite::queries::writer_effects_from_claims(
                        tx,
                        params.0,
                        params.1,
                        params.2.to_owned(),
                        params.3.to_owned(),
                    )
                    .await?;

                    let digests = claims
                        .into_iter()
                        .chain(desired)
                        .map(|(digest, _)| digest)
                        .collect();
                    Self::reconcile(&service, ownership, digests).await
                })
            })
            .await
    }

    pub(crate) async fn is_inventory(&self, doc: &DocId) -> Res<bool> {
        Ok(!sqlite::queries::is_inventory(
            &self.sql,
            doc.to_string(),
            crate::workers::INVENTORY_KIND.to_owned(),
        )
        .await?
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
        self.service
            .ownership
            .transaction(move |ownership| {
                Box::pin(async move {
                    let tx = ownership.sql();
                    let mut digests: BTreeSet<String> =
                        sqlite::queries::retention_digests(tx, doc_id.clone())
                            .await?
                            .into_iter()
                            .collect();
                    sqlite::queries::retention_effects_for_doc(tx, doc_id.clone()).await?;
                    if !preserve {
                        sqlite::queries::delete_retention_doc(tx, doc_id.clone()).await?;
                    }
                    for (digest, plaintext, length, root) in representations {
                        match root {
                            MetaRoot::Public => {
                                sqlite::queries::insert_retention_plain(
                                    tx,
                                    doc_id.clone(),
                                    digest.to_string(),
                                    plaintext.expect("public retention has plaintext"),
                                    i64::try_from(length)?,
                                    root.as_str().to_owned(),
                                )
                                .await?;
                            }
                            MetaRoot::Sensitive => {
                                sqlite::queries::insert_retention_cipher(
                                    tx,
                                    doc_id.clone(),
                                    digest.to_string(),
                                    i64::try_from(length)?,
                                    root.as_str().to_owned(),
                                )
                                .await?;
                            }
                        }
                    }
                    for (digest, length) in pins {
                        sqlite::queries::insert_retention_pin(
                            tx,
                            doc_id.clone(),
                            digest.to_string(),
                            i64::try_from(length)?,
                        )
                        .await?;
                    }
                    sqlite::queries::retention_effects_for_doc(tx, doc_id.clone()).await?;
                    digests.extend(sqlite::queries::retention_digests(tx, doc_id).await?);
                    Self::reconcile(&service, ownership, digests).await
                })
            })
            .await?;
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
            Err(ClientError::Method(
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
        let effects = sqlite::queries::writer_effects(&self.sql).await?;
        let mut changed = BTreeSet::new();
        for effect in effects {
            let inventory = effect.inventory_id;
            let digest = effect.digest;
            let inventory_id: DocId = inventory.parse()?;
            let id: BlobId = digest.parse()?;
            let lengths =
                sqlite::queries::claim_lengths(&self.sql, inventory.clone(), digest.clone())
                    .await?;
            eyre::ensure!(
                lengths.len() <= 1,
                "writer claims disagree on object length"
            );
            let length = lengths.first().copied().map(u64::try_from).transpose()?;
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
                        sqlite::queries::mark_retention_effect(tx, digest.clone()).await?;
                        sqlite::queries::delete_writer_effect(tx, inventory, digest.clone())
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
        let effects = sqlite::queries::retention_effect_digests(&self.sql).await?;
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
                        sqlite::queries::delete_retention_effect(ownership.sql(), digest.clone())
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
