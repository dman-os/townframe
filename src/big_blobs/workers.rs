use crate::documents::DeferralReason;
use crate::documents::DocumentBlobs;
use crate::projection::BlobProjection;
use crate::retention::Retention;
use crate::service::BlobService;
use crate::work_queue::WorkQueue;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::BlobPin;
use big_blobs_core::CipherBlob;
use big_blobs_core::Jwk;
use big_cabinet::index::DatumSetStore;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_repo::SharedPartStore;
use big_sync_core::ObjKey;
use big_sync_core::PartKey;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;
use utils_rs::prelude::Res;

pub struct WorkerConfig {
    pub sql: Database,
    pub service: Arc<BlobService>,
    pub docs: Client<DocsProtocol>,
    pub capability: CapId,
    pub inventory: DocId,
    pub datum_set: Arc<DatumSetStore>,
    pub parts: SharedPartStore,
    pub repo: big_repo::SharedBigRepo,
    pub policy: watch::Receiver<u64>,
}

pub struct BlobWorkers {
    pub projection: Arc<BlobProjection>,
    pub parts: SharedPartStore,
    pub inventory: DocId,
}

pub fn inventory_part(document: &DocId) -> PartKey {
    let digest = blake3::derive_key(
        "townframe.big_blobs.inventory.v1",
        document.to_string().as_bytes(),
    );
    PartKey::new(digest)
}

pub async fn boot(config: WorkerConfig) -> Res<(BlobWorkers, StopToken)> {
    let projection = Arc::new(BlobProjection::new(config.sql.clone()).await?);
    let queue = WorkQueue::new(config.sql.clone()).await?;
    queue.rearm().await?;
    for unresolved in config.service.roots.unresolved().await? {
        if let Some((document, _)) = unresolved.claimed_by {
            queue.enqueue(&document.parse()?).await?;
        }
    }
    apply_pins_migration(&config.sql).await?;
    let retention = Retention::new(
        config.sql.clone(),
        Arc::clone(&config.service),
        config.docs.clone(),
        config.capability,
        config.inventory.clone(),
    )
    .await?;
    let adapter = DocumentBlobs::new(
        config.docs.clone(),
        config.capability,
        config.service.store().clone(),
        Arc::clone(&config.service.provider),
        config.service.roots.clone(),
    );
    let workers = BlobWorkers {
        projection: Arc::clone(&projection),
        parts: Arc::clone(&config.parts),
        inventory: config.inventory.clone(),
    };
    let (stop, tasks) = StopToken::new();
    let cancel = tasks.clone();
    let permissions_repo = Arc::clone(&config.repo);
    let permissions_queue = queue.clone();
    tasks
        .spawn(async move {
            crate::permissions::run(permissions_repo, permissions_queue, cancel.cancelled())
                .await
                .unwrap();
        })
        .expect("permission ingress starts before shutdown");
    let cancel = tasks.clone();
    let source = Arc::clone(&config.datum_set);
    let docs = config.docs.clone();
    let capability = config.capability;
    let projected = Arc::clone(&projection);
    tasks
        .spawn(async move {
            projected
                .run(source, docs, capability, cancel.cancelled())
                .await
                .unwrap();
        })
        .expect("projection starts before shutdown");
    let cancel = tasks.clone();
    tasks.spawn(async move {
        let run = async {
            let mut datums = config.datum_set.open_all(queue.progress("datums").await?).await?;
            let mut availability = config.service.open_availability(queue.progress("availability").await?).await?;
            let mut policy = config.policy;
            loop {
                if cancel.is_cancelled() { return Ok::<_, utils_rs::prelude::eyre::Report>(()); }
                retention.flush().await?;
                if let Some((document, generation)) = queue.next().await? {
                    let context = RequestContext::root();
                    let read = config.docs.call(context.child(), ReadDatums {
                        capability: config.capability, doc_id: document.clone(), at: At::Current, datums: None, optimistic: false,
                    }).await;
                    let (pairs, pins, waiting) = match read {
                        Ok(read) => {
                            let mut pins = BTreeMap::new();
                            let mut pending = false;
                            for (key, datum) in read.datums {
                                if key.tag().as_str() != BlobPin::ID { continue; }
                                match datum {
                                    DatumRead::Accepted { value, .. } => {
                                        let pin: BlobPin = serde_json::from_value(value)?;
                                        if let Ok(id) = pin_id(&key) { pins.insert(id, pin.length_octets); }
                                    }
                                    DatumRead::Pending { .. } | DatumRead::Undecodable { .. } => pending = true,
                                    _ => (),
                                }
                            }
                            let report = adapter.reconcile(&context, &document).await?;
                            let denied = report.deferred.iter().any(|deferred| matches!(deferred.reason, DeferralReason::Document { error: DocsError::Denied, .. }));
                            if denied { (Vec::new(), BTreeMap::new(), false) }
                            else { (report.retained, pins, pending || !report.deferred.is_empty()) }
                        }
                        Err(ClientError::Operation(DocsError::Denied)) => (Vec::new(), BTreeMap::new(), false),
                        Err(ClientError::Operation(DocsError::DocNotFound | DocsError::Pending { .. } | DocsError::UnknownHeads { .. } | DocsError::AuthorityUnavailable(_))) => {
                            queue.finish(&document, generation, true).await?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    retention.replace(&document, &pairs, pins.clone(), waiting).await?;
                    retention.flush().await?;
                    reconcile_parts(&config.sql, &config.parts, &document, &pins, waiting).await?;
                    crate::permissions::seed(&config.repo, &config.parts, &document).await?;
                    if !waiting {
                        for unresolved in config.service.roots.unresolved().await? {
                            if unresolved.claimed_by.as_ref().is_some_and(|(doc, _)| doc == &document.to_string()) {
                                let id = BlobId::new(*unresolved.cipher_hash.as_bytes());
                                let claimed = config.sql.query("SELECT 1 FROM big_blobs_retention WHERE digest = ? LIMIT 1", (id.to_string(),)).await?.all::<i64>()?;
                                if claimed.is_empty() {
                                    crate::encrypt::drop_pair_tags(config.service.store(), unresolved.cipher_hash).await?;
                                    config.service.provider.remove(&unresolved.cipher_hash);
                                }
                                config.service.roots.clear(unresolved.cipher_hash).await?;
                            }
                        }
                    }
                    queue.finish(&document, generation, waiting).await?;
                    continue;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = queue.changed.notified() => (),
                    read = datums.next(RevisionReadLimits::default()) => {
                        if let RevisionRead::Entries { revision, entries } = read? {
                            let mut documents = BTreeSet::new();
                            let mut rearm = false;
                            for entry in entries {
                                let tag = entry.key.datum_key.tag().as_str();
                                if [Blob::ID, CipherBlob::ID, BlobPin::ID, Jwk::ID].contains(&tag) {
                                    rearm |= tag == Jwk::ID;
                                    documents.insert(entry.key.doc_id);
                                }
                            }
                            queue.ingest("datums", revision, documents, rearm).await?;
                        }
                    }
                    read = availability.next(RevisionReadLimits::default()) => {
                        if let RevisionRead::Entries { revision, .. } = read? {
                            queue.ingest("availability", revision, BTreeSet::new(), true).await?;
                            projection.rearm();
                        }
                    }
                    changed = policy.changed() => {
                        changed.expect("drawer policy outlives blob workers");
                        queue.rearm().await?;
                        projection.rearm();
                    }
                }
            }
        };
        run.await.unwrap();
    }).expect("blob worker starts before shutdown");
    Ok((workers, stop))
}

/// The per-document pin ledger the worker reconciles against lives beside the blob store's own
/// tables; it is applied here because only the workers read and write it.
async fn apply_pins_migration(sql: &Database) -> Res<()> {
    sql.apply_migrations(
        "big_blobs_pins_migrations",
        &[sqlite_rs::Migration {
            version: 1,
            name: "document_pins",
            sql: "CREATE TABLE big_blobs_document_pins (
             doc_id TEXT NOT NULL
           , digest TEXT NOT NULL
           , PRIMARY KEY (doc_id, digest)
        ) STRICT;",
        }],
    )
    .await?;
    Ok(())
}

fn pin_id(key: &DatumKey) -> Res<BlobId> {
    Ok(key
        .to_string()
        .split_once('/')
        .expect("datum key has a tag")
        .1
        .parse()?)
}

async fn reconcile_parts(
    sql: &Database,
    parts: &SharedPartStore,
    document: &DocId,
    pins: &BTreeMap<BlobId, u64>,
    preserve: bool,
) -> Res<()> {
    let previous = sql
        .query(
            "SELECT digest FROM big_blobs_document_pins WHERE doc_id = ?",
            (document.to_string(),),
        )
        .await?
        .all::<String>()?;
    let part = inventory_part(document);
    parts.ensure_part(part.clone()).await?;
    for (id, length) in pins {
        let obj: ObjKey = (*id).into();
        parts
            .set_obj_payload(obj.clone(), serde_json::json!({"lengthOctets": length}))
            .await?;
        parts.add_obj_to_parts(obj, vec![part.clone()]).await?;
    }
    if !preserve {
        for digest in previous {
            let id: BlobId = digest.parse()?;
            if !pins.contains_key(&id) {
                parts.remove_obj_from_part(id.into(), part.clone()).await?;
            }
        }
    }
    let tx = sql.begin_write().await?;
    if !preserve {
        tx.execute(
            "DELETE FROM big_blobs_document_pins WHERE doc_id = ?",
            (document.to_string(),),
        )
        .await?;
    }
    for id in pins.keys() {
        tx.execute(
            "INSERT OR IGNORE INTO big_blobs_document_pins (doc_id, digest) VALUES (?, ?)",
            (document.to_string(), id.to_string()),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}
