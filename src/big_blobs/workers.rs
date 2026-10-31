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
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
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

/// The `DrawerMetaDoc` kind whose doc carries a drawer's blob pins (ADR 026 §5).
/// `big_blobs_core` owns the vocabulary; Cabinet only stores the slot.
pub const INVENTORY_KIND: &str = "blobInventory";

pub struct WorkerConfig {
    pub sql: Database,
    pub service: Arc<BlobService>,
    pub docs: Client<DocsProtocol>,
    pub capability: CapId,
    pub inventory: DocId,
    /// The drawers whose docs this node maintains blobs for (ADR 026 §9). Each gets its
    /// own `PollDatums` cursor; inventories are discovered per drawer at flush time.
    pub drawers: Vec<DocId>,
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

/// The per-drawer plaintext part (ADR 026 §7): its members are the drawer's docs-group
/// readers, who are entitled to plaintext, unlike the ciphertext part above whose
/// members need only Read on the drawer's inventory doc.
pub fn plaintext_part(drawer: &DocId) -> PartKey {
    let digest = blake3::derive_key(
        "townframe.big_blobs.plaintext.v1",
        drawer.to_string().as_bytes(),
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
    // One poll cursor per configured drawer (ADR 026 §9): the worker sees exactly the
    // docs its drawer reaches at Read, and roster joins/leaves arrive as events on the
    // same cursor. Inventory docs are meta docs, not drawer entries, so each known
    // inventory is polled by name for its pins.
    for drawer in &config.drawers {
        let cancel = tasks.clone();
        let docs = config.docs.clone();
        let capability = config.capability;
        let queue = queue.clone();
        let drawer = drawer.clone();
        tasks
            .spawn(async move {
                poll_drawer(docs, capability, queue, drawer, cancel.cancelled())
                    .await
                    .unwrap();
            })
            .expect("drawer poll starts before shutdown");
    }
    {
        let cancel = tasks.clone();
        let docs = config.docs.clone();
        let capability = config.capability;
        let queue = queue.clone();
        let inventory = config.inventory.clone();
        tasks
            .spawn(async move {
                poll_inventory(docs, capability, queue, inventory, cancel.cancelled())
                    .await
                    .unwrap();
            })
            .expect("inventory poll starts before shutdown");
    }
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
    spawn_worker(&tasks, &config, queue, retention, adapter, projection);
    Ok((workers, stop))
}

fn spawn_worker(
    tasks: &service_utils_rs::Spawner,
    config: &WorkerConfig,
    queue: WorkQueue,
    retention: Retention,
    adapter: DocumentBlobs,
    projection: Arc<BlobProjection>,
) {
    let config = WorkerParts {
        sql: config.sql.clone(),
        service: Arc::clone(&config.service),
        docs: config.docs.clone(),
        capability: config.capability,
        inventory: config.inventory.clone(),
        parts: Arc::clone(&config.parts),
        repo: Arc::clone(&config.repo),
        policy: config.policy.clone(),
    };
    let cancel = tasks.clone();
    tasks
        .spawn(async move {
            let config = &config;
            let run = async {
            let mut availability = config.service.open_availability(queue.progress("availability").await?).await?;
            let mut policy = config.policy.clone();
            loop {
                if cancel.is_cancelled() { return Ok::<_, utils_rs::prelude::eyre::Report>(()); }
                retention.flush().await?;
                if let Some((document, generation)) = queue.next().await? {
                    let context = RequestContext::root();
                    // Pins live only in inventory docs (ADR 026 §5); a BlobPin datum
                    // anywhere else authors nothing. The node-local fallback inventory
                    // counts even before its meta-doc projection lands.
                    let inventory_doc = document == config.inventory
                        || retention.is_inventory(&document).await?;
                    let read = config.docs.call(context.child(), ReadDatums {
                        capability: config.capability, doc_id: document.clone(), at: At::Current, datums: None, optimistic: false,
                    }).await;
                    let (pairs, pins, waiting) = match read {
                        Ok(read) => {
                            let mut pins = BTreeMap::new();
                            let mut pending = false;
                            if inventory_doc {
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
                            }
                            if inventory_doc {
                                (Vec::new(), pins, pending)
                            } else {
                                let report = adapter.reconcile(&context, &document).await?;
                                let denied = report.deferred.iter().any(|deferred| matches!(deferred.reason, DeferralReason::Document { error: DocsError::Denied, .. }));
                                // A doc we cannot currently read must not shrink
                                // retention (ADR 026 §5): park it and keep every row
                                // until an observed removal says otherwise.
                                if denied { (Vec::new(), BTreeMap::new(), true) }
                                else { (report.retained, pins, pending || !report.deferred.is_empty()) }
                            }
                        }
                        Err(ClientError::Operation(DocsError::Denied)) => (Vec::new(), BTreeMap::new(), true),
                        Err(ClientError::Operation(DocsError::DocNotFound | DocsError::Pending { .. } | DocsError::UnknownHeads { .. } | DocsError::AuthorityUnavailable(_))) => {
                            queue.finish(&document, generation, true).await?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    retention.replace(&document, &pairs, pins.clone(), waiting).await?;
                    retention.flush().await?;
                    if inventory_doc {
                        reconcile_parts(&config.sql, &config.parts, &document, &pins, waiting).await?;
                        crate::permissions::seed(&config.repo, &config.parts, &document).await?;
                    } else if !waiting {
                        distribute_parts(config, &retention, &document, &pairs).await?;
                    }
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
        })
        .expect("blob worker starts before shutdown");
}

struct WorkerParts {
    sql: Database,
    service: Arc<BlobService>,
    docs: Client<DocsProtocol>,
    capability: CapId,
    inventory: DocId,
    parts: SharedPartStore,
    repo: big_repo::SharedBigRepo,
    policy: watch::Receiver<u64>,
}

fn blob_tags() -> BTreeSet<DatumTag> {
    [Blob::ID, CipherBlob::ID, BlobPin::ID, Jwk::ID]
        .into_iter()
        .map(|tag| DatumTag::new(tag).expect("a blob tag is well formed"))
        .collect()
}

/// Poll one drawer for its blob-bearing docs (ADR 026 §9). The cursor is the work
/// queue's per-source progress, so a restart resumes instead of replaying the drawer.
async fn poll_drawer(
    docs: Client<DocsProtocol>,
    capability: CapId,
    queue: WorkQueue,
    drawer: DocId,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let source = format!("poll:{drawer}");
    let mut after = queue.progress(&source).await?;
    let predicate = Predicate::And {
        clauses: vec![
            Predicate::InDrawer {
                drawer_id: drawer,
                transitive: false,
                at_least: big_cabinet_core::drawer::DrawerAccess::Read,
            },
            Predicate::Or {
                clauses: blob_tags()
                    .into_iter()
                    .map(|tag| Predicate::HasTag { tag })
                    .collect(),
            },
        ],
    };
    let datums = DatumSelection::Of {
        tags: blob_tags(),
        keys: BTreeSet::new(),
    };
    tokio::pin!(shutdown);
    loop {
        let page = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            page = docs.call(RequestContext::root(), big_cabinet_core::doc::PollDatums {
                capability,
                predicate: predicate.clone(),
                datums: datums.clone(),
                after,
                limit: 0,
                hold_ms: 5_000,
            }) => page?,
        };
        let documents: BTreeSet<DocId> = page
            .events
            .iter()
            .map(|event| match event {
                big_cabinet_core::doc::DatumEvent::Enter { doc_id, .. }
                | big_cabinet_core::doc::DatumEvent::Datum { doc_id, .. }
                | big_cabinet_core::doc::DatumEvent::Leave { doc_id, .. } => doc_id.clone(),
            })
            .collect();
        // A JWK change can flip which cipherBlob a slot resolves to, so parked docs
        // re-reconcile with it, as the old frontier reader rearmed on JWK entries.
        let rearm = page.events.iter().any(|event| match event {
            big_cabinet_core::doc::DatumEvent::Enter { .. } => true,
            big_cabinet_core::doc::DatumEvent::Datum { datum_key, .. } => {
                datum_key.tag().as_str() == Jwk::ID
            }
            big_cabinet_core::doc::DatumEvent::Leave { .. } => false,
        });
        queue
            .ingest(&source, page.through, documents, rearm)
            .await?;
        after = page.through;
    }
}

/// Poll one inventory doc for pin changes. Inventories are meta docs, so no drawer
/// predicate reaches them; they are named directly.
async fn poll_inventory(
    docs: Client<DocsProtocol>,
    capability: CapId,
    queue: WorkQueue,
    inventory: DocId,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let source = format!("poll-inventory:{inventory}");
    let mut after = queue.progress(&source).await?;
    let predicate = Predicate::And {
        clauses: vec![
            Predicate::IsDoc {
                doc_id: inventory.clone(),
            },
            Predicate::HasTag {
                tag: DatumTag::new(BlobPin::ID).expect("a blob tag is well formed"),
            },
        ],
    };
    let datums = DatumSelection::Of {
        tags: BTreeSet::from([DatumTag::new(BlobPin::ID).expect("a blob tag is well formed")]),
        keys: BTreeSet::new(),
    };
    tokio::pin!(shutdown);
    loop {
        let page = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            page = docs.call(RequestContext::root(), big_cabinet_core::doc::PollDatums {
                capability,
                predicate: predicate.clone(),
                datums: datums.clone(),
                after,
                limit: 0,
                hold_ms: 5_000,
            }) => page?,
        };
        if !page.events.is_empty() {
            queue.enqueue(&inventory).await?;
        }
        queue
            .ingest(&source, page.through, BTreeSet::new(), false)
            .await?;
        after = page.through;
    }
}

/// Put a reconciled doc's representations into the sync parts they belong to
/// (ADR 026 §7): ciphertext into each containing drawer's inventory part, plaintext
/// into each containing drawer's plaintext part.
async fn distribute_parts(
    config: &WorkerParts,
    retention: &Retention,
    document: &DocId,
    pairs: &[crate::documents::RetainedRepresentation],
) -> Res<()> {
    if pairs.is_empty() {
        return Ok(());
    }
    let mut inventories = retention.inventories_for(document).await?;
    if inventories.is_empty() {
        inventories.push(config.inventory.clone());
    }
    let drawers = config
        .sql
        .query(
            "SELECT drawer_id FROM big_cabinet_drawer_docs WHERE doc_id = ?",
            (document.to_string(),),
        )
        .await?
        .all::<String>()?;
    for drawer in &drawers {
        let drawer: DocId = drawer.parse().expect("a drawer id parses");
        if let Some(groups) = drawer_groups(&config.sql, &drawer).await? {
            crate::permissions::seed_drawer(
                &config.repo,
                &config.parts,
                &drawer,
                groups.docs_group.0,
            )
            .await?;
        }
    }
    for pair in pairs {
        let cipher_obj: ObjKey = pair.ciphertext.into();
        config
            .parts
            .set_obj_payload(
                cipher_obj.clone(),
                serde_json::json!({"lengthOctets": pair.length_octets}),
            )
            .await?;
        config
            .parts
            .add_obj_to_parts(cipher_obj, inventories.iter().map(inventory_part).collect())
            .await?;
        let plain_obj: ObjKey = pair.plaintext.into();
        config
            .parts
            .add_obj_to_parts(
                plain_obj,
                drawers
                    .iter()
                    .map(|drawer| plaintext_part(&drawer.parse().expect("a drawer id parses")))
                    .collect(),
            )
            .await?;
    }
    Ok(())
}

/// The groups the drawer's descriptor states, read from the Cabinet projection the
/// workers share a database with. `None` for layouts without groups (self-doc).
async fn drawer_groups(
    sql: &Database,
    drawer: &DocId,
) -> Res<Option<big_cabinet_core::drawer::DrawerGroups>> {
    let rows = sql
        .query(
            "SELECT descriptor_json FROM big_cabinet_drawers WHERE drawer_id = ?",
            (drawer.to_string(),),
        )
        .await?
        .all::<String>()?;
    let Some(json) = rows.into_iter().next() else {
        return Ok(None);
    };
    let descriptor: big_cabinet_core::drawer::Drawer = serde_json::from_str(&json)?;
    Ok(descriptor.groups())
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
    Ok(std::str::from_utf8(key.slot_bytes())?.parse()?)
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
