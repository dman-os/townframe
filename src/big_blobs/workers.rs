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
use big_sync_core::PartKey;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use futures::future::BoxFuture;
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
use utils_rs::prelude::eyre;

mod sqlite;

#[cfg(test)]
mod tests;

/// The `DrawerMetaDoc` kind whose doc carries a drawer's blob pins (ADR 026 §5).
/// `big_blobs_core` owns the vocabulary; Cabinet only stores the slot.
pub const INVENTORY_KIND: &str = "blobInventory";

pub type MetadataCapability = Arc<
    dyn Fn(
            big_cabinet_core::doc::DocScope,
        ) -> BoxFuture<'static, Result<CapId, ClientError<DocsError>>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct MetadataCapabilities {
    mint: MetadataCapability,
    policy: watch::Receiver<u64>,
    revision: u64,
    capabilities: BTreeMap<(DocId, bool), CapId>,
}

impl MetadataCapabilities {
    pub(crate) fn new(mint: MetadataCapability, policy: watch::Receiver<u64>) -> Self {
        Self {
            mint,
            policy,
            revision: 0,
            capabilities: BTreeMap::new(),
        }
    }

    pub(crate) async fn call<T, F: Future<Output = Result<T, ClientError<DocsError>>>>(
        &mut self,
        document: &DocId,
        write: bool,
        operation: impl FnOnce(CapId) -> F,
    ) -> Result<T, ClientError<DocsError>> {
        let key = (document.clone(), write);
        let revision = *self.policy.borrow();
        if self.revision != revision {
            self.revision = revision;
            self.capabilities.clear();
        }
        let capability = if let Some(capability) = self.capabilities.get(&key) {
            *capability
        } else {
            let capability = (self.mint)(big_cabinet_core::doc::DocScope {
                read: BTreeSet::from([document.clone()]),
                write: if write {
                    BTreeSet::from([document.clone()])
                } else {
                    BTreeSet::new()
                },
                ..Default::default()
            })
            .await?;
            self.capabilities.insert(key.clone(), capability);
            capability
        };
        let result = operation(capability).await;
        if matches!(
            result,
            Err(ClientError::Operation(
                DocsError::Denied | DocsError::Access(_) | DocsError::AuthorityUnavailable(_)
            ))
        ) {
            self.capabilities.remove(&key);
        }
        result
    }
}

pub struct WorkerConfig {
    pub sql: Database,
    pub service: Arc<BlobService>,
    pub docs: Client<DocsProtocol>,
    pub capability: CapId,
    pub metadata_capability: MetadataCapability,
    /// The drawers whose docs this node maintains blobs for (ADR 026 §9). Each gets its
    /// own `PollDatums` cursor; inventories are discovered per drawer at flush time.
    pub drawers: Vec<DocId>,
    pub datum_set: Arc<DatumSetStore>,
    pub parts: SharedPartStore,
    pub repo: big_repo::SharedBigRepo,
    pub policy: watch::Receiver<u64>,
    pub metadata_observer: big_cabinet::drawer::DrawerService,
}

pub struct BlobWorkers {
    pub projection: Arc<BlobProjection>,
    pub parts: SharedPartStore,
    pub mirrors: crate::admission::InventoryMirrors,
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
    config
        .sql
        .apply_migrations(
            sqlite::queries::MIGRATION_TABLE,
            sqlite::queries::MIGRATIONS,
        )
        .await?;
    let metadata = MetadataCapabilities::new(
        Arc::clone(&config.metadata_capability),
        config.policy.clone(),
    );
    let retention = Retention::new(
        config.sql.clone(),
        Arc::clone(&config.service),
        config.docs.clone(),
        metadata.clone(),
        Arc::clone(&config.parts),
    )
    .await?;
    let adapter = DocumentBlobs::new(
        config.docs.clone(),
        config.capability,
        config.service.store().clone(),
        Arc::clone(&config.service.provider),
        config.service.roots.clone(),
    );
    let (mirrors, mirror_stop) = crate::admission::InventoryMirrors::boot(
        config.sql.clone(),
        Arc::clone(&config.service),
        Arc::clone(&config.parts),
        Arc::clone(&config.repo),
        queue.clone(),
    )
    .await?;
    let workers = BlobWorkers {
        projection: Arc::clone(&projection),
        parts: Arc::clone(&config.parts),
        mirrors: mirrors.clone(),
    };
    let (stop, tasks) = StopToken::new();
    mirrors.start_discovery(
        &tasks,
        config.metadata_observer.clone(),
        Arc::clone(&config.datum_set),
        config.sql.clone(),
        config.policy.clone(),
    );
    let cancel = tasks.clone();
    let permissions_repo = Arc::clone(&config.repo);
    let permissions_queue = queue.clone();
    let permissions_parts = Arc::clone(&config.parts);
    let permissions_sql = config.sql.clone();
    let permissions_mirrors = mirrors.clone();
    tasks
        .spawn(async move {
            crate::permissions::run(
                permissions_repo,
                permissions_parts,
                permissions_sql,
                permissions_queue,
                permissions_mirrors,
                cancel.cancelled(),
            )
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
        queue.enqueue(drawer).await?;
        let drawer = drawer.clone();
        tasks
            .spawn(async move {
                poll_drawer(docs, capability, queue, drawer, cancel.cancelled())
                    .await
                    .unwrap();
            })
            .expect("drawer poll starts before shutdown");
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
    let worker = WorkerParts {
        sql: config.sql.clone(),
        service: Arc::clone(&config.service),
        docs: config.docs.clone(),
        capability: config.capability,
        metadata,
        mirrors,
        metadata_observer: config.metadata_observer,
        policy: config.policy.clone(),
    };
    spawn_worker(&tasks, worker, queue, retention, adapter, projection);
    Ok((workers, stop.then(mirror_stop)))
}

fn spawn_worker(
    tasks: &service_utils_rs::Spawner,
    config: WorkerParts,
    queue: WorkQueue,
    mut retention: Retention,
    adapter: DocumentBlobs,
    projection: Arc<BlobProjection>,
) {
    let cancel = tasks.clone();
    let inventory_tasks = tasks.clone();
    tasks
        .spawn(async move {
            let config = &config;
            let mut metadata = config.metadata.clone();
            let run = async {
            let mut polled = BTreeSet::new();
            let mut availability = config.service.open_availability(queue.progress("availability").await?).await?;
            let mut policy = config.policy.clone();
            poll_writer_documents(&inventory_tasks, config, &queue, &mut polled, None).await?;
            loop {
                if cancel.is_cancelled() { return Ok::<_, utils_rs::prelude::eyre::Report>(()); }
                for inventory in retention.flush().await? { queue.enqueue(&inventory).await?; }
                if let Some((document, generation)) = queue.next().await? {
                    if let Some(waiting) = enqueue_root_members(config, &queue, &document).await? {
                        poll_writer_documents(&inventory_tasks, config, &queue, &mut polled, Some(&document)).await?;
                        queue.finish(&document, generation, waiting).await?;
                        continue;
                    }
                    let context = RequestContext::root();
                     let inventory_doc = retention.is_inventory(&document).await?
                         || config.mirrors.admission.inventory(&inventory_part(&document)).await?.is_some();
                    let read = if inventory_doc {
                        metadata.call(&document, false, |capability| config.docs.call(context.child(), ReadDatums {
                            capability, doc_id: document.clone(), at: At::Current, datums: None, optimistic: false,
                        })).await
                    } else {
                        config.docs.call(context.child(), ReadDatums {
                            capability: config.capability, doc_id: document.clone(), at: At::Current, datums: None, optimistic: false,
                        }).await
                    };
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
                                        DatumRead::Pending { .. } | DatumRead::Rejected { .. } | DatumRead::Undecodable { .. } => pending = true,
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
                        Err(ClientError::Operation(DocsError::Denied | DocsError::Access(_))) => (Vec::new(), BTreeMap::new(), true),
                        Err(ClientError::Operation(DocsError::DocNotFound | DocsError::Pending { .. } | DocsError::UnknownHeads { .. } | DocsError::AuthorityUnavailable(_))) => {
                            queue.finish(&document, generation, true).await?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    retention.replace(&document, &pairs, pins.clone(), waiting).await?;
                    poll_writer_documents(&inventory_tasks, config, &queue, &mut polled, Some(&document)).await?;
                    for inventory in retention.flush().await? { queue.enqueue(&inventory).await?; }
                    if inventory_doc {
                         config.mirrors.reconcile(&document, generation, &pins, !waiting).await?;
                    }
                    if !waiting {
                        for unresolved in config.service.roots.unresolved().await? {
                            if unresolved.claimed_by.as_ref().is_some_and(|(doc, _)| doc == &document.to_string()) {
                                let id = BlobId::new(*unresolved.cipher_hash.as_bytes());
                                config.service.ownership.resolve_pair(&id).await?;
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
                             config.mirrors.admission.rearm().await?;

                        }
                    }
                    changed = policy.changed() => {
                        changed.expect("drawer policy outlives blob workers");
                         queue.rearm().await?;
                         projection.rearm();
                         config.mirrors.refresh_policy().await?;

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
    metadata: MetadataCapabilities,
    mirrors: crate::admission::InventoryMirrors,
    metadata_observer: big_cabinet::drawer::DrawerService,
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
                drawer_id: drawer.clone(),
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

async fn poll_document(
    docs: Client<DocsProtocol>,
    mut metadata: MetadataCapabilities,
    queue: WorkQueue,
    document: DocId,
    mut policy: watch::Receiver<u64>,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let source = format!("poll-document:{document}");
    let mut after = queue.progress(&source).await?;
    let predicate = Predicate::IsDoc {
        doc_id: document.clone(),
    };
    let datums = DatumSelection::All;
    tokio::pin!(shutdown);
    loop {
        let page = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            page = metadata.call(&document, false, |capability| docs.call(RequestContext::root(), big_cabinet_core::doc::PollDatums {
                capability,
                predicate: predicate.clone(),
                datums: datums.clone(),
                after,
                limit: 0,
                hold_ms: 5_000,
            })) => page,
        };
        let page = match page {
            Ok(page) => page,
            Err(ClientError::Operation(
                DocsError::Denied
                | DocsError::DocNotFound
                | DocsError::Pending { .. }
                | DocsError::UnknownHeads { .. }
                | DocsError::AuthorityUnavailable(_)
                | DocsError::Access(_),
            )) => {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    changed = policy.changed() => changed.expect("drawer policy outlives inventory polls"),
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let documents = if page.events.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from([document.clone()])
        };
        queue
            .ingest(&source, page.through, documents, false)
            .await?;
        after = page.through;
    }
}

async fn poll_writer_documents(
    tasks: &service_utils_rs::Spawner,
    config: &WorkerParts,
    queue: &WorkQueue,
    polled: &mut BTreeSet<DocId>,
    document: Option<&DocId>,
) -> Res<()> {
    let rows =
        sqlite::queries::writer_documents(&config.sql, document.map(ToString::to_string)).await?;
    for row in rows {
        let document = row.document;
        let inventory = row.inventory;
        let document: DocId = document.parse()?;
        if !polled.insert(document.clone()) {
            continue;
        }
        if inventory {
            config.mirrors.refresh_authorization(&document).await?;
        }
        let docs = config.docs.clone();
        let metadata = config.metadata.clone();
        let queue = queue.clone();
        let policy = config.policy.clone();
        let cancel = tasks.clone();
        if tasks
            .spawn(async move {
                poll_document(docs, metadata, queue, document, policy, cancel.cancelled())
                    .await
                    .unwrap();
            })
            .is_none()
        {
            return Ok(());
        }
    }
    Ok(())
}

async fn enqueue_root_members(
    config: &WorkerParts,
    queue: &WorkQueue,
    document: &DocId,
) -> Res<Option<bool>> {
    use big_cabinet::drawer::MetadataRootStatus;
    use big_cabinet_core::drawer::protocol::DrawersError;
    let sql = &config.sql;

    let roots = sqlite::queries::root_drawers(sql, document.to_string()).await?;
    let mut metadata = None;
    for drawer in roots {
        let descriptor = sqlite::queries::drawer_descriptor(sql, drawer.clone())
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("drawer descriptor missing"))?;

        let descriptor: big_cabinet_core::drawer::Drawer = serde_json::from_str(&descriptor)?;
        if descriptor.groups().is_none() {
            continue;
        }
        let waiting = metadata.get_or_insert(false);
        match config
            .metadata_observer
            .observe_metadata(&RequestContext::root(), &drawer.parse()?)
            .await
        {
            Ok(observation) => {
                *waiting |= observation
                    .roots
                    .values()
                    .any(|status| !matches!(status, MetadataRootStatus::Authoritative))
            }
            Err(
                DrawersError::Denied
                | DrawersError::RosterUnreadable(_)
                | DrawersError::Docs(
                    DocsError::Denied
                    | DocsError::Access(_)
                    | DocsError::DocNotFound
                    | DocsError::Pending { .. }
                    | DocsError::UnknownHeads { .. }
                    | DocsError::AuthorityUnavailable(_),
                )
                | DrawersError::AuthorityUnavailable(_)
                | DrawersError::DocsUnavailable(_),
            ) => {
                *waiting = true;
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        let members = sqlite::queries::drawer_members(sql, drawer.clone()).await?;

        for member in members {
            queue.enqueue(&member.parse()?).await?;
        }
    }
    Ok(metadata)
}

fn pin_id(key: &DatumKey) -> Res<BlobId> {
    Ok(std::str::from_utf8(key.slot_bytes())?.parse()?)
}
