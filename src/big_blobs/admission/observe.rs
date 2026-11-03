use super::*;
use crate::admission::discovery::DrawerSnapshot;
use big_cabinet::drawer::DrawerMetadataObservation;
use big_cabinet::drawer::DrawerService;
use big_cabinet::drawer::MetadataRootStatus;
use big_cabinet::index::DatumSetStore;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::drawer::DrawerLayout;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use futures::FutureExt;
use service_utils_rs::RequestContext;
use std::collections::BTreeSet;
use std::collections::HashMap;

impl InventoryMirrors {
    pub(crate) fn start_discovery(
        &self,
        tasks: &Spawner,
        observer: DrawerService,
        datum_set: Arc<DatumSetStore>,
        sql: Database,
        mut policy: watch::Receiver<u64>,
    ) {
        let runtime = self.clone();
        let cancellation = tasks.clone();
        let input = self.clone();
        let input_sql = sql.clone();
        let input_cancel = tasks.clone();
        tasks
            .spawn(async move {
                input
                    .discovery_input(datum_set, input_sql, input_cancel)
                    .await
                    .unwrap();
            })
            .expect("discovery ingress starts before shutdown");
        tasks.spawn(async move {
            let mut changes = runtime.admission.subscribe_changes();
            let mut interests = HashMap::new();
            loop {
                if cancellation.is_cancelled() { return Ok(()); }
                changes.borrow_and_update();
                runtime.discovery_interests(&sql, &mut interests).await?;
                if let Some((drawer, generation)) = runtime.admission.discovery_work().await? {
                    let observation = match observer.observe_metadata(&RequestContext::root(), &drawer).await {
                        Ok(observation) => Some(observation),
                        Err(error) if unavailable(&error) => None,
                        Err(error) => return Err(error.into()),
                    };
                    let snapshot = observation.and_then(snapshot);
                    runtime.admission.observe_drawer(&drawer, generation, snapshot).await?;
                    continue;
                }
                let mut readiness: FuturesUnordered<_> = interests.iter().map(|(document, (_, receiver))| {
                    let document = document.clone();
                    let mut readiness = receiver.clone();
                    Box::pin(async move {
                        readiness.changed().await.expect("document readiness outlives discovery interest");
                        document
                    })
                }).collect();
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Ok::<_, utils_rs::prelude::eyre::Report>(()),
                    changed = changes.changed() => changed.expect("admission outlives discovery"),
                    changed = policy.changed() => {
                        changed.expect("drawer policy outlives discovery");
                        runtime.admission.rearm_discovery().await?;
                    }
                    Some(document) = readiness.next(), if !interests.is_empty() => {
                        interests.get_mut(&document).unwrap().1.borrow_and_update();
                        runtime.invalidate_metadata(&sql, BTreeSet::from([document.clone()])).await?;
                        if runtime.admission.selected_inventories().await?.contains(&document) {
                            runtime.refresh_authorization(&document).await?;
                            runtime.queue.enqueue(&document).await?;
                        }
                    }
                }
            }
        }.map(|result| result.unwrap())).expect("discovery starts before shutdown");
    }

    async fn discovery_interests(
        &self,
        sql: &Database,
        interests: &mut HashMap<DocId, (big_repo::DocInterest, watch::Receiver<bool>)>,
    ) -> Res<()> {
        let drawers = self.admission.discovery_documents().await?;
        let inventories = self.admission.selected_inventories().await?;
        let mut desired: BTreeSet<_> = drawers.iter().chain(&inventories).cloned().collect();
        for root in sql
            .query(
                "SELECT json_extract(descriptor_json, '$.layout.sensitiveRoot')
               FROM big_cabinet_drawers
              WHERE drawer_id IN (SELECT value FROM json_each(?))
                AND json_extract(descriptor_json, '$.layout.sensitiveRoot') IS NOT NULL",
                (serde_json::to_string(
                    &drawers.iter().map(ToString::to_string).collect::<Vec<_>>(),
                )?,),
            )
            .await?
            .all::<String>()?
        {
            let root: DocId = root.parse()?;
            if self.projection.readable(&root).await? {
                desired.insert(root);
            }
        }
        interests.retain(|document, _| desired.contains(document));
        for document in desired {
            if !interests.contains_key(&document) {
                let interest = self.projection.repo.interest(document.clone()).await?;
                let mut readiness = interest.readiness();
                let ready = *readiness.borrow_and_update();
                interests.insert(document.clone(), (interest, readiness));
                if ready {
                    self.invalidate_metadata(sql, BTreeSet::from([document.clone()]))
                        .await?;
                }
                if inventories.contains(&document) {
                    self.refresh_authorization(&document).await?;
                    self.queue.enqueue(&document).await?;
                }
            }
        }
        Ok(())
    }

    async fn discovery_input(
        &self,
        datum_set: Arc<DatumSetStore>,
        sql: Database,
        cancellation: Spawner,
    ) -> Res<()> {
        let mut reader = datum_set
            .open_all(self.queue.progress("drawer-discovery").await?)
            .await?;
        loop {
            let read = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                read = reader.next(RevisionReadLimits::default()) => read?,
            };
            if let RevisionRead::Entries { revision, entries } = read {
                let documents: BTreeSet<_> =
                    entries.into_iter().map(|entry| entry.key.doc_id).collect();
                self.invalidate_metadata(&sql, documents.clone()).await?;
                let inventories: BTreeSet<_> = self
                    .admission
                    .selected_inventories()
                    .await?
                    .into_iter()
                    .collect();
                self.queue
                    .ingest(
                        "drawer-discovery",
                        revision,
                        documents.intersection(&inventories).cloned().collect(),
                        false,
                    )
                    .await?;
            }
        }
    }

    async fn invalidate_metadata(&self, sql: &Database, mut documents: BTreeSet<DocId>) -> Res<()> {
        let encoded = serde_json::to_string(
            &documents
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
        for drawer in sql.query(
            "SELECT drawer_id
               FROM big_cabinet_drawers
              WHERE json_extract(descriptor_json, '$.layout.sensitiveRoot') IN (SELECT value FROM json_each(?))",
            (encoded,),
        ).await?.all::<String>()? {
            documents.insert(drawer.parse()?);
        }
        self.admission.invalidate_discovery(documents).await
    }
}

fn snapshot(observation: DrawerMetadataObservation) -> Option<DrawerSnapshot> {
    let self_doc = match observation.descriptor.layout {
        DrawerLayout::SelfDoc(_) => true,
        DrawerLayout::SingleGroup(_) => false,
        DrawerLayout::Unknown(_) => return None,
    };
    let roots = observation
        .roots
        .into_iter()
        .filter_map(|(root, status)| {
            matches!(status, MetadataRootStatus::Authoritative).then_some(root)
        })
        .collect();
    let mut inventories = BTreeMap::new();
    for (root, slots) in observation.meta_docs {
        for (slot, document) in slots {
            if let Some(name) = slot.strip_prefix("blobInventory/") {
                inventories.insert((root, name.to_owned()), document);
            }
        }
    }
    Some(DrawerSnapshot {
        self_doc,
        roots,
        inventories,
        children: observation.children.into_keys().collect(),
    })
}

fn unavailable(error: &DrawersError) -> bool {
    matches!(
        error,
        DrawersError::Denied
            | DrawersError::UnknownDrawer(_)
            | DrawersError::RosterUnreadable(_)
            | DrawersError::AuthorityUnavailable(_)
            | DrawersError::DocsUnavailable(_)
            | DrawersError::Docs(
                DocsError::Denied
                    | DocsError::Access(_)
                    | DocsError::DocNotFound
                    | DocsError::Pending { .. }
                    | DocsError::UnknownHeads { .. }
                    | DocsError::AuthorityUnavailable(_)
            )
    )
}
