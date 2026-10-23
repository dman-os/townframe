//! ObserveFacets: doc-scoped observation over the ungated `facet_set` projection,
//! filtered by the same read access as gated reads (ADR 009 §6).

use super::*;
use crate::index::facet_set::{FacetMembership, FacetRoute};
use big_sync_core::keyed_frontier::{FrontierEntry, KeyedFrontierError};
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};

/// The scope of an [`ObserveFacets`] subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "scope",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObserveScope {
    /// Every facet route in one document. Read access to the document is
    /// required before the stream opens.
    Doc { doc_id: DocId },
    /// Every facet route carrying one tag, across all documents. Each
    /// document is filtered by the observer's read access as entries arrive.
    Tag { tag: FacetTag },
    /// Every facet route (optionally narrowed to `tag`) in documents the
    /// observer may read through `drawer_id`. Read access on `drawer_id` is
    /// required before the stream opens, and live drawer membership or grant
    /// changes emit `Present` or `AccessLost` for affected documents.
    Drawer {
        drawer_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag: Option<FacetTag>,
    },
}

/// One observed facet route, as projected by `facet_set` (ADR 009). The
/// projection is ungated: `version` carries unverified signature-introduction
/// heads, and consumers that read values do so through the gated read path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ObservedFacet {
    Present {
        doc_heads: Vec<ChangeHash>,
        version: FacetVersion,
    },
    Removed {
        doc_heads: Option<Vec<ChangeHash>>,
        version: FacetVersion,
    },
    /// The observer lost (or never had) read access to this event's document.
    /// Emitted once per document per loss; observation of the document
    /// resumes if access returns.
    AccessLost,
}

/// One item on an [`ObserveFacets`] stream: either a `facet_set` frontier
/// entry filtered to the observer's scope and authority, or the marker that
/// the replay captured when the stream opened has been fully delivered.
/// `revision` on an entry is the frontier revision to resume from (`after`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum FacetObserved {
    Entry {
        revision: u64,
        doc_id: DocId,
        facet_key: FacetKey,
        facet: ObservedFacet,
    },
    /// Every entry at or below `through` that the stream will ever replay has
    /// been delivered (subject to authority filtering); later entries are live.
    ReplayComplete { through: u64 },
}

/// Subscription: facet membership changes after frontier revision `after`
/// (0 replays the current matching set). See ADR 009 §6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveFacets {
    pub capability: CapId,
    pub scope: ObserveScope,
    #[serde(default)]
    pub after: u64,
}

impl Operation for ObserveFacets {
    type Output = EventStream<FacetObserved, DocsError>;
    type Error = DocsError;
    const XRPC: XrpcOperation =
        XrpcOperation::subscription("org.example.daybook.docs.observeFacets")
            .with_schemas("FacetObserved", "DocsError");
}

impl Subscription for ObserveFacets {
    type Event = FacetObserved;
}

impl Handler<ObserveFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<ObserveFacets>,
    ) -> Result<ResponseEnvelope<EventStream<FacetObserved, DocsError>>, DocsError> {
        let params = request.params;
        let actor = self.actor(&request.context, params.capability).await?;
        let mut changes = self.access.subscribe_changes();
        let mut drawer_readable = match &params.scope {
            ObserveScope::Doc { doc_id } => {
                self.authorize(&request.context, &actor, AccessRequest::Read(doc_id))
                    .await?;
                None
            }
            ObserveScope::Tag { .. } => None,
            ObserveScope::Drawer { drawer_id, .. } => {
                let readable = self
                    .readable_in_drawer(&request.context, &actor, drawer_id)
                    .await?;
                if let Some(rx) = changes.as_mut() {
                    rx.borrow_and_update();
                }
                Some(readable)
            }
        };
        let Some(store) = self.facet_set.clone() else {
            return Err(DocsError::Repository(
                "facet observation is not configured on this repository".into(),
            ));
        };
        let access = Arc::clone(&self.access);
        let tasks = self.tasks.clone();
        let pump_tasks = tasks.clone();
        let context = request.context.child();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let Some(_pump) = tasks.spawn(async move {
            let tasks = pump_tasks;
            // Tag scope (and tag-filtered Drawer scope) opens only its own
            // frontier namespace; Doc and unfiltered Drawer scopes open
            // everything and filter by document below.
            let opened = match &params.scope {
                ObserveScope::Doc { .. } | ObserveScope::Drawer { tag: None, .. } => {
                    store.open_all(params.after).await
                }
                ObserveScope::Tag { tag }
                | ObserveScope::Drawer {
                    tag: Some(tag), ..
                } => store.open_tags([(tag.clone(), params.after)]).await,
            };
            let mut reader = match opened {
                Ok(reader) => reader,
                Err(error) => {
                    drop(sender.send(Err(repository(error))).await);
                    return;
                }
            };
            // Documents this stream has delivered an entry for. `AccessLost`
            // is only meaningful — and only non-leaking — for those: naming a
            // document the observer never saw would disclose its existence.
            // A `Doc` scope names its document itself, so it starts delivered.
            let mut delivered: BTreeSet<DocId> = BTreeSet::new();
            if let ObserveScope::Doc { doc_id } = &params.scope {
                delivered.insert(doc_id.clone());
            }
            let mut delivered_keys: BTreeMap<DocId, FacetKey> = BTreeMap::new();
            let mut last_present: BTreeMap<(DocId, FacetKey), (Vec<ChangeHash>, FacetVersion)> =
                BTreeMap::new();
            let mut lost: BTreeSet<DocId> = BTreeSet::new();
            let mut replay_complete = false;
            enum Wake {
                Frontier(
                    Result<
                        RevisionRead<u64, FrontierEntry<FacetRoute, FacetMembership>>,
                        KeyedFrontierError,
                    >,
                ),
                MembershipChanged,
            }
            'pump: loop {
                let wake = tokio::select! {
                    _ = tasks.cancelled() => break 'pump,
                    _ = sender.closed() => break 'pump,
                    Ok(()) = async {
                        match changes.as_mut() {
                            Some(rx) => rx.changed().await,
                            None => std::future::pending().await,
                        }
                    }, if replay_complete && drawer_readable.is_some() => Wake::MembershipChanged,
                    read = reader.next(RevisionReadLimits::default()) => Wake::Frontier(read),
                };
                let entries = match wake {
                    Wake::MembershipChanged => {
                        let ObserveScope::Drawer { drawer_id, tag } = &params.scope else {
                            continue;
                        };
                        let current_readable = drawer_readable
                            .as_mut()
                            .expect("drawer scope initializes readable set");
                        let next_readable = match access
                            .readable_in_drawer(&context, actor.uuid, drawer_id)
                            .await
                        {
                            Ok(Some(docs)) => docs,
                            Ok(None) => BTreeSet::new(),
                            Err(error) => {
                                drop(sender.send(Err(access_error(error))).await);
                                break 'pump;
                            }
                        };
                        let revision = match store.latest_revision().await {
                            Ok(revision) => revision,
                            Err(error) => {
                                drop(sender.send(Err(repository(error))).await);
                                break 'pump;
                            }
                        };
                        let removed_docs: Vec<DocId> = current_readable
                            .difference(&next_readable)
                            .cloned()
                            .collect();
                        let added_docs: BTreeSet<DocId> = next_readable
                            .difference(current_readable)
                            .cloned()
                            .collect();
                        *current_readable = next_readable;
                        for doc_id in removed_docs {
                            last_present.retain(|(doc, _), _| doc != &doc_id);
                            if delivered.contains(&doc_id)
                                && lost.insert(doc_id.clone())
                                && let Some(facet_key) = delivered_keys.get(&doc_id).cloned()
                                && sender
                                    .send(Ok(FacetObserved::Entry {
                                        revision,
                                        doc_id,
                                        facet_key,
                                        facet: ObservedFacet::AccessLost,
                                    }))
                                    .await
                                    .is_err()
                            {
                                break 'pump;
                            }
                        }
                        if !added_docs.is_empty() {
                            let records = match store.query_docs(&added_docs, tag.as_ref(), None).await {
                                Ok(records) => records,
                                Err(error) => {
                                    drop(sender.send(Err(repository(error))).await);
                                    break 'pump;
                                }
                            };
                            for record in records {
                                let version = FacetVersion {
                                    heads: record.facet_heads,
                                    clock: record.clock,
                                };
                                let pair = (record.route.doc_id.clone(), record.route.facet_key.clone());
                                let state = (record.doc_heads.clone(), version.clone());
                                if last_present.get(&pair) == Some(&state) {
                                    continue;
                                }
                                last_present.insert(pair, state);
                                delivered.insert(record.route.doc_id.clone());
                                delivered_keys
                                    .entry(record.route.doc_id.clone())
                                    .or_insert_with(|| record.route.facet_key.clone());
                                lost.remove(&record.route.doc_id);
                                if sender
                                    .send(Ok(FacetObserved::Entry {
                                        revision,
                                        doc_id: record.route.doc_id,
                                        facet_key: record.route.facet_key,
                                        facet: ObservedFacet::Present {
                                            doc_heads: record.doc_heads,
                                            version,
                                        },
                                    }))
                                    .await
                                    .is_err()
                                {
                                    break 'pump;
                                }
                            }
                        }
                        continue;
                    }
                    Wake::Frontier(Ok(RevisionRead::Entries { entries, .. })) => entries,
                    Wake::Frontier(Ok(RevisionRead::ReplayComplete { through })) => {
                        replay_complete = true;
                        if sender
                            .send(Ok(FacetObserved::ReplayComplete { through }))
                            .await
                            .is_err()
                        {
                            break 'pump;
                        }
                        continue;
                    }
                    Wake::Frontier(Err(error)) => {
                        drop(sender.send(Err(repository(error))).await);
                        break 'pump;
                    }
                };
                // Access may change between batches but not within one read of
                // the committed projection, so checks memoize per batch.
                let mut checks: BTreeMap<DocId, bool> = BTreeMap::new();
                for entry in entries {
                    let FrontierEntry {
                        revision,
                        key: route,
                        value,
                    } = entry;
                    let in_scope = match &params.scope {
                        ObserveScope::Doc { doc_id } => &route.doc_id == doc_id,
                        ObserveScope::Tag { tag } => route.facet_key.tag() == tag,
                        ObserveScope::Drawer { tag, .. } => {
                            tag.as_ref().is_none_or(|tag| route.facet_key.tag() == tag)
                        }
                    };
                    if !in_scope {
                        continue;
                    }
                    let allowed = if let Some(readable) = &drawer_readable {
                        readable.contains(&route.doc_id)
                    } else {
                        match checks.get(&route.doc_id) {
                            Some(allowed) => *allowed,
                            None => {
                                let checked = match access
                                    .check(
                                        &context,
                                        actor.uuid,
                                        AccessRequest::Read(&route.doc_id),
                                    )
                                    .await
                                {
                                    Ok(allowed) => allowed,
                                    Err(error) => {
                                        drop(sender.send(Err(access_error(error))).await);
                                        break 'pump;
                                    }
                                };
                                checks.insert(route.doc_id.clone(), checked);
                                checked
                            }
                        }
                    };
                    if !allowed {
                        last_present.retain(|(doc, _), _| doc != &route.doc_id);
                        // Silent for never-delivered documents; one tombstone
                        // per loss for documents this stream has delivered.
                        if delivered.contains(&route.doc_id)
                            && lost.insert(route.doc_id.clone())
                            && sender
                                .send(Ok(FacetObserved::Entry {
                                    revision,
                                    doc_id: route.doc_id,
                                    facet_key: route.facet_key,
                                    facet: ObservedFacet::AccessLost,
                                }))
                                .await
                                .is_err()
                        {
                            break 'pump;
                        }
                        continue;
                    }
                    let facet = match value {
                        Some(FacetMembership::Present {
                            doc_heads,
                            facet_heads,
                            clock,
                        }) => {
                            let version = FacetVersion {
                                heads: facet_heads,
                                clock,
                            };
                            let pair = (route.doc_id.clone(), route.facet_key.clone());
                            let state = (doc_heads.clone(), version.clone());
                            if last_present.get(&pair) == Some(&state) {
                                continue;
                            }
                            last_present.insert(pair, state);
                            ObservedFacet::Present { doc_heads, version }
                        }
                        Some(FacetMembership::Removed {
                            doc_heads,
                            facet_heads,
                            clock,
                        }) => {
                            last_present.remove(&(route.doc_id.clone(), route.facet_key.clone()));
                            ObservedFacet::Removed {
                                doc_heads,
                                version: FacetVersion {
                                    heads: facet_heads,
                                    clock,
                                },
                            }
                        }
                        // The frontier only ever applies membership values;
                        // a tombstone row here means the projection broke its
                        // own contract, so fail the stream rather than forge
                        // a clock-0 removal.
                        None => {
                            drop(
                                sender
                                    .send(Err(repository(
                                        "facet-set frontier published an entry without membership",
                                    )))
                                    .await,
                            );
                            break 'pump;
                        }
                    };
                    delivered.insert(route.doc_id.clone());
                    delivered_keys
                        .entry(route.doc_id.clone())
                        .or_insert_with(|| route.facet_key.clone());
                    lost.remove(&route.doc_id);
                    if sender
                        .send(Ok(FacetObserved::Entry {
                            revision,
                            doc_id: route.doc_id,
                            facet_key: route.facet_key,
                            facet,
                        }))
                        .await
                        .is_err()
                    {
                        break 'pump;
                    }
                }
            }
        }) else {
            return Err(DocsError::Repository(
                "documents service is shutting down".into(),
            ));
        };
        let stream: EventStream<FacetObserved, DocsError> = Box::pin(futures::stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
        ));
        Ok(stream.into())
    }
}
