//! ObserveFacets: doc-scoped observation over the ungated `facet_set` projection,
//! filtered by the same read access as gated reads (ADR 009 §6).

use super::*;
use crate::index::facet_set::FacetMembership;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits, RevisionedStoreReader};

/// The scope of an [`ObserveFacets`] subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "camelCase")]
pub enum ObserveScope {
    /// Every facet route in one document. Read access to the document is
    /// required before the stream opens.
    Doc { doc_id: DocId },
    /// Every facet route carrying one tag, across all documents. Each
    /// document is filtered by the observer's read access as entries arrive.
    Tag { tag: FacetTag },
}

/// One observed facet route, as projected by `facet_set` (ADR 009). The
/// projection is ungated: `version` carries unverified signature-introduction
/// heads, and consumers that read values do so through the gated read path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
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
        if let ObserveScope::Doc { doc_id } = &params.scope {
            self.authorize(&request.context, &actor, AccessRequest::Read(doc_id))
                .await?;
        }
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
            // Tag scope opens only its own frontier namespace; Doc scope has
            // no frontier dimension narrower than the whole projection, so it
            // opens everything and filters by document below.
            let opened = match &params.scope {
                ObserveScope::Doc { .. } => store.open_all(params.after).await,
                ObserveScope::Tag { tag } => {
                    store.open_tags([(tag.clone(), params.after)]).await
                }
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
            let mut lost: BTreeSet<DocId> = BTreeSet::new();
            'pump: loop {
                let read = tokio::select! {
                    _ = tasks.cancelled() => break 'pump,
                    _ = sender.closed() => break 'pump,
                    read = reader.next(RevisionReadLimits::default()) => read,
                };
                let entries = match read {
                    Ok(RevisionRead::Entries { entries, .. }) => entries,
                    Ok(RevisionRead::ReplayComplete { through }) => {
                        if sender
                            .send(Ok(FacetObserved::ReplayComplete { through }))
                            .await
                            .is_err()
                        {
                            break 'pump;
                        }
                        continue;
                    }
                    Err(error) => {
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
                    };
                    if !in_scope {
                        continue;
                    }
                    let allowed = match checks.get(&route.doc_id) {
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
                    };
                    if !allowed {
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
                    delivered.insert(route.doc_id.clone());
                    lost.remove(&route.doc_id);
                    let facet = match value {
                        Some(FacetMembership::Present {
                            doc_heads,
                            facet_heads,
                            clock,
                        }) => ObservedFacet::Present {
                            doc_heads,
                            version: FacetVersion {
                                heads: facet_heads,
                                clock,
                            },
                        },
                        Some(FacetMembership::Removed {
                            doc_heads,
                            facet_heads,
                            clock,
                        }) => ObservedFacet::Removed {
                            doc_heads,
                            version: FacetVersion {
                                heads: facet_heads,
                                clock,
                            },
                        },
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
