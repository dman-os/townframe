//! ObserveFacets: doc-scoped observation over the ungated `facet_set` projection,
//! filtered by the same read access as gated reads (ADR 009 §6).

use super::*;
use crate::authority::{CapabilityStatus, ObserveCapability};
use crate::index::facet_set::{FacetMembership, FacetRoute};
use big_sync_core::keyed_frontier::{FrontierEntry, KeyedFrontierError};
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use futures::StreamExt;

impl Handler<ObserveFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<ObserveFacets>,
    ) -> Result<ResponseEnvelope<EventStream<FacetObserved, DocsError>>, DocsError> {
        let params = request.params;
        let mut authority_changes = self
            .authority
            .subscribe(
                request.context.child(),
                ObserveCapability {
                    capability: params.capability,
                },
            )
            .await
            .map_err(authority_error)?;
        let caller = self.invocation(&request.context, params.capability).await?;
        let capabilities = self.capabilities.clone();
        let mut changes = self.access.subscribe_changes();
        let mut drawer_readable = match &params.scope {
            ObserveScope::Doc { doc_id } => {
                self.authorize(
                    &request.context,
                    &caller.authority,
                    AccessRequest::Read(doc_id),
                )
                .await?;
                None
            }
            ObserveScope::Tag { .. } => None,
            ObserveScope::Drawer { drawer_id, .. } => {
                let readable = Self::readable_in_drawer(
                    self.access.as_ref(),
                    &request.context,
                    &caller.authority,
                    drawer_id,
                )
                .await?;
                Some(readable)
            }
        };
        let Some(store) = self.facet_set.clone() else {
            return Err(DocsError::Repository(
                "facet observation is not configured on this repository".into(),
            ));
        };
        let repo = Arc::clone(&self.big_repo);
        let mut interest = match &params.scope {
            ObserveScope::Doc { doc_id } => {
                Some(repo.interest(doc_id.clone()).await.map_err(repository)?)
            }
            ObserveScope::Tag { .. } | ObserveScope::Drawer { .. } => None,
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
                ObserveScope::Tag { tag } | ObserveScope::Drawer { tag: Some(tag), .. } => {
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
                    }, if replay_complete => Wake::MembershipChanged,
                    change = authority_changes.next() => match change {
                        Some(Ok(CapabilityStatus::Live)) => Wake::MembershipChanged,
                        Some(Ok(CapabilityStatus::Retracted)) => {
                            drop(sender.send(Err(DocsError::Denied)).await);
                            break 'pump;
                        }
                        Some(Err(error)) => {
                            drop(sender.send(Err(authority_error(error))).await);
                            break 'pump;
                        }
                        None => {
                            drop(sender.send(Err(DocsError::AuthorityUnavailable(
                                "authority change stream ended without retraction".into(),
                            ))).await);
                            break 'pump;
                        }
                    },
                    read = reader.next(RevisionReadLimits::default()) => Wake::Frontier(read),
                };
                let caller = match capabilities
                    .resolve::<DocScope>(&context, params.capability)
                    .await
                {
                    Ok(caller) => caller,
                    Err(error) => {
                        drop(sender.send(Err(authority_error(error))).await);
                        break 'pump;
                    }
                };
                let entries = match wake {
                    Wake::MembershipChanged if drawer_readable.is_none() => {
                        let revision = match store.latest_revision().await {
                            Ok(revision) => revision,
                            Err(error) => {
                                drop(sender.send(Err(repository(error))).await);
                                break 'pump;
                            }
                        };
                        let readable = match Self::allowed_docs(
                            access.as_ref(),
                            &context,
                            &caller.authority,
                            &delivered,
                            big_cabinet_core::drawer::DrawerAccess::Read,
                        )
                        .await
                        {
                            Ok(readable) => readable,
                            Err(error) => {
                                drop(sender.send(Err(error)).await);
                                break 'pump;
                            }
                        };
                        let mut restored = BTreeSet::new();
                        for doc_id in &delivered {
                            let allowed = readable.contains(doc_id);
                            if matches!(&params.scope, ObserveScope::Doc { .. }) {
                                if allowed && interest.is_none() {
                                    interest = match repo.interest(doc_id.clone()).await {
                                        Ok(interest) => Some(interest),
                                        Err(error) => {
                                            drop(sender.send(Err(repository(error))).await);
                                            break 'pump;
                                        }
                                    };
                                } else if !allowed {
                                    interest = None;
                                }
                            }
                            if allowed {
                                if lost.contains(doc_id) {
                                    restored.insert(doc_id.clone());
                                }
                            } else {
                                last_present.retain(|(doc, _), _| doc != doc_id);
                                if lost.insert(doc_id.clone())
                                    && let Some(facet_key) = delivered_keys.get(doc_id).cloned()
                                    && sender
                                        .send(Ok(FacetObserved::Entry {
                                            revision,
                                            doc_id: doc_id.clone(),
                                            facet_key,
                                            facet: ObservedFacet::AccessLost,
                                        }))
                                        .await
                                        .is_err()
                                {
                                    break 'pump;
                                }
                            }
                        }
                        let tag = match &params.scope {
                            ObserveScope::Tag { tag } => Some(tag),
                            ObserveScope::Doc { .. } | ObserveScope::Drawer { .. } => None,
                        };
                        match store.query_docs(&restored, tag, None).await {
                            Ok(records) => records
                                .into_iter()
                                .map(|record| FrontierEntry {
                                    revision,
                                    key: record.route,
                                    value: Some(FacetMembership::Present {
                                        doc_heads: record.doc_heads,
                                        facet_heads: record.facet_heads,
                                        clock: record.clock,
                                    }),
                                })
                                .collect(),
                            Err(error) => {
                                drop(sender.send(Err(repository(error))).await);
                                break 'pump;
                            }
                        }
                    }
                    Wake::MembershipChanged => {
                        let ObserveScope::Drawer { drawer_id, tag } = &params.scope else {
                            continue;
                        };
                        let current_readable = drawer_readable
                            .as_mut()
                            .expect("drawer scope initializes readable set");
                        let next_readable = match Self::readable_in_drawer(
                            access.as_ref(),
                            &context,
                            &caller.authority,
                            drawer_id,
                        )
                        .await
                        {
                            Ok(docs) => docs,
                            Err(DocsError::Denied) => BTreeSet::new(),
                            Err(error) => {
                                drop(sender.send(Err(error)).await);
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
                        match store.query_docs(&added_docs, tag.as_ref(), None).await {
                            Ok(records) => records
                                .into_iter()
                                .map(|record| FrontierEntry {
                                    revision,
                                    key: record.route,
                                    value: Some(FacetMembership::Present {
                                        doc_heads: record.doc_heads,
                                        facet_heads: record.facet_heads,
                                        clock: record.clock,
                                    }),
                                })
                                .collect(),
                            Err(error) => {
                                drop(sender.send(Err(repository(error))).await);
                                break 'pump;
                            }
                        }
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
                let entries: Vec<_> = entries
                    .into_iter()
                    .filter(|entry| match &params.scope {
                        ObserveScope::Doc { doc_id } => &entry.key.doc_id == doc_id,
                        ObserveScope::Tag { tag } => entry.key.facet_key.tag() == tag,
                        ObserveScope::Drawer { tag, .. } => tag
                            .as_ref()
                            .is_none_or(|tag| entry.key.facet_key.tag() == tag),
                    })
                    .collect();
                let batch_readable;
                let readable = if let Some(readable) = &drawer_readable {
                    readable
                } else {
                    let candidates = entries
                        .iter()
                        .map(|entry| entry.key.doc_id.clone())
                        .collect();
                    batch_readable = match Self::allowed_docs(
                        access.as_ref(),
                        &context,
                        &caller.authority,
                        &candidates,
                        big_cabinet_core::drawer::DrawerAccess::Read,
                    )
                    .await
                    {
                        Ok(readable) => readable,
                        Err(error) => {
                            drop(sender.send(Err(error)).await);
                            break 'pump;
                        }
                    };
                    &batch_readable
                };
                for entry in entries {
                    let FrontierEntry {
                        revision,
                        key: route,
                        value,
                    } = entry;
                    let allowed = readable.contains(&route.doc_id);
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
