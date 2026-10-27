//! ObserveFacets: doc-scoped observation over the ungated `facet_set` projection,
//! filtered by the same read access as gated reads (ADR 009 §6).

use super::*;
use crate::index::facet_set::FacetMembership;
use crate::index::facet_set::FacetRoute;
use big_cabinet_core::FacetTag;
use big_cabinet_core::authority::CapabilityStatus;
use big_cabinet_core::authority::ObserveCapability;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::keyed_frontier::KeyedFrontierError;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use futures::StreamExt;

impl Handler<ObserveFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<ObserveFacets>,
    ) -> Result<ResponseEnvelope<EventStream<FacetObserved, DocsError>>, DocsError> {
        let params = request.params;
        let authority_changes = self
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
        let changes = self.access.subscribe_changes();
        let drawer_readable = match &params.scope {
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
        let interest = match &params.scope {
            ObserveScope::Doc { doc_id } => Some(
                self.big_repo
                    .interest(doc_id.clone())
                    .await
                    .map_err(repository)?,
            ),
            ObserveScope::Tag { .. } | ObserveScope::Drawer { .. } => None,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let pump = FacetPump {
            delivered: Delivered::new(&params.scope),
            params,
            repo: Arc::clone(&self.big_repo),
            access: Arc::clone(&self.access),
            capabilities: self.capabilities.clone(),
            context: request.context.child(),
            sender,
            drawer_readable,
            interest,
        };
        let tasks = self.tasks.clone();
        let Some(_pump) = self
            .tasks
            .spawn(pump.run(store, tasks, authority_changes, changes))
        else {
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

/// Why a [`FacetPump`] stops.
enum Halt {
    /// The stream ends with this error as its last item.
    Fail(DocsError),
    /// The service is stopping or the consumer dropped the stream: nothing is left to tell.
    Stop,
}

impl From<DocsError> for Halt {
    fn from(error: DocsError) -> Self {
        Self::Fail(error)
    }
}

/// What one `ObserveFacets` stream has handed its consumer, so access changes are reported only
/// about documents the consumer already knows of, and a re-published state is not repeated.
struct Delivered {
    /// Documents this stream has delivered an entry for. `AccessLost` is only meaningful — and
    /// only non-leaking — for those: naming a document the observer never saw would disclose
    /// its existence. A `Doc` scope names its document itself, so it starts delivered.
    docs: BTreeSet<DocId>,
    /// The first facet key delivered per document, which [`Self::access_lost`] reports under.
    first_keys: BTreeMap<DocId, FacetKey>,
    last_present: BTreeMap<(DocId, FacetKey), (Vec<ChangeHash>, FacetVersion)>,
    /// Delivered documents whose loss of access has been reported and not yet restored.
    lost: BTreeSet<DocId>,
}

impl Delivered {
    fn new(scope: &ObserveScope) -> Self {
        let mut docs = BTreeSet::new();
        if let ObserveScope::Doc { doc_id } = scope {
            docs.insert(doc_id.clone());
        }
        Self {
            docs,
            first_keys: BTreeMap::new(),
            last_present: BTreeMap::new(),
            lost: BTreeSet::new(),
        }
    }

    /// Forgets the present facets of a document the caller can no longer read, and reports
    /// whether the stream owes it an `AccessLost`: only for a delivered document, once per loss.
    fn lose(&mut self, doc_id: &DocId) -> bool {
        self.last_present.retain(|(doc, _), _| doc != doc_id);
        self.docs.contains(doc_id) && self.lost.insert(doc_id.clone())
    }

    /// The `AccessLost` an access change, rather than a frontier entry, owes `doc_id`: reported
    /// under the first facet key the stream delivered for it.
    fn access_lost(&mut self, revision: u64, doc_id: DocId) -> Option<FacetObserved> {
        if self.lose(&doc_id)
            && let Some(facet_key) = self.first_keys.get(&doc_id).cloned()
        {
            return Some(FacetObserved::Entry {
                revision,
                doc_id,
                facet_key,
                facet: ObservedFacet::AccessLost,
            });
        }
        None
    }

    /// Records a readable facet's membership and returns what to deliver for it, or `None` when
    /// it repeats the facet's last delivered present state.
    fn admit(&mut self, route: &FacetRoute, membership: FacetMembership) -> Option<ObservedFacet> {
        let facet = match membership {
            FacetMembership::Present {
                doc_heads,
                facet_heads,
                clock,
            } => {
                let version = FacetVersion {
                    heads: facet_heads,
                    clock,
                };
                let pair = (route.doc_id.clone(), route.facet_key.clone());
                let state = (doc_heads.clone(), version.clone());
                if self.last_present.get(&pair) == Some(&state) {
                    return None;
                }
                self.last_present.insert(pair, state);
                ObservedFacet::Present { doc_heads, version }
            }
            FacetMembership::Removed {
                doc_heads,
                facet_heads,
                clock,
            } => {
                self.last_present
                    .remove(&(route.doc_id.clone(), route.facet_key.clone()));
                ObservedFacet::Removed {
                    doc_heads,
                    version: FacetVersion {
                        heads: facet_heads,
                        clock,
                    },
                }
            }
        };
        self.docs.insert(route.doc_id.clone());
        self.first_keys
            .entry(route.doc_id.clone())
            .or_insert_with(|| route.facet_key.clone());
        self.lost.remove(&route.doc_id);
        Some(facet)
    }
}

/// The task behind one `ObserveFacets` stream: it replays and then follows the facet-set
/// frontier, and re-evaluates read access whenever the capability or the access policy changes.
struct FacetPump {
    params: ObserveFacets,
    repo: SharedBigRepo,
    access: Arc<dyn DocAccess>,
    capabilities: DomainCapabilities,
    context: RequestContext,
    sender: tokio::sync::mpsc::Sender<Result<FacetObserved, DocsError>>,
    /// The readable documents of a `Drawer` scope, kept current across access changes; `None`
    /// for the other scopes, which check each batch of entries instead.
    drawer_readable: Option<BTreeSet<DocId>>,
    /// Keeps a `Doc` scope's document synced while the caller can read it.
    interest: Option<big_repo::DocInterest>,
    delivered: Delivered,
}

impl FacetPump {
    async fn run(
        mut self,
        store: Arc<FacetSetStore>,
        tasks: Spawner,
        authority_changes: service_utils_rs::ClientStream<ObserveCapability>,
        changes: Option<tokio::sync::watch::Receiver<u64>>,
    ) {
        if let Err(Halt::Fail(error)) = self.pump(&store, &tasks, authority_changes, changes).await
        {
            drop(self.sender.send(Err(error)).await);
        }
    }

    async fn pump(
        &mut self,
        store: &FacetSetStore,
        tasks: &Spawner,
        mut authority_changes: service_utils_rs::ClientStream<ObserveCapability>,
        mut changes: Option<tokio::sync::watch::Receiver<u64>>,
    ) -> Result<(), Halt> {
        enum Wake {
            Frontier(
                Result<
                    RevisionRead<u64, FrontierEntry<FacetRoute, FacetMembership>>,
                    KeyedFrontierError,
                >,
            ),
            MembershipChanged,
        }
        // Tag scope (and tag-filtered Drawer scope) opens only its own frontier namespace; Doc
        // and unfiltered Drawer scopes open everything and filter by document below.
        let mut reader = match &self.params.scope {
            ObserveScope::Doc { .. } | ObserveScope::Drawer { tag: None, .. } => {
                store.open_all(self.params.after).await
            }
            ObserveScope::Tag { tag } | ObserveScope::Drawer { tag: Some(tag), .. } => {
                store.open_tags([(tag.clone(), self.params.after)]).await
            }
        }
        .map_err(repository)?;
        let mut replay_complete = false;
        loop {
            let wake = tokio::select! {
                _ = tasks.cancelled() => return Err(Halt::Stop),
                _ = self.sender.closed() => return Err(Halt::Stop),
                Ok(()) = async {
                    match changes.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                }, if replay_complete => Wake::MembershipChanged,
                change = authority_changes.next() => match change {
                    Some(Ok(CapabilityStatus::Live)) => Wake::MembershipChanged,
                    Some(Ok(CapabilityStatus::Retracted)) => return Err(DocsError::Denied.into()),
                    Some(Err(error)) => return Err(authority_error(error).into()),
                    None => return Err(DocsError::AuthorityUnavailable(
                        "authority change stream ended without retraction".into(),
                    ).into()),
                },
                read = reader.next(RevisionReadLimits::default()) => Wake::Frontier(read),
            };
            let caller = self
                .capabilities
                .resolve::<DocScope>(&self.context, self.params.capability)
                .await
                .map_err(authority_error)?;
            let entries = match wake {
                Wake::MembershipChanged if self.drawer_readable.is_none() => {
                    self.recheck_delivered(store, &caller.authority).await?
                }
                Wake::MembershipChanged => self.recheck_drawer(store, &caller.authority).await?,
                Wake::Frontier(Ok(RevisionRead::Entries { entries, .. })) => entries,
                Wake::Frontier(Ok(RevisionRead::ReplayComplete { through })) => {
                    replay_complete = true;
                    self.emit(FacetObserved::ReplayComplete { through }).await?;
                    continue;
                }
                Wake::Frontier(Err(error)) => return Err(repository(error).into()),
            };
            self.deliver(entries, &caller.authority).await?;
        }
    }

    async fn emit(&self, event: FacetObserved) -> Result<(), Halt> {
        self.sender.send(Ok(event)).await.map_err(|_| Halt::Stop)
    }

    /// Re-checks read access to every delivered document of a `Doc` or `Tag` scope after an
    /// access change. Lost documents get their `AccessLost`; the current facets of documents
    /// readable again are returned for delivery.
    async fn recheck_delivered(
        &mut self,
        store: &FacetSetStore,
        authority: &DomainAuthority<DocScope>,
    ) -> Result<Vec<FrontierEntry<FacetRoute, FacetMembership>>, Halt> {
        let revision = store.latest_revision().await.map_err(repository)?;
        let readable = DocsService::allowed_docs(
            self.access.as_ref(),
            &self.context,
            authority,
            &self.delivered.docs,
            big_cabinet_core::drawer::DrawerAccess::Read,
        )
        .await?;
        // A `Doc` scope has delivered only its own document.
        if let ObserveScope::Doc { doc_id } = &self.params.scope {
            if !readable.contains(doc_id) {
                self.interest = None;
            } else if self.interest.is_none() {
                self.interest = Some(
                    self.repo
                        .interest(doc_id.clone())
                        .await
                        .map_err(repository)?,
                );
            }
        }
        let restored: BTreeSet<DocId> = self
            .delivered
            .lost
            .intersection(&readable)
            .cloned()
            .collect();
        let revoked: Vec<DocId> = self.delivered.docs.difference(&readable).cloned().collect();
        for doc_id in revoked {
            if let Some(event) = self.delivered.access_lost(revision, doc_id) {
                self.emit(event).await?;
            }
        }
        let tag = match &self.params.scope {
            ObserveScope::Tag { tag } => Some(tag),
            ObserveScope::Doc { .. } | ObserveScope::Drawer { .. } => None,
        };
        Self::present_entries(store, &restored, tag, revision).await
    }

    /// Re-reads a `Drawer` scope's readable documents after an access change. Documents that
    /// left it get their `AccessLost`; the current facets of documents that joined are returned
    /// for delivery.
    async fn recheck_drawer(
        &mut self,
        store: &FacetSetStore,
        authority: &DomainAuthority<DocScope>,
    ) -> Result<Vec<FrontierEntry<FacetRoute, FacetMembership>>, Halt> {
        let ObserveScope::Drawer { drawer_id, tag } = &self.params.scope else {
            return Ok(Vec::new());
        };
        let next_readable = match DocsService::readable_in_drawer(
            self.access.as_ref(),
            &self.context,
            authority,
            drawer_id,
        )
        .await
        {
            Ok(docs) => docs,
            Err(DocsError::Denied) => BTreeSet::new(),
            Err(error) => return Err(error.into()),
        };
        let revision = store.latest_revision().await.map_err(repository)?;
        let current_readable = self
            .drawer_readable
            .as_mut()
            .expect("drawer scope initializes readable set");
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
            if let Some(event) = self.delivered.access_lost(revision, doc_id) {
                self.emit(event).await?;
            }
        }
        Self::present_entries(store, &added_docs, tag.as_ref(), revision).await
    }

    /// The current facets of `docs`, as frontier entries stamped with `revision`.
    async fn present_entries(
        store: &FacetSetStore,
        docs: &BTreeSet<DocId>,
        tag: Option<&FacetTag>,
        revision: u64,
    ) -> Result<Vec<FrontierEntry<FacetRoute, FacetMembership>>, Halt> {
        let records = store
            .query_docs(docs, tag, None)
            .await
            .map_err(repository)?;
        Ok(records
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
            .collect())
    }

    /// Delivers the entries in scope that the caller can read. An unreadable entry of a
    /// delivered document raises one `AccessLost` per loss; one of a never-delivered document
    /// is dropped silently.
    async fn deliver(
        &mut self,
        entries: Vec<FrontierEntry<FacetRoute, FacetMembership>>,
        authority: &DomainAuthority<DocScope>,
    ) -> Result<(), Halt> {
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|entry| match &self.params.scope {
                ObserveScope::Doc { doc_id } => &entry.key.doc_id == doc_id,
                ObserveScope::Tag { tag } => entry.key.facet_key.tag() == tag,
                ObserveScope::Drawer { tag, .. } => tag
                    .as_ref()
                    .is_none_or(|tag| entry.key.facet_key.tag() == tag),
            })
            .collect();
        let batch_readable;
        let readable = if let Some(readable) = &self.drawer_readable {
            readable
        } else {
            let candidates = entries
                .iter()
                .map(|entry| entry.key.doc_id.clone())
                .collect();
            batch_readable = DocsService::allowed_docs(
                self.access.as_ref(),
                &self.context,
                authority,
                &candidates,
                big_cabinet_core::drawer::DrawerAccess::Read,
            )
            .await?;
            &batch_readable
        };
        for entry in entries {
            let FrontierEntry {
                revision,
                key: route,
                value,
            } = entry;
            if !readable.contains(&route.doc_id) {
                if self.delivered.lose(&route.doc_id) {
                    self.emit(FacetObserved::Entry {
                        revision,
                        doc_id: route.doc_id,
                        facet_key: route.facet_key,
                        facet: ObservedFacet::AccessLost,
                    })
                    .await?;
                }
                continue;
            }
            // The frontier only ever applies membership values; a tombstone row here means the
            // projection broke its own contract, so fail the stream rather than forge a clock-0
            // removal.
            let Some(membership) = value else {
                return Err(
                    repository("facet-set frontier published an entry without membership").into(),
                );
            };
            let Some(facet) = self.delivered.admit(&route, membership) else {
                continue;
            };
            self.emit(FacetObserved::Entry {
                revision,
                doc_id: route.doc_id,
                facet_key: route.facet_key,
                facet,
            })
            .await?;
        }
        Ok(())
    }
}
