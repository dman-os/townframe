use super::*;
use big_cabinet_core::authority::CapabilityStatus;
use big_cabinet_core::authority::ObserveCapability;
use big_repo::BigRepoDocIdFilter;
use big_repo::BigRepoLocalFilter;
use big_repo::MaterializationState;
use futures::StreamExt;

async fn authorize_availability(
    capabilities: &DomainCapabilities,
    access: &dyn DocAccess,
    context: &RequestContext,
    capability: CapId,
    doc_id: &DocId,
) -> Result<(), DocsError> {
    context.validate()?;
    let caller = capabilities
        .resolve::<DocScope>(context, capability)
        .await
        .map_err(authority_error)?;
    if !matches!(&caller.authority, DomainAuthority::Scoped(_))
        || !DocsService::allowed(
            access,
            context,
            &caller.authority,
            AccessRequest::Read(doc_id),
        )
        .await?
    {
        return Err(DocsError::Denied);
    }
    Ok(())
}

fn local_usability(state: Option<MaterializationState>) -> LocalDocUsability {
    match state {
        None => LocalDocUsability::NotObserved,
        Some(MaterializationState::Missing) => LocalDocUsability::NoLocalContent,
        Some(MaterializationState::Pending) => LocalDocUsability::PendingMaterialization,
        Some(MaterializationState::PartiallyMaterialized | MaterializationState::Materialized) => {
            LocalDocUsability::Usable
        }
    }
}

impl DocsService {
    async fn availability(
        &self,
        context: &RequestContext,
        params: GetDocAvailability,
    ) -> Result<DocAvailability, DocsError> {
        authorize_availability(
            &self.capabilities,
            self.access.as_ref(),
            context,
            params.capability,
            &params.doc_id,
        )
        .await?;
        let state = self
            .big_repo
            .inspect_doc_head_state(params.doc_id.clone())
            .await
            .map_err(repository)?;
        authorize_availability(
            &self.capabilities,
            self.access.as_ref(),
            context,
            params.capability,
            &params.doc_id,
        )
        .await?;
        Ok(DocAvailability {
            sync_demand: self
                .big_repo
                .sync_demand()
                .documents()
                .contains(&params.doc_id),
            doc_id: params.doc_id,
            local_usability: local_usability(state.map(|state| state.state)),
        })
    }

    async fn observe_availability(
        &self,
        context: &RequestContext,
        params: ObserveDocAvailability,
    ) -> Result<EventStream<DocAvailability, DocsError>, DocsError> {
        context.validate()?;
        let authority_changes = self
            .authority
            .subscribe(
                context.child(),
                ObserveCapability {
                    capability: params.capability,
                },
            )
            .await
            .map_err(authority_error)?;
        let policy_changes = self.access.subscribe_changes();
        authorize_availability(
            &self.capabilities,
            self.access.as_ref(),
            context,
            params.capability,
            &params.doc_id,
        )
        .await?;
        let observer =
            |context: RequestContext, params: ObserveDocAvailability| AvailabilityObserver {
                capabilities: self.capabilities.clone(),
                access: Arc::clone(&self.access),
                context,
                params,
                tasks: self.tasks.clone(),
            };
        let delivery = observer(context.child(), params.clone());
        let pump = observer(context.child(), params);
        let repo = Arc::clone(&self.big_repo);
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (completion, completed) = tokio::sync::oneshot::channel();
        self.tasks
            .spawn(async move {
                let result = tokio::select! {
                    biased;
                    _ = pump.tasks.cancelled() => Ok(()),
                    _ = pump.context.cancellation.cancelled() => Ok(()),
                    _ = pump.context.expired() => Ok(()),
                    _ = sender.closed() => Ok(()),
                    result = pump.watch_access(authority_changes, policy_changes) => result,
                    result = pump.pump(&repo, &sender) => result,
                };
                let result = match result {
                    Err(error) if ends_quietly(&error) => Ok(()),
                    result => result,
                };
                let _consumer_closed = completion.send(result);
            })
            .ok_or(ContextError::Cancelled)?;
        let stream =
            futures::stream::unfold(Some((receiver, completed, delivery)), |state| async move {
                let (mut receiver, mut completed, delivery) = state?;
                match delivery.next(&mut receiver, &mut completed).await {
                    Ok(Some(snapshot)) => {
                        Some((Ok(snapshot), Some((receiver, completed, delivery))))
                    }
                    Ok(None) => None,
                    Err(error) if ends_quietly(&error) => None,
                    Err(error) => Some((Err(error), None)),
                }
            });
        Ok(Box::pin(stream))
    }
}

/// A retraction, a revoked read, or the subscriber going away ends an availability
/// subscription without an error item.
fn ends_quietly(error: &DocsError) -> bool {
    matches!(
        error,
        DocsError::Denied
            | DocsError::Context(ContextError::Cancelled | ContextError::DeadlineExceeded)
    )
}

/// One side of an `ObserveDocAvailability` subscription: the pump that produces snapshots or the
/// stream that delivers them. Each re-checks read access under its own child context, so a
/// retraction or policy change stops both the pump before it reads and the stream before it
/// hands out a snapshot the pump queued while access still held.
struct AvailabilityObserver {
    capabilities: DomainCapabilities,
    access: Arc<dyn DocAccess>,
    context: RequestContext,
    params: ObserveDocAvailability,
    tasks: Spawner,
}

impl AvailabilityObserver {
    async fn authorize(&self) -> Result<(), DocsError> {
        authorize_availability(
            &self.capabilities,
            self.access.as_ref(),
            &self.context,
            self.params.capability,
            &self.params.doc_id,
        )
        .await
    }

    /// Re-authorizes after every capability or access-policy change. Returns `Ok` once the
    /// capability is retracted, and the first authorization failure otherwise.
    async fn watch_access(
        &self,
        mut authority_changes: service_utils_rs::ClientStream<ObserveCapability>,
        mut policy_changes: Option<tokio::sync::watch::Receiver<u64>>,
    ) -> Result<(), DocsError> {
        loop {
            tokio::select! {
                biased;
                change = authority_changes.next() => match change {
                    Some(Ok(CapabilityStatus::Live)) => {}
                    Some(Ok(CapabilityStatus::Retracted)) => return Ok(()),
                    Some(Err(error)) => return Err(authority_error(error)),
                    None => return Err(DocsError::AuthorityUnavailable(
                        "authority change stream ended without retraction".into(),
                    )),
                },
                change = async {
                    match policy_changes.as_mut() {
                        Some(changes) => changes.changed().await,
                        None => std::future::pending().await,
                    }
                } => change.map_err(|_| DocsError::Access(
                    "access policy change stream ended".into(),
                ))?,
            }
            self.authorize().await?;
        }
    }

    /// Queues a snapshot whenever local usability or sync demand changes. The channel holds one
    /// snapshot, so changes arriving while the consumer is busy coalesce into the next one, and
    /// an unchanged snapshot is never sent twice. Local state is re-read only after a local
    /// change or the first readiness, never for a sync-demand change alone.
    async fn pump(
        &self,
        repo: &SharedBigRepo,
        sender: &tokio::sync::mpsc::Sender<DocAvailability>,
    ) -> Result<(), DocsError> {
        let doc_id = &self.params.doc_id;
        let (_registration, mut local_changes) = repo
            .subscribe_local_listener(BigRepoLocalFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            })
            .await
            .map_err(repository)?;
        self.authorize().await?;
        let interest = repo.interest(doc_id.clone()).await.map_err(repository)?;
        let mut demand = repo.sync_demand();
        let ready = interest.wait_until_ready();
        tokio::pin!(ready);
        let mut readiness_observed = false;
        let mut previous: Option<DocAvailability> = None;
        let mut pending = true;
        let mut local_dirty = true;
        loop {
            tokio::select! {
                permit = sender.reserve(), if pending => {
                    let Ok(permit) = permit else {
                        return Ok(());
                    };
                    self.authorize().await?;
                    let usability = if local_dirty {
                        let state = repo
                            .inspect_doc_head_state(doc_id.clone())
                            .await
                            .map_err(repository)?;
                        local_usability(state.map(|state| state.state))
                    } else {
                        previous.as_ref().expect("initial snapshot reads local state").local_usability
                    };
                    let snapshot = DocAvailability {
                        doc_id: doc_id.clone(),
                        local_usability: usability,
                        sync_demand: demand.documents().contains(doc_id),
                    };
                    if previous.as_ref() != Some(&snapshot) {
                        previous = Some(snapshot.clone());
                        permit.send(snapshot);
                    }
                    pending = false;
                    local_dirty = false;
                }
                change = local_changes.recv() => {
                    change.ok_or_else(|| repository("local document change stream ended"))?;
                    pending = true;
                    local_dirty = true;
                }
                result = &mut ready, if !readiness_observed => {
                    result.map_err(repository)?;
                    readiness_observed = true;
                    if previous.as_ref().is_none_or(|snapshot| snapshot.local_usability != LocalDocUsability::Usable) {
                        pending = true;
                        local_dirty = true;
                    }
                }
                documents = demand.changed() => {
                    let sync_demand = documents.map_err(repository)?.contains(doc_id);
                    if previous.as_ref().is_none_or(|snapshot| snapshot.sync_demand != sync_demand) {
                        pending = true;
                    }
                }
            }
        }
    }

    /// The next snapshot the pump queued, re-authorized before it is handed out. `None` once the
    /// subscription is cancelled or the pump finished cleanly; the pump's error otherwise.
    async fn next(
        &self,
        receiver: &mut tokio::sync::mpsc::Receiver<DocAvailability>,
        completed: &mut tokio::sync::oneshot::Receiver<Result<(), DocsError>>,
    ) -> Result<Option<DocAvailability>, DocsError> {
        tokio::select! {
            biased;
            _ = self.tasks.cancelled() => Ok(None),
            _ = self.context.cancellation.cancelled() => Ok(None),
            _ = self.context.expired() => Ok(None),
            result = &mut *completed => result
                .expect("availability pump reports completion")
                .map(|()| None),
            result = async {
                let snapshot = receiver.recv().await
                    .expect("availability pump completes before closing snapshots");
                self.authorize().await?;
                Ok::<_, DocsError>(snapshot)
            } => result.map(Some),
        }
    }
}

impl Handler<GetDocAvailability> for DocsService {
    async fn call(
        &self,
        request: Envelope<GetDocAvailability>,
    ) -> Result<ResponseEnvelope<DocAvailability>, DocsError> {
        tokio::select! {
            biased;
            _ = self.tasks.cancelled() => Err(ContextError::Cancelled.into()),
            _ = request.context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = request.context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = self.availability(&request.context, request.params) => result.map(Into::into),
        }
    }
}

impl Handler<ObserveDocAvailability> for DocsService {
    async fn call(
        &self,
        request: Envelope<ObserveDocAvailability>,
    ) -> Result<ResponseEnvelope<EventStream<DocAvailability, DocsError>>, DocsError> {
        tokio::select! {
            biased;
            _ = self.tasks.cancelled() => Err(ContextError::Cancelled.into()),
            _ = request.context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = request.context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = self.observe_availability(&request.context, request.params) => result.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests;
