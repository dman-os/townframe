use automerge::ChangeHash;
use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumStore;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocAvailability;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::LocalDocUsability;
use big_cabinet_core::doc::ObserveDocAvailability;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::doc::StoreSnapshot;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use futures::FutureExt;
use futures::StreamExt;
use service_client_core_rs::ClientStream;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::ContextError;
use service_utils_rs::EventStream;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::future::Future;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

/// How long the server may hold an empty poll; an expiry is an ordinary empty page.
const POLL_HOLD_MS: u32 = 5_000;

type EditFn<S> = Box<dyn FnOnce(&mut S) -> Result<(), StoreError> + Send + 'static>;
type SnapshotReply<S> = oneshot::Sender<(u64, Result<StoreSnapshot<S>, StoreError>)>;
type WatchReply<S> =
    oneshot::Sender<Result<(u64, StoreSnapshot<S>, watch::Receiver<u64>), StoreError>>;

enum StoreRequest<S> {
    Snapshot {
        reply: SnapshotReply<S>,
    },
    Watch {
        reply: WatchReply<S>,
    },
    Mutate {
        context: Box<RequestContext>,
        command: Option<CommandId>,
        edit: EditFn<S>,
        reply: oneshot::Sender<Result<Option<WriteReceipt>, StoreError>>,
    },
}

#[derive(Clone)]
pub struct DatumStoreHandle<S> {
    docs: Client<DocsProtocol>,
    capability: CapId,
    doc_id: DocId,
    sender: mpsc::Sender<StoreRequest<S>>,
}

impl<S> std::fmt::Debug for DatumStoreHandle<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatumStoreHandle")
            .field("capability", &self.capability)
            .field("doc_id", &self.doc_id)
            .finish_non_exhaustive()
    }
}

impl<S: DatumStore> DatumStoreHandle<S> {
    pub async fn load(
        context: &RequestContext,
        docs: Client<DocsProtocol>,
        capability: CapId,
        doc_id: DocId,
    ) -> Result<(Self, StopToken), StoreError> {
        context.validate()?;
        let stream_context = RequestContext::root();
        let observe_availability = with_context(
            context,
            docs.subscribe(
                stream_context.child(),
                ObserveDocAvailability {
                    capability,
                    doc_id: doc_id.clone(),
                },
            ),
        )
        .await?
        .map_err(map_client_error)?;

        // The cursor comes from the replay itself, taken before the read below: a write that
        // lands between the two is past `after`, so the first live poll redelivers it and
        // `should_apply_observed` folds it into the already-read state.
        let after = replay_cursor(context, &docs, capability, &doc_id).await?;

        let raw = read_owned::<S>(context, &docs, capability, &doc_id, At::Current, None).await?;
        let seen_head_hashes: HashSet<ChangeHash> = raw.heads.iter().copied().collect();
        let current = StoreSnapshot {
            heads: raw.heads.clone(),
            value: S::hydrate(&raw),
        };
        let (version_tx, _version_rx) = watch::channel(0u64);
        let (page_sender, pages) = mpsc::channel(1);
        let owner = StoreOwner {
            docs: docs.clone(),
            capability,
            doc_id: doc_id.clone(),
            raw,
            current,
            seen_head_hashes,
            needs_full_refresh: false,
            terminal_error: None,
            version: 0,
            version_tx,
            pages,
            observe_availability,
            availability_ended: false,
        };

        let (stop_token, tasks) = StopToken::new();
        let (sender, receiver) = mpsc::channel(32);
        if tasks
            .spawn(poll_pages(
                docs.clone(),
                capability,
                doc_id.clone(),
                after,
                stream_context.child(),
                tasks.clone(),
                page_sender,
            ))
            .is_none()
        {
            return Err(StoreError::Docs(DocsError::Context(
                ContextError::Cancelled,
            )));
        }
        let loop_tasks = tasks.clone();
        if tasks
            .spawn(async move {
                owner.run(loop_tasks, stream_context, receiver).await;
            })
            .is_none()
        {
            return Err(StoreError::Docs(DocsError::Context(
                ContextError::Cancelled,
            )));
        }

        Ok((
            Self {
                docs,
                capability,
                doc_id,
                sender,
            },
            stop_token,
        ))
    }

    pub async fn snapshot(&self) -> Result<StoreSnapshot<S>, StoreError> {
        let (_, snapshot) = self.fetch_snapshot().await?;
        snapshot
    }

    async fn fetch_snapshot(
        &self,
    ) -> Result<(u64, Result<StoreSnapshot<S>, StoreError>), StoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(StoreRequest::Snapshot { reply: reply_tx })
            .await
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))
    }

    pub async fn at(&self, heads: Vec<ChangeHash>) -> Result<StoreSnapshot<S>, StoreError> {
        let read = read_owned::<S>(
            &RequestContext::root(),
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Heads(heads),
            None,
        )
        .await
        .map_err(map_post_load_read_error)?;
        Ok(StoreSnapshot {
            heads: read.heads.clone(),
            value: S::hydrate(&read),
        })
    }

    pub async fn mutate<O>(
        &self,
        context: &RequestContext,
        command: Option<CommandId>,
        edit: impl FnOnce(&mut S) -> Result<O, StoreError> + Send + 'static,
    ) -> Result<(O, Option<WriteReceipt>), StoreError>
    where
        O: Send + 'static,
    {
        context.validate()?;
        let (output_tx, mut output_rx) = oneshot::channel::<O>();
        let boxed_edit: EditFn<S> = Box::new(move |value: &mut S| {
            let output = edit(value)?;
            drop(output_tx.send(output));
            Ok(())
        });
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = StoreRequest::Mutate {
            context: Box::new(context.clone()),
            command,
            edit: boxed_edit,
            reply: reply_tx,
        };
        with_context(context, self.sender.send(request))
            .await?
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))?;
        let receipt = with_context(context, reply_rx)
            .await?
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))??;
        let output = output_rx
            .try_recv()
            .expect("successful edit closure produced output");
        Ok((output, receipt))
    }

    pub async fn watch(&self) -> Result<EventStream<StoreSnapshot<S>, StoreError>, StoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(StoreRequest::Watch { reply: reply_tx })
            .await
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))?;
        let (initial_version, initial, rx) = reply_rx
            .await
            .map_err(|_| StoreError::DocsUnavailable("datum store task has stopped".into()))??;
        let handle = self.clone();
        let stream = futures::stream::unfold(
            (handle, rx, initial_version, Some(initial), false),
            |(handle, mut rx, mut seen_version, pending, terminated)| async move {
                if terminated {
                    return None;
                }
                if let Some(first) = pending {
                    return Some((Ok(first), (handle, rx, seen_version, None, false)));
                }
                loop {
                    rx.changed().await.ok()?;
                    let (version, item) = handle.fetch_snapshot().await.ok()?;
                    if version == seen_version && item.is_ok() {
                        continue;
                    }
                    seen_version = version;
                    let is_err = item.is_err();
                    return Some((item, (handle, rx, seen_version, None, is_err)));
                }
            },
        );
        Ok(Box::pin(stream))
    }
}

struct StoreOwner<S> {
    docs: Client<DocsProtocol>,
    capability: CapId,
    doc_id: DocId,
    raw: DatumsRead,
    current: StoreSnapshot<S>,
    seen_head_hashes: HashSet<ChangeHash>,
    needs_full_refresh: bool,
    terminal_error: Option<StoreError>,
    version: u64,
    version_tx: watch::Sender<u64>,
    pages: mpsc::Receiver<Result<DatumPage, StoreError>>,
    observe_availability: ClientStream<ObserveDocAvailability>,
    availability_ended: bool,
}

impl<S: DatumStore> StoreOwner<S> {
    async fn run(
        mut self,
        tasks: Spawner,
        stream_context: RequestContext,
        mut receiver: mpsc::Receiver<StoreRequest<S>>,
    ) {
        let mut requests_open = true;
        loop {
            if self.terminal_error.is_some() {
                tokio::select! {
                    biased;
                    _ = tasks.cancelled() => break,
                    request = receiver.recv(), if requests_open => match request {
                        Some(request) => self.handle_request(request).await,
                        None => break,
                    },
                    else => break,
                }
                continue;
            }
            tokio::select! {
                biased;
                _ = tasks.cancelled() => break,
                request = receiver.recv(), if requests_open => match request {
                    Some(request) => self.handle_request(request).await,
                    None => requests_open = false,
                },
                page = self.pages.recv() => {
                    self.handle_page(page).await;
                }
                availability = self.observe_availability.next(), if !self.availability_ended => {
                    self.handle_availability_observed(availability).await;
                }
            }
        }
        stream_context.cancellation.cancel();
    }

    async fn handle_request(&mut self, request: StoreRequest<S>) {
        if self.terminal_error.is_none()
            && let Err(error) = self.drain_ready_observations(&RequestContext::root()).await
        {
            self.fail(error);
        }
        match request {
            StoreRequest::Snapshot { reply } => {
                let snapshot = match &self.terminal_error {
                    Some(error) => Err(error.clone()),
                    None => Ok(self.current.clone()),
                };
                drop(reply.send((self.version, snapshot)));
            }
            StoreRequest::Watch { reply } => {
                let response = match &self.terminal_error {
                    Some(error) => Err(error.clone()),
                    None => {
                        let mut rx = self.version_tx.subscribe();
                        let version = *rx.borrow_and_update();
                        Ok((version, self.current.clone(), rx))
                    }
                };
                drop(reply.send(response));
            }
            StoreRequest::Mutate {
                context,
                command,
                edit,
                reply,
            } => {
                let result = self.handle_mutate(&context, command, edit).await;
                drop(reply.send(result));
            }
        }
    }

    async fn handle_mutate(
        &mut self,
        context: &RequestContext,
        command: Option<CommandId>,
        edit: EditFn<S>,
    ) -> Result<Option<WriteReceipt>, StoreError> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        context.validate()?;
        if self.needs_full_refresh {
            self.refresh_full(context).await?;
        }
        let mut staged = self.current.value.clone();
        edit(&mut staged)?;
        let patch = S::diff(&self.current.value, &staged)?;
        for key in patch.set.keys().chain(patch.remove.iter()) {
            if !S::owns(key) {
                return Err(StoreError::InvalidEdit(format!(
                    "datum store does not own key {key}"
                )));
            }
        }
        if patch.is_empty() {
            return Ok(None);
        }

        let patched_keys: BTreeSet<DatumKey> = patch
            .set
            .keys()
            .chain(patch.remove.iter())
            .cloned()
            .collect();
        let write_result = with_context(
            context,
            self.docs.call(
                context.child(),
                WriteDatums {
                    capability: self.capability,
                    command: command.clone(),
                    doc_id: self.doc_id.clone(),
                    mode: WriteMode::AtHeads {
                        heads: self.raw.heads.clone(),
                    },
                    set: patch.set,
                    remove: patch.remove,
                },
            ),
        )
        .await;

        let receipt = match write_result {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(ClientError::Transport {
                message,
                outcome_unknown: true,
            })) => {
                self.needs_full_refresh = true;
                return Err(StoreError::DocsUnavailable(message));
            }
            Ok(Err(ClientError::Operation(DocsError::Context(error))))
            | Err(StoreError::Docs(DocsError::Context(error))) => {
                self.needs_full_refresh = true;
                return Err(StoreError::Docs(DocsError::Context(error)));
            }
            Ok(Err(ClientError::Operation(DocsError::Denied))) => {
                return Err(self.classify_write_denied().await);
            }
            Ok(Err(error)) => return Err(map_client_error(error)),
            Err(error) => return Err(error),
        };

        let post_write = if receipt.heads.len() == 1
            && command.is_none()
            && !self.needs_full_refresh
        {
            let mut keys_to_read = patched_keys;
            self.collect_non_accepted_keys(&mut keys_to_read);
            self.refresh_keys_at_heads(&RequestContext::root(), receipt.heads.clone(), keys_to_read)
                .await
        } else {
            self.refresh_at_heads(&RequestContext::root(), receipt.heads.clone())
                .await
        };
        if let Err(error) = post_write {
            self.needs_full_refresh = true;
            if matches!(
                error,
                StoreError::AccessLost | StoreError::DocsUnavailable(_)
            ) {
                self.fail(error.clone());
            }
            return Err(error);
        }
        Ok(Some(receipt))
    }

    async fn classify_write_denied(&mut self) -> StoreError {
        let probe = read_owned::<S>(
            &RequestContext::root(),
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Current,
            Some(BTreeSet::new()),
        )
        .await;
        if matches!(probe, Err(StoreError::Docs(DocsError::Denied))) {
            self.fail(StoreError::AccessLost);
            StoreError::AccessLost
        } else {
            StoreError::Docs(DocsError::Denied)
        }
    }

    async fn drain_ready_observations(
        &mut self,
        context: &RequestContext,
    ) -> Result<(), StoreError> {
        let mut batch = Vec::new();
        // A disconnected channel only follows a delivered error or a shutdown, so there is
        // nothing further to drain from it.
        while let Ok(page) = self.pages.try_recv() {
            batch.extend(self.page_batch(page?));
        }
        if !batch.is_empty() {
            self.apply_observed_batch(context, batch).await?;
        }
        while let Some(item) = self.observe_availability.next().now_or_never().flatten() {
            if let Err(error) = item {
                return Err(map_stream_error(error));
            }
        }
        Ok(())
    }

    async fn handle_page(&mut self, page: Option<Result<DatumPage, StoreError>>) {
        let page = match page {
            Some(Ok(page)) => page,
            Some(Err(error)) => {
                self.fail(error);
                return;
            }
            None => {
                self.fail(StoreError::DocsUnavailable(
                    "datum poll task ended unexpectedly".into(),
                ));
                return;
            }
        };
        let batch = self.page_batch(page);
        if !batch.is_empty()
            && let Err(error) = self
                .apply_observed_batch(&RequestContext::root(), batch)
                .await
        {
            self.fail(error);
        }
    }

    /// Flattens a page into observed datums. A move-in restates a document's selected rows,
    /// which `should_apply_observed` dedups against what is already read.
    fn page_batch(&self, page: DatumPage) -> Vec<(DatumKey, ObservedDatum)> {
        let mut batch = Vec::new();
        for event in page.events {
            match event {
                DatumEvent::Datum {
                    datum_key, datum, ..
                } => batch.push((datum_key, datum)),
                DatumEvent::Leave { doc_id, .. } => unreachable!(
                    "poll of the single document {} sent leave for {doc_id}: an `IsDoc` poll \
                     matches its own document and drops every other document's entries",
                    self.doc_id
                ),
            }
        }
        batch
    }

    async fn handle_availability_observed(
        &mut self,
        item: Option<Result<DocAvailability, ClientError<DocsError>>>,
    ) {
        match item {
            Some(Ok(availability)) => {
                if availability.local_usability == LocalDocUsability::Usable
                    && (self.has_non_accepted_datums() || self.raw.heads.is_empty())
                    && let Err(error) = self.refresh_full(&RequestContext::root()).await
                {
                    self.fail(error);
                }
            }
            Some(Err(error)) => {
                self.availability_ended = true;
                self.fail(map_stream_error(error));
            }
            None => {
                self.availability_ended = true;
                if let Err(error) = self.probe_access().await {
                    self.fail(error);
                }
            }
        }
    }

    async fn probe_access(&mut self) -> Result<(), StoreError> {
        read_owned::<S>(
            &RequestContext::root(),
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Current,
            Some(BTreeSet::new()),
        )
        .await
        .map(|_| ())
        .map_err(map_post_load_read_error)
    }

    /// Converges to the document's current state when a page carries anything not already
    /// read. One current read subsumes every event of the page, which is why the page's own
    /// per-datum heads are only a freshness test and never a read position: an `enter` lists
    /// each datum at the heads that last changed *it*, and reading at those would roll the
    /// state back behind a newer datum.
    async fn apply_observed_batch(
        &mut self,
        context: &RequestContext,
        batch: Vec<(DatumKey, ObservedDatum)>,
    ) -> Result<(), StoreError> {
        let fresh = batch.iter().any(|(key, datum)| match datum {
            ObservedDatum::Present { doc_heads, .. } => {
                self.should_apply_observed(key, Some(doc_heads))
            }
            ObservedDatum::Removed { doc_heads, .. } => {
                self.should_apply_observed(key, doc_heads.as_deref())
            }
        });
        if fresh {
            self.refresh_full(context).await?;
        }
        Ok(())
    }

    /// Whether an observed datum says something the state has not read: heads it has never
    /// seen, or a removal of a key it still holds.
    fn should_apply_observed(
        &self,
        datum_key: &DatumKey,
        doc_heads: Option<&[ChangeHash]>,
    ) -> bool {
        match doc_heads {
            Some(heads) => heads
                .iter()
                .any(|hash| !self.seen_head_hashes.contains(hash)),
            None => S::owns(datum_key) && self.raw.datums.contains_key(datum_key),
        }
    }

    fn record_local_heads(&mut self, heads: &[ChangeHash]) {
        self.seen_head_hashes.extend(heads.iter().copied());
    }

    fn has_non_accepted_datums(&self) -> bool {
        self.raw
            .datums
            .values()
            .any(|read| !matches!(read, DatumRead::Accepted { .. }))
    }

    fn collect_non_accepted_keys(&self, keys: &mut BTreeSet<DatumKey>) {
        for (key, read) in &self.raw.datums {
            if !matches!(read, DatumRead::Accepted { .. }) {
                keys.insert(key.clone());
            }
        }
    }

    async fn refresh_keys_at_heads(
        &mut self,
        context: &RequestContext,
        heads: Vec<ChangeHash>,
        keys: BTreeSet<DatumKey>,
    ) -> Result<(), StoreError> {
        let partial = read_owned::<S>(
            context,
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Heads(heads),
            Some(keys),
        )
        .await
        .map_err(map_post_load_read_error)?;
        self.record_local_heads(&partial.heads);
        self.raw.heads = partial.heads;
        for (key, read) in partial.datums {
            if matches!(read, DatumRead::Absent { .. }) {
                self.raw.datums.remove(&key);
            } else {
                self.raw.datums.insert(key, read);
            }
        }
        self.needs_full_refresh = false;
        self.current = StoreSnapshot {
            heads: self.raw.heads.clone(),
            value: S::hydrate(&self.raw),
        };
        self.publish_current();
        Ok(())
    }

    async fn refresh_at_heads(
        &mut self,
        context: &RequestContext,
        heads: Vec<ChangeHash>,
    ) -> Result<(), StoreError> {
        let read = read_owned::<S>(
            context,
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Heads(heads),
            None,
        )
        .await
        .map_err(map_post_load_read_error)?;
        self.record_local_heads(&read.heads);
        self.raw = read;
        self.needs_full_refresh = false;
        self.current = StoreSnapshot {
            heads: self.raw.heads.clone(),
            value: S::hydrate(&self.raw),
        };
        self.publish_current();
        Ok(())
    }

    async fn refresh_full(&mut self, context: &RequestContext) -> Result<(), StoreError> {
        let read = read_owned::<S>(
            context,
            &self.docs,
            self.capability,
            &self.doc_id,
            At::Current,
            None,
        )
        .await
        .map_err(map_post_load_read_error)?;
        self.record_local_heads(&read.heads);
        self.raw = read;
        self.needs_full_refresh = false;
        self.current = StoreSnapshot {
            heads: self.raw.heads.clone(),
            value: S::hydrate(&self.raw),
        };
        self.publish_current();
        Ok(())
    }

    fn publish_current(&mut self) {
        self.version += 1;
        self.version_tx.send_replace(self.version);
    }

    fn fail(&mut self, error: StoreError) {
        if self.terminal_error.is_none() {
            self.terminal_error = Some(error);
            self.version += 1;
            self.version_tx.send_replace(self.version);
        }
    }
}

/// The poll request for the store's one document. `IsDoc` anchors it on a `Read` check of that
/// document, and `All` delivers every datum, owned or not, because an unowned datum still moves
/// the document's heads.
fn poll_request(capability: CapId, doc_id: &DocId, after: u64, hold_ms: u32) -> PollDatums {
    PollDatums {
        capability,
        predicate: Predicate::IsDoc {
            doc_id: doc_id.clone(),
        },
        datums: DatumSelection::All,
        after,
        limit: 0,
        hold_ms,
    }
}

/// Pages the document's frontier from the start without holding, and returns the cursor of its
/// end. The events are discarded: the caller reads the state itself and resumes from the cursor.
async fn replay_cursor(
    context: &RequestContext,
    docs: &Client<DocsProtocol>,
    capability: CapId,
    doc_id: &DocId,
) -> Result<u64, StoreError> {
    let mut after = 0;
    loop {
        let page = with_context(
            context,
            docs.call(context.child(), poll_request(capability, doc_id, after, 0)),
        )
        .await?
        .map_err(map_client_error)?;
        after = page.through;
        if page.caught_up {
            return Ok(after);
        }
    }
}

/// Long-polls the document from `after` and hands each page to the owner. The cursor is the only
/// state; a poll error is delivered once and ends the task, and the owner dropping its receiver
/// (shutdown) ends it too, which is why a failed send is not an error here.
async fn poll_pages(
    docs: Client<DocsProtocol>,
    capability: CapId,
    doc_id: DocId,
    mut after: u64,
    context: RequestContext,
    tasks: Spawner,
    pages: mpsc::Sender<Result<DatumPage, StoreError>>,
) {
    loop {
        let page = tokio::select! {
            biased;
            _ = tasks.cancelled() => return,
            page = docs.call(
                context.child(),
                poll_request(capability, &doc_id, after, POLL_HOLD_MS),
            ) => page.map_err(map_stream_error),
        };
        let failed = page.is_err();
        if let Ok(page) = &page {
            after = page.through;
        }
        if pages.send(page).await.is_err() || failed {
            return;
        }
    }
}

async fn read_owned<S: DatumStore>(
    context: &RequestContext,
    docs: &Client<DocsProtocol>,
    capability: CapId,
    doc_id: &DocId,
    at: At,
    datums: Option<BTreeSet<DatumKey>>,
) -> Result<DatumsRead, StoreError> {
    let mut read = with_context(
        context,
        docs.call(
            context.child(),
            ReadDatums {
                capability,
                doc_id: doc_id.clone(),
                at,
                datums,
                optimistic: false,
            },
        ),
    )
    .await?
    .map_err(map_client_error)?;
    read.datums.retain(|key, _| S::owns(key));
    Ok(read)
}

async fn with_context<F, T>(context: &RequestContext, future: F) -> Result<T, StoreError>
where
    F: Future<Output = T>,
{
    context.validate()?;
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
        _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
        output = future => Ok(output),
    }
}

fn map_client_error(error: ClientError<DocsError>) -> StoreError {
    match error {
        ClientError::Operation(error) => StoreError::Docs(error),
        ClientError::Transport { message, .. } => StoreError::DocsUnavailable(message),
    }
}

fn map_stream_error(error: ClientError<DocsError>) -> StoreError {
    match error {
        ClientError::Operation(DocsError::Denied) => StoreError::AccessLost,
        ClientError::Operation(error) => StoreError::Docs(error),
        ClientError::Transport { message, .. } => StoreError::DocsUnavailable(message),
    }
}

fn map_post_load_read_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Docs(DocsError::Denied) => StoreError::AccessLost,
        other => other,
    }
}

#[cfg(test)]
mod tests;
