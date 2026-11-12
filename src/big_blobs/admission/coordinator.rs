use super::*;
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

type Operation = Box<dyn FnOnce(State) -> BoxFuture<'static, ()> + Send>;

#[derive(Clone)]
pub(crate) struct Admission {
    sender: mpsc::Sender<Operation>,
    pub changed: Arc<Notify>,
    changes: watch::Sender<u64>,
}

impl Admission {
    pub async fn boot(sql: Database) -> Res<(Self, service_utils_rs::StopToken)> {
        let state = State::new(sql).await?;
        let changed = Arc::clone(&state.changed);
        let changes = state.changes.clone();
        let (sender, mut receiver) = mpsc::channel::<Operation>(128);
        let (stop, tasks) = service_utils_rs::StopToken::new();
        let cancellation = tasks.clone();
        tasks
            .spawn(async move {
                loop {
                    let operation = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            receiver.close();
                            break;
                        }
                        operation = receiver.recv() => operation,
                    };
                    let Some(operation) = operation else {
                        return;
                    };
                    operation(state.clone()).await;
                }
                while let Some(operation) = receiver.recv().await {
                    operation(state.clone()).await;
                }
            })
            .expect("admission owner starts before shutdown");
        Ok((
            Self {
                sender,
                changed,
                changes,
            },
            stop,
        ))
    }

    pub(super) async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(State) -> BoxFuture<'static, Res<T>> + Send + 'static,
    ) -> Res<T> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Box::new(move |state| {
                Box::pin(async move {
                    let result = operation(state).await;
                    if let Err(result) = reply.send(result) {
                        result.expect("cancelled admission operation failed");
                    }
                })
            }))
            .await
            .unwrap_or_else(|_| panic!("admission owner stopped before its clients"));
        response
            .await
            .expect("admission owner dropped its response")
    }

    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    #[cfg(test)]
    pub async fn register(&self, inventory: &DocId) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.register(&inventory).await }))
            .await
    }

    pub async fn inventory(&self, part: &PartKey) -> Res<Option<DocId>> {
        let part = part.clone();
        self.run(move |state| Box::pin(async move { state.inventory(&part).await }))
            .await
    }

    pub async fn recover(&self) -> Res<()> {
        self.run(move |state| Box::pin(async move { state.recover().await }))
            .await
    }

    pub async fn select_root(
        &self,
        drawer: &DocId,
        selection: Option<InventorySelection>,
    ) -> Res<()> {
        let drawer = drawer.clone();
        self.run(move |state| {
            Box::pin(async move { state.discovery_policy(&drawer, "root", selection).await })
        })
        .await
    }

    pub async fn override_drawer(
        &self,
        drawer: &DocId,
        selection: Option<InventorySelection>,
    ) -> Res<()> {
        let drawer = drawer.clone();
        self.run(move |state| {
            Box::pin(async move { state.discovery_policy(&drawer, "override", selection).await })
        })
        .await
    }

    pub async fn discovery_work(&self) -> Res<Option<(DocId, u64)>> {
        self.run(move |state| Box::pin(async move { state.discovery_work().await }))
            .await
    }

    pub async fn discovery_documents(&self) -> Res<Vec<DocId>> {
        self.run(move |state| Box::pin(async move { state.discovery_documents().await }))
            .await
    }

    pub async fn observe_drawer(
        &self,
        drawer: &DocId,
        generation: u64,
        snapshot: Option<DrawerSnapshot>,
    ) -> Res<()> {
        let drawer = drawer.clone();
        self.run(move |state| {
            Box::pin(async move { state.observe_drawer(&drawer, generation, snapshot).await })
        })
        .await
    }

    pub async fn selected_inventories(&self) -> Res<Vec<DocId>> {
        self.run(move |state| Box::pin(async move { state.selected_inventories().await }))
            .await
    }

    pub async fn invalidate_discovery(
        &self,
        documents: std::collections::BTreeSet<DocId>,
    ) -> Res<()> {
        self.run(move |state| Box::pin(async move { state.invalidate_discovery(documents).await }))
            .await
    }

    pub async fn rearm_discovery(&self) -> Res<()> {
        self.run(move |state| Box::pin(async move { state.rearm_discovery().await }))
            .await
    }

    pub async fn coverage(&self, root: &DocId) -> Res<Coverage> {
        let root = root.clone();
        self.run(move |state| Box::pin(async move { state.coverage(&root).await }))
            .await
    }

    pub async fn select(&self, inventory: &DocId, owner: &str, selected: bool) -> Res<()> {
        let inventory = inventory.clone();
        let owner = owner.to_owned();
        self.run(move |state| {
            Box::pin(async move { state.select(&inventory, &owner, selected).await })
        })
        .await
    }

    pub async fn authorize(&self, inventory: &DocId, readable: bool) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.authorize(&inventory, readable).await }))
            .await
    }

    pub async fn pins(
        &self,
        inventory: &DocId,
        revision: u64,
        pins: &BTreeMap<BlobId, u64>,
        complete: bool,
    ) -> Res<()> {
        let inventory = inventory.clone();
        let pins = pins.clone();
        self.run(move |state| {
            Box::pin(async move { state.pins(&inventory, revision, &pins, complete).await })
        })
        .await
    }

    pub async fn observe(
        &self,
        inventory: &DocId,
        digest: BlobId,
        peer: &PeerKey,
        revision: u64,
        present: bool,
        payload: Option<&serde_json::Value>,
    ) -> Res<()> {
        let inventory = inventory.clone();
        let peer = peer.clone();
        let payload = payload.cloned();
        self.run(move |state| {
            Box::pin(async move {
                state
                    .observe(
                        &inventory,
                        digest,
                        &peer,
                        revision,
                        present,
                        payload.as_ref(),
                    )
                    .await
            })
        })
        .await
    }

    #[cfg(test)]
    pub async fn produce(
        &self,
        inventory: &DocId,
        digest: BlobId,
        owner: &str,
        active: bool,
    ) -> Res<()> {
        let inventory = inventory.clone();
        let owner = owner.to_owned();
        self.run(move |state| {
            Box::pin(async move { state.produce(&inventory, digest, &owner, active).await })
        })
        .await
    }

    pub async fn state(&self, inventory: &DocId, digest: BlobId) -> Res<Option<PairState>> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.state(&inventory, digest).await }))
            .await
    }

    pub async fn available(&self, inventory: &DocId, digest: BlobId, available: bool) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| {
            Box::pin(async move { state.available(&inventory, digest, available).await })
        })
        .await
    }

    pub async fn length(&self, inventory: &DocId, digest: BlobId) -> Res<Option<u64>> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.length(&inventory, digest).await }))
            .await
    }

    pub async fn inventories(&self) -> Res<Vec<DocId>> {
        self.run(move |state| Box::pin(async move { state.inventories().await }))
            .await
    }

    pub async fn dirty(&self) -> Res<Vec<(DocId, BlobId, u64)>> {
        self.run(move |state| Box::pin(async move { state.dirty().await }))
            .await
    }

    #[cfg(test)]
    pub async fn settle(&self, inventory: &DocId, digest: BlobId, generation: u64) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| {
            Box::pin(async move { state.settle(&inventory, digest, generation).await })
        })
        .await
    }

    pub async fn rearm(&self) -> Res<()> {
        self.run(move |state| Box::pin(async move { state.rearm().await }))
            .await
    }

    pub async fn fail(&self, attempt: &Attempt) -> Res<()> {
        let attempt = attempt.clone();
        self.run(move |state| Box::pin(async move { state.fail(&attempt).await }))
            .await
    }

    #[cfg(test)]
    pub async fn begin(&self, inventory: &DocId, digest: BlobId) -> Res<Option<Attempt>> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.begin(&inventory, digest).await }))
            .await
    }

    pub async fn begin_from(
        &self,
        inventory: &DocId,
        digest: BlobId,
        peers: std::collections::BTreeSet<PeerKey>,
    ) -> Res<Option<Attempt>> {
        let inventory = inventory.clone();
        self.run(move |state| {
            Box::pin(async move { state.begin_from(&inventory, digest, Some(&peers)).await })
        })
        .await
    }

    pub async fn complete(&self, attempt: &Attempt, authorized: bool) -> Res<bool> {
        let attempt = attempt.clone();
        self.run(move |state| Box::pin(async move { state.complete(&attempt, authorized).await }))
            .await
    }

    pub async fn publish(
        &self,
        inventory: &DocId,
        digest: BlobId,
        generation: u64,
        effect: impl FnOnce(PairState, Option<u64>) -> BoxFuture<'static, Res<bool>> + Send + 'static,
    ) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| {
            Box::pin(async move { state.publish(&inventory, digest, generation, effect).await })
        })
        .await
    }
}
