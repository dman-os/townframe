use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_channel::Receiver as AsyncReceiver;
use futures::future::BoxFuture;
use subduction_core::peer::id::PeerId;
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_ephemeral::{
    clock::{std_clock::StdClock, Clock},
    config::EphemeralEvent,
    handler::EphemeralHandler,
    message::{EphemeralMessage, EphemeralPayload},
    policy::OpenEphemeralPolicy,
    topic::Topic,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{interlude::*, runtime::BigRepoIrohTransport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigEphemeralTopic([u8; 32]);

impl BigEphemeralTopic {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<BigEphemeralTopic> for Topic {
    fn from(topic: BigEphemeralTopic) -> Self {
        Self::new(topic.0)
    }
}

impl From<Topic> for BigEphemeralTopic {
    fn from(topic: Topic) -> Self {
        Self(*topic.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigEphemeralEvent {
    pub topic: BigEphemeralTopic,
    pub sender: PeerId,
    pub nonce: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BigEphemeralFilter {
    pub topic: BigEphemeralTopic,
    pub sender: Option<PeerId>,
}

impl BigEphemeralFilter {
    #[must_use]
    pub const fn new(topic: BigEphemeralTopic) -> Self {
        Self {
            topic,
            sender: None,
        }
    }

    #[must_use]
    pub const fn with_sender(mut self, sender: PeerId) -> Self {
        self.sender = Some(sender);
        self
    }

    fn matches(&self, event: &BigEphemeralEvent) -> bool {
        self.topic == event.topic && self.sender.is_none_or(|sender| sender == event.sender)
    }
}

#[derive(Clone)]
pub struct BigEphemeral {
    backend: Arc<dyn BigEphemeralBackend>,
    switchboard: BigEphemeralSwitchboard,
}

impl BigEphemeral {
    pub(crate) fn new(
        backend: Arc<dyn BigEphemeralBackend>,
        switchboard: BigEphemeralSwitchboard,
    ) -> Self {
        Self {
            backend,
            switchboard,
        }
    }

    pub async fn publish(&self, topic: BigEphemeralTopic, payload: Vec<u8>) -> Res<()> {
        self.backend.publish(topic, payload).await
    }

    pub async fn subscribe(&self, filter: BigEphemeralFilter) -> Res<BigEphemeralSubscription> {
        self.switchboard.subscribe(filter).await
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BigEphemeralSwitchboard {
    cmd_tx: mpsc::UnboundedSender<BigEphemeralSwitchboardCmd>,
    next_subscription_id: Arc<AtomicU64>,
}

impl BigEphemeralSwitchboard {
    pub(crate) fn spawn(
        backend: Arc<dyn BigEphemeralBackend>,
        event_rx: AsyncReceiver<EphemeralEvent>,
        runtime_stop: CancellationToken,
        runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    ) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let next_subscription_id = Arc::new(AtomicU64::new(0));

        let mut state = SwitchboardState {
            listeners: HashMap::new(),
            topic_refcounts: HashMap::new(),
        };
        let fut = async move {
            let mut cmd_closed = false;
            loop {
                tokio::select! {
                    biased;
                    event = event_rx.recv() => {
                        let Ok(event) = event else {
                            break;
                        };
                        let event = BigEphemeralEvent {
                            topic: BigEphemeralTopic::from(event.id),
                            sender: event.sender,
                            nonce: event.nonce,
                            payload: event.payload,
                        };
                        state.dispatch_event(&backend, event).await?;
                    }
                    cmd = cmd_rx.recv(), if !cmd_closed => {
                        match cmd {
                            Some(BigEphemeralSwitchboardCmd::Register {
                                subscription_id,
                                filter,
                                event_tx,
                                ack_tx,
                            }) => {
                                state.register_listener(&backend, subscription_id, filter, event_tx).await?;
                                ack_tx.send(()).ok();
                            }
                            Some(BigEphemeralSwitchboardCmd::Unregister { subscription_id }) => {
                                state.unregister_listener(&backend, subscription_id).await?;
                            }
                            None => {
                                cmd_closed = true;
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        };
        runtime_tasks
            .spawn({
                let stop = runtime_stop.child_token();
                async move {
                    if let Some(Err(err)) = stop.run_until_cancelled(fut).await {
                        panic!("{:?}", err);
                    }
                }
                .instrument(tracing::info_span!("BigEphemeralSwitchboard"))
            })
            .expect(ERROR_TOKIO);

        Self {
            cmd_tx,
            next_subscription_id,
        }
    }

    pub async fn subscribe(&self, filter: BigEphemeralFilter) -> Res<BigEphemeralSubscription> {
        let subscription_id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (ack_tx, ack_rx) = oneshot::channel();

        self.cmd_tx
            .send(BigEphemeralSwitchboardCmd::Register {
                subscription_id,
                filter,
                event_tx,
                ack_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;

        ack_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;

        Ok(BigEphemeralSubscription {
            subscription_id,
            cmd_tx: self.cmd_tx.clone(),
            event_rx,
        })
    }
}

#[derive(Debug)]
pub struct BigEphemeralSubscription {
    subscription_id: u64,
    cmd_tx: mpsc::UnboundedSender<BigEphemeralSwitchboardCmd>,
    event_rx: mpsc::UnboundedReceiver<BigEphemeralEvent>,
}

impl BigEphemeralSubscription {
    pub async fn recv(&mut self) -> Option<BigEphemeralEvent> {
        self.event_rx.recv().await
    }
}

impl Drop for BigEphemeralSubscription {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(BigEphemeralSwitchboardCmd::Unregister {
            subscription_id: self.subscription_id,
        });
    }
}

pub(crate) trait BigEphemeralBackend: Send + Sync {
    fn publish(&self, topic: BigEphemeralTopic, payload: Vec<u8>) -> BoxFuture<'_, Res<()>>;
    fn subscribe_peer(&self, peer_id: PeerId) -> BoxFuture<'_, ()>;
    fn subscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()>;
    fn unsubscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()>;
}

#[derive(Clone)]
pub(crate) struct BigRepoEphemeralBackend<C = BigRepoIrohTransport>
where
    C: Clone + 'static,
{
    signer: MemorySigner,
    handler: Arc<EphemeralHandler<future_form::Sendable, C, OpenEphemeralPolicy, StdClock>>,
}

impl<C> BigRepoEphemeralBackend<C>
where
    C: Clone,
{
    pub(crate) fn new(
        signer: MemorySigner,
        handler: Arc<EphemeralHandler<future_form::Sendable, C, OpenEphemeralPolicy, StdClock>>,
    ) -> Self {
        Self { signer, handler }
    }
}

impl<C> BigEphemeralBackend for BigRepoEphemeralBackend<C>
where
    C: subduction_core::connection::Connection<future_form::Sendable, EphemeralMessage>
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn publish(&self, topic: BigEphemeralTopic, payload: Vec<u8>) -> BoxFuture<'_, Res<()>> {
        Box::pin(async move {
            let timestamp = StdClock.now();
            let nonce: u64 = rand::random();
            let payload = EphemeralPayload {
                id: topic.into(),
                nonce,
                timestamp,
                payload,
            };
            let signed = Signed::seal::<future_form::Sendable, _>(&self.signer, payload).await;
            self.handler
                .publish(EphemeralMessage::Ephemeral(Box::new(signed.into_signed())))
                .await;
            Ok(())
        })
    }

    fn subscribe_peer(&self, peer_id: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.handler.subscribe_peer(peer_id).await;
        })
    }

    fn subscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.handler
                .subscribe(nonempty12::nonempty![topic.into()])
                .await;
        })
    }

    fn unsubscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.handler
                .unsubscribe(nonempty12::nonempty![topic.into()])
                .await;
        })
    }
}

enum BigEphemeralSwitchboardCmd {
    Register {
        subscription_id: u64,
        filter: BigEphemeralFilter,
        event_tx: mpsc::UnboundedSender<BigEphemeralEvent>,
        ack_tx: oneshot::Sender<()>,
    },
    Unregister {
        subscription_id: u64,
    },
}

struct SwitchboardState {
    listeners: HashMap<u64, ListenerEntry>,
    topic_refcounts: HashMap<BigEphemeralTopic, usize>,
}

struct ListenerEntry {
    filter: BigEphemeralFilter,
    event_tx: mpsc::UnboundedSender<BigEphemeralEvent>,
}

impl SwitchboardState {
    async fn register_listener<B: BigEphemeralBackend + ?Sized>(
        &mut self,
        backend: &Arc<B>,
        subscription_id: u64,
        filter: BigEphemeralFilter,
        event_tx: mpsc::UnboundedSender<BigEphemeralEvent>,
    ) -> Res<()> {
        let previous = self
            .listeners
            .insert(subscription_id, ListenerEntry { filter, event_tx });
        assert!(previous.is_none(), "duplicate ephemeral subscription id");

        let topic = filter.topic;
        let count = self.topic_refcounts.entry(topic).or_insert(0);
        if *count == 0 {
            backend.subscribe_topic(topic).await;
        }
        *count += 1;
        Ok(())
    }

    async fn unregister_listener<B: BigEphemeralBackend + ?Sized>(
        &mut self,
        backend: &Arc<B>,
        subscription_id: u64,
    ) -> Res<()> {
        let Some(entry) = self.listeners.remove(&subscription_id) else {
            return Ok(());
        };
        let topic = entry.filter.topic;
        let Some(count) = self.topic_refcounts.get_mut(&topic) else {
            return Ok(());
        };
        assert!(*count > 0, "ephemeral topic refcount underflow");
        *count -= 1;
        if *count == 0 {
            self.topic_refcounts.remove(&topic);
            backend.unsubscribe_topic(topic).await;
        }
        Ok(())
    }

    async fn dispatch_event<B: BigEphemeralBackend + ?Sized>(
        &mut self,
        backend: &Arc<B>,
        event: BigEphemeralEvent,
    ) -> Res<()> {
        let mut stale_listener_ids = Vec::new();
        for (subscription_id, listener) in &self.listeners {
            if listener.filter.matches(&event) && listener.event_tx.send(event.clone()).is_err() {
                stale_listener_ids.push(*subscription_id);
            }
        }

        let mut topics_to_unsubscribe = Vec::new();
        for subscription_id in stale_listener_ids {
            if let Some(entry) = self.listeners.remove(&subscription_id) {
                let topic = entry.filter.topic;
                let Some(count) = self.topic_refcounts.get_mut(&topic) else {
                    continue;
                };
                assert!(*count > 0, "ephemeral topic refcount underflow");
                *count -= 1;
                if *count == 0 {
                    self.topic_refcounts.remove(&topic);
                    topics_to_unsubscribe.push(topic);
                }
            }
        }

        for topic in topics_to_unsubscribe {
            backend.unsubscribe_topic(topic).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::timeout;

    #[derive(Debug, Default)]
    struct MockBackend {
        subscribes: Arc<tokio::sync::Mutex<Vec<BigEphemeralTopic>>>,
        unsubscribes: Arc<tokio::sync::Mutex<Vec<BigEphemeralTopic>>>,
        publish_count: AtomicUsize,
    }

    impl BigEphemeralBackend for MockBackend {
        fn publish(&self, _topic: BigEphemeralTopic, _payload: Vec<u8>) -> BoxFuture<'_, Res<()>> {
            Box::pin(async move {
                self.publish_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn subscribe_peer(&self, _peer_id: PeerId) -> BoxFuture<'_, ()> {
            Box::pin(async move {})
        }

        fn subscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()> {
            let subscribes = Arc::clone(&self.subscribes);
            Box::pin(async move {
                subscribes.lock().await.push(topic);
            })
        }

        fn unsubscribe_topic(&self, topic: BigEphemeralTopic) -> BoxFuture<'_, ()> {
            let unsubscribes = Arc::clone(&self.unsubscribes);
            Box::pin(async move {
                unsubscribes.lock().await.push(topic);
            })
        }
    }

    #[tokio::test]
    async fn switchboard_filters_and_refcounts() -> Res<()> {
        let backend = Arc::new(MockBackend::default());
        let mut state = SwitchboardState {
            listeners: HashMap::new(),
            topic_refcounts: HashMap::new(),
        };

        let topic = BigEphemeralTopic::new([7; 32]);
        let other_topic = BigEphemeralTopic::new([9; 32]);
        let sender_a = PeerId::new([1; 32]);
        let sender_b = PeerId::new([2; 32]);
        let (listener_a_tx, mut listener_a_rx) = tokio::sync::mpsc::unbounded_channel();
        let (listener_b_tx, mut listener_b_rx) = tokio::sync::mpsc::unbounded_channel();

        state
            .register_listener(
                &backend,
                1,
                BigEphemeralFilter::new(topic).with_sender(sender_a),
                listener_a_tx,
            )
            .await?;
        state
            .register_listener(
                &backend,
                2,
                BigEphemeralFilter::new(topic).with_sender(sender_b),
                listener_b_tx,
            )
            .await?;

        assert_eq!(backend.subscribes.lock().await.as_slice(), &[topic]);

        state
            .dispatch_event(
                &backend,
                BigEphemeralEvent {
                    topic,
                    sender: sender_a,
                    nonce: 1,
                    payload: vec![1, 2, 3],
                },
            )
            .await?;

        let event = timeout(std::time::Duration::from_secs(1), listener_a_rx.recv())
            .await
            .expect("timed out waiting for matching event")
            .expect("listener closed unexpectedly");
        assert_eq!(event.topic, topic);
        assert_eq!(event.sender, sender_a);
        assert_eq!(event.payload, vec![1, 2, 3]);
        assert!(
            timeout(std::time::Duration::from_millis(100), listener_b_rx.recv())
                .await
                .is_err()
        );

        state
            .dispatch_event(
                &backend,
                BigEphemeralEvent {
                    topic: other_topic,
                    sender: sender_a,
                    nonce: 2,
                    payload: vec![9],
                },
            )
            .await?;
        assert!(
            timeout(std::time::Duration::from_millis(100), listener_a_rx.recv())
                .await
                .is_err()
        );

        state.unregister_listener(&backend, 1).await?;
        assert!(backend.unsubscribes.lock().await.is_empty());
        state.unregister_listener(&backend, 2).await?;
        timeout(std::time::Duration::from_secs(1), async {
            loop {
                let should_stop = {
                    let unsubscribes = backend.unsubscribes.lock().await;
                    unsubscribes.as_slice() == [topic]
                };
                if should_stop {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for unsubscribe");
        Ok(())
    }
}
