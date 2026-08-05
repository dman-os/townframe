//! Tier 9 — ephemeral topic delivery and filtering.

use super::harness::topo::Node;
use super::harness::Pair;
use crate::{BigEphemeralEvent, BigEphemeralFilter, BigEphemeralSubscription, BigEphemeralTopic};
use std::time::Duration;
use tokio::time::timeout;

/// Ephemeral delivery is fire-and-forget (see `subduction_ephemeral`'s
/// design): a publish that races the subscriber's `Subscribe` — still queued
/// on the publisher's listener task right after a fresh connect — is dropped
/// silently. Retry until the event lands, bounded by the outer timeout.
async fn publish_until_delivered(
    publisher: &Node,
    topic: BigEphemeralTopic,
    payload: Vec<u8>,
    subscription: &mut BigEphemeralSubscription,
) -> crate::Res<BigEphemeralEvent> {
    timeout(Duration::from_secs(5), async {
        loop {
            publisher
                .repo
                .ephemeral()
                .publish(topic, payload.clone())
                .await?;
            match timeout(Duration::from_millis(200), subscription.recv()).await {
                Ok(Some(event)) => return Ok(event),
                Ok(None) => return Err(crate::ferr!("ephemeral subscription closed unexpectedly")),
                Err(_) => continue,
            }
        }
    })
    .await
    .map_err(|_| crate::ferr!("timed out waiting for ephemeral event"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn tier9_ephemeral_roundtrip_between_two_nodes() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(210, 211, "Publisher", "Subscriber").await?;
    let topic = BigEphemeralTopic::new([0xAB; 32]);
    let sender = subduction_core::peer::id::PeerId::new(*pair.left().peer_id().as_bytes());
    let mut subscription = pair
        .right()
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(topic).with_sender(sender))
        .await?;
    pair.connect().await?;

    let event = publish_until_delivered(
        pair.left(),
        topic,
        b"hello-ephemeral".to_vec(),
        &mut subscription,
    )
    .await?;
    assert_eq!(event.topic, topic);
    assert_eq!(event.sender, sender);
    assert_eq!(event.payload, b"hello-ephemeral");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier9_ephemeral_filters_topic_and_sender() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(212, 213, "Publisher", "Subscriber").await?;
    let topic = BigEphemeralTopic::new([0xAC; 32]);
    let other_topic = BigEphemeralTopic::new([0xAD; 32]);
    let sender = subduction_core::peer::id::PeerId::new(*pair.left().peer_id().as_bytes());
    let other_sender = subduction_core::peer::id::PeerId::new(*pair.right().peer_id().as_bytes());
    let mut matching = pair
        .right()
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(topic).with_sender(sender))
        .await?;
    let mut wrong_sender = pair
        .right()
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(topic).with_sender(other_sender))
        .await?;
    let mut wrong_topic = pair
        .right()
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(other_topic).with_sender(sender))
        .await?;
    pair.connect().await?;

    let event =
        publish_until_delivered(pair.left(), topic, b"matching".to_vec(), &mut matching).await?;
    assert_eq!(event.payload, b"matching");
    assert!(timeout(Duration::from_millis(250), wrong_sender.recv())
        .await
        .is_err());
    assert!(timeout(Duration::from_millis(250), wrong_topic.recv())
        .await
        .is_err());
    Ok(())
}
