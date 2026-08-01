//! Tier 9 — ephemeral topic delivery and filtering.

use super::harness::Pair;
use crate::{BigEphemeralFilter, BigEphemeralTopic};
use std::time::Duration;
use tokio::time::timeout;

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

    pair.left()
        .repo
        .ephemeral()
        .publish(topic, b"hello-ephemeral".to_vec())
        .await?;

    let event = timeout(Duration::from_secs(5), subscription.recv())
        .await
        .map_err(|_| crate::ferr!("timed out waiting for ephemeral event"))?
        .ok_or_else(|| crate::ferr!("ephemeral subscription closed unexpectedly"))?;
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

    pair.left()
        .repo
        .ephemeral()
        .publish(topic, b"matching".to_vec())
        .await?;

    let event = timeout(Duration::from_secs(5), matching.recv())
        .await
        .map_err(|_| crate::ferr!("timed out waiting for matching ephemeral event"))?
        .ok_or_else(|| crate::ferr!("matching ephemeral subscription closed unexpectedly"))?;
    assert_eq!(event.payload, b"matching");
    assert!(timeout(Duration::from_millis(250), wrong_sender.recv())
        .await
        .is_err());
    assert!(timeout(Duration::from_millis(250), wrong_topic.recv())
        .await
        .is_err());
    Ok(())
}
