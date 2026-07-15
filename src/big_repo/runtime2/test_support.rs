//! Test-only memory transport support for runtime2.
//!
//! Provides paired in-memory raw byte transports using `async_channel`
//! directly (no Tokio types in the transport layer) and a connector-level
//! test proving that Subduction handshake identity is authoritative:
//! even with a misleading discovery audience, the returned peer IDs equal
//! the signer-derived handshake IDs.
//!
//! # What is tested
//!
//! Two [`MemorySigner`]s create a transport pair. One side runs
//! [`handshake::initiate`] with a deliberately bogus discovery audience;
//! the other runs [`handshake::respond`] with the same bogus audience.
//! The test asserts the returned peer IDs match the signers' verifying
//! keys, proving that the handshake (not the transport layer) is
//! authoritative for identity.
//!
//! [`MemorySigner`]: subduction_crypto::signer::memory::MemorySigner

use core::time::Duration;
use std::sync::Arc;

use async_channel;
use future_form::{FutureForm, Sendable};
use subduction_core::{
    handshake::audience::Audience,
    handshake::{self, Handshake},
    nonce_cache::NonceCache,
    peer::id::PeerId as SubductionPeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};

// ─── Error type ────────────────────────────────────────────────────────────

/// Unified error for [`MemoryRawTransport`] send/recv operations.
/// Both sides of the paired transport can fail because the peer dropped
/// their end of the channel.
#[derive(Debug, thiserror::Error)]
pub enum MemoryTransportError {
    #[error("memory transport send failed: channel closed")]
    SendClosed,
    #[error("memory transport recv failed: channel closed")]
    RecvClosed,
}

// ─── MemoryRawTransport ────────────────────────────────────────────────────

/// A minimal raw byte transport backed by an `async_channel` sender/receiver
/// pair.
///
/// Implements [`Handshake<Sendable>`] so it works with
/// `subduction_core::handshake::initiate` and `handshake::respond`.
/// Contains **no Tokio types** — only `async_channel` and `futures`.
pub struct MemoryRawTransport {
    /// Channel for sending bytes to the paired peer.
    tx: async_channel::Sender<Vec<u8>>,
    /// Channel for receiving bytes from the paired peer.
    rx: async_channel::Receiver<Vec<u8>>,
}

impl MemoryRawTransport {
    fn new(tx: async_channel::Sender<Vec<u8>>, rx: async_channel::Receiver<Vec<u8>>) -> Self {
        Self { tx, rx }
    }
}

impl Handshake<Sendable> for MemoryRawTransport {
    type Error = MemoryTransportError;

    fn send(
        &mut self,
        bytes: Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        let tx = self.tx.clone();
        <Sendable as FutureForm>::from_future(async move {
            tx.send(bytes)
                .await
                .map_err(|_| MemoryTransportError::SendClosed)
        })
    }

    fn recv(&mut self) -> <Sendable as FutureForm>::Future<'_, Result<Vec<u8>, Self::Error>> {
        let rx = self.rx.clone();
        <Sendable as FutureForm>::from_future(async move {
            rx.recv()
                .await
                .map_err(|_| MemoryTransportError::RecvClosed)
        })
    }
}

// ─── Factory ───────────────────────────────────────────────────────────────

/// Create a pair of cross-connected [`MemoryRawTransport`]s.
///
/// Returns `(transport_a, transport_b)` where bytes sent on `transport_a`
/// are received on `transport_b`, and vice versa.
pub fn memory_transport_pair() -> (MemoryRawTransport, MemoryRawTransport) {
    // Channel: A sends → B receives
    let (a_to_b_tx, a_to_b_rx) = async_channel::unbounded::<Vec<u8>>();
    // Channel: B sends → A receives
    let (b_to_a_tx, b_to_a_rx) = async_channel::unbounded::<Vec<u8>>();

    let a = MemoryRawTransport::new(b_to_a_tx, a_to_b_rx);
    let b = MemoryRawTransport::new(a_to_b_tx, b_to_a_rx);
    (a, b)
}

const BOGUS_DISCOVERY_ID: &[u8] = b"bogus-discovery-id-for-test";

// ─── Test ──────────────────────────────────────────────────────────────────

/// Prove that the Subduction handshake identity is authoritative, even when
/// the transport layer carries a misleading (bogus) discovery audience.
///
/// Two peers are arranged with cross-connected memory transports. Each side
/// uses a real [`MemorySigner`]. The initiator passes an intentionally bogus
/// [`Audience::Discover`] label. The test asserts that:
///
/// * The initiator's returned peer ID equals the responder's signer public key.
/// * The responder's returned peer ID equals the initiator's signer public key.
#[tokio::test]
async fn handshake_identity_is_authoritative() {
    utils_rs::testing::setup_tracing_once();

    // ── seeds ──────────────────────────────────────────────────────────────
    let initiator_signer = MemorySigner::from_bytes(&[0xAB; 32]);
    let responder_signer = MemorySigner::from_bytes(&[0xCD; 32]);

    let initiator_expected = {
        let key = initiator_signer.verifying_key();
        SubductionPeerId::new(key.to_bytes())
    };
    let responder_expected = {
        let key = responder_signer.verifying_key();
        SubductionPeerId::new(key.to_bytes())
    };

    let bogus_audience = Audience::discover(BOGUS_DISCOVERY_ID);

    let now = TimestampSeconds::new(1_000_000);
    let nonce = Nonce::from_u128(42);
    let nonce_cache = Arc::new(NonceCache::default());
    let max_drift = Duration::from_secs(60);

    // ── create paired transports ──────────────────────────────────────────
    let (initiator_transport, responder_transport) = memory_transport_pair();

    // ── spawn initiator task ──────────────────────────────────────────────
    // build_connection returns (SubductionPeerId, ()) — the peer ID from the
    // handshake is the actual connection type C used in Authenticated<C, Sendable>.
    let initiator_handle = tokio::spawn(async move {
        handshake::initiate::<Sendable, _, _, _, MemorySigner>(
            initiator_transport,
            |_transport, peer_id| (peer_id, ()),
            &initiator_signer,
            bogus_audience,
            now,
            nonce,
        )
        .await
    });

    // ── spawn responder task ──────────────────────────────────────────────
    let responder_handle = tokio::spawn(async move {
        handshake::respond::<Sendable, _, _, _, MemorySigner>(
            responder_transport,
            |_transport, peer_id| (peer_id, ()),
            &responder_signer,
            &nonce_cache,
            responder_expected,
            Some(bogus_audience),
            now,
            max_drift,
        )
        .await
    });

    // ── await both sides ──────────────────────────────────────────────────
    let initiator_result = initiator_handle
        .await
        .expect("initiator task panicked")
        .expect("initiator handshake failed");

    let responder_result = responder_handle
        .await
        .expect("responder task panicked")
        .expect("responder handshake failed");

    // Extract the Authenticated wrapper from the result tuple.
    // initiate/respond return: (Authenticated<C, Sendable>, E)
    // Here C = SubductionPeerId, E = ()
    let initiator_auth = initiator_result.0;
    let responder_auth = responder_result.0;

    // ── assertions ────────────────────────────────────────────────────────
    // The initiator learns the *responder's* identity from the handshake response.
    assert_eq!(
        initiator_auth.peer_id(),
        responder_expected,
        "initiator sees responder's signer-derived peer ID"
    );

    // The responder learns the *initiator's* identity from the challenge signature.
    assert_eq!(
        responder_auth.peer_id(),
        initiator_expected,
        "responder sees initiator's signer-derived peer ID"
    );

    // Cross-verify: neither side should return a zero peer ID (the bogus
    // audience label should not influence the authenticated identity).
    assert_ne!(
        initiator_auth.peer_id(),
        SubductionPeerId::new([0; 32]),
        "initiator should NOT see a zero peer ID"
    );
    assert_ne!(
        responder_auth.peer_id(),
        SubductionPeerId::new([0; 32]),
        "responder should NOT see a zero peer ID"
    );
}
