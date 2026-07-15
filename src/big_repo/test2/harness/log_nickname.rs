//! Peer-id → human nickname mapping for readable test diagnostics.
//!
//! Tests register nicknames at boot (e.g. peer "Alice"/"Bob"). The
//! [`nickname`] helper renders any `PeerId` as its nickname (falling back to a
//! short hex prefix), so assertion failures and [`super::dump`] diagnostics
//! read like "Alice sedimentree-heads ≠ Bob" instead of raw hashes.
//!
//! This is a lightweight, dependency-free stand-in for keyhive's tracing
//! `LogRewriter`; it covers the per-message value we need in our own dump
//! output without rewiring the global tracing subscriber.

use crate::PeerId;
use std::collections::HashMap;
use std::sync::OnceLock;

static REGISTRY: OnceLock<std::sync::Mutex<HashMap<[u8; 32], String>>> = OnceLock::new();

fn registry() -> &'static std::sync::Mutex<HashMap<[u8; 32], String>> {
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Register `name` for `peer_id`. Idempotent.
pub fn register(peer_id: PeerId, name: impl Into<String>) {
    let mut map = registry().lock().expect("nickname registry poisoned");
    map.insert(*peer_id.as_bytes(), name.into());
}

/// Render `peer_id` as its registered nickname, or a short hex prefix if none.
pub fn nickname(peer_id: &PeerId) -> String {
    let map = registry().lock().expect("nickname registry poisoned");
    if let Some(name) = map.get(peer_id.as_bytes()) {
        return name.clone();
    }
    let bytes = peer_id.as_bytes();
    format!(
        "peer:{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// Clear all nicknames (used between independent test cases).
#[allow(dead_code)]
pub fn clear() {
    registry()
        .lock()
        .expect("nickname registry poisoned")
        .clear();
}
