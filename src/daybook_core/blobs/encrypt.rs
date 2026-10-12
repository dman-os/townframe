//! Encrypted (RFC 8188 `aes128gcm`) blob flows over iroh-blobs.
//!
//! The model: an encrypted blob (`C`) is a *virtual* entry in the
//! iroh-blobs store — the store keeps only `C`'s bao outboard and a provider
//! name, never the bytes — while the plaintext (`P`) is an ordinary stored
//! blob. Serving `C` re-encrypts `P` on demand, record by record, so
//! ciphertext is never materialized locally. This is what lets a node hold a
//! 4K video in its inventory and serve it in byte windows without ever
//! writing the ciphertext out.
//!
//! Durability is carried by two durable named tags, `ct:<C>` → `C` and
//! `pt:<C>` → `P` (rooted at [`store::set_pair_tags`], released by
//! [`store::drop_pair_tags`]): named tags are what the store's GC seeds its
//! root set from, so a registered pair lives until it is deliberately
//! released by deleting both tags. Key material never touches this store —
//! it lives in the document layer as a JWK facet and arrives through
//! [`CipherKeySource`].
//!
//! Salt derivation (a deliberate deviation from vanilla RFC 8188): the salt
//! is not persisted state but derived as
//! `BLAKE3-derive("daybook.cipherblob.salt.v1", master_key ‖ P_hash ‖ BE32(rs) ‖ padding-octet)[..16]`.
//! GCM is catastrophic under (CEK, nonce) reuse, and nonces reset per
//! message, so two plaintexts under one shared master key MUST NOT share a
//! salt; content-deriving the salt makes that collision impossible by
//! construction — no persistence-layer discipline required — while keeping
//! encryption deterministic (`C` is reconstructable byte-identically from
//! `P` + key, which is what install-from-scratch and resumed downloads rely
//! on). Consequence: rotating only the salt is impossible by design (rotate
//! keys instead).
//!
//! The module map, by concern:
//!
//! - [`params`]: framing vocabulary shared with the cipherBlob facet's
//!   `encodingParameters` (padding policy, record size).
//! - [`keys`]: [`MasterKey`], the [`CipherKeySource`] resolution seam, and
//!   the [`JwkOct`] facet codec.
//! - [`codec`]: the pure RFC 8188 codec — header, per-record crypto, and the
//!   incremental encryptor/decryptor the store flows are built from.
//! - [`store`]: store-side flows — import plaintext, run the encrypt pass
//!   (install `C`'s virtual entry), root/release the pair tags, and
//!   fetch-and-decrypt locally ([`get_decrypted`]).
//! - [`download`]: resumable encrypted downloads ([`download_encrypted`] +
//!   [`FsDownloadLedger`]).
//! - [`serve`]: the [`CipherBlobProvider`] registry that serves virtual `C`
//!   entries by encrypting windows of stored `P`.
//! - [`windowed`]: random-access plaintext reads over a stored ciphertext
//!   ([`CipherReader`]).
//!
//! Memory ceiling across all of it: one RFC 8188 record plus a pipe buffer
//! plus a bao outboard (~1/512 of the blob), regardless of blob size, on any
//! store backend.
//!
//! The complete design — why the facets, pins, and worker are shaped the way
//! they are — lives in `docs/adrs/003-cipherblob.md`.

use crate::interlude::*;

pub type Res<T> = eyre::Result<T>;

mod codec;
mod download;
mod keys;
mod params;
mod serve;
mod store;
mod windowed;

pub use codec::{decrypt_bytes, encrypt_bytes, encrypt_with_rs};
pub use download::{FsDownloadLedger, download_encrypted};
pub use keys::{CipherKeySource, JwkOct, MapKeySource, MasterKey};
pub use params::{
    CONTENT_ENCODING_AES128GCM, DEFAULT_PADDING, EncodingParams, Padding, RECORD_SIZE,
};
pub use serve::CipherBlobProvider;
pub use store::{
    PROVIDER_NAME, TAG_CT_PREFIX, TAG_PT_PREFIX, add_encrypted, add_encrypted_stream,
    ensure_stored, get_decrypted,
};
pub(crate) use store::{drop_pair_tags, has_pair_tags};
pub use windowed::CipherReader;

// Crate-internal plumbing, reachable under the umbrella path (tests read it
// through `use super::*`; sibling blob modules use it directly).

#[cfg(test)]
mod tests;
