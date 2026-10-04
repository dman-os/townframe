//! Framing vocabulary of the `aes128gcm` ciphertext codec.
//!
//! Two inputs describe *how* a ciphertext is laid out on the wire, and neither
//! is derivable from anything else: the padding policy ([`Padding`]) and the
//! record size ([`RECORD_SIZE`]). Both travel together in a
//! [`EncodingParams`] value.
//!
//! Any path that *reconstructs* a ciphertext (re-encrypting a stored
//! plaintext, serving a window of it) needs the framing, because a different
//! record size means records start at different offsets — bytes written at one
//! framing cannot verify against a bao outboard built at another. Paths that
//! only *decrypt* do not need it: RFC 8188 stores `rs` in the ciphertext's own
//! authenticated header, so the decoder reads it back from the wire.
//!
//! The serde spelling of these types is camelCase JSON on purpose: this is the
//! schema a cipherBlob facet's `encodingParameters` value carries, so the
//! facet vocabulary and the codec vocabulary are one type and cannot drift.
//! (ADR 003 §3 records why the facet shape looks like this.)

use crate::interlude::*;

use super::Res;
use super::codec::RECORD_OVERHEAD;

/// This codec version's ceiling on a header-carried record size: the largest
/// `rs` a peer may name in an RFC 8188 header or an `encodingParameters`
/// facet. It is the bound that preserves the no-whole-blob-buffering
/// guarantee of the streaming decryptor — a decoder may buffer at most one
/// unauthenticated record, so without a ceiling a hostile `rs` of
/// 0xFFFF_FFFF would force up to 4 GiB of buffering before a single byte was
/// authenticated — while staying comfortably above the 64 KiB
/// [`RECORD_SIZE`] default.
pub(crate) const MAX_WIRE_RECORD_SIZE: u64 = 1 << 20;

/// The one validation rule for a peer-supplied, pre-auth `rs`, shared by every
/// place such an `rs` enters the codec: the facet's `encodingParameters` (via
/// [`EncodingParams::new`]) and each header-parse seam (`StreamDecryptor::push`,
/// `decrypt_bytes_ikm`, `CipherReader::open`). Every consumer of an
/// unvalidated `rs` — the record-buffering budget, `record_size` strides,
/// `payload_size` math — must sit behind this.
pub(crate) fn validate_record_size(record_size: u64) -> Res<()> {
    eyre::ensure!(
        record_size > RECORD_OVERHEAD as u64,
        "record size {record_size} leaves no room for a record payload"
    );
    eyre::ensure!(
        record_size <= MAX_WIRE_RECORD_SIZE,
        "record size {record_size} exceeds this codec's maximum wire record size \
         of {MAX_WIRE_RECORD_SIZE} octets"
    );
    Ok(())
}

/// Default record size for new ciphertexts (RFC 8188's recommended 64 KiB).
pub const RECORD_SIZE: u64 = 64 * 1024;

/// Padding policy for new ciphertexts: what the *final* record does with the
/// leftover space. Only the final record is affected; non-final records are
/// always exactly `rs` wire bytes.
///
/// The serde spelling of a variant is its `encodingParameters.padding` token,
/// so a facet and a codec cannot disagree about the policy in flight.
///
/// (The policy exists because ciphertext length is metadata a relay sees; see
/// ADR 003 §17 for the threat model behind the default.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Padding {
    /// Final record carries only the delimiter: minimal wire length, so the
    /// body reveals the precise tail length.
    Minimal,
    /// Final record is zero-padded to full record size: the body length
    /// reveals only the record count, never the tail (v1 default — avoids
    /// byte-length correlation across files by relays).
    #[default]
    Record,
}
impl Padding {
    /// The canonical one-octet identity of this padding policy in the salt
    /// derivation. Values are frozen forever (`Minimal = 1`, `Record = 2`),
    /// and a future policy takes a fresh value — never a reshuffle — so
    /// derivations under variants that existed before it keep reproducing
    /// byte-identically (see [`MasterKey::salt_for`]).
    pub(crate) fn domain_byte(&self) -> u8 {
        match self {
            Padding::Minimal => 1,
            Padding::Record => 2,
        }
    }
}

/// Padding policy used by the store flows and new representations.
pub const DEFAULT_PADDING: Padding = Padding::Record;

/// How a ciphertext is framed: the RFC 8188 record size `rs` and the padding
/// policy.
///
/// These are per-representation inputs that are not derivable from anything
/// else. A different `rs` is a different wire format — records start `rs`
/// octets apart — so every path that reconstructs or serves a ciphertext has
/// to be told them rather than assuming. Paths that only decrypt do not: RFC
/// 8188 puts `rs` in the authenticated header, so the decoder reads it from
/// the wire.
///
/// The serde spelling of the fields is the facet's `encodingParameters` JSON
/// spelling, so one type serves the codec and the schema instead of two that
/// can drift (ADR 003 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncodingParams {
    /// RFC 8188 `rs`: record size in octets, both a header field and the
    /// stride between records.
    pub record_size: u64,
    /// Padding policy. Only the final record differs.
    pub padding: Padding,
}

impl EncodingParams {
    /// The framing used by the store flows and new representations.
    pub const DEFAULT: Self = Self {
        record_size: RECORD_SIZE,
        padding: DEFAULT_PADDING,
    };

    /// Check what a peer-supplied `encodingParameters` must satisfy before the
    /// rest of the codec may assume it: the shared pre-auth record-size rule
    /// ([`validate_record_size`]). The [`MAX_WIRE_RECORD_SIZE`] ceiling and the
    /// payload-room floor are exactly what the header-parse seams enforce too,
    /// so facet-declared and header-carried `rs` cannot diverge in validity.
    pub fn new(record_size: u64, padding: Padding) -> Res<Self> {
        validate_record_size(record_size)?;
        Ok(Self {
            record_size,
            padding,
        })
    }

    /// Interpret a cipherBlob facet's `(contentEncoding, encodingParameters)`.
    ///
    /// `contentEncoding` selects the schema of `encodingParameters`, so a
    /// scheme this codec does not implement is rejected rather than guessed
    /// at, and the record size is validated because both values arrive from a
    /// peer's facet.
    pub fn from_encoding_parameters(
        content_encoding: &str,
        parameters: &serde_json::Value,
    ) -> Res<Self> {
        eyre::ensure!(
            content_encoding == CONTENT_ENCODING_AES128GCM,
            "unsupported content encoding {content_encoding:?}"
        );
        let parsed: Self = serde_json::from_value(parameters.clone()).wrap_err_with(|| {
            format!("error parsing {CONTENT_ENCODING_AES128GCM} encodingParameters")
        })?;
        Self::new(parsed.record_size, parsed.padding)
    }

    /// The `encodingParameters` value describing this framing: what a facet
    /// has to record for a peer to reconstruct the same ciphertext.
    pub fn to_encoding_parameters(&self) -> serde_json::Value {
        serde_json::to_value(self).expect(ERROR_JSON)
    }
}

impl Default for EncodingParams {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The `contentEncoding` token this codec implements. It is the algorithm
/// pivot, so it is also what a facet must carry for this codec to read its
/// representation at all.
pub const CONTENT_ENCODING_AES128GCM: &str = "aes128gcm";
