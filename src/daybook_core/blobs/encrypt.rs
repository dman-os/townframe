//! Encrypted (RFC 8188 `aes128gcm`) blob flows over iroh-blobs.
//!
//! Ciphertext blobs (`C`) live in iroh-blobs as *virtual* entries: the store
//! keeps only the bao outboard and a provider name, never the bytes. The
//! plaintext (`P`) is a normal stored blob. Serving `C` re-encrypts `P` on
//! demand record-by-record, so ciphertext is never materialized locally.
//!
//! Tag scheme (durable GC roots + linkage; ADR 003):
//! - `ct:<C>` -> `C` (raw): roots the virtual entry
//! - `pt:<C>` -> `P` (raw): roots the plaintext; keyed by `C` so the pair is
//!   resolvable from either side at startup
//!
//! Key storage follows ADR 003 §6: the JWK facet lives in the Keyhive-protected
//! Automerge document (encrypted at rest by the document layer). No key
//! material is persisted in this store; [`CipherKeySource`] resolves through
//! the facet graph (C -> cipherBlob facet -> keyRef -> JWK -> secret).
//!
//! The store flows are streaming end to end: import consumes a plaintext
//! `Stream` (never a whole-file buffer), the encrypt pass re-reads stored `P`
//! in one incremental sweep, re-hashing what it reads to prove the
//! (key, digest) binding, and computes `C`'s hash + outboard with bao_tree's
//! streaming outboard (`bao_tree::io::sync::outboard`) - ciphertext bytes
//! are never stored, spilt to disk, or accumulated. Downloads feed verified
//! leaves through decryption straight into the importer. Memory ceiling: one
//! RFC 8188 record plus a pipe buffer plus the outboard (~1/512 of the
//! blob), regardless of blob size, on any store backend.
//!
//! RFC 8188 notes: the 26-byte header carries salt + record size + key id;
//! the CEK (16B) and nonce base (12B) are HKDF-SHA256 expansions of the
//! master key under the salt; each record is its plaintext chunk plus a
//! delimiter byte (0x01, or 0x02 on the final record) encrypted with AES-GCM;
//! nonces are `nonce_base XOR seq_be96`.
//!
//! Salt derivation (Daybook deviation-in-the-good-direction): the salt is
//! not persisted state but derived as
//! `BLAKE3("daybook.cipherblob.salt.v1" || master_key || P_hash)[..16]`.
//! Because GCM is catastrophic under (CEK, nonce) reuse, and nonces reset per
//! message, two plaintexts under one shared master key MUST NOT share a salt.
//! Content-deriving the salt makes that collision impossible by construction
//! - no persistence-layer discipline required - while keeping encryption
//! deterministic (reconstruct C byte-identically from P + key). Consequence:
//! rotating only the salt is impossible by design (non-goal; rotate keys).

use crate::interlude::*;

use bytes::Bytes;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, sync::Mutex};

use aes_gcm::{
    Aes128Gcm, Nonce,
    aead::{AeadInPlace, KeyInit},
};
use bao_tree::io::mixed::ReadBytesAt as _;
use hkdf::Hkdf;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, remote::GetStreamPair},
    store::virtual_blob::SyncReader,
};
use rand::RngCore;
use sha2::Sha256;

/// Default record size for new ciphertexts (RFC 8188's recommended 64 KiB).
pub const RECORD_SIZE: u64 = 64 * 1024;
const SALT_LEN: usize = 16;
/// GCM tag (16) + record delimiter byte (1).
const RECORD_OVERHEAD: usize = 17;

/// Padding policy for new ciphertexts (ADR 003 §17). Only the final
/// record differs; non-final records are always exactly `rs` wire bytes.
///
/// The `serde` spelling of a variant *is* its `encodingParameters.padding`
/// token (ADR 003 §3), so the facet's vocabulary and the codec's cannot drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Padding {
    /// Final record carries only the delimiter: minimal wire length, so the
    /// body reveals the precise tail length.
    Minimal,
    /// Final record is zero-padded to full record size: the body length
    /// reveals only the record count, never the tail (v1 default - avoids
    /// byte-length correlation across files by relays).
    #[default]
    Record,
}

/// Padding policy used by the store flows and new representations.
pub const DEFAULT_PADDING: Padding = Padding::Record;

/// How a ciphertext is framed: the RFC 8188 record size `rs` and the padding
/// policy.
///
/// These are per-representation inputs that are not derivable from anything
/// else, which is exactly why the cipherBlob facet carries them as
/// `encodingParameters` (ADR 003 §3). A different `rs` is a different wire
/// format - records start `rs` octets apart - so every path that *reconstructs*
/// or *serves* a ciphertext has to be told them rather than assuming. Paths
/// that only decrypt do not: RFC 8188 puts `rs` in the authenticated header, so
/// the decoder reads it from the wire (see [`HeaderFacts`]).
///
/// This *is* the `encodingParameters` shape for `aes128gcm` (ADR 003 §3): the
/// `serde` spelling of the fields is the facet's JSON spelling, so one type
/// serves the codec and the schema instead of two that can drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncodingParams {
    /// RFC 8188 `rs`: record size in octets, both a header field and the stride
    /// between records.
    pub record_size: u64,
    /// Padding policy (ADR 003 §17). Only the final record differs.
    pub padding: Padding,
}

impl EncodingParams {
    /// The framing used by the store flows and new representations.
    pub const DEFAULT: Self = Self {
        record_size: RECORD_SIZE,
        padding: DEFAULT_PADDING,
    };

    /// Check what a peer-supplied `encodingParameters` must satisfy before the
    /// rest of the codec may assume it: the RFC 8188 header stores `rs` in four
    /// octets, and a record has to leave room for its own tag and delimiter.
    pub fn new(record_size: u64, padding: Padding) -> Res<Self> {
        eyre::ensure!(
            record_size > RECORD_OVERHEAD as u64,
            "record size {record_size} leaves no room for a record payload"
        );
        eyre::ensure!(
            record_size <= u32::MAX as u64,
            "record size {record_size} does not fit the RFC 8188 header field"
        );
        Ok(Self {
            record_size,
            padding,
        })
    }

    /// Interpret a cipherBlob facet's `(contentEncoding, encodingParameters)`.
    ///
    /// `contentEncoding` selects the schema of `encodingParameters` (ADR 003
    /// §3), so a scheme this codec does not implement is rejected rather than
    /// guessed at, and the record size is validated because both values arrive
    /// from a peer's facet.
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

    /// The `encodingParameters` value describing this framing: what a facet has
    /// to record for a peer to reconstruct the same ciphertext.
    pub fn to_encoding_parameters(&self) -> serde_json::Value {
        serde_json::to_value(self).expect(ERROR_JSON)
    }
}

impl Default for EncodingParams {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The `contentEncoding` token this codec implements (ADR 003 §3). It is the
/// algorithm pivot, so it is also what a facet must carry for this codec to
/// read its representation at all.
pub const CONTENT_ENCODING_AES128GCM: &str = "aes128gcm";

/// Payload octets per record: what is left of `rs` after the GCM tag and the
/// delimiter. Callers that accept `rs` from outside validate it first
/// ([`EncodingParams::new`]); the clamp only keeps a nonsensical internal `rs`
/// from panicking on underflow.
fn payload_size(rs: u64) -> u64 {
    (rs - RECORD_OVERHEAD as u64).max(1)
}

/// encode; foreign key ids are tolerated and skipped when decoding).
const HEADER_LEN: usize = SALT_LEN + 4 + 1;
const KEY_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const MASTER_KEY_LEN: usize = 32;

const CEK_INFO: &[u8] = b"Content-Encoding: aes128gcm\x00";
const NONCE_INFO: &[u8] = b"Content-Encoding: nonce\x00";
const DELIMITER: u8 = 0x01;
const LAST_RECORD_DELIMITER: u8 = 0x02;

pub const PROVIDER_NAME: &str = "daybook-cipherblob-v1";
pub const TAG_CT_PREFIX: &str = "ct:";
pub const TAG_PT_PREFIX: &str = "pt:";

pub type Res<T> = eyre::Result<T>;

fn raw(h: Hash) -> HashAndFormat {
    HashAndFormat {
        hash: h,
        format: BlobFormat::Raw,
    }
}

/// Master key for one cipherblob family. The caller owns persistence
/// (facet/JWK codecs land later); this module only consumes it.
///
/// The RFC 8188 salt is NOT part of this binding - it is derived per
/// representation from (master key, plaintext digest), so the same master key
/// can safely encrypt any number of distinct representations without any
/// cross-node coordination (see module docs).
#[derive(Clone, PartialEq, Eq)]
pub struct MasterKey([u8; MASTER_KEY_LEN]);

impl MasterKey {
    pub fn random() -> Self {
        let mut key = [0u8; MASTER_KEY_LEN];
        rand::rng().fill_bytes(&mut key);
        Self(key)
    }

    /// The salt for a plaintext of this representation: a pure function of
    /// (master key, plaintext digest). Two different plaintexts under one key
    /// therefore derive different CEKs/nonce bases - a GCM nonce collision
    /// across messages is unrepresentable.
    fn salt_for(&self, p_hash: &Hash) -> [u8; SALT_LEN] {
        let mut hasher = blake3::Hasher::new_derive_key("daybook.cipherblob.salt.v1");
        hasher.update(&self.0);
        hasher.update(p_hash.as_bytes());
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&hasher.finalize().as_bytes()[..SALT_LEN]);
        salt
    }

    fn cipher_for(&self, p_hash: &Hash) -> Cipher {
        self.cipher_with_salt(&self.salt_for(p_hash))
    }

    fn cipher_with_salt(&self, salt: &[u8; SALT_LEN]) -> Cipher {
        cipher_from_ikm(&self.0, salt)
    }
}

/// RFC 8188 §2.2/§2.3: CEK and nonce base are HKDF-SHA256 expansions of the
/// input-keying material under the (header-carried) salt.
fn cipher_from_ikm(ikm: &[u8], salt: &[u8; SALT_LEN]) -> Cipher {
    let hk = Hkdf::<Sha256>::new(Some(salt), ikm);
    let mut cek = [0u8; KEY_LEN];
    let mut nonce_base = [0u8; NONCE_LEN];
    // Lengths are consts; expansion cannot fail.
    hk.expand(CEK_INFO, &mut cek).expect("cek length valid");
    hk.expand(NONCE_INFO, &mut nonce_base)
        .expect("nonce length valid");
    Cipher {
        aead: Aes128Gcm::new_from_slice(&cek).expect("cek length valid"),
        nonce_base,
    }
}

/// Resolve the [`MasterKey`] for an existing ciphertext hash.
///
/// Minimal seam on purpose: the implementation over the document layer
/// (cipherBlob facet -> keyRef -> JWK facet -> secret, ADR 003 §6) lives with
/// the repo, not here. The salt is not resolved: it is recomputed from the
/// plaintext when encrypting, and taken from the (GCM-authenticated) header
/// when decrypting.
#[async_trait::async_trait]
pub trait CipherKeySource: Send + Sync {
    async fn key_for(&self, c: &Hash) -> Res<MasterKey>;

    /// Framing of the ciphertext `C` (ADR 003 §3): the `rs` and padding it was
    /// built with. Only needed where `C` must be reconstructed or served
    /// locally - a resumed download re-installs the virtual entry by
    /// re-encrypting the completed plaintext, and the serving provider frames
    /// its windows - because decryption reads `rs` from the authenticated
    /// header instead. The facet-driven implementation reads
    /// `encodingParameters`; the v1 default is [`EncodingParams::DEFAULT`].
    async fn encoding_for(&self, _c: &Hash) -> Res<EncodingParams> {
        Ok(EncodingParams::DEFAULT)
    }
}

/// Trivial in-memory resolver (tests, and callers that already hold keys).
#[derive(Clone, Default)]
pub struct MapKeySource(pub HashMap<Hash, MasterKey>);

#[async_trait::async_trait]
impl CipherKeySource for MapKeySource {
    async fn key_for(&self, c: &Hash) -> Res<MasterKey> {
        self.0
            .get(c)
            .cloned()
            .ok_or_else(|| eyre::eyre!("no key registered for ciphertext {c}"))
    }
}

/// JWK base64url, no padding (RFC 7515 §2); 32-octet keys encode to exactly
/// 43 characters. Decoding uses the strict canonical form: non-canonical
/// trailing bits are rejected by `data_encoding`.
fn b64url_encode_32(key: &[u8; MASTER_KEY_LEN]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(key)
}

fn b64url_decode_32(s: &str) -> Res<[u8; MASTER_KEY_LEN]> {
    eyre::ensure!(
        s.len() == 43,
        "expected 43 unpadded base64url chars for a 32-octet key, got {}",
        s.len()
    );
    let bytes = data_encoding::BASE64URL_NOPAD
        .decode(s.as_bytes())
        .map_err(|e| eyre::eyre!("invalid unpadded base64url: {e}"))?;
    let out: [u8; MASTER_KEY_LEN] = bytes
        .try_into()
        .expect("43 unpadded base64url chars decode to 32 octets");
    Ok(out)
}

/// The `org.example.daybook.jwk` shape from ADR 003 §5: a plain RFC 7517
/// oct-sequence JWK whose `k` carries the cipherblob master key,
/// base64url-encoded without padding.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JwkOct {
    pub kty: String,
    pub k: String,
}

impl JwkOct {
    pub fn from_master_key(key: &MasterKey) -> Self {
        Self {
            kty: "oct".to_owned(),
            k: b64url_encode_32(&key.0),
        }
    }

    pub fn to_master_key(&self) -> Res<MasterKey> {
        eyre::ensure!(self.kty == "oct", "unsupported JWK key type {:?}", self.kty);
        let oct = b64url_decode_32(&self.k)?;
        Ok(MasterKey(oct))
    }
}

// ---------------------------------------------------------------------------
// Core codec
// ---------------------------------------------------------------------------

struct Cipher {
    aead: Aes128Gcm,
    nonce_base: [u8; NONCE_LEN],
}

impl Cipher {
    fn nonce(&self, seq: u64) -> [u8; NONCE_LEN] {
        let mut n = self.nonce_base;
        for (i, b) in seq.to_be_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        n
    }

    /// Encrypt one record: `plaintext` is the record content (without the
    /// delimiter); `pad_zeros` zero octets follow the delimiter so the
    /// AEAD input reaches the chosen framing length.
    fn encrypt_record(
        &self,
        seq: u64,
        last: bool,
        mut plaintext: Vec<u8>,
        pad_zeros: usize,
    ) -> Vec<u8> {
        plaintext.push(if last {
            LAST_RECORD_DELIMITER
        } else {
            DELIMITER
        });
        plaintext.extend(std::iter::repeat_n(0u8, pad_zeros));
        self.aead
            .encrypt_in_place(Nonce::from_slice(&self.nonce(seq)), &[], &mut plaintext)
            .expect("aes-gcm encryption cannot fail");
        plaintext
    }

    /// Decrypt one record; returns `(payload, is_final)`. Errors on auth
    /// failure or a bad delimiter. RFC 8188 padding: the delimiter is the
    /// last non-zero octet; foreign encoders may pad after it.
    fn decrypt_record(&self, seq: u64, ct: &[u8]) -> Res<(Vec<u8>, bool)> {
        let mut buf = ct.to_vec();
        self.aead
            .decrypt_in_place(Nonce::from_slice(&self.nonce(seq)), &[], &mut buf)
            .map_err(|e| eyre::eyre!("record decryption failed: {e:?}"))?;
        let Some(pos) = buf.iter().rposition(|&b| b != 0) else {
            eyre::bail!("record contains no delimiter octet");
        };
        let payload = &buf[..pos];
        match buf[pos] {
            DELIMITER => Ok((payload.to_vec(), false)),
            LAST_RECORD_DELIMITER => Ok((payload.to_vec(), true)),
            other => eyre::bail!("bad record delimiter {other:#x}"),
        }
    }
}

fn header(salt: &[u8; SALT_LEN], rs: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..SALT_LEN].copy_from_slice(salt);
    h[SALT_LEN..SALT_LEN + 4].copy_from_slice(&(rs as u32).to_be_bytes());
    h[HEADER_LEN - 1] = 0; // id_len: no sender key id
    h
}

/// Encrypt a complete plaintext buffer with an explicit record size and
/// padding policy (ADR 003 §17).
///
/// Deterministic: the salt derives from (key, plaintext digest), so
/// re-encrypting produces byte-identical ciphertext for the same policy.
pub fn encrypt_with_rs(key: &MasterKey, plaintext: &[u8], rs: u64, padding: Padding) -> Vec<u8> {
    let p_hash = Hash::new(plaintext);
    encrypt_raw_ikm(&key.0, &key.salt_for(&p_hash), rs, padding, plaintext)
}

/// [`encrypt_with_rs`] with an explicit RFC 8188 (ikm, salt) - the shape
/// interop tests and reference vectors exercise.
fn encrypt_raw_ikm(
    ikm: &[u8],
    salt: &[u8; SALT_LEN],
    rs: u64,
    padding: Padding,
    plaintext: &[u8],
) -> Vec<u8> {
    let rc = cipher_from_ikm(ikm, salt);
    let payload_max = (rs as usize - RECORD_OVERHEAD).max(1);
    let mut out = Vec::with_capacity(HEADER_LEN + plaintext.len() / payload_max * (rs as usize));
    out.extend_from_slice(&header(salt, rs));
    let n_records = if plaintext.is_empty() {
        1
    } else {
        plaintext.len().div_ceil(payload_max)
    };
    // Non-final records always carry a full payload chunk and no padding;
    // their delimiter fills the frame to exactly `rs` wire bytes.
    let full = n_records - 1;
    for i in 0..full {
        let start = i * payload_max;
        let chunk = plaintext[start..start + payload_max].to_vec();
        out.extend_from_slice(&rc.encrypt_record(i as u64, false, chunk, 0));
    }
    // The final record carries the tail (possibly empty) plus the policy's
    // padding: a full frame under [`Padding::Record`], delimiter-only under
    // [`Padding::Minimal`].
    let final_content = &plaintext[full * payload_max..];
    let pad_zeros = match padding {
        Padding::Minimal => 0,
        Padding::Record => payload_max - final_content.len(),
    };
    out.extend_from_slice(&rc.encrypt_record(full as u64, true, final_content.to_vec(), pad_zeros));
    out
}

/// Encrypt with the default record size and v1 default padding.
pub fn encrypt_bytes(key: &MasterKey, plaintext: &[u8]) -> Vec<u8> {
    encrypt_with_rs(key, plaintext, RECORD_SIZE, DEFAULT_PADDING)
}

/// Decrypt a complete ciphertext buffer.
pub fn decrypt_bytes(key: &MasterKey, ciphertext: impl AsRef<[u8]>) -> Res<Vec<u8>> {
    decrypt_bytes_ikm(&key.0, ciphertext)
}

/// [`decrypt_bytes`] for arbitrary RFC 8188 input-keying material: the codec
/// never assumes the 32-octet [`MasterKey`] shape at the wire level.
fn decrypt_bytes_ikm(ikm: &[u8], ciphertext: impl AsRef<[u8]>) -> Res<Vec<u8>> {
    let ct = ciphertext.as_ref();
    eyre::ensure!(ct.len() >= HEADER_LEN, "ciphertext shorter than header");
    let rs = u32::from_be_bytes(ct[SALT_LEN..SALT_LEN + 4].try_into()?) as u64;
    // Foreign encoders may carry a key id; we never encode one but skip it.
    let idlen = ct[HEADER_LEN - 1] as usize;
    eyre::ensure!(
        ct.len() >= HEADER_LEN + idlen,
        "ciphertext truncated in key id"
    );
    let records = &ct[HEADER_LEN + idlen..];
    eyre::ensure!(!records.is_empty(), "ciphertext has no records");
    let record_len = rs as usize;
    eyre::ensure!(record_len > RECORD_OVERHEAD, "record size too small");
    let salt: [u8; SALT_LEN] = ct[..SALT_LEN].try_into()?;
    let cipher = cipher_from_ikm(ikm, &salt);
    let mut out = Vec::with_capacity(records.len());
    // The final record may be shorter than `rs`; the delimiter bit marks it.
    let mut offset = 0;
    let mut seq = 0u64;
    let mut saw_final = false;
    while offset < records.len() {
        eyre::ensure!(!saw_final, "data after final record");
        let take = record_len.min(records.len() - offset);
        eyre::ensure!(take >= RECORD_OVERHEAD, "record too short");
        let (payload, is_final) = cipher.decrypt_record(seq, &records[offset..offset + take])?;
        saw_final |= is_final;
        out.extend_from_slice(&payload);
        offset += take;
        seq += 1;
    }
    eyre::ensure!(saw_final, "ciphertext does not end with a final record");
    Ok(out)
}

// ---------------------------------------------------------------------------
// Streaming decryptor
// ---------------------------------------------------------------------------

/// Incremental decryptor fed arbitrary ciphertext byte chunks (bao leaves are
/// chunk-aligned, not record-aligned, so internal buffering is required).
///
/// The master key must be supplied up front; the CEK derives from the salt
/// carried in the (GCM-authenticated) header.
struct StreamDecryptor {
    key: MasterKey,
    cipher: Option<Cipher>,
    header: Vec<u8>,
    pending: Vec<u8>,
    seq: u64,
    record_len: usize,
    saw_final: bool,
    outbox: Vec<Vec<u8>>,
    outboard: Vec<u8>,
    ciphertext_len: u64,
    /// Foreign key-id octets from the header not yet skipped over.
    pending_id_skip: usize,
    /// key-id length from the header (0 for our own encoding).
    idlen: u8,
}

/// Header facts of a ciphertext under decryption: everything a later attempt
/// needs to reconstruct the decryptor mid-stream (the CEK and nonce base are
/// derived from `(key, salt)`; records beyond the resume point are
/// self-contained).
#[derive(Clone, Copy, Debug)]
struct HeaderFacts {
    salt: [u8; SALT_LEN],
    rs: u32,
    idlen: u8,
}

impl HeaderFacts {
    /// plaintext octets carried by a full (non-final) record
    fn payload_max(&self) -> u64 {
        self.rs as u64 - RECORD_OVERHEAD as u64
    }
    /// first byte offset of record `seq` in the ciphertext
    fn record_start(&self, seq: u64) -> u64 {
        HEADER_LEN as u64 + self.idlen as u64 + seq * self.rs as u64
    }
}

impl StreamDecryptor {
    /// A decryptor for a fresh stream: parses the header from it.
    fn new(key: MasterKey) -> Self {
        Self {
            idlen: 0,
            cipher: None,
            header: Vec::new(),
            pending: Vec::new(),
            seq: 0,
            record_len: 0,
            saw_final: false,
            outbox: Vec::new(),
            outboard: Vec::new(),
            ciphertext_len: 0,
            pending_id_skip: 0,
            key,
        }
    }

    /// Header facts, once the header has been parsed; `None` before the
    /// header octets arrived (a resumed decryptor never sees a header - its
    /// facts were persisted by the attempt that parsed it).
    fn header_facts(&self) -> Option<HeaderFacts> {
        if self.header.len() < HEADER_LEN {
            return None;
        }
        Some(HeaderFacts {
            salt: self.header[..SALT_LEN].try_into().expect("len checked"),
            rs: self.record_len as u32,
            idlen: self.idlen,
        })
    }

    /// A decryptor continuing at record `next_seq`: the header facts come
    /// from a previous attempt (the resumed stream starts inside the first
    /// record of the remaining suffix, so no header is expected).
    fn resumed(key: MasterKey, facts: HeaderFacts, next_seq: u64) -> Self {
        Self {
            idlen: facts.idlen,
            cipher: Some(key.cipher_with_salt(&facts.salt)),
            header: Vec::new(),
            pending: Vec::new(),
            seq: next_seq,
            record_len: facts.rs as usize,
            saw_final: false,
            outbox: Vec::new(),
            outboard: Vec::new(),
            ciphertext_len: 0,
            pending_id_skip: 0,
            key,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Res<()> {
        eyre::ensure!(!self.saw_final, "data after final record");
        self.ciphertext_len += bytes.len() as u64;
        let mut rest = bytes;
        if self.record_len == 0 {
            let take = (HEADER_LEN - self.header.len()).min(rest.len());
            self.header.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.header.len() < HEADER_LEN {
                return Ok(());
            }
            let salt: [u8; SALT_LEN] = self.header[..SALT_LEN].try_into()?;
            let rs = u32::from_be_bytes(
                self.header[SALT_LEN..SALT_LEN + 4]
                    .try_into()
                    .expect("header holds 4 rs octets"),
            ) as u64;
            eyre::ensure!(rs > RECORD_OVERHEAD as u64, "record size too small");
            // Foreign encoders may carry a key id; skip it. We never encode one.
            self.idlen = self.header[HEADER_LEN - 1];
            self.pending_id_skip = self.idlen as usize;
            self.record_len = rs as usize;
            self.cipher = Some(self.key.cipher_with_salt(&salt));
        }
        // Drop any foreign key-id octets still owed from the header.
        if self.pending_id_skip > 0 {
            let skip = self.pending_id_skip.min(rest.len());
            rest = &rest[skip..];
            self.pending_id_skip -= skip;
            if rest.is_empty() {
                return Ok(());
            }
        }
        self.pending.extend_from_slice(rest);
        while !self.saw_final && self.pending.len() >= self.record_len {
            let rec: Vec<u8> = self.pending.drain(..self.record_len).collect();
            let cipher = self.cipher.as_ref().expect("set with header");
            let (payload, is_final) = cipher.decrypt_record(self.seq, &rec)?;
            self.outbox.push(payload);
            self.seq += 1;
            self.saw_final |= is_final;
        }
        Ok(())
    }

    /// Take decrypted payload chunks emitted since the last drain.
    fn drain_outbox(&mut self) -> std::vec::IntoIter<Vec<u8>> {
        std::mem::replace(&mut self.outbox, Vec::new()).into_iter()
    }

    /// Consume the trailing final (possibly short) record once the stream
    /// ends. After this the outbox holds the record's payload; take it before
    /// dropping the decryptor.
    fn finish(&mut self) -> Res<()> {
        eyre::ensure!(self.record_len != 0, "empty ciphertext stream");
        if !self.saw_final {
            eyre::ensure!(
                !self.pending.is_empty(),
                "truncated ciphertext: no final record"
            );
            eyre::ensure!(
                self.pending.len() > RECORD_OVERHEAD,
                "final record too short"
            );
            let cipher = self.cipher.as_ref().expect("set with header");
            let rec = std::mem::take(&mut self.pending);
            let (payload, _is_final) = cipher.decrypt_record(self.seq, &rec)?;
            self.outbox.push(payload);
            self.saw_final = true;
        }
        eyre::ensure!(self.pending.is_empty(), "data after final record");
        Ok(())
    }

    /// Materialize all decrypted plaintext (one-shot consumers).
    fn into_plaintext(mut self) -> Vec<u8> {
        let mut out = Vec::new();
        for payload in self.drain_outbox() {
            out.extend_from_slice(&payload);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Store flows
// ---------------------------------------------------------------------------

/// Root the (C, P) linkage so both entries survive GC: `ct:<C>` -> `C`,
/// `pt:<C>` -> `P`. Key linkage is facet-level, not store-level (ADR 003 §6).
///
/// These are durable named tags, and named tags are exactly what the store's
/// GC treats as roots: its mark phase walks `tags().list()` and seeds the live
/// set with every tag's hash before sweeping anything unreached. So a
/// registered pair cannot be collected out from under a reader, and - the
/// other half of the same fact - deleting these two tags is the *only* thing
/// that releases the pair. Nothing removes them incidentally: releasing a
/// cipherblob is a deliberate act, and it belongs to whatever derives pins from
/// the facets that name the blob (ADR 003 §13).
async fn set_pair_tags(store: &Store, c_hash: Hash, p_hash: Hash) -> Res<()> {
    store
        .tags()
        .set(format!("{TAG_CT_PREFIX}{c_hash}"), raw(c_hash))
        .await?;
    store
        .tags()
        .set(format!("{TAG_PT_PREFIX}{c_hash}"), raw(p_hash))
        .await?;
    Ok(())
}

/// Release a pair: delete the two named tags that root it.
///
/// Both tag names carry `C`, so releasing needs no (C, P) map - the `pt:<C>`
/// tag records which plaintext it roots, and nothing else reads that back.
/// Deletion is the release: a tag that is never deleted pins the ciphertext's
/// outboard *and* the plaintext that serves it forever, because named tags are
/// what the store's GC mark phase seeds its root set from. So this is the
/// deliberate counterpart of [`set_pair_tags`], driven by whatever derives pins
/// from the facets that name the blob (ADR 003 §13/§19): the pin worker deletes
/// the tags when a ciphertext pin leaves the encrypted-representation
/// inventory. Deleting an absent tag is a no-op, so re-running is safe.
pub(crate) async fn drop_pair_tags(store: &Store, c_hash: Hash) -> Res<()> {
    store
        .tags()
        .delete(format!("{TAG_CT_PREFIX}{c_hash}"))
        .await?;
    store
        .tags()
        .delete(format!("{TAG_PT_PREFIX}{c_hash}"))
        .await?;
    Ok(())
}

/// Import a plaintext stream without whole-blob buffering; returns a temp tag
/// (keep alive until a named tag roots the entry) and the plaintext digest.
pub async fn ensure_stored<S>(store: &Store, p_stream: S) -> Res<(iroh_blobs::api::TempTag, Hash)>
where
    S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static,
{
    let tag = add_progress_to_tag(store.blobs().add_stream(p_stream).await).await?;
    let hash = tag.hash();
    Ok((tag, hash))
}

/// Encrypt-and-store a plaintext stream: import `P` incrementally (never
/// buffered whole), then [`CipherBlobProvider::install`] runs the §11 pass and
/// makes `C` servable, which roots `C` and `P` under named tags (see
/// [`set_pair_tags`]). Returns `(C, P)` hashes.
pub async fn add_encrypted_stream<S>(
    store: &Store,
    provider: &CipherBlobProvider,
    key: &MasterKey,
    encoding: EncodingParams,
    p_stream: S,
) -> Res<(Hash, Hash)>
where
    S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static,
{
    let (p_tag, p_hash) = ensure_stored(store, p_stream).await?;
    // The framing is the caller's policy - in production it is the facet's
    // `encodingParameters` - so the codec never picks one for them.
    let c_hash = match provider.install(store, key, p_hash, encoding).await {
        Ok(c_hash) => c_hash,
        Err(e) => {
            drop(p_tag);
            return Err(e);
        }
    };
    drop(p_tag);
    Ok((c_hash, p_hash))
}

/// [`add_encrypted_stream`] for data already fully in memory (tests, small
/// payloads). Large media must use the streaming form.
pub async fn add_encrypted(
    store: &Store,
    provider: &CipherBlobProvider,
    key: &MasterKey,
    encoding: EncodingParams,
    plaintext: &[u8],
) -> Res<(Hash, Hash)> {
    let chunk = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(
        plaintext.to_vec(),
    ))]);
    add_encrypted_stream(store, provider, key, encoding, chunk).await
}

/// Drive an add/import progress stream to completion, returning its temp tag.
async fn add_progress_to_tag(
    progress: iroh_blobs::api::blobs::AddProgress<'_>,
) -> Res<iroh_blobs::api::TempTag> {
    use futures::StreamExt;
    let stream = progress.stream().await;
    futures::pin_mut!(stream);
    loop {
        match stream.next().await {
            Some(iroh_blobs::api::proto::AddProgressItem::Done(tag)) => return Ok(tag),
            Some(iroh_blobs::api::proto::AddProgressItem::Error(e)) => {
                return Err(eyre::eyre!("import failed: {e}"));
            }
            Some(_) => {}
            None => eyre::bail!("import progress stream ended without completion"),
        }
    }
}

/// Streaming encryptor over a plaintext byte source: emits the RFC 8188
/// header, then whole ciphertext records as their plaintext arrives; the
/// final record at the end of stream, padded per the chosen policy.
struct RecordEncryptor {
    salt: [u8; SALT_LEN],
    cipher: Cipher,
    encoding: EncodingParams,
    pending: Vec<u8>,
    seq: u64,
    payload_max: usize,
    emitted_final: bool,
}

impl RecordEncryptor {
    fn new(key: &MasterKey, p_hash: &Hash, encoding: EncodingParams) -> Self {
        let salt = key.salt_for(p_hash);
        let cipher = key.cipher_with_salt(&salt);
        Self {
            salt,
            cipher,
            encoding,
            pending: Vec::new(),
            seq: 0,
            payload_max: payload_size(encoding.record_size) as usize,
            emitted_final: false,
        }
    }

    fn bake_header(&self) -> Vec<u8> {
        header(&self.salt, self.encoding.record_size).to_vec()
    }

    /// Absorb plaintext bytes; returns ciphertext wire bytes (never partial
    /// records - each emitted block is whole records or nothing, except the
    /// header which goes out first.
    ///
    /// A chunk that exactly fills a record is held back unencrypted until
    /// more data arrives or the stream ends - the final record is decided
    /// only at end of plaintext.
    fn feed(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        assert!(!self.emitted_final, "feeding after final record");
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        while self.pending.len() > self.payload_max {
            let chunk: Vec<u8> = self.pending.drain(..self.payload_max).collect();
            // non-final records are full content + delimiter: exactly a full
            // `rs` frame with no zero padding
            out.push(self.cipher.encrypt_record(self.seq, false, chunk, 0));
            self.seq += 1;
        }
        out
    }

    /// End of plaintext: emit the final record. It carries whatever is still
    /// pending - a full chunk (exact-multiple length), a short remainder, or
    /// nothing at all (empty plaintext) - padded per the policy.
    fn finish(&mut self) -> Vec<Vec<u8>> {
        assert!(!self.emitted_final, "finish called twice");
        self.emitted_final = true;
        let chunk = std::mem::take(&mut self.pending);
        let pad_zeros = match self.encoding.padding {
            Padding::Minimal => 0,
            Padding::Record => self.payload_max - chunk.len(),
        };
        vec![self.cipher.encrypt_record(self.seq, true, chunk, pad_zeros)]
    }
}

/// Number of records framing `p_len` plaintext octets: non-final records
/// hold one full payload chunk each; the final record carries the remainder.
fn n_records_for(p_len: u64, payload_max: u64) -> u64 {
    if p_len == 0 {
        1
    } else {
        p_len.div_ceil(payload_max)
    }
}

/// Total encoded body length (header + records) for `p_len` plaintext octets
/// under `encoding`.
fn ciphertext_len(p_len: u64, encoding: EncodingParams) -> u64 {
    let payload_max = payload_size(encoding.record_size);
    let n = n_records_for(p_len, payload_max);
    let final_content = p_len - (n - 1) * payload_max;
    let final_wire = match encoding.padding {
        // Minimal: content + delimiter + tag (a full-content final record is
        // exactly `rs` wire bytes - content + delimiter = rs - 16).
        Padding::Minimal => final_content + RECORD_OVERHEAD as u64,
        // Record: the final frame is padded up to full record size.
        Padding::Record => encoding.record_size,
    };
    HEADER_LEN as u64 + (n - 1) * encoding.record_size + final_wire
}

/// Pass 2 over already-stored plaintext: one incremental read sweep of `P`
/// through `export_bao`, re-hashing what it reads, encrypting
/// record-by-record, and feeding the ciphertext into a streaming bao
/// outboard computation. The ciphertext bytes are never stored, spilt to
/// disk, or accumulated; memory is one record plus the outboard itself.
///
/// Rejects with an error if the re-read plaintext no longer hashes to
/// `p_hash`: the (key, digest) salt binding must describe the bytes actually
/// encrypted, otherwise the resulting ciphertext's `pt:` mapping would lie
/// about its content.
///
/// Primitive of [`CipherBlobProvider::install`]: it installs the virtual entry
/// but neither registers a provider nor roots the tags, so a caller that stops
/// here leaves an entry the store cannot serve.
async fn install_virtual_encrypted(
    store: &Store,
    key: &MasterKey,
    p_hash: Hash,
    encoding: EncodingParams,
) -> Res<Hash> {
    use bao_tree::io::mixed::EncodedItem;
    use bao_tree::io::sync::outboard as bao_sync_outboard;
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;

    let mut items = store
        .blobs()
        .export_bao(p_hash, bao_tree::ChunkRanges::all())
        .stream();

    // The exporter announces the plaintext size first, which fixes the
    // ciphertext length (and therefore the ciphertext's bao tree).
    let p_len: u64 = match items.next().await {
        Some(EncodedItem::Size(len)) => len,
        Some(EncodedItem::Error(e)) => eyre::bail!("export of {p_hash} failed: {e:?}"),
        other => eyre::bail!("export of {p_hash} did not announce a size: {other:?}"),
    };
    let c_len = ciphertext_len(p_len, encoding);
    let tree = bao_tree::BaoTree::new(c_len, iroh_blobs::store::IROH_BLOCK_SIZE);
    let outboard_len = tree.outboard_size() as usize;

    // Producer: re-hash the plaintext while encrypting; write the ciphertext
    // wire bytes (header + whole records) into the pipe. Ending the future
    // closes the pipe: that is what tells the hasher the stream is complete.
    let enc = RecordEncryptor::new(key, &p_hash, encoding);
    let (mut c_tx, c_rx) = tokio::io::duplex(64 * 1024);
    let (rebuilt_tx, rebuilt_rx) = tokio::sync::oneshot::channel::<blake3::Hash>();
    let producer = async {
        let mut enc = enc;
        let mut p_hasher = blake3::Hasher::new();
        c_tx.write_all(&enc.bake_header()).await?;
        while let Some(item) = items.next().await {
            match item {
                EncodedItem::Leaf(leaf) => {
                    p_hasher.update(&leaf.data);
                    for rec in enc.feed(&leaf.data) {
                        c_tx.write_all(&rec).await?;
                    }
                }
                EncodedItem::Error(e) => eyre::bail!("export of {p_hash} failed: {e:?}"),
                EncodedItem::Parent(_) | EncodedItem::Size(_) | EncodedItem::Done => {}
            }
        }
        let rebuilt = p_hasher.finalize();
        for rec in enc.finish() {
            c_tx.write_all(&rec).await?;
        }
        c_tx.shutdown().await?;
        let _ = rebuilt_tx.send(rebuilt);
        Ok::<(), eyre::Report>(())
    };

    // Consumer: incremental bao hashing over the ciphertext stream. Purely
    // synchronous (bao_tree's streaming outboard); bridged off the async
    // producer through the duplex pipe. Peak memory: chunk-group buffer +
    // the outboard bytes (~1/512 of the ciphertext).
    let consumer = tokio::task::spawn_blocking(move || {
        let mut reader = tokio_util::io::SyncIoBridge::new(c_rx);
        let mut ob = bao_tree::io::outboard::PreOrderMemOutboard::<Vec<u8>> {
            root: blake3::hash(&[]), // placeholder, set below
            tree,
            data: vec![0u8; outboard_len],
        };
        bao_sync_outboard(&mut reader, tree, &mut ob).map(|root| (root, ob))
    });

    let (producer_res, consumer_res) = tokio::join!(producer, async { consumer.await });
    producer_res?;
    let (c_root, mut ob) = consumer_res.expect("outboard task panicked")?;
    let rebuilt = rebuilt_rx
        .await
        .expect("encrypt producer completed without sending the digest");
    eyre::ensure!(
        Hash::from(rebuilt) == p_hash,
        "stored plaintext {p_hash} changed under us: re-read digest {rebuilt:?} differs"
    );

    ob.root = c_root;
    let c_hash = Hash::from(c_root);
    store
        .blobs()
        .add_virtual_with_outboard(c_hash, c_len, ob.data, PROVIDER_NAME)
        .await?;
    Ok(c_hash)
}

/// Convert a tokio receiver into a futures stream.
fn mpsc_into_stream(
    rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
) -> impl futures::Stream<Item = std::io::Result<bytes::Bytes>> {
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

/// Feed verified bao items through the decryptor into the importer's byte
/// channel. Owns `p_tx`: when this future ends (or errors out) the channel
/// closes, which is what tells the streaming import that the plaintext is
/// complete. The sender must not outlive this future - a lingering sender
/// would keep the import waiting for bytes forever.
///
/// `resume_from` makes this a resumed consume: it is the absolute ciphertext
/// byte offset where the decrypted output should continue (the start of the
/// first not-yet-spilled record). Bytes of the first received leaf before
/// that offset (the chunk-range floor) are dropped. Parents are ignored - a
/// resumed download re-installs its virtual entry by re-encrypting the
/// completed plaintext, never from received outboard fragments.
///
/// Returns the number of leaf items seen. Zero leaves with a resumed
/// decryptor means the previous attempt had already spilled the final
/// record: the request range started past the blob end, so the spill is
/// already complete and `finish` must not be attempted (the final record
/// will never arrive a second time).
async fn consume_decrypted(
    mut item_rx: irpc::channel::mpsc::Receiver<bao_tree::io::BaoContentItem>,
    dec: &mut StreamDecryptor,
    p_tx: tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>,
    mut resume_from: Option<u64>,
    ledger_consume: Option<LedgerConsume<'_>>,
) -> Res<u64> {
    let mut leaves = 0u64;
    let was_resumed = resume_from.is_some();
    let mut meta_written = false;
    while let Some(item) = item_rx.recv().await.ok().flatten() {
        match item {
            bao_tree::io::BaoContentItem::Leaf(leaf) => {
                let mut data = &leaf.data[..];
                if let Some(target) = resume_from {
                    // Only the first leaf starts before the resume point.
                    let floor = leaf.offset;
                    eyre::ensure!(
                        floor <= target && target <= floor + leaf.data.len() as u64,
                        "resume point {} not covered by first leaf at {} (+{} bytes)",
                        target,
                        floor,
                        leaf.data.len()
                    );
                    data = &data[(target - floor) as usize..];
                    resume_from = None;
                }
                if data.is_empty() {
                    continue;
                }
                leaves += 1;
                dec.push(data)?;
                // Persist the header facts on the first decrypted record so a
                // crash from this point onward is resumable with the spill.
                // (Only fresh decoders: a resumed attempt's attempt-1 meta is
                // already on disk and unchanged.)
                if !meta_written && !was_resumed {
                    meta_written = true;
                    if let Some(lc) = &ledger_consume {
                        let facts = dec
                            .header_facts()
                            .expect("header parsed before record data decrypts");
                        lc.ledger.write_meta(&lc.c_hash, facts, lc.padding)?;
                    }
                }
                for payload in dec.drain_outbox() {
                    p_tx.send(Ok(bytes::Bytes::from(payload)))
                        .await
                        .expect("import receiver dropped before fetch ended");
                }
            }
            bao_tree::io::BaoContentItem::Parent(parent) => {
                dec.outboard.extend_from_slice(parent.pair.0.as_bytes());
                dec.outboard.extend_from_slice(parent.pair.1.as_bytes());
            }
            #[allow(unreachable_patterns)]
            other => eyre::bail!("unexpected bao content item {other:?}"),
        }
    }
    if leaves == 0 && dec.ciphertext_len == 0 {
        // Resumed suffix past the blob end: nothing arrives, the spill
        // already holds the final record.
        return Ok(0);
    }
    dec.finish()?;
    for payload in dec.drain_outbox() {
        p_tx.send(Ok(bytes::Bytes::from(payload)))
            .await
            .expect("import receiver dropped before fetch ended");
    }
    Ok(leaves)
}

/// Durable backing for resumable encrypted downloads (see [`download_encrypted`]):
/// one directory per ciphertext holding the decryption header facts (`meta`,
/// fixed 27-byte layout) and the decrypted plaintext prefix (`spill`). Lives
/// outside the blob store on purpose - the spill is torn down as soon as the
/// plaintext import completes, and an interrupted download simply leaves the
/// directory behind for the next attempt. Progress lives in the spill length
/// itself (plaintext arrives in whole records until the final one), so no
/// separate watermark has to be kept crash-consistent.
pub struct FsDownloadLedger {
    root: std::path::PathBuf,
}

const LEDGER_META: [u8; 5] = *b"dcbr\x01";
const LEDGER_META_LEN: usize = 27;

impl FsDownloadLedger {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn dir_for(&self, c: &Hash) -> std::path::PathBuf {
        self.root
            .join(data_encoding::BASE64URL_NOPAD.encode(c.as_bytes()))
    }

    pub(crate) fn spill_path(&self, c: &Hash) -> std::path::PathBuf {
        self.dir_for(c).join("spill.bin")
    }

    fn meta_path(&self, c: &Hash) -> std::path::PathBuf {
        self.dir_for(c).join("meta.bin")
    }

    /// Remove all ledger state for `c` (no-op when nothing is recorded).
    pub fn clear(&self, c: &Hash) -> Res<()> {
        match std::fs::remove_dir_all(self.dir_for(c)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist the header facts + padding of an in-flight download. Written
    /// once, right after the first attempt parsed the header; the resume
    /// point itself is not stored - it is recovered from the spill length.
    fn write_meta(&self, c: &Hash, facts: HeaderFacts, padding: Padding) -> Res<()> {
        let dir = self.dir_for(c);
        std::fs::create_dir_all(&dir)?;
        let mut buf = Vec::with_capacity(LEDGER_META_LEN);
        buf.extend_from_slice(&LEDGER_META);
        buf.extend_from_slice(&facts.salt);
        buf.extend_from_slice(&facts.rs.to_be_bytes());
        buf.push(facts.idlen);
        buf.push(match padding {
            Padding::Minimal => 0,
            Padding::Record => 1,
        });
        eyre::ensure!(buf.len() == LEDGER_META_LEN, "meta layout drifted");
        let path = self.meta_path(c);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    fn read_meta(&self, c: &Hash) -> Res<Option<(HeaderFacts, Padding)>> {
        let bytes = match std::fs::read(self.meta_path(c)) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        eyre::ensure!(
            bytes.len() == LEDGER_META_LEN,
            "corrupt download ledger meta"
        );
        eyre::ensure!(
            &bytes[..LEDGER_META.len()] == &LEDGER_META[..],
            "foreign download ledger meta"
        );
        let salt = bytes[5..21].try_into().expect("16 salt octets");
        let rs = u32::from_be_bytes(bytes[21..25].try_into().unwrap());
        let idlen = bytes[25];
        let padding = match bytes[26] {
            0 => Padding::Minimal,
            1 => Padding::Record,
            other => eyre::bail!("unknown padding tag {other}"),
        };
        Ok(Some((HeaderFacts { salt, rs, idlen }, padding)))
    }
}

/// What a resumed consume needs beyond the decryptor: the spill file (opened
/// at the progress watermark) and where to persist the header facts on the
/// first decrypted record.
struct LedgerConsume<'a> {
    ledger: &'a FsDownloadLedger,
    c_hash: Hash,
    padding: Padding,
}

/// Download a remote ciphertext without storing it: verified leaves stream
/// through decryption straight into a streaming plaintext import (no whole-
/// blob buffering); received parents become `C`'s outboard so this node can
/// re-serve `C` virtually. Resolves the master key through `keys`, then roots
/// the plaintext and virtual entry (see [`set_pair_tags`]); returns the
/// plaintext hash.
///
/// With `ledger` set the download is *resumable* and critical for mobile:
/// decrypted records are appended to a spill file as they are verified, so an
/// interrupted attempt leaves the completed prefix on disk. The next call
/// re-derives the progress watermark from the spill length, requests only the
/// missing ciphertext record suffix ([`ChunkRanges`]-ranged fetch - the
/// provider serves any window), and imports the spill once the full
/// plaintext has landed. The virtual entry of a resumed download is
/// installed by re-encrypting the completed plaintext - deterministic in
/// (key, padding) - because a ranged transfer only carries the outboard
/// fragments that verify its requested suffix and cannot be reassembled by
/// concatenation the way a full transfer's pre-order stream can (ADR 003
/// §12). NOTE for future profiling: this means a resumed download pays one
/// extra local sequential read of `P` at completion (re-deriving `C`'s
/// outboard) on top of the plaintext import. That is a deliberate trade
/// against a second scratch file; if it ever matters, persist received
/// `(TreeNode, pair)` fragments in the ledger and scatter-merge them via
/// `BaoTree::pre_order_offset` instead.
/// spill is deleted once the plaintext is durably imported and tagged.
pub async fn download_encrypted(
    store: &Store,
    provider: &CipherBlobProvider,
    conn: impl GetStreamPair,
    c_hash: Hash,
    keys: &dyn CipherKeySource,
    ledger: Option<&FsDownloadLedger>,
) -> Res<Hash> {
    use bao_tree::io::BaoContentItem;
    use std::io::SeekFrom;
    use tokio::io::AsyncSeekExt;

    let key = keys.key_for(&c_hash).await?;

    // Recover resumable state. Every ledger-mode download spills (the point
    // of the ledger is surviving mid-flight crashes), so a spill file is
    // always opened here; the prior attempt's meta - when present - carries
    // the decryption facts and the progress watermark. `seq` is the record
    // watermark implied by the spill length.
    let prior = match ledger {
        None => None,
        Some(l) => {
            let path = l.spill_path(&c_hash);
            std::fs::create_dir_all(path.parent().expect("spill has a parent"))?;
            Some((l, path, l.read_meta(&c_hash)?))
        }
    };
    let (spill, encoding) = match prior {
        // No ledger: the facet is the authority on how C is framed. It matters
        // even here, because registering the pair makes this node a server of
        // C, and the pair frames what it serves.
        None => (None, keys.encoding_for(&c_hash).await?),
        Some((_, path, None)) => {
            // Fresh attempt under a ledger: start an empty spill.
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await?;
            let encoding = keys.encoding_for(&c_hash).await?;
            (Some((file, None, 0u64)), encoding)
        }
        Some((_, path, Some((facts, padding)))) => {
            let len = tokio::fs::metadata(&path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            // A crash can leave a torn final record: resume from whole
            // records only. A complete short final record under
            // `Padding::Minimal` hits the same path - it is re-fetched
            // and re-spilled.
            let payload = facts.payload_max();
            let seq = len / payload;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .await?;
            file.set_len(seq * payload).await?;
            file.seek(SeekFrom::Start(seq * payload)).await?;
            // Reproduce the ciphertext that was actually downloaded: its record
            // size comes from its own authenticated header, and only the
            // padding policy - which the wire does not carry - comes from the
            // facet.
            let encoding = EncodingParams {
                record_size: u64::from(facts.rs),
                padding,
            };
            (Some((file, Some(facts), seq)), encoding)
        }
    };
    // seq == 0 (no meta at all, or meta with an empty spill) means the
    // header must be parsed from the stream again: a fully fresh decryptor,
    // full range request.
    let seq = spill.as_ref().map_or(0, |(_, _, seq)| *seq);
    let fresh_decoder = seq == 0;

    // Chunk ranges are 1 KiB blake3-chunk units: request from the start of
    // the chunk containing the first missing record, open-ended.
    let first_byte = spill
        .as_ref()
        .and_then(|(_, facts, seq)| facts.as_ref().map(|f| f.record_start(*seq)))
        .unwrap_or(0);
    let ranges = if fresh_decoder {
        bao_tree::ChunkRanges::all()
    } else {
        const CHUNK_BYTES: u64 = 1024; // blake3 chunk = range unit
        let start_chunk = first_byte / CHUNK_BYTES;
        bao_tree::ChunkRanges::from(bao_tree::ChunkNum(start_chunk)..)
    };

    let mut dec = if fresh_decoder {
        StreamDecryptor::new(key.clone())
    } else {
        let (_, facts, seq) = spill.as_ref().expect("seq > 0 implies a prior attempt");
        StreamDecryptor::resumed(key.clone(), facts.expect("seq > 0 implies meta"), *seq)
    };

    let (item_tx, item_rx) = irpc::channel::mpsc::channel::<BaoContentItem>(64);
    let fetch = store.remote().fetch_bao_to(conn, c_hash, ranges, item_tx);

    // Decrypted records stream into the spill/importer; the bounded channel
    // applies backpressure to the fetch whenever the sink falls behind.
    let (p_tx, p_rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(16);
    let ledger_consume = ledger.map(|l| LedgerConsume {
        ledger: l,
        c_hash,
        padding: encoding.padding,
    });
    let resume_from = if fresh_decoder {
        None
    } else {
        Some(first_byte)
    };
    let consume = consume_decrypted(item_rx, &mut dec, p_tx, resume_from, ledger_consume);

    // fetch, consume, and sink must all run concurrently: the fetch stalls
    // once its item channel fills, and the sink can only finish once the
    // consume future (which owns the plaintext channel's sender) ends.
    let ledger_mode = ledger.is_some();
    let (p_tag, ciphertext_len, outboard) = match spill {
        Some((file, _, _)) => {
            let (fetch_res, consume_res, spill_res) =
                tokio::join!(fetch, consume, spill_writer(p_rx, file));
            fetch_res.map_err(|e| eyre::eyre!("fetch_bao_to failed: {e:?}"))?;
            consume_res?;
            spill_res?;
            // Crash-during-a-prior-attempt can mean this suffix request lands
            // past the blob end (the final record was already spilled): zero
            // leaves means the spill is already the complete plaintext. Either
            // way the plaintext is complete here: import the spill.
            let tag = import_file_to_tag(
                store,
                &ledger
                    .expect("ledger set with resumable spill")
                    .spill_path(&c_hash),
            )
            .await?;
            (tag, 0, Vec::new())
        }
        None => {
            let (fetch_res, consume_res, tag_res) = tokio::join!(
                fetch,
                consume,
                import_stream_to_tag(store, mpsc_into_stream(p_rx))
            );
            fetch_res.map_err(|e| eyre::eyre!("fetch_bao_to failed: {e:?}"))?;
            consume_res?;
            let tag = tag_res?;
            let len = dec.ciphertext_len;
            (tag, len, std::mem::take(&mut dec.outboard))
        }
    };
    let p_hash = p_tag.hash();

    // Install the virtual entry and register the pair. Installing and
    // registering are one act; registering is also what roots both entries
    // under the `ct:`/`pt:` tags.
    if !ledger_mode {
        // Fresh: the full transfer delivers parents in pre-order, so the
        // received appends are already C's outboard; install it directly and
        // register against the plaintext that was just imported. A node that
        // can decrypt a representation can serve it.
        store
            .blobs()
            .add_virtual_with_outboard(c_hash, ciphertext_len, outboard, PROVIDER_NAME)
            .await?;
        provider
            .register_pair(store, c_hash, &key, p_hash, encoding)
            .await?;
    } else {
        // Resume: re-derive C deterministically (see the fn doc + ADR 003 §12
        // for why the received fragments are not reused).
        let c2 = provider.install(store, &key, p_hash, encoding).await?;
        eyre::ensure!(
            c2 == c_hash,
            "re-encrypted ciphertext {c2} differs from the downloaded {c_hash}: corrupt spill?"
        );
    }

    if let Some(l) = ledger {
        l.clear(&c_hash)?;
    }
    drop(p_tag); // named tags now root both entries
    Ok(p_hash)
}

/// Append decrypted plaintext records to the spill file. Owns the plaintext
/// channel receiver; when the consume future ends the channel closes and
/// this future finishes, leaving the file positioned after the last
/// complete record.
async fn spill_writer(
    mut p_rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    mut file: tokio::fs::File,
) -> Res<()> {
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = p_rx.recv().await {
        file.write_all(&chunk?).await?;
    }
    Ok(())
}

/// Stream a spill file into a fresh store entry, verifying nothing (the
/// bytes were authenticated record-by-record during the download).
async fn import_file_to_tag(
    store: &Store,
    path: &std::path::Path,
) -> Res<iroh_blobs::api::TempTag> {
    use tokio::io::AsyncReadExt;
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(16);
    let mut file = tokio::fs::File::open(path).await?;
    let reader = async move {
        loop {
            let mut buf = bytes::BytesMut::zeroed(64 * 1024);
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if tx.send(Ok(buf.split_to(n).freeze())).await.is_err() {
                break; // import side gone; its result surfaces the cause
            }
        }
        eyre::Ok(())
    };
    let handle = tokio::spawn(async move { reader.await });
    let tag = import_stream_to_tag(store, mpsc_into_stream(rx)).await?;
    handle
        .await
        .expect("spill reader task must not panic")
        .expect("spill reader must not fail on a verified spill");
    Ok(tag)
}

/// Run a streaming import to completion, returning the temp tag of the entry.
async fn import_stream_to_tag(
    store: &Store,
    data: impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static,
) -> Res<iroh_blobs::api::TempTag> {
    add_progress_to_tag(store.blobs().add_stream(data).await).await
}

/// Fetch-and-decrypt a ciphertext verifiable on this node (stored, or virtual
/// with a live provider registered).
pub async fn get_decrypted(store: &Store, keys: &dyn CipherKeySource, c: Hash) -> Res<Vec<u8>> {
    let key = keys.key_for(&c).await?;
    // Virtual entries are only served through export_bao; its Leaf items are
    // raw ciphertext bytes.
    let stream = store
        .blobs()
        .export_bao(c, bao_tree::ChunkRanges::all())
        .stream();
    futures::pin_mut!(stream);
    let mut dec = StreamDecryptor::new(key);
    while let Some(item) = stream.next().await {
        match item {
            bao_tree::io::mixed::EncodedItem::Leaf(leaf) => dec.push(&leaf.data)?,
            bao_tree::io::mixed::EncodedItem::Parent(_)
            | bao_tree::io::mixed::EncodedItem::Size(_) => {}
            bao_tree::io::mixed::EncodedItem::Done => break,
            bao_tree::io::mixed::EncodedItem::Error(e) => {
                eyre::bail!("export_bao failed: {e:?}")
            }
        }
    }
    dec.finish()?;
    Ok(dec.into_plaintext())
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// Serves virtual ciphertext entries by encrypting stored plaintext on demand.
///
/// `reader_for(C)` looks up a pair `(key, plaintext reader)` registered via
/// [`CipherBlobProvider::register_pair`] and returns a random-access source
/// that encrypts only the records overlapping each requested byte window.
///
/// The plaintext is not held in memory: the pair reads it back through a
/// synchronous reader over the store's own storage for `P`, so serving `C`
/// costs one record of memory no matter how large `P` is. Serving is therefore
/// only as durable as `P` is - see [`register_pair`] for the lifecycle
/// requirement.
///
/// [`register_pair`]: CipherBlobProvider::register_pair
pub struct CipherBlobProvider {
    pairs: RwLock<HashMap<Hash, Arc<PlainPair>>>,
}

/// A registered `C -> P` binding with everything a window read needs derived
/// once, at registration: the record cipher, the wire header, the plaintext
/// reader, and the framing facts. The crypto state is a pure function of
/// `(key, plaintext digest, padding)`, and deriving it per window would make
/// serving cost a whole-plaintext hash plus HKDF per requested window -
/// thousands of windows for a streamed video.
struct PlainPair {
    cipher: Cipher,
    header: [u8; HEADER_LEN],
    /// The plaintext side of the binding, kept for diagnostics: the map is
    /// keyed by `C`, so a failed read would otherwise not say which plaintext
    /// went missing.
    p_hash: Hash,
    /// Synchronous random-access reads over the stored plaintext. Never
    /// awaited: serving runs inside the store's own export path, which is
    /// already calling into us.
    reader: SyncReader,
    /// Plaintext length as the reader reports it. Every framing fact below is
    /// derived from it, so they cannot disagree with what we can read.
    p_len: u64,
    /// The framing this pair serves. `rs` is not a constant here: a
    /// representation built with a different record size is a different wire
    /// format, and serving it at the wrong stride would produce bytes that
    /// cannot verify against the outboard the entry was installed with.
    encoding: EncodingParams,
    payload_max: u64,
    n_records: u64,
    total_c_len: u64,
    /// Most recently encrypted record, as `(index, ciphertext)`. See
    /// [`PlainPair::record`] for why one entry is the whole win.
    cache: Mutex<Option<(u64, Bytes)>>,
    /// Test-only: how many records this pair has encrypted. Serving is a pure
    /// function of the stored bytes, so this is the whole cost of serving and
    /// a test can pin it (see `record_cache_avoids_re_encrypting`).
    #[cfg(test)]
    encryptions: AtomicU64,
}

impl PlainPair {
    fn new(key: &MasterKey, p_hash: Hash, reader: SyncReader, encoding: EncodingParams) -> Self {
        let salt = key.salt_for(&p_hash);
        let payload_max = payload_size(encoding.record_size);
        let p_len = reader.len();
        Self {
            cipher: key.cipher_for(&p_hash),
            header: header(&salt, encoding.record_size),
            p_hash,
            reader,
            p_len,
            encoding,
            payload_max,
            n_records: n_records_for(p_len, payload_max),
            total_c_len: ciphertext_len(p_len, encoding),
            cache: Mutex::new(None),
            #[cfg(test)]
            encryptions: AtomicU64::new(0),
        }
    }

    /// The ciphertext of record `idx`, encrypted on first use and kept for
    /// the reads that follow it.
    ///
    /// Records are `rs` octets of ciphertext (64 KiB); the store's bao leaves
    /// are 16 KiB, so four leaf reads out of five land in the record just
    /// encrypted, and one in four straddles into the next. Without this cache
    /// every leaf read re-encrypts each record it overlaps *and* re-reads its
    /// plaintext: serving is then 4x the encryption work and 4x the plaintext
    /// I/O for the same window. One entry is enough because reads arrive in
    /// offset order - the record that just went cold is the one the next read
    /// wants.
    ///
    /// Encryption runs outside the lock, so concurrent readers cannot
    /// serialize on it. Two readers missing the same record both compute it;
    /// RFC 8188 encryption is deterministic, so they agree, and the loser's
    /// work is wasted but never wrong.
    fn record(&self, idx: u64) -> io::Result<Bytes> {
        if let Some((cached, ct)) = self.cache.lock().expect("cache lock poisoned").as_ref() {
            if *cached == idx {
                return Ok(ct.clone());
            }
        }
        #[cfg(test)]
        self.encryptions.fetch_add(1, Ordering::Relaxed);
        let ct = Bytes::from(self.encrypt_record(idx)?);
        *self.cache.lock().expect("cache lock poisoned") = Some((idx, ct.clone()));
        Ok(ct)
    }

    /// Encrypt a single record straight from the stored plaintext.
    fn encrypt_record(&self, idx: u64) -> io::Result<Vec<u8>> {
        let pt_start = idx * self.payload_max;
        let pt_end = (pt_start + self.payload_max).min(self.p_len);
        let want = (pt_end - pt_start) as usize;
        // Read exactly the record's plaintext: only the records overlapping
        // the requested window are ever touched, so this is bounded by the
        // window, not by the size of `P`.
        let plain = self.reader.read_bytes_at(pt_start, want)?;
        // A short read means the plaintext is no longer fully readable
        // (collected, truncated, or swapped out from under us). Encrypting a
        // short record would produce ciphertext that cannot verify against the
        // outboard the virtual entry was installed with, so fail instead.
        if plain.len() != want {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "plaintext {} unreadable at {pt_start}: {} of {want} octets",
                    self.p_hash,
                    plain.len()
                ),
            ));
        }
        let last = idx == self.n_records - 1;
        let pad_zeros = if last {
            match self.encoding.padding {
                Padding::Minimal => 0,
                Padding::Record => (self.payload_max - plain.len() as u64) as usize,
            }
        } else {
            0
        };
        Ok(self
            .cipher
            .encrypt_record(idx, last, plain.to_vec(), pad_zeros))
    }
}

impl CipherBlobProvider {
    pub fn new() -> Self {
        Self {
            pairs: RwLock::new(HashMap::new()),
        }
    }

    /// Register the binding for ciphertext `c`: key + the plaintext it was
    /// derived from + the padding policy the ciphertext was framed with.
    ///
    /// `p_hash` must be the digest the store holds those plaintext bytes
    /// under, because it is what the reader is resolved from. Passing it in
    /// rather than hashing the plaintext here is deliberate: in production
    /// `p_hash` is the entry's own identity, so registering a 2 GB plaintext
    /// costs no read at all.
    ///
    /// Fails if `P` has no readable stored data - absent, incomplete, or a
    /// virtual entry that keeps an outboard but no bytes.
    ///
    /// Registering is what it means to serve `c` at all, so this also roots
    /// both entries under the `ct:`/`pt:` named tags (see [`set_pair_tags`]):
    /// a pair whose plaintext has been collected is not a pair. Installing and
    /// registering are therefore one act - see [`CipherBlobProvider::install`].
    ///
    /// Lifecycle: the reader reads the store's storage for `P`, so the pair is
    /// only meaningful while that entry is alive. The named tags set here root
    /// it durably, but a caller who only holds a `TempTag` must keep holding it
    /// until this returns. If `P` is collected anyway, reads fail rather than
    /// serving stale bytes; on Unix a duplicated file handle keeps the inode
    /// readable, so there the pair keeps working while holding the only
    /// reference to the bytes.
    ///
    /// A provider serves one store's storage: register pairs for the store the
    /// provider was created against.
    pub async fn register_pair(
        &self,
        store: &Store,
        c: Hash,
        key: &MasterKey,
        p_hash: Hash,
        encoding: EncodingParams,
    ) -> Res<()> {
        let Some(reader) = store.sync_reader(p_hash).await? else {
            eyre::bail!("cannot serve {c}: plaintext {p_hash} has no readable stored data");
        };
        set_pair_tags(store, c, p_hash).await?;
        let pair = Arc::new(PlainPair::new(key, p_hash, reader, encoding));
        self.pairs
            .write()
            .expect("pairs lock poisoned")
            .insert(c, pair);
        Ok(())
    }

    /// Make `c` servable in one step: run the §11 pass over the stored
    /// plaintext (`install_virtual_encrypted`), then register the resulting
    /// pair, which roots `C` and `P`.
    ///
    /// Returns the computed representation digest, a pure function of
    /// `(key, p_hash, padding)`.
    ///
    /// This is the only way a caller should install an encrypted
    /// representation: installing without registering leaves a virtual entry
    /// that names [`PROVIDER_NAME`] while the provider holds no data for it,
    /// so the store serves it as not found.
    pub async fn install(
        &self,
        store: &Store,
        key: &MasterKey,
        p_hash: Hash,
        encoding: EncodingParams,
    ) -> Res<Hash> {
        let c = install_virtual_encrypted(store, key, p_hash, encoding).await?;
        self.register_pair(store, c, key, p_hash, encoding).await?;
        Ok(c)
    }

    /// Register this provider under [`PROVIDER_NAME`] on a live registry.
    pub fn register(
        self: &Arc<Self>,
        virtuals: &iroh_blobs::store::virtual_blob::VirtualProviders,
    ) -> std::io::Result<()> {
        virtuals.register(PROVIDER_NAME, self.clone())
    }
}

impl Default for CipherBlobProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl iroh_blobs::store::virtual_blob::Provider for CipherBlobProvider {
    fn reader_for(&self, hash: &Hash) -> Option<iroh_blobs::store::virtual_blob::DynVirtualSource> {
        let pairs = self.pairs.read().expect("pairs lock poisoned");
        let pair = pairs.get(hash)?;
        Some(Arc::new(CipherSource { pair: pair.clone() }))
    }
}

/// Random-access ciphertext view over one registered pair. Purely
/// synchronous: RFC 8188 records are independently computable given
/// key + salt + record index, so only overlapping records are encrypted.
/// The pair's derived state is shared, not copied: building a reader is one
/// refcount bump. The plaintext for those records is read from the pair's
/// reader as it is needed, so no plaintext is held here either.
struct CipherSource {
    pair: Arc<PlainPair>,
}

impl CipherSource {
    fn read_window(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        let pair = &self.pair;
        let end = offset + size as u64;
        let mut pos = offset;
        let mut out = Vec::with_capacity(size);

        // RFC 8188 header precedes the record stream.
        if pos < HEADER_LEN as u64 {
            let take = ((HEADER_LEN as u64 - pos) as usize).min(size);
            out.extend_from_slice(&pair.header[pos as usize..pos as usize + take]);
            pos += take as u64;
        }
        if pos >= end {
            return Ok(out.into());
        }

        // Under `Padding::Record` every record is a full `rs` frame; under
        // `Padding::Minimal` the final frame ends after its delimiter.
        let wire = pair.encoding.record_size;
        let n_records = pair.n_records;
        let end = end.min(pair.total_c_len);

        if pos < end {
            // At this point `pos` is at or past the header: if the window began
            // inside it, the header branch above either consumed the rest of
            // the window or moved `pos` to exactly `HEADER_LEN`.
            let rel = pos - HEADER_LEN as u64;
            let idx = rel / wire;
            let within = (rel % wire) as usize;
            let want = (end - pos) as usize;
            // Fast path: the window is an interior slice of a single record,
            // and the header contributed nothing to it. This is the common case
            // for bao leaf reads, and it is served as a slice of the cached
            // record: no encryption, no copy, no allocation. A window that
            // began inside the header already holds those octets in `out`, so
            // it must go the general way.
            if out.is_empty() && idx < n_records {
                let rec_ct = pair.record(idx)?;
                if within + want <= rec_ct.len() {
                    return Ok(rec_ct.slice(within..within + want));
                }
            }
            // General path: the window crosses at least one record boundary,
            // so its pieces are gathered into one buffer.
            while pos < end {
                let rel = pos - HEADER_LEN as u64;
                let idx = rel / wire;
                let within = (rel % wire) as usize;
                let rec_ct = pair.record(idx)?;
                let avail = rec_ct.len().saturating_sub(within);
                // `pos < end <= total_c_len` and the record covers `pos`, so
                // there is always at least one octet to take.
                let take = avail.min((end - pos) as usize);
                debug_assert!(take > 0, "record {idx} does not cover {pos}");
                out.extend_from_slice(&rec_ct[within..within + take]);
                pos += take as u64;
            }
        }
        Ok(out.into())
    }
}

impl bao_tree::io::mixed::ReadBytesAt for CipherSource {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        self.read_window(offset, size)
    }
}

// ---------------------------------------------------------------------------
// Windowed decryption
// ---------------------------------------------------------------------------

/// Random-access *plaintext* reads over a stored ciphertext: the inverse of
/// [`CipherSource`], which serves a ciphertext derived from plaintext.
///
/// RFC 8188 derives each record's nonce from its sequence number and
/// authenticates it on its own, so decryption is random-access: a read touches
/// only the records its range overlaps. That is what lets a multi-gigabyte
/// encrypted video start playing without decrypting, or even reading, the whole
/// thing, and it bounds memory by the requested range rather than by the file.
///
/// Framing comes from the ciphertext's own header - `rs` is authenticated
/// there - so this never needs the facet's `encodingParameters`. The plaintext
/// length is the exception: RFC 8188 does not encode it, and under
/// `Padding::Record` the padded tail deliberately hides it (ADR 003 §17), so
/// the caller supplies it from its own metadata (`Blob.lengthOctets`). What
/// comes off the wire is checked against that metadata in both directions,
/// because a wrong length would otherwise show up as a silently truncated file.
pub struct CipherReader {
    /// Synchronous random-access reads over the stored ciphertext.
    reader: SyncReader,
    cipher: Cipher,
    /// Where the record stream begins: the header plus any foreign key id.
    records_start: u64,
    /// Record stride, from the authenticated header.
    rs: u64,
    payload_max: u64,
    n_records: u64,
    p_len: u64,
    /// Diagnostics only: a failing read should say which ciphertext it was.
    c_hash: Hash,
}

impl CipherReader {
    /// Open a reader over the stored ciphertext `c`, which the caller's
    /// metadata says decrypts to `p_len` plaintext octets.
    ///
    /// Fails if `c` has no readable local data, or if that data is too short to
    /// hold `p_len` octets under its own framing.
    pub async fn open(store: &Store, keys: &dyn CipherKeySource, c: Hash, p_len: u64) -> Res<Self> {
        let key = keys.key_for(&c).await?;
        let Some(reader) = store.sync_reader(c).await? else {
            eyre::bail!("cannot read {c}: no readable stored data");
        };
        eyre::ensure!(
            reader.len() >= HEADER_LEN as u64,
            "ciphertext {c} is shorter than an RFC 8188 header"
        );
        let head = reader.read_bytes_at(0, HEADER_LEN)?;
        let salt: [u8; SALT_LEN] = head[..SALT_LEN].try_into()?;
        let rs = u64::from(u32::from_be_bytes(head[SALT_LEN..SALT_LEN + 4].try_into()?));
        // Foreign encoders may carry a key id; we never encode one, but the
        // record stream starts after it.
        let records_start = HEADER_LEN as u64 + u64::from(head[HEADER_LEN - 1]);
        eyre::ensure!(
            reader.len() > records_start,
            "ciphertext {c} carries no records"
        );

        let payload_max = payload_size(rs);
        let n_records = n_records_for(p_len, payload_max);
        // The shortest body that could hold `p_len` octets: every non-final
        // record is exactly `rs` wire octets, and the final one carries at least
        // its payload and delimiter. Padding only makes a body longer, so this
        // is a true lower bound, and it is checked against the *wire* rather
        // than against the metadata that produced it.
        let shortest = records_start
            + (n_records - 1) * rs
            + (p_len - (n_records - 1) * payload_max)
            + RECORD_OVERHEAD as u64;
        eyre::ensure!(
            reader.len() >= shortest,
            "ciphertext {c} holds {} octets, too few for {p_len} plaintext octets",
            reader.len()
        );

        Ok(Self {
            reader,
            cipher: cipher_from_ikm(&key.0, &salt),
            records_start,
            rs,
            payload_max,
            n_records,
            p_len,
            c_hash: c,
        })
    }

    /// Plaintext octets this reader reports, as given to [`CipherReader::open`].
    pub fn plaintext_len(&self) -> u64 {
        self.p_len
    }

    /// Read up to `size` plaintext octets at `offset`, clamped at the end of the
    /// plaintext like a file read at EOF.
    pub fn read_plaintext_at(&self, offset: u64, size: usize) -> Res<Bytes> {
        let end = (offset + size as u64).min(self.p_len);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let (first_seq, last_seq) = (offset / self.payload_max, (end - 1) / self.payload_max);
        let mut out = Vec::with_capacity((end - offset) as usize);
        for seq in first_seq..=last_seq {
            let pt_start = seq * self.payload_max;
            let ct_start = self.records_start + seq * self.rs;
            // Non-final records are exactly `rs` wire octets; the final one may
            // be shorter under `Padding::Minimal`, so take what is there and let
            // the AEAD reject a record that was truncated by a bad `p_len`.
            let take = self.rs.min(self.reader.len() - ct_start) as usize;
            let ct = self.reader.read_bytes_at(ct_start, take)?;
            let (payload, is_final) = self.cipher.decrypt_record(seq, &ct)?;
            eyre::ensure!(
                is_final == (seq == self.n_records - 1),
                "record {seq} of {} contradicts the plaintext length metadata",
                self.c_hash
            );
            if is_final {
                // The final record's payload *is* the plaintext tail, so it is
                // the one place the wire states the plaintext length. Comparing
                // it against the caller's metadata is the only check that
                // catches a length that is too small - `open` can only bound it
                // from the other side.
                let expected = self.p_len - (self.n_records - 1) * self.payload_max;
                eyre::ensure!(
                    payload.len() as u64 == expected,
                    "final record of {} holds {} plaintext octets, metadata claims {expected}",
                    self.c_hash,
                    payload.len()
                );
            }
            let from = (offset.max(pt_start) - pt_start) as usize;
            let to = (end.min(pt_start + self.payload_max) - pt_start) as usize;
            eyre::ensure!(
                to <= payload.len(),
                "record {seq} of {} carried {} plaintext octets, expected {to}",
                self.c_hash,
                payload.len()
            );
            out.extend_from_slice(&payload[from..to]);
        }
        Ok(out.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMALL_RS: u64 = 1024;
    /// The store's bao leaf size: the granularity its export path reads the
    /// ciphertext at, and therefore the reads the server actually issues.
    /// `iroh_blobs::store::IROH_BLOCK_SIZE` is chunk-log 4, so 2^14 octets.
    const BAO_LEAF_SIZE: usize = 16 * 1024;

    /// The codec's framing *is* the facet's `encodingParameters`: one type is
    /// both, so the two cannot drift. Pinned in both directions, plus the
    /// rejections that make a peer's bad facet loud instead of creative.
    #[test]
    fn encoding_parameters_match_the_facet_shape() {
        // The exact shape ADR 003 §3 documents.
        assert_eq!(
            EncodingParams::DEFAULT.to_encoding_parameters(),
            serde_json::json!({"recordSize": 65536, "padding": "record"})
        );

        let encoding = EncodingParams {
            record_size: 4096,
            padding: Padding::Minimal,
        };
        let json = encoding.to_encoding_parameters();
        assert_eq!(
            json,
            serde_json::json!({"recordSize": 4096, "padding": "minimal"})
        );
        assert_eq!(
            EncodingParams::from_encoding_parameters(CONTENT_ENCODING_AES128GCM, &json).unwrap(),
            encoding
        );

        // Another content coding is not this codec's to interpret...
        assert!(
            EncodingParams::from_encoding_parameters("br", &json).is_err(),
            "an unknown content coding must not be read as aes128gcm"
        );
        // ...and neither is a parameters object that does not fit the scheme.
        for bad in [
            serde_json::json!({"recordSize": 4096}),
            serde_json::json!({"padding": "record"}),
            serde_json::json!({"recordSize": 4096, "padding": "bucketed"}),
            serde_json::json!({"recordSize": 8, "padding": "record"}),
            serde_json::json!({"recordSize": u64::from(u32::MAX) + 1, "padding": "record"}),
            serde_json::json!({"recordSize": 4096, "padding": "record", "extra": 1}),
        ] {
            assert!(
                EncodingParams::from_encoding_parameters(CONTENT_ENCODING_AES128GCM, &bad).is_err(),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn roundtrip_various_sizes() {
        let key = MasterKey::random();
        let chunk_max = (SMALL_RS - RECORD_OVERHEAD as u64) as usize;
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            b"x".to_vec(),
            b"hello cipherblob".to_vec(),
            vec![0xAB; chunk_max],
            vec![0xCD; chunk_max - 1],
            vec![0xEF; 3 * chunk_max + 5],
            vec![0xCD; chunk_max - 1],
            // exact multiples: last full chunk is the final record,
            // no trailing empty record (reference-encoder convention)
            vec![0x99; 2 * chunk_max],
            vec![0xEE; 3 * chunk_max],
            vec![0xEF; 3 * chunk_max + 5],
        ];
        for padding in [Padding::Minimal, Padding::Record] {
            for pt in &cases {
                let ct = encrypt_with_rs(&key, pt, SMALL_RS, padding);
                assert_eq!(decrypt_bytes(&key, &ct).unwrap(), *pt);
            }
        }
    }

    /// Framing for the exact-multiple case: 2 full records with payload_max
    /// payload each; the second (full) record is the final one, with no
    /// trailing empty record. Total wire = header + n * RECORD_SIZE.
    #[test]
    fn exact_multiple_framing() {
        let key = MasterKey::random();
        let chunk_max = (SMALL_RS - RECORD_OVERHEAD as u64) as usize;
        for n_records in [1u64, 2, 3] {
            let pt = vec![0x42u8; n_records as usize * chunk_max];
            let ct = encrypt_with_rs(&key, &pt, SMALL_RS, Padding::Minimal);
            let expected = HEADER_LEN + n_records as usize * SMALL_RS as usize;
            assert_eq!(
                ct.len(),
                expected,
                "exact-multiple length must be header + n full records (n={n_records})"
            );
            assert_eq!(decrypt_bytes(&key, &ct).unwrap(), pt);
        }
        // And the empty case: header + a single minimal (unpadded) final record.
        let ct = encrypt_with_rs(&key, &[], SMALL_RS, Padding::Minimal);
        assert_eq!(ct.len(), HEADER_LEN + RECORD_OVERHEAD);
        assert_eq!(decrypt_bytes(&key, &ct).unwrap(), Vec::<u8>::new());
    }

    /// `Padding::Record`: the tail is padded so every record is a full `rs`
    /// frame - the body length reveals only the record count.
    #[test]
    fn padded_tail_hidden() {
        let key = MasterKey::random();
        let chunk_max = (SMALL_RS - RECORD_OVERHEAD as u64) as usize;
        for n_records in [1u64, 3] {
            for extra in [1usize, chunk_max / 2, chunk_max] {
                let pt_len = (n_records as usize - 1) * chunk_max + extra;
                let pt = vec![0x77u8; pt_len];
                let ct = encrypt_with_rs(&key, &pt, SMALL_RS, Padding::Record);
                assert_eq!(
                    ct.len(),
                    HEADER_LEN + n_records as usize * SMALL_RS as usize,
                    "padded body must be header + n full records (n={n_records}, extra={extra})"
                );
                assert_eq!(decrypt_bytes(&key, &ct).unwrap(), pt);
            }
        }
        // Empty plaintext still occupies one full padded record.
        let ct = encrypt_with_rs(&key, &[], SMALL_RS, Padding::Record);
        assert_eq!(ct.len(), HEADER_LEN + SMALL_RS as usize);
        assert_eq!(decrypt_bytes(&key, &ct).unwrap(), Vec::<u8>::new());
    }

    /// RFC 8188 §3.1 as a cross-implementation fixture: any conforming
    /// encoder must reproduce this byte stream from these inputs, and our
    /// decoder must accept it. Body decoded from the RFC's base64url.
    #[test]
    fn rfc8188_section3_1_roundtrip() {
        let ikm = data_encoding::BASE64URL_NOPAD
            .decode(b"yqdlZ-tYemfogSmv7Ws5PQ")
            .unwrap();
        let body = data_encoding::BASE64URL_NOPAD
            .decode(b"I1BsxtFttlv3u_Oo94xnmwAAEAAA-NAVub2qFgBEuQKRapoZu-IxkIva3MEB1PD-ly8Thjg")
            .unwrap();
        let pt = b"I am the walrus";
        // header: salt 16, rs 4096 (32-bit BE), idlen 0
        assert_eq!(&body[16..20], 4096u32.to_be_bytes().as_slice());
        assert_eq!(&body[SALT_LEN + 4..SALT_LEN + 5], &[0]);
        // decode direction
        assert_eq!(decrypt_bytes_ikm(&ikm, &body).unwrap(), pt);
        // encode direction: byte-identical to the reference stream
        let salt: [u8; SALT_LEN] = body[..SALT_LEN].try_into().unwrap();
        assert_eq!(
            encrypt_raw_ikm(&ikm, &salt, 4096, Padding::Minimal, pt),
            body
        );
    }

    /// RFC 8188 §3.2: multiple records with padding and a foreign keyid -
    /// the decoder must skip the key id and strip interior padding.
    #[test]
    fn rfc8188_section3_2_decode() {
        let ikm = data_encoding::BASE64URL_NOPAD
            .decode(b"BO3ZVPxUlnLORbVGMpbT1Q")
            .unwrap();
        let body = data_encoding::BASE64URL_NOPAD
            .decode(
                b"uNCkWiNYzKTnBN9ji3-qWAAAABkCYTHOG8chz_gnvgOqdGYovxyjuqRyJFjEDyoF1Fvkj6hQPdPHI51OEUKEpgz3SsLWIqS_uA",
            )
            .unwrap();
        // header: rs 25, idlen 2, keyid "a1"
        assert_eq!(&body[16..20], 25u32.to_be_bytes().as_slice());
        assert_eq!(&body[SALT_LEN + 4..SALT_LEN + 5], &[2]);
        assert_eq!(&body[SALT_LEN + 5..SALT_LEN + 7], b"a1");
        let decoded = decrypt_bytes_ikm(&ikm, &body).unwrap();
        assert_eq!(decoded, b"I am the walrus");
    }

    #[test]
    fn corruption_is_rejected() {
        let key = MasterKey::random();
        let pt = vec![1u8; 500];
        let mut ct = encrypt_with_rs(&key, &pt, SMALL_RS, Padding::Record);
        let mid = ct.len() / 2;
        ct[mid] ^= 0xFF;
        assert!(decrypt_bytes(&key, &ct).is_err(), "bit-flip must fail auth");
    }

    #[test]
    fn wrong_key_is_rejected() {
        let key = MasterKey::random();
        let other = MasterKey::random();
        let ct = encrypt_bytes(&key, b"secret");
        assert!(decrypt_bytes(&other, &ct).is_err());
    }

    #[test]
    fn truncated_stream_is_rejected() {
        let key = MasterKey::random();
        let ct = encrypt_with_rs(&key, &[9u8; 5000], SMALL_RS, Padding::Record);
        // Cut into the final record: header + one full record only.
        let wire = SMALL_RS as usize + RECORD_OVERHEAD;
        assert!(decrypt_bytes(&key, &ct[..HEADER_LEN + wire]).is_err());
    }

    #[test]
    fn deterministic_per_content() {
        let key = MasterKey::random();
        let pt = b"deterministic bytes".to_vec();
        let ct1 = encrypt_bytes(&key, &pt);
        let ct2 = encrypt_bytes(&key, &pt);
        assert_eq!(
            ct1, ct2,
            "same key + plaintext must reproduce identical ciphertext"
        );
        assert_eq!(Hash::new(&ct1), Hash::new(&ct2));
        // Same key, different plaintext: salts must diverge (GCM safety).
        let other = b"different plaintext".to_vec();
        let salt1 = key.salt_for(&Hash::new(&pt));
        let salt2 = key.salt_for(&Hash::new(&other));
        assert_ne!(salt1, salt2);
    }

    #[test]
    fn jwk_oct_roundtrip() {
        let key = MasterKey::random();
        let jwk = JwkOct::from_master_key(&key);
        assert_eq!(jwk.kty, "oct");
        assert_eq!(jwk.k.len(), 43, "32 octets = 43 unpadded base64url chars");
        assert!(!jwk.k.contains('='), "JWK base64url carries no padding");
        assert_eq!(jwk.to_master_key().unwrap().0, key.0);

        // JSON roundtrip keeps the ADR's JWK wire shape.
        let json = serde_json::to_vec(&jwk).unwrap();
        assert!(
            serde_json::from_slice::<serde_json::Value>(&json)
                .unwrap()
                .get("kty")
                .is_some(),
            "serialized form must be a JWK object"
        );
        let parsed: JwkOct = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed, jwk);
        assert_eq!(parsed.to_master_key().unwrap().0, key.0);

        // Wrong key type, short encoding, and non-canonical trailing bits
        // must all be rejected.
        let bad_kty = JwkOct {
            kty: "RSA".to_owned(),
            k: jwk.k.clone(),
        };
        assert!(bad_kty.to_master_key().is_err());
        let short = JwkOct {
            kty: "oct".to_owned(),
            k: jwk.k[..42].to_owned(),
        };
        assert!(short.to_master_key().is_err());
        // The all-zero oct key canonically ends in 'A' (zero padding bits);
        // bumping that character must trip the canonicality check.
        let zeros = JwkOct::from_master_key(&MasterKey([0u8; MASTER_KEY_LEN]));
        assert_eq!(zeros.k.as_bytes().last().copied(), Some(b'A'));
        let noncanon = JwkOct {
            kty: "oct".to_owned(),
            k: format!("{}B", &zeros.k[..42]),
        };
        assert!(noncanon.to_master_key().is_err());
    }

    /// Mem-store round trip through the public flows: add -> serve -> get,
    /// plus both durable tags present.
    #[tokio::test(flavor = "multi_thread")]
    async fn add_get_roundtrip_and_tags_mem() -> Res<()> {
        let (store, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let key = MasterKey::random();

        let plaintext = b"daybook cipherblob payload".to_vec();

        // `add_encrypted` installs and registers as one act, and registering
        // is what roots both entries under the named tags.
        let provider = Arc::new(CipherBlobProvider::new());
        let (c, p_hash) =
            add_encrypted(&store, &provider, &key, EncodingParams::DEFAULT, &plaintext).await?;
        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c, key.clone());
        let keys = keys_map;
        provider.register(&virtuals)?;

        let got = get_decrypted(&store, &keys, c).await?;
        assert_eq!(got, plaintext);

        let names: HashSet<Vec<u8>> = {
            use futures::StreamExt;
            let s = store.tags().list().await?;
            futures::pin_mut!(s);
            let mut out = HashSet::new();
            while let Some(info) = s.next().await {
                out.insert(info?.name.0.to_vec());
            }
            out
        };
        assert!(names.contains(format!("{TAG_CT_PREFIX}{c}").as_bytes()));
        assert!(names.contains(format!("{TAG_PT_PREFIX}{c}").as_bytes()));
        // Name presence is not the invariant: `ct:<C>` must point at C and
        // `pt:<C>` at P, or the tags root nothing while reading as a pass.
        let ct_tag = store
            .tags()
            .get(format!("{TAG_CT_PREFIX}{c}"))
            .await?
            .expect("ct: tag exists");
        assert_eq!(ct_tag.hash, c);
        let pt_tag = store
            .tags()
            .get(format!("{TAG_PT_PREFIX}{c}"))
            .await?
            .expect("pt: tag exists");
        assert_eq!(pt_tag.hash, p_hash);
        assert!(
            !names.iter().any(|n| n.starts_with(b"key:")),
            "no key material in the blob store"
        );
        Ok(())
    }

    /// Streaming paths must be byte-identical to the buffered codec paths:
    /// same C digest, same P digest; odd chunk boundaries must not matter.
    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_matches_buffered() -> Res<()> {
        let (store, _virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let provider = Arc::new(CipherBlobProvider::new());
        let key = MasterKey::random();

        // Both framings the facet can name: the importer must produce the
        // bytes its caller's `encodingParameters` describe, not the default's.
        for encoding in [
            EncodingParams::DEFAULT,
            EncodingParams {
                record_size: SMALL_RS,
                ..EncodingParams::DEFAULT
            },
        ] {
            // Spans multiple RFC 8188 records and bao chunks; deliberately
            // record- and chunk-unaligned stream pieces.
            let plaintext: Vec<u8> = (0..250_000u32).map(|i| (i % 251) as u8).collect();
            let pieces: Vec<Result<bytes::Bytes, std::io::Error>> = plaintext
                .chunks(13_003)
                .map(|c| Ok(bytes::Bytes::from(c.to_vec())))
                .collect();

            let (c_streamed, _p_hash) = add_encrypted_stream(
                &store,
                &provider,
                &key,
                encoding,
                futures::stream::iter(pieces),
            )
            .await?;

            // C must equal the buffered codec's bytes byte-for-byte.
            let ct = encrypt_with_rs(&key, &plaintext, encoding.record_size, encoding.padding);
            assert_eq!(
                Hash::new(&ct),
                c_streamed,
                "streamed digest under {encoding:?}"
            );
            // P must be stored unmodified.
            let (c_buffered, p_hash) =
                add_encrypted(&store, &provider, &key, encoding, &plaintext).await?;
            assert_eq!(c_streamed, c_buffered);
            let stored_p = store.blobs().get_bytes(p_hash).await?;
            assert_eq!(stored_p.as_ref(), &plaintext[..]);

            // The streamed entry must decrypt end-to-end: register the serving
            // provider (as the pin worker would) and read it back.
            let (store2, virtuals) =
                iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
            let store2 = Store::from(store2);
            // A provider serves one store's storage, so store2 needs its own.
            let provider2 = Arc::new(CipherBlobProvider::new());
            let (c2, p2) = add_encrypted(&store2, &provider2, &key, encoding, &plaintext).await?;
            assert_eq!(c2, c_streamed, "buffered path must match streamed path");
            assert_eq!(
                Hash::new(&plaintext),
                p2,
                "P digest identity from streaming pass 1"
            );
            provider2.register(&virtuals)?;
            let mut keys_map = MapKeySource::default();
            keys_map.0.insert(c2, key.clone());
            let got = get_decrypted(&store2, &keys_map, c2).await?;
            assert_eq!(got, plaintext, "round trip under {encoding:?}");
        }
        Ok(())
    }

    /// Serving must not re-encrypt what it just encrypted: bao leaf reads are
    /// 16 KiB against 64 KiB records, so consecutive leaf reads land in the
    /// record just encrypted. Without the cache every leaf read re-encrypts
    /// each record it overlaps and re-reads its plaintext - 4x the encryption
    /// work and 4x the plaintext I/O for the same window. Pinned by counting
    /// the encryptions the pair actually performs.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_cache_avoids_re_encrypting() -> Res<()> {
        let (store, _virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let key = MasterKey::random();
        // Spans three records, so record 1 is a full, non-final frame under
        // both padding policies.
        let plain = vec![0x5Au8; 2 * (RECORD_SIZE as usize) + 17];
        let p_hash = Hash::new(&plain);
        let _p_tag = store.blobs().add_bytes(plain.clone()).temp_tag().await?;
        let reader = store
            .sync_reader(p_hash)
            .await?
            .expect("the stored plaintext has a reader");
        let encryptions = |p: &PlainPair| p.encryptions.load(Ordering::Relaxed);

        for padding in [Padding::Record, Padding::Minimal] {
            let expected = encrypt_with_rs(&key, &plain, RECORD_SIZE, padding);
            let rec_len = RECORD_SIZE as usize;
            let start = HEADER_LEN + rec_len;

            // One entry is enough because reads arrive in offset order, so the
            // cache must never change the bytes it serves.
            let encoding = EncodingParams {
                padding,
                ..EncodingParams::DEFAULT
            };
            let pair = PlainPair::new(&key, p_hash, reader.clone(), encoding);
            assert_eq!(pair.record(1)?, &expected[start..start + rec_len]);
            assert_eq!(encryptions(&pair), 1);
            assert_eq!(
                pair.record(1)?,
                &expected[start..start + rec_len],
                "a repeat read must be the cached bytes"
            );
            assert_eq!(encryptions(&pair), 1, "a repeat read must not re-encrypt");
            // Reading another record evicts; the evicted record is recomputed
            // when it is asked for again.
            let _evictor = pair.record(0)?;
            assert_eq!(encryptions(&pair), 2);
            assert_eq!(pair.record(1)?, &expected[start..start + rec_len]);
            assert_eq!(encryptions(&pair), 3);

            // Sweep the whole ciphertext in store-sized bao leaves: every
            // record is encrypted exactly once, however many leaves it spans.
            let pair = Arc::new(PlainPair::new(&key, p_hash, reader.clone(), encoding));
            let src = CipherSource { pair: pair.clone() };
            let total = expected.len() as u64;
            let mut off = 0;
            while off < total {
                let got = src.read_window(off, BAO_LEAF_SIZE)?;
                assert_eq!(
                    got.as_ref(),
                    &expected[off as usize..off as usize + got.len()],
                    "leaf read at {off} under {padding:?}"
                );
                assert!(!got.is_empty());
                off += got.len() as u64;
            }
            assert_eq!(
                encryptions(&pair),
                pair.n_records,
                "serving must encrypt each record once, not once per leaf read"
            );
        }
        Ok(())
    }

    /// The virtual provider must serve *any* byte window of the ciphertext,
    /// and every window must agree with a full encryption of the same
    /// plaintext. Covers the header alone, windows straddling record
    /// boundaries, windows smaller and larger than one record, the last byte,
    /// and reads at or past the end - under both padding policies. This is the
    /// window-level net under `CipherSource`: the QUIC tests only read whole
    /// blobs through it.
    ///
    /// The plaintext is a real stored entry, read back through the store's
    /// synchronous reader, so this covers the resolution path serving uses
    /// rather than a snapshot handed in by hand.
    #[tokio::test(flavor = "multi_thread")]
    async fn served_windows_match_full_encryption() -> Res<()> {
        use iroh_blobs::store::virtual_blob::Provider as _;

        // `rs` is a per-representation input - the facet's
        // `encodingParameters` - not a constant, so serve at the default *and*
        // at a small record size, where record boundaries fall in entirely
        // different places. Serving at the wrong stride cannot verify against
        // C's outboard, so the per-window comparison is what pins that the
        // stride comes from the pair rather than from a constant.
        for encoding in [
            EncodingParams::DEFAULT,
            EncodingParams {
                record_size: SMALL_RS,
                ..EncodingParams::DEFAULT
            },
        ] {
            let rs = encoding.record_size;
            let payload_max = payload_size(rs) as usize;
            // Three records: two full plus a partial final one.
            let plain = vec![0x3Cu8; 2 * payload_max + 1234];
            let (store, _virtuals) =
                iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
            let store = Store::from(store);
            let _p_tag = store.blobs().add_bytes(plain.clone()).temp_tag().await?;
            let p_hash = Hash::new(&plain);
            let stored_len = store
                .sync_reader(p_hash)
                .await?
                .expect("the stored plaintext has a reader")
                .len();
            assert_eq!(stored_len, plain.len() as u64);

            for padding in [Padding::Record, Padding::Minimal] {
                let encoding = EncodingParams {
                    padding,
                    ..encoding
                };
                let key = MasterKey::random();
                let expected = encrypt_with_rs(&key, &plain, rs, padding);
                let total = expected.len() as u64;
                let c = Hash::new(&expected);

                let provider = CipherBlobProvider::new();
                provider
                    .register_pair(&store, c, &key, p_hash, encoding)
                    .await?;
                let src = provider.reader_for(&c).expect("registered pair is served");

                let got = |offset: u64, size: usize| -> Res<Vec<u8>> {
                    Ok(src.read_bytes_at(offset, size)?.to_vec())
                };
                // A window read clamps at the ciphertext end, like a file read at EOF.
                let want = |offset: u64, size: usize| -> Vec<u8> {
                    let start = offset.min(total) as usize;
                    let end = (offset + size as u64).min(total) as usize;
                    expected[start..end].to_vec()
                };

                let windows: Vec<(u64, usize)> = vec![
                    (0, 0),
                    (0, 1),
                    (0, HEADER_LEN),
                    (0, HEADER_LEN + 7),
                    (HEADER_LEN as u64 - 1, 2),
                    (HEADER_LEN as u64, 1),
                    (HEADER_LEN as u64, rs as usize),
                    (HEADER_LEN as u64 + rs - 5, 10),
                    (0, rs as usize * 2),
                    (0, total as usize),
                    (total - 1, 1),
                    (total - 10, 100),
                    (total, 10),
                    (total + 5, 10),
                ];
                for (offset, size) in windows {
                    assert_eq!(
                        got(offset, size)?,
                        want(offset, size),
                        "window ({offset}, {size}) wrong under rs={rs} {padding:?}"
                    );
                }

                // Exhaustive 1 KiB sweep: every window start, straddling whatever
                // records it lands in. Range-sized reads are what serving does.
                for offset in (0..total).step_by(1024) {
                    assert_eq!(
                        got(offset, 1024)?,
                        want(offset, 1024),
                        "sweep window at {offset} wrong under rs={rs} {padding:?}"
                    );
                }
            }
        }

        Ok(())
    }

    /// Windowed reads must return exactly the plaintext bytes for any range,
    /// while decrypting only the records that range overlaps. Framing comes
    /// from the ciphertext's own header, so this runs at a non-default record
    /// size: a reader that assumed the default would put every offset in the
    /// wrong place.
    #[tokio::test(flavor = "multi_thread")]
    async fn cipher_reader_reads_plaintext_windows() -> Res<()> {
        let (store, _virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(store);
        let key = MasterKey::random();
        let rs = SMALL_RS;
        let payload_max = payload_size(rs) as usize;
        // Four records: three full plus a partial final one.
        let plain: Vec<u8> = (0..(3 * payload_max as u64 + 1234))
            .map(|i| (i % 251) as u8)
            .collect();
        let p_len = plain.len() as u64;
        let last_record_start = 3 * payload_max as u64;

        for padding in [Padding::Record, Padding::Minimal] {
            let ct = encrypt_with_rs(&key, &plain, rs, padding);
            let c_hash = Hash::new(&ct);
            let _c_tag = store.blobs().add_bytes(ct).temp_tag().await?;
            let mut keys = MapKeySource::default();
            keys.0.insert(c_hash, key.clone());

            let reader = CipherReader::open(&store, &keys, c_hash, p_len).await?;
            assert_eq!(reader.plaintext_len(), p_len);

            let want = |offset: u64, size: usize| -> Vec<u8> {
                let start = offset.min(p_len) as usize;
                let end = (offset + size as u64).min(p_len) as usize;
                plain[start..end].to_vec()
            };
            let windows: Vec<(u64, usize)> = vec![
                (0, 0),
                (0, 1),
                (payload_max as u64 - 1, 2), // straddles records 0 and 1
                (payload_max as u64, payload_max),
                (0, 2 * payload_max),
                (last_record_start - 1, 2), // straddles records 2 and the last
                (last_record_start, 1),
                (0, p_len as usize),
                (p_len - 1, 1),
                (p_len - 10, 100),
                (p_len, 10),
                (p_len + 5, 10),
            ];
            for (offset, size) in windows {
                assert_eq!(
                    reader.read_plaintext_at(offset, size)?.to_vec(),
                    want(offset, size),
                    "window ({offset}, {size}) wrong under {padding:?}"
                );
            }
            // Exhaustive sweep, so every record boundary is crossed.
            for offset in (0..p_len).step_by(333) {
                assert_eq!(
                    reader.read_plaintext_at(offset, 700)?.to_vec(),
                    want(offset, 700),
                    "sweep window at {offset} wrong under {padding:?}"
                );
            }

            // A length the ciphertext cannot hold is rejected at open.
            assert!(
                CipherReader::open(&store, &keys, c_hash, p_len + 4096)
                    .await
                    .is_err(),
                "a ciphertext too short for the claimed plaintext must not open"
            );
            // A length that is too *small* can still fit the wire - the body
            // only bounds it from below - so it has to be caught by the final
            // record's own extent when the tail is read.
            let short = CipherReader::open(&store, &keys, c_hash, 4 * payload_max as u64).await?;
            assert!(
                short
                    .read_plaintext_at(4 * payload_max as u64 - 1, 1)
                    .is_err(),
                "a length that contradicts the final record must fail"
            );
        }
        Ok(())
    }

    /// The production store is the fs store, where a large entry's data lives
    /// in its own file - read through a duplicated handle - while a small one
    /// is inlined in the database. Serving a virtual ciphertext has to work
    /// over that backend, not only the mem store the other tests use: install
    /// the entry, register the pair, then read the plaintext back through the
    /// store's own export path, which verifies every served byte against the
    /// outboard `C` before it is decrypted.
    #[tokio::test(flavor = "multi_thread")]
    async fn serves_virtual_ciphertext_from_fs_store() -> Res<()> {
        use iroh_blobs::store::fs::{FsStore, options::Options};

        let dir = tempfile::tempdir()?;
        let (fs, virtuals) =
            FsStore::load_with_virtuals(dir.path().join("blobs.db"), Options::new(dir.path()))
                .await?;
        let store = Store::from(fs);
        let key = MasterKey::random();
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;

        // One inline plaintext and one that spills to a data file spanning
        // several records and bao chunks.
        for plaintext in [
            b"daybook cipherblob, inline".to_vec(),
            vec![0x9Eu8; 3 * (RECORD_SIZE as usize) + 1234],
        ] {
            let p_hash = Hash::new(&plaintext);
            // The caller keeps `P` protected for as long as the pair is
            // registered; the pin machinery is what does this in production.
            let _p_tag = store
                .blobs()
                .add_bytes(plaintext.clone())
                .temp_tag()
                .await?;
            let c = provider
                .install(&store, &key, p_hash, EncodingParams::DEFAULT)
                .await?;
            assert_eq!(c, Hash::new(&encrypt_bytes(&key, &plaintext)));

            let mut keys_map = MapKeySource::default();
            keys_map.0.insert(c, key.clone());
            assert_eq!(
                get_decrypted(&store, &keys_map, c).await?,
                plaintext,
                "ciphertext served off the fs store must decrypt to the stored plaintext"
            );
        }
        Ok(())
    }

    /// Shared two-node helper: an in-memory iroh-blobs node with QUIC routing
    /// and a virtual-provider handle.
    async fn setup_node() -> Res<(
        iroh::protocol::Router,
        Store,
        iroh::address_lookup::MemoryLookup,
        iroh_blobs::store::virtual_blob::VirtualProviders,
    )> {
        use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets, protocol::Router};
        use iroh_blobs::{ALPN, BlobsProtocol};
        let (mem, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(mem);
        let sp = MemoryLookup::new();
        let ep = Endpoint::builder(presets::Minimal)
            .relay_mode(iroh::RelayMode::Default)
            .address_lookup(sp.clone())
            .bind()
            .await?;
        let blobs = BlobsProtocol::new(&store, None);
        let router = Router::builder(ep).accept(ALPN, blobs).spawn();
        Ok((router, store, sp, virtuals))
    }

    /// The encrypt pass re-hashes what it reads; installing with a digest
    /// that does not describe the stored bytes must abort before any
    /// virtual entry exists, while the honest digest installs and decrypts.
    #[tokio::test(flavor = "multi_thread")]
    async fn install_rejects_foreign_digest() -> Res<()> {
        let (mem, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(mem);
        let key = MasterKey::random();
        let plaintext = b"actual bytes under this digest".to_vec();
        let (p_tag, p_hash) = ensure_stored(
            &store,
            futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(
                plaintext.clone(),
            ))]),
        )
        .await?;

        let provider = Arc::new(CipherBlobProvider::new());
        let lie = Hash::new(b"different content entirely");
        assert!(
            provider
                .install(&store, &key, lie, EncodingParams::DEFAULT)
                .await
                .is_err(),
            "digest that does not match the stored bytes must abort the install"
        );

        // The honest digest installs fine and the entry decrypts.
        let c = provider
            .install(&store, &key, p_hash, EncodingParams::DEFAULT)
            .await?;
        provider.register(&virtuals)?;
        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c, key.clone());
        assert_eq!(get_decrypted(&store, &keys_map, c).await?, plaintext);
        drop(p_tag);
        Ok(())
    }
    /// The main event: node A serves stored ciphertext C; node B downloads it
    /// without storing C (plaintext lands instead), then re-serves C to node C
    /// over QUIC from its virtual entry. Unregistering B's provider must make
    /// C's GET fail with NotFound; re-registering restores service.
    #[tokio::test(flavor = "multi_thread")]
    async fn download_then_serve_over_quic() -> Res<()> {
        use iroh_blobs::ALPN;
        // Node A: stores the ciphertext as a plain blob (a relay/peer holding C).
        let (r_a, store_a, _sp_a, _v_a) = setup_node().await?;
        let key = MasterKey::random();
        let plaintext = vec![7u8; 200_000]; // spans multiple records and bao chunks
        let ct = encrypt_bytes(&key, &plaintext);
        let c_hash = Hash::new(&ct);
        let _tt = store_a.blobs().add_bytes(ct.clone()).temp_tag().await?;

        // Node B: downloads encrypted, keeps plaintext + virtual entry.
        let (r_b, store_b, sp_b, virtuals_b) = setup_node().await?;
        sp_b.add_endpoint_info(r_a.endpoint().addr());
        let conn = r_b
            .endpoint()
            .connect(r_a.endpoint().addr(), ALPN)
            .await
            .map_err(|e| eyre::eyre!("connect failed: {e:?}"))?;
        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c_hash, key.clone());
        let keys = Arc::new(keys_map);
        // Node B serves C virtually; node C GETs it over QUIC. Downloading
        // installs and registers, so B can serve what it just decrypted.
        let provider = Arc::new(CipherBlobProvider::new());
        let p_hash =
            download_encrypted(&store_b, &provider, conn, c_hash, keys.as_ref(), None).await?;
        assert_eq!(
            store_b.blobs().get_bytes(p_hash).await?.as_ref(),
            &plaintext[..],
        );
        provider.register(&virtuals_b)?;

        let (r_c, store_c, sp_c, _v_c) = setup_node().await?;
        sp_c.add_endpoint_info(r_b.endpoint().addr());
        let conn_c = r_c
            .endpoint()
            .connect(r_b.endpoint().addr(), ALPN)
            .await
            .map_err(|e| eyre::eyre!("connect failed: {e:?}"))?;
        // probe: first fetch a PLAIN blob from B to verify transport
        let plain_marker = b"plain marker".to_vec();
        let _mt = store_b
            .blobs()
            .add_bytes(plain_marker.clone())
            .temp_tag()
            .await?;
        let marker_hash = Hash::new(&plain_marker);
        store_c
            .remote()
            .fetch(conn_c.clone(), marker_hash)
            .await
            .map_err(|e| eyre::eyre!("plain fetch failed: {e:?}"))?;
        store_c.remote().fetch(conn_c.clone(), c_hash).await?;
        let got_ct = store_c.get_bytes(c_hash).await?;
        assert_eq!(
            got_ct.as_ref(),
            &ct[..],
            "node C must receive exact ciphertext"
        );

        // Negative: unregistered provider => remote GET fails.
        virtuals_b.unregister(PROVIDER_NAME);
        let conn_c2 = r_c
            .endpoint()
            .connect(r_b.endpoint().addr(), ALPN)
            .await
            .map_err(|e| eyre::eyre!("connect failed: {e:?}"))?;
        let other = Hash::new(b"no such blob");
        assert!(
            store_c
                .remote()
                .fetch(conn_c2.clone(), other)
                .await
                .is_err()
        );

        tokio::try_join!(r_a.shutdown(), r_b.shutdown(), r_c.shutdown())?;
        Ok(())
    }

    /// Resume: interrupt a download mid-transfer, then finish from the ledger.
    /// A resumable download spills decrypted records as their ciphertext
    /// verifies; the interrupted attempt must leave that progress on disk,
    /// the resumed attempt must complete from it, and the end state
    /// (plaintext bytes, virtual entry re-serving C) must be identical to an
    /// uninterrupted download.
    #[tokio::test(flavor = "multi_thread")]
    async fn download_interrupted_then_resume() -> Res<()> {
        // Node A: relay holding C as a plain stored blob.
        let (r_a, store_a, _sp_a, _v_a) = setup_node().await?;
        let key = MasterKey::random();
        // 16 MiB: several hundred records; enough transfer time for the
        // interrupt to land mid-flight.
        let plaintext = vec![7u8; 16 * 1024 * 1024];
        let ct = encrypt_bytes(&key, &plaintext);
        let c_hash = Hash::new(&ct);
        let _tt = store_a.blobs().add_bytes(ct.clone()).temp_tag().await?;

        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c_hash, key.clone());
        let keys = Arc::new(keys_map);

        // Node B: interrupted first attempt.
        let ledger_dir = tempfile::tempdir()?;
        let ledger = FsDownloadLedger::new(ledger_dir.path());
        let (r_b, store_b, sp_b, _virtuals_b) = setup_node().await?;
        sp_b.add_endpoint_info(r_a.endpoint().addr());
        let conn = r_b
            .endpoint()
            .connect(r_a.endpoint().addr(), iroh_blobs::ALPN)
            .await?;
        let provider_b = Arc::new(CipherBlobProvider::new());
        let keys2 = keys.clone();
        // Drive the first attempt in place and interrupt it once progress is
        // observable: dropping the pinned future cancels the download mid-
        // transfer, which is exactly the crash we want to survive.
        let mut attempt = Box::pin(download_encrypted(
            &store_b,
            &provider_b,
            conn,
            c_hash,
            keys2.as_ref(),
            Some(&ledger),
        ));
        // Drive the attempt and cancel it as soon as the first records are
        // spilled: the select keeps the download future polled while the poll
        // checks progress, and dropping the pinned future cancels the
        // download mid-transfer, which is exactly the crash we must survive.
        // If the whole transfer ever completes first (loopback too fast to
        // interrupt believably), fail loudly instead of testing nothing.
        let spill_path = ledger.spill_path(&c_hash);
        let payload = RECORD_SIZE - RECORD_OVERHEAD as u64;
        loop {
            tokio::select! {
                biased;
                res = &mut attempt => {
                    let _p = res?;
                    eyre::bail!(
                        "download completed before the interruption could land; \
                         too fast for the test harness"
                    );
                }
                _ = tokio::time::sleep(std::time::Duration::from_micros(500)) => {
                    if tokio::fs::metadata(&spill_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0)
                        >= 2 * payload
                    {
                        break;
                    }
                }
            }
        }
        drop(attempt);
        let spill_len = tokio::fs::metadata(&spill_path).await?.len();
        assert!(
            spill_len >= 2 * payload,
            "spilled progress must survive the interruption"
        );

        // Resumed attempt on a fresh connection: must complete from the spill.
        let conn = r_b
            .endpoint()
            .connect(r_a.endpoint().addr(), iroh_blobs::ALPN)
            .await?;
        let p_hash = download_encrypted(
            &store_b,
            &provider_b,
            conn,
            c_hash,
            keys.as_ref(),
            Some(&ledger),
        )
        .await?;
        assert_eq!(p_hash, Hash::new(&plaintext));
        let got = store_b.blobs().get_bytes(p_hash).await?;
        assert_eq!(got.as_ref(), &plaintext[..]);
        // The spill is torn down once the plaintext is durably tagged.
        assert!(
            !ledger.spill_path(&c_hash).exists(),
            "completed resume must clear the ledger"
        );

        tokio::try_join!(r_a.shutdown(), r_b.shutdown())?;
        Ok(())
    }

    /// The crash-between-spill-and-import edge: the final record was already
    /// spilled (exact multiple under `Padding::Record`, so the spill length is
    /// a whole number of payloads) when the attempt died. The resume must
    /// detect completeness from the empty suffix transfer, import the spill,
    /// re-install the virtual entry, and clear the ledger - without ever
    /// asking for more records.
    #[tokio::test(flavor = "multi_thread")]
    async fn resume_with_final_record_already_spilled() -> Res<()> {
        let (r_a, store_a, _sp_a, _v_a) = setup_node().await?;
        let key = MasterKey::random();
        // Exact multiple of the default payload: every record is full.
        let payload = RECORD_SIZE - RECORD_OVERHEAD as u64;
        let plaintext = vec![0x55u8; 2 * payload as usize];
        let ct = encrypt_bytes(&key, &plaintext);
        let c_hash = Hash::new(&ct);
        let _tt = store_a.blobs().add_bytes(ct.clone()).temp_tag().await?;

        // Simulate the crashed attempt: header facts + a complete spill.
        let ledger_dir = tempfile::tempdir()?;
        let ledger = FsDownloadLedger::new(ledger_dir.path());
        let facts = HeaderFacts {
            salt: ct[..SALT_LEN].try_into().expect("16 salt octets"),
            rs: RECORD_SIZE as u32,
            idlen: 0,
        };
        ledger.write_meta(&c_hash, facts, Padding::Record)?;
        let spill = ledger.spill_path(&c_hash);
        std::fs::create_dir_all(spill.parent().expect("spill has a parent"))?;
        std::fs::write(&spill, &plaintext)?;

        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c_hash, key.clone());
        let (r_b, store_b, sp_b, virtuals_b) = setup_node().await?;
        sp_b.add_endpoint_info(r_a.endpoint().addr());
        let conn = r_b
            .endpoint()
            .connect(r_a.endpoint().addr(), iroh_blobs::ALPN)
            .await?;
        let provider = Arc::new(CipherBlobProvider::new());
        let p_hash =
            download_encrypted(&store_b, &provider, conn, c_hash, &keys_map, Some(&ledger)).await?;
        assert_eq!(p_hash, Hash::new(&plaintext));
        let got = store_b.blobs().get_bytes(p_hash).await?;
        assert_eq!(got.as_ref(), &plaintext[..]);
        assert!(!spill.exists(), "completed resume must clear the ledger");

        // The re-derived virtual entry serves decryptions exactly.
        provider.register(&virtuals_b)?;
        assert_eq!(get_decrypted(&store_b, &keys_map, c_hash).await?, plaintext);

        tokio::try_join!(r_a.shutdown(), r_b.shutdown())?;
        Ok(())
    }
}
