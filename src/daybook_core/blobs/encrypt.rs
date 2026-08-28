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
use std::io;

use aes_gcm::{
    Aes128Gcm, Nonce,
    aead::{AeadInPlace, KeyInit},
};
use hkdf::Hkdf;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, remote::GetStreamPair},
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

    /// Encoding policy of the ciphertext `C` (ADR 003 §17). Only needed
    /// where `C` must be reconstructed locally: a resumed download re-installs
    /// the virtual entry by re-encrypting the completed plaintext, which
    /// requires the same padding the original `C` was built with. The
    /// facet-driven implementation reads `encodingParameters.padding`; the
    /// v1 default matches [`DEFAULT_PADDING`].
    async fn encoding_for(&self, _c: &Hash) -> Res<Padding> {
        Ok(DEFAULT_PADDING)
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
/// buffered whole), then [`install_virtual_encrypted`] installs `C` as a
/// virtual entry served by [`PROVIDER_NAME`]. Roots `C` and `P` under named
/// tags (see [`set_pair_tags`]) and returns `(C, P)` hashes.
pub async fn add_encrypted_stream<S>(
    store: &Store,
    key: &MasterKey,
    p_stream: S,
) -> Res<(Hash, Hash)>
where
    S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static,
{
    let (p_tag, p_hash) = ensure_stored(store, p_stream).await?;
    // The streaming flow uses the v1 default (record padding); a future
    // facet-driven worker will plumb the facet's encodingParameters through.
    let c_hash = match install_virtual_encrypted(store, key, p_hash, DEFAULT_PADDING).await {
        Ok(c_hash) => c_hash,
        Err(e) => {
            drop(p_tag);
            return Err(e);
        }
    };
    set_pair_tags(store, c_hash, p_hash).await?;
    drop(p_tag);
    Ok((c_hash, p_hash))
}

/// [`add_encrypted_stream`] for data already fully in memory (tests, small
/// payloads). Large media must use the streaming form.
pub async fn add_encrypted(store: &Store, key: &MasterKey, plaintext: &[u8]) -> Res<(Hash, Hash)> {
    let chunk = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(
        plaintext.to_vec(),
    ))]);
    add_encrypted_stream(store, key, chunk).await
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
    padding: Padding,
    pending: Vec<u8>,
    seq: u64,
    payload_max: usize,
    emitted_final: bool,
}

impl RecordEncryptor {
    fn new(key: &MasterKey, p_hash: &Hash, padding: Padding) -> Self {
        let salt = key.salt_for(p_hash);
        let cipher = key.cipher_with_salt(&salt);
        Self {
            salt,
            cipher,
            padding,
            pending: Vec::new(),
            seq: 0,
            payload_max: (RECORD_SIZE - RECORD_OVERHEAD as u64) as usize,
            emitted_final: false,
        }
    }

    fn bake_header(&self) -> Vec<u8> {
        header(&self.salt, RECORD_SIZE).to_vec()
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
        let pad_zeros = match self.padding {
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
/// under `padding`.
fn ciphertext_len(p_len: u64, padding: Padding, payload_max: u64) -> u64 {
    let n = n_records_for(p_len, payload_max);
    let final_content = p_len - (n - 1) * payload_max;
    let final_wire = match padding {
        // Minimal: content + delimiter + tag (a full-content final record is
        // exactly `rs` wire bytes - content + delimiter = rs - 16).
        Padding::Minimal => final_content + RECORD_OVERHEAD as u64,
        // Record: the final frame is padded up to full record size.
        Padding::Record => RECORD_SIZE,
    };
    HEADER_LEN as u64 + (n - 1) * RECORD_SIZE + final_wire
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
pub async fn install_virtual_encrypted(
    store: &Store,
    key: &MasterKey,
    p_hash: Hash,
    padding: Padding,
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
    let c_len = ciphertext_len(p_len, padding, RECORD_SIZE - RECORD_OVERHEAD as u64);
    let tree = bao_tree::BaoTree::new(c_len, iroh_blobs::store::IROH_BLOCK_SIZE);
    let outboard_len = tree.outboard_size() as usize;

    // Producer: re-hash the plaintext while encrypting; write the ciphertext
    // wire bytes (header + whole records) into the pipe. Ending the future
    // closes the pipe: that is what tells the hasher the stream is complete.
    let enc = RecordEncryptor::new(key, &p_hash, padding);
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
    let (spill, padding) = match prior {
        None => (None, DEFAULT_PADDING),
        Some((_, path, None)) => {
            // Fresh attempt under a ledger: start an empty spill.
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await?;
            let padding = keys.encoding_for(&c_hash).await?;
            (Some((file, None, padding, 0u64)), padding)
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
            (Some((file, Some(facts), padding, seq)), padding)
        }
    };
    // seq == 0 (no meta at all, or meta with an empty spill) means the
    // header must be parsed from the stream again: a fully fresh decryptor,
    // full range request.
    let seq = spill.as_ref().map_or(0, |(_, _, _, seq)| *seq);
    let fresh_decoder = seq == 0;

    // Chunk ranges are 1 KiB blake3-chunk units: request from the start of
    // the chunk containing the first missing record, open-ended.
    let first_byte = spill
        .as_ref()
        .and_then(|(_, facts, _, seq)| facts.as_ref().map(|f| f.record_start(*seq)))
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
        let (_, facts, _, seq) = spill.as_ref().expect("seq > 0 implies a prior attempt");
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
        padding,
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
        Some((file, _, _, _)) => {
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

    // Root the plaintext and install the virtual entry.
    if !ledger_mode {
        // Fresh: the full transfer delivers parents in pre-order, so the
        // received appends are already C's outboard; install it directly.
        store
            .blobs()
            .add_virtual_with_outboard(c_hash, ciphertext_len, outboard, PROVIDER_NAME)
            .await?;
    } else {
        // Resume: re-derive C deterministically (see the fn doc + ADR 003 §12
        // for why the received fragments are not reused).
        let c2 = install_virtual_encrypted(store, &key, p_hash, padding).await?;
        eyre::ensure!(
            c2 == c_hash,
            "re-encrypted ciphertext {c2} differs from the downloaded {c_hash}: corrupt spill?"
        );
    }

    set_pair_tags(store, c_hash, p_hash).await?;
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
/// `reader_for(C)` looks up a snapshot pair `(key, plaintext)` registered via
/// [`CipherBlobProvider::register_pair`] and returns a random-access source
/// that encrypts only the records overlapping each requested byte window.
///
/// The plaintext lives in memory inside the provider for the lifetime of the
/// registration; swapping in an fs-backed sync reader is a future step.
pub struct CipherBlobProvider {
    pairs: RwLock<HashMap<Hash, PlainPair>>,
}

/// A registered C -> (P bytes, key, padding) binding.
struct PlainPair {
    key: MasterKey,
    plain: Arc<Vec<u8>>,
    padding: Padding,
}
impl CipherBlobProvider {
    pub fn new() -> Self {
        Self {
            pairs: RwLock::new(HashMap::new()),
        }
    }

    /// Register the binding for ciphertext `c`: key + plaintext snapshot +
    /// the padding policy the ciphertext was framed with.
    pub fn register_pair(&self, c: Hash, key: MasterKey, plain: Arc<Vec<u8>>, padding: Padding) {
        self.pairs.write().expect("pairs lock poisoned").insert(
            c,
            PlainPair {
                key,
                plain,
                padding,
            },
        );
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
        Some(Arc::new(CipherSource {
            key: pair.key.clone(),
            plain: pair.plain.clone(),
            padding: pair.padding,
        }))
    }
}

/// Random-access ciphertext view over one plaintext snapshot. Purely
/// synchronous: RFC 8188 records are independently computable given
/// key + salt + record index, so only overlapping records are encrypted.
struct CipherSource {
    key: MasterKey,
    plain: Arc<Vec<u8>>,
    padding: Padding,
}

impl CipherSource {
    fn read_window(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        let end = offset + size as u64;
        let mut pos = offset;
        let mut out = Vec::with_capacity(size);

        let p_hash = Hash::new(&self.plain[..]);
        let salt = self.key.salt_for(&p_hash);
        let cipher = self.key.cipher_for(&p_hash);
        // RFC 8188 header precedes the record stream.
        if pos < HEADER_LEN as u64 {
            let h = header(&salt, RECORD_SIZE);
            let take = ((HEADER_LEN as u64 - pos) as usize).min(size);
            out.extend_from_slice(&h[pos as usize..pos as usize + take]);
            pos += take as u64;
        }
        if pos >= end {
            return Ok(out.into());
        }

        // Under `Padding::Record` every record is a full `rs` frame; under
        // `Padding::Minimal` the final frame ends after its delimiter.
        let payload_max = (RECORD_SIZE - RECORD_OVERHEAD as u64).max(1);
        let wire = RECORD_SIZE;
        let p_size = self.plain.len() as u64;
        let n_records = n_records_for(p_size, payload_max);
        let total_c_len = ciphertext_len(p_size, self.padding, payload_max);
        let end = end.min(total_c_len);

        let mut cur_record: Option<(u64, Vec<u8>)> = None;
        while pos < end {
            let rel = pos - HEADER_LEN as u64;
            let idx = rel / wire;
            let within = (rel % wire) as usize;
            let rec_ct = match cur_record.take() {
                Some((i, ct)) if i == idx => ct,
                _ => {
                    let pt_start = idx * payload_max;
                    let pt_end = (pt_start + payload_max).min(p_size);
                    let chunk = self.plain[pt_start as usize..pt_end as usize].to_vec();
                    let pad_zeros = if idx == n_records - 1 {
                        match self.padding {
                            Padding::Minimal => 0,
                            Padding::Record => (payload_max - chunk.len() as u64) as usize,
                        }
                    } else {
                        0
                    };
                    cipher.encrypt_record(idx, idx == n_records - 1, chunk, pad_zeros)
                }
            };
            let avail = rec_ct.len().saturating_sub(within);
            let take = avail.min((end - pos) as usize);
            out.extend_from_slice(&rec_ct[within..within + take]);
            if within + take < rec_ct.len() {
                cur_record = Some((idx, rec_ct));
            }
            pos += take as u64;
        }
        Ok(out.into())
    }
}

impl bao_tree::io::mixed::ReadBytesAt for CipherSource {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        self.read_window(offset, size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMALL_RS: u64 = 1024;

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

        let (c, _p_hash) = add_encrypted(&store, &key, &plaintext).await?;
        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c, key.clone());
        let keys = keys_map;
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(c, key.clone(), Arc::new(plaintext.clone()), Padding::Record);
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
        let key = MasterKey::random();

        // Spans multiple RFC 8188 records and bao chunks; deliberately
        // record- and chunk-unaligned stream pieces.
        let plaintext: Vec<u8> = (0..250_000u32).map(|i| (i % 251) as u8).collect();
        let pieces: Vec<Result<bytes::Bytes, std::io::Error>> = plaintext
            .chunks(13_003)
            .map(|c| Ok(bytes::Bytes::from(c.to_vec())))
            .collect();

        let (c_streamed, _p_hash) =
            add_encrypted_stream(&store, &key, futures::stream::iter(pieces)).await?;

        // C must equal the buffered codec's bytes byte-for-byte.
        let ct = encrypt_bytes(&key, &plaintext);
        assert_eq!(Hash::new(&ct), c_streamed);
        // P must be stored unmodified.
        let (c_buffered, p_hash) = add_encrypted(&store, &key, &plaintext).await?;
        assert_eq!(c_streamed, c_buffered);
        let stored_p = store.blobs().get_bytes(p_hash).await?;
        assert_eq!(stored_p.as_ref(), &plaintext[..]);

        // The streamed entry must decrypt end-to-end: register the serving
        // provider (as the pin worker would) and read it back.
        let (store2, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store2 = Store::from(store2);
        let (c2, p2) = add_encrypted(&store2, &key, &plaintext).await?;
        assert_eq!(c2, c_streamed, "buffered path must match streamed path");
        assert_eq!(
            Hash::new(&plaintext),
            p2,
            "P digest identity from streaming pass 1"
        );
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(
            c2,
            key.clone(),
            Arc::new(plaintext.clone()),
            Padding::Record,
        );
        provider.register(&virtuals)?;
        let mut keys_map = MapKeySource::default();
        keys_map.0.insert(c2, key.clone());
        let got = get_decrypted(&store2, &keys_map, c2).await?;
        assert_eq!(got, plaintext);
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

        let lie = Hash::new(b"different content entirely");
        assert!(
            install_virtual_encrypted(&store, &key, lie, Padding::Record)
                .await
                .is_err(),
            "digest that does not match the stored bytes must abort the install"
        );

        // The honest digest installs fine and the entry decrypts.
        let c = install_virtual_encrypted(&store, &key, p_hash, Padding::Record).await?;
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(c, key.clone(), Arc::new(plaintext.clone()), Padding::Record);
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
        let p_hash = download_encrypted(&store_b, conn, c_hash, keys.as_ref(), None).await?;
        assert_eq!(
            store_b.blobs().get_bytes(p_hash).await?.as_ref(),
            &plaintext[..],
        );
        // Node B serves C virtually; node C GETs it over QUIC.
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(c_hash, key.clone(), Arc::new(plaintext), Padding::Record);
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
        let keys2 = keys.clone();
        // Drive the first attempt in place and interrupt it once progress is
        // observable: dropping the pinned future cancels the download mid-
        // transfer, which is exactly the crash we want to survive.
        let mut attempt = Box::pin(download_encrypted(
            &store_b,
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
        let p_hash =
            download_encrypted(&store_b, conn, c_hash, keys.as_ref(), Some(&ledger)).await?;
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
        let p_hash = download_encrypted(&store_b, conn, c_hash, &keys_map, Some(&ledger)).await?;
        assert_eq!(p_hash, Hash::new(&plaintext));
        let got = store_b.blobs().get_bytes(p_hash).await?;
        assert_eq!(got.as_ref(), &plaintext[..]);
        assert!(!spill.exists(), "completed resume must clear the ledger");

        // The re-derived virtual entry serves decryptions exactly.
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(
            c_hash,
            key.clone(),
            Arc::new(plaintext.clone()),
            Padding::Record,
        );
        provider.register(&virtuals_b)?;
        assert_eq!(get_decrypted(&store_b, &keys_map, c_hash).await?, plaintext);

        tokio::try_join!(r_a.shutdown(), r_b.shutdown())?;
        Ok(())
    }
}
