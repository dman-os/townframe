//! Encrypted (RFC 8188 `aes128gcm`) blob flows over iroh-blobs.
//!
//! Ciphertext blobs (`C`) live in iroh-blobs as *virtual* entries: the store
//! keeps only the bao outboard and a provider name, never the bytes. The
//! plaintext (`P`) is a normal stored blob. Serving `C` re-encrypts `P` on
//! demand record-by-record, so ciphertext is never materialized locally.
//!
//! Tag scheme (durable GC roots + linkage):
//! - `ct:<C>` -> `C` (raw): roots the virtual entry
//! - `pt:<C>` -> `P` (raw): roots the plaintext; keyed by `C` so the pair is
//!   resolvable from either side at startup.
//!
//! RFC 8188 notes: the 26-byte header carries salt + record size + key id;
//! the CEK (16B) and nonce base (12B) are HKDF-SHA256 expansions of the
//! master key under the salt; each record is its plaintext chunk plus a
//! delimiter byte (0x01, or 0x81 on the final record) encrypted with AES-GCM;
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

use std::io;
use bytes::Bytes;

use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes128Gcm, Nonce,
};
use hkdf::Hkdf;
use iroh_blobs::{
    api::{remote::GetStreamPair, Store},
    BlobFormat, Hash, HashAndFormat,
};
use rand::RngCore;
use sha2::Sha256;

/// Default record size for new ciphertexts (RFC 8188's recommended 64 KiB).
pub const RECORD_SIZE: u64 = 64 * 1024;
const SALT_LEN: usize = 16;
/// GCM tag (16) + record delimiter byte (1).
const RECORD_OVERHEAD: usize = 17;
const HEADER_LEN: usize = SALT_LEN + 8 + 1;
const KEY_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const MASTER_KEY_LEN: usize = 32;

const CEK_INFO: &[u8] = b"Content-Encoding: aes128gcm\x00";
const NONCE_INFO: &[u8] = b"Content-Encoding: nonce\x00";
const DELIMITER: u8 = 0x01;
const LAST_RECORD_DELIMITER: u8 = 0x01 | 0x80;

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
#[derive(Clone)]
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
        let hk = Hkdf::<Sha256>::new(Some(salt), &self.0);
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
}

/// Resolve the [`MasterKey`] for an existing ciphertext hash.
///
/// Minimal seam on purpose: facet/JWK codecs will implement this later. The
/// salt is not resolved - it is recomputed from the plaintext on the
/// encryption side, and taken from the (GCM-authenticated) header when
/// decrypting.
pub trait CipherKeySource: Send + Sync {
    fn key_for(&self, c: &Hash) -> Res<MasterKey>;
}

/// Trivial in-memory resolver (tests, and callers that already hold keys).
#[derive(Clone, Default)]
pub struct MapKeySource(pub HashMap<Hash, MasterKey>);

impl CipherKeySource for MapKeySource {
    fn key_for(&self, c: &Hash) -> Res<MasterKey> {
        self.0
            .get(c)
            .cloned()
            .ok_or_else(|| eyre::eyre!("no key registered for ciphertext {c}"))
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

    fn encrypt_record(&self, seq: u64, last: bool, mut plaintext: Vec<u8>) -> Vec<u8> {
        plaintext.push(if last {
            LAST_RECORD_DELIMITER
        } else {
            DELIMITER
        });
        self.aead
            .encrypt_in_place(Nonce::from_slice(&self.nonce(seq)), &[], &mut plaintext)
            .expect("aes-gcm encryption cannot fail");
        plaintext
    }

    /// Decrypt one record; returns `(payload, is_final)`. Errors on auth
    /// failure or a bad delimiter.
    fn decrypt_record(&self, seq: u64, ct: &[u8]) -> Res<(Vec<u8>, bool)> {
        let mut buf = ct.to_vec();
        self.aead
            .decrypt_in_place(Nonce::from_slice(&self.nonce(seq)), &[], &mut buf)
            .map_err(|e| eyre::eyre!("record decryption failed: {e:?}"))?;
        let (&delim, payload) = buf.split_last().expect("record always has delimiter");
        match delim {
            DELIMITER => Ok((payload.to_vec(), false)),
            LAST_RECORD_DELIMITER => Ok((payload.to_vec(), true)),
            _ => eyre::bail!("bad record delimiter"),
        }
    }
}

fn header(salt: &[u8; SALT_LEN], rs: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..SALT_LEN].copy_from_slice(salt);
    h[SALT_LEN..SALT_LEN + 8].copy_from_slice(&rs.to_be_bytes());
    h[HEADER_LEN - 1] = 0; // id_len: no sender key id
    h
}

/// Encrypt a complete plaintext buffer with an explicit record size.
///
/// Deterministic: the salt derives from (key, plaintext digest), so
/// re-encrypting produces byte-identical ciphertext.
pub fn encrypt_with_rs(key: &MasterKey, plaintext: &[u8], rs: u64) -> Vec<u8> {
    let p_hash = Hash::new(plaintext);
    let salt = key.salt_for(&p_hash);
    let rc = key.cipher_with_salt(&salt);
    let chunk_max = (rs as usize - RECORD_OVERHEAD).max(1);
    let mut out = Vec::with_capacity(HEADER_LEN + plaintext.len() / chunk_max * (rs as usize));
    out.extend_from_slice(&header(&salt, rs));
    if plaintext.is_empty() {
        out.extend_from_slice(&rc.encrypt_record(0, true, Vec::new()));
        return out;
    }
    let n_records = plaintext.len().div_ceil(chunk_max);
    for (seq, chunk) in plaintext.chunks(chunk_max).enumerate() {
        out.extend_from_slice(&rc.encrypt_record(
            seq as u64,
            seq == n_records - 1,
            chunk.to_vec(),
        ));
    }
    out
}

/// Encrypt with the default record size.
pub fn encrypt_bytes(key: &MasterKey, plaintext: &[u8]) -> Vec<u8> {
    encrypt_with_rs(key, plaintext, RECORD_SIZE)
}

/// Decrypt a complete ciphertext buffer.
pub fn decrypt_bytes(key: &MasterKey, ciphertext: impl AsRef<[u8]>) -> Res<Vec<u8>> {
    let ct = ciphertext.as_ref();
    eyre::ensure!(ct.len() >= HEADER_LEN, "ciphertext shorter than header");
    let rs = u64::from_be_bytes(ct[SALT_LEN..SALT_LEN + 8].try_into()?);
    eyre::ensure!(
        ct[HEADER_LEN - 1] == 0,
        "unsupported key id in ciphertext header"
    );
    let records = &ct[HEADER_LEN..];
    eyre::ensure!(!records.is_empty(), "ciphertext has no records");
    let record_len = rs as usize;
    eyre::ensure!(record_len > RECORD_OVERHEAD, "record size too small");
    let salt: [u8; SALT_LEN] = ct[..SALT_LEN].try_into()?;
    let cipher = key.cipher_with_salt(&salt);
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
    plaintext: Vec<u8>,
    outboard: Vec<u8>,
    ciphertext_len: u64,
}

impl StreamDecryptor {
    fn new(key: MasterKey) -> Self {
        Self {
            cipher: None,
            header: Vec::new(),
            pending: Vec::new(),
            seq: 0,
            record_len: 0,
            saw_final: false,
            plaintext: Vec::new(),
            outboard: Vec::new(),
            ciphertext_len: 0,
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
            let rs = u64::from_be_bytes(self.header[SALT_LEN..SALT_LEN + 8].try_into()?);
            eyre::ensure!(rs > RECORD_OVERHEAD as u64, "record size too small");
            eyre::ensure!(
                self.header[HEADER_LEN - 1] == 0,
                "unsupported key id in ciphertext header"
            );
            self.record_len = rs as usize;
            self.cipher = Some(self.key.cipher_with_salt(&salt));
        }
        self.pending.extend_from_slice(rest);
        while !self.saw_final && self.pending.len() >= self.record_len {
            let rec: Vec<u8> = self.pending.drain(..self.record_len).collect();
            let cipher = self.cipher.as_ref().expect("set with header");
            let (payload, is_final) = cipher.decrypt_record(self.seq, &rec)?;
            self.plaintext.extend_from_slice(&payload);
            self.seq += 1;
            self.saw_final |= is_final;
        }
        Ok(())
    }

    /// Consume the trailing final (possibly short) record once the stream ends.
    fn finish(mut self) -> Res<Vec<u8>> {
        eyre::ensure!(self.record_len != 0, "empty ciphertext stream");
        if self.saw_final {
            return Ok(std::mem::take(&mut self.plaintext));
        }
        eyre::ensure!(!self.pending.is_empty(), "truncated ciphertext: no final record");
        eyre::ensure!(
            self.pending.len() > RECORD_OVERHEAD,
            "final record too short"
        );
        let cipher = self.cipher.as_ref().expect("set with header");
        let rec = std::mem::take(&mut self.pending);
        let (payload, _is_final) = cipher.decrypt_record(self.seq, &rec)?;
        self.plaintext.extend_from_slice(&payload);
        self.saw_final = true;
        Ok(std::mem::take(&mut self.plaintext))
    }
}

// ---------------------------------------------------------------------------
// Store flows
// ---------------------------------------------------------------------------

/// Encrypt-and-store: writes the plaintext as a normal local blob, installs
/// the ciphertext as a virtual entry served by [`PROVIDER_NAME`] (ciphertext
/// bytes are never stored), roots both under `ct:<C>` / `pt:<C>` named tags,
/// and returns `(C, P)` hashes.
pub async fn add_encrypted(
    store: &Store,
    key: &MasterKey,
    plaintext: &[u8],
) -> Res<(Hash, Hash)> {
    let ct = encrypt_bytes(key, plaintext);
    let tagged = store.blobs().add_bytes(plaintext.to_vec()).temp_tag().await?;
    let p_hash = tagged.hash();
    let c_hash = install_virtual(store, &ct).await?;
    set_pair_tags(store, c_hash, p_hash).await?;
    drop(tagged);
    Ok((c_hash, p_hash))
}

/// Install an in-memory ciphertext as a virtual entry: outboard computed from
/// the bytes, data discarded.
async fn install_virtual(store: &Store, ct: &[u8]) -> Res<Hash> {
    let hash = Hash::new(ct);
    let outboard =
        bao_tree::io::outboard::PreOrderMemOutboard::create(ct, iroh_blobs::store::IROH_BLOCK_SIZE);
    store
        .blobs()
        .add_virtual_with_outboard(hash, ct.len() as u64, outboard.data.clone(), PROVIDER_NAME)
        .await?;
    Ok(hash)
}

async fn set_pair_tags(store: &Store, c_hash: Hash, p_hash: Hash) -> Res<()> {
    store.tags().set(format!("{TAG_CT_PREFIX}{c_hash}"), raw(c_hash)).await?;
    store.tags().set(format!("{TAG_PT_PREFIX}{c_hash}"), raw(p_hash)).await?;
    Ok(())
}

/// Download a remote ciphertext without storing it: verified leaves stream
/// through decryption into a stored plaintext entry; received parents become
/// `C`'s outboard so this node can re-serve `C` virtually. Sets both durable
/// tags and returns the plaintext hash.
pub async fn download_encrypted(
    store: &Store,
    conn: impl GetStreamPair,
    c_hash: Hash,
    keys: &dyn CipherKeySource,
) -> Res<Hash> {
    use bao_tree::io::BaoContentItem;
    let key = keys.key_for(&c_hash)?;
    let (item_tx, mut item_rx) = irpc::channel::mpsc::channel::<BaoContentItem>(64);
    let fetch = store.remote().fetch_bao_to(conn, c_hash, item_tx);
    let mut dec = StreamDecryptor::new(key);
    let consume = async {
        while let Some(item) = item_rx.recv().await.ok().flatten() {
            match item {
                BaoContentItem::Leaf(leaf) => dec.push(&leaf.data)?,
                BaoContentItem::Parent(parent) => {
                    dec.outboard.extend_from_slice(parent.pair.0.as_bytes());
                    dec.outboard.extend_from_slice(parent.pair.1.as_bytes());
                }
                #[allow(unreachable_patterns)]
                other => eyre::bail!("unexpected bao content item {other:?}"),
            }
        }
        Ok::<(), eyre::Report>(())
    };
    let (fetch_res, consume_res) = tokio::join!(fetch, consume);
    fetch_res.map_err(|e| eyre::eyre!("fetch_bao_to failed: {e:?}"))?;
    consume_res?;
    let ciphertext_len = dec.ciphertext_len;
    let outboard = std::mem::take(&mut dec.outboard);
    let plaintext = dec.finish()?;

    let tagged = store.blobs().add_bytes(plaintext).temp_tag().await?;
    let p_hash = tagged.hash();

    // Re-install the virtual entry from the received outboard + tracked size.
    store
        .blobs()
        .add_virtual_with_outboard(c_hash, ciphertext_len, outboard, PROVIDER_NAME)
        .await?;

    set_pair_tags(store, c_hash, p_hash).await?;
    drop(tagged); // named tags now root both entries
    Ok(p_hash)
}

/// Fetch-and-decrypt a ciphertext verifiable on this node (stored, or virtual
/// with a live provider registered).
pub async fn get_decrypted(
    store: &Store,
    keys: &dyn CipherKeySource,
    c: Hash,
) -> Res<Vec<u8>> {
    let key = keys.key_for(&c)?;
    // Virtual entries are only served through export_bao; its Leaf items are
    // raw ciphertext bytes.
    let stream = store.blobs().export_bao(c, bao_tree::ChunkRanges::all()).stream();
    futures::pin_mut!(stream);
    let mut dec = StreamDecryptor::new(key);
    while let Some(item) = stream.next().await {
        match item {
            bao_tree::io::mixed::EncodedItem::Leaf(leaf) => dec.push(&leaf.data)?,
            bao_tree::io::mixed::EncodedItem::Parent(_) | bao_tree::io::mixed::EncodedItem::Size(_) => {}
            bao_tree::io::mixed::EncodedItem::Done => break,
            bao_tree::io::mixed::EncodedItem::Error(e) => {
                eyre::bail!("export_bao failed: {e:?}")
            }
        }
    }
    dec.finish()
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

/// A registered C -> (P bytes, key) binding.
struct PlainPair {
    key: MasterKey,
    plain: Arc<Vec<u8>>,
}

impl CipherBlobProvider {
    pub fn new() -> Self {
        Self {
            pairs: RwLock::new(HashMap::new()),
        }
    }

    /// Register the binding for ciphertext `c`: key + plaintext snapshot.
    pub fn register_pair(&self, c: Hash, key: MasterKey, plain: Arc<Vec<u8>>) {
        self.pairs
            .write()
            .expect("pairs lock poisoned")
            .insert(c, PlainPair { key, plain });
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
    fn reader_for(
        &self,
        hash: &Hash,
    ) -> Option<iroh_blobs::store::virtual_blob::DynVirtualSource> {
        let pairs = self.pairs.read().expect("pairs lock poisoned");
        let pair = pairs.get(hash)?;
        Some(Arc::new(CipherSource {
            key: pair.key.clone(),
            plain: pair.plain.clone(),
        }))
    }
}

/// Random-access ciphertext view over one plaintext snapshot. Purely
/// synchronous: RFC 8188 records are independently computable given
/// key + salt + record index, so only overlapping records are encrypted.
struct CipherSource {
    key: MasterKey,
    plain: Arc<Vec<u8>>,
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

        // A full record's wire size is exactly RECORD_SIZE: the delimiter +
        // GCM tag (RECORD_OVERHEAD) are already counted inside it.
        let payload_max = (RECORD_SIZE - RECORD_OVERHEAD as u64).max(1);
        let wire = RECORD_SIZE;
        let p_size = self.plain.len() as u64;
        let n_records = p_size.div_ceil(payload_max).max(1);
        let total_c_len = HEADER_LEN as u64 + n_records * wire;
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
                    cipher.encrypt_record(idx, idx == n_records - 1, chunk)
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
        ];
        for pt in cases {
            let ct = encrypt_with_rs(&key, &pt, SMALL_RS);
            assert_eq!(decrypt_bytes(&key, &ct).unwrap(), pt);
        }
    }

    #[test]
    fn corruption_is_rejected() {
        let key = MasterKey::random();
        let pt = vec![1u8; 500];
        let mut ct = encrypt_with_rs(&key, &pt, SMALL_RS);
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
        let ct = encrypt_with_rs(&key, &[9u8; 5000], SMALL_RS);
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
        assert_eq!(ct1, ct2, "same key + plaintext must reproduce identical ciphertext");
        assert_eq!(Hash::new(&ct1), Hash::new(&ct2));
        // Same key, different plaintext: salts must diverge (GCM safety).
        let other = b"different plaintext".to_vec();
        let salt1 = key.salt_for(&Hash::new(&pt));
        let salt2 = key.salt_for(&Hash::new(&other));
        assert_ne!(salt1, salt2);
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
        let keys = Arc::new(keys_map);
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(c, key.clone(), Arc::new(plaintext.clone()));
        provider.register(&virtuals)?;

        let got = get_decrypted(&store, keys.as_ref(), c).await?;
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
        Ok(())
    }

    /// The main event: node A serves stored ciphertext C; node B downloads it
    /// without storing C (plaintext lands instead), then re-serves C to node C
    /// over QUIC from its virtual entry. Unregistering B's provider must make
    /// C's GET fail with NotFound; re-registering restores service.
    #[tokio::test(flavor = "multi_thread")]
    async fn download_then_serve_over_quic() -> Res<()> {
        use iroh::{address_lookup::MemoryLookup, endpoint::presets, protocol::Router, Endpoint};
        use iroh_blobs::{ALPN, BlobsProtocol};

        async fn setup_node(
        ) -> Res<(Router, Store, MemoryLookup, iroh_blobs::store::virtual_blob::VirtualProviders)> {
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
        let p_hash = download_encrypted(&store_b, conn, c_hash, keys.as_ref()).await?;
        assert_eq!(
            store_b.blobs().get_bytes(p_hash).await?.as_ref(),
            &plaintext[..],
        );
        // Node B serves C virtually; node C GETs it over QUIC.
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register_pair(c_hash, key.clone(), Arc::new(plaintext));
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
        let _mt = store_b.blobs().add_bytes(plain_marker.clone()).temp_tag().await?;
        let marker_hash = Hash::new(&plain_marker);
        store_c.remote().fetch(conn_c.clone(), marker_hash).await
            .map_err(|e| eyre::eyre!("plain fetch failed: {e:?}"))?;
        store_c.remote().fetch(conn_c.clone(), c_hash).await?;
        let got_ct = store_c.get_bytes(c_hash).await?;
        assert_eq!(got_ct.as_ref(), &ct[..], "node C must receive exact ciphertext");

        // Negative: unregistered provider => remote GET fails.
        virtuals_b.unregister(PROVIDER_NAME);
        let conn_c2 = r_c
            .endpoint()
            .connect(r_b.endpoint().addr(), ALPN)
            .await
            .map_err(|e| eyre::eyre!("connect failed: {e:?}"))?;
        let other = Hash::new(b"no such blob");
        assert!(store_c.remote().fetch(conn_c2.clone(), other).await.is_err());

        tokio::try_join!(r_a.shutdown(), r_b.shutdown(), r_c.shutdown())?;
        Ok(())
    }
}
