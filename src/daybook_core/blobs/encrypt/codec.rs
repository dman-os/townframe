//! The pure RFC 8188 `aes128gcm` codec: header construction, per-record
//! encryption and decryption, and the incremental encryptor/decryptor pair
//! used by the streaming store flows.
//!
//! Everything here knows crypto and wire framing, and nothing else: no store,
//! no filesystem, no network. Framing is the [`EncodingParams`] type; the
//! master key and its salt derivation live next door in [`super::keys`]. The
//! buffered form is deterministic — the salt is derived from (key, plaintext
//! digest), so encrypting the same bytes under the same key and framing
//! reproduces the ciphertext byte-for-byte and a ciphertext's identity can be
//! re-derived at will.
//!
//! Record layout, for orientation: an RFC 8188 `aes128gcm` payload is a
//! 26-byte header (salt + record size + key-id length), then one AES-GCM
//! record per plaintext chunk. A record is its payload chunk plus a delimiter
//! byte (0x01, or 0x02 on the final record) plus the 16-byte GCM tag. Nonces
//! are the header salt's nonce base XORed with the big-endian sequence
//! number, so records are independently computable and independently
//! authenticated — that is what makes random-access serving and windowed
//! decryption possible at all.

use crate::interlude::*;

use aes_gcm::{
    Aes128Gcm, Nonce,
    aead::{AeadInPlace, KeyInit},
};
use hkdf::Hkdf;
use iroh_blobs::Hash;
use sha2::Sha256;

use super::Res;
use super::keys::MasterKey;
use super::params::{DEFAULT_PADDING, EncodingParams, Padding, RECORD_SIZE};
pub(crate) const SALT_LEN: usize = 16;
/// GCM tag (16) + record delimiter byte (1).
pub(crate) const RECORD_OVERHEAD: usize = 17;

/// Payload octets per record: what is left of `rs` after the GCM tag and the
/// delimiter. Callers that accept `rs` from outside validate it first
/// ([`EncodingParams::new`]); the clamp only keeps a nonsensical internal `rs`
/// from panicking on underflow.
pub(crate) fn payload_size(rs: u64) -> u64 {
    (rs - RECORD_OVERHEAD as u64).max(1)
}

/// RFC 8188 header length: 16 salt octets + 4 record-size octets + 1 key-id
/// length octet. We never encode a key id (zero length); foreign key ids are
/// tolerated and skipped when decoding.
pub(crate) const HEADER_LEN: usize = SALT_LEN + 4 + 1;
pub(crate) const KEY_LEN: usize = 16;
pub(crate) const NONCE_LEN: usize = 12;
pub(crate) const MASTER_KEY_LEN: usize = 32;

pub(crate) const CEK_INFO: &[u8] = b"Content-Encoding: aes128gcm\x00";
pub(crate) const NONCE_INFO: &[u8] = b"Content-Encoding: nonce\x00";
pub(crate) const DELIMITER: u8 = 0x01;
pub(crate) const LAST_RECORD_DELIMITER: u8 = 0x02;

/// Derive the record cipher from input-keying material: the CEK and the
/// nonce base are HKDF-SHA256 expansions under the (header-carried) salt,
/// exactly as RFC 8188 §2.2/§2.3 prescribes. Decryptors run this against the
/// salt read from the authenticated header; encryptors against the derived
/// salt.
pub(crate) fn cipher_from_ikm(ikm: &[u8], salt: &[u8; SALT_LEN]) -> Cipher {
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

pub(crate) struct Cipher {
    aead: Aes128Gcm,
    nonce_base: [u8; NONCE_LEN],
}

impl Cipher {
    fn nonce(&self, seq: u64) -> [u8; NONCE_LEN] {
        let mut masked = self.nonce_base;
        for (idx, octet) in seq.to_be_bytes().iter().enumerate() {
            masked[4 + idx] ^= *octet;
        }
        masked
    }

    /// Encrypt one record: `plaintext` is the record content (without the
    /// delimiter); `pad_zeros` zero octets follow the delimiter so the
    /// AEAD input reaches the chosen framing length.
    pub(crate) fn encrypt_record(
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
    pub(crate) fn decrypt_record(&self, seq: u64, ct: &[u8]) -> Res<(Vec<u8>, bool)> {
        let mut buf = ct.to_vec();
        self.aead
            .decrypt_in_place(Nonce::from_slice(&self.nonce(seq)), &[], &mut buf)
            .map_err(|err| eyre::eyre!("record decryption failed: {err:?}"))?;
        let Some(pos) = buf.iter().rposition(|&octet| octet != 0) else {
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

pub(crate) fn header(salt: &[u8; SALT_LEN], rs: u64) -> [u8; HEADER_LEN] {
    let mut hdr = [0u8; HEADER_LEN];
    hdr[..SALT_LEN].copy_from_slice(salt);
    hdr[SALT_LEN..SALT_LEN + 4].copy_from_slice(&(rs as u32).to_be_bytes());
    hdr[HEADER_LEN - 1] = 0; // id_len: no sender key id
    hdr
}

/// Encrypt a complete plaintext buffer with an explicit record size and
/// padding policy. Deterministic: the salt derives from (key, plaintext
/// digest), so re-encrypting produces byte-identical ciphertext for the same
/// framing. (Padding policies are an inventory-privacy choice; see ADR 003
/// §17.)
pub fn encrypt_with_rs(key: &MasterKey, plaintext: &[u8], rs: u64, padding: Padding) -> Vec<u8> {
    let p_hash = Hash::new(plaintext);
    encrypt_raw_ikm(&key.0, &key.salt_for(&p_hash), rs, padding, plaintext)
}

/// [`encrypt_with_rs`] with an explicit RFC 8188 (ikm, salt) - the shape
/// interop tests and reference vectors exercise.
pub(crate) fn encrypt_raw_ikm(
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
    for rec_index in 0..full {
        let start = rec_index * payload_max;
        let chunk = plaintext[start..start + payload_max].to_vec();
        out.extend_from_slice(&rc.encrypt_record(rec_index as u64, false, chunk, 0));
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
pub(crate) fn decrypt_bytes_ikm(ikm: &[u8], ciphertext: impl AsRef<[u8]>) -> Res<Vec<u8>> {
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

/// Incremental decryptor fed arbitrary ciphertext byte chunks (bao leaves are
/// chunk-aligned, not record-aligned, so internal buffering is required).
///
/// The master key must be supplied up front; the CEK derives from the salt
/// carried in the (GCM-authenticated) header.
pub(crate) struct StreamDecryptor {
    key: MasterKey,
    cipher: Option<Cipher>,
    header: Vec<u8>,
    pending: Vec<u8>,
    seq: u64,
    record_len: usize,
    saw_final: bool,
    outbox: Vec<Vec<u8>>,
    pub(crate) outboard: Vec<u8>,
    pub(crate) ciphertext_len: u64,
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
pub(crate) struct HeaderFacts {
    pub(crate) salt: [u8; SALT_LEN],
    pub(crate) rs: u32,
    pub(crate) idlen: u8,
}

impl HeaderFacts {
    /// plaintext octets carried by a full (non-final) record
    pub(crate) fn payload_max(&self) -> u64 {
        self.rs as u64 - RECORD_OVERHEAD as u64
    }
    /// first byte offset of record `seq` in the ciphertext
    pub(crate) fn record_start(&self, seq: u64) -> u64 {
        HEADER_LEN as u64 + self.idlen as u64 + seq * self.rs as u64
    }
}

impl StreamDecryptor {
    /// A decryptor for a fresh stream: parses the header from it.
    pub(crate) fn new(key: MasterKey) -> Self {
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
    pub(crate) fn header_facts(&self) -> Option<HeaderFacts> {
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
    pub(crate) fn resumed(key: MasterKey, facts: HeaderFacts, next_seq: u64) -> Self {
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

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Res<()> {
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
    pub(crate) fn drain_outbox(&mut self) -> std::vec::IntoIter<Vec<u8>> {
        std::mem::take(&mut self.outbox).into_iter()
    }

    /// Consume the trailing final (possibly short) record once the stream
    /// ends. After this the outbox holds the record's payload; take it before
    /// dropping the decryptor.
    pub(crate) fn finish(&mut self) -> Res<()> {
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
    pub(crate) fn into_plaintext(mut self) -> Vec<u8> {
        let mut out = Vec::new();
        for payload in self.drain_outbox() {
            out.extend_from_slice(&payload);
        }
        out
    }
}

/// Streaming encryptor over a plaintext byte source: emits the RFC 8188
/// header, then whole ciphertext records as their plaintext arrives; the
/// final record at the end of stream, padded per the chosen policy.
pub(crate) struct RecordEncryptor {
    salt: [u8; SALT_LEN],
    cipher: Cipher,
    encoding: EncodingParams,
    pending: Vec<u8>,
    seq: u64,
    payload_max: usize,
    emitted_final: bool,
}

impl RecordEncryptor {
    pub(crate) fn new(key: &MasterKey, p_hash: &Hash, encoding: EncodingParams) -> Self {
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

    pub(crate) fn bake_header(&self) -> Vec<u8> {
        header(&self.salt, self.encoding.record_size).to_vec()
    }

    /// Absorb plaintext bytes; returns ciphertext wire bytes (never partial
    /// records - each emitted block is whole records or nothing, except the
    /// header which goes out first.
    ///
    /// A chunk that exactly fills a record is held back unencrypted until
    /// more data arrives or the stream ends - the final record is decided
    /// only at end of plaintext.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
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
    pub(crate) fn finish(&mut self) -> Vec<Vec<u8>> {
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
pub(crate) fn n_records_for(p_len: u64, payload_max: u64) -> u64 {
    if p_len == 0 {
        1
    } else {
        p_len.div_ceil(payload_max)
    }
}

/// Total encoded body length (header + records) for `p_len` plaintext octets
/// under `encoding`.
pub(crate) fn ciphertext_len(p_len: u64, encoding: EncodingParams) -> u64 {
    let payload_max = payload_size(encoding.record_size);
    let record_count = n_records_for(p_len, payload_max);
    let final_content = p_len - (record_count - 1) * payload_max;
    let final_wire = match encoding.padding {
        // Minimal: content + delimiter + tag (a full-content final record is
        // exactly `rs` wire bytes - content + delimiter = rs - 16).
        Padding::Minimal => final_content + RECORD_OVERHEAD as u64,
        // Record: the final frame is padded up to full record size.
        Padding::Record => encoding.record_size,
    };
    HEADER_LEN as u64 + (record_count - 1) * encoding.record_size + final_wire
}
