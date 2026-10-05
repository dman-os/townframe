//! Pure buffered RFC 8188 `aes128gcm` encryption and decryption.
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
//! 21-byte header (salt + record size + key-id length), then one AES-GCM
//! record per plaintext chunk. A record is its payload chunk plus a delimiter
//! byte (0x01, or 0x02 on the final record) plus the 16-byte GCM tag. Nonces
//! are the header salt's nonce base XORed with the big-endian sequence
//! number, so records are independently computable and independently
//! authenticated — that is what makes random-access serving and windowed
//! decryption possible at all.

use crate::interlude::*;

use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes128Gcm, Nonce,
};
use hkdf::Hkdf;
use iroh_blobs::Hash;
use sha2::Sha256;

use super::keys::MasterKey;
use super::params::{validate_record_size, EncodingParams, Padding, DEFAULT_PADDING, RECORD_SIZE};
use super::Res;
pub(crate) const SALT_LEN: usize = 16;
/// GCM tag (16) + record delimiter byte (1).
pub(crate) const RECORD_OVERHEAD: usize = 17;

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
    fn encrypt_record(
        &self,
        seq: u64,
        last: bool,
        plaintext: &[u8],
        pad_zeros: usize,
        out: &mut Vec<u8>,
    ) {
        let start = out.len();
        out.extend_from_slice(plaintext);
        out.push(if last {
            LAST_RECORD_DELIMITER
        } else {
            DELIMITER
        });
        out.resize(out.len() + pad_zeros, 0);
        let tag = self
            .aead
            .encrypt_in_place_detached(Nonce::from_slice(&self.nonce(seq)), &[], &mut out[start..])
            .expect("aes-gcm encryption cannot fail");
        out.extend_from_slice(&tag);
    }

    /// Decrypt into the shared output buffer and return whether this record is final.
    /// Errors on authentication failure or a bad delimiter. RFC 8188 padding: the delimiter is the
    /// last non-zero octet; foreign encoders may pad after it.
    fn decrypt_record(&self, seq: u64, ct: &[u8], out: &mut Vec<u8>) -> Res<bool> {
        let start = out.len();
        let tag_start = ct.len() - 16;
        out.extend_from_slice(&ct[..tag_start]);
        self.aead
            .decrypt_in_place_detached(
                Nonce::from_slice(&self.nonce(seq)),
                &[],
                &mut out[start..],
                aes_gcm::Tag::from_slice(&ct[tag_start..]),
            )
            .map_err(|err| eyre::eyre!("record decryption failed: {err:?}"))?;
        let Some(pos) = out[start..].iter().rposition(|&octet| octet != 0) else {
            eyre::bail!("record contains no delimiter octet");
        };
        let is_final = match out[start + pos] {
            DELIMITER => false,
            LAST_RECORD_DELIMITER => true,
            other => eyre::bail!("bad record delimiter {other:#x}"),
        };
        out.truncate(start + pos);
        Ok(is_final)
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
    let framing = EncodingParams {
        record_size: rs,
        padding,
    };
    encrypt_raw_ikm(
        &key.0,
        &key.salt_for(&p_hash, &framing),
        rs,
        padding,
        plaintext,
    )
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
    validate_record_size(rs).expect("encryption framing is caller-validated");
    let rc = cipher_from_ikm(ikm, salt);
    let payload_max = rs as usize - RECORD_OVERHEAD;
    let n_records = if plaintext.is_empty() {
        1
    } else {
        plaintext.len().div_ceil(payload_max)
    };
    let wire_len = match padding {
        Padding::Record => n_records * rs as usize,
        Padding::Minimal => plaintext.len() + n_records * RECORD_OVERHEAD,
    };
    let mut out = Vec::with_capacity(HEADER_LEN + wire_len);
    out.extend_from_slice(&header(salt, rs));
    // Non-final records always carry a full payload chunk and no padding;
    // their delimiter fills the frame to exactly `rs` wire bytes.
    let full = n_records - 1;
    for rec_index in 0..full {
        let start = rec_index * payload_max;
        rc.encrypt_record(
            rec_index as u64,
            false,
            &plaintext[start..start + payload_max],
            0,
            &mut out,
        );
    }
    // The final record carries the tail (possibly empty) plus the policy's
    // padding: a full frame under [`Padding::Record`], delimiter-only under
    // [`Padding::Minimal`].
    let final_content = &plaintext[full * payload_max..];
    let pad_zeros = match padding {
        Padding::Minimal => 0,
        Padding::Record => payload_max - final_content.len(),
    };
    rc.encrypt_record(full as u64, true, final_content, pad_zeros, &mut out);
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
    // The header's rs octets are pre-auth at this point: anything the framing
    // math below assumes (buffering, pacing, record alignment) must be checked
    // through the shared rule before use.
    validate_record_size(rs)?;
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
        saw_final = cipher.decrypt_record(seq, &records[offset..offset + take], &mut out)?;
        offset += take;
        seq += 1;
    }
    eyre::ensure!(saw_final, "ciphertext does not end with a final record");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_boundaries_preserve_zero_bytes_and_reject_truncation_reordering_and_tampering() {
        let key = MasterKey([42; 32]);
        let rs = 64u64;
        let payload = rs as usize - RECORD_OVERHEAD;
        for padding in [Padding::Minimal, Padding::Record] {
            for length in [0, 1, payload - 1, payload, payload + 1, 3 * payload] {
                let mut plaintext = vec![0; length];
                for (index, byte) in plaintext.iter_mut().enumerate() {
                    *byte = (index % 5) as u8;
                }
                let ciphertext = encrypt_with_rs(&key, &plaintext, rs, padding);
                assert_eq!(decrypt_bytes(&key, &ciphertext).unwrap(), plaintext);
                let mut corrupted = ciphertext.clone();
                corrupted[HEADER_LEN] ^= 1;
                assert!(decrypt_bytes(&key, corrupted).is_err());
                assert!(decrypt_bytes(&key, &ciphertext[..ciphertext.len() - 1]).is_err());
                if length > payload {
                    assert!(decrypt_bytes(&key, &ciphertext[..HEADER_LEN + rs as usize]).is_err());
                }
                if length == 3 * payload {
                    let mut reordered = ciphertext.clone();
                    for index in 0..rs as usize {
                        reordered.swap(HEADER_LEN + index, HEADER_LEN + rs as usize + index);
                    }
                    assert!(decrypt_bytes(&key, reordered).is_err());
                }
                let mut trailing = ciphertext;
                trailing.push(0);
                assert!(decrypt_bytes(&key, trailing).is_err());
            }
        }
    }
}
