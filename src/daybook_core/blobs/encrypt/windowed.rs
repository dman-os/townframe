//! Random-access *plaintext* reads over a stored ciphertext: the inverse of
//! serving (which derives ciphertext windows from stored plaintext).
//!
//! RFC 8188 derives each record's nonce from its sequence number and
//! authenticates it on its own, so decryption is random-access: a read
//! touches only the records its range overlaps. That is what lets a
//! multi-gigabyte encrypted video start playing without decrypting — or even
//! reading — the whole thing, and it bounds memory by the requested range
//! rather than by the file.
//!
//! Framing comes from the ciphertext's own authenticated header, so a reader
//! never needs the facet's `encodingParameters`. The plaintext length is the
//! one thing the wire does not carry (padding deliberately hides the tail),
//! so [`CipherReader::open`] takes it from caller metadata and cross-checks
//! what actually decrypts against it in both directions — a wrong length
//! otherwise shows up as a silently truncated file.

use crate::interlude::*;

use bao_tree::io::mixed::ReadBytesAt as _;
use bytes::Bytes;
use iroh_blobs::{Hash, api::Store, store::virtual_blob::SyncReader};

use super::Res;
use super::codec::{
    Cipher, HEADER_LEN, RECORD_OVERHEAD, SALT_LEN, cipher_from_ikm, n_records_for, payload_size,
};
use super::keys::CipherKeySource;
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
    /// Open a reader over the stored ciphertext `ct_hash`, which the caller's
    /// metadata says decrypts to `p_len` plaintext octets.
    ///
    /// Fails if `ct_hash` has no readable local data, or if that data is too short to
    /// hold `p_len` octets under its own framing.
    pub async fn open(
        store: &Store,
        keys: &dyn CipherKeySource,
        ct_hash: Hash,
        p_len: u64,
    ) -> Res<Self> {
        let key = keys.key_for(&ct_hash).await?;
        let Some(reader) = store.sync_reader(ct_hash).await? else {
            eyre::bail!("cannot read {ct_hash}: no readable stored data");
        };
        eyre::ensure!(
            reader.len() >= HEADER_LEN as u64,
            "ciphertext {ct_hash} is shorter than an RFC 8188 header"
        );
        let head = reader.read_bytes_at(0, HEADER_LEN)?;
        let salt: [u8; SALT_LEN] = head[..SALT_LEN].try_into()?;
        let rs = u64::from(u32::from_be_bytes(head[SALT_LEN..SALT_LEN + 4].try_into()?));
        // Foreign encoders may carry a key id; we never encode one, but the
        // record stream starts after it.
        let records_start = HEADER_LEN as u64 + u64::from(head[HEADER_LEN - 1]);
        eyre::ensure!(
            reader.len() > records_start,
            "ciphertext {ct_hash} carries no records"
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
            "ciphertext {ct_hash} holds {} octets, too few for {p_len} plaintext octets",
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
            c_hash: ct_hash,
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
