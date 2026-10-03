//! On-demand serving of virtual ciphertext entries.
//!
//! [`CipherBlobProvider`] is the registry behind iroh-blobs' virtual entries:
//! a ciphertext `C` was never stored anywhere, so when the store is asked for
//! `C`'s bytes, `reader_for(C)` looks up the registered pair `(key, plaintext
//! reader)` and returns a random-access source that encrypts only the records
//! overlapping each requested byte window. The plaintext is not held in
//! memory — the pair reads it back through a synchronous reader over the
//! store's own storage for `P` — so serving a multi-gigabyte blob costs one
//! RFC 8188 record of memory, and serving is exactly as durable as `P` is.
//!
//! Two lifecycle facts to keep in mind:
//!
//! - Registering a pair is what it means to serve `C` at all, so
//!   [`CipherBlobProvider::register_pair`] also roots both entries under the
//!   `ct:`/`pt:` named tags (see [`super::store::set_pair_tags`]). Installing
//!   a virtual entry without registering leaves an unnamed entry the store
//!   serves as not-found — [`CipherBlobProvider::install`] does both acts.
//! - The pair holds no bytes, only a reader into the store; if `P` is later
//!   collected, reads fail rather than serve stale bytes.

use crate::interlude::*;

use bao_tree::io::mixed::ReadBytesAt as _;
use bytes::Bytes;
use iroh_blobs::{Hash, api::Store, store::virtual_blob::SyncReader};
use std::io;
use std::sync::Mutex;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use super::Res;
use super::codec::{Cipher, HEADER_LEN, ciphertext_len, header, n_records_for, payload_size};
use super::keys::MasterKey;
use super::params::{EncodingParams, Padding};
use super::store::{PROVIDER_NAME, install_virtual_encrypted, set_pair_tags};
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
        let salt = key.salt_for(&p_hash, &encoding);
        let payload_max = payload_size(encoding.record_size);
        let p_len = reader.len();
        Self {
            cipher: key.cipher_for(&p_hash, &encoding),
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
        if let Some((cached, ct)) = self.cache.lock().expect("cache lock poisoned").as_ref()
            && *cached == idx
        {
            return Ok(ct.clone());
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
        c_hash: Hash,
        key: &MasterKey,
        p_hash: Hash,
        encoding: EncodingParams,
    ) -> Res<()> {
        let Some(reader) = store.sync_reader(p_hash).await? else {
            eyre::bail!("cannot serve {c_hash}: plaintext {p_hash} has no readable stored data");
        };
        set_pair_tags(store, c_hash, p_hash).await?;
        let pair = Arc::new(PlainPair::new(key, p_hash, reader, encoding));
        self.pairs
            .write()
            .expect("pairs lock poisoned")
            .insert(c_hash, pair);
        Ok(())
    }

    /// Make `c` servable in one step: run the encrypt pass over the stored
    /// plaintext ([`install_virtual_encrypted`]), then register the resulting
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
        let c_hash = install_virtual_encrypted(store, key, p_hash, encoding).await?;
        self.register_pair(store, c_hash, key, p_hash, encoding)
            .await?;
        Ok(c_hash)
    }

    /// Register this provider under [`PROVIDER_NAME`] on a live registry.
    pub fn register(
        self: &Arc<Self>,
        virtuals: &iroh_blobs::store::virtual_blob::VirtualProviders,
    ) -> std::io::Result<()> {
        let this = std::sync::Arc::clone(self);
        virtuals.register(PROVIDER_NAME, this)
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
            pair: std::sync::Arc::clone(pair),
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::{RECORD_SIZE, encrypt_with_rs};

    /// The store's bao leaf size: the granularity its export path reads the
    /// ciphertext at, and therefore the reads a server actually issues.
    /// `iroh_blobs::store::IROH_BLOCK_SIZE` is chunk-log 4, so 2^14 octets.
    const BAO_LEAF_SIZE: usize = 16 * 1024;

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
            let src = CipherSource {
                pair: Arc::clone(&pair),
            };
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
}
