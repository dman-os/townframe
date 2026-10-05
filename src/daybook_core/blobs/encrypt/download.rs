//! Resumable encrypted downloads.
//!
//! [`download_encrypted`] pulls a remote ciphertext over the network without
//! ever storing it: verified bao leaves stream through decryption straight
//! into a streaming plaintext import — no whole-blob buffering — while
//! received parents become `C`'s outboard, so this node can re-serve `C`
//! virtually after the fact.
//!
//! With a [`FsDownloadLedger`] the download is *resumable*, which is what
//! makes multi-gigabyte files tractable on mobile: decrypted plaintext records
//! are appended to a spill file as they are verified, so an interrupted
//! attempt leaves the completed prefix on disk. The next attempt re-derives
//! its progress watermark from the spill length itself (plaintext arrives in
//! whole records until the final one, so the length *is* the watermark — no
//! separate crash-consistent counter), requests only the missing ciphertext
//! record suffix as a ranged transfer, and imports the spill once the full
//! plaintext has landed.
//!
//! A resumed download re-installs the virtual entry by re-encrypting the
//! completed plaintext rather than reusing received outboard fragments:
//! encryption is deterministic in (key, framing), so the re-derived entry is
//! byte-identical, and a ranged transfer's fragments cannot be reassembled by
//! concatenation the way a full transfer's pre-order stream can (the cost is
//! one extra local sequential read of `P` at completion — a deliberate trade
//! against a second scratch file; see ADR 003 §12 if the trade ever needs
//! revisiting).

use crate::blobs::pair_roots::PairRoots;
use crate::interlude::*;

use iroh_blobs::{
    Hash,
    api::{Store, remote::GetStreamPair},
};

use super::Res;
use super::codec::{HeaderFacts, StreamDecryptor};
use super::keys::CipherKeySource;
use super::params::{EncodingParams, Padding};
use super::serve::CipherBlobProvider;
use super::store::{PROVIDER_NAME, add_progress_to_tag};
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
                    // The importer's own failure is the real error and is
                    // reported by its task; a dead consumer only ends this
                    // fetch, it must not escalate to the process panic handler.
                    p_tx.send(Ok(bytes::Bytes::from(payload)))
                        .await
                        .map_err(|_| {
                            eyre::eyre!("the download's plaintext importer stopped consuming")
                        })?;
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
            .map_err(|_| eyre::eyre!("the download's plaintext importer stopped consuming"))?;
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

    fn dir_for(&self, ct_hash: &Hash) -> std::path::PathBuf {
        self.root
            .join(data_encoding::BASE64URL_NOPAD.encode(ct_hash.as_bytes()))
    }

    pub(crate) fn spill_path(&self, ct_hash: &Hash) -> std::path::PathBuf {
        self.dir_for(ct_hash).join("spill.bin")
    }

    fn meta_path(&self, ct_hash: &Hash) -> std::path::PathBuf {
        self.dir_for(ct_hash).join("meta.bin")
    }

    /// Remove all ledger state for `c` (no-op when nothing is recorded).
    pub fn clear(&self, ct_hash: &Hash) -> Res<()> {
        match std::fs::remove_dir_all(self.dir_for(ct_hash)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Persist the header facts + padding of an in-flight download. Written
    /// once, right after the first attempt parsed the header; the resume
    /// point itself is not stored - it is recovered from the spill length.
    pub(crate) fn write_meta(
        &self,
        ct_hash: &Hash,
        facts: HeaderFacts,
        padding: Padding,
    ) -> Res<()> {
        let dir = self.dir_for(ct_hash);
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
        let path = self.meta_path(ct_hash);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    fn read_meta(&self, ct_hash: &Hash) -> Res<Option<(HeaderFacts, Padding)>> {
        let bytes = match std::fs::read(self.meta_path(ct_hash)) {
            Ok(read) => read,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        eyre::ensure!(
            bytes.len() == LEDGER_META_LEN,
            "corrupt download ledger meta"
        );
        eyre::ensure!(
            bytes[..LEDGER_META.len()] == LEDGER_META[..],
            "foreign download ledger meta"
        );
        let salt = bytes[5..21].try_into().expect("16 salt octets");
        let rs = u32::from_be_bytes(bytes[21..25].try_into().unwrap());
        // The ledger meta was written by our own downloads, but it is local
        // disk and it was not authenticated: the same header-`rs` bound every
        // other decode seam applies here too, before any stride math trusts it.
        crate::blobs::encrypt::params::validate_record_size(u64::from(rs))?;
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
/// concatenation the way a full transfer's pre-order stream can (see the
/// module docs + ADR 003 §12).
///
/// The ledger state (meta + spill) is deleted once the plaintext is durably
/// imported and tagged.
pub async fn download_encrypted(
    store: &Store,
    provider: &CipherBlobProvider,
    roots: &PairRoots,
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
        Some(ledger) => {
            let path = ledger.spill_path(&c_hash);
            std::fs::create_dir_all(path.parent().expect("spill has a parent"))?;
            Some((ledger, path, ledger.read_meta(&c_hash)?))
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
                .map(|meta| meta.len())
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
                // The resume watermark is re-established by `set_len` below;
                // nothing here may truncate the spill being resumed.
                .truncate(false)
                .open(&path)
                .await?;
            file.set_len(seq * payload).await?;
            file.seek(SeekFrom::Start(seq * payload)).await?;
            // Reproduce the ciphertext that was actually downloaded: its record
            // size comes from its own authenticated header, and only the
            // padding policy - which the wire does not carry - comes from the
            // facet.
            let encoding = EncodingParams::new(u64::from(facts.rs), padding)?;
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
        .and_then(|(_, facts, seq)| {
            facts
                .as_ref()
                .map(|header_facts| header_facts.record_start(*seq))
        })
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
    let ledger_consume = ledger.map(|ledger| LedgerConsume {
        ledger,
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
            fetch_res.map_err(|err| eyre::eyre!("fetch_bao_to failed: {err:?}"))?;
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
            fetch_res.map_err(|err| eyre::eyre!("fetch_bao_to failed: {err:?}"))?;
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
            .register_pair(store, roots, c_hash, &key, p_hash, encoding)
            .await?;
    } else {
        // Resume: re-derive C deterministically (see the fn doc + ADR 003 §12
        // for why the received fragments are not reused).
        let c2 = provider
            .install(store, roots, &key, p_hash, encoding)
            .await?;
        eyre::ensure!(
            c2 == c_hash,
            "re-encrypted ciphertext {c2} differs from the downloaded {c_hash}: corrupt spill?"
        );
    }

    if let Some(ledger) = ledger {
        ledger.clear(&c_hash)?;
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
            let read_bytes = file.read(&mut buf).await?;
            if read_bytes == 0 {
                break;
            }
            if tx
                .send(Ok(buf.split_to(read_bytes).freeze()))
                .await
                .is_err()
            {
                break; // import side gone; its result surfaces the cause
            }
        }
        eyre::Ok(())
    };
    let handle = tokio::spawn(reader);
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
