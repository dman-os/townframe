//! Store-side flows over iroh-blobs: importing plaintext, installing the
//! virtual ciphertext entry ("the encrypt pass"), rooting and releasing the
//! `(C, P)` pair, and the fetch-and-decrypt helper.
//!
//! The model, in one paragraph: a *plaintext* blob `P` is a normal stored
//! entry. Its ciphertext `C` is a *virtual* store entry — the store keeps
//! only `C`'s bao outboard and a provider name, never the bytes — so serving
//! `C` re-encrypts `P` on demand and ciphertext is never materialized
//! locally. A pair is kept alive (and later released) through two durable
//! named tags, `ct:<C>` → `C` and `pt:<C>` → `P`: named tags are exactly what
//! the store's garbage collector seeds its root set from, so a registered
//! pair cannot be collected out from under a reader, and deleting the two
//! tags is the *only* thing that releases it. Nothing removes them
//! incidentally; releasing is a deliberate act that belongs to whatever
//! derives pins from the facets naming the blob.
//!
//! Every flow here is streaming end to end: import consumes a plaintext
//! stream (never a whole-file buffer), the install pass re-reads stored `P`
//! in one incremental sweep while re-hashing what it reads (proving the
//! (key, digest) binding) and feeding a streaming bao outboard — ciphertext
//! bytes are never stored, spilt to disk, or accumulated. Memory ceiling: one
//! RFC 8188 record plus a pipe buffer plus the outboard (~1/512 of the blob),
//! regardless of blob size, on any store backend.
//!
//! Key material is never persisted in the store: the facet graph resolves it
//! (see [`super::keys::CipherKeySource`]), and the salt is re-derived from
//! the plaintext digest when encrypting (see [`MasterKey::salt_for`]).

use crate::blobs::pair_roots::PairRoots;
use crate::interlude::*;

use iroh_blobs::{BlobFormat, Hash, HashAndFormat, api::Store};

use super::Res;
use super::codec::{RecordEncryptor, StreamDecryptor, ciphertext_len};
use super::keys::{CipherKeySource, MasterKey};
use super::params::EncodingParams;
use super::serve::CipherBlobProvider;

pub const PROVIDER_NAME: &str = "daybook-cipherblob-v1";
pub const TAG_CT_PREFIX: &str = "ct:";
pub const TAG_PT_PREFIX: &str = "pt:";

fn raw(hash: Hash) -> HashAndFormat {
    HashAndFormat {
        hash,
        format: BlobFormat::Raw,
    }
}

/// Root the (C, P) linkage so both entries survive GC: `ct:<C>` → `C`,
/// `pt:<C>` → `P`. Key linkage is facet-level, not store-level: the store
/// never holds key material, so these tags carry no secrets to leak.
///
/// These are durable named tags, and named tags are exactly what the store's
/// GC treats as roots: its mark phase walks `tags().list()` and seeds the live
/// set with every tag's hash before sweeping anything unreached. So a
/// registered pair cannot be collected out from under a reader, and — the
/// other half of the same fact — deleting these two tags is the *only* thing
/// that releases the pair. Nothing removes them incidentally; releasing a
/// cipherblob is a deliberate act (see [`drop_pair_tags`]).
pub(crate) async fn set_pair_tags(store: &Store, c_hash: Hash, p_hash: Hash) -> Res<()> {
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
/// Both tag names carry `C`, so releasing needs no (C, P) map — the `pt:<C>`
/// tag records which plaintext it roots, and nothing else reads that back.
/// Deletion is the release: a tag that is never deleted pins the ciphertext's
/// outboard *and* the plaintext that serves it forever, because named tags are
/// what the store's GC mark phase seeds its root set from. This is therefore
/// the deliberate counterpart of [`set_pair_tags`], driven by whatever derives
/// pins from the facets that name the blob: the pin worker deletes the tags
/// when a ciphertext pin leaves the encrypted-representation inventory.
/// Deleting an absent tag is a no-op, so re-running is safe.
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

/// Whether either half of a pair is still rooted.
///
/// The boot drain asks this first: a row whose tags are already gone describes a
/// pair that was released before the crash that left the row, so there is nothing
/// left to release and the row is only stale. A read, not a guard - deleting an
/// absent tag is a no-op.
pub(crate) async fn has_pair_tags(store: &Store, c_hash: Hash) -> Res<bool> {
    if store
        .tags()
        .get(format!("{TAG_CT_PREFIX}{c_hash}"))
        .await?
        .is_some()
    {
        return Ok(true);
    }
    Ok(store
        .tags()
        .get(format!("{TAG_PT_PREFIX}{c_hash}"))
        .await?
        .is_some())
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
/// buffered whole), then [`CipherBlobProvider::install`] runs the encrypt pass
/// and makes `C` servable, which roots `C` and `P` under named tags (see
/// [`set_pair_tags`]). Returns `(C, P)` hashes.
pub async fn add_encrypted_stream<S>(
    store: &Store,
    provider: &CipherBlobProvider,
    roots: &PairRoots,
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
    let c_hash = match provider.install(store, roots, key, p_hash, encoding).await {
        Ok(c_hash) => c_hash,
        Err(err) => {
            drop(p_tag);
            return Err(err);
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
    roots: &PairRoots,
    key: &MasterKey,
    encoding: EncodingParams,
    plaintext: &[u8],
) -> Res<(Hash, Hash)> {
    let chunk = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(
        plaintext.to_vec(),
    ))]);
    add_encrypted_stream(store, provider, roots, key, encoding, chunk).await
}

/// Drive an add/import progress stream to completion, returning its temp tag.
pub(crate) async fn add_progress_to_tag(
    progress: iroh_blobs::api::blobs::AddProgress<'_>,
) -> Res<iroh_blobs::api::TempTag> {
    use futures::StreamExt;
    let stream = progress.stream().await;
    futures::pin_mut!(stream);
    loop {
        match stream.next().await {
            Some(iroh_blobs::api::proto::AddProgressItem::Done(tag)) => return Ok(tag),
            Some(iroh_blobs::api::proto::AddProgressItem::Error(err)) => {
                return Err(eyre::eyre!("import failed: {err}"));
            }
            Some(_) => {}
            None => eyre::bail!("import progress stream ended without completion"),
        }
    }
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
pub(crate) async fn install_virtual_encrypted(
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
        Some(EncodedItem::Error(err)) => eyre::bail!("export of {p_hash} failed: {err:?}"),
        other => eyre::bail!("export of {p_hash} did not announce a size: {other:?}"),
    };
    let c_len = ciphertext_len(p_len, encoding);
    let tree = bao_tree::BaoTree::new(c_len, iroh_blobs::store::IROH_BLOCK_SIZE);
    let outboard_len = tree.outboard_size() as usize;

    // Producer: re-hash the plaintext while encrypting; write the ciphertext
    // wire bytes (header + whole records) into the pipe. The producer OWNS the
    // pipe writer (async move, never a borrow): ending the future on any exit
    // - the `?` bails below included - drops `c_tx` and so closes the pipe,
    // which is what tells the consumer's bao tree the input ended. A writer
    // merely borrowed from this scope would outlive an early producer exit and
    // only drop when this function returns - after the join - so the spawned
    // blocking consumer would wait for an EOF that never comes while the
    // install runs: create/rotate/reuse and resume installs deadlock.
    let enc = RecordEncryptor::new(key, &p_hash, encoding);
    let (mut c_tx, c_rx) = tokio::io::duplex(64 * 1024);
    let (rebuilt_tx, rebuilt_rx) = tokio::sync::oneshot::channel::<blake3::Hash>();
    let producer = async move {
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
                EncodedItem::Error(err) => eyre::bail!("export of {p_hash} failed: {err:?}"),
                EncodedItem::Parent(_) | EncodedItem::Size(_) | EncodedItem::Done => {}
            }
        }
        let rebuilt = p_hasher.finalize();
        for rec in enc.finish() {
            c_tx.write_all(&rec).await?;
        }
        c_tx.shutdown().await?;
        // The only failure mode is a dropped receiver, which cannot happen:
        // the digest is read from `rebuilt_rx` right after the join below.
        let _sent: Result<(), blake3::Hash> = rebuilt_tx.send(rebuilt);
        Ok::<(), eyre::Report>(())
    };

    // Consumer: incremental bao hashing over the ciphertext stream. Purely
    // synchronous (bao_tree's streaming outboard); bridged off the async
    // producer through the duplex pipe - its EOF is the producer's drop of
    // `c_tx`, never this scope's. Peak memory: chunk-group buffer +
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

    let (producer_res, consumer_res) = tokio::join!(producer, consumer);
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

/// Fetch-and-decrypt a ciphertext verifiable on this node (stored, or virtual
/// with a live provider registered).
pub async fn get_decrypted(
    store: &Store,
    keys: &dyn CipherKeySource,
    ct_hash: Hash,
) -> Res<Vec<u8>> {
    let key = keys.key_for(&ct_hash).await?;
    // Virtual entries are only served through export_bao; its Leaf items are
    // raw ciphertext bytes.
    let stream = store
        .blobs()
        .export_bao(ct_hash, bao_tree::ChunkRanges::all())
        .stream();
    futures::pin_mut!(stream);
    let mut dec = StreamDecryptor::new(key);
    while let Some(item) = stream.next().await {
        match item {
            bao_tree::io::mixed::EncodedItem::Leaf(leaf) => dec.push(&leaf.data)?,
            bao_tree::io::mixed::EncodedItem::Parent(_)
            | bao_tree::io::mixed::EncodedItem::Size(_) => {}
            bao_tree::io::mixed::EncodedItem::Done => break,
            bao_tree::io::mixed::EncodedItem::Error(err) => {
                eyre::bail!("export_bao failed: {err:?}")
            }
        }
    }
    dec.finish()?;
    Ok(dec.into_plaintext())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io;
    use std::sync::Arc;

    use bao_tree::io::mixed::ReadBytesAt;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use iroh_blobs::store::virtual_blob::{DynVirtualSource, Provider};

    /// Provider name for the mid-stream-error source below. Deliberately not
    /// [`PROVIDER_NAME`] so this test's entry can never be served by real
    /// cipherblob machinery.
    const FAILING_SOURCE: &str = "test-failing-source";

    /// A virtual source that serves the first leaf window (offset 0) with the
    /// real bytes and fails every later window read. `export_bao` surfaces
    /// that as `Size` .. `Leaf` .. `Error` mid-stream - leaf 0 must serve
    /// correctly, or its bao validation would fail before any leaf item is
    /// emitted. Exactly the early producer exit that used to park the
    /// install's spawned consumer on an EOF that could only arrive after the
    /// join: the `c_tx`-ownership deadlock.
    struct FailsAfterFirstLeaf {
        plain: Vec<u8>,
    }

    impl ReadBytesAt for FailsAfterFirstLeaf {
        fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<bytes::Bytes> {
            if offset == 0 && size > 0 {
                let end = size.min(self.plain.len());
                return Ok(bytes::Bytes::copy_from_slice(&self.plain[..end]));
            }
            Err(io::Error::other("mid-stream failure"))
        }
    }

    impl Provider for FailsAfterFirstLeaf {
        fn reader_for(&self, _hash: &Hash) -> Option<DynVirtualSource> {
            Some(Arc::new(FailsAfterFirstLeaf {
                plain: self.plain.clone(),
            }))
        }
    }

    /// The install producer must own its pipe writer: when the plaintext
    /// export fails *mid-stream* (here: a virtual source that errors on the
    /// second leaf read), every exit path from the producer future drops the
    /// writer, the spawned consumer sees EOF and finishes, and
    /// [`install_virtual_encrypted`] returns the export error promptly. With
    /// the writer merely borrowed, the consumer parked on it until this
    /// function returned - after the join - and the install hung forever.
    /// The timeout is only a hang tripwire, generously bounded; the
    /// assertion under test is a prompt `Err` naming the export failure.
    #[tokio::test(flavor = "multi_thread")]
    async fn install_fails_promptly_when_export_errors_mid_stream() -> Res<()> {
        let (mem, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(mem);

        // 40_000 octets = more than the two 16 KiB bao leaves the scenario
        // needs (leaf 0 serves, a later leaf read errors mid-stream).
        const PLAIN_LEN: usize = 40_000;
        let plain = vec![0x11u8; PLAIN_LEN];
        let ob = PreOrderMemOutboard::create(&plain, iroh_blobs::store::IROH_BLOCK_SIZE);
        let p_hash: Hash = ob.root.into();
        store
            .blobs()
            .add_virtual_with_outboard(p_hash, plain.len() as u64, ob.data, FAILING_SOURCE)
            .await?;
        virtuals
            .register(
                FAILING_SOURCE,
                Arc::new(FailsAfterFirstLeaf {
                    plain: plain.clone(),
                }),
            )
            .expect("registering the test provider cannot fail");

        let key = MasterKey::random();
        match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            install_virtual_encrypted(&store, &key, p_hash, EncodingParams::DEFAULT),
        )
        .await
        {
            Err(_) => panic!(
                "install hung when the export failed mid-stream; \
                 this is the c_tx-ownership deadlock"
            ),
            Ok(Err(err)) => assert!(
                err.to_string().contains("export of"),
                "the failure must be the producer's export bail, not a different error: {err}"
            ),
            Ok(Ok(c)) => panic!("a mid-stream export failure must not install a ciphertext: {c}"),
        }
        Ok(())
    }
}
