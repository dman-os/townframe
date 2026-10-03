use super::codec::{
    HEADER_LEN, HeaderFacts, MASTER_KEY_LEN, RECORD_OVERHEAD, SALT_LEN, decrypt_bytes_ikm,
    encrypt_raw_ikm, payload_size,
};
use super::*;

use crate::interlude::eyre;

use iroh_blobs::{Hash, api::Store};

// The store's bao leaf size: the granularity its export path reads the
// ciphertext at. Re-declared next to the serving tests that actually issue
// leaf-sized reads (`iroh_blobs::store::IROH_BLOCK_SIZE` is chunk-log 4, so
// 2^14 octets).
const SMALL_RS: u64 = 1024;

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
    let (store, virtuals) = iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
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
        FsStore::load_with_virtuals(dir.path().join("blobs.db"), Options::new(dir.path())).await?;
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
        assert_eq!(c, Hash::new(encrypt_bytes(&key, &plaintext)));

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
    let (mem, virtuals) = iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
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
    let (mem, virtuals) = iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
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
    let p_hash = download_encrypted(&store_b, &provider, conn, c_hash, keys.as_ref(), None).await?;
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
    let keys2 = std::sync::Arc::clone(&keys);
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
                    .map(|meta| meta.len())
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
