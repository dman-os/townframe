//! [`EncodeMeadowcapAuthorisedEntry`]: the absolute encoding of an authorised entry.
//!
//! This is the layer the other suites are expressed through. A store persists entries with
//! exactly these bytes, and the `data_model` suites encode their inputs the same way, so a
//! divergence here would show up everywhere and is worth pinning first.
//!
//! [`EncodeMeadowcapAuthorisedEntry`]: https://willowprotocol.org/specs/encodings/index.html#EncodeMeadowcapAuthorisedEntry

use ufotofu::codec::{Decodable, Encodable};
use ufotofu::prelude::*;
use willow25::prelude::*;

use super::{failure_report, vector_bytes, vector_names};

/// The suite these checks read.
const SUITE: &str = "codec/EncodeMeadowcapAuthorisedEntry";

/// Decodes one absolute encoding, or `None` when the bytes are not a valid encoding of exactly
/// one entry.
///
/// Trailing bytes are a failure rather than a success. Each vector encodes one value, and a
/// decoder that accepted a valid prefix would accept a blob that had been truncated and padded,
/// which is exactly the corruption a store reading its own bytes back needs to rule out. The
/// corpus does not discriminate this: it holds no vector that is a valid encoding followed by
/// extra bytes, so the requirement is stricter than the vectors demand and stands on its own.
async fn decode(bytes: &[u8]) -> Option<AuthorisedEntry> {
    let mut producer = ufotofu::producer::clone_from_slice(bytes);
    let entry = AuthorisedEntry::decode(&mut producer).await.ok()?;
    (producer.offset() == bytes.len()).then_some(entry)
}

/// Encodes one entry, with the same consumer the store's persistence path uses.
async fn encode(entry: &AuthorisedEntry) -> Vec<u8> {
    let mut consumer = Vec::<u8>::new().into_consumer();
    entry
        .encode(&mut consumer)
        .await
        .expect("a `Vec` consumer cannot fail");
    Vec::from(consumer)
}

/// Every `yay` vector decodes, and re-encodes to the bytes the corpus records.
///
/// The two halves are one check because they fail together: an encoding is a canonical
/// representation, so decoding and re-encoding must be the identity on the corpus.
#[tokio::test]
async fn every_yay_vector_decodes_and_reencodes() {
    let names = vector_names(SUITE, "yay");
    assert!(!names.is_empty(), "the corpus holds no `yay` vectors");
    assert_eq!(
        names,
        vector_names(SUITE, "reencoded"),
        "every `yay` vector needs a `reencoded` counterpart",
    );

    let mut failed = Vec::new();
    for name in &names {
        let Some(entry) = decode(&vector_bytes(SUITE, "yay", name)).await else {
            failed.push(format!("{name}: did not decode"));
            continue;
        };

        let expected = vector_bytes(SUITE, "reencoded", name);
        let actual = encode(&entry).await;
        if actual != expected {
            failed.push(format!(
                "{name}: re-encoded to {} bytes, the corpus records {}",
                actual.len(),
                expected.len(),
            ));
        }
    }

    assert!(
        failed.is_empty(),
        "{}",
        failure_report("`yay`", names.len(), &failed),
    );
}

/// Every `nay` vector fails to decode.
///
/// The corpus records a reason per vector, but those reasons are coarse ("unexpected end of
/// input", "their fault"), so this asserts only the verdict. A check that matched reasons would
/// be pinning `willow25`'s error taxonomy rather than the specification's encoding relation.
#[tokio::test]
async fn every_nay_vector_fails_to_decode() {
    let names = vector_names(SUITE, "nay");
    assert!(!names.is_empty(), "the corpus holds no `nay` vectors");

    let mut failed = Vec::new();
    for name in &names {
        if decode(&vector_bytes(SUITE, "nay", name)).await.is_some() {
            failed.push(format!("{name}: decoded, but the corpus says it must not"));
        }
    }

    assert!(
        failed.is_empty(),
        "{}",
        failure_report("`nay`", names.len(), &failed),
    );
}
