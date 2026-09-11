//! The specification's entry relations: recency, prefix pruning, and its reciprocal.
//!
//! The store's outdated check is `Entry::is_newer_than` and its prune pass is the negation of
//! it, so these suites check the two relations the store's correctness rests on. They are also
//! the only place the corpus says anything about recency ordering, which is what decides
//! whether a publication replaces an earlier one.
//!
//! Each vector holds two absolutely encoded `Entry` values, and the directory it sits in is the
//! verdict for the predicate named by the suite.

use ufotofu::codec::Decodable;
use willow25::prelude::*;

use super::{failure_report, vector_bytes, vector_names};

/// Decodes the entries in a corpus file, which holds one `encode_entry` value per entry.
///
/// The encoding is self-delimiting, so the values are decoded in sequence from one producer.
async fn entries(bytes: &[u8]) -> Vec<Entry> {
    let mut producer = ufotofu::producer::clone_from_slice(bytes);
    let mut decoded = Vec::new();
    while producer.offset() < bytes.len() {
        decoded.push(
            Entry::decode(&mut producer)
                .await
                .expect("the corpus encodes valid entries"),
        );
    }
    decoded
}

/// An entry rendered for a failure message.
///
/// The relations compare the timestamp, the digest and the length, so a failure is only
/// diagnosable with all of them. An entry's debug form carries them and runs to a few hundred
/// bytes, so this bounds it rather than dumping it whole.
fn show(entry: &Entry) -> String {
    const SHOWN: usize = 200;

    let rendered = format!("{entry:?}");
    if rendered.chars().count() <= SHOWN {
        rendered
    } else {
        format!("{}…", rendered.chars().take(SHOWN).collect::<String>())
    }
}

/// A binary predicate over two entries, as the `entry_*` suites name them.
type EntryPredicate = fn(&Entry, &Entry) -> bool;

/// Every `entry_*` suite, each a binary predicate over two entries.
#[tokio::test]
async fn entry_relations_match_the_corpus() {
    let suites: [(&str, EntryPredicate); 3] = [
        ("data_model/entry_is_newer_than", |left, right| {
            left.is_newer_than(right)
        }),
        ("data_model/entry_prunes", |left, right| left.prunes(right)),
        ("data_model/entry_is_pruned_by", |left, right| {
            left.is_pruned_by(right)
        }),
    ];

    let mut total = 0;
    let mut failed = Vec::new();

    for (suite, predicate) in suites {
        for (dir, expected) in [("true", true), ("false", false)] {
            let subdirectory = format!("{dir}/input");
            let names = vector_names(suite, &subdirectory);
            assert!(
                !names.is_empty(),
                "the corpus holds no `{suite}/{subdirectory}` vectors",
            );

            for name in &names {
                total += 1;
                let decoded = entries(&vector_bytes(suite, &subdirectory, name)).await;
                let [left, right] = decoded.as_slice() else {
                    failed.push(format!(
                        "{suite} {dir}/{name}: expected two entries, found {}",
                        decoded.len(),
                    ));
                    continue;
                };

                if predicate(left, right) != expected {
                    failed.push(format!(
                        "{suite} {dir}/{name}: predicate is {}, the corpus says {expected}\n      \
                         left:  {}\n      right: {}",
                        !expected,
                        show(left),
                        show(right),
                    ));
                }
            }
        }
    }

    assert!(
        failed.is_empty(),
        "{}",
        failure_report("entry relation", total, &failed),
    );
}
