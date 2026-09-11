//! The specification's path relation and prefix-successor, against `prefix_range`.
//!
//! The store keys entries by `big_willow`'s own order-preserving `encode_path`, and every prefix
//! query it makes is a `prefix_range` test. That makes this the property the whole storage layout
//! rests on: a byte-keyed store cannot call `Path::is_prefix_of`, so if `prefix_range` disagreed
//! with the specification's prefix relation, every prune and every area read would be wrong.
//!
//! `prefix_range` is deliberately not the specification's `encode_path`: it is an
//! order-preserving encoding for storage keys, whereas the corpus encodes its inputs with the
//! specification's encoding. So the corpus bytes are decoded into `Path` values and re-encoded
//! with `big_willow`'s encoder, which is the only way to compare the two.

use ufotofu::codec::Decodable;
use willow25::prelude::*;

use super::{failure_report, vector_bytes, vector_names};
use crate::path_codec::{PrefixRange, encode_path, prefix_range};

/// The suite for the prefix relation.
const PREFIX_SUITE: &str = "data_model/path_is_prefix_of";

/// The suite for the prefix-successor operation.
const SUCCESSOR_SUITE: &str =
    "data_model/path_least_path_lexicographically_greater_but_not_prefixed_by_original";

/// Decodes the paths in a corpus file, which holds one `encode_path` value per path.
///
/// The encoding is self-delimiting, so the values are decoded in sequence from one producer.
async fn paths(bytes: &[u8]) -> Vec<Path> {
    let mut producer = ufotofu::producer::clone_from_slice(bytes);
    let mut decoded = Vec::new();
    while producer.offset() < bytes.len() {
        decoded.push(
            Path::decode(&mut producer)
                .await
                .expect("the corpus encodes valid paths"),
        );
    }
    decoded
}

/// A path rendered for a failure message.
///
/// Corpus paths run to thousands of bytes, so a failure that dumped them whole would bury the
/// report it belongs to.
fn show(path: &Path) -> String {
    const SHOWN: usize = 40;

    let rendered = path.to_string();
    if rendered.chars().count() <= SHOWN {
        rendered
    } else {
        format!("{}…", rendered.chars().take(SHOWN).collect::<String>())
    }
}

/// Whether `encoded` falls in `range`, which is the question a byte-keyed store asks instead of
/// calling `Path::is_prefix_of`.
fn in_range(range: &PrefixRange, encoded: &[u8]) -> bool {
    range.lo.as_slice() <= encoded
        && range
            .hi
            .as_ref()
            .is_none_or(|hi| encoded < hi.as_slice())
}

/// `prefix_range` admits exactly the paths the specification says a path prefixes.
///
/// Each vector names two paths and says whether the first is a prefix of the second. Both the
/// relation and the range are checked against that verdict, so a divergence between them is
/// caught even where both happen to agree with the corpus.
#[tokio::test]
async fn prefix_range_matches_the_specifications_prefix_relation() {
    let mut total = 0;
    let mut failed = Vec::new();

    for (dir, expected) in [("true", true), ("false", false)] {
        let subdirectory = format!("{dir}/input");
        let names = vector_names(PREFIX_SUITE, &subdirectory);
        assert!(!names.is_empty(), "the corpus holds no `{subdirectory}` vectors");

        for name in &names {
            total += 1;
            let decoded = paths(&vector_bytes(PREFIX_SUITE, &subdirectory, name)).await;
            let [first, second] = decoded.as_slice() else {
                failed.push(format!("{dir}/{name}: expected two paths, found {}", decoded.len()));
                continue;
            };

            let relation = first.is_prefix_of(second);
            let range = in_range(&prefix_range(first), &encode_path(second));

            if relation != expected {
                failed.push(format!(
                    "{dir}/{name}: `{}` is_prefix_of `{}` is {relation}, the corpus says {expected}",
                    show(first),
                    show(second),
                ));
            }
            if range != expected {
                failed.push(format!(
                    "{dir}/{name}: the range of `{}` {} `{}`, the corpus says {expected}",
                    show(first),
                    if range { "admits" } else { "rejects" },
                    show(second),
                ));
            }
        }
    }

    assert!(
        failed.is_empty(),
        "{}",
        failure_report("`path_is_prefix_of`", total, &failed),
    );
}

/// The upper bound of a `prefix_range` agrees with the specification's successor operation.
///
/// When a path has a successor, that successor lies outside the subtree and above the path, so
/// its encoding must be at or above the range's upper bound. That follows from `prefix_range`'s
/// exactness, checked here against an operation the corpus defines independently.
///
/// The absence of a successor is *not* the same as an unbounded range. The specification's
/// `encode_path` has a maximum total path length, and willow-ts's `successorNotPrefixed` shows
/// the consequence: a component's successor is defined only when it fits in the remaining
/// budget, so a path near the limit has none. The corpus's `none` vectors are exactly that
/// case. The storage encoding has no such limit, so the bound exists for every path except the
/// empty one, which is what this checks there.
#[tokio::test]
async fn the_prefix_range_bound_agrees_with_the_specifications_successor() {
    let mut total = 0;
    let mut failed = Vec::new();

    let none = vector_names(SUCCESSOR_SUITE, "none/input");
    assert!(!none.is_empty(), "the corpus holds no `none` vectors");
    for name in &none {
        total += 1;
        let decoded = paths(&vector_bytes(SUCCESSOR_SUITE, "none/input", name)).await;
        let [path] = decoded.as_slice() else {
            failed.push(format!("none/{name}: expected one path, found {}", decoded.len()));
            continue;
        };
        let unbounded = prefix_range(path).hi.is_none();
        let empty = encode_path(path).is_empty();
        if unbounded != empty {
            failed.push(format!(
                "none/{name}: `{}` is {}, and its range is {}",
                show(path),
                if empty { "the empty path" } else { "not the empty path" },
                if unbounded { "unbounded" } else { "bounded" },
            ));
        }
    }

    let some = vector_names(SUCCESSOR_SUITE, "some/input");
    assert!(!some.is_empty(), "the corpus holds no `some` vectors");
    for name in &some {
        total += 1;
        let decoded = paths(&vector_bytes(SUCCESSOR_SUITE, "some/input", name)).await;
        let [path] = decoded.as_slice() else {
            failed.push(format!("some/{name}: expected one path, found {}", decoded.len()));
            continue;
        };
        let successors = paths(&vector_bytes(SUCCESSOR_SUITE, "some/output", name)).await;
        let [successor] = successors.as_slice() else {
            failed.push(format!(
                "some/{name}: expected one successor, found {}",
                successors.len(),
            ));
            continue;
        };

        let Some(hi) = prefix_range(path).hi else {
            failed.push(format!(
                "some/{name}: `{}` has a successor, but the range is unbounded",
                show(path),
            ));
            continue;
        };
        let encoded = encode_path(successor);
        if encoded < hi {
            failed.push(format!(
                "some/{name}: the successor `{}` encodes below the bound of `{}`",
                show(successor),
                show(path),
            ));
        }
    }

    assert!(
        failed.is_empty(),
        "{}",
        failure_report("successor", total, &failed),
    );
}
