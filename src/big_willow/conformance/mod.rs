//! Conformance against the upstream Willow test vectors.
//!
//! The vectors are not vendored. `./x/willow-vectors.ts` fetches the suites this crate consumes
//! into a gitignored directory pinned to an exact revision, and these checks read them from
//! there. They are compiled only when the `conformance` feature is on and the crate is built
//! for tests, and then they fail loudly when the corpus is missing: an absent corpus would
//! otherwise turn every check into a vacuous pass, which is worse than not running them at all.
//!
//! `WILLOW_TEST_VECTORS` overrides the corpus location.
//!
//! Each suite is a directory of subdirectories holding files of the same name. Which
//! subdirectories exist depends on the suite, so the helpers here are per-directory rather than
//! per-suite: `codec` suites have `yay`/`nay`/`reencoded`, and the `data_model` suites have
//! `input`/`output`.

mod codec;
mod entry_relations;
mod path_ops;
mod store_pruning;

use std::path::{Path, PathBuf};

/// Root of the test-vector corpus.
///
/// # Panics
///
/// If the corpus is not there. Fetching the vectors is a setup step, so a missing corpus is a
/// mistake in the environment rather than an outcome of the check, and it is not recoverable
/// from here.
fn corpus_root() -> PathBuf {
    let root = std::env::var_os("WILLOW_TEST_VECTORS")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/willow_test_vectors")
        });
    assert!(
        root.is_dir(),
        "the Willow test vectors are not at {}; run `./x/willow-vectors.ts` to fetch them, or \
         set WILLOW_TEST_VECTORS to an existing corpus",
        root.display(),
    );
    root
}

/// The name of every vector in one subdirectory of `suite`, sorted.
///
/// The names are the identity of a vector across the suite's subdirectories, so a check pairs
/// them by name. Sorting keeps a failure report stable between runs.
fn vector_names(suite: &str, dir: &str) -> Vec<String> {
    let path = corpus_root().join(suite).join(dir);
    let entries = std::fs::read_dir(&path)
        .unwrap_or_else(|error| panic!("cannot read the vectors in {}: {error}", path.display()));
    let mut names: Vec<String> = entries
        .map(|entry| {
            entry
                .expect("a readable directory entry")
                .file_name()
                .into_string()
                .expect("the corpus uses utf-8 file names")
        })
        .collect();
    names.sort();
    names
}

/// The bytes of one vector.
///
/// # Panics
///
/// If the file is missing. Callers obtain names from [`vector_names`], so a missing file means
/// the corpus is inconsistent with itself.
fn vector_bytes(suite: &str, dir: &str, name: &str) -> Vec<u8> {
    let path = corpus_root().join(suite).join(dir).join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

/// Renders why a suite failed, capped so that one systematically wrong vector cannot flood the
/// output. A failure report is read by a person, and the first few cases always identify the
/// cause.
fn failure_report(kind: &str, total: usize, failed: &[String]) -> String {
    const SHOWN: usize = 10;

    let mut report = format!("{}/{} {kind} vectors failed:", failed.len(), total);
    for failure in failed.iter().take(SHOWN) {
        report.push_str("\n  ");
        report.push_str(failure);
    }
    if failed.len() > SHOWN {
        report.push_str(&format!("\n  ... and {} more", failed.len() - SHOWN));
    }
    report
}
