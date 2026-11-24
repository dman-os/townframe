//! Mechanical checks on the parity matrix (`PARITY.md`, `parity/matrix.psv`),
//! which `node oracle/parity.mjs` generates from the upstream file list and the
//! mapping rules. The generator already fails on an unmapped upstream file;
//! these tests check the half it cannot see: that every Rust file the matrix
//! cites exists and carries a header naming the upstream file, and that no
//! ported file escapes the matrix. Test-only, original to this crate.

use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;

struct Row {
    upstream: String,
    status: String,
    rust: Vec<String>,
}

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_psv(relative: &str) -> Vec<Vec<String>> {
    let text = std::fs::read_to_string(crate_dir().join(relative))
        .unwrap_or_else(|error| panic!("reading {relative}: {error}"));
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.splitn(6, '|').map(str::to_owned).collect())
        .collect()
}

fn matrix() -> Vec<Row> {
    read_psv("parity/matrix.psv")
        .into_iter()
        .map(|columns| {
            assert_eq!(columns.len(), 6, "matrix row {columns:?}");
            Row {
                upstream: columns[0].clone(),
                status: columns[2].clone(),
                rust: columns[4]
                    .split(';')
                    .filter(|path| !path.is_empty())
                    .map(str::to_owned)
                    .collect(),
            }
        })
        .collect()
}

fn rust_files_with_ported_headers(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("readable crate directory") {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name != "oracle") {
                rust_files_with_ported_headers(&path, found);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = std::fs::read_to_string(&path).expect("readable source");
            if text
                .lines()
                .take(3)
                .any(|line| line.starts_with("// Ported from"))
            {
                found.push(path);
            }
        }
    }
}

#[test]
fn matrix_lists_every_upstream_file_once() {
    let upstream: Vec<String> = read_psv("parity/upstream_files.psv")
        .into_iter()
        .map(|columns| columns[0].clone())
        .collect();
    let in_matrix: Vec<String> = matrix().into_iter().map(|row| row.upstream).collect();
    assert_eq!(upstream, in_matrix);
    let unique: BTreeSet<&String> = in_matrix.iter().collect();
    assert_eq!(unique.len(), in_matrix.len(), "duplicate matrix rows");
    assert!(in_matrix.len() > 400);
}

#[test]
fn statuses_are_known_and_only_ported_rows_cite_rust() {
    for row in matrix() {
        assert!(
            ["PORTED", "REPLACED", "PENDING", "OUT", "DEFER"].contains(&row.status.as_str()),
            "{}: status {}",
            row.upstream,
            row.status
        );
        assert_eq!(
            row.status == "PORTED" || row.status == "REPLACED",
            !row.rust.is_empty(),
            "{}: PORTED/REPLACED iff a Rust location is cited",
            row.upstream
        );
    }
}

#[test]
fn ported_rows_cite_existing_files_with_matching_headers() {
    for row in matrix().into_iter().filter(|row| row.status == "PORTED") {
        // `pierre/src/types.ts` -> `src/types.ts`; `jsdiff/libesm/diff/base.js` -> `libesm/diff/base.js`.
        let upstream_tail = row
            .upstream
            .split_once('/')
            .expect("upstream paths are prefixed")
            .1;
        for rust in &row.rust {
            let path = crate_dir().join(rust);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{}: citing {rust}: {error}", row.upstream));
            if rust.ends_with(".rs") {
                let header: String = text.lines().take(30).collect::<Vec<_>>().join("\n");
                assert!(
                    header.starts_with("// Ported from"),
                    "{rust} must open with a `// Ported from` header"
                );
                assert!(
                    header.contains(upstream_tail),
                    "{rust}: header does not name {upstream_tail}"
                );
                assert!(
                    header.contains("Changes:"),
                    "{rust}: header lacks a `Changes:` line (Apache-2.0 section 4)"
                );
            }
        }
    }
}

#[test]
fn replaced_rows_cite_existing_files() {
    for row in matrix().into_iter().filter(|row| row.status == "REPLACED") {
        for rust in &row.rust {
            assert!(
                crate_dir().join(rust).is_file(),
                "{}: {rust} missing",
                row.upstream
            );
        }
    }
}

#[test]
fn every_ported_header_is_listed_in_the_matrix() {
    let cited: BTreeSet<String> = matrix().into_iter().flat_map(|row| row.rust).collect();
    let mut found = Vec::new();
    rust_files_with_ported_headers(&crate_dir(), &mut found);
    assert!(found.len() >= 8, "expected the S1 ports, found {found:?}");
    for path in found {
        let relative = path
            .strip_prefix(crate_dir())
            .expect("under the crate dir")
            .to_string_lossy()
            .into_owned();
        assert!(
            cited.contains(&relative),
            "{relative} has a `Ported from` header but no matrix row cites it"
        );
    }
}
