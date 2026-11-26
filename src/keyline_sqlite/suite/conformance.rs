//! Upstream's scenarios and laws, which `MemoryKeyline` runs, run against the SQLite backend:
//! the contract is that it agrees with the reference on every set.

use future_form::Local;
use future_form::Sendable;
use keyline::test_utils::conformance::laws;

use super::Sqlite;

mod local {
    use super::*;
    keyline::keyline_conformance!(Sqlite<(), Local>);
}

mod sendable {
    use super::*;
    keyline::keyline_conformance!(Sqlite<(), Sendable>);
}

/// A watermark type with a variable-length encoding: at `()` a `retain` entry carries no bytes,
/// so the laws above never see the `retain` column hold any. These re-run the laws about
/// `retain` with watermarks that do.
type WithBytes = Sqlite<Vec<u8>, Local>;

#[test]
fn matches_naive_oracle_with_watermark_bytes() {
    laws::matches_naive_oracle_with_revocations::<WithBytes>();
}

#[test]
fn matches_threshold_oracle_with_watermark_bytes() {
    laws::matches_threshold_oracle::<WithBytes>();
}

#[test]
fn retain_with_bytes_is_inert() {
    laws::retain_is_inert::<WithBytes>();
}
