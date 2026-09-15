//! Every SQL statement the SQLite store executes.
//!
//! Statements live in `sql/queries` as files and are pulled in with `include_str!`, so the
//! store never assembles SQL and the schema and its queries can be read together. Each file
//! documents the bind order its statement expects.
//!
//! `EXPLAIN_*` files are the corresponding query prefixed with `EXPLAIN QUERY PLAN`. A test
//! asserts each one is exactly that prefix plus the query it explains, so the plan a test
//! checks cannot drift from the statement the store runs.

pub const INSERT_SCOPE: &str = include_str!("../sql/queries/insert_scope.sql");
pub const SELECT_SCOPE_ID: &str = include_str!("../sql/queries/select_scope_id.sql");

pub const SELECT_RECENCY_AT_PATH: &str = include_str!("../sql/queries/select_recency_at_path.sql");
pub const UPSERT_ENTRY: &str = include_str!("../sql/queries/upsert_entry.sql");
pub const UPSERT_PAYLOAD: &str = include_str!("../sql/queries/upsert_payload.sql");
pub const DELETE_PRUNED_ENTRIES: &str =
    include_str!("../sql/queries/delete_pruned_entries.sql");

pub const SELECT_ENTRY: &str = include_str!("../sql/queries/select_entry.sql");
pub const SELECT_PAYLOAD: &str = include_str!("../sql/queries/select_payload.sql");
pub const READ_AREA_IN_SUBSPACE: &str = include_str!("../sql/queries/read_area_in_subspace.sql");
pub const READ_AREA_ANY_SUBSPACE: &str = include_str!("../sql/queries/read_area_any_subspace.sql");

pub const DELETE_ENTRY: &str = include_str!("../sql/queries/delete_entry.sql");
pub const DELETE_AREA_IN_SUBSPACE: &str = include_str!("../sql/queries/delete_area_in_subspace.sql");
pub const DELETE_AREA_ANY_SUBSPACE: &str =
    include_str!("../sql/queries/delete_area_any_subspace.sql");
pub const DELETE_NAMESPACE: &str = include_str!("../sql/queries/delete_namespace.sql");

pub const WAL_CHECKPOINT: &str = include_str!("../sql/queries/wal_checkpoint.sql");

// The remaining constants exist for this crate's own tests, which check schema invariants and
// the query plan rather than the trait's observable behaviour.
#[cfg(test)]
pub const SELECT_DENORMALISED: &str = include_str!("../sql/queries/select_denormalised.sql");
#[cfg(test)]
pub const COUNT_ORPHAN_PAYLOADS: &str = include_str!("../sql/queries/count_orphan_payloads.sql");

/// Prefix that turns one of the `READ_AREA_*` queries into a plan query.
#[cfg(test)]
pub const EXPLAIN_QUERY_PLAN: &str = "EXPLAIN QUERY PLAN\n";
#[cfg(test)]
pub const EXPLAIN_READ_AREA_IN_SUBSPACE: &str =
    include_str!("../sql/queries/explain_read_area_in_subspace.sql");
#[cfg(test)]
pub const EXPLAIN_READ_AREA_ANY_SUBSPACE: &str =
    include_str!("../sql/queries/explain_read_area_any_subspace.sql");
