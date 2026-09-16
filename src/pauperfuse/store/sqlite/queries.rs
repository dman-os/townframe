//! Every SQL statement the SQLite store executes.
//!
//! Statements live in `sql/queries` as files and are pulled in with `include_str!`, so the store
//! never assembles SQL and the schema and its queries can be read together. The schema itself is
//! in `sql/migrations`. Each file documents the bind order its statement expects.
//!
//! `EXPLAIN_*` files are the corresponding query prefixed with `EXPLAIN QUERY PLAN`. A test
//! asserts each one is exactly that prefix plus the query it explains, so the plan a test checks
//! cannot drift from the statement the store runs.

pub const INSERT_REP: &str = include_str!("../../sql/queries/insert_rep.sql");
pub const BUMP_REP: &str = include_str!("../../sql/queries/bump_rep.sql");
pub const SELECT_GENERATION: &str = include_str!("../../sql/queries/select_generation.sql");
pub const DELETE_REP: &str = include_str!("../../sql/queries/delete_rep.sql");
pub const LIST_REPS: &str = include_str!("../../sql/queries/list_reps.sql");

pub const UPSERT_ENTRY: &str = include_str!("../../sql/queries/upsert_entry.sql");
pub const INSERT_IMPLIED_DIR: &str = include_str!("../../sql/queries/insert_implied_dir.sql");
pub const DELETE_ENTRY: &str = include_str!("../../sql/queries/delete_entry.sql");

pub const SELECT_ENTRY: &str = include_str!("../../sql/queries/select_entry.sql");
pub const SELECT_PAGE_FROM_START: &str =
    include_str!("../../sql/queries/select_page_from_start.sql");
pub const SELECT_PAGE_AFTER: &str = include_str!("../../sql/queries/select_page_after.sql");

// The remaining constants exist for this crate's own tests, which check schema invariants and the
// query plan rather than the trait's observable behaviour.
#[cfg(test)]
pub const CORRUPT_KIND: &str = include_str!("../../sql/queries/corrupt_kind.sql");
#[cfg(test)]
pub const CORRUPT_PATH: &str = include_str!("../../sql/queries/corrupt_path.sql");

/// Prefix that turns one of the `SELECT_*` queries into a plan query.
#[cfg(test)]
pub const EXPLAIN_QUERY_PLAN: &str = "EXPLAIN QUERY PLAN\n";
#[cfg(test)]
pub const EXPLAIN_SELECT_ENTRY: &str = include_str!("../../sql/queries/explain_select_entry.sql");
#[cfg(test)]
pub const EXPLAIN_SELECT_PAGE_FROM_START: &str =
    include_str!("../../sql/queries/explain_select_page_from_start.sql");
#[cfg(test)]
pub const EXPLAIN_SELECT_PAGE_AFTER: &str =
    include_str!("../../sql/queries/explain_select_page_after.sql");
