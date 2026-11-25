//! Helpers for the statements whose shape depends on a batch: multi-row `VALUES` and `IN`
//! lists, split to stay under SQLite's bound-variable limit.

use big_automata::part_store::AccessLevel;
use big_automata::part_store::Generation;
use big_automata::rev::Rev;
use sqlite_rs::BindTuple;
use sqlite_rs::Executor;
use sqlite_rs::Sql;

use crate::interlude::*;

/// Headroom under SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` of 32766.
const MAX_PARAMS: usize = 30_000;

/// Split `rows`, each binding `width` variables (plus `fixed` variables the statement binds
/// besides its rows), into batches one statement can take.
pub fn chunk_rows<T>(rows: Vec<T>, width: usize, fixed: usize) -> Vec<Vec<T>> {
    let per_statement = ((MAX_PARAMS - fixed) / width).max(1);
    let mut rows = rows.into_iter().peekable();
    let mut chunks = Vec::new();
    while rows.peek().is_some() {
        chunks.push(rows.by_ref().take(per_statement).collect());
    }
    chunks
}

/// `VALUES (..), (..)`.
pub fn push_values<T: BindTuple>(sql: &mut Sql, rows: impl IntoIterator<Item = T>) {
    sql.push("VALUES ");
    for (index, row) in rows.into_iter().enumerate() {
        if index != 0 {
            sql.push(", ");
        }
        row.bind_tuple(sql);
    }
}

/// Execute `head`, then the rows as `VALUES`, then `tail`, once per batch of rows.
pub async fn execute_values<T: BindTuple>(
    executor: &impl Executor,
    head: &str,
    rows: Vec<T>,
    width: usize,
    tail: &str,
) -> Res<()> {
    for chunk in chunk_rows(rows, width, 0) {
        let mut sql = Sql::new(head);
        push_values(&mut sql, chunk);
        sql.push(tail);
        sql.execute(executor).await?;
    }
    Ok(())
}

/// A `LIMIT` for a count that may be `usize::MAX`.
pub fn limit(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

pub fn rev_to_sql(rev: Rev) -> i64 {
    i64::try_from(rev).expect("a rev fits SQLite's integer")
}

pub fn rev_from_sql(rev: i64) -> Rev {
    Rev::try_from(rev).expect("a stored rev is not negative")
}

/// A generation is stored as the 16 bytes of its UUID: fixed width and compact, with no case or
/// hyphenation to normalize, and what `randomblob(16)` in the migration produces.
pub fn generation_to_sql(generation: Generation) -> Vec<u8> {
    generation.0.as_bytes().to_vec()
}

pub fn generation_from_sql(generation: &[u8]) -> Generation {
    Generation(Uuid::from_slice(generation).expect("a stored generation is 16 bytes"))
}

pub fn level_to_sql(level: AccessLevel) -> i64 {
    match level {
        AccessLevel::Pull => 0,
        AccessLevel::Read => 1,
        AccessLevel::Write => 2,
        AccessLevel::Admin => 3,
    }
}

pub fn level_from_sql(level: i64) -> AccessLevel {
    match level {
        0 => AccessLevel::Pull,
        1 => AccessLevel::Read,
        2 => AccessLevel::Write,
        3 => AccessLevel::Admin,
        other => unreachable!("stored access level {other}"),
    }
}
