//! Bugs the suite plants to prove its checks can fail (`suite/demand.rs`).
//!
//! The evaluation's statements carry marker comments (`/*GROUND*/`, `/*COMPOSE*/`, `/*OWN*/`,
//! `/*EXCL*/`) at the conditions that make them correct. [`Checked`] wraps the executor the
//! evaluation runs through and, in test builds with a bug planted on the thread, rewrites those
//! conditions away. In every other build it passes statements through untouched, so the
//! evaluation's code has no test branches.

use std::borrow::Cow;

use sqlite_rs::ExecuteResult;
use sqlite_rs::Executor;
use sqlite_rs::IntoParams;
use sqlite_rs::QueryExecutor;
use sqlite_rs::ResultSet;
use sqlite_rs::SqlResult;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Planted {
    None,
    /// Membership does not compose: only edges about the subject itself conduct.
    NoComposition,
    /// A delegation conducts whether or not its issuer has standing: optimistic seeding.
    Ungrounded,
    /// A hop need not be live in its own context (the daisy-chain rule dropped).
    IgnoreOwnContext,
    /// An exclusion context does not exclude the hop's audience.
    IgnoreExclusion,
    /// An insert does not recompute the live sets of the disputed subjects it touched.
    SkipRefresh,
}

#[cfg(test)]
thread_local! {
    pub(crate) static PLANTED: std::cell::Cell<Planted> = const { std::cell::Cell::new(Planted::None) };
}

#[cfg(test)]
fn rewrite(sql: &str) -> Cow<'_, str> {
    let (marker, with) = match PLANTED.with(std::cell::Cell::get) {
        Planted::None => return Cow::Borrowed(sql),
        Planted::NoComposition => ("/*COMPOSE*/ 1 = 1", "/*COMPOSE*/ 1 = 0"),
        Planted::Ungrounded => ("/*GROUND*/ (", "/*GROUND*/ (1 = 1 OR "),
        Planted::IgnoreOwnContext => ("/*OWN*/ (", "/*OWN*/ (1 = 1 OR "),
        Planted::IgnoreExclusion => ("/*EXCL*/ (", "/*EXCL*/ (1 = 1 OR "),
        Planted::SkipRefresh => return Cow::Borrowed(sql),
    };
    Cow::Owned(sql.replace(marker, with))
}

#[cfg(not(test))]
fn rewrite(sql: &str) -> Cow<'_, str> {
    Cow::Borrowed(sql)
}

/// An executor that runs the evaluation's statements through [`rewrite`].
pub struct Checked<'a, E>(pub &'a E);

impl<E: QueryExecutor> QueryExecutor for Checked<'_, E> {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        self.0.query(&rewrite(sql), params).await
    }
}

impl<E: Executor> Executor for Checked<'_, E> {
    async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        self.0.execute(&rewrite(sql), params).await
    }
}

/// Whether the planted bug is "skip the refresh" (the refresh is Rust control flow, not a
/// statement, so it is checked here rather than by rewriting SQL).
#[cfg(test)]
pub(crate) fn skips_refresh() -> bool {
    PLANTED.with(std::cell::Cell::get) == Planted::SkipRefresh
}

#[cfg(not(test))]
pub(crate) fn skips_refresh() -> bool {
    false
}
