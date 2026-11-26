//! The hive's trees as a [`RevSource`]: key = tree id, one entry per tree at the `rev` of its
//! latest change, value `()` for a tree that exists and a tombstone for one that was deleted.
//!
//! A `rev` is one [`Batch`](super::Batch), the trees of a batch share it, and
//! `sedimentree_trees` keeps exactly one row per tree at its latest `rev`, so the table is
//! already the shape the contract asks for and a read is one indexed range scan. Consumers use
//! [`RevStream`](big_automata::rev::RevStream); nothing here is a reader.
//!
//! `watch` moves from [`SedimentreeStore::committed`], so it only moves for this process's
//! commits. A reader in another process (or a stream over a batch nobody reported) learns of
//! changes by re-reading `latest_rev`, on a timer of the embedder's choosing: the crate owns no
//! timers, to stay buildable for the browser.

use big_automata::rev::Entry;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata::rev::RevWatch;
use sedimentree_core::depth::DepthMetric;
use sedimentree_core::id::SedimentreeId;
use sqlite_rs::SqliteDb;
use utils_rs::prelude::Res;

use super::SedimentreeStore;
use super::write::rev_from_sql;
use crate::sqlite::queries;
use crate::sqlite::queries::ChangesWindowRow;

/// What an entry costs against [`Limits::max_bytes`]: its key.
const ENTRY_BYTES: usize = 32;

fn entry_of(row: ChangesWindowRow) -> Entry<SedimentreeId, ()> {
    Entry {
        key: SedimentreeId::new(
            row.tree_id
                .try_into()
                .expect("a tree id is 32 bytes (CHECK)"),
        ),
        rev: rev_from_sql(row.rev),
        value: row.live.then_some(()),
    }
}

impl<D: SqliteDb, M: DepthMetric + Send + Sync + 'static> RevSource for SedimentreeStore<D, M> {
    type Key = SedimentreeId;
    type Value = ();
    /// Every tree: there is nothing to select by.
    type Selector = ();

    async fn latest_rev(&self) -> Res<Rev> {
        let tx = self.db.begin_read().await?;
        let rev = queries::latest_rev(&tx).await;
        D::end_read(tx).await?;
        Ok(rev_from_sql(rev?))
    }

    async fn read(
        &self,
        (): &(),
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<SedimentreeId, ()>,
    ) -> Res<()> {
        assert!(after < until, "empty read window ({after}, {until}]");
        let max_entries = limits.max_bytes.map_or(limits.max_entries.get(), |bytes| {
            limits
                .max_entries
                .get()
                .min((bytes.get() / ENTRY_BYTES).max(1))
        });
        let (after_sql, until_sql) = (
            i64::try_from(after).expect("a rev fits i64"),
            i64::try_from(until).expect("a rev fits i64"),
        );
        let tx = self.db.begin_read().await?;
        let read = async {
            // One row past the bound tells whether the window holds more.
            let fetched = queries::changes_window(
                &tx,
                after_sql,
                until_sql,
                i64::try_from(max_entries).map_or(i64::MAX, |n| n.saturating_add(1)),
            )
            .await?;
            page.entries.clear();
            if fetched.len() <= max_entries {
                page.entries.extend(fetched.into_iter().map(entry_of));
                page.through = until;
                return Ok::<_, sqlite_rs::SqlError>(());
            }
            // More follows, and the last revision fetched may be cut: a page never splits one.
            let cut = fetched.last().expect("more than the bound was fetched").rev;
            let whole: Vec<_> = fetched
                .into_iter()
                .take_while(|row| row.rev != cut)
                .collect();
            if whole.is_empty() {
                // The first revision alone is over the bound, so it is the whole page.
                let rows = queries::changes_window(&tx, cut - 1, cut, i64::MAX).await?;
                page.entries.extend(rows.into_iter().map(entry_of));
                let more = queries::changes_beyond(&tx, cut, until_sql).await?;
                page.through = if more { rev_from_sql(cut) } else { until };
            } else {
                page.through = rev_from_sql(whole.last().expect("not empty").rev);
                page.entries.extend(whole.into_iter().map(entry_of));
            }
            Ok(())
        }
        .await;
        D::end_read(tx).await?;
        Ok(read?)
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.shared.committed.subscribe())
    }
}
