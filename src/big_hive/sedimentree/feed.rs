//! The change feed: which trees changed, tailed by `rev`.

use std::sync::Arc;

use sedimentree_core::id::SedimentreeId;
use sqlite_rs::SqliteDb;
use tokio::sync::watch;

use super::Error;
use super::SedimentreeStore;
use crate::sqlite::queries;

/// Rows a poll reads per statement (small under test, so the tests page).
const PAGE: i64 = if cfg!(test) { 3 } else { 1024 };

/// A tree's latest change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub tree_id: SedimentreeId,
    /// The tree's `rev` at this change.
    pub rev: i64,
    /// Whether the tree exists after it: `false` once it is deleted.
    pub live: bool,
}

/// A cursor over the hive's trees ordered by `rev`: each [`poll`](Self::poll) returns the trees
/// that changed since the last one, each once, at its latest `rev`.
///
/// The feed reads the database, so it sees every writer's commits, in this process or another.
/// Within the process a commit also wakes [`next_local`](Self::next_local). Across processes
/// nothing wakes a reader: its embedder calls [`poll`](Self::poll) on a timer of its choosing
/// (this crate owns none, to stay buildable for the browser), and a poll of an idle hive is one
/// indexed lookup. `PRAGMA data_version` would be the cheaper probe, but `sqlite_rs` serves a
/// read from any connection of a pool and the value is per connection, so two probes are not
/// comparable.
///
/// A spurious or coalesced wake is harmless: a poll returns what changed, whatever woke it.
pub struct ChangeFeed<D: SqliteDb> {
    db: D,
    /// Held so the channel `local` waits on never closes.
    _sender: Arc<watch::Sender<i64>>,
    local: watch::Receiver<i64>,
    /// The `rev` of the last change returned.
    cursor: i64,
}

impl<D: SqliteDb> ChangeFeed<D> {
    /// The changes after `rev`, with the last of them returned by a poll as the new cursor.
    pub async fn poll(&mut self) -> Result<Vec<Change>, Error> {
        let tx = self.db.begin_read().await?;
        let read = self.read_pages(&tx).await;
        D::end_read(tx).await?;
        let changes = read?;
        if let Some(last) = changes.last() {
            self.cursor = last.rev;
        }
        Ok(changes)
    }

    async fn read_pages(&self, tx: &D::Read) -> Result<Vec<Change>, Error> {
        let mut changes = Vec::new();
        let mut after = self.cursor;
        loop {
            let page = queries::changes_after(tx, after, PAGE).await?;
            let full = i64::try_from(page.len()).expect("a page fits i64") == PAGE;
            for row in page {
                after = row.rev;
                changes.push(Change {
                    tree_id: SedimentreeId::new(
                        row.tree_id
                            .try_into()
                            .expect("a tree id is 32 bytes (CHECK)"),
                    ),
                    rev: row.rev,
                    live: row.live,
                });
            }
            if !full {
                return Ok(changes);
            }
        }
    }

    /// Wait for a commit of this process past the cursor, then return what changed. Loops on a
    /// wake that finds nothing (a commit of an already-seen tree).
    pub async fn next_local(&mut self) -> Result<Vec<Change>, Error> {
        loop {
            let changes = self.poll().await?;
            if !changes.is_empty() {
                return Ok(changes);
            }
            let cursor = self.cursor;
            // The sender is held by `self`, so the channel never closes.
            self.local
                .wait_for(|latest| *latest > cursor)
                .await
                .expect("the feed holds the sender");
        }
    }

    /// The `rev` of the last change returned: the point to resume from.
    pub fn cursor(&self) -> i64 {
        self.cursor
    }
}

impl<D: SqliteDb, M> SedimentreeStore<D, M> {
    /// The highest `rev` committed, or 0 for an empty hive: a point to tail from.
    pub async fn latest_rev(&self) -> Result<i64, Error> {
        let tx = self.db.begin_read().await?;
        let rev = queries::latest_rev(&tx).await;
        D::end_read(tx).await?;
        Ok(rev?)
    }

    /// A feed of the changes after `rev`: 0 for every tree's latest change, or
    /// [`latest_rev`](Self::latest_rev) for only what changes from now.
    pub fn changes_after(&self, rev: i64) -> ChangeFeed<D> {
        ChangeFeed {
            db: self.db.clone(),
            _sender: Arc::clone(&self.shared.committed),
            local: self.shared.committed.subscribe(),
            cursor: rev,
        }
    }
}
