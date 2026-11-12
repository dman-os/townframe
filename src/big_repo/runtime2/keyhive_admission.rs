//! Durable Keyhive admission-log source shared by runtime2 workers.

use crate::store::sqlite::SqliteBigRepoStore;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use std::collections::VecDeque;
use std::sync::Arc;
use utils_rs::prelude::eyre;

/// One admitted Keyhive event from the durable admission log.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AdmittedRow {
    pub(crate) seq: u64,
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) event_hash: [u8; 32],
    pub(crate) source_id: Option<Vec<u8>>,
}

#[derive(Clone)]
pub(crate) struct Store {
    pub(crate) store: SqliteBigRepoStore,
}

/// Tails the admission log from a cursor.
///
/// A caught-up reader parks on the store's admission watermark rather than
/// on a timer: [`SqliteBigRepoStore::append_admitted_events`] is the only
/// writer of the log and raises the watermark after every commit that adds
/// rows. No backstop poll is kept, because there is no append the watermark
/// does not see — rows admitted before this process booted (the only ones it
/// never announces) are below the `through` frozen at `open` and served by
/// the replay.
pub(crate) struct Reader {
    store: SqliteBigRepoStore,
    appends: tokio::sync::watch::Receiver<u64>,
    cursor: u64,
    through: u64,
    replay_complete: bool,
    buffered: VecDeque<crate::store::sqlite::AdmissionEventRow>,
}

#[async_trait::async_trait]
impl RevisionedStore for Store {
    type Revision = u64;
    type Entry = AdmittedRow;
    type Selector = ();
    type Error = eyre::Report;
    type Reader<'a> = Reader;

    async fn latest_revision(&self) -> Result<u64, eyre::Report> {
        Ok(self.store.admission_head().await?)
    }

    async fn open<'a>(&'a self, (): (), after: u64) -> Result<Self::Reader<'a>, eyre::Report> {
        Ok(Reader {
            store: self.store.clone(),
            appends: self.store.admission_appends(),
            cursor: after,
            through: self.latest_revision().await?,
            replay_complete: false,
            buffered: VecDeque::new(),
        })
    }
}

#[async_trait::async_trait]
impl RevisionedStoreReader<u64, AdmittedRow, eyre::Report> for Reader {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, AdmittedRow>, eyre::Report> {
        loop {
            if !self.replay_complete && self.cursor >= self.through {
                self.replay_complete = true;
                return Ok(RevisionRead::ReplayComplete {
                    through: self.through,
                });
            }

            let rows = if self.replay_complete && !self.buffered.is_empty() {
                self.buffered
                    .drain(..limits.max_entries.get().min(self.buffered.len()))
                    .collect()
            } else {
                // Mark the watermark seen *before* reading the table, never
                // after: an append committed before this mark is in the read
                // below, and one that publishes after it leaves `changed()`
                // ready for the park at the bottom of the loop, so no append
                // can fall between the read and the park unannounced. A
                // cancelled read (callers drive this from `select!`) has moved
                // nothing but the mark, and the retry re-marks and re-reads.
                self.appends.borrow_and_update();
                self.store
                    .admission_events_after(self.cursor, limits.max_entries.get() as u32)
                    .await?
            };
            let rows = if self.replay_complete {
                rows
            } else {
                let mut replay = Vec::new();
                for row in rows {
                    if row.seq <= self.through {
                        replay.push(row);
                    } else {
                        self.buffered.push_back(row);
                    }
                }
                replay
            };
            let entries = rows
                .into_iter()
                .map(|row| AdmittedRow {
                    seq: row.seq,
                    bytes: row.bytes.into(),
                    event_hash: row.event_hash,
                    source_id: row.source_id,
                })
                .collect::<Vec<_>>();
            if let Some(revision) = entries.iter().map(|row| row.seq).max() {
                self.cursor = revision;
                return Ok(RevisionRead::Entries { revision, entries });
            }
            if !self.replay_complete {
                self.replay_complete = true;
                return Ok(RevisionRead::ReplayComplete {
                    through: self.through,
                });
            }
            self.appends
                .changed()
                .await
                .expect("the admission watermark sender lives in the store this reader holds");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sqlite::SqliteBigRepoStore;
    use big_sync_core::BuckId;
    use big_sync_core::revisioned_store::contract::RevisionedStoreContractHarness;
    use big_sync_core::revisioned_store::contract::assert_revisioned_store_contract;
    use sqlite_rs::Database;
    use subduction_keyhive::storage::StorageHash;
    use utils_rs::prelude::async_trait;

    struct SqliteAdmissionHarness {
        _sql: Database,
        store: SqliteBigRepoStore,
        source: Store,
    }

    impl SqliteAdmissionHarness {
        async fn new() -> Self {
            let sql = sqlite_rs::SqliteRuntime::new(Default::default())
                .ephemeral(&[])
                .expect("create sqlite database");
            let store =
                SqliteBigRepoStore::new(sql.clone(), "admission-contract", BuckId::MAX_LEVEL)
                    .await
                    .expect("create sqlite big repo store");
            let source = Store {
                store: store.clone(),
            };
            Self {
                _sql: sql,
                store,
                source,
            }
        }
    }

    #[async_trait]
    impl RevisionedStoreContractHarness for SqliteAdmissionHarness {
        type Store = Store;

        fn store(&self) -> &Self::Store {
            &self.source
        }

        async fn commit(
            &self,
            entry: <Self::Store as RevisionedStore>::Entry,
        ) -> Result<u64, <Self::Store as RevisionedStore>::Error> {
            self.store
                .save_keyhive_event(
                    StorageHash::new(entry.event_hash),
                    entry.bytes.to_vec(),
                    entry.source_id.map(|bytes| {
                        subduction_keyhive::KeyhivePeerId::from_bytes(
                            bytes.try_into().expect("source id is 32 bytes"),
                        )
                    }),
                )
                .await
                .expect("save keyhive event");
            self.store
                .append_admitted_events(vec![StorageHash::new(entry.event_hash)], None)
                .await
        }

        fn entry(&self, index: u64) -> <Self::Store as RevisionedStore>::Entry {
            AdmittedRow {
                seq: index,
                bytes: Arc::from(vec![index as u8]),
                event_hash: [index as u8; 32],
                source_id: None,
            }
        }

        fn entries_match(
            &self,
            expected: &<Self::Store as RevisionedStore>::Entry,
            actual: &<Self::Store as RevisionedStore>::Entry,
        ) -> bool {
            // The reader stamps the delivered seq into the row; the harness's
            // expected rows carry a placeholder seq.
            expected.event_hash == actual.event_hash && expected.bytes == actual.bytes
        }

        fn all_selector(&self, _after: u64) -> <Self::Store as RevisionedStore>::Selector {}
    }

    #[tokio::test]
    async fn sqlite_admission_revisioned_store_contract() {
        assert_revisioned_store_contract(&SqliteAdmissionHarness::new().await).await;
    }

    /// Rows admitted while a reader is replaying carry a `seq` above the
    /// `through` the reader froze at `open`. They must be buffered and released
    /// only after the replay boundary: a reader that stops at `through` loses
    /// them, and one that re-reads from its cursor serves them twice.
    #[tokio::test]
    async fn the_replay_boundary_buffers_rows_admitted_after_open() {
        let harness = SqliteAdmissionHarness::new().await;
        harness.commit(harness.entry(1)).await.expect("admit row 1");
        harness.commit(harness.entry(2)).await.expect("admit row 2");

        // `through` is frozen at 2 here; rows 3 and 4 land after it.
        let mut reader = harness.source.open((), 0).await.expect("open reader");
        harness.commit(harness.entry(3)).await.expect("admit row 3");
        harness.commit(harness.entry(4)).await.expect("admit row 4");

        let limits = RevisionReadLimits::default();
        assert_eq!(
            reader.next(limits).await.expect("read replay batch"),
            RevisionRead::Entries {
                revision: 2,
                entries: vec![harness.entry(1), harness.entry(2)],
            },
            "replay must stop at the frozen boundary and buffer what followed"
        );
        assert_eq!(
            reader.next(limits).await.expect("read replay boundary"),
            RevisionRead::ReplayComplete { through: 2 },
        );
        assert_eq!(
            reader.next(limits).await.expect("read buffered rows"),
            RevisionRead::Entries {
                revision: 4,
                entries: vec![harness.entry(3), harness.entry(4)],
            },
            "rows admitted after the boundary arrive after it, exactly once"
        );
        // The live tail resumes past the buffered rows without re-serving them.
        harness.commit(harness.entry(5)).await.expect("admit row 5");
        assert_eq!(
            reader.next(limits).await.expect("read live tail"),
            RevisionRead::Entries {
                revision: 5,
                entries: vec![harness.entry(5)],
            },
        );
    }

    /// A caught-up reader parks on the admission watermark, not on a poll: the
    /// append itself is what wakes it. Nothing here advances time, so a reader
    /// that went back to sleeping on a timer between table reads would leave
    /// the parked read pending and only the hard test timeout would fire.
    #[tokio::test]
    async fn a_caught_up_reader_wakes_on_append() {
        let harness = SqliteAdmissionHarness::new().await;
        let mut reader = harness.source.open((), 0).await.expect("open reader");
        let limits = RevisionReadLimits::default();
        assert_eq!(
            reader.next(limits).await.expect("read replay boundary"),
            RevisionRead::ReplayComplete { through: 0 },
            "an empty log replays nothing"
        );

        let parked = tokio::spawn(async move { reader.next(limits).await });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !parked.is_finished(),
            "a reader with nothing to read must park, not return"
        );
        harness.commit(harness.entry(1)).await.expect("admit row 1");
        assert_eq!(
            parked
                .await
                .expect("reader task must not panic")
                .expect("read the admitted row"),
            RevisionRead::Entries {
                revision: 1,
                entries: vec![harness.entry(1)],
            },
        );
    }
}
