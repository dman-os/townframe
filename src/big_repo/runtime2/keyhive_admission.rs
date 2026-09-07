//! Durable Keyhive admission-log source shared by runtime2 workers.

use crate::runtime2::Timer;
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use future_form::Sendable;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use utils_rs::prelude::eyre;

/// Idle recovery bound only: `notify_one` wakeups deliver new rows
/// promptly, so this is how long a missed wakeup can linger — not a
/// latency target. Three workers tail the same admission table; a tighter
/// poll would multiply idle read-pool queries for no benefit.
pub(crate) const IDLE_POLL: Duration = Duration::from_millis(250);

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
    pub(crate) timer: Arc<dyn Timer<Sendable>>,
}

pub(crate) struct Reader {
    store: SqliteBigRepoStore,
    timer: Arc<dyn Timer<Sendable>>,
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
            timer: Arc::clone(&self.timer),
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

            let rows = if self.replay_complete {
                if self.buffered.is_empty() {
                    self.store
                        .admission_events_after(self.cursor, limits.max_entries.get() as u32)
                        .await?
                } else {
                    self.buffered
                        .drain(..limits.max_entries.get().min(self.buffered.len()))
                        .collect()
                }
            } else {
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
            self.timer.sleep(IDLE_POLL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime2::tasks::TokioTimer;
    use crate::store::sqlite::SqliteBigRepoStore;
    use big_sync_core::BuckId;
    use big_sync_core::revisioned_store::contract::{
        RevisionedStoreContractHarness, assert_revisioned_store_contract,
    };
    use sqlx_utils_rs::SqlCtx;
    use subduction_keyhive::storage::StorageHash;
    use utils_rs::prelude::async_trait;

    struct SqliteAdmissionHarness {
        _sql: SqlCtx,
        store: SqliteBigRepoStore,
        source: Store,
    }

    impl SqliteAdmissionHarness {
        async fn new() -> Self {
            let sql = SqlCtx::memory().await.expect("create sqlite database");
            let store =
                SqliteBigRepoStore::new(sql.clone(), "admission-contract", BuckId::MAX_LEVEL)
                    .await
                    .expect("create sqlite big repo store");
            let source = Store {
                store: store.clone(),
                timer: Arc::new(TokioTimer),
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
}
