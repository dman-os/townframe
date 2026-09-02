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
        assert!(
            limits.max_entries > 0,
            "admission read limit must be non-zero"
        );
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
                        .admission_events_after(self.cursor, limits.max_entries as u32)
                        .await?
                } else {
                    self.buffered
                        .drain(..limits.max_entries.min(self.buffered.len()))
                        .collect()
                }
            } else {
                self.store
                    .admission_events_after(self.cursor, limits.max_entries as u32)
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
