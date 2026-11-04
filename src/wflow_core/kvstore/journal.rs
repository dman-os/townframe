use crate::interlude::*;

use super::atomic::CommitError;
use super::atomic::Mutation;
use super::atomic::Store;
use super::atomic::Writer;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::num::NonZeroUsize;
use std::ops::Bound;

const METADATA_KEY: &[u8] = b"\0wflow-journal";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metadata {
    version: u32,
    partition_incarnation: u64,
    authority_epoch: u64,
    frontier: u64,
    #[serde(default)]
    snapshot_cut: u64,
    #[serde(default)]
    compacted_through: u64,
}

impl Metadata {
    fn validate(&self) -> Res<()> {
        if self.version != FORMAT_VERSION {
            eyre::bail!("unsupported journal format {}", self.version);
        }
        if self.snapshot_cut > self.frontier {
            eyre::bail!(
                "corrupt journal metadata: snapshot cut {} exceeds frontier {}",
                self.snapshot_cut,
                self.frontier
            );
        }
        if self.compacted_through > self.snapshot_cut {
            eyre::bail!(
                "corrupt journal metadata: compacted_through {} exceeds snapshot cut {}",
                self.compacted_through,
                self.snapshot_cut
            );
        }
        Ok(())
    }
}

pub struct Journal<W> {
    writer: W,
    metadata: Metadata,
    revision: u64,
    interrupted: bool,
}

impl<W: Writer> Journal<W> {
    pub async fn open(mut writer: W, partition_incarnation: u64) -> Res<Self> {
        let read = writer
            .reader()
            .read(
                &[METADATA_KEY.into()],
                (Bound::Unbounded, Bound::Unbounded),
                1,
            )
            .await?;
        let mut metadata = match read.values[0].as_deref() {
            Some(bytes) => {
                let metadata: Metadata = serde_json::from_slice(bytes)?;
                metadata.validate()?;
                if metadata.partition_incarnation != partition_incarnation {
                    eyre::bail!("journal belongs to another partition incarnation");
                }
                metadata
            }
            None if read.entries.is_empty() => Metadata {
                version: FORMAT_VERSION,
                partition_incarnation,
                authority_epoch: 0,
                frontier: 0,
                snapshot_cut: 0,
                compacted_through: 0,
            },
            None => {
                eyre::bail!("unversioned nonempty journal; offline migration or reset required")
            }
        };
        metadata.authority_epoch = metadata.authority_epoch.checked_add(1).unwrap();
        let revision = writer
            .commit(read.revision, vec![metadata_mutation(&metadata)])
            .await?;
        Ok(Self {
            writer,
            metadata,
            revision,
            interrupted: false,
        })
    }

    pub fn frontier(&self) -> u64 {
        self.metadata.frontier
    }

    pub fn snapshot_cut(&self) -> u64 {
        self.metadata.snapshot_cut
    }

    pub fn compacted_through(&self) -> u64 {
        self.metadata.compacted_through
    }

    pub fn authority_epoch(&self) -> u64 {
        self.metadata.authority_epoch
    }

    pub fn reader(&self) -> JournalReader {
        JournalReader {
            store: self.writer.reader(),
        }
    }

    pub async fn append(
        &mut self,
        expected_frontier: u64,
        entry: Arc<[u8]>,
        indexes: Vec<Mutation>,
    ) -> Result<u64, CommitError> {
        if self.interrupted {
            return Err(CommitError::NotCommitted(ferr!(
                "reopen journal to reconcile interrupted commit"
            )));
        }
        if expected_frontier != self.metadata.frontier {
            return Err(CommitError::NotCommitted(ferr!(
                "unexpected journal frontier"
            )));
        }
        let mut metadata = self.metadata.clone();
        metadata.frontier = metadata.frontier.checked_add(1).unwrap();
        let mut mutations = Vec::with_capacity(indexes.len() + 2);
        mutations.push(metadata_mutation(&metadata));
        mutations.push(Mutation {
            key: entry_key(metadata.frontier),
            value: Some(entry),
        });
        mutations.extend(indexes.into_iter().map(|mutation| Mutation {
            key: index_key(&mutation.key),
            value: mutation.value,
        }));
        self.commit_metadata(metadata, mutations).await?;
        Ok(self.metadata.frontier)
    }

    /// Durably publishes a snapshot at `cut` and advances `snapshot_cut` in a
    /// single transaction before any old snapshot or log entry is reclaimed.
    pub async fn save_snapshot(
        &mut self,
        cut: u64,
        snapshot: Arc<[u8]>,
    ) -> Result<u64, CommitError> {
        if self.interrupted {
            return Err(CommitError::NotCommitted(ferr!(
                "reopen journal to reconcile interrupted commit"
            )));
        }
        if cut < self.metadata.snapshot_cut || cut > self.metadata.frontier {
            return Err(CommitError::NotCommitted(ferr!(
                "invalid snapshot cut {cut} for snapshot_cut {} and frontier {}",
                self.metadata.snapshot_cut,
                self.metadata.frontier
            )));
        }
        let mut metadata = self.metadata.clone();
        metadata.snapshot_cut = cut;
        let mutations = vec![
            metadata_mutation(&metadata),
            Mutation {
                key: snapshot_key(cut),
                value: Some(snapshot),
            },
        ];
        self.commit_metadata(metadata, mutations).await?;
        Ok(self.metadata.snapshot_cut)
    }

    /// Reclaims log entries through `through` (and any superseded snapshots
    /// strictly before `snapshot_cut`), requiring a durably published snapshot
    /// at `snapshot_cut >= through`.
    pub async fn compact(&mut self, through: u64) -> Result<u64, CommitError> {
        if self.interrupted {
            return Err(CommitError::NotCommitted(ferr!(
                "reopen journal to reconcile interrupted commit"
            )));
        }
        if through > self.metadata.snapshot_cut {
            return Err(CommitError::NotCommitted(ferr!(
                "cannot compact through {through} beyond published snapshot cut {}",
                self.metadata.snapshot_cut
            )));
        }
        if through <= self.metadata.compacted_through {
            return Ok(self.metadata.compacted_through);
        }
        let read = self
            .writer
            .reader()
            .read(
                &[snapshot_key(self.metadata.snapshot_cut)],
                (
                    Bound::Included(snapshot_key(0)),
                    Bound::Excluded(snapshot_key(self.metadata.snapshot_cut)),
                ),
                usize::MAX,
            )
            .await
            .map_err(CommitError::NotCommitted)?;
        if read.values[0].is_none() {
            return Err(CommitError::NotCommitted(ferr!(
                "published snapshot at cut {} is missing",
                self.metadata.snapshot_cut
            )));
        }
        let mut metadata = self.metadata.clone();
        metadata.compacted_through = through;
        let entry_deletes = (self.metadata.compacted_through + 1)..=through;
        let mut mutations =
            Vec::with_capacity(1 + entry_deletes.size_hint().0 + read.entries.len());
        mutations.push(metadata_mutation(&metadata));
        for position in entry_deletes {
            mutations.push(Mutation {
                key: entry_key(position),
                value: None,
            });
        }
        for old_snapshot in read.entries {
            mutations.push(Mutation {
                key: old_snapshot.key,
                value: None,
            });
        }
        self.commit_metadata(metadata, mutations).await?;
        Ok(self.metadata.compacted_through)
    }

    async fn commit_metadata(
        &mut self,
        metadata: Metadata,
        mutations: Vec<Mutation>,
    ) -> Result<(), CommitError> {
        self.interrupted = true;
        match self.writer.commit(self.revision, mutations).await {
            Ok(revision) => {
                self.revision = revision;
                self.metadata = metadata;
                self.interrupted = false;
                Ok(())
            }
            Err(error @ CommitError::NotCommitted(_)) => {
                self.interrupted = false;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

fn metadata_mutation(metadata: &Metadata) -> Mutation {
    Mutation {
        key: METADATA_KEY.into(),
        value: Some(serde_json::to_vec(metadata).unwrap().into()),
    }
}

fn entry_key(position: u64) -> Arc<[u8]> {
    let mut key = [0; 9];
    key[0] = 1;
    key[1..].copy_from_slice(&position.to_be_bytes());
    key.into()
}

fn index_key(key: &[u8]) -> Arc<[u8]> {
    let mut prefixed = Vec::with_capacity(key.len() + 1);
    prefixed.push(2);
    prefixed.extend_from_slice(key);
    prefixed.into()
}

fn snapshot_key(cut: u64) -> Arc<[u8]> {
    let mut key = [0; 9];
    key[0] = 3;
    key[1..].copy_from_slice(&cut.to_be_bytes());
    key.into()
}

#[derive(Clone)]
pub struct JournalReader {
    store: Arc<dyn Store>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct JournalEntry {
    pub position: u64,
    pub value: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSnapshot {
    pub cut: u64,
    pub compacted_through: u64,
    pub frontier: u64,
    pub value: Arc<[u8]>,
}

#[derive(Debug)]
pub struct Page {
    pub frontier: u64,
    pub entries: Vec<JournalEntry>,
}

impl JournalReader {
    pub async fn index(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let mut read = self
            .store
            .read(&[index_key(key)], (Bound::Unbounded, Bound::Unbounded), 0)
            .await?;
        Ok(read.values.pop().unwrap())
    }

    /// Reads the journal metadata and the published snapshot blob in one
    /// consistent store read. If `snapshot_cut > 0` and the corresponding
    /// snapshot blob is missing, returns a recovery error.
    pub async fn snapshot(&self) -> Res<Option<JournalSnapshot>> {
        let read = self
            .store
            .read(
                &[METADATA_KEY.into()],
                (
                    Bound::Included(snapshot_key(0)),
                    Bound::Excluded(Arc::from(&b"\x04"[..])),
                ),
                usize::MAX,
            )
            .await?;
        let metadata: Metadata = serde_json::from_slice(
            read.values[0]
                .as_deref()
                .ok_or_else(|| ferr!("journal metadata missing"))?,
        )?;
        metadata.validate()?;
        if metadata.snapshot_cut == 0 {
            return Ok(None);
        }
        let expected_key = snapshot_key(metadata.snapshot_cut);
        let entry = read
            .entries
            .into_iter()
            .find(|entry| entry.key == expected_key)
            .ok_or_else(|| {
                ferr!(
                    "missing published journal snapshot at cut {}",
                    metadata.snapshot_cut
                )
            })?;
        Ok(Some(JournalSnapshot {
            cut: metadata.snapshot_cut,
            compacted_through: metadata.compacted_through,
            frontier: metadata.frontier,
            value: entry.value,
        }))
    }

    pub async fn read(&self, from: u64, limit: NonZeroUsize) -> Res<Page> {
        assert!(from > 0, "journal positions start at one");
        let read = self
            .store
            .read(
                &[METADATA_KEY.into()],
                (
                    Bound::Included(entry_key(from)),
                    Bound::Excluded(Arc::from(&b"\x02"[..])),
                ),
                limit.get(),
            )
            .await?;
        let metadata: Metadata = serde_json::from_slice(
            read.values[0]
                .as_deref()
                .ok_or_else(|| ferr!("journal metadata missing"))?,
        )?;
        metadata.validate()?;
        if from <= metadata.compacted_through {
            eyre::bail!(
                "journal position {from} has been compacted through {}",
                metadata.compacted_through
            );
        }
        let count = metadata
            .frontier
            .saturating_sub(from - 1)
            .min(limit.get() as u64);
        if read.entries.len() as u64 != count {
            eyre::bail!("journal is not a contiguous committed prefix");
        }
        let mut entries = Vec::with_capacity(read.entries.len());
        for (offset, entry) in read.entries.into_iter().enumerate() {
            let position = from.checked_add(offset as u64).unwrap();
            if entry.key != entry_key(position) {
                eyre::bail!("missing journal position {position}");
            }
            entries.push(JournalEntry {
                position,
                value: entry.value,
            });
        }
        Ok(Page {
            frontier: metadata.frontier,
            entries,
        })
    }

    pub fn tail(&self, from: u64, limit: NonZeroUsize) -> BoxStream<'_, Res<JournalEntry>> {
        let revisions = self.store.subscribe();
        futures::stream::try_unfold(
            (from, Vec::new().into_iter(), revisions),
            move |(mut cursor, mut pending, mut revisions)| async move {
                loop {
                    if let Some(entry) = pending.next() {
                        return Ok(Some((entry, (cursor, pending, revisions))));
                    }
                    revisions.borrow_and_update();
                    let page = self.read(cursor, limit).await?;
                    if let Some(last) = page.entries.last() {
                        cursor = last.position.checked_add(1).unwrap();
                        pending = page.entries.into_iter();
                    } else if revisions.changed().await.is_err() {
                        return Ok(None);
                    }
                }
            },
        )
        .boxed()
    }
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
