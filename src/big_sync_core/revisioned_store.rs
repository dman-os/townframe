//! Inert source boundary for ordered, replayable revisions.
//!
//! A store owns revision assignment and replay-boundary capture. A reader
//! yields complete atomic revisions, including revisions whose projection has
//! no entries, then emits one replay-complete marker before following later
//! revisions. The contract contains no acknowledgements, persistence policy,
//! tasks, or consumer execution.

use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::keyed_frontier::{
    FrontierEntry, FrontierRead, FrontierRevision, KeyedFrontierError, KeyedFrontierReader,
};

/// One reader result. `Entries` is one complete atomic revision; its entries
/// may be empty when a projection advances progress without typed output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionRead<R, E> {
    Entries { revision: R, entries: Vec<E> },
    ReplayComplete { through: R },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisionReadLimits {
    pub max_entries: usize,
}

impl Default for RevisionReadLimits {
    fn default() -> Self {
        Self { max_entries: 256 }
    }
}

/// Stateful session opened against a revisioned store.
#[async_trait]
pub trait RevisionedStoreReader<R, E, Error>: Send
where
    R: Send + Sync,
    E: Send + Sync,
    Error: Send + Sync,
{
    /// Returns atomic revisions, exactly one replay boundary, then live
    /// revisions. A live reader may wait inside this call for new work.
    async fn next(&mut self) -> Result<RevisionRead<R, E>, Error>;
}

/// Inert source contract for a replayable revision stream.
#[async_trait]
pub trait RevisionedStore: Send + Sync {
    type Revision: Ord + Clone + Send + Sync + 'static;
    type Entry: Send + Sync + 'static;
    type Selector: Send + Sync + 'static;
    type Error: Send + Sync + 'static;
    type Reader<'a>: RevisionedStoreReader<Self::Revision, Self::Entry, Self::Error> + 'a
    where
        Self: 'a;

    /// Return the source's current global revision without opening a reader.
    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error>;

    /// Opens a stateful reader and captures its replay boundary once.
    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: Self::Revision,
        limits: RevisionReadLimits,
    ) -> Result<Self::Reader<'a>, Self::Error>;
}

/// Adapts a keyed-frontier reader to the source-level revision contract.
pub struct KeyedFrontierRevisionReader<'a, K, V> {
    inner: Box<dyn KeyedFrontierReader<K, V> + 'a>,
    pending: std::collections::VecDeque<RevisionRead<FrontierRevision, FrontierEntry<K, V>>>,
    pending_progress: Option<FrontierRevision>,
    replay_complete_seen: bool,
}

impl<'a, K, V> KeyedFrontierRevisionReader<'a, K, V> {
    pub fn new(inner: Box<dyn KeyedFrontierReader<K, V> + 'a>) -> Self {
        Self {
            inner,
            pending: Default::default(),
            pending_progress: None,
            replay_complete_seen: false,
        }
    }
}

#[async_trait]
impl<K, V> RevisionedStoreReader<FrontierRevision, FrontierEntry<K, V>, KeyedFrontierError>
    for KeyedFrontierRevisionReader<'_, K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn next(
        &mut self,
    ) -> Result<RevisionRead<FrontierRevision, FrontierEntry<K, V>>, KeyedFrontierError> {
        if let Some(read) = self.pending.pop_front() {
            return Ok(read);
        }
        if let Some(revision) = self.pending_progress.take() {
            return Ok(RevisionRead::Entries {
                revision,
                entries: Vec::new(),
            });
        }
        match self.inner.next().await? {
            FrontierRead::Entries { entries, through } if entries.is_empty() => {
                Ok(RevisionRead::Entries {
                    revision: through,
                    entries: Vec::new(),
                })
            }
            FrontierRead::Entries { entries, through } => {
                let mut grouped = BTreeMap::<FrontierRevision, Vec<_>>::new();
                for entry in entries {
                    grouped.entry(entry.revision).or_default().push(entry);
                }
                let last = grouped
                    .keys()
                    .next_back()
                    .copied()
                    .expect("entries non-empty");
                self.pending.extend(
                    grouped
                        .into_iter()
                        .map(|(revision, entries)| RevisionRead::Entries { revision, entries }),
                );
                if through > last {
                    self.pending_progress = Some(through);
                }
                Ok(self.pending.pop_front().expect("grouped entries non-empty"))
            }
            FrontierRead::ReplayComplete { through } => {
                if self.replay_complete_seen {
                    return Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "keyed frontier emitted ReplayComplete twice",
                    ))));
                }
                self.replay_complete_seen = true;
                Ok(RevisionRead::ReplayComplete { through })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_revision_watch::LiveRevisionWatch;
    use std::collections::VecDeque;

    #[derive(Debug, thiserror::Error)]
    #[error("script exhausted")]
    struct ScriptError;

    struct ScriptedStore {
        reads: VecDeque<RevisionRead<u64, u64>>,
    }

    struct ScriptedReader {
        reads: VecDeque<RevisionRead<u64, u64>>,
    }

    #[async_trait]
    impl RevisionedStoreReader<u64, u64, ScriptError> for ScriptedReader {
        async fn next(&mut self) -> Result<RevisionRead<u64, u64>, ScriptError> {
            self.reads.pop_front().ok_or(ScriptError)
        }
    }

    #[async_trait]
    impl RevisionedStore for ScriptedStore {
        type Revision = u64;
        type Entry = u64;
        type Selector = ();
        type Error = ScriptError;
        type Reader<'a>
            = ScriptedReader
        where
            Self: 'a;

        async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
            Ok(0)
        }

        async fn open<'a>(
            &'a self,
            (): Self::Selector,
            _after: u64,
            _limits: RevisionReadLimits,
        ) -> Result<Self::Reader<'a>, Self::Error> {
            Ok(ScriptedReader {
                reads: self.reads.clone(),
            })
        }
    }

    #[test]
    fn preserves_atomic_batches_empty_progress_and_live_tail() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([
                    RevisionRead::Entries {
                        revision: 3,
                        entries: vec![1, 2],
                    },
                    RevisionRead::Entries {
                        revision: 4,
                        entries: Vec::new(),
                    },
                    RevisionRead::ReplayComplete { through: 4 },
                    RevisionRead::Entries {
                        revision: 5,
                        entries: vec![3],
                    },
                ]),
            };
            let mut reader = store
                .open((), 0, RevisionReadLimits::default())
                .await
                .unwrap();
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 3,
                    entries: vec![1, 2]
                }
            );
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 4,
                    entries: Vec::new()
                }
            );
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::ReplayComplete { through: 4 }
            );
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 5,
                    entries: vec![3]
                }
            );
        });
    }

    #[test]
    fn live_watch_hides_initial_replay_boundary() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([
                    RevisionRead::Entries {
                        revision: 2,
                        entries: vec![7],
                    },
                    RevisionRead::ReplayComplete { through: 2 },
                    RevisionRead::Entries {
                        revision: 3,
                        entries: vec![8],
                    },
                ]),
            };
            let mut watch = LiveRevisionWatch::open(&store, (), RevisionReadLimits::default())
                .await
                .unwrap();
            assert_eq!(
                watch.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 2,
                    entries: vec![7]
                }
            );
            assert_eq!(
                watch.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 3,
                    entries: vec![8]
                }
            );
        });
    }

    struct FrontierScriptReader {
        reads: VecDeque<crate::keyed_frontier::FrontierRead<u64, u64>>,
    }

    #[async_trait]
    impl crate::keyed_frontier::KeyedFrontierReader<u64, u64> for FrontierScriptReader {
        async fn next(
            &mut self,
        ) -> Result<crate::keyed_frontier::FrontierRead<u64, u64>, KeyedFrontierError> {
            self.reads.pop_front().ok_or_else(|| {
                KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "script exhausted",
                )))
            })
        }
    }

    #[test]
    fn keyed_adapter_groups_entries_and_preserves_progress() {
        futures::executor::block_on(async {
            let mut reader = KeyedFrontierRevisionReader {
                inner: Box::new(FrontierScriptReader {
                    reads: VecDeque::from([
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![
                                FrontierEntry {
                                    key: 1,
                                    revision: 3,
                                    value: Some(10),
                                },
                                FrontierEntry {
                                    key: 2,
                                    revision: 3,
                                    value: Some(20),
                                },
                            ],
                            through: 3,
                        },
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![FrontierEntry {
                                key: 3,
                                revision: 4,
                                value: Some(30),
                            }],
                            through: 5,
                        },
                        crate::keyed_frontier::FrontierRead::ReplayComplete { through: 5 },
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![FrontierEntry {
                                key: 4,
                                revision: 6,
                                value: Some(40),
                            }],
                            through: 6,
                        },
                    ]),
                }),
                pending: VecDeque::new(),
                pending_progress: None,
                replay_complete_seen: false,
            };
            let RevisionRead::Entries { revision, entries } = reader.next().await.unwrap() else {
                panic!("expected grouped entries");
            };
            assert_eq!(revision, 3);
            assert_eq!(entries.len(), 2);
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 4,
                    entries: vec![FrontierEntry {
                        key: 3,
                        revision: 4,
                        value: Some(30)
                    }],
                }
            );
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 5,
                    entries: Vec::new(),
                }
            );
            assert_eq!(
                reader.next().await.unwrap(),
                RevisionRead::ReplayComplete { through: 5 }
            );
            assert!(matches!(
                reader.next().await.unwrap(),
                RevisionRead::Entries { revision: 6, .. }
            ));
        });
    }
}
