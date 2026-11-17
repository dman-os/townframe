use crate::interlude::*;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;
use tokio::sync::watch;

pub type KeyRange = (Bound<Arc<[u8]>>, Bound<Arc<[u8]>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Arc<[u8]>,
    pub value: Arc<[u8]>,
}

#[derive(Debug, Clone)]
pub struct Mutation {
    pub key: Arc<[u8]>,
    pub value: Option<Arc<[u8]>>,
}

#[derive(Debug)]
pub struct Read {
    pub revision: u64,
    pub values: Vec<Option<Arc<[u8]>>>,
    pub entries: Vec<Entry>,
}

#[derive(Debug)]
pub enum CommitError {
    Conflict { actual_revision: u64 },
    NotCommitted(eyre::Report),
    Unknown(eyre::Report),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict { actual_revision } => {
                write!(fmt, "namespace revision changed to {actual_revision}")
            }
            Self::NotCommitted(error) => write!(fmt, "not committed: {error}"),
            Self::Unknown(error) => write!(fmt, "unknown commit outcome: {error}"),
        }
    }
}

impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conflict { .. } => None,
            Self::NotCommitted(error) | Self::Unknown(error) => Some(error.as_ref()),
        }
    }
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn read(&self, keys: &[Arc<[u8]>], range: KeyRange, limit: usize) -> Res<Read>;
    fn subscribe(&self) -> watch::Receiver<u64>;
}

#[async_trait]
pub trait Writer: Send + Sync {
    fn reader(&self) -> Arc<dyn Store>;
    async fn commit(
        &mut self,
        expected_revision: u64,
        mutations: Vec<Mutation>,
    ) -> Result<u64, CommitError>;
}

#[derive(Clone)]
pub struct MemoryStore {
    state: Arc<Mutex<MemoryState>>,
    revisions: watch::Sender<u64>,
}

#[derive(Default)]
struct MemoryState {
    entries: BTreeMap<Arc<[u8]>, Arc<[u8]>>,
    revision: u64,
    writer_owned: bool,
}

impl Default for MemoryStore {
    fn default() -> Self {
        let (revisions, _) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            revisions,
        }
    }
}

impl MemoryStore {
    pub fn writer(&self) -> Res<MemoryWriter> {
        let mut state = self.state.lock().unwrap();
        if state.writer_owned {
            return Err(ferr!("namespace already has a writer"));
        }
        state.writer_owned = true;
        Ok(MemoryWriter {
            store: self.clone(),
        })
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn read(&self, keys: &[Arc<[u8]>], range: KeyRange, limit: usize) -> Res<Read> {
        let state = self.state.lock().unwrap();
        Ok(Read {
            revision: state.revision,
            values: keys
                .iter()
                .map(|key| state.entries.get(key).cloned())
                .collect(),
            entries: state
                .entries
                .range::<Arc<[u8]>, _>(range)
                .take(limit)
                .map(|(key, value)| Entry {
                    key: Arc::clone(key),
                    value: Arc::clone(value),
                })
                .collect(),
        })
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.revisions.subscribe()
    }
}

pub struct MemoryWriter {
    store: MemoryStore,
}

impl Drop for MemoryWriter {
    fn drop(&mut self) {
        let mut state = self.store.state.lock().unwrap();
        assert!(state.writer_owned);
        state.writer_owned = false;
    }
}

#[async_trait]
impl Writer for MemoryWriter {
    fn reader(&self) -> Arc<dyn Store> {
        Arc::new(self.store.clone())
    }

    async fn commit(
        &mut self,
        expected_revision: u64,
        mutations: Vec<Mutation>,
    ) -> Result<u64, CommitError> {
        let mut state = self.store.state.lock().unwrap();
        if state.revision != expected_revision {
            return Err(CommitError::Conflict {
                actual_revision: state.revision,
            });
        }
        let revision = state.revision.checked_add(1).unwrap();
        for mutation in mutations {
            match mutation.value {
                Some(value) => {
                    state.entries.insert(mutation.key, value);
                }
                None => {
                    state.entries.remove(&mutation.key);
                }
            }
        }
        state.revision = revision;
        self.store.revisions.send_replace(revision);
        Ok(revision)
    }
}

#[cfg(any(test, feature = "test-harness"))]
#[path = "atomic_tests.rs"]
pub mod tests;
