use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::sync::Arc;

use super::*;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FixtureError {
    #[error("output version unavailable: {0:?}")]
    Unavailable(OutputVersion),
}

#[derive(Default)]
pub(crate) struct Activity {
    pub(crate) opens: AtomicUsize,
    pub(crate) reads: AtomicUsize,
}

pub(crate) struct VersionedProducer {
    pub(crate) id: BackendId,
    current: BTreeMap<RelPath, OutputVersion>,
    retained: HashMap<OutputVersion, Arc<[u8]>>,
    pub(crate) activity: Arc<Activity>,
}

impl VersionedProducer {
    pub(crate) fn new() -> Self {
        Self {
            id: BackendId("fixture".into()),
            current: BTreeMap::new(),
            retained: HashMap::new(),
            activity: Arc::default(),
        }
    }

    pub(crate) fn publish(&mut self, path: &str, output: &str, version: &str, bytes: &[u8]) {
        let selected = OutputVersion {
            output: output.as_bytes().to_vec(),
            version: version.as_bytes().to_vec(),
        };
        let replaced = self.retained.insert(selected.clone(), Arc::from(bytes));
        assert!(replaced.is_none(), "a version cannot be published twice");
        self.current
            .insert(RelPath::try_new(vec![path.into()]).unwrap(), selected);
    }

    pub(crate) fn evict(&mut self, selected: &OutputVersion) {
        assert!(self.retained.remove(selected).is_some());
    }
}

pub(crate) struct ObservedTree(VecDeque<TreeEntry>);

impl BackendTree for ObservedTree {
    type Error = FixtureError;

    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        Ok(self.0.pop_front())
    }
}

pub(crate) struct VersionReader {
    bytes: Arc<[u8]>,
    activity: Arc<Activity>,
}

impl ByteReader for VersionReader {
    type Error = FixtureError;

    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        self.activity.reads.fetch_add(1, Ordering::Relaxed);
        let Ok(start) = usize::try_from(offset) else {
            return Ok(0);
        };
        let available = self.bytes.get(start..).unwrap_or_default();
        let count = buffer.len().min(available.len());
        buffer[..count].copy_from_slice(&available[..count]);
        Ok(count)
    }
}

impl Producer for VersionedProducer {
    type Error = FixtureError;
    type Tree = ObservedTree;
    type Reader = VersionReader;

    fn id(&self) -> &BackendId {
        &self.id
    }

    async fn observe(&self) -> Result<Self::Tree, Self::Error> {
        Ok(ObservedTree(
            self.current
                .iter()
                .map(|(path, selected)| TreeEntry {
                    path: path.clone(),
                    description: Description::File {
                        source: Source {
                            backend: self.id.clone(),
                            output: selected.clone(),
                        },
                        // Size need not be known before lazy production.
                        size: None,
                    },
                })
                .collect(),
        ))
    }

    async fn open(&self, selected: &OutputVersion) -> Result<Self::Reader, Self::Error> {
        self.activity.opens.fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .retained
            .get(selected)
            .ok_or_else(|| FixtureError::Unavailable(selected.clone()))?;
        Ok(VersionReader {
            bytes: Arc::clone(bytes),
            activity: Arc::clone(&self.activity),
        })
    }
}
