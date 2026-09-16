//! An in-memory store: what tests and short-lived processes use.
//!
//! It is the reference implementation of [`VtreeStore`]: the same ordering, the
//! same one-transaction rep updates, with no database in the way.

use super::{VtreeStore, implied_parents};
use crate::backend::BackendId;
use crate::delta::Delta;
use crate::entry::Entry;
use crate::interlude::*;
use crate::path::RelPath;

/// One rep's rows.
#[derive(Debug, Default)]
struct RepState {
    generation: u64,
    entries: BTreeMap<RelPath, Entry>,
}

/// Reps held in memory.
#[derive(Debug, Default)]
pub struct MemVtreeStore {
    reps: RwLock<BTreeMap<BackendId, RepState>>,
}

impl MemVtreeStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl VtreeStore for MemVtreeStore {
    async fn apply(&self, rep: &BackendId, deltas: &[Delta]) -> Result<u64> {
        let mut reps = self.reps.write().expect(ERROR_MUTEX);
        if deltas.is_empty() {
            return Ok(reps.get(rep).map_or(0, |state| state.generation));
        }
        let state = reps.entry(rep.clone()).or_default();
        for delta in deltas {
            match delta {
                Delta::Removed { path, .. } => {
                    state.entries.remove(path);
                }
                Delta::Added { path, entry }
                | Delta::Touched { path, entry }
                | Delta::Changed {
                    path, to: entry, ..
                } => {
                    for parent in implied_parents(path) {
                        state
                            .entries
                            .entry(parent)
                            .or_insert_with(|| Entry::dir(None));
                    }
                    state.entries.insert(path.clone(), entry.clone());
                }
            }
        }
        state.generation += 1;
        Ok(state.generation)
    }

    async fn entry(&self, rep: &BackendId, path: &RelPath) -> Result<Option<Entry>> {
        let reps = self.reps.read().expect(ERROR_MUTEX);
        Ok(reps
            .get(rep)
            .and_then(|state| state.entries.get(path))
            .cloned())
    }

    async fn scan_page(
        &self,
        rep: &BackendId,
        from: Option<&RelPath>,
        limit: usize,
    ) -> Result<Vec<(RelPath, Entry)>> {
        let reps = self.reps.read().expect(ERROR_MUTEX);
        let Some(state) = reps.get(rep) else {
            return Ok(Vec::new());
        };
        let page = match from {
            Some(from) => state
                .entries
                .range((Bound::Excluded(from.clone()), Bound::Unbounded))
                .take(limit)
                .map(|(path, entry)| (path.clone(), entry.clone()))
                .collect(),
            None => state
                .entries
                .iter()
                .take(limit)
                .map(|(path, entry)| (path.clone(), entry.clone()))
                .collect(),
        };
        Ok(page)
    }

    async fn generation(&self, rep: &BackendId) -> Result<Option<u64>> {
        let reps = self.reps.read().expect(ERROR_MUTEX);
        Ok(reps.get(rep).map(|state| state.generation))
    }

    async fn reps(&self) -> Result<Vec<BackendId>> {
        let reps = self.reps.read().expect(ERROR_MUTEX);
        Ok(reps.keys().cloned().collect())
    }

    async fn drop_rep(&self, rep: &BackendId) -> Result<bool> {
        let mut reps = self.reps.write().expect(ERROR_MUTEX);
        Ok(reps.remove(rep).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{StatFingerprint, TimeStamp, Token};

    fn path(text: &str) -> RelPath {
        RelPath::try_new(text.split('/').map(OsString::from).collect()).expect(ERROR_PARSE)
    }

    fn file(digest: u8) -> Entry {
        Entry::file(
            Token::blake3([digest; 32]),
            StatFingerprint {
                len: 1,
                mode: 0o644,
                mtime: TimeStamp { secs: 0, nanos: 0 },
            },
        )
    }

    fn added(text: &str, digest: u8) -> Delta {
        Delta::Added {
            path: path(text),
            entry: file(digest),
        }
    }

    async fn store() -> MemVtreeStore {
        let store = MemVtreeStore::new();
        store
            .apply(
                &BackendId::new("fs"),
                &[
                    added("notes/plan.md", 1),
                    added("z.txt", 2),
                    added("a.txt", 3),
                ],
            )
            .await
            .expect("the rep applies");
        store
    }

    #[tokio::test]
    async fn scans_come_back_in_path_order_and_resume_at_a_cursor() -> Result<()> {
        let store = store().await;
        let rep = BackendId::new("fs");

        let all = store.scan_page(&rep, None, 10).await?;
        assert_eq!(
            all.iter()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>(),
            vec!["a.txt", "notes", "notes/plan.md", "z.txt"],
            "implied directories are recorded, in walk order"
        );

        let after_a = store.scan_page(&rep, Some(&path("a.txt")), 1).await?;
        assert_eq!(after_a.len(), 1);
        assert_eq!(after_a[0].0, path("notes"));
        let after_notes = store.scan_page(&rep, Some(&path("notes")), 10).await?;
        assert_eq!(
            after_notes
                .iter()
                .map(|(path, _)| path.to_string())
                .collect::<Vec<_>>(),
            vec!["notes/plan.md", "z.txt"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn applying_is_idempotent_and_bumps_the_generation() -> Result<()> {
        let store = store().await;
        let rep = BackendId::new("fs");
        let first = store.generation(&rep).await?.expect("the rep was written");
        let before = store.entry(&rep, &path("a.txt")).await?;

        assert_eq!(
            store.apply(&rep, &[]).await?,
            first,
            "an empty batch is a no-op"
        );
        assert_eq!(store.apply(&rep, &[added("a.txt", 3)]).await?, first + 1);
        assert_eq!(store.entry(&rep, &path("a.txt")).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn removals_and_changes_land_in_one_update() -> Result<()> {
        let store = store().await;
        let rep = BackendId::new("fs");
        store
            .apply(
                &rep,
                &[
                    Delta::Removed {
                        path: path("a.txt"),
                        entry: file(3),
                    },
                    Delta::Changed {
                        path: path("z.txt"),
                        from: file(2),
                        to: file(9),
                    },
                    Delta::Touched {
                        path: path("notes/plan.md"),
                        entry: file(1),
                    },
                ],
            )
            .await?;

        assert_eq!(store.entry(&rep, &path("a.txt")).await?, None);
        assert_eq!(
            store.entry(&rep, &path("z.txt")).await?,
            Some(file(9)),
            "a change upserts"
        );
        assert_eq!(
            store.entry(&rep, &path("notes/plan.md")).await?,
            Some(file(1))
        );
        Ok(())
    }

    #[tokio::test]
    async fn reps_are_listed_and_dropped() -> Result<()> {
        let store = store().await;
        store
            .apply(&BackendId::new("doc"), &[added("a.txt", 3)])
            .await?;
        assert_eq!(
            store.reps().await?,
            vec![BackendId::new("doc"), BackendId::new("fs")]
        );
        assert!(store.drop_rep(&BackendId::new("doc")).await?);
        assert!(!store.drop_rep(&BackendId::new("doc")).await?);
        assert_eq!(store.generation(&BackendId::new("doc")).await?, None);
        assert_eq!(
            store.entry(&BackendId::new("doc"), &path("a.txt")).await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_rep_reads_as_empty() -> Result<()> {
        let store = MemVtreeStore::new();
        let rep = BackendId::new("nobody");
        assert_eq!(store.scan_page(&rep, None, 10).await?, Vec::new());
        assert_eq!(store.generation(&rep).await?, None);
        assert_eq!(store.entry(&rep, &path("a.txt")).await?, None);
        Ok(())
    }
}
