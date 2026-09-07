//! Durable state primitives for embedder-driven delta walkers.
//!
//! The repository stores consumer progress for one durable delta walker. It
//! does not read a source, schedule work, or own an event loop. Consumers that
//! need sparse per-key memory opt into the separate sparse-state capability.

use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaWalkerProgress {
    pub upstream_revision: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DeltaWalkerStateError {
    #[error("delta walker revision {next} did not advance beyond {current}")]
    NonAdvancingRevision { current: u64, next: u64 },
    #[error("delta walker progress changed while advancing from {expected}")]
    StaleProgress { expected: u64 },
    #[error("delta walker state backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

pub type DeltaWalkerStateResult<T> = Result<T, DeltaWalkerStateError>;

/// Transaction over one stable consumer's durable walker state.
#[async_trait]
pub trait DeltaWalkerStateTransaction: Send {
    type Context: Send;

    fn context_mut(&mut self) -> &mut Self::Context;
    async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress>;
    async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()>;
    async fn commit(self) -> DeltaWalkerStateResult<()>;
    async fn rollback(self) -> DeltaWalkerStateResult<()>;
}

/// Passive durable state for one stable consumer in one namespace.
#[async_trait]
pub trait DeltaWalkerStateRepo: Send + Sync {
    type Context<'a>: Send
    where
        Self: 'a;
    type Transaction<'a>: DeltaWalkerStateTransaction<Context = Self::Context<'a>> + 'a
    where
        Self: 'a;

    async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress>;
    async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> DeltaWalkerStateResult<Self::Transaction<'a>>;
}

/// Portable contract suite for delta-walker state backends.
///
/// The walker tests exercise their own in-memory doubles; this suite pins the
/// semantics those doubles are supposed to mirror, so a real backend (e.g.
/// the SQLite one) is verified against the same contract instead of being
/// trusted to match by inspection. Backends implement
/// [`DeltaWalkerStateContractHarness`] once and run
/// [`assert_delta_walker_state_contract`] against their own storage.
#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;
    use crate::delta_walker_sparse_state::{
        DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
    };

    /// Supplies a repo and key/value constructors to the reusable
    /// DeltaWalkerState contract suite.
    pub trait DeltaWalkerStateContractHarness: Sync {
        type Repo: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + Sync;

        fn repo(&self) -> &Self::Repo;

        fn key(&self, index: u64) -> Vec<u8>;

        fn value(&self, index: u64) -> Vec<u8>;
    }

    /// Runs the portable cursor-advance, rollback and sparse-memory contract.
    pub async fn assert_delta_walker_state_contract<H>(harness: &H)
    where
        H: DeltaWalkerStateContractHarness,
        for<'a> <H::Repo as DeltaWalkerStateRepo>::Transaction<'a>:
            DeltaWalkerSparseStateTransaction,
    {
        let repo = harness.repo();
        let key1 = harness.key(1);
        let key2 = harness.key(2);
        let key3 = harness.key(3);
        let key4 = harness.key(4);
        let key5 = harness.key(5);
        let value10 = harness.value(10);
        let value20 = harness.value(20);
        let value30 = harness.value(30);
        let value31 = harness.value(31);
        let value40 = harness.value(40);

        // Fresh repo starts at revision zero with no sparse state.
        assert_eq!(
            repo.progress()
                .await
                .expect("initial progress")
                .upstream_revision,
            0
        );
        assert_eq!(repo.get(&key1).await.expect("initial sparse get"), None);
        assert_eq!(
            repo.get_many(&[]).await.expect("empty get_many"),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );

        // A committed advance is durable and visible outside the transaction.
        let mut tx = repo.begin().await.expect("begin advance transaction");
        tx.advance_from(0, 1).await.expect("advance 0 -> 1");
        tx.commit().await.expect("commit advance");
        assert_eq!(
            repo.progress()
                .await
                .expect("progress after commit")
                .upstream_revision,
            1
        );

        // Non-advancing revisions are rejected.
        let mut tx = repo.begin().await.expect("begin non-advancing transaction");
        let error = tx
            .advance_from(1, 1)
            .await
            .expect_err("equal revision must be rejected");
        assert!(matches!(
            error,
            DeltaWalkerStateError::NonAdvancingRevision {
                current: 1,
                next: 1
            }
        ));
        tx.rollback()
            .await
            .expect("rollback non-advancing transaction");

        // Stale expected revisions are rejected.
        let mut tx = repo.begin().await.expect("begin stale transaction");
        let error = tx
            .advance_from(0, 2)
            .await
            .expect_err("stale expected revision must be rejected");
        assert!(matches!(
            error,
            DeltaWalkerStateError::StaleProgress { expected: 0 }
        ));
        tx.rollback().await.expect("rollback stale transaction");

        // Rollback discards a staged advance.
        let mut tx = repo
            .begin()
            .await
            .expect("begin rollback-advance transaction");
        tx.advance_from(1, 2).await.expect("stage advance 1 -> 2");
        tx.rollback().await.expect("rollback staged advance");
        assert_eq!(
            repo.progress()
                .await
                .expect("progress after rollback")
                .upstream_revision,
            1
        );

        // Sparse state is read-your-writes inside the transaction.
        let mut tx = repo.begin().await.expect("begin sparse ryw transaction");
        assert_eq!(tx.get(&key1).await.expect("sparse get before put"), None);
        tx.put(key1.clone(), value10.clone())
            .await
            .expect("stage sparse put");
        assert_eq!(
            tx.get(&key1).await.expect("sparse read your write"),
            Some(value10.clone())
        );
        tx.delete(&key1).await.expect("stage sparse delete");
        assert_eq!(
            tx.get(&key1).await.expect("sparse read your tombstone"),
            None
        );
        tx.rollback()
            .await
            .expect("rollback sparse ryw transaction");

        // Sparse writes and the cursor advance commit atomically.
        let mut tx = repo.begin().await.expect("begin atomic commit transaction");
        tx.put(key1.clone(), value10.clone())
            .await
            .expect("stage sparse put");
        tx.advance_from(1, 2).await.expect("stage advance 1 -> 2");
        tx.commit().await.expect("atomic commit");
        assert_eq!(
            repo.progress()
                .await
                .expect("progress after atomic commit")
                .upstream_revision,
            2
        );
        assert_eq!(
            repo.get(&key1)
                .await
                .expect("sparse state after atomic commit"),
            Some(value10)
        );

        // Rollback discards sparse writes AND the staged advance together.
        let mut tx = repo
            .begin()
            .await
            .expect("begin atomic rollback transaction");
        tx.put(key2.clone(), value20.clone())
            .await
            .expect("stage sparse put");
        tx.advance_from(2, 3).await.expect("stage advance 2 -> 3");
        tx.rollback().await.expect("atomic rollback");
        assert_eq!(
            repo.progress()
                .await
                .expect("progress after atomic rollback")
                .upstream_revision,
            2
        );
        assert_eq!(
            repo.get(&key2)
                .await
                .expect("sparse state after atomic rollback"),
            None
        );

        // get_many returns only present keys, in input order.
        let mut tx = repo.begin().await.expect("begin get_many transaction");
        tx.put(key3.clone(), value30.clone())
            .await
            .expect("stage key3");
        tx.put(key4.clone(), value40.clone())
            .await
            .expect("stage key4");
        tx.commit().await.expect("commit get_many transaction");
        let many = repo
            .get_many(&[key3.clone(), key5.clone()])
            .await
            .expect("get_many with one absent key");
        assert_eq!(many, vec![(key3.clone(), value30.clone())]);

        // An in-transaction overwrite replaces the committed value.
        let mut tx = repo.begin().await.expect("begin overwrite transaction");
        tx.put(key3.clone(), value31.clone())
            .await
            .expect("stage overwrite");
        tx.commit().await.expect("commit overwrite");
        assert_eq!(
            repo.get(&key3).await.expect("overwritten sparse value"),
            Some(value31)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::contract::{DeltaWalkerStateContractHarness, assert_delta_walker_state_contract};
    use crate::delta_walker_sparse_state::{
        DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
    };
    use crate::delta_walker_state::{
        DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateResult,
        DeltaWalkerStateTransaction,
    };
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    /// Faithful in-memory double: staged sparse writes and a staged cursor
    /// advance, both discarded by rollback — mirroring the SQLite backend's
    /// single-transaction settlement semantics (sparse memory and upstream
    /// cursor commit or roll back together).
    #[derive(Default)]
    struct MemoryState {
        progress: u64,
        keys: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    struct MemoryRepo {
        inner: Mutex<MemoryState>,
    }

    impl Default for MemoryRepo {
        fn default() -> Self {
            Self {
                inner: Mutex::new(MemoryState::default()),
            }
        }
    }

    struct MemoryTx<'a> {
        inner: &'a Mutex<MemoryState>,
        ctx: (),
        staged_progress: Option<u64>,
        /// `None` value = staged delete.
        staged_keys: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl<'a> DeltaWalkerStateTransaction for MemoryTx<'a> {
        type Context = ();

        fn context_mut(&mut self) -> &mut Self::Context {
            &mut self.ctx
        }

        async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            let state = self.inner.lock();
            Ok(DeltaWalkerProgress {
                upstream_revision: state.progress,
            })
        }

        async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()> {
            let state = self.inner.lock();
            if state.progress != expected {
                return Err(DeltaWalkerStateError::StaleProgress { expected });
            }
            if next <= state.progress {
                return Err(DeltaWalkerStateError::NonAdvancingRevision {
                    current: state.progress,
                    next,
                });
            }
            self.staged_progress = Some(next);
            Ok(())
        }

        async fn commit(self) -> DeltaWalkerStateResult<()> {
            let mut state = self.inner.lock();
            if let Some(next) = self.staged_progress {
                state.progress = next;
            }
            for (key, value) in self.staged_keys {
                match value {
                    Some(value) => {
                        state.keys.insert(key, value);
                    }
                    None => {
                        state.keys.remove(&key);
                    }
                }
            }
            Ok(())
        }

        async fn rollback(self) -> DeltaWalkerStateResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerSparseStateTransaction for MemoryTx<'_> {
        async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            if let Some(staged) = self.staged_keys.get(key) {
                return Ok(staged.clone());
            }
            Ok(self.inner.lock().keys.get(key).cloned())
        }

        async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
            self.staged_keys.insert(key, Some(value));
            Ok(())
        }

        async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()> {
            self.staged_keys.insert(key.to_vec(), None);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateRepo for MemoryRepo {
        type Context<'a>
            = ()
        where
            Self: 'a;
        type Transaction<'a>
            = MemoryTx<'a>
        where
            Self: 'a;

        async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: self.inner.lock().progress,
            })
        }

        async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemoryTx {
                inner: &self.inner,
                ctx: (),
                staged_progress: None,
                staged_keys: BTreeMap::new(),
            })
        }

        async fn begin_with_context<'a>(
            &'a self,
            _context: Self::Context<'a>,
        ) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            self.begin().await
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerSparseStateRepo for MemoryRepo {
        async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self.inner.lock().keys.get(key).cloned())
        }

        async fn get_many(
            &self,
            keys: &[Vec<u8>],
        ) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let state = self.inner.lock();
            Ok(keys
                .iter()
                .filter_map(|key| {
                    state
                        .keys
                        .get(key)
                        .map(|value| (key.clone(), value.clone()))
                })
                .collect())
        }
    }

    struct MemoryContractHarness {
        repo: MemoryRepo,
    }

    impl DeltaWalkerStateContractHarness for MemoryContractHarness {
        type Repo = MemoryRepo;

        fn repo(&self) -> &Self::Repo {
            &self.repo
        }

        fn key(&self, index: u64) -> Vec<u8> {
            format!("branch-{index}").into_bytes()
        }

        fn value(&self, index: u64) -> Vec<u8> {
            format!("heads-{index}").into_bytes()
        }
    }

    #[test]
    fn memory_delta_walker_state_contract() {
        futures::executor::block_on(async {
            assert_delta_walker_state_contract(&MemoryContractHarness {
                repo: MemoryRepo::default(),
            })
            .await;
        });
    }
}
