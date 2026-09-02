//! Generic keyed-frontier implementations.

mod memory;
mod sqlite_generic;
mod sqlite_read;

pub use memory::{
    MemoryKeySelector, MemoryKeyedFrontier, MemoryKeyedFrontierSelector, MemoryKeyedFrontierSource,
    MemoryKeyedFrontierTable, MemoryKeyedFrontierTxn, MemoryKeyedFrontierView,
    open_memory_keyed_frontier,
};
pub use sqlite_generic::{
    SqliteFrontierCodec, SqliteFrontierSelector, SqliteKeyedFrontier,
    SqliteKeyedFrontierTransaction,
};
pub(crate) use sqlite_read::{SqliteReadError, SqliteReadSource, open_sqlite_reader};

#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use big_sync_core::keyed_frontier::{
        FrontierEntry, FrontierMutation, FrontierRead, FrontierReadLimits, FrontierRevision,
        KeyedFrontier, KeyedFrontierError, KeyedFrontierReader, KeyedFrontierTransaction,
        TransactionIsolation,
    };
    use std::collections::BTreeMap;
    /// Supplies a frontier and selector constructors to the reusable
    /// KeyedFrontier contract suite. Backends can implement this once and run
    /// [`assert_keyed_frontier_contract`] against their own storage.
    pub trait KeyedFrontierContractHarness: Sync {
        type Key: Ord + Clone + std::fmt::Debug + Send + Sync + 'static;
        type Value: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static;
        type Frontier: KeyedFrontier<Self::Key, Self::Value> + Sync;

        fn frontier(&self) -> &Self::Frontier;

        fn key(&self, index: u64) -> Self::Key;

        fn value(&self, index: u64) -> Self::Value;

        fn values_match(&self, expected: &Self::Value, actual: &Self::Value) -> bool {
            expected == actual
        }

        fn all_selector(
            &self,
            after: FrontierRevision,
        ) -> <Self::Frontier as KeyedFrontier<Self::Key, Self::Value>>::Selector;

        fn keys_selector(
            &self,
            bounds: BTreeMap<Self::Key, FrontierRevision>,
        ) -> <Self::Frontier as KeyedFrontier<Self::Key, Self::Value>>::Selector;
    }

    async fn replay<R, K, V>(reader: &mut R) -> Vec<FrontierEntry<K, V>>
    where
        R: KeyedFrontierReader<K, V> + ?Sized,
        K: Send + Sync,
        V: Send + Sync,
    {
        let mut entries = Vec::new();
        loop {
            match reader.next().await.expect("frontier read") {
                FrontierRead::Entries {
                    entries: page,
                    through: _,
                } => entries.extend(page),
                FrontierRead::ReplayComplete { .. } => return entries,
            }
        }
    }

    async fn commit<K, V>(
        frontier: &impl KeyedFrontier<K, V>,
        mutations: impl IntoIterator<Item = FrontierMutation<K, V>>,
    ) -> FrontierRevision
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        let mut transaction = frontier.begin().await.expect("begin frontier transaction");
        let expected_revision = transaction
            .revision()
            .await
            .expect("reserve frontier revision");
        assert_eq!(
            transaction
                .revision()
                .await
                .expect("read reserved frontier revision"),
            expected_revision
        );
        for mutation in mutations {
            match mutation {
                FrontierMutation::Put { key, value } => transaction
                    .put(key, value)
                    .await
                    .expect("stage frontier put"),
                FrontierMutation::Delete { key } => transaction
                    .delete(key)
                    .await
                    .expect("stage frontier delete"),
            }
        }
        let revision = transaction
            .commit()
            .await
            .expect("commit frontier transaction");
        assert_eq!(revision, expected_revision);
        revision
    }

    fn assert_entry<H>(
        harness: &H,
        entry: &FrontierEntry<H::Key, H::Value>,
        key: &H::Key,
        revision: FrontierRevision,
        value: Option<&H::Value>,
    ) where
        H: KeyedFrontierContractHarness,
    {
        assert_eq!(&entry.key, key);
        assert_eq!(entry.revision, revision);
        match (value, entry.value.as_ref()) {
            (Some(expected), Some(actual)) => assert!(harness.values_match(expected, actual)),
            (None, None) => {}
            (Some(_), None) => panic!("expected a frontier value"),
            (None, Some(_)) => panic!("expected a frontier tombstone"),
        }
    }

    /// Runs the portable transaction, replay and live-handoff guarantees.
    pub async fn assert_keyed_frontier_contract<H>(harness: &H)
    where
        H: KeyedFrontierContractHarness,
    {
        let frontier = harness.frontier();
        let key1 = harness.key(1);
        let key2 = harness.key(2);
        let key3 = harness.key(3);
        let key4 = harness.key(4);
        let key5 = harness.key(5);
        let key6 = harness.key(6);
        let key7 = harness.key(7);
        let key8 = harness.key(8);
        let key9 = harness.key(9);
        let value10 = harness.value(10);
        let value20 = harness.value(20);
        let value30 = harness.value(30);
        let value40 = harness.value(40);
        let value50 = harness.value(50);
        let value60 = harness.value(60);
        let value61 = harness.value(61);
        let value70 = harness.value(70);
        let value80 = harness.value(80);
        let value90 = harness.value(90);
        let mut transaction = frontier.begin().await.expect("begin transaction");
        assert!(matches!(
            transaction.isolation(),
            TransactionIsolation::ReadCommitted | TransactionIsolation::Serializable
        ));
        let rolled_back_revision = transaction
            .revision()
            .await
            .expect("reserve rollback revision");
        assert_eq!(
            transaction
                .revision()
                .await
                .expect("read rollback revision"),
            rolled_back_revision
        );
        transaction
            .put(key1.clone(), value10.clone())
            .await
            .expect("stage put");
        assert_eq!(
            transaction.get(&key1).await.expect("read your write"),
            Some(value10.clone())
        );
        transaction
            .delete(key1.clone())
            .await
            .expect("stage delete");
        assert_eq!(
            transaction.get(&key1).await.expect("read your tombstone"),
            None
        );
        transaction.rollback().await.expect("rollback transaction");

        let mut transaction = frontier.begin().await.expect("begin after rollback");
        assert_eq!(
            transaction
                .get(&key1)
                .await
                .expect("read rolled back value"),
            None
        );
        assert_eq!(transaction.commit().await.expect("empty commit"), 0);

        assert!(matches!(
            frontier
                .open(
                    harness.all_selector(0),
                    FrontierReadLimits { max_entries: 0 }
                )
                .await,
            Err(KeyedFrontierError::EmptyReadLimit)
        ));

        let revision = commit(
            frontier,
            [
                FrontierMutation::Put {
                    key: key1.clone(),
                    value: value10.clone(),
                },
                FrontierMutation::Put {
                    key: key2.clone(),
                    value: value20.clone(),
                },
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: value30.clone(),
                },
            ],
        )
        .await;
        assert_eq!(revision, 1);

        let mut precommit_reader = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open pre-commit reader");
        let mut uncommitted = frontier
            .begin()
            .await
            .expect("begin uncommitted transaction");
        uncommitted
            .put(key9.clone(), value90.clone())
            .await
            .expect("stage uncommitted put");
        assert!(
            replay(&mut *precommit_reader)
                .await
                .iter()
                .all(|entry| entry.key != key9)
        );
        uncommitted
            .rollback()
            .await
            .expect("rollback uncommitted put");

        let mut reader = frontier
            .open(
                harness.all_selector(0),
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .expect("open bounded replay");
        let mut pages = Vec::new();
        let mut replay_markers = 0;
        loop {
            match reader.next().await.expect("bounded replay read") {
                FrontierRead::Entries { entries, through } => pages.push((entries, through)),
                FrontierRead::ReplayComplete { through } => {
                    assert_eq!(through, revision);
                    replay_markers += 1;
                    break;
                }
            }
        }
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].0.len(),
            3,
            "an atomic revision may exceed the soft limit"
        );
        assert_eq!(pages[0].1, revision);
        assert!(pages[0].0.iter().all(|entry| entry.revision == revision));
        assert_eq!(replay_markers, 1);

        let following_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key4.clone(),
                value: value40.clone(),
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } =
            reader.next().await.expect("following entry after replay")
        else {
            panic!("expected following entry")
        };
        assert_eq!(through, following_revision);
        assert_eq!(entries.len(), 1);
        assert_entry(
            harness,
            &entries[0],
            &key4,
            following_revision,
            Some(&value40),
        );

        let selector_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key5.clone(),
                value: value50.clone(),
            }],
        )
        .await;
        let mut selected = frontier
            .open(
                harness.keys_selector(BTreeMap::from([
                    (key4.clone(), 0),
                    (key5.clone(), selector_revision),
                ])),
                FrontierReadLimits::default(),
            )
            .await
            .expect("open selected replay");
        let selected_entries = replay(&mut *selected).await;
        assert_eq!(selected_entries.len(), 1);
        assert_entry(
            harness,
            &selected_entries[0],
            &key4,
            following_revision,
            Some(&value40),
        );

        let overwrite_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key1.clone(),
                value: harness.value(11),
            }],
        )
        .await;
        let mut collapsed = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open collapsed replay");
        let collapsed_entries = replay(&mut *collapsed).await;
        let collapsed_key1 = collapsed_entries
            .iter()
            .filter(|entry| entry.key == key1)
            .collect::<Vec<_>>();
        assert_eq!(collapsed_key1.len(), 1);
        let value11 = harness.value(11);
        assert_entry(
            harness,
            collapsed_key1[0],
            &key1,
            overwrite_revision,
            Some(&value11),
        );

        let repeated_revision = commit(
            frontier,
            [
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: harness.value(31),
                },
                FrontierMutation::Delete { key: key3.clone() },
                FrontierMutation::Put {
                    key: key3.clone(),
                    value: harness.value(32),
                },
            ],
        )
        .await;
        let mut repeated = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open repeated-write replay");
        let repeated_entries = replay(&mut *repeated)
            .await
            .into_iter()
            .filter(|entry| entry.key == key3)
            .collect::<Vec<_>>();
        assert_eq!(repeated_entries.len(), 1);
        let value32 = harness.value(32);
        assert_entry(
            harness,
            &repeated_entries[0],
            &key3,
            repeated_revision,
            Some(&value32),
        );

        let tombstone_revision =
            commit(frontier, [FrontierMutation::Delete { key: key2.clone() }]).await;
        let mut tombstones = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open tombstone replay");
        assert_eq!(
            replay(&mut *tombstones)
                .await
                .into_iter()
                .find(|entry| entry.key == key2),
            Some(FrontierEntry {
                key: key2.clone(),
                revision: tombstone_revision,
                value: None,
            })
        );

        let boundary_value_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key6.clone(),
                value: value60.clone(),
            }],
        )
        .await;
        let mut boundary = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open boundary replay");
        let overwritten_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key6.clone(),
                value: value61.clone(),
            }],
        )
        .await;
        let replayed = replay(&mut *boundary).await;
        assert!(
            replayed
                .iter()
                .all(|entry| entry.revision <= boundary_value_revision)
        );
        let FrontierRead::Entries { entries, through } =
            boundary.next().await.expect("following overwritten value")
        else {
            panic!("expected following overwritten value")
        };
        assert_eq!(through, overwritten_revision);
        assert_eq!(entries.len(), 1);
        assert_entry(
            harness,
            &entries[0],
            &key6,
            overwritten_revision,
            Some(&value61),
        );

        let mut idle = frontier
            .open(harness.all_selector(0), FrontierReadLimits::default())
            .await
            .expect("open idle reader");
        replay(&mut *idle).await;
        let (read, committed) = tokio::join!(
            idle.next(),
            commit(
                frontier,
                [FrontierMutation::Put {
                    key: key7,
                    value: value70,
                }],
            )
        );
        let idle_revision = committed;
        let FrontierRead::Entries { entries, through } = read.expect("woken reader") else {
            panic!("expected woken entry")
        };
        assert_eq!(through, idle_revision);
        let key7 = harness.key(7);
        let value70 = harness.value(70);
        assert_entry(harness, &entries[0], &key7, idle_revision, Some(&value70));

        let handoff_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key8,
                value: value80,
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } = idle
            .next()
            .await
            .expect("entry after replay-to-live handoff")
        else {
            panic!("expected handoff entry")
        };
        assert_eq!(through, handoff_revision);
        let key8 = harness.key(8);
        let value80 = harness.value(80);
        assert_entry(
            harness,
            &entries[0],
            &key8,
            handoff_revision,
            Some(&value80),
        );

        let mut selected_live = frontier
            .open(
                harness.keys_selector(BTreeMap::from([(key1.clone(), 0)])),
                FrontierReadLimits::default(),
            )
            .await
            .expect("open selected live reader");
        replay(&mut *selected_live).await;
        let _nonmatching_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key9,
                value: value90,
            }],
        )
        .await;
        let matching_revision = commit(
            frontier,
            [FrontierMutation::Put {
                key: key1.clone(),
                value: harness.value(12),
            }],
        )
        .await;
        let FrontierRead::Entries { entries, through } = selected_live
            .next()
            .await
            .expect("selected reader crossed nonmatching revision")
        else {
            panic!("expected selected live entry")
        };
        assert_eq!(through, matching_revision);
        let value12 = harness.value(12);
        assert_entry(
            harness,
            &entries[0],
            &key1,
            matching_revision,
            Some(&value12),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::keyed_frontier::{
        FrontierRead, FrontierReadLimits, FrontierRevision, KeyedFrontier, KeyedFrontierError,
        KeyedFrontierReader, KeyedFrontierResult, KeyedFrontierTransaction, TransactionIsolation,
    };
    use std::collections::BTreeMap;
    use utils_rs::prelude::async_trait;
    struct MemoryContractHarness {
        frontier: MemoryKeyedFrontier<u64, u64>,
    }

    impl contract::KeyedFrontierContractHarness for MemoryContractHarness {
        type Key = u64;
        type Value = u64;
        type Frontier = MemoryKeyedFrontier<u64, u64>;

        fn frontier(&self) -> &Self::Frontier {
            &self.frontier
        }

        fn key(&self, index: u64) -> u64 {
            index
        }

        fn value(&self, index: u64) -> u64 {
            index
        }

        fn all_selector(&self, after: FrontierRevision) -> MemoryKeySelector<u64> {
            MemoryKeySelector::All { after }
        }

        fn keys_selector(&self, bounds: BTreeMap<u64, FrontierRevision>) -> MemoryKeySelector<u64> {
            MemoryKeySelector::Keys(bounds)
        }
    }

    #[tokio::test]
    async fn memory_keyed_frontier_contract() {
        contract::assert_keyed_frontier_contract(&MemoryContractHarness {
            frontier: MemoryKeyedFrontier::default(),
        })
        .await;
    }

    struct SqliteContextProbe;

    struct SqliteContextProbeTransaction<'a> {
        transaction: Option<sqlx::Transaction<'a, sqlx::Sqlite>>,
    }

    #[async_trait]
    impl<'a> KeyedFrontierTransaction<u64, u64> for SqliteContextProbeTransaction<'a> {
        type Context = sqlx::Transaction<'a, sqlx::Sqlite>;

        fn isolation(&self) -> TransactionIsolation {
            TransactionIsolation::Serializable
        }

        fn context_mut(&mut self) -> &mut Self::Context {
            self.transaction
                .as_mut()
                .expect("probe transaction present")
        }

        async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
            Ok(0)
        }

        async fn get(&mut self, _key: &u64) -> KeyedFrontierResult<Option<u64>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe does not implement frontier operations",
            ))))
        }

        async fn put(&mut self, _key: u64, _value: u64) -> KeyedFrontierResult<()> {
            self.get(&_key).await.map(|_| ())
        }

        async fn delete(&mut self, _key: u64) -> KeyedFrontierResult<()> {
            self.get(&_key).await.map(|_| ())
        }

        async fn commit(mut self) -> KeyedFrontierResult<FrontierRevision> {
            self.transaction
                .take()
                .expect("probe transaction present")
                .commit()
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
            Ok(0)
        }

        async fn rollback(mut self) -> KeyedFrontierResult<()> {
            self.transaction
                .take()
                .expect("probe transaction present")
                .rollback()
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
        }
    }

    #[async_trait]
    impl KeyedFrontier<u64, u64> for SqliteContextProbe {
        type Selector = MemoryKeySelector<u64>;
        type Context<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
        type Transaction<'a> = SqliteContextProbeTransaction<'a>;

        async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe requires caller-owned transaction",
            ))))
        }

        async fn begin_with_context<'a>(
            &'a self,
            context: Self::Context<'a>,
        ) -> KeyedFrontierResult<Self::Transaction<'a>> {
            Ok(SqliteContextProbeTransaction {
                transaction: Some(context),
            })
        }

        async fn open(
            &self,
            _selector: Self::Selector,
            _limits: FrontierReadLimits,
        ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<u64, u64> + '_>> {
            Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "probe does not implement reads",
            ))))
        }
    }

    #[tokio::test]
    async fn gat_context_round_trips_a_real_sqlite_transaction() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("create sqlite pool");
        let transaction = pool.begin().await.expect("begin sqlite transaction");
        let probe = SqliteContextProbe;
        let mut transaction = probe
            .begin_with_context(transaction)
            .await
            .expect("adapt caller-owned sqlite transaction");
        sqlx::query("CREATE TABLE context_probe (value INTEGER NOT NULL)")
            .execute(&mut **transaction.context_mut())
            .await
            .expect("use caller-owned sqlite transaction");
        transaction
            .rollback()
            .await
            .expect("rollback sqlite transaction");
        let table_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'context_probe'",
        )
        .fetch_one(&pool)
        .await
        .expect("verify rollback through pool");
        assert_eq!(table_count, 0);
    }

    #[tokio::test]
    async fn bounded_replay_never_splits_an_atomic_revision_and_then_follows() {
        let frontier = MemoryKeyedFrontier::<u64, &'static str>::default();
        let mut tx = frontier.begin().await.unwrap();
        tx.put(1, "one").await.unwrap();
        tx.put(2, "two").await.unwrap();
        tx.put(3, "three").await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 1);

        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .unwrap();
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected entries");
        };
        assert_eq!(entries.len(), 3);
        assert_eq!(through, 1);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 1 }
        );

        let mut tx = frontier.begin().await.unwrap();
        tx.delete(2).await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 2);
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected following entries");
        };
        assert_eq!(through, 2);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value, None);
    }

    #[tokio::test]
    async fn replay_pages_are_bounded_across_revisions() {
        let frontier = MemoryKeyedFrontier::<u64, u64>::default();
        for key in 1..=3 {
            let mut tx = frontier.begin().await.unwrap();
            tx.put(key, key).await.unwrap();
            let _revision = tx.commit().await.unwrap();
        }
        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 2 },
            )
            .await
            .unwrap();
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected first page");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(through, 2);
        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected second page");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(through, 3);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 3 }
        );
    }

    #[tokio::test]
    async fn commits_during_replay_follow_the_captured_boundary() {
        let frontier = MemoryKeyedFrontier::<u64, u64>::default();
        for key in 1..=2 {
            let mut tx = frontier.begin().await.unwrap();
            tx.put(key, key).await.unwrap();
            let _revision = tx.commit().await.unwrap();
        }
        let mut reader = frontier
            .open(
                MemoryKeySelector::All { after: 0 },
                FrontierReadLimits { max_entries: 1 },
            )
            .await
            .unwrap();

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected first replay page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(through, 1);

        let mut tx = frontier.begin().await.unwrap();
        tx.put(3, 3).await.unwrap();
        assert_eq!(tx.commit().await.unwrap(), 3);

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected second replay page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [2]
        );
        assert_eq!(through, 2);
        assert_eq!(
            reader.next().await.unwrap(),
            FrontierRead::ReplayComplete { through: 2 }
        );

        let FrontierRead::Entries { entries, through } = reader.next().await.unwrap() else {
            panic!("expected following page");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [3]
        );
        assert_eq!(through, 3);
    }
}
