use super::*;
use crate::StepKind;
use crate::kvstore::atomic::MemoryStore;
use crate::kvstore::atomic::MemoryWriter;
use crate::partition::RetryBudget;
use crate::partition::RetryPolicy;
use crate::partition::authority::Authority;
use crate::partition::authority::Command;
use crate::partition::authority::Decision;
use crate::partition::authority::Disposition;
use crate::partition::authority::Intent;
use crate::partition::authority::Transition;
use futures::FutureExt;
use futures::channel::oneshot;

const LIMIT: NonZeroUsize = NonZeroUsize::new(2).unwrap();

enum Fault {
    Before(oneshot::Receiver<()>),
    After(oneshot::Receiver<()>),
    Reject,
    Unknown,
}

struct FaultWriter {
    inner: MemoryWriter,
    fault: Option<Fault>,
    commits: usize,
    mutation_counts: Vec<usize>,
}

#[async_trait]
impl Writer for FaultWriter {
    fn reader(&self) -> Arc<dyn Store> {
        self.inner.reader()
    }

    async fn commit(
        &mut self,
        revision: u64,
        mutations: Vec<Mutation>,
    ) -> Result<u64, CommitError> {
        self.commits += 1;
        self.mutation_counts.push(mutations.len());
        let after = match self.fault.take() {
            Some(Fault::Reject) => {
                return Err(CommitError::NotCommitted(ferr!("injected rollback")));
            }
            Some(Fault::Before(gate)) => {
                gate.await.unwrap();
                None
            }
            fault => fault,
        };
        let revision = self.inner.commit(revision, mutations).await?;
        match after {
            Some(Fault::After(gate)) => {
                gate.await.unwrap();
            }
            Some(Fault::Unknown) => return Err(CommitError::Unknown(ferr!("injected lost reply"))),
            None => {}
            _ => unreachable!(),
        }
        Ok(revision)
    }
}

async fn journal(store: &MemoryStore) -> Res<Journal<FaultWriter>> {
    Journal::open(
        FaultWriter {
            inner: store.writer()?,
            fault: None,
            commits: 0,
            mutation_counts: Vec::new(),
        },
        42,
    )
    .await
}

fn receipt() -> Vec<Mutation> {
    vec![Mutation {
        key: b"receipt".as_slice().into(),
        value: Some(b"accepted".as_slice().into()),
    }]
}

#[test]
fn cancellation_before_commit_leaves_no_hole_or_notification() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        let reader = log.reader();
        let mut tail = reader.tail(1, LIMIT);
        assert!(tail.next().now_or_never().is_none());
        let (release, gate) = oneshot::channel();
        log.writer.fault = Some(Fault::Before(gate));
        let mut pending = Box::pin(log.append(0, b"not committed".as_slice().into(), receipt()));
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(reader.read(1, LIMIT).await?.frontier, 0);
        assert!(reader.index(b"receipt").await?.is_none());
        assert!(tail.next().now_or_never().is_none());
        drop(pending);
        assert!(release.send(()).is_err());
        assert!(
            log.append(0, b"blocked".as_slice().into(), vec![])
                .await
                .is_err()
        );
        drop(log);
        let mut reopened = journal(&store).await?;
        assert_eq!(reopened.authority_epoch(), 2);
        assert_eq!(
            reopened
                .append(0, b"first".as_slice().into(), receipt())
                .await?,
            1
        );
        let entry = tail.next().now_or_never().unwrap().unwrap()?;
        assert_eq!(entry.position, 1);
        assert_eq!(entry.value.as_ref(), b"first");
        Ok(())
    })
}

#[test]
fn cancellation_after_commit_preserves_entry_frontier_receipt_and_wakeup() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        let reader = log.reader();
        let mut tail = reader.tail(1, LIMIT);
        assert!(tail.next().now_or_never().is_none());
        let (release, gate) = oneshot::channel();
        log.writer.fault = Some(Fault::After(gate));
        let mut pending = Box::pin(log.append(0, b"committed".as_slice().into(), receipt()));
        assert!(futures::poll!(&mut pending).is_pending());
        assert_eq!(reader.read(1, LIMIT).await?.frontier, 1);
        assert_eq!(
            reader.index(b"receipt").await?.unwrap().as_ref(),
            b"accepted"
        );
        let entry = tail.next().now_or_never().unwrap().unwrap()?;
        assert_eq!(entry.value.as_ref(), b"committed");
        drop(pending);
        assert!(release.send(()).is_err());
        assert_eq!(log.frontier(), 0);
        assert!(
            log.append(0, b"blocked".as_slice().into(), vec![])
                .await
                .is_err()
        );
        drop(log);
        let mut reopened = journal(&store).await?;
        assert_eq!(reopened.frontier(), 1);
        assert_eq!(
            reopened
                .append(1, b"second".as_slice().into(), vec![])
                .await?,
            2
        );
        assert_eq!(tail.next().now_or_never().unwrap().unwrap()?.position, 2);
        Ok(())
    })
}

#[test]
fn definite_failure_can_retry_but_unknown_outcome_requires_reopen() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        log.writer.fault = Some(Fault::Reject);
        assert!(matches!(
            log.append(0, b"step".as_slice().into(), receipt()).await,
            Err(CommitError::NotCommitted(_))
        ));
        assert_eq!(log.reader().read(1, LIMIT).await?.frontier, 0);
        assert!(log.reader().index(b"receipt").await?.is_none());
        log.writer.fault = Some(Fault::Unknown);
        assert!(matches!(
            log.append(0, b"step".as_slice().into(), receipt()).await,
            Err(CommitError::Unknown(_))
        ));
        assert!(
            log.append(0, b"step".as_slice().into(), receipt())
                .await
                .is_err()
        );
        drop(log);
        let reopened = journal(&store).await?;
        assert_eq!(reopened.frontier(), 1);
        assert_eq!(reopened.reader().read(1, LIMIT).await?.entries.len(), 1);
        Ok(())
    })
}

#[test]
fn legacy_unknown_format_and_incarnation_mismatch_do_not_mutate() -> Res<()> {
    futures::executor::block_on(async {
        for bytes in [None, Some(1), Some(999)] {
            let store = MemoryStore::default();
            let mut writer = store.writer()?;
            let mutation = match bytes {
                None => Mutation {
                    key: b"___kv_store_log_latest_id".as_slice().into(),
                    value: Some(1u64.to_le_bytes().into()),
                },
                Some(version) => metadata_mutation(&Metadata {
                    version,
                    partition_incarnation: 42,
                    authority_epoch: 3,
                    frontier: 0,
                    snapshot_cut: 0,
                    compacted_through: 0,
                }),
            };
            writer.commit(0, vec![mutation]).await?;
            let before = store
                .read(&[], (Bound::Unbounded, Bound::Unbounded), 4)
                .await?;
            assert!(Journal::open(writer, 42).await.is_err());
            let after = store
                .read(&[], (Bound::Unbounded, Bound::Unbounded), 4)
                .await?;
            assert_eq!(before.entries, after.entries);
            assert_eq!(before.revision, after.revision);
        }
        let store = MemoryStore::default();
        drop(journal(&store).await?);
        assert!(Journal::open(store.writer()?, 43).await.is_err());
        let reopened = journal(&store).await?;
        assert_eq!(reopened.authority_epoch(), 2);
        Ok(())
    })
}

#[test]
fn bounded_tail_orders_across_byte_boundaries_and_rejects_holes() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        for frontier in 0..258u64 {
            assert_eq!(
                log.append(frontier, frontier.to_be_bytes().into(), vec![])
                    .await?,
                frontier + 1
            );
        }
        let reader = log.reader();
        let page = reader.read(255, LIMIT).await?;
        assert_eq!(page.frontier, 258);
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.position)
                .collect::<Vec<_>>(),
            vec![255, 256]
        );
        let mut tail = reader.tail(255, LIMIT);
        for position in 255..=258 {
            assert_eq!(
                tail.next().now_or_never().unwrap().unwrap()?.position,
                position
            );
        }
        assert!(tail.next().now_or_never().is_none());
        assert!(
            log.append(0, b"stale".as_slice().into(), vec![])
                .await
                .is_err()
        );
        drop(log);
        let mut writer = store.writer()?;
        let revision = store
            .read(&[], (Bound::Unbounded, Bound::Unbounded), 0)
            .await?
            .revision;
        writer
            .commit(
                revision,
                vec![Mutation {
                    key: entry_key(256),
                    value: None,
                }],
            )
            .await?;
        assert!(
            reader
                .read(255, LIMIT)
                .await
                .unwrap_err()
                .to_string()
                .contains("missing journal position")
        );
        Ok(())
    })
}

async fn commit(
    log: &mut Journal<FaultWriter>,
    authority: &mut Authority,
    command: Command,
) -> Res<Vec<Intent>> {
    let Decision::Commit(proposal) = authority.prepare(command).unwrap() else {
        panic!("expected new transition")
    };
    let intents = proposal.intents.clone();
    let before = log.writer.commits;
    let position = log
        .append(
            proposal.expected_revision,
            serde_json::to_vec(&proposal.transition)?.into(),
            receipt(),
        )
        .await?;
    assert_eq!(log.writer.commits, before + 1);
    assert_eq!(authority.revision() + 1, position);
    assert_eq!(authority.publish(*proposal).position, position);
    Ok(intents)
}

#[test]
fn authority_warm_step_has_one_commit_and_cold_replay_cannot_continue_old_lease() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        let mut authority = Authority::new(42, log.authority_epoch());
        commit(
            &mut log,
            &mut authority,
            Command::Submit {
                key: "job".into(),
                args: "{}".into(),
                policy: RetryPolicy::Immediate,
                budget: RetryBudget::default(),
            },
        )
        .await?;
        let incarnation = authority.jobs()["job"].incarnation;
        commit(
            &mut log,
            &mut authority,
            Command::Grant {
                key: "job".into(),
                incarnation,
                worker: 11,
                now_ms: 0,
            },
        )
        .await?;
        let lease = authority.jobs()["job"].lease.clone().unwrap();
        let command = Command::CommitStep {
            key: "job".into(),
            lease,
            step: 0,
            attempt: 0,
            value: "created".into(),
            kind: StepKind::Effect,
            name: None,
        };
        let intents = commit(&mut log, &mut authority, command.clone()).await?;
        assert!(matches!(intents.as_slice(), [Intent::Continue { .. }]));
        drop(log);
        let reopened = journal(&store).await?;
        let page = reopened
            .reader()
            .read(1, NonZeroUsize::new(8).unwrap())
            .await?;
        let transitions: Vec<Transition> = page
            .entries
            .iter()
            .map(|entry| serde_json::from_slice(&entry.value).unwrap())
            .collect();
        let recovered = Authority::recover(42, reopened.authority_epoch(), None, &transitions)?;
        assert!(recovered.jobs()["job"].lease.is_none());
        let Decision::Duplicate(ack) = recovered.prepare(command).unwrap() else {
            panic!("expected duplicate receipt")
        };
        assert_eq!(ack.disposition, Disposition::Park);
        assert_eq!(ack.position, 3);
        Ok(())
    })
}

#[test]
fn paged_compaction_is_bounded_and_rejects_unpublished_cut() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        for frontier in 0..5u64 {
            log.append(
                frontier,
                format!("entry-{}", frontier + 1).into_bytes().into(),
                vec![],
            )
            .await?;
        }

        assert!(matches!(
            log.compact_page(1, LIMIT).await,
            Err(CommitError::NotCommitted(_))
        ));
        assert!(
            log.save_snapshot(6, b"beyond-frontier".as_slice().into())
                .await
                .is_err()
        );
        log.save_snapshot(2, b"snap-2".as_slice().into()).await?;
        log.save_snapshot(5, b"snap-5".as_slice().into()).await?;
        assert!(matches!(
            log.compact_page(6, LIMIT).await,
            Err(CommitError::NotCommitted(_))
        ));

        let before = log.writer.mutation_counts.len();
        assert_eq!(
            log.compact_page(5, LIMIT).await?,
            CompactionProgress::Pending {
                compacted_through: 2
            }
        );
        assert_eq!(
            log.compact_page(5, LIMIT).await?,
            CompactionProgress::Pending {
                compacted_through: 4
            }
        );
        assert_eq!(
            log.compact_page(5, LIMIT).await?,
            CompactionProgress::Complete {
                compacted_through: 5
            }
        );
        assert_eq!(&log.writer.mutation_counts[before..], &[3, 3, 2]);
        assert_eq!(log.compacted_through(), 5);
        assert_eq!(
            log.reader().snapshot().await?,
            Some(JournalSnapshot {
                cut: 5,
                compacted_through: 5,
                frontier: 5,
                value: b"snap-5".as_slice().into(),
            })
        );

        let snapshots = store
            .read(
                &[],
                (
                    Bound::Included(Arc::from(&b"\x03"[..])),
                    Bound::Excluded(Arc::from(&b"\x04"[..])),
                ),
                usize::MAX,
            )
            .await?;
        assert_eq!(snapshots.entries.len(), 1, "one published snapshot slot");
        Ok(())
    })
}

#[test]
fn compaction_rollback_and_unknown_outcome_reconcile_on_reopen() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        for frontier in 0..4u64 {
            log.append(frontier, frontier.to_be_bytes().into(), vec![])
                .await?;
        }
        log.save_snapshot(4, b"snapshot".as_slice().into()).await?;

        log.writer.fault = Some(Fault::Reject);
        assert!(matches!(
            log.compact_page(4, LIMIT).await,
            Err(CommitError::NotCommitted(_))
        ));
        assert_eq!(log.compacted_through(), 0);
        assert_eq!(log.reader().read(1, LIMIT).await?.entries.len(), 2);

        log.writer.fault = Some(Fault::Unknown);
        assert!(matches!(
            log.compact_page(4, LIMIT).await,
            Err(CommitError::Unknown(_))
        ));
        assert_eq!(log.compacted_through(), 0);
        assert!(matches!(
            log.compact_page(4, LIMIT).await,
            Err(CommitError::NotCommitted(_))
        ));

        drop(log);
        let mut reopened = journal(&store).await?;
        assert_eq!(reopened.compacted_through(), 2);
        assert_eq!(
            reopened.compact_page(4, LIMIT).await?,
            CompactionProgress::Complete {
                compacted_through: 4
            }
        );
        Ok(())
    })
}

#[test]
fn missing_snapshot_blocks_recovery_and_compaction() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        log.append(0, b"entry".as_slice().into(), vec![]).await?;
        log.save_snapshot(1, b"snapshot".as_slice().into()).await?;
        let reader = log.reader();

        log.commit_metadata(
            log.metadata.clone(),
            vec![Mutation {
                key: PUBLISHED_SNAPSHOT_KEY.into(),
                value: None,
            }],
        )
        .await?;

        assert!(
            reader
                .snapshot()
                .await
                .unwrap_err()
                .to_string()
                .contains("missing published journal snapshot at cut 1")
        );
        assert!(matches!(
            log.compact_page(1, LIMIT).await,
            Err(CommitError::NotCommitted(_))
        ));
        assert_eq!(log.compacted_through(), 0);
        Ok(())
    })
}

#[test]
fn stale_read_and_tail_report_typed_error_once_then_require_snapshot_reload() -> Res<()> {
    futures::executor::block_on(async {
        let store = MemoryStore::default();
        let mut log = journal(&store).await?;
        for frontier in 0..4u64 {
            log.append(frontier, frontier.to_be_bytes().into(), vec![])
                .await?;
        }
        log.save_snapshot(2, b"snapshot".as_slice().into()).await?;
        assert!(matches!(
            log.compact_page(2, LIMIT).await?,
            CompactionProgress::Complete {
                compacted_through: 2
            }
        ));
        let reader = log.reader();

        let error = reader.read(1, LIMIT).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<StaleReaderError>(),
            Some(&StaleReaderError {
                requested: 1,
                compacted_through: 2,
            })
        );

        let mut tail = reader.tail(2, LIMIT);
        let error = tail.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.downcast_ref::<StaleReaderError>(),
            Some(&StaleReaderError {
                requested: 2,
                compacted_through: 2,
            })
        );
        assert!(
            tail.next().await.is_none(),
            "stale tail terminates after one error"
        );
        drop(tail);

        let snapshot = reader.snapshot().await?.unwrap();
        let suffix = reader.read(snapshot.cut + 1, LIMIT).await?;
        assert_eq!(
            suffix
                .entries
                .iter()
                .map(|entry| entry.position)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        Ok(())
    })
}
