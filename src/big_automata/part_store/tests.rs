//! The part store laws against the memory store, and the rev stream laws over its log.

use futures::executor::block_on;
use proptest::prelude::*;

use super::laws::Backend;
use super::laws::LogStreams;
use super::laws::{self};
use super::memory::MemoryPartStore;
use super::memory::MemoryPartTx;
use super::*;
use crate::rev::laws::{self as stream_laws};

#[derive(Clone)]
struct Memory;

#[async_trait]
impl Backend for Memory {
    type Store = MemoryPartStore;

    async fn open(&self) -> Res<MemoryPartStore> {
        Ok(default())
    }

    async fn begin(&self, store: &MemoryPartStore) -> Res<MemoryPartTx> {
        Ok(store.begin())
    }

    async fn commit(&self, tx: MemoryPartTx) -> Res<()> {
        tx.commit();
        Ok(())
    }

    async fn rollback(&self, tx: MemoryPartTx) -> Res<()> {
        drop(tx);
        Ok(())
    }
}

#[test]
fn replay_reconstructs_membership_and_access() -> Res<()> {
    block_on(laws::replay_reconstructs_membership_and_access(&Memory))
}

#[test]
fn collapse_and_tombstones() -> Res<()> {
    block_on(laws::collapse_and_tombstones(&Memory))
}

#[test]
fn pending_goes_live_once() -> Res<()> {
    block_on(laws::pending_goes_live_once(&Memory))
}

#[test]
fn authorization_follows_current_access() -> Res<()> {
    block_on(laws::authorization_follows_current_access(&Memory))
}

#[test]
fn withheld_is_not_deleted() -> Res<()> {
    block_on(laws::withheld_is_not_deleted(&Memory))
}

#[test]
fn dirty_count_counts_log_rows() -> Res<()> {
    block_on(laws::dirty_count_counts_log_rows(&Memory))
}

#[test]
fn peer_cursors_only_advance() -> Res<()> {
    block_on(laws::peer_cursors_only_advance(&Memory))
}

#[test]
fn writes_are_atomic() -> Res<()> {
    block_on(laws::writes_are_atomic(&Memory))
}

#[test]
fn unchanged_writes_stamp_nothing() -> Res<()> {
    block_on(laws::unchanged_writes_stamp_nothing(&Memory))
}

#[test]
fn log_stream_commit_during_replay() -> Res<()> {
    block_on(stream_laws::stream_commit_during_replay(&LogStreams::new(
        Memory,
    )))
}

#[test]
fn log_stream_overwrite_of_unread_key() -> Res<()> {
    block_on(stream_laws::stream_overwrite_of_unread_key(
        &LogStreams::new(Memory),
    ))
}

#[test]
fn log_stream_commit_during_read() -> Res<()> {
    block_on(stream_laws::stream_commit_during_read(&LogStreams::new(
        Memory,
    )))
}

proptest! {
    #[test]
    fn differential(ops in laws::part_ops()) {
        block_on(laws::differential(&Memory, ops))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }

    #[test]
    fn log_stream_follows(script in stream_laws::stream_scripts()) {
        block_on(stream_laws::stream_follows(&LogStreams::new(Memory), script))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}

#[test]
#[should_panic(expected = "a transaction was used on another part store")]
fn a_transaction_belongs_to_its_store() {
    let store = MemoryPartStore::default();
    let mut tx = MemoryPartStore::default().begin();
    block_on(store.ensure_parts(&mut tx, &[laws::part(0)])).expect("the stage panics first");
}

/// The leaves of a bucket page by `after` without loss: one entry at a time reaches every
/// member the unbounded read returns, in the same order.
#[test]
fn bucket_leaves_page_without_loss() -> Res<()> {
    let store = MemoryPartStore::default();
    let mut tx = store.begin();
    let objs = (0..12).map(laws::obj).collect::<Vec<_>>();
    block_on(
        store.set_payloads(
            &mut tx,
            objs.iter()
                .map(|obj| (obj.clone(), laws::payload(1)))
                .collect(),
        ),
    )?;
    let members = objs
        .iter()
        .map(|obj| (obj.clone(), laws::part(0)))
        .collect::<Vec<_>>();
    block_on(store.add_members(&mut tx, &members))?;
    block_on(store.remove_members(&mut tx, &members[..3]))?;
    tx.commit();

    let part = laws::part(0);
    let all = block_on(store.bucket_leaves(&part, BuckId::ROOT, None, usize::MAX))?;
    assert_eq!(all.len(), 12);
    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let page = block_on(store.bucket_leaves(&part, BuckId::ROOT, after.as_ref(), 1))?;
        let Some(entry) = page.into_iter().next() else {
            break;
        };
        after = Some(entry.obj.clone());
        paged.push(entry);
    }
    assert_eq!(paged, all);
    Ok(())
}
