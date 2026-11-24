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
fn revocation_hides_every_row_of_the_part() -> Res<()> {
    block_on(laws::revocation_hides_every_row_of_the_part(&Memory))
}

#[test]
fn revocation_mid_stream_hides_the_unread_rows() -> Res<()> {
    block_on(laws::revocation_mid_stream_hides_the_unread_rows(&Memory))
}

#[test]
fn object_routes_follow_the_readable_parts() -> Res<()> {
    block_on(laws::object_routes_follow_the_readable_parts(&Memory))
}

#[test]
fn revocation_hides_the_parts_buckets() -> Res<()> {
    block_on(laws::revocation_hides_the_parts_buckets(&Memory))
}

#[test]
fn an_empty_window_reads_verdicts_only() -> Res<()> {
    block_on(laws::an_empty_window_reads_verdicts_only(&Memory))
}

#[test]
fn authorized_reads_carry_their_payloads() -> Res<()> {
    block_on(laws::authorized_reads_carry_their_payloads(&Memory))
}

#[test]
fn grant_returns_the_backlog() -> Res<()> {
    block_on(laws::grant_returns_the_backlog(&Memory))
}

#[test]
fn removals_reach_every_route_above_its_bound() -> Res<()> {
    block_on(laws::removals_reach_every_route_above_its_bound(&Memory))
}

#[test]
fn a_payload_with_no_canonical_form_is_refused() -> Res<()> {
    block_on(laws::a_payload_with_no_canonical_form_is_refused(&Memory))
}

#[test]
fn dirty_count_counts_log_rows() -> Res<()> {
    block_on(laws::dirty_count_counts_log_rows(&Memory))
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
fn a_dropped_payload_is_gone() -> Res<()> {
    block_on(laws::a_dropped_payload_is_gone(&Memory))
}

#[test]
#[should_panic(expected = "live member")]
fn dropping_the_payload_of_a_member_is_a_caller_bug() {
    block_on(laws::dropping_the_payload_of_a_member_is_a_caller_bug(
        &Memory,
    ))
    .unwrap();
}

#[test]
fn removing_then_dropping_in_one_transaction_is_valid() -> Res<()> {
    block_on(laws::removing_then_dropping_in_one_transaction_is_valid(
        &Memory,
    ))
}

#[test]
fn a_partless_object_is_listed_until_its_payload_is_dropped() -> Res<()> {
    block_on(laws::a_partless_object_is_listed_until_its_payload_is_dropped(&Memory))
}

#[test]
fn dropping_is_atomic_with_its_transaction() -> Res<()> {
    block_on(laws::dropping_is_atomic_with_its_transaction(&Memory))
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
    block_on(PartStoreWrite::ensure_parts(
        &store,
        &mut tx,
        &[laws::part(0)],
    ))
    .expect("the stage panics first");
}

/// The leaves of a bucket page by `after` without loss: one entry at a time reaches every
/// member the unbounded read returns, in the same order.
#[test]
fn bucket_leaves_page_without_loss() -> Res<()> {
    let store = MemoryPartStore::default();
    let mut tx = store.begin();
    let objs = (0..12).map(laws::obj).collect::<Vec<_>>();
    block_on(PartStoreWrite::set_payloads(
        &store,
        &mut tx,
        objs.iter()
            .map(|obj| (obj.clone(), laws::payload(1)))
            .collect(),
    ))?;
    let members = objs
        .iter()
        .map(|obj| (obj.clone(), laws::part(0)))
        .collect::<Vec<_>>();
    block_on(PartStoreWrite::add_members(&store, &mut tx, &members))?;
    block_on(PartStoreWrite::remove_members(
        &store,
        &mut tx,
        &members[..3],
    ))?;
    let (part, peer) = (laws::part(0), laws::principal(0));
    block_on(PartStoreWrite::grant_access(
        &store,
        &mut tx,
        &[(part.clone(), peer.clone(), AccessLevel::Read)],
    ))?;
    tx.commit();

    let leaves = |after: Option<ObjKey>, limit| {
        block_on(PartStoreServe::bucket_leaves(
            &store,
            &peer,
            &part,
            &[(BuckId::ROOT, after)],
            0,
            limit,
        ))
        .map(|leaves| leaves.expect("the part is readable").remove(0))
    };
    let all = leaves(None, usize::MAX)?;
    assert_eq!(all.len(), 12);
    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let page = leaves(after.clone(), 1)?;
        let Some(entry) = page.into_iter().next() else {
            break;
        };
        after = Some(entry.obj.clone());
        paged.push(entry);
    }
    assert_eq!(paged, all);
    Ok(())
}
