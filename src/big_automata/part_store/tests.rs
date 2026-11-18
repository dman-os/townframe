//! The part store laws against the memory store, and the rev stream laws over its log.

use future_form::Sendable;
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
fn grant_returns_the_backlog() -> Res<()> {
    block_on(laws::grant_returns_the_backlog(&Memory))
}

#[test]
fn removals_skip_routes_that_never_saw_the_member() -> Res<()> {
    block_on(laws::removals_skip_routes_that_never_saw_the_member(
        &Memory,
    ))
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
    block_on(PartStoreWrite::<Sendable>::ensure_parts(
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
    block_on(PartStoreWrite::<Sendable>::set_payloads(
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
    block_on(PartStoreWrite::<Sendable>::add_members(
        &store, &mut tx, &members,
    ))?;
    block_on(PartStoreWrite::<Sendable>::remove_members(
        &store,
        &mut tx,
        &members[..3],
    ))?;
    let (part, peer) = (laws::part(0), laws::principal(0));
    block_on(PartStoreWrite::<Sendable>::grant_access(
        &store,
        &mut tx,
        &[(part.clone(), peer.clone(), AccessLevel::Read)],
    ))?;
    tx.commit();

    let leaves = |after: Option<ObjKey>, limit| {
        block_on(PartStoreServe::<Sendable>::bucket_leaves(
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

/// The memory part store through the `Local` form, as the single-threaded web node drives it:
/// writes, the client's batched reads, the log as a stream and the responder's counts.
#[test]
fn the_local_form_drives_the_memory_part_store() -> Res<()> {
    use future_form::Local;

    use crate::rev::Page;
    use crate::rev::RevStream;
    use crate::rev::laws::limits;

    let store = MemoryPartStore::default();
    let mut tx = store.begin();
    let (obj, part, peer) = (laws::obj(0), laws::part(0), laws::principal(0));
    block_on(PartStoreWrite::<Local>::set_payloads(
        &store,
        &mut tx,
        vec![(obj.clone(), laws::payload(1))],
    ))?;
    block_on(PartStoreWrite::<Local>::add_members(
        &store,
        &mut tx,
        &[(obj.clone(), part.clone())],
    ))?;
    block_on(PartStoreWrite::<Local>::grant_access(
        &store,
        &mut tx,
        &[(part.clone(), peer.clone(), AccessLevel::Read)],
    ))?;
    assert_eq!(tx.commit(), Some(1));

    let states = block_on(PartStoreRead::<Local>::obj_states(
        &store,
        std::slice::from_ref(&obj),
    ))?;
    assert_eq!(
        states,
        vec![Some(ObjState {
            content: Some((laws::payload(1), PayloadDigest::of(&laws::payload(1))?)),
            parts: [part.clone()].into(),
        })]
    );
    assert_eq!(
        block_on(PartStoreServe::<Local>::part_summaries(
            &store,
            &peer,
            &[(part.clone(), 0)]
        ))?,
        vec![Some(PartSummary {
            latest_cursor: 1,
            member_count: 1,
            deepest_bucket_level: BuckId::MAX_LEVEL,
            dirty: PartDirtyCount {
                member_changes: 1,
                access_changes: 1,
            },
        })]
    );

    let mut stream = RevStream::<Local, _>::new(store.clone(), LogSelector::All, 0);
    let mut page = Page::default();
    block_on(stream.next_into(&mut page, limits(10, None)))?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>(),
        vec![
            LogKey::Member {
                obj: obj.clone(),
                part: part.clone(),
            },
            LogKey::Access {
                part,
                principal: peer,
            },
        ]
    );
    Ok(())
}
