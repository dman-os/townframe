//! Properties the laws' fixed universe cannot reach: keys of every length against the oracle,
//! and the bookkeeping columns against a recount.

use std::num::NonZeroUsize;

use big_automata::part_store::AccessLevel;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::LogSelector;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::laws::part_writes;
use big_automata::part_store::laws::payload;
use big_automata::part_store::laws::stage;
use big_automata::part_store::oracle::PartOracle;
use big_automata::part_store::oracle::PartWrite;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use futures::executor::block_on;
use proptest::prelude::*;

use super::Store;
use super::config;
use crate::interlude::*;
use crate::*;

/// Keys on both sides of the inline/heap boundary and where length order and byte order
/// disagree (`[1]` sorts before `[0, 0]`).
fn key_bytes(index: usize) -> Vec<u8> {
    [
        vec![],
        vec![1],
        vec![0, 0],
        vec![255; 32],
        vec![7; 33],
        vec![7; 40],
    ][index]
        .clone()
}

fn any_obj() -> impl Strategy<Value = ObjKey> {
    (0..6_usize).prop_map(|index| ObjKey::new(&key_bytes(index)))
}
fn any_part() -> impl Strategy<Value = PartKey> {
    (0..6_usize).prop_map(|index| PartKey::new(&key_bytes(index)))
}
fn any_principal() -> impl Strategy<Value = PeerKey> {
    (0..3_usize).prop_map(|index| PeerKey::new(&key_bytes(index + 1)))
}

fn long_key_writes() -> impl Strategy<Value = Vec<PartWrite>> {
    use proptest::collection::vec;
    let write = prop_oneof![
        3 => vec((any_obj(), (0u8..3).prop_map(payload)), 1..4).prop_map(PartWrite::SetPayloads),
        3 => vec((any_obj(), any_part()), 1..4).prop_map(PartWrite::AddMembers),
        2 => vec((any_obj(), any_part()), 1..3).prop_map(PartWrite::RemoveMembers),
        2 => vec((any_part(), any_principal()), 1..3).prop_map(|pairs| PartWrite::GrantAccess(
            pairs.into_iter().map(|(part, principal)| (part, principal, AccessLevel::Read)).collect()
        )),
        1 => vec((any_part(), any_principal()), 1..3).prop_map(PartWrite::RevokeAccess),
    ];
    vec(write, 1..4)
}

async fn commit(store: &Store, writes: &[PartWrite]) -> Res<()> {
    let mut tx = store.begin().await?;
    for write in writes {
        stage(store, &mut tx, write.clone()).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Every page of a full replay, by unauthorized and authorized reads, is the oracle's.
async fn assert_pages(store: &Store, oracle: &PartOracle, latest: Rev) -> Res<()> {
    let limits = Limits {
        max_entries: NonZeroUsize::new(3).expect("nonzero"),
        max_bytes: None,
    };
    let mut after = 0;
    while after < latest {
        let mut page = Page::default();
        store
            .read(&LogSelector::All, after, latest, limits, &mut page)
            .await?;
        assert_eq!(page, oracle.read(&LogSelector::All, after, latest, limits));
        after = page.through;
    }
    let objs = (0..6)
        .map(|index| ObjKey::new(&key_bytes(index)))
        .collect::<Vec<_>>();
    let parts = (0..6)
        .map(|index| PartKey::new(&key_bytes(index)))
        .collect::<Vec<_>>();
    let routes = LogRoutes {
        parts: parts.iter().map(|part| (part.clone(), 0)).collect(),
        objects: objs.iter().map(|obj| (obj.clone(), 0)).collect(),
    };
    for index in 0..3 {
        let principal = PeerKey::new(&key_bytes(index + 1));
        let mut after = 0;
        while after < latest {
            let mut page = AuthorizedPage::default();
            store
                .read_authorized(&principal, &routes, after, latest, limits, &mut page)
                .await?;
            assert_eq!(
                page,
                oracle.read_authorized(&principal, &routes, after, latest, limits)
            );
            after = page.rows.through;
        }
    }
    assert_eq!(
        store.obj_states(&objs).await?,
        objs.iter()
            .map(|obj| oracle.obj_state(obj))
            .collect::<Vec<_>>()
    );
    Ok(())
}

proptest! {
    #![proptest_config(config())]

    /// Keys of lengths 0 to 40 order, page and authorize exactly as the oracle's do, whatever
    /// their bytes: the store's blob encoding preserves the keys' order.
    #[test]
    fn keys_of_every_length_match_the_oracle(
        batches in proptest::collection::vec(long_key_writes(), 1..8),
    ) {
        let result: Res<()> = block_on(async {
            let store = super::fresh().await?;
            let mut oracle = PartOracle::default();
            for writes in batches {
                commit(&store, &writes).await?;
                oracle.commit(writes);
                assert_pages(&store, &oracle, oracle.latest_rev()).await?;
            }
            Ok(())
        });
        result.map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }

    /// `part_count`, which the part-less object index rests on, is the object's live
    /// memberships after any sequence of writes.
    #[test]
    fn part_count_is_the_live_membership_count(
        batches in proptest::collection::vec(part_writes(), 1..10),
    ) {
        let result: Res<()> = block_on(async {
            let store = super::fresh().await?;
            for writes in batches {
                commit(&store, &writes).await?;
            }
            let wrong = store
                .db
                .query(
                    "SELECT o.obj_key
                       FROM big_automata_objs AS o
                      WHERE o.part_count != (
                            SELECT COUNT(*)
                              FROM big_automata_members AS m
                             WHERE m.obj_key = o.obj_key
                               AND m.live = 1
                            )",
                    (),
                )
                .await?;
            assert!(wrong.rows.is_empty(), "objects with a wrong part_count: {wrong:?}");
            Ok(())
        });
        result.map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}

/// One revision holding keys whose length order and byte order disagree pages in the keys'
/// order, as the oracle does: `[1]` before `[0, 0]`, the 32 byte key before the 33 byte one.
#[test]
fn length_first_keys_page_in_key_order() -> Res<()> {
    block_on(async {
        let store = super::fresh().await?;
        let mut oracle = PartOracle::default();
        let part = PartKey::new(&[9]);
        let objs = [vec![0, 0], vec![1], vec![255; 32], vec![0; 33], vec![]]
            .iter()
            .map(|bytes| ObjKey::new(bytes))
            .collect::<Vec<_>>();
        let writes = vec![
            PartWrite::SetPayloads(objs.iter().map(|obj| (obj.clone(), payload(1))).collect()),
            PartWrite::AddMembers(objs.iter().map(|obj| (obj.clone(), part.clone())).collect()),
        ];
        commit(&store, &writes).await?;
        oracle.commit(writes);
        let mut page = Page::default();
        store
            .read(
                &LogSelector::All,
                0,
                1,
                Limits {
                    max_entries: NonZeroUsize::MAX,
                    max_bytes: None,
                },
                &mut page,
            )
            .await?;
        let paged = page
            .entries
            .iter()
            .map(|entry| match &entry.key {
                big_automata::part_store::LogKey::Member { obj, .. } => obj.clone(),
                big_automata::part_store::LogKey::Access { .. } => unreachable!(),
            })
            .collect::<Vec<_>>();
        let mut sorted = objs.clone();
        sorted.sort();
        assert_eq!(paged, sorted);
        assert_pages(&store, &oracle, 1).await
    })
}
