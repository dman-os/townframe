//! The edges of bucket order: what a listing does at the last bucket of a level.

use super::*;

/// Law: bucket order is by level, then index, and the id after a level's last bucket
/// ([`BuckId::increment`]) lists nothing at its own level and nothing before the next one, so a
/// listing that reaches the last bucket of a level neither repeats it nor skips what follows.
///
/// The last bucket of level 1 covers the top sixteenth of the deepest indices, including the
/// deepest bucket `u16::MAX`, which is also the last bucket of level 4: a member there is under
/// both.
pub async fn a_listing_ends_at_the_last_bucket_of_a_level<B: Backend>(backend: &B) -> Res<()> {
    let last_of_one = BuckId::new(1, u16::MAX);
    let last_of_four = BuckId::new(BuckId::MAX_LEVEL, u16::MAX);
    assert_eq!(
        last_of_one,
        BuckId::new(1, 15),
        "the last bucket of level 1 is canonical"
    );
    let high = obj_in_bucket(last_of_one, 0);
    let top = obj_in_bucket(last_of_four, 0);
    let low = obj_in_bucket(BuckId::new(1, 0), 0);
    assert_ne!(high, top);

    let members = [high, top.clone(), low];
    let mut harness = Harness::open_over(backend, members.to_vec()).await?;
    harness.commit(vec![grant(&[(0, 0)])]).await?;
    harness
        .commit(vec![
            PartWrite::SetPayloads(
                members
                    .iter()
                    .map(|key| (key.clone(), payload(1)))
                    .collect(),
            ),
            PartWrite::AddMembers(members.iter().map(|key| (key.clone(), part(0))).collect()),
        ])
        .await?;

    let principal = principal(0);
    let list = |from: BuckId, to_level: BuckLevel| {
        let (store, principal) = (&harness.store, &principal);
        async move {
            eyre::Ok(
                store
                    .bucket_list(principal, &part(0), from, to_level, 0, usize::MAX)
                    .await?
                    .expect("the part is readable"),
            )
        }
    };
    let ids = |listing: &[BucketSummary]| listing.iter().map(|buck| buck.id).collect::<Vec<_>>();

    // From the last bucket itself: it is listed, with both members under it.
    let at_last = list(last_of_one, 1).await?;
    assert_eq!(ids(&at_last), [last_of_one]);
    assert_eq!(at_last[0].len, 2);

    // Past it: nothing at the level, and what the next level has is what a listing from the
    // next level's own start has.
    let past = last_of_one.increment();
    assert_eq!(past.level(), 1, "the step stays on the level it left");
    assert!(past > last_of_one && past < BuckId::new(2, 0));
    assert_eq!(list(past, 1).await?, []);
    let next_level = list(past, 4).await?;
    assert_eq!(next_level, list(BuckId::new(2, 0), 4).await?);
    assert!(
        next_level.iter().all(|buck| buck.id.level() >= 2) && !next_level.is_empty(),
        "{:?}",
        ids(&next_level)
    );

    // The last bucket of the deepest level: listed once, and the step past it lists nothing
    // at all, rather than the bucket again.
    let at_top = list(last_of_four, 4).await?;
    assert_eq!(ids(&at_top), [last_of_four]);
    assert_eq!(at_top[0].len, 1);
    let after_top = last_of_four.increment();
    assert!(after_top > last_of_four, "the last bucket has a successor");
    assert_eq!(list(after_top, BuckId::MAX_LEVEL).await?, []);

    // Its leaves are the member it holds, and the last bucket of level 1 reaches it too.
    let leaves = |id| {
        let (store, principal) = (&harness.store, &principal);
        async move {
            eyre::Ok(
                store
                    .bucket_leaves(principal, &part(0), &[(id, None)], 0, usize::MAX)
                    .await?
                    .expect("the part is readable")
                    .remove(0),
            )
        }
    };
    assert_eq!(
        leaves(last_of_four)
            .await?
            .iter()
            .map(|entry| &entry.obj)
            .collect::<Vec<_>>(),
        [&top]
    );
    let under_one = leaves(last_of_one).await?;
    assert_eq!(under_one.len(), 2);
    assert_eq!(
        under_one.last().map(|entry| &entry.obj),
        Some(&top),
        "deepest index u16::MAX orders last"
    );
    Ok(())
}
