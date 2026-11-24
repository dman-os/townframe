//! The bucket index: each part's summaries at every level, moved by member transitions.

use big_automata::part_store::bucket_summary::BucketMemberKind;
use big_automata::part_store::bucket_summary::BucketSummary;
use big_automata::part_store::bucket_summary::BucketSummaryRaw;
use big_automata::part_store::bucket_summary::BucketSummaryState;
use big_automata::rev::Rev;
use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::interlude::*;
use crate::keys::BlobKey;
use crate::sql;

/// `BuckId`'s packed form, as the `buck_id` column holds it.
pub fn buck_raw(id: BuckId) -> i64 {
    (i64::from(id.level()) << 16) | i64::from(id.index())
}

/// The deepest bucket index of an object: the `buck` column of its member rows.
pub fn deepest_index(obj: &ObjKey) -> i64 {
    i64::from(BuckId::deepest_from_obj_key(obj).index())
}

/// The deepest bucket indices a bucket covers, as a half-open range (the last bucket's end is
/// past `u16::MAX`).
pub fn deepest_range(buck: BuckId) -> (i64, i64) {
    let shift = u32::from(BuckId::BITS_PER_LEVEL) * u32::from(BuckId::MAX_LEVEL - buck.level());
    let start = i64::from(buck.index()) << shift;
    (start, start + (1 << shift))
}

/// One member's move between two states, in every bucket of `part` that contains `obj`.
pub struct Transition {
    pub part: PartKey,
    pub obj: ObjKey,
    pub old: BucketMemberKind,
    pub new: BucketMemberKind,
}

type Raw = (Vec<u8>, i64, i64, i64, i64, i64, i64);

fn state_from_row(
    (_, _, changed_at, live_count, dead_count, live_fp, dead_fp): Raw,
) -> BucketSummaryState {
    BucketSummaryRaw {
        changed_at: sql::rev_from_sql(changed_at),
        live_count: u32::try_from(live_count).expect(ERROR_IMPOSSIBLE),
        dead_count: u32::try_from(dead_count).expect(ERROR_IMPOSSIBLE),
        live_fp: live_fp as u64,
        dead_fp: dead_fp as u64,
    }
    .into()
}

/// Apply `transitions` in order, stamping every bucket they touch with `rev`. Each touched
/// bucket row is read once and written once, however many transitions pass through it.
pub async fn apply(
    executor: &(impl Executor + Sync),
    rev: Rev,
    transitions: Vec<Transition>,
) -> Res<()> {
    if transitions.is_empty() {
        return Ok(());
    }
    let touched = |transition: &Transition| {
        let deepest = BuckId::deepest_from_obj_key(&transition.obj);
        (0..=BuckId::MAX_LEVEL).map(move |level| deepest.to_level(level))
    };
    let keys = transitions
        .iter()
        .flat_map(|transition| touched(transition).map(|id| (transition.part.clone(), id)))
        .collect::<BTreeSet<_>>();

    let mut states = BTreeMap::new();
    for chunk in sql::chunk_rows(keys.iter().collect(), 2, 0) {
        let mut query = Sql::new(
            "SELECT part_key
                  , buck_id
                  , changed_at
                  , live_count
                  , dead_count
                  , live_fp
                  , dead_fp
               FROM big_automata_buckets
              WHERE (part_key, buck_id) IN (",
        );
        sql::push_values(
            &mut query,
            chunk.iter().map(|(part, id)| (part.blob(), buck_raw(*id))),
        );
        query.push(")");
        for row in query.query(executor).await?.all::<Raw>()? {
            let key = (PartKey::from_blob(&row.0), from_raw(row.1));
            states.insert(key, state_from_row(row));
        }
    }

    for transition in &transitions {
        for id in touched(transition) {
            states
                .entry((transition.part.clone(), id))
                .or_default()
                .apply_transition(
                    id,
                    transition.obj.clone(),
                    rev,
                    transition.old,
                    transition.new,
                );
        }
    }

    let rows = states
        .into_iter()
        .map(|((part, id), state)| {
            let raw = state.raw();
            (
                part.blob(),
                buck_raw(id),
                sql::rev_to_sql(raw.changed_at),
                i64::from(raw.live_count),
                i64::from(raw.dead_count),
                raw.live_fp as i64,
                raw.dead_fp as i64,
            )
        })
        .collect();
    sql::execute_values(
        executor,
        "INSERT INTO big_automata_buckets (
               part_key
             , buck_id
             , changed_at
             , live_count
             , dead_count
             , live_fp
             , dead_fp
         ) ",
        rows,
        7,
        " ON CONFLICT (part_key, buck_id) DO UPDATE SET
              changed_at = excluded.changed_at
            , live_count = excluded.live_count
            , dead_count = excluded.dead_count
            , live_fp = excluded.live_fp
            , dead_fp = excluded.dead_fp",
    )
    .await
}

/// A bucket id from its packed form.
pub fn from_raw(raw: i64) -> BuckId {
    BuckId::new((raw >> 16) as u8, raw as u16)
}

/// The part's summaries of the buckets asked, in order; a bucket with no row is the empty one.
pub async fn summaries(
    executor: &impl QueryExecutor,
    part: &PartKey,
    ids: &[BuckId],
) -> Res<Vec<BucketSummary>> {
    let mut found = BTreeMap::new();
    for chunk in sql::chunk_rows(ids.to_vec(), 1, 1) {
        let mut query = Sql::new(
            "SELECT part_key
                  , buck_id
                  , changed_at
                  , live_count
                  , dead_count
                  , live_fp
                  , dead_fp
               FROM big_automata_buckets
              WHERE part_key = ",
        );
        query.bind(part.blob()).push(" AND buck_id IN ");
        query.bind_list(chunk.iter().map(|id| buck_raw(*id)));
        for row in query.query(executor).await?.all::<Raw>()? {
            found.insert(row.1, state_from_row(row));
        }
    }
    Ok(ids
        .iter()
        .map(|id| {
            found
                .get(&buck_raw(*id))
                .cloned()
                .unwrap_or_default()
                .summary(*id)
        })
        .collect())
}
