//! The responder's reads. Every function takes the one read snapshot its caller began, decides
//! access from the principal's current access rows in it, and reads under it.

use big_automata::BuckLevel;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::LeafEntry;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::ObjPayload;
use big_automata::part_store::PartDirtyCount;
use big_automata::part_store::PartSummary;
use big_automata::part_store::PayloadDigest;
use big_automata::part_store::ReadVerdict;
use big_automata::part_store::bucket_summary::BucketSummary;
use big_automata::part_store::bucket_summary::BucketSummaryRaw;
use big_automata::part_store::bucket_summary::BucketSummaryState;
use big_automata::rev::Limits;
use big_automata::rev::Rev;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::buckets::buck_raw;
use crate::buckets::deepest_index;
use crate::buckets::deepest_range;
use crate::buckets::from_raw;
use crate::interlude::*;
use crate::keys::BlobKey;
use crate::log;
use crate::log::Arm;
use crate::read;
use crate::sql;
use crate::sqlite::queries;

async fn permits(x: &impl QueryExecutor, principal: &PeerKey, part: &PartKey) -> Res<bool> {
    Ok(queries::access_permits(x, part.blob(), principal.blob()).await? > 0)
}

/// Each part route's verdict, in the order of `parts`.
async fn part_verdicts(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    parts: &[&PartKey],
) -> Res<Vec<ReadVerdict>> {
    let mut known = BTreeMap::new();
    for chunk in sql::chunk_rows(parts.to_vec(), 1, 1) {
        let mut query = Sql::new(
            "SELECT p.part_key
                  , a.level IS NOT NULL
               FROM big_automata_parts AS p
               LEFT JOIN big_automata_access AS a
                 ON a.part_key = p.part_key
                AND a.principal = ",
        );
        query.bind(principal.blob());
        query.push(" WHERE p.part_key IN ");
        query.bind_list(chunk.iter().map(|part| part.blob()));
        for (part, permitted) in query.query(x).await?.all::<(Vec<u8>, i64)>()? {
            known.insert(PartKey::from_blob(&part), permitted == 1);
        }
    }
    Ok(parts
        .iter()
        .map(|part| match known.get(*part) {
            None => ReadVerdict::UnknownPart,
            Some(true) => ReadVerdict::Permitted,
            Some(false) => ReadVerdict::Denied,
        })
        .collect())
}

/// Each object route's verdict: permitted while the object is a live member of a part the
/// principal may read.
async fn object_verdicts(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    objs: &[&ObjKey],
) -> Res<Vec<ReadVerdict>> {
    let mut permitted = BTreeSet::new();
    for chunk in sql::chunk_rows(objs.to_vec(), 1, 1) {
        let mut query = Sql::new(
            "SELECT DISTINCT m.obj_key
               FROM big_automata_members AS m
               JOIN big_automata_access AS a
                 ON a.part_key = m.part_key
                AND a.level IS NOT NULL
                AND a.principal = ",
        );
        query.bind(principal.blob());
        query.push(" WHERE m.live = 1 AND m.obj_key IN ");
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for obj in query.query(x).await?.all::<Vec<u8>>()? {
            permitted.insert(ObjKey::from_blob(&obj));
        }
    }
    Ok(objs
        .iter()
        .map(|obj| {
            if permitted.contains(*obj) {
                ReadVerdict::Permitted
            } else {
                ReadVerdict::Denied
            }
        })
        .collect())
}

pub async fn read_authorized(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    routes: &LogRoutes,
    after: Rev,
    until: Rev,
    limits: Limits,
    page: &mut AuthorizedPage,
) -> Res<()> {
    assert!(
        after <= until,
        "read window ({after}, {until}] runs backwards"
    );
    let latest = read::latest_rev(x).await?;
    assert!(
        until <= latest,
        "read until {until} past the committed rev {latest}"
    );

    let part_keys = routes.parts.keys().collect::<Vec<_>>();
    let obj_keys = routes.objects.keys().collect::<Vec<_>>();
    page.parts = part_verdicts(x, principal, &part_keys).await?;
    page.objects = object_verdicts(x, principal, &obj_keys).await?;

    if after == until {
        // The empty window: verdicts only.
        page.rows.entries.clear();
        page.rows.through = until;
    } else {
        let arms = routes
            .parts
            .iter()
            .zip(&page.parts)
            .filter(|(_, verdict)| **verdict == ReadVerdict::Permitted)
            .map(|((part, lo), _)| Arm::Part { part, lo: *lo })
            .chain(
                routes
                    .objects
                    .iter()
                    .zip(&page.objects)
                    .filter(|(_, verdict)| **verdict == ReadVerdict::Permitted)
                    .map(|((obj, lo), _)| Arm::ReadableObject {
                        obj,
                        lo: *lo,
                        principal,
                    }),
            )
            .collect::<Vec<_>>();
        (page.rows.entries, page.rows.through) =
            log::read_arms(x, &arms, after, until, limits).await?;
    }
    page_payloads(x, page).await
}

/// The payload of every object with a live row in the page, sorted by object, each once.
async fn page_payloads(x: &impl QueryExecutor, page: &mut AuthorizedPage) -> Res<()> {
    let objs = page
        .rows
        .entries
        .iter()
        .filter(|entry| entry.value.is_some())
        .map(|entry| match &entry.key {
            big_automata::part_store::LogKey::Member { obj, .. } => obj,
            big_automata::part_store::LogKey::Access { .. } => {
                unreachable!("an authorized read carries no access row")
            }
        })
        .collect::<BTreeSet<_>>();
    page.payloads.clear();
    for chunk in sql::chunk_rows(objs.into_iter().collect(), 1, 0) {
        let mut query =
            Sql::new("SELECT obj_key, payload FROM big_automata_objs WHERE obj_key IN ");
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for (obj, payload) in query.query(x).await?.all::<(Vec<u8>, String)>()? {
            page.payloads.push((
                ObjKey::from_blob(&obj),
                serde_json::from_str::<ObjPayload>(&payload)
                    .expect("a stored payload is valid JSON"),
            ));
        }
    }
    page.payloads
        .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    Ok(())
}

pub async fn part_summaries(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    parts: &[(PartKey, Rev)],
) -> Res<Vec<Option<PartSummary>>> {
    let mut summaries = Vec::with_capacity(parts.len());
    for (part, since) in parts {
        if !permits(x, principal, part).await? {
            summaries.push(None);
            continue;
        }
        let since = sql::rev_to_sql(*since);
        let member_changes = queries::part_member_changes(x, part.blob(), since).await?;
        let access_rev = queries::access_rev(x, part.blob(), principal.blob()).await?;
        summaries.push(Some(PartSummary {
            latest_cursor: sql::rev_from_sql(
                queries::part_latest_cursor(x, part.blob())
                    .await?
                    .unwrap_or(0),
            ),
            member_count: read::member_count(x, part).await?,
            deepest_bucket_level: BuckId::MAX_LEVEL,
            dirty: PartDirtyCount {
                member_changes: u64::try_from(member_changes).expect(ERROR_IMPOSSIBLE),
                access_changes: u64::from(access_rev.is_some_and(|rev| rev > since)),
            },
        }));
    }
    Ok(summaries)
}

pub async fn bucket_list(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    part: &PartKey,
    from: BuckId,
    to_level: BuckLevel,
    since: Rev,
    limit: usize,
) -> Res<Option<Vec<BucketSummary>>> {
    if !permits(x, principal, part).await? {
        return Ok(None);
    }
    let last = BuckId::new(to_level, u16::MAX);
    if from > last {
        return Ok(Some(Vec::new()));
    }
    let rows = queries::bucket_list(
        x,
        part.blob(),
        buck_raw(from),
        buck_raw(last),
        sql::rev_to_sql(since),
        sql::limit(limit),
    )
    .await?;
    Ok(Some(
        rows.into_iter()
            .map(|row| {
                BucketSummaryState::from(BucketSummaryRaw {
                    changed_at: sql::rev_from_sql(row.changed_at),
                    live_count: u32::try_from(row.live_count).expect(ERROR_IMPOSSIBLE),
                    dead_count: u32::try_from(row.dead_count).expect(ERROR_IMPOSSIBLE),
                    live_fp: row.live_fp as u64,
                    dead_fp: row.dead_fp as u64,
                })
                .summary(from_raw(row.buck_id))
            })
            .collect(),
    ))
}

pub async fn bucket_leaves(
    x: &impl QueryExecutor,
    principal: &PeerKey,
    part: &PartKey,
    bucks: &[(BuckId, Option<ObjKey>)],
    since: Rev,
    limit: usize,
) -> Res<Option<Vec<Vec<LeafEntry>>>> {
    if !permits(x, principal, part).await? {
        return Ok(None);
    }
    let mut leaves = Vec::with_capacity(bucks.len());
    for (buck, after) in bucks {
        let changed_at = queries::bucket_changed_at(x, part.blob(), buck_raw(*buck)).await?;
        if changed_at.is_none_or(|changed_at| sql::rev_from_sql(changed_at) <= since) {
            leaves.push(Vec::new());
            continue;
        }
        let (start, end) = deepest_range(*buck);
        let (after_buck, after_key) = match after {
            None => (-1, Vec::new()),
            Some(after) => {
                let index = deepest_index(after);
                assert!(
                    (start..end).contains(&index),
                    "leaf cursor {after} is not in bucket {buck:?}"
                );
                (index, after.blob())
            }
        };
        let rows = queries::bucket_leaves(
            x,
            part.blob(),
            start,
            end,
            after_buck,
            after_key,
            sql::limit(limit),
        )
        .await?;
        leaves.push(
            rows.into_iter()
                .map(|row| LeafEntry {
                    obj: ObjKey::from_blob(&row.obj_key),
                    live: (row.live == 1).then(|| {
                        PayloadDigest(
                            row.digest
                                .expect("a live member has a payload")
                                .try_into()
                                .expect("a stored digest is 32 bytes"),
                        )
                    }),
                })
                .collect(),
        );
    }
    Ok(Some(leaves))
}
