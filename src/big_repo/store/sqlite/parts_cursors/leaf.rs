use super::*;
use big_sync_core::rpc::LeafBucketRequest;

pub(super) fn query(
    store: &SqliteBigRepoStore,
    part_id: PartKey,
    since: CursorIndex,
    buckets: &[LeafBucketRequest],
    limit_hint: u32,
) -> Sql {
    let mut query =
        Sql::new("WITH requested(req_ord, buck_id, lower_index, upper_index, after_id) AS (");
    for (req_ord, buck_req) in buckets.iter().enumerate() {
        let (lower_index, upper_index) = big_sync::bucket_index_bounds(buck_req.buck_id);
        if req_ord > 0 {
            query.push(" UNION ALL ");
        }
        query.push("SELECT ");
        query.bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
        query.push(" AS req_ord, ");
        query.bind(SqliteBigRepoStore::buck_i64(buck_req.buck_id));
        query.push(" AS buck_id, ");
        query.bind(i64::from(lower_index));
        query.push(" AS lower_index, ");
        if let Some(upper_index) = upper_index {
            query.bind(i64::from(upper_index));
        } else {
            query.push("NULL");
        }
        query.push(" AS upper_index, ");
        if let Some(after) = buck_req.after.clone() {
            query.bind(SqliteBigRepoStore::obj_blob(after));
        } else {
            query.push("NULL");
        }
        query.push(" AS after_id");
    }
    query.push(
        "), ranked AS (
                SELECT r.req_ord
                     , r.buck_id
                     , o.obj_id
                     , m.event_type
                     , m.txid AS source_revision
                     , o.payload_json
                     , COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count
                     , ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY o.obj_id ASC) AS row_num
                  FROM requested r
                  JOIN big_sync_members m
                    ON m.scope_id = ",
    );
    query.bind(store.scope().id());
    query.push(" AND m.maybe_part_ref = (");
    query.push("SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
    query.bind(store.scope().id());
    query.push(" AND part_id = ");
    query.bind(SqliteBigRepoStore::part_blob(part_id));
    query.push(")");
    query.push(
        "
                JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
                JOIN big_sync_buckets s
                  ON s.scope_id = m.scope_id
                 AND s.part_ref = m.maybe_part_ref
                 AND s.buck_id = r.buck_id
                 AND s.changed_at > ",
    );
    query.bind(i64::try_from(since).expect(ERROR_IMPOSSIBLE));
    query.push(" WHERE o.buck_index >= r.lower_index");
    query.push(" AND (r.upper_index IS NULL OR o.buck_index < r.upper_index)");
    query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
    query.push(
        "
            )
            SELECT req_ord
                 , buck_id
                 , obj_id
                 , source_revision
                 , event_type
                 , payload_json
                 , total_count
              FROM ranked
             WHERE row_num <= ",
    );
    query.bind(i64::from(limit_hint.max(1)));
    query.push(" ORDER BY req_ord, obj_id ASC");
    query
}
