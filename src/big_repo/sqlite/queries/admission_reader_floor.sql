-- name: admission_reader_floor :one
-- param: scope_id i64
-- returns: reader_floor Option<i64>
SELECT MIN(COALESCE(p.upstream_revision, 0)) AS reader_floor
  FROM big_repo_keyhive_admission_readers r
  LEFT JOIN delta_walker_progress p ON (p.namespace || '/' || p.consumer_id) = r.reader
 WHERE r.scope_id = :scope_id;
