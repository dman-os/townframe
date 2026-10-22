-- name: unapplied_keyhive_event_counts :many
-- param: scope_id i64
-- returns: source_id Option<Vec<u8>>, count i64
SELECT e.source_id AS source_id
     , COUNT(*) AS count
  FROM big_repo_keyhive_event_log e
 WHERE e.scope_id = :scope_id
   AND NOT EXISTS (
       SELECT 1
         FROM big_repo_keyhive_admissions a
        WHERE a.scope_id = e.scope_id AND a.event_hash = e.event_hash
   )
 GROUP BY e.source_id;
