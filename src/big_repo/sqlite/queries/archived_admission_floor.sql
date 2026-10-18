-- name: archived_admission_floor :one
-- param: scope_id i64
-- returns: floor i64
SELECT COALESCE(
           (SELECT MIN(a.seq)
              FROM big_repo_keyhive_admissions a
             WHERE a.scope_id = :scope_id
               AND NOT EXISTS (
                   SELECT 1
                     FROM big_repo_keyhive_event_tombstones t
                    WHERE t.scope_id = a.scope_id AND t.event_hash = a.event_hash
               ))
         , (SELECT COALESCE(MAX(seq), 0) + 1
              FROM big_repo_keyhive_admissions
             WHERE scope_id = :scope_id)
       ) - 1 AS floor;
