-- name: prune_admitted_events :exec
-- param: scope_id i64
-- param: watermark i64
DELETE FROM big_repo_keyhive_event_log
 WHERE scope_id = :scope_id
   AND EXISTS (
       SELECT 1
         FROM big_repo_keyhive_admissions a
         JOIN big_repo_keyhive_event_tombstones t
           ON t.scope_id = a.scope_id AND t.event_hash = a.event_hash
        WHERE a.scope_id = big_repo_keyhive_event_log.scope_id
          AND a.event_hash = big_repo_keyhive_event_log.event_hash
          AND a.seq <= :watermark
   );
