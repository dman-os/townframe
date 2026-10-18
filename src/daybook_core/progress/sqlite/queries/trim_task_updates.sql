-- name: trim_task_updates :exec
-- param: task_id String
-- param: keep i64
DELETE FROM progress_task_updates
 WHERE task_id = :task_id
   AND sequence <= COALESCE(
         (
           SELECT sequence
             FROM progress_task_updates
            WHERE task_id = :task_id
            ORDER BY sequence DESC
            LIMIT 1 OFFSET :keep
         )
       , -1
       )
