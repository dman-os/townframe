-- name: purge_done_commands :exec
-- param: cutoff i64
DELETE FROM big_cabinet_commands
 WHERE state = 'done'
   AND done_at < :cutoff;
