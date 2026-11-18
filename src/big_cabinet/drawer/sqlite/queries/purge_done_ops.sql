-- name: purge_done_ops :exec
-- param: cutoff i64
DELETE FROM big_cabinet_drawer_ops
 WHERE step = 'done'
   AND done_at < :cutoff;
