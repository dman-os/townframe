-- name: delete_resolution_locks :exec
-- param: plug_id String
DELETE FROM nodule_resolution_lock
 WHERE plug_id = :plug_id;
