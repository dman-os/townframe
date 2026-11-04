-- name: delete_init_done :exec
-- param: plug_id String
DELETE FROM nodule_init_done
 WHERE plug_id = :plug_id;
