-- name: delete_applied_operations :exec
-- param: plug_id String
DELETE FROM nodule_applied_operations
 WHERE plug_id = :plug_id;
