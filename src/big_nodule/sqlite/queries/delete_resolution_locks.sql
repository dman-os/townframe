-- name: delete_resolution_locks :exec
-- param: cartridge_id String
DELETE FROM nodule_resolution_lock
 WHERE cartridge_id = :cartridge_id;
