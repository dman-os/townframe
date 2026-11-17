-- name: delete_applied_operations :exec
-- param: cartridge_id String
DELETE FROM nodule_applied_operations
 WHERE cartridge_id = :cartridge_id;
