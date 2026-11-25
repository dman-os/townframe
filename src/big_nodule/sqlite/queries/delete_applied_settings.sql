-- name: delete_applied_settings :exec
-- param: cartridge_id String
DELETE FROM nodule_applied_settings
 WHERE cartridge_id = :cartridge_id;
