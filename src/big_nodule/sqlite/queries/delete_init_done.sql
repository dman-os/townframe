-- name: delete_init_done :exec
-- param: cartridge_id String
DELETE FROM nodule_init_done
 WHERE cartridge_id = :cartridge_id;
