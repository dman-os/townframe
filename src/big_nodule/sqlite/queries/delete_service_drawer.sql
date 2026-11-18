-- name: delete_service_drawer :exec
-- param: drawer_id String
-- param: cartridge_id String
DELETE FROM nodule_service_drawers
 WHERE drawer_id = :drawer_id
   AND cartridge_id = :cartridge_id;
