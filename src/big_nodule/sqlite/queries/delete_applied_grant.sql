-- name: delete_applied_grant :exec
-- param: cartridge_id String
-- param: drawer_id String
DELETE FROM nodule_applied_grants
 WHERE cartridge_id = :cartridge_id
   AND drawer_id = :drawer_id;
