-- name: applied_operations :many
-- returns: cartridge_id String, scope String, capability_id Vec<u8>
SELECT cartridge_id
     , scope
     , capability_id
  FROM nodule_applied_operations
 ORDER BY cartridge_id;
