-- name: applied_operations :many
-- returns: plug_id String, scope String, capability_id Vec<u8>
SELECT plug_id
     , scope
     , capability_id
  FROM nodule_applied_operations
 ORDER BY plug_id;
