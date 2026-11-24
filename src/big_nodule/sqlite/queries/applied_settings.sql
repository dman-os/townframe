-- name: applied_settings :many
-- returns: cartridge_id String, doc_id String, kinds String, capability_id Vec<u8>
SELECT cartridge_id
     , doc_id
     , kinds
     , capability_id
  FROM nodule_applied_settings
 ORDER BY cartridge_id;
