-- name: applied_grants :many
-- returns: cartridge_id String, drawer_id String, access String, capability_id Vec<u8>
SELECT cartridge_id
     , drawer_id
     , access
     , capability_id
  FROM nodule_applied_grants
 ORDER BY cartridge_id
        , drawer_id;
