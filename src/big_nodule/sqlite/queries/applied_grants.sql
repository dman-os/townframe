-- name: applied_grants :many
-- returns: plug_id String, drawer_id String, access String, capability_id Vec<u8>
SELECT plug_id
     , drawer_id
     , access
     , capability_id
  FROM nodule_applied_grants
 ORDER BY plug_id
        , drawer_id;
