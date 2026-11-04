-- name: upsert_applied_grant :exec
-- param: plug_id String
-- param: drawer_id String
-- param: access String
-- param: capability_id Vec<u8>
INSERT INTO nodule_applied_grants
     ( plug_id
     , drawer_id
     , access
     , capability_id
     )
VALUES ( :plug_id
       , :drawer_id
       , :access
       , :capability_id
       )
ON CONFLICT (plug_id, drawer_id) DO UPDATE
   SET access = excluded.access
     , capability_id = excluded.capability_id;
