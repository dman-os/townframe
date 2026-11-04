-- name: upsert_applied_operations :exec
-- param: plug_id String
-- param: scope String
-- param: capability_id Vec<u8>
INSERT INTO nodule_applied_operations
     ( plug_id
     , scope
     , capability_id
     )
VALUES ( :plug_id
       , :scope
       , :capability_id
       )
ON CONFLICT (plug_id) DO UPDATE
   SET scope = excluded.scope
     , capability_id = excluded.capability_id;
