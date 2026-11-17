-- name: upsert_applied_operations :exec
-- param: cartridge_id String
-- param: scope String
-- param: capability_id Vec<u8>
INSERT INTO nodule_applied_operations
     ( cartridge_id
     , scope
     , capability_id
     )
VALUES ( :cartridge_id
       , :scope
       , :capability_id
       )
ON CONFLICT (cartridge_id) DO UPDATE
   SET scope = excluded.scope
     , capability_id = excluded.capability_id;
