-- name: upsert_service_drawer :exec
-- param: drawer_id String
-- param: cartridge_id String
-- param: origin String
INSERT INTO nodule_service_drawers
     ( drawer_id
     , cartridge_id
     , origin
     )
VALUES ( :drawer_id
       , :cartridge_id
       , :origin
       )
ON CONFLICT (drawer_id, cartridge_id) DO UPDATE
   SET origin = CASE
           WHEN nodule_service_drawers.origin = 'created' THEN 'created'
           ELSE excluded.origin
       END;
