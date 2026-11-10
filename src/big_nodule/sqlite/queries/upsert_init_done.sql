-- name: upsert_init_done :exec
-- param: cartridge_id String
-- param: init_key String
-- param: revision String
INSERT INTO nodule_init_done
     ( cartridge_id
     , init_key
     , revision
     )
VALUES ( :cartridge_id
       , :init_key
       , :revision
       )
ON CONFLICT (cartridge_id, init_key) DO UPDATE
   SET revision = excluded.revision;
