-- name: upsert_init_done :exec
-- param: plug_id String
-- param: init_key String
-- param: revision String
INSERT INTO nodule_init_done
     ( plug_id
     , init_key
     , revision
     )
VALUES ( :plug_id
       , :init_key
       , :revision
       )
ON CONFLICT (plug_id, init_key) DO UPDATE
   SET revision = excluded.revision;
