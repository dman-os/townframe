-- name: init_done :many
-- returns: cartridge_id String, init_key String, revision String
SELECT cartridge_id
     , init_key
     , revision
  FROM nodule_init_done
 ORDER BY cartridge_id
        , init_key;
