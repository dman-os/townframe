-- name: init_done :many
-- returns: plug_id String, init_key String, revision String
SELECT plug_id
     , init_key
     , revision
  FROM nodule_init_done
 ORDER BY plug_id
        , init_key;
