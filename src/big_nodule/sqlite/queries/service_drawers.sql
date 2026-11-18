-- name: service_drawers :many
-- returns: drawer_id String, cartridge_id String, origin String
SELECT drawer_id
     , cartridge_id
     , origin
  FROM nodule_service_drawers
 ORDER BY drawer_id
        , cartridge_id;
