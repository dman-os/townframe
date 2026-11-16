-- name: actors :many
-- returns: cartridge_id String, actor_uuid String
SELECT cartridge_id
     , actor_uuid
  FROM nodule_actors
 ORDER BY cartridge_id;
