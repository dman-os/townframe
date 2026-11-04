-- name: actors :many
-- returns: plug_id String, actor_uuid String
SELECT plug_id
     , actor_uuid
  FROM nodule_actors
 ORDER BY plug_id;
