-- name: actors :many
-- returns: plug_id String, nodule_id String, actor_uuid String
SELECT plug_id
     , nodule_id
     , actor_uuid
  FROM nodule_actors
 ORDER BY plug_id;
