-- name: upsert_actor :exec
-- param: plug_id String
-- param: actor_uuid String
INSERT INTO nodule_actors
     ( plug_id
     , actor_uuid
     )
VALUES ( :plug_id
       , :actor_uuid
       )
ON CONFLICT (plug_id) DO UPDATE
   SET actor_uuid = excluded.actor_uuid;
