-- name: upsert_actor :exec
-- param: plug_id String
-- param: nodule_id String
-- param: actor_uuid String
INSERT INTO nodule_actors
     ( plug_id
     , nodule_id
     , actor_uuid
     )
VALUES ( :plug_id
       , :nodule_id
       , :actor_uuid
       )
ON CONFLICT (plug_id) DO UPDATE
   SET nodule_id = excluded.nodule_id
     , actor_uuid = excluded.actor_uuid;
