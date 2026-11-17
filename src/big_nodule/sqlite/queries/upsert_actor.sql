-- name: upsert_actor :exec
-- param: cartridge_id String
-- param: actor_uuid String
INSERT INTO nodule_actors
     ( cartridge_id
     , actor_uuid
     )
VALUES ( :cartridge_id
       , :actor_uuid
       )
ON CONFLICT (cartridge_id) DO UPDATE
   SET actor_uuid = excluded.actor_uuid;
