-- name: set_registry_drawer :exec
-- param: drawer_id String
INSERT INTO plugs_registry_drawer
     ( singleton
     , drawer_id
     )
VALUES ( 1
       , :drawer_id
       )
ON CONFLICT (singleton) DO UPDATE
   SET drawer_id = excluded.drawer_id;
