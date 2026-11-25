-- name: set_shared_config_drawer :exec
-- param: drawer_id String
INSERT INTO nodule_shared_config_drawer
     ( singleton
     , drawer_id
     )
VALUES ( 1
       , :drawer_id
       )
ON CONFLICT (singleton) DO UPDATE
   SET drawer_id = excluded.drawer_id;
