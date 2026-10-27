-- name: load_local :many
-- returns: role String, drawer_id String
SELECT role
     , drawer_id
  FROM big_cabinet_drawer_local;
