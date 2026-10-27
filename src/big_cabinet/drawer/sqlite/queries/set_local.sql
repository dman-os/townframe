-- name: set_local :exec
-- param: role String
-- param: drawer_id String
INSERT INTO big_cabinet_drawer_local (role, drawer_id)
VALUES (:role, :drawer_id)
ON CONFLICT (role) DO UPDATE SET drawer_id = excluded.drawer_id;
